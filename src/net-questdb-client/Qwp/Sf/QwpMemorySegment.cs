/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     RAM-backed segment used when sf_dir is null. Same envelope format as
///     <see cref="QwpMmapSegment" /> but skips the on-disk header and seal trailer — the segment
///     is never recovered, so persistence-only fields would be dead weight.
/// </summary>
internal sealed class QwpMemorySegment : IQwpSegment
{
    public const int EnvelopeHeaderSize = QwpMmapSegment.EnvelopeHeaderSize;
    public const int DefaultMaxFrameLength = QwpMmapSegment.DefaultMaxFrameLength;

    // A single RAM-backed segment is malloc'd up front; cap it so an absurd configured capacity
    // surfaces as a clear config error instead of an opaque NativeMemory.Alloc OOM.
    public const long MaxCapacity = 4L * 1024 * 1024 * 1024;

    // Outstanding bytes held by live native segment buffers. Incremented when Allocate hands back a
    // segment, decremented when its buffer is freed. A non-zero residual after a full GC means a
    // segment was dropped without being freed — i.e. a native-memory leak. Test/diagnostic seam.
    internal static long LiveNativeBytes;

    // Raw NativeMemory.Alloc'd buffer. The segment is a "dumb" resource: it frees its buffer in
    // Dispose but carries no finalizer of its own. The holder (the ring it lives in) is responsible
    // for disposing every segment on its own Dispose/finalizer, so a sender dropped without teardown
    // does not leak.
    private readonly unsafe byte* _basePtr;
    private readonly long _capacity;
    private readonly int _maxFrameLength;
    private long[] _offsetTable;
    private int _offsetTableCount;
    private long _writePosition;
    private bool _disposed;

    private unsafe QwpMemorySegment(byte* basePtr, long capacity, long baseFsn, int maxFrameLength)
    {
        _basePtr = basePtr;
        _capacity = capacity;
        BaseFsn = baseFsn;
        _maxFrameLength = maxFrameLength;
        _offsetTable = new long[16];
        _offsetTableCount = 0;
        _writePosition = 0;
    }

    public static unsafe QwpMemorySegment Allocate(long capacity, long baseFsn, int maxFrameLength = DefaultMaxFrameLength)
    {
        if (capacity <= EnvelopeHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must exceed envelope header size");
        }

        if (capacity > MaxCapacity)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity),
                $"capacity {capacity} exceeds the {MaxCapacity}-byte memory-segment cap");
        }

        var ptr = (byte*)NativeMemory.Alloc((nuint)capacity);
        try
        {
            new Span<byte>(ptr, (int)Math.Min(capacity, int.MaxValue)).Clear();
            for (long off = int.MaxValue; off < capacity; off += int.MaxValue)
            {
                var n = (int)Math.Min(capacity - off, int.MaxValue);
                new Span<byte>(ptr + off, n).Clear();
            }
            var segment = new QwpMemorySegment(ptr, capacity, baseFsn, maxFrameLength);
            Interlocked.Add(ref LiveNativeBytes, capacity);
            return segment;
        }
        catch
        {
            NativeMemory.Free(ptr);
            throw;
        }
    }

    public string Path => "<memory>";
    public long Capacity => _capacity;
    public long BaseFsn { get; }
    public long WritePosition => Volatile.Read(ref _writePosition);
    public long NextFsn => BaseFsn + EnvelopeCount;
    public long EnvelopeCount => Volatile.Read(ref _offsetTableCount);
    public bool IsSealed { get; private set; }

    public unsafe bool TryAppend(ReadOnlySpan<byte> frame)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpMemorySegment));
        }

        if (IsSealed)
        {
            throw new InvalidOperationException("segment is sealed");
        }

        if (frame.Length == 0)
        {
            throw new ArgumentException("empty frames are not permitted on the wire", nameof(frame));
        }

        if (frame.Length > _maxFrameLength)
        {
            throw new ArgumentException(
                $"frame length {frame.Length} exceeds the cap {_maxFrameLength}", nameof(frame));
        }

        var envelopeStart = WritePosition;
        var totalSize = (long)EnvelopeHeaderSize + frame.Length;
        if (envelopeStart + totalSize > _capacity)
        {
            return false;
        }

        Span<byte> header = stackalloc byte[EnvelopeHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(4, 4), (uint)frame.Length);
        var crcOverLen = QwpCrc32C.Compute(header.Slice(4, 4));
        var crc = QwpCrc32C.Compute(frame, crcOverLen);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(0, 4), crc);

        WriteSpan(envelopeStart, header);
        WriteSpan(envelopeStart + EnvelopeHeaderSize, frame);

        Volatile.Write(ref _writePosition, _writePosition + totalSize);
        AppendOffset(envelopeStart);
        return true;
    }

    public int TryReadFrame(long offset, Span<byte> destination, out long envelopeFsn)
    {
        envelopeFsn = -1;
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpMemorySegment));
        }

        if (offset < 0 || offset >= WritePosition)
        {
            return -1;
        }

        Span<byte> header = stackalloc byte[EnvelopeHeaderSize];
        ReadSpan(offset, header);

        var crc = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(0, 4));
        var lenU = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(4, 4));
        if (lenU == 0 || lenU > (uint)_maxFrameLength)
        {
            return -1;
        }

        var len = (int)lenU;
        if (destination.Length < len)
        {
            throw new ArgumentException(
                $"destination too small: need {len}, got {destination.Length}", nameof(destination));
        }

        ReadSpan(offset + EnvelopeHeaderSize, destination.Slice(0, len));

        var crcOverLen = QwpCrc32C.Compute(header.Slice(4, 4));
        var actual = QwpCrc32C.Compute(destination.Slice(0, len), crcOverLen);
        if (actual != crc)
        {
            throw new InvalidDataException(
                $"memory segment: envelope at offset {offset} failed CRC verification");
        }

        envelopeFsn = OffsetToFsn(offset);
        return len;
    }

    public long? OffsetOfEnvelope(long envelopeIndex)
    {
        var count = Volatile.Read(ref _offsetTableCount);
        if (envelopeIndex < 0 || envelopeIndex >= count)
        {
            return null;
        }

        var table = Volatile.Read(ref _offsetTable);
        return Volatile.Read(ref table[(int)envelopeIndex]);
    }

    public void Seal()
    {
        IsSealed = true;
    }

    public void Flush()
    {
    }

    public unsafe void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        NativeMemory.Free(_basePtr);
        Interlocked.Add(ref LiveNativeBytes, -_capacity);
    }

    private void AppendOffset(long offset)
    {
        var table = _offsetTable;
        var count = _offsetTableCount;
        if (count >= table.Length)
        {
            var grown = new long[table.Length * 2];
            Array.Copy(table, grown, count);
            grown[count] = offset;
            Volatile.Write(ref _offsetTable, grown);
            Volatile.Write(ref _offsetTableCount, count + 1);
            return;
        }

        Volatile.Write(ref table[count], offset);
        Volatile.Write(ref _offsetTableCount, count + 1);
    }

    private long OffsetToFsn(long offset)
    {
        var count = Volatile.Read(ref _offsetTableCount);
        var table = Volatile.Read(ref _offsetTable);
        var idx = Array.BinarySearch(table, 0, count, offset);
        if (idx < 0) idx = ~idx - 1;
        if (idx < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "offset precedes the first envelope");
        }
        return BaseFsn + idx;
    }

    private unsafe void WriteSpan(long offset, ReadOnlySpan<byte> bytes)
    {
        if ((ulong)offset + (ulong)bytes.Length > (ulong)_capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        var dest = new Span<byte>(_basePtr + offset, bytes.Length);
        bytes.CopyTo(dest);
    }

    private unsafe void ReadSpan(long offset, Span<byte> bytes)
    {
        if ((ulong)offset + (ulong)bytes.Length > (ulong)_capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        var src = new ReadOnlySpan<byte>(_basePtr + offset, bytes.Length);
        src.CopyTo(bytes);
    }
}
