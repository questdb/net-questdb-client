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
using System.IO.MemoryMappedFiles;
using Microsoft.Win32.SafeHandles;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     A single fixed-size, memory-mapped segment file holding back-to-back QWP frame envelopes.
/// </summary>
/// <remarks>
///     File layout (all little-endian):
///     <code>
///       offset  size  field
///         0       4   magic  = 0x31304653 ('SF01')
///         4       1   version = 1
///         5       1   flags   = 0
///         6       2   reserved = 0
///         8       8   baseSeq
///        16       8   createdAtMicros
///        24      ..   envelope stream: [u32 crc32c | u32 frame_len | frame bytes] back-to-back
///     </code>
///     The CRC covers <c>frame_len</c> + <c>frame bytes</c>. Replay walks envelopes from
///     <see cref="HeaderSize" /> and stops on a torn tail (oversized length, CRC mismatch, or
///     envelope crossing the segment boundary).
///     <para />
///     The file is pre-extended to <see cref="Capacity" /> so writes never grow the file at
///     append time. Trailing zeros indicate "no envelope here yet".
///     <para />
///     <b>Thread safety.</b> The segment is not internally synchronised. The owning
///     <see cref="QwpSegmentRing" /> and <see cref="Sf.QwpCursorSendEngine" /> serialise all access
///     to a given segment under <c>_stateLock</c> — producer's <see cref="TryAppend" /> never overlaps
///     with reader's <see cref="OffsetOfEnvelope" /> / <see cref="TryReadFrame" /> on the same
///     instance. Don't use this class directly without that external serialisation; <c>List&lt;long&gt;</c>
///     for the offset table is not safe for concurrent mutation.
/// </remarks>
internal sealed class QwpMmapSegment : IDisposable
{
    public const int EnvelopeHeaderSize = 8;
    public const int HeaderSize = 24;
    public const uint FileMagic = 0x31304653;
    public const byte FileVersion = 1;
    public const int DefaultMaxFrameLength = 16 * 1024 * 1024;

    private const int OffsetFlags = 5;
    private const byte FlagSealed = 0x01;

    private readonly MemoryMappedFile _mmap;
    private readonly MemoryMappedViewAccessor _view;
    private readonly SafeMemoryMappedViewHandle _handle;
    private readonly List<long> _offsetTable;
    private readonly unsafe byte* _basePtr;
    private readonly long _viewSize;
    private readonly int _maxFrameLength;
    private readonly bool _flushOnAppend;
    private bool _disposed;

    private unsafe QwpMmapSegment(
        string path,
        MemoryMappedFile mmap,
        MemoryMappedViewAccessor view,
        long capacity,
        long baseFsn,
        long writePosition,
        List<long> offsetTable,
        int maxFrameLength,
        bool flushOnAppend)
    {
        Path = path;
        _mmap = mmap;
        _view = view;
        _handle = view.SafeMemoryMappedViewHandle;
        Capacity = capacity;
        BaseFsn = baseFsn;
        WritePosition = writePosition;
        _offsetTable = offsetTable;
        _maxFrameLength = maxFrameLength;
        _flushOnAppend = flushOnAppend;

        byte* ptr = null;
        _handle.AcquirePointer(ref ptr);
        _basePtr = ptr + view.PointerOffset;
        _viewSize = checked((long)_handle.ByteLength);
    }

    /// <summary>Filesystem path of the segment file.</summary>
    public string Path { get; }

    /// <summary>Total mmap'd byte capacity. Fixed at construction.</summary>
    public long Capacity { get; }

    /// <summary>FSN of the first envelope in this segment.</summary>
    public long BaseFsn { get; }

    /// <summary>Byte offset where the next envelope will be written.</summary>
    public long WritePosition { get; private set; }

    /// <summary>FSN of the next envelope (if appended).</summary>
    public long NextFsn => BaseFsn + EnvelopeCount;

    /// <summary>Number of valid envelopes in the segment.</summary>
    public long EnvelopeCount => _offsetTable.Count;

    /// <summary>True when the segment cannot accept further appends (sealed by the manager).</summary>
    public bool IsSealed { get; private set; }

    /// <summary>
    ///     Opens an existing segment file and replays it to find the last good write position. If
    ///     the file is fresh (zeroed), writes the SF01 header with the supplied <paramref name="baseFsn" />.
    ///     If the file already has a valid SF01 header, validates magic+version and uses the on-disk
    ///     <c>baseSeq</c> (which must match <paramref name="baseFsn" />).
    /// </summary>
    /// <exception cref="InvalidDataException">If the on-disk header is corrupt or version-mismatched.</exception>
    public static QwpMmapSegment Open(
        string path,
        long capacity,
        long baseFsn,
        int maxFrameLength = DefaultMaxFrameLength,
        bool flushOnAppend = false)
    {
        if (capacity <= HeaderSize + EnvelopeHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be larger than the file + envelope header");
        }

        var mmap = QwpFiles.OpenMemoryMappedSegment(path, capacity);
        MemoryMappedViewAccessor? view = null;
        try
        {
            view = mmap.CreateViewAccessor(0, capacity, MemoryMappedFileAccess.ReadWrite);

            var (onDiskBaseFsn, sealed_) = ReadOrInitHeader(view, path, baseFsn);
            if (onDiskBaseFsn != baseFsn)
            {
                throw new InvalidDataException(
                    $"segment {path}: on-disk baseSeq {onDiskBaseFsn} does not match expected {baseFsn}");
            }

            var (writePos, offsets) = ScanForLastGoodEnvelope(view, capacity, maxFrameLength);
            ZeroViewRange(view, writePos, capacity - writePos);

            var seg = new QwpMmapSegment(path, mmap, view, capacity, baseFsn, writePos, offsets, maxFrameLength, flushOnAppend);
            if (sealed_)
            {
                seg.IsSealed = true;
            }
            return seg;
        }
        catch (Exception)
        {
            view?.Dispose();
            mmap.Dispose();
            throw;
        }
    }


    /// <summary>
    ///     Tries to append an envelope wrapping <paramref name="frame" />. Returns false if the
    ///     segment doesn't have room (caller should rotate to a new segment).
    /// </summary>
    /// <exception cref="InvalidOperationException">If the segment is sealed.</exception>
    public unsafe bool TryAppend(ReadOnlySpan<byte> frame)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpMmapSegment));
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
            // Replay would truncate this as a torn tail on next reopen.
            throw new ArgumentException(
                $"frame length {frame.Length} exceeds the replay cap {_maxFrameLength}",
                nameof(frame));
        }

        var envelopeStart = WritePosition;
        var totalSize = (long)EnvelopeHeaderSize + frame.Length;
        if (envelopeStart + totalSize > Capacity)
        {
            return false;
        }

        // Layout: [crc(4)][len(4)][frame...]. CRC covers len+frame.
        Span<byte> header = stackalloc byte[EnvelopeHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(4, 4), (uint)frame.Length);
        var crcOverLen = QwpCrc32C.Compute(header.Slice(4, 4));
        var crc = QwpCrc32C.Compute(frame, crcOverLen);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(0, 4), crc);

        WriteSpan(envelopeStart, header);
        WriteSpan(envelopeStart + EnvelopeHeaderSize, frame);

        if (_flushOnAppend)
        {
            _view.Flush();
        }

        WritePosition += totalSize;
        _offsetTable.Add(envelopeStart);
        return true;
    }

    /// <summary>
    ///     Reads the envelope at <paramref name="offset" /> into <paramref name="destination" />.
    /// </summary>
    /// <returns>
    ///     Number of frame bytes copied into <paramref name="destination" />, or <c>-1</c> if the
    ///     offset is past the last valid envelope.
    /// </returns>
    /// <exception cref="ArgumentException">If <paramref name="destination" /> is too small.</exception>
    /// <exception cref="InvalidDataException">If the envelope CRC fails to verify on-disk content.</exception>
    public int TryReadFrame(long offset, Span<byte> destination, out long envelopeFsn)
    {
        envelopeFsn = -1;
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpMmapSegment));
        }

        if (offset < 0 || offset >= WritePosition)
        {
            return -1;
        }

        Span<byte> header = stackalloc byte[EnvelopeHeaderSize];
        ReadSpan(offset, header);

        var crc = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(0, 4));
        var len = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(4, 4));
        if (len <= 0)
        {
            return -1;
        }

        if (destination.Length < len)
        {
            throw new ArgumentException(
                $"destination too small: need {len}, got {destination.Length}", nameof(destination));
        }

        ReadSpan(offset + EnvelopeHeaderSize, destination.Slice(0, len));

        // Verify per read: mmap pages can corrupt out-of-band after the Open() replay.
        var crcOverLen = QwpCrc32C.Compute(header.Slice(4, 4));
        var actual = QwpCrc32C.Compute(destination.Slice(0, len), crcOverLen);
        if (actual != crc)
        {
            throw new InvalidDataException(
                $"segment {Path}: envelope at offset {offset} failed CRC verification " +
                $"(expected 0x{crc:x8}, got 0x{actual:x8})");
        }

        envelopeFsn = OffsetToFsn(offset);
        return len;
    }

    /// <summary>
    ///     Returns the byte offset of envelope <paramref name="envelopeIndex" /> (0-based within
    ///     this segment). O(1) — backed by the offset table.
    /// </summary>
    public long? OffsetOfEnvelope(long envelopeIndex)
    {
        if (envelopeIndex < 0 || envelopeIndex >= _offsetTable.Count)
        {
            return null;
        }

        return _offsetTable[(int)envelopeIndex];
    }

    /// <summary>Marks the segment as no longer accepting appends and persists the flag to disk.</summary>
    public void Seal()
    {
        if (IsSealed)
        {
            return;
        }

        IsSealed = true;
        Span<byte> oneByte = stackalloc byte[1];
        oneByte[0] = FlagSealed;
        WriteToView(_view, OffsetFlags, oneByte);
    }

    /// <summary>Forces dirty pages to be written to disk.</summary>
    public void Flush()
    {
        if (_disposed)
        {
            return;
        }

        _view.Flush();
    }

    /// <summary>
    ///     Disposes the view and underlying mmap handle. A failed flush propagates as
    ///     <see cref="IOException" /> after teardown completes — SF's data-on-disk promise depends
    ///     on observing msync failures rather than swallowing them.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        Exception? flushError = null;
        try
        {
            _view.Flush();
        }
        catch (Exception ex)
        {
            flushError = ex;
        }

        SfCleanup.Run(() => _handle.ReleasePointer());
        _view.Dispose();
        _mmap.Dispose();

        if (flushError is not null)
        {
            throw flushError;
        }
    }

    /// <summary>
    ///     Public test seam: replays the entire mmap and returns the last good offset and the table
    ///     of envelope start offsets.
    /// </summary>
    internal static (long WritePosition, List<long> Offsets) ScanForLastGoodEnvelope(
        MemoryMappedViewAccessor view,
        long capacity,
        int maxFrameLength)
    {
        long offset = HeaderSize;
        var offsets = new List<long>();
        Span<byte> header = stackalloc byte[EnvelopeHeaderSize];
        byte[]? frameScratch = null;

        while (offset + EnvelopeHeaderSize <= capacity)
        {
            ViewToSpan(view, offset, header);

            var crc = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(0, 4));
            var len = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(4, 4));

            if (len == 0 && crc == 0)
            {
                break;
            }

            if (len <= 0 || len > maxFrameLength)
            {
                break;
            }

            if (offset + EnvelopeHeaderSize + len > capacity)
            {
                break;
            }

            if (frameScratch is null || frameScratch.Length < len)
            {
                frameScratch = new byte[Math.Max(len, 4096)];
            }
            ViewToSpan(view, offset + EnvelopeHeaderSize, frameScratch.AsSpan(0, len));
            var crcOverLen = QwpCrc32C.Compute(header.Slice(4, 4));
            var actual = QwpCrc32C.Compute(frameScratch.AsSpan(0, len), crcOverLen);
            if (actual != crc)
            {
                break;
            }

            offsets.Add(offset);
            offset += EnvelopeHeaderSize + len;
        }

        return (offset, offsets);
    }

    private static (long BaseFsn, bool Sealed) ReadOrInitHeader(
        MemoryMappedViewAccessor view, string path, long baseFsn)
    {
        Span<byte> hdr = stackalloc byte[HeaderSize];
        ViewToSpan(view, 0, hdr);

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(hdr.Slice(0, 4));
        if (magic == 0)
        {
            var allZero = true;
            for (var i = 4; i < HeaderSize; i++)
            {
                if (hdr[i] != 0) { allZero = false; break; }
            }
            if (!allZero)
            {
                throw new InvalidDataException($"segment {path}: missing SF01 magic but header bytes are non-zero");
            }
            WriteHeader(view, baseFsn, NowMicros());
            return (baseFsn, false);
        }

        if (magic != FileMagic)
        {
            throw new InvalidDataException(
                $"segment {path}: bad magic 0x{magic:x8}, expected 0x{FileMagic:x8} ('SF01')");
        }

        var version = hdr[4];
        if (version != FileVersion)
        {
            throw new InvalidDataException($"segment {path}: unsupported version {version}");
        }

        var flags = hdr[OffsetFlags];
        var sealed_ = (flags & FlagSealed) != 0;
        return (BinaryPrimitives.ReadInt64LittleEndian(hdr.Slice(8, 8)), sealed_);
    }

    private static void WriteHeader(MemoryMappedViewAccessor view, long baseFsn, long createdAtMicros)
    {
        Span<byte> hdr = stackalloc byte[HeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(hdr.Slice(0, 4), FileMagic);
        hdr[4] = FileVersion;
        hdr[5] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(hdr.Slice(6, 2), 0);
        BinaryPrimitives.WriteInt64LittleEndian(hdr.Slice(8, 8), baseFsn);
        BinaryPrimitives.WriteInt64LittleEndian(hdr.Slice(16, 8), createdAtMicros);
        WriteToView(view, 0, hdr);
    }

    private static long NowMicros() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;

    /// <summary>
    ///     Returns the FSN at the given offset using the offset table. O(log N) via binary search.
    ///     Used by replay paths that resolve an offset back to an FSN.
    /// </summary>
    private long OffsetToFsn(long offset)
    {
        var idx = _offsetTable.BinarySearch(offset);
        if (idx < 0)
        {
            // Offset doesn't sit on an envelope boundary; the bit-flip equivalent is the insertion
            // point. Treat the preceding envelope as the FSN.
            idx = ~idx - 1;
        }

        if (idx < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "offset precedes the first envelope");
        }

        return BaseFsn + idx;
    }

    private unsafe void WriteSpan(long offset, ReadOnlySpan<byte> bytes)
    {
        if ((ulong)offset + (ulong)bytes.Length > (ulong)_viewSize)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        var dest = new Span<byte>(_basePtr + offset, bytes.Length);
        bytes.CopyTo(dest);
    }

    private unsafe void ReadSpan(long offset, Span<byte> bytes)
    {
        if ((ulong)offset + (ulong)bytes.Length > (ulong)_viewSize)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        var src = new ReadOnlySpan<byte>(_basePtr + offset, bytes.Length);
        src.CopyTo(bytes);
    }

    /// <summary>
    ///     Static span access used by <see cref="ScanForLastGoodEnvelope" /> at construction time —
    ///     before <see cref="QwpMmapSegment" /> exists, so no instance pointer is available.
    /// </summary>
    private static unsafe void ViewToSpan(MemoryMappedViewAccessor view, long offset, Span<byte> dest)
    {
        var handle = view.SafeMemoryMappedViewHandle;
        byte* ptr = null;
        handle.AcquirePointer(ref ptr);
        try
        {
            var src = new ReadOnlySpan<byte>(ptr + view.PointerOffset + offset, dest.Length);
            src.CopyTo(dest);
        }
        finally
        {
            handle.ReleasePointer();
        }
    }

    private static unsafe void WriteToView(MemoryMappedViewAccessor view, long offset, ReadOnlySpan<byte> src)
    {
        var handle = view.SafeMemoryMappedViewHandle;
        byte* ptr = null;
        handle.AcquirePointer(ref ptr);
        try
        {
            var dest = new Span<byte>(ptr + view.PointerOffset + offset, src.Length);
            src.CopyTo(dest);
        }
        finally
        {
            handle.ReleasePointer();
        }
    }

    private static unsafe void ZeroViewRange(MemoryMappedViewAccessor view, long offset, long length)
    {
        if (length <= 0)
        {
            return;
        }

        var handle = view.SafeMemoryMappedViewHandle;
        byte* ptr = null;
        handle.AcquirePointer(ref ptr);
        try
        {
            var dest = ptr + view.PointerOffset + offset;
            // Loop in int.MaxValue chunks so Span<byte> can hold the slice.
            var remaining = length;
            while (remaining > 0)
            {
                var n = (int)Math.Min(remaining, int.MaxValue);
                new Span<byte>(dest, n).Clear();
                dest += n;
                remaining -= n;
            }
        }
        finally
        {
            handle.ReleasePointer();
        }
    }
}
