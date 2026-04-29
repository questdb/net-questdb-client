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
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     A single fixed-size, memory-mapped segment file holding back-to-back QWP frame envelopes.
/// </summary>
/// <remarks>
///     Wire-on-disk envelope: <c>[u32 crc32c | u32 frame_len | frame bytes]</c> stored
///     little-endian. The CRC covers <c>frame_len</c> + <c>frame bytes</c> (everything after the
///     CRC field itself).
///     <para />
///     Replay strategy: walk envelopes from offset 0; stop on a torn tail (oversized length, CRC
///     mismatch, or envelope crossing the segment boundary). The last-good offset becomes the
///     new write position; bytes beyond it are zeroed for clean reuse on next append.
///     <para />
///     The file is pre-extended to <see cref="Capacity" /> so writes never grow the file at
///     append time. Trailing zeros indicate "no envelope here yet" — see
///     <see cref="ScanForLastGoodEnvelope" />.
///     <para />
///     <b>Performance.</b> The view is acquired via <c>SafeMemoryMappedViewHandle.AcquirePointer</c>
///     and held for the segment lifetime; reads and writes go through that pointer with no per-call
///     <c>byte[]</c> allocation. An offset table indexed by <c>(fsn - BaseFsn)</c> provides O(1)
///     envelope lookups; appends update it incrementally and replay rebuilds it.
/// </remarks>
internal sealed class QwpMmapSegment : IDisposable
{
    /// <summary>Per-envelope header: 4 bytes CRC32C + 4 bytes frame length.</summary>
    public const int EnvelopeHeaderSize = 8;

    /// <summary>Default soft cap on a single frame's length, beyond which replay treats it as torn.</summary>
    public const int DefaultMaxFrameLength = 16 * 1024 * 1024;

    private readonly MemoryMappedFile _mmap;
    private readonly MemoryMappedViewAccessor _view;
    private readonly SafeMemoryMappedViewHandle _handle;
    // Offsets of the envelopes currently in the segment, indexed by `fsn - BaseFsn`.
    private readonly List<long> _offsetTable;
    private readonly unsafe byte* _basePtr;
    private readonly long _viewSize;
    private readonly int _maxFrameLength;
    private bool _disposed;

    private unsafe QwpMmapSegment(
        string path,
        MemoryMappedFile mmap,
        MemoryMappedViewAccessor view,
        long capacity,
        long baseFsn,
        long writePosition,
        List<long> offsetTable,
        int maxFrameLength)
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

        byte* ptr = null;
        _handle.AcquirePointer(ref ptr);
        // PointerOffset accounts for the OS-level alignment of the view's actual base.
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
    ///     the file does not exist, creates and zero-initialises one of the requested capacity.
    /// </summary>
    /// <param name="path">Filesystem path. The directory must already exist.</param>
    /// <param name="capacity">Segment size in bytes. Existing files smaller than this are extended.</param>
    /// <param name="baseFsn">FSN of the first envelope in this segment.</param>
    /// <param name="maxFrameLength">Frame-length cap used to detect torn / corrupt envelopes.</param>
    public static QwpMmapSegment Open(string path, long capacity, long baseFsn, int maxFrameLength = DefaultMaxFrameLength)
    {
        if (capacity <= EnvelopeHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be larger than the envelope header");
        }

        var mmap = QwpFiles.OpenMemoryMappedSegment(path, capacity);
        MemoryMappedViewAccessor? view = null;
        try
        {
            view = mmap.CreateViewAccessor(0, capacity, MemoryMappedFileAccess.ReadWrite);
            var (writePos, offsets) = ScanForLastGoodEnvelope(view, capacity, maxFrameLength);

            // Zero any garbage past the last good envelope so subsequent appends start clean.
            ZeroViewRange(view, writePos, capacity - writePos);

            return new QwpMmapSegment(path, mmap, view, capacity, baseFsn, writePos, offsets, maxFrameLength);
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

        // Don't verify CRC here — the segment was already replayed at Open. We trust the in-memory state.
        // Tests deliberately corrupting bytes will call ScanForLastGoodEnvelope explicitly.
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

    /// <summary>Marks the segment as no longer accepting appends.</summary>
    public void Seal()
    {
        IsSealed = true;
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

    /// <summary>Disposes the view and underlying mmap handle.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        try
        {
            _view.Flush();
        }
        catch (Exception)
        {
            // best-effort
        }

        try
        {
            _handle.ReleasePointer();
        }
        catch (Exception)
        {
            // best-effort; release pairs with AcquirePointer in the constructor.
        }

        _view.Dispose();
        _mmap.Dispose();
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
        long offset = 0;
        var offsets = new List<long>();
        Span<byte> header = stackalloc byte[EnvelopeHeaderSize];
        // Reused frame buffer for CRC validation; sized up to maxFrameLength only when we see one.
        byte[]? frameScratch = null;

        while (offset + EnvelopeHeaderSize <= capacity)
        {
            ViewToSpan(view, offset, header);

            var crc = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(0, 4));
            var len = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(4, 4));

            // A zero-length envelope (or all-zero header) is the natural "end of writes" sentinel.
            if (len == 0 && crc == 0)
            {
                break;
            }

            // Defensive: bit-rot or torn tail can leave plausible-looking but invalid headers.
            if (len <= 0 || len > maxFrameLength)
            {
                break;
            }

            if (offset + EnvelopeHeaderSize + len > capacity)
            {
                // Envelope claims to extend past the segment boundary — torn.
                break;
            }

            // Validate CRC against the frame payload.
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
