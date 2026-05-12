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
using System.Diagnostics;
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
internal sealed class EmptySegmentHeaderException : IOException
{
    public EmptySegmentHeaderException(string path)
        : base($"segment {path}: header is empty (no SF01 written yet)")
    {
    }
}

internal sealed class QwpMmapSegment : IQwpSegment
{
    public const int EnvelopeHeaderSize = 8;
    public const int HeaderSize = 24;
    public const uint FileMagic = 0x31304653;
    public const byte FileVersion = 1;
    // Frames are bounded only by their enclosing segment; CRC + length sanity catch torn tails.
    public const int DefaultMaxFrameLength = int.MaxValue;
    // Optional sidecar `<segment>.seal` written on Seal(); lets recovery skip the O(n) envelope
    // CRC scan when present. The segment file itself is byte-identical to the spec layout.
    public const int SealSidecarSize = 16;
    public const uint SealSidecarMagic = 0x534C5453;
    public const string SealSidecarSuffix = ".seal";

    private readonly MemoryMappedFile _mmap;
    private readonly MemoryMappedViewAccessor _view;
    private readonly FileStream _fileStream;
    private readonly SafeMemoryMappedViewHandle _handle;
    // Volatile-published immutable snapshot. Producer copy-on-grow; readers Volatile.Read.
    private long[] _offsetTable;
    private int _offsetTableCount;
    private readonly unsafe byte* _basePtr;
    private readonly long _viewSize;
    private readonly int _maxFrameLength;
    private bool _disposed;

    private unsafe QwpMmapSegment(
        string path,
        MemoryMappedFile mmap,
        MemoryMappedViewAccessor view,
        FileStream fileStream,
        long capacity,
        long baseFsn,
        long writePosition,
        List<long> offsetTable,
        int maxFrameLength)
    {
        Path = path;
        _mmap = mmap;
        _view = view;
        _fileStream = fileStream;
        _handle = view.SafeMemoryMappedViewHandle;
        Capacity = capacity;
        BaseFsn = baseFsn;
        _writePosition = writePosition;
        var initialCapacity = Math.Max(16, offsetTable.Count);
        _offsetTable = new long[initialCapacity];
        for (var i = 0; i < offsetTable.Count; i++) _offsetTable[i] = offsetTable[i];
        _offsetTableCount = offsetTable.Count;
        _maxFrameLength = maxFrameLength;

        byte* ptr = null;
        _handle.AcquirePointer(ref ptr);
        try
        {
            _basePtr = ptr + view.PointerOffset;
            _viewSize = checked((long)_handle.ByteLength);
        }
        catch
        {
            _handle.ReleasePointer();
            throw;
        }
    }

    /// <summary>Filesystem path of the segment file.</summary>
    public string Path { get; }

    /// <summary>Total mmap'd byte capacity. Fixed at construction.</summary>
    public long Capacity { get; }

    /// <summary>FSN of the first envelope in this segment.</summary>
    public long BaseFsn { get; }

    private long _writePosition;

    /// <summary>Byte offset where the next envelope will be written.</summary>
    public long WritePosition => Volatile.Read(ref _writePosition);

    /// <summary>FSN of the next envelope (if appended).</summary>
    public long NextFsn => BaseFsn + EnvelopeCount;

    /// <summary>Number of valid envelopes in the segment.</summary>
    public long EnvelopeCount => Volatile.Read(ref _offsetTableCount);

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
        int maxFrameLength = DefaultMaxFrameLength)
    {
        if (capacity <= HeaderSize + EnvelopeHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be larger than the file + envelope header");
        }

        var (mmap, fs) = QwpFiles.OpenMemoryMappedSegment(path, capacity);
        MemoryMappedViewAccessor? view = null;
        try
        {
            view = mmap.CreateViewAccessor(0, capacity, MemoryMappedFileAccess.ReadWrite);

            var onDiskBaseFsn = ReadOrInitHeader(view, path, baseFsn);
            if (baseFsn >= 0 && onDiskBaseFsn != baseFsn)
            {
                throw new InvalidDataException(
                    $"segment {path}: on-disk baseSeq {onDiskBaseFsn} does not match expected {baseFsn}");
            }

            var trustedEnd = TryReadSealSidecar(path, capacity);
            var (writePos, offsets) = ScanForLastGoodEnvelope(view, capacity, maxFrameLength, trustedEnd);

            var tornByteCount = CountTornTailBytes(view, writePos, capacity);
            if (tornByteCount > 0)
            {
                Trace.TraceWarning(
                    "QWP segment recovery: torn tail in {0} at offset {1} ({2} non-zero byte(s) in 8-byte boundary window); truncating.",
                    path, writePos, tornByteCount);
            }

            ZeroViewRange(view, writePos, capacity - writePos);

            return new QwpMmapSegment(path, mmap, view, fs, capacity, onDiskBaseFsn, writePos, offsets, maxFrameLength);
        }
        catch (Exception)
        {
            view?.Dispose();
            mmap.Dispose();
            fs.Dispose();
            throw;
        }
    }

    /// <summary>Opens an existing segment, reading baseFsn from the header. Returns null if header is empty.</summary>
    public static QwpMmapSegment? OpenExisting(
        string path,
        long capacity,
        int maxFrameLength = DefaultMaxFrameLength)
    {
        try
        {
            return Open(path, capacity, baseFsn: -1, maxFrameLength);
        }
        catch (EmptySegmentHeaderException)
        {
            return null;
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

        Volatile.Write(ref _writePosition, _writePosition + totalSize);
        AppendOffset(envelopeStart);
        return true;
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
        var count = Volatile.Read(ref _offsetTableCount);
        if (envelopeIndex < 0 || envelopeIndex >= count)
        {
            return null;
        }

        var table = Volatile.Read(ref _offsetTable);
        return Volatile.Read(ref table[(int)envelopeIndex]);
    }

    /// <summary>
    ///     Marks the segment as no longer accepting appends. Flushes the segment, then writes a
    ///     <see cref="SealSidecarSuffix" /> sidecar so the next Open() can skip the per-envelope CRC
    ///     walk. Sidecar failures are non-fatal — recovery falls back to a full scan.
    /// </summary>
    public void Seal()
    {
        if (IsSealed)
        {
            return;
        }

        IsSealed = true;
        Flush();
        WriteSealSidecar(Path, _writePosition);
    }

    /// <summary>Returns the sidecar path for a given segment file.</summary>
    public static string SidecarPath(string segmentPath) => segmentPath + SealSidecarSuffix;

    private static void WriteSealSidecar(string segmentPath, long lastGoodOffset)
    {
        if (lastGoodOffset < HeaderSize) return;

        Span<byte> sidecar = stackalloc byte[SealSidecarSize];
        BinaryPrimitives.WriteUInt32LittleEndian(sidecar.Slice(0, 4), SealSidecarMagic);
        BinaryPrimitives.WriteUInt32LittleEndian(sidecar.Slice(4, 4), 0);
        BinaryPrimitives.WriteInt64LittleEndian(sidecar.Slice(8, 8), lastGoodOffset);

        var sidecarPath = SidecarPath(segmentPath);
        var tmpPath = sidecarPath + ".tmp";
        try
        {
            using (var fs = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                fs.Write(sidecar);
                fs.Flush(flushToDisk: true);
            }
            File.Move(tmpPath, sidecarPath, overwrite: true);
        }
        catch
        {
            SfCleanup.DeleteFile(tmpPath);
        }
    }

    private static long TryReadSealSidecar(string segmentPath, long capacity)
    {
        var sidecarPath = SidecarPath(segmentPath);
        if (!File.Exists(sidecarPath)) return -1L;

        Span<byte> sidecar = stackalloc byte[SealSidecarSize];
        try
        {
            using var fs = new FileStream(sidecarPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (fs.Length != SealSidecarSize) return -1L;
            var n = fs.Read(sidecar);
            if (n != SealSidecarSize) return -1L;
        }
        catch
        {
            return -1L;
        }

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(sidecar.Slice(0, 4));
        if (magic != SealSidecarMagic) return -1L;

        var offset = BinaryPrimitives.ReadInt64LittleEndian(sidecar.Slice(8, 8));
        if (offset < HeaderSize || offset > capacity) return -1L;

        return offset;
    }

    /// <summary>Forces dirty pages to be written to disk.</summary>
    public void Flush()
    {
        if (_disposed)
        {
            return;
        }

        _view.Flush();
        _fileStream.Flush(flushToDisk: true);
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
            _fileStream.Flush(flushToDisk: true);
        }
        catch (Exception ex)
        {
            flushError = ex;
        }

        SfCleanup.Run(() => _handle.ReleasePointer());
        _view.Dispose();
        _mmap.Dispose();
        SfCleanup.Dispose(_fileStream);

        if (flushError is not null)
        {
            throw flushError;
        }
    }

    /// <summary>
    ///     Public test seam: replays the mmap and returns the last good offset and the table
    ///     of envelope start offsets. When <paramref name="trustedEnd" /> is non-negative (set
    ///     by Open after a valid seal sidecar), envelopes between HeaderSize and trustedEnd are
    ///     indexed without per-envelope CRC verification.
    /// </summary>
    internal static unsafe (long WritePosition, List<long> Offsets) ScanForLastGoodEnvelope(
        MemoryMappedViewAccessor view,
        long capacity,
        int maxFrameLength = DefaultMaxFrameLength,
        long trustedEnd = -1L)
    {
        long offset = HeaderSize;
        var offsets = new List<long>();

        var handle = view.SafeMemoryMappedViewHandle;
        byte* basePtr = null;
        handle.AcquirePointer(ref basePtr);
        try
        {
            var skipCrc = trustedEnd >= HeaderSize;
            var endLimit = skipCrc ? trustedEnd : capacity;
            while (offset + EnvelopeHeaderSize <= endLimit)
            {
                var header = new ReadOnlySpan<byte>(basePtr + offset, EnvelopeHeaderSize);

                var crc = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(0, 4));
                var lenU = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(4, 4));

                if (lenU == 0 && crc == 0)
                {
                    break;
                }

                if (lenU == 0 || lenU > (uint)maxFrameLength)
                {
                    break;
                }

                var len = (int)lenU;
                if (offset + EnvelopeHeaderSize + len > endLimit)
                {
                    break;
                }

                if (!skipCrc)
                {
                    var frame = new ReadOnlySpan<byte>(basePtr + offset + EnvelopeHeaderSize, len);
                    var crcOverLen = QwpCrc32C.Compute(header.Slice(4, 4));
                    var actual = QwpCrc32C.Compute(frame, crcOverLen);
                    if (actual != crc)
                    {
                        break;
                    }
                }

                offsets.Add(offset);
                offset += EnvelopeHeaderSize + len;
            }
        }
        finally
        {
            handle.ReleasePointer();
        }

        // If we trusted the sidecar and the envelope walk ended before reaching it, the sidecar
        // was lying — fall back to a full CRC scan from scratch.
        if (trustedEnd >= HeaderSize && offset != trustedEnd)
        {
            return ScanForLastGoodEnvelope(view, capacity, maxFrameLength, trustedEnd: -1L);
        }

        return (offset, offsets);
    }

    private static unsafe int CountTornTailBytes(MemoryMappedViewAccessor view, long offset, long capacity)
    {
        var window = (int)Math.Min(8, capacity - offset);
        if (window <= 0) return 0;

        var handle = view.SafeMemoryMappedViewHandle;
        byte* basePtr = null;
        handle.AcquirePointer(ref basePtr);
        try
        {
            var count = 0;
            for (var i = 0; i < window; i++)
            {
                if (basePtr[offset + i] != 0) count++;
            }
            return count;
        }
        finally
        {
            handle.ReleasePointer();
        }
    }

    private static long ReadOrInitHeader(
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
            if (baseFsn < 0)
            {
                throw new EmptySegmentHeaderException(path);
            }
            WriteHeader(view, baseFsn, NowMicros());
            return baseFsn;
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

        var readBaseFsn = BinaryPrimitives.ReadInt64LittleEndian(hdr.Slice(8, 8));
        if (readBaseFsn < 0)
        {
            throw new InvalidDataException($"segment {path}: bad baseFsn {readBaseFsn}");
        }
        return readBaseFsn;
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

    private static long NowMicros()
    {
        const long unixEpochTicks = 621355968000000000L;
        return (DateTime.UtcNow.Ticks - unixEpochTicks) / 10L;
    }

    /// <summary>
    ///     Returns the FSN at the given offset using the offset table. O(log N) via binary search.
    ///     Used by replay paths that resolve an offset back to an FSN.
    /// </summary>
    private long OffsetToFsn(long offset)
    {
        var count = Volatile.Read(ref _offsetTableCount);
        var table = Volatile.Read(ref _offsetTable);
        var idx = Array.BinarySearch(table, 0, count, offset);
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
