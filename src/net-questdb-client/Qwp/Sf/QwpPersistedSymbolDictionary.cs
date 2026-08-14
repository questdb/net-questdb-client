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
using QuestDB.Utils;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Per-slot, append-only symbol dictionary used to make delta QWP frames recoverable after a
///     process restart. The format matches the Java client's <c>.symbol-dict</c> side file.
/// </summary>
/// <remarks>
///     File header: <c>SYD1</c> little-endian, version 1, three reserved zero bytes. Each chunk is
///     <c>[entryCount varint][entryBytes varint][[len varint][utf8]...][crc32c u32]</c>; the CRC
///     covers both chunk varints and the complete entry region. New entries are written before the
///     frame that references them is published to the SF ring.
/// </remarks>
internal sealed class QwpPersistedSymbolDictionary : IDisposable
{
    internal const string FileName = ".symbol-dict";
    internal const uint FileMagic = 0x31445953; // "SYD1" little-endian
    internal const byte Version = 1;
    internal const int HeaderSize = 8;
    private const int CrcSize = sizeof(uint);

    private readonly object _lock = new();
    private readonly string _filePath;
    private readonly List<string> _entries;
    private FileStream? _stream;

    private QwpPersistedSymbolDictionary(string filePath, FileStream stream, List<string> entries)
    {
        _filePath = filePath;
        _stream = stream;
        _entries = entries;
    }

    public int Count
    {
        get { lock (_lock) return _entries.Count; }
    }

    public long FileLength
    {
        get
        {
            lock (_lock)
            {
                return _stream?.Length ?? 0L;
            }
        }
    }

    /// <summary>
    ///     Opens or reconstructs a slot dictionary and validates every surviving frame against it.
    ///     An empty ring starts a fresh id generation. A legacy self-sufficient ring can rebuild a
    ///     missing side file from its start-at-zero prefixes; a true delta gap fails closed.
    /// </summary>
    public static QwpPersistedSymbolDictionary OpenOrRecover(
        string slotDirectory,
        QwpSegmentRing ring)
    {
        ArgumentNullException.ThrowIfNull(slotDirectory);
        ArgumentNullException.ThrowIfNull(ring);
        Directory.CreateDirectory(slotDirectory);

        var filePath = Path.Combine(slotDirectory, FileName);
        if (ring.NextFsn <= ring.OldestFsn)
        {
            return CreateClean(filePath, Array.Empty<string>());
        }

        FileStream? stream = null;
        List<string>? entries = null;
        var validFile = false;
        try
        {
            (stream, entries, validFile) = TryOpenExisting(filePath);
            entries ??= new List<string>();
            var loadedCount = entries.Count;

            FoldRing(ring, entries);

            if (!validFile)
            {
                SfCleanup.Dispose(stream);
                stream = null;
                return CreateClean(filePath, entries);
            }

            var result = new QwpPersistedSymbolDictionary(filePath, stream!, entries);
            stream = null;
            try
            {
                if (entries.Count > loadedCount)
                {
                    result.AppendRange(entries, loadedCount, entries.Count);
                }
                return result;
            }
            catch
            {
                SfCleanup.Dispose(result);
                throw;
            }
        }
        catch
        {
            SfCleanup.Dispose(stream);
            throw;
        }
    }

    /// <summary>
    ///     Persists every symbol not already present in this side file. Idempotent after a failed
    ///     ring publication: a retry observes the advanced persisted count and does not duplicate it.
    /// </summary>
    public void AppendNewSymbols(QwpSymbolDictionary dictionary)
    {
        ArgumentNullException.ThrowIfNull(dictionary);
        lock (_lock)
        {
            EnsureOpen();
            if (_entries.Count > dictionary.Count)
            {
                throw new InvalidDataException(
                    $"persisted symbol dictionary contains {_entries.Count} entries but producer contains {dictionary.Count}");
            }
            if (_entries.Count == dictionary.Count)
            {
                return;
            }

            var additions = new string[dictionary.Count - _entries.Count];
            for (var i = 0; i < additions.Length; i++)
            {
                additions[i] = dictionary.GetSymbol(_entries.Count + i);
            }
            AppendRangeLocked(additions, 0, additions.Length);
            _entries.AddRange(additions);
        }
    }

    public string[] SnapshotEntries()
    {
        lock (_lock)
        {
            return _entries.ToArray();
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            var stream = _stream;
            _stream = null;
            SfCleanup.Dispose(stream);
        }
    }

    internal static void RemoveOrphan(string slotDirectory)
    {
        SfCleanup.DeleteFile(Path.Combine(slotDirectory, FileName));
    }

    private static QwpPersistedSymbolDictionary CreateClean(
        string filePath,
        IReadOnlyList<string> entries)
    {
        var tempPath = filePath + ".tmp-" + Guid.NewGuid().ToString("N");
        try
        {
            using (var temp = OpenFile(tempPath, FileMode.CreateNew))
            {
                WriteHeader(temp);
                if (entries.Count > 0)
                {
                    WriteChunk(temp, entries, 0, entries.Count);
                }
                temp.Flush(flushToDisk: false);
            }

            File.Move(tempPath, filePath, overwrite: true);
            var stream = OpenFile(filePath, FileMode.Open);
            stream.Position = stream.Length;
            return new QwpPersistedSymbolDictionary(filePath, stream, entries.ToList());
        }
        catch
        {
            SfCleanup.DeleteFile(tempPath);
            throw;
        }
    }

    private static (FileStream? Stream, List<string>? Entries, bool Valid) TryOpenExisting(
        string filePath)
    {
        if (!File.Exists(filePath))
        {
            return (null, null, false);
        }

        var fileLength = new FileInfo(filePath).Length;
        if (fileLength < HeaderSize || fileLength > int.MaxValue)
        {
            return (null, null, false);
        }

        var bytes = File.ReadAllBytes(filePath);
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0, 4)) != FileMagic
            || bytes[4] != Version)
        {
            return (null, null, false);
        }

        var entries = new List<string>();
        var p = HeaderSize;
        var validEnd = HeaderSize;
        while (p < bytes.Length)
        {
            var chunkStart = p;
            if (!TryReadIntVarint(bytes, ref p, bytes.Length, out var entryCount)
                || !TryReadIntVarint(bytes, ref p, bytes.Length, out var entryBytes)
                || entryCount <= 0 || entryBytes <= 0
                || entryCount > QwpConstants.MaxSymbolDictionarySize - entries.Count
                || entryBytes > bytes.Length - p - CrcSize)
            {
                break;
            }

            var entriesEnd = p + entryBytes;
            var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(entriesEnd, CrcSize));
            var actualCrc = QwpCrc32C.Compute(bytes.AsSpan(chunkStart, entriesEnd - chunkStart));
            if (storedCrc != actualCrc)
            {
                break;
            }

            var before = entries.Count;
            try
            {
                for (var i = 0; i < entryCount; i++)
                {
                    if (!TryReadIntVarint(bytes, ref p, entriesEnd, out var len)
                        || len < 0 || len > entriesEnd - p)
                    {
                        throw new InvalidDataException("malformed symbol dictionary entry");
                    }
                    entries.Add(QwpStrictUtf8.Encoding.GetString(bytes, p, len));
                    p += len;
                }
                if (p != entriesEnd)
                {
                    throw new InvalidDataException("symbol dictionary chunk has trailing entry bytes");
                }
            }
            catch (Exception ex) when (ex is InvalidDataException or System.Text.DecoderFallbackException)
            {
                entries.RemoveRange(before, entries.Count - before);
                p = chunkStart;
                break;
            }

            p = entriesEnd + CrcSize;
            validEnd = p;
        }

        var stream = OpenFile(filePath, FileMode.Open);
        try
        {
            if (validEnd < stream.Length)
            {
                stream.SetLength(validEnd);
                stream.Flush(flushToDisk: false);
            }
            stream.Position = validEnd;
            return (stream, entries, true);
        }
        catch
        {
            SfCleanup.Dispose(stream);
            throw;
        }
    }

    private static void FoldRing(QwpSegmentRing ring, List<string> entries)
    {
        // A frame can never be larger than the segment that contains its envelope. The ring's
        // default protocol ceiling is int.MaxValue, so allocating MaxFrameLength directly would
        // attempt a 2 GiB array even for a tiny on-disk segment during orphan recovery.
        var frameCapacity = checked((int)Math.Min(ring.SegmentCapacity, ring.MaxFrameLength));
        var frame = new byte[frameCapacity];
        for (var fsn = ring.OldestFsn; fsn < ring.NextFsn; fsn++)
        {
            var length = ring.TryReadFrame(fsn, frame);
            if (length < 0)
            {
                throw new InvalidDataException($"cannot recover QWP frame FSN {fsn} while rebuilding symbol dictionary");
            }
            FoldFrame(frame.AsSpan(0, length), entries, fsn);
        }
    }

    private static void FoldFrame(ReadOnlySpan<byte> frame, List<string> entries, long fsn)
    {
        if (frame.Length < QwpConstants.HeaderSize
            || BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(QwpConstants.OffsetMagic, 4)) != QwpConstants.Magic)
        {
            throw new InvalidDataException($"invalid QWP frame header at FSN {fsn}");
        }
        if (frame[QwpConstants.OffsetVersion] != QwpConstants.SupportedVersion)
        {
            throw new InvalidDataException(
                $"unsupported QWP frame version {frame[QwpConstants.OffsetVersion]} at FSN {fsn}");
        }
        if ((frame[QwpConstants.OffsetFlags] & QwpConstants.FlagDeltaSymbolDict) == 0)
        {
            return;
        }

        var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(
            frame.Slice(QwpConstants.OffsetPayloadLength, 4));
        if (payloadLength > (uint)(frame.Length - QwpConstants.HeaderSize))
        {
            throw new InvalidDataException($"truncated QWP frame payload at FSN {fsn}");
        }
        var limit = checked(QwpConstants.HeaderSize + (int)payloadLength);
        var p = QwpConstants.HeaderSize;
        var start = ReadFrameVarint(frame, ref p, limit, fsn, "symbol delta start");
        var count = ReadFrameVarint(frame, ref p, limit, fsn, "symbol delta count");
        if (start > int.MaxValue || count > int.MaxValue
            || start + count > QwpConstants.MaxSymbolDictionarySize)
        {
            throw new InvalidDataException($"symbol dictionary range out of bounds at FSN {fsn}");
        }
        if ((int)start > entries.Count)
        {
            throw new InvalidDataException(
                $"unreplayable symbol dictionary gap at FSN {fsn}: delta starts at {start}, recovered {entries.Count}");
        }

        for (var i = 0; i < (int)count; i++)
        {
            var len = ReadFrameVarint(frame, ref p, limit, fsn, "symbol length");
            if (len > int.MaxValue || len > (ulong)(limit - p))
            {
                throw new InvalidDataException($"malformed symbol entry at FSN {fsn}");
            }
            string value;
            try
            {
                value = QwpStrictUtf8.Encoding.GetString(frame.Slice(p, (int)len));
            }
            catch (System.Text.DecoderFallbackException ex)
            {
                throw new InvalidDataException($"invalid UTF-8 symbol entry at FSN {fsn}", ex);
            }
            p += (int)len;

            var id = (int)start + i;
            if (id < entries.Count)
            {
                if (!string.Equals(entries[id], value, StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        $"symbol dictionary mismatch at FSN {fsn}, id {id}: persisted and frame values differ");
                }
            }
            else
            {
                entries.Add(value);
            }
        }
    }

    private static ulong ReadFrameVarint(
        ReadOnlySpan<byte> frame,
        ref int p,
        int limit,
        long fsn,
        string field)
    {
        if (p >= limit)
        {
            throw new InvalidDataException($"truncated {field} at FSN {fsn}");
        }
        try
        {
            var value = QwpVarint.Read(frame.Slice(p, limit - p), out var read);
            p += read;
            return value;
        }
        catch (Exception ex)
        {
            throw new InvalidDataException($"malformed {field} at FSN {fsn}", ex);
        }
    }

    private void AppendRange(IReadOnlyList<string> entries, int from, int to)
    {
        lock (_lock)
        {
            EnsureOpen();
            AppendRangeLocked(entries, from, to);
        }
    }

    private void AppendRangeLocked(IReadOnlyList<string> entries, int from, int to)
    {
        if (to <= from)
        {
            return;
        }
        var stream = _stream!;
        var rollbackOffset = stream.Position;
        try
        {
            WriteChunk(stream, entries, from, to);
            stream.Flush(flushToDisk: false);
        }
        catch
        {
            try
            {
                stream.SetLength(rollbackOffset);
                stream.Position = rollbackOffset;
            }
            catch (Exception truncateError)
            {
                throw new InvalidDataException(
                    $"failed to roll back partial symbol dictionary append at {_filePath}", truncateError);
            }
            throw;
        }
    }

    private static void WriteHeader(FileStream stream)
    {
        Span<byte> header = stackalloc byte[HeaderSize];
        header.Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(header, FileMagic);
        header[4] = Version;
        stream.Write(header);
    }

    private static void WriteChunk(
        FileStream stream,
        IReadOnlyList<string> entries,
        int from,
        int to)
    {
        var encoded = new QwpEncoder.FrameBuilder(4096);
        Span<byte> varint = stackalloc byte[QwpVarint.MaxBytes];
        for (var i = from; i < to; i++)
        {
            var value = entries[i];
            var len = QwpStrictUtf8.Encoding.GetByteCount(value);
            var n = QwpVarint.Write(varint, (ulong)len);
            encoded.WriteBytes(varint.Slice(0, n));
            var destination = encoded.Allocate(len);
            QwpStrictUtf8.Encoding.GetBytes(value.AsSpan(), destination);
        }

        var body = new QwpEncoder.FrameBuilder(encoded.Length + 16);
        body.WriteVarint((ulong)(to - from));
        body.WriteVarint((ulong)encoded.Length);
        body.WriteBytes(encoded.AsSpan(0, encoded.Length));

        stream.Write(body.AsSpan(0, body.Length));
        Span<byte> crc = stackalloc byte[CrcSize];
        BinaryPrimitives.WriteUInt32LittleEndian(crc, QwpCrc32C.Compute(body.AsSpan(0, body.Length)));
        stream.Write(crc);
    }

    private static bool TryReadIntVarint(byte[] bytes, ref int p, int limit, out int value)
    {
        ulong result = 0;
        var shift = 0;
        for (var i = 0; i < 5 && p < limit; i++)
        {
            var b = bytes[p++];
            result |= (ulong)(b & 0x7f) << shift;
            if ((b & 0x80) == 0)
            {
                if (result > int.MaxValue)
                {
                    value = 0;
                    return false;
                }
                value = (int)result;
                return true;
            }
            shift += 7;
        }
        value = 0;
        return false;
    }

    private static FileStream OpenFile(string path, FileMode mode) =>
        new(path, mode, FileAccess.ReadWrite, FileShare.Read, bufferSize: 1, FileOptions.SequentialScan);

    private void EnsureOpen()
    {
        if (_stream is null)
        {
            throw new ObjectDisposedException(nameof(QwpPersistedSymbolDictionary));
        }
    }
}
