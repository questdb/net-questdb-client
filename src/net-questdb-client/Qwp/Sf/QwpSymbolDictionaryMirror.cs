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
///     I/O-thread-owned mirror of every symbol dictionary entry carried by frames sent during this
///     engine's lifetime. A fresh WebSocket connection starts with an empty server dictionary, so
///     the mirror is replayed in table-less catch-up frames before unacked data frames are resent.
/// </summary>
internal sealed class QwpSymbolDictionaryMirror
{
    private const int InitialCapacity = 4096;
    private const int UnadvertisedPackingLimit = 64 * 1024;

    private readonly List<int> _entryEnds = new();
    private readonly QwpEncoder.FrameBuilder _catchUpFrame = new(InitialCapacity);
    private byte[] _encodedEntries = new byte[InitialCapacity];
    private int _encodedLength;

    public int Count => _entryEnds.Count;

    /// <summary>Seeds the mirror from a CRC-validated persisted dictionary before the loop starts.</summary>
    public void Seed(IEnumerable<string> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (Count != 0)
        {
            throw new InvalidOperationException("symbol dictionary mirror is already seeded");
        }

        Span<byte> varint = stackalloc byte[QwpVarint.MaxBytes];
        foreach (var value in entries)
        {
            if (Count >= QwpConstants.MaxSymbolDictionarySize)
            {
                throw new InvalidDataException(
                    $"persisted symbol dictionary exceeds {QwpConstants.MaxSymbolDictionarySize} entries");
            }
            var byteCount = QwpStrictUtf8.Encoding.GetByteCount(value);
            var prefixLength = QwpVarint.Write(varint, (ulong)byteCount);
            EnsureCapacity(checked(_encodedLength + prefixLength + byteCount));
            varint.Slice(0, prefixLength).CopyTo(_encodedEntries.AsSpan(_encodedLength));
            _encodedLength += prefixLength;
            QwpStrictUtf8.Encoding.GetBytes(value.AsSpan(), _encodedEntries.AsSpan(_encodedLength, byteCount));
            _encodedLength += byteCount;
            _entryEnds.Add(_encodedLength);
        }
    }

    /// <summary>
    ///     Validates that a frame's symbol delta does not skip ids the mirror has never observed.
    ///     Frames may overlap an already mirrored prefix: QWP deliberately permits that for replay.
    /// </summary>
    public void ValidateContinuity(ReadOnlySpan<byte> frame)
    {
        var delta = ParseDelta(frame);
        if (!delta.HasDelta || delta.Start <= Count)
        {
            return;
        }

        throw new InvalidDataException(
            $"QWP symbol dictionary gap: frame delta starts at {delta.Start}, mirror contains {Count} entries");
    }

    /// <summary>
    ///     Extends the mirror with the previously unseen suffix of a successfully sent frame.
    ///     Replayed/self-overlapping deltas are idempotent.
    /// </summary>
    public void Accumulate(ReadOnlySpan<byte> frame)
    {
        var delta = ParseDelta(frame);
        if (!delta.HasDelta || delta.Count == 0)
        {
            return;
        }
        if (delta.Start > Count)
        {
            throw new InvalidDataException(
                $"QWP symbol dictionary gap: frame delta starts at {delta.Start}, mirror contains {Count} entries");
        }

        var deltaEnd = checked(delta.Start + delta.Count);
        if (deltaEnd <= Count)
        {
            return;
        }

        var firstNewId = Count;
        var skip = firstNewId - delta.Start;
        var p = delta.EntriesStart;
        for (var i = 0; i < skip; i++)
        {
            p = SkipEntry(frame, p, delta.EntriesEnd);
        }

        // ParseDelta already walked every entry, so the tail provably ends at EntriesEnd and the
        // SkipEntry calls below cannot throw. Reserving both capacities before the first mutation
        // keeps the mirror consistent on any failure path.
        var tailStart = p;
        var tailLength = delta.EntriesEnd - tailStart;
        EnsureCapacity(checked(_encodedLength + tailLength));
        _entryEnds.EnsureCapacity(_entryEnds.Count + (deltaEnd - firstNewId));
        frame.Slice(tailStart, tailLength).CopyTo(_encodedEntries.AsSpan(_encodedLength));
        for (var id = firstNewId; id < deltaEnd; id++)
        {
            p = SkipEntry(frame, p, delta.EntriesEnd);
            _entryEnds.Add(_encodedLength + (p - tailStart));
        }
        _encodedLength += tailLength;
    }

    /// <summary>
    ///     Re-registers the mirrored dictionary on a fresh connection. The dictionary is split only
    ///     between entries so every catch-up frame stays within the negotiated server batch cap.
    /// </summary>
    /// <returns>Number of wire sequences consumed by catch-up frames.</returns>
    public async Task<int> SendCatchUpAsync(IQwpCursorTransport transport, CancellationToken ct)
    {
        if (Count == 0)
        {
            return 0;
        }

        var advertisedCap = transport.NegotiatedMaxBatchSize;
        var packingLimit = advertisedCap > 0 ? advertisedCap : UnadvertisedPackingLimit;
        // Without an advertised cap, split ordinary dictionaries conservatively but never invent a
        // new terminal for one indivisible symbol that already fitted in its original data frame.
        var soloFrameLimit = advertisedCap > 0 ? advertisedCap : QwpConstants.MaxBatchBytes;
        var framesSent = 0;
        var startId = 0;

        while (startId < Count)
        {
            var startOffset = EntryStart(startId);
            var count = 0;
            var endOffset = startOffset;

            while (startId + count < Count)
            {
                var candidateCount = count + 1;
                var candidateEnd = _entryEnds[startId + count];
                var candidateLength = checked(
                    QwpConstants.HeaderSize
                    + QwpVarint.GetByteCount((ulong)startId)
                    + QwpVarint.GetByteCount((ulong)candidateCount)
                    + candidateEnd - startOffset);
                if (candidateLength > packingLimit)
                {
                    if (count == 0)
                    {
                        if (candidateLength > soloFrameLimit)
                        {
                            // A smaller-cap failover node may be temporary. Model this cap gap as a
                            // transport outage so the foreground sender keeps retrying until a
                            // compatible node returns.
                            throw new QwpCatchUpCapGapException(
                                $"symbol dictionary entry {startId} needs a {candidateLength}-byte catch-up frame, " +
                                $"exceeding server batch cap {soloFrameLimit}; retrying on reconnect");
                        }

                        // No cap was advertised and this entry is wider than the conservative
                        // multi-entry packing target. It cannot be split, so send it alone.
                        count = candidateCount;
                        endOffset = candidateEnd;
                    }
                    break;
                }

                count = candidateCount;
                endOffset = candidateEnd;
            }

            BuildCatchUpFrame(startId, count, startOffset, endOffset);
            await transport.SendBinaryAsync(_catchUpFrame.WrittenMemory, ct).ConfigureAwait(false);
            framesSent++;
            startId += count;
        }

        return framesSent;
    }

    private void BuildCatchUpFrame(int startId, int count, int startOffset, int endOffset)
    {
        _catchUpFrame.Reset();
        _catchUpFrame.Allocate(QwpConstants.HeaderSize);
        _catchUpFrame.WriteVarint((ulong)startId);
        _catchUpFrame.WriteVarint((ulong)count);
        _catchUpFrame.WriteBytes(_encodedEntries.AsSpan(startOffset, endOffset - startOffset));

        var payloadLength = _catchUpFrame.Length - QwpConstants.HeaderSize;
        var header = _catchUpFrame.AsSpan(0, QwpConstants.HeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(QwpConstants.OffsetMagic, 4), QwpConstants.Magic);
        header[QwpConstants.OffsetVersion] = QwpConstants.SupportedVersion;
        // Catch-up registers connection state but carries no rows. Deferring its empty commit keeps
        // this safe even if catch-up is ever reused while a deferred transaction is in progress.
        header[QwpConstants.OffsetFlags] = (byte)(QwpConstants.FlagDeltaSymbolDict
                                                   | QwpConstants.FlagGorilla
                                                   | QwpConstants.FlagDeferCommit);
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(QwpConstants.OffsetTableCount, 2), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(
            header.Slice(QwpConstants.OffsetPayloadLength, 4), (uint)payloadLength);
    }

    private int EntryStart(int id) => id == 0 ? 0 : _entryEnds[id - 1];

    private void EnsureCapacity(int required)
    {
        if (_encodedEntries.Length >= required)
        {
            return;
        }

        var capacity = _encodedEntries.Length;
        while (capacity < required)
        {
            var doubled = (long)capacity * 2;
            capacity = doubled > Array.MaxLength ? required : (int)doubled;
        }
        Array.Resize(ref _encodedEntries, capacity);
    }

    private static ParsedDelta ParseDelta(ReadOnlySpan<byte> frame)
    {
        if (frame.Length < QwpConstants.HeaderSize
            || BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(QwpConstants.OffsetMagic, 4)) != QwpConstants.Magic
            || (frame[QwpConstants.OffsetFlags] & QwpConstants.FlagDeltaSymbolDict) == 0)
        {
            return default;
        }
        if (frame[QwpConstants.OffsetVersion] != QwpConstants.SupportedVersion)
        {
            throw new InvalidDataException(
                $"unsupported QWP frame version {frame[QwpConstants.OffsetVersion]}");
        }

        var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(
            frame.Slice(QwpConstants.OffsetPayloadLength, 4));
        if (payloadLength > (uint)(frame.Length - QwpConstants.HeaderSize))
        {
            throw new InvalidDataException(
                $"QWP frame payload length {payloadLength} exceeds available bytes {frame.Length - QwpConstants.HeaderSize}");
        }

        var limit = checked(QwpConstants.HeaderSize + (int)payloadLength);
        var p = QwpConstants.HeaderSize;
        var startRaw = ReadVarint(frame, ref p, limit, "symbol delta start");
        var countRaw = ReadVarint(frame, ref p, limit, "symbol delta count");
        if (startRaw > QwpConstants.MaxSymbolDictionarySize
            || countRaw > QwpConstants.MaxSymbolDictionarySize
            || startRaw + countRaw > QwpConstants.MaxSymbolDictionarySize)
        {
            throw new InvalidDataException(
                $"QWP symbol delta range exceeds the {QwpConstants.MaxSymbolDictionarySize}-entry limit: " +
                $"start={startRaw}, count={countRaw}");
        }

        var entriesStart = p;
        for (ulong i = 0; i < countRaw; i++)
        {
            p = SkipEntry(frame, p, limit);
        }

        return new ParsedDelta(true, (int)startRaw, (int)countRaw, entriesStart, p);
    }

    private static int SkipEntry(ReadOnlySpan<byte> frame, int p, int limit)
    {
        var len = ReadVarint(frame, ref p, limit, "symbol length");
        if (len > int.MaxValue || len > (ulong)(limit - p))
        {
            throw new InvalidDataException($"QWP symbol entry length {len} exceeds remaining payload {limit - p}");
        }
        return checked(p + (int)len);
    }

    private static ulong ReadVarint(ReadOnlySpan<byte> frame, ref int p, int limit, string field)
    {
        if (p >= limit)
        {
            throw new InvalidDataException($"QWP {field} is truncated");
        }

        try
        {
            var value = QwpVarint.Read(frame.Slice(p, limit - p), out var read);
            p += read;
            return value;
        }
        catch (Exception ex) when (ex is not InvalidDataException)
        {
            throw new InvalidDataException($"QWP {field} is malformed", ex);
        }
    }

    private readonly record struct ParsedDelta(
        bool HasDelta,
        int Start,
        int Count,
        int EntriesStart,
        int EntriesEnd);
}

/// <summary>
///     Marks the heterogeneous-cluster case where an entry accepted by one node cannot fit in a
///     reconnect catch-up under another node's smaller advertised batch cap.
/// </summary>
internal sealed class QwpCatchUpCapGapException : IngressError
{
    public QwpCatchUpCapGapException(string message)
        : base(QuestDB.Enums.ErrorCode.SocketError, message)
    {
    }
}
