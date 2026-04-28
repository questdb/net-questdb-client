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
 ******************************************************************************/

using System.Buffers.Binary;
using System.Text;

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Decoder for inbound QWP egress RESULT_BATCH frames. The .NET counterpart of
///     Java's <c>QwpResultBatchDecoder</c> on java-questdb-client main 64b7ee69.
///     Parses the payload in-place — values stay in the supplied
///     <see cref="QwpBatchBuffer"/>'s POH-pinned scratch and the produced
///     <see cref="QwpColumnBatch"/> references it by offset.
/// </summary>
/// <remarks>
///     Experimental. PR 11a covers the foundation: header validation, SCHEMA_MODE_FULL,
///     null section, and the fixed-width / DECIMAL / VARCHAR / BINARY / GEOHASH
///     wire types. Compression (FLAG_ZSTD), Gorilla (FLAG_GORILLA), the delta SYMBOL
///     dictionary, SCHEMA_MODE_REFERENCE, SYMBOL columns, and ARRAY columns are
///     deferred to a follow-up sub-PR — this decoder rejects them with a clear
///     <see cref="QwpDecodeException"/> rather than producing partial output.
/// </remarks>
internal sealed class QwpResultBatchDecoder
{
    /// <summary>Hard cap on row_count per batch — matches the server-side limit.</summary>
    private const int MAX_ROWS_PER_BATCH = 1_048_576;

    /// <summary>
    ///     Hard cap on the connection-scoped SYMBOL dict's entry count. Mirrors Java's
    ///     <c>MAX_CONN_DICT_SIZE</c> guard. A hostile server that never emits CACHE_RESET
    ///     can't drive _connDictSize past this without being rejected first.
    /// </summary>
    private const int MAX_CONN_DICT_SIZE = 8_388_608;

    /// <summary>
    ///     Hard cap on the connection-scoped SYMBOL dict's UTF-8 heap. 256 MiB matches
    ///     Java's MAX_CONN_DICT_HEAP_BYTES. Servers approaching this cap are expected to
    ///     emit CACHE_RESET.
    /// </summary>
    private const int MAX_CONN_DICT_HEAP_BYTES = 256 * 1024 * 1024;

    // §3.2c — connection-scoped SYMBOL dict state. Reused across batches on the same
    // decoder instance; FLAG_DELTA_SYMBOL_DICT messages append new entries; CACHE_RESET
    // (§3.3c) wipes them. SYMBOL columns in delta mode reference these buffers via
    // QwpColumnLayout.SymbolHeapBuffer / SymbolEntriesBuffer.
    private byte[] _connDictHeapBytes = Array.Empty<byte>();
    private int _connDictHeapLength;
    private byte[] _connDictEntries = Array.Empty<byte>(); // packed (offset:i32 | length:i32<<32)
    private int _connDictSize;
    // True for the duration of a Decode() call when the message carries
    // FLAG_DELTA_SYMBOL_DICT — read by ParseSymbolColumn to choose the delta path.
    private bool _deltaMode;

    // §3.2b — true when the message carries FLAG_GORILLA. TIMESTAMP / TIMESTAMP_NANOS /
    // DATE columns then prefix their data with a 1-byte encoding discriminator
    // (0x00 raw / 0x01 Gorilla DoD bitstream).
    private bool _gorillaMode;
    private readonly QwpGorillaDecoder _gorillaDecoder = new();

    /// <summary>
    ///     Decodes the RESULT_BATCH frame whose payload has been copied into
    ///     <paramref name="buffer"/>. Populates <c>buffer.Batch</c> and <c>buffer.LayoutPool</c>.
    /// </summary>
    public void Decode(QwpBatchBuffer buffer)
    {
        var payload = buffer.Payload;
        if (payload.Length < QwpConstants.HEADER_SIZE + 10)
        {
            throw new QwpDecodeException($"RESULT_BATCH payload too short: {payload.Length}");
        }

        // QWP message header: magic(4) + version(1) + flags(1) + ... — full layout pinned in QwpConstants.
        var magic = BinaryPrimitives.ReadInt32LittleEndian(payload[..4]);
        if (magic != QwpConstants.MAGIC_MESSAGE)
        {
            throw new QwpDecodeException($"bad magic 0x{magic:x8}");
        }
        var version = payload[4];
        if (version < QwpConstants.VERSION_1 || version > QwpConstants.MAX_SUPPORTED_VERSION)
        {
            throw new QwpDecodeException($"unsupported version {version}");
        }
        var flags = payload[QwpConstants.HEADER_OFFSET_FLAGS];
        if ((flags & QwpConstants.FLAG_ZSTD) != 0)
        {
            throw new QwpDecodeException("FLAG_ZSTD not yet supported by this decoder");
        }
        _gorillaMode = (flags & QwpConstants.FLAG_GORILLA) != 0;
        _deltaMode = (flags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0;

        var p = QwpConstants.HEADER_SIZE;

        var msgKind = payload[p++];
        if (msgKind != QwpEgressMsgKind.RESULT_BATCH)
        {
            throw new QwpDecodeException(
                $"expected RESULT_BATCH (0x{QwpEgressMsgKind.RESULT_BATCH:x2}), got 0x{msgKind:x2}");
        }

        if (p + 8 > payload.Length) throw new QwpDecodeException("truncated request_id");
        // request_id is read but not surfaced on the .NET batch view yet (the IO thread
        // owns the in-flight map; PR 11b will plumb it through).
        BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(p, 8));
        p += 8;
        // batchSeq follows; same — not surfaced yet, owned by IO thread.
        ReadVarint(payload, ref p, out _);

        // §3.2c — delta section sits between the request_id/batchSeq prelude and the
        // table block. New SYMBOL dict entries since the last batch are appended to the
        // connection-scoped dict here so the per-column delta-mode SYMBOL parse below
        // can reference dict ids straight against the updated dict.
        if (_deltaMode)
        {
            p = ParseDeltaSymbolDict(payload, p);
        }

        // Table block: name, row_count, column_count, schema, columns.
        ReadVarint(payload, ref p, out var nameLen);
        if (nameLen < 0 || nameLen > QwpConstants.MAX_TABLE_NAME_LENGTH)
        {
            throw new QwpDecodeException($"table name length out of range: {nameLen}");
        }
        if (p + nameLen > payload.Length) throw new QwpDecodeException("truncated table name");
        p += (int)nameLen;

        ReadVarint(payload, ref p, out var rowCountVar);
        if (rowCountVar < 0 || rowCountVar > MAX_ROWS_PER_BATCH)
        {
            throw new QwpDecodeException($"row_count out of range: {rowCountVar}");
        }
        var rowCount = (int)rowCountVar;

        ReadVarint(payload, ref p, out var columnCountVar);
        if (columnCountVar < 0 || columnCountVar > QwpConstants.MAX_COLUMNS_PER_TABLE)
        {
            throw new QwpDecodeException($"column_count out of range: {columnCountVar}");
        }
        var columnCount = (int)columnCountVar;

        // Schema mode + schema_id.
        if (p >= payload.Length) throw new QwpDecodeException("truncated schema mode");
        var schemaMode = payload[p++];
        ReadVarint(payload, ref p, out _); // schema_id, ignored until SCHEMA_MODE_REFERENCE lands

        if (schemaMode == QwpConstants.SCHEMA_MODE_REFERENCE)
        {
            throw new QwpDecodeException("SCHEMA_MODE_REFERENCE not yet supported by this decoder");
        }
        if (schemaMode != QwpConstants.SCHEMA_MODE_FULL)
        {
            throw new QwpDecodeException($"unknown schema mode 0x{schemaMode:x2}");
        }

        // Reset the layout pool size and seed the column infos.
        var pool = buffer.LayoutPool;
        while (pool.Count < columnCount) pool.Add(new QwpColumnLayout());

        for (var i = 0; i < columnCount; i++)
        {
            var layout = pool[i];
            layout.Clear();
            ReadVarint(payload, ref p, out var colNameLen);
            if (colNameLen < 0 || colNameLen > QwpConstants.MAX_COLUMN_NAME_LENGTH)
            {
                throw new QwpDecodeException($"column name length out of range: {colNameLen}");
            }
            if (p + colNameLen + 1 > payload.Length)
            {
                throw new QwpDecodeException("truncated column def");
            }
            var colName = colNameLen == 0
                ? string.Empty
                : Encoding.UTF8.GetString(payload.Slice(p, (int)colNameLen));
            p += (int)colNameLen;
            var wireType = payload[p++];
            layout.Info = new QwpEgressColumnInfo { Name = colName, WireType = wireType };
        }

        // Reset the user-facing batch view BEFORE column parsing so layouts the parser
        // populates land on the same instance the user reads.
        var batch = buffer.Batch;
        batch.Reset(buffer, rowCount);
        batch.Layouts.Clear();
        for (var i = 0; i < columnCount; i++)
        {
            batch.Layouts.Add(pool[i]);
        }

        // Per-column wire bytes: null section then values.
        for (var ci = 0; ci < columnCount; ci++)
        {
            p = ParseColumn(pool[ci], payload, rowCount, p);
        }
    }

    /// <summary>Reads a 7-bit unsigned LEB128 varint at <paramref name="p"/> and advances it.</summary>
    private static void ReadVarint(ReadOnlySpan<byte> payload, ref int p, out long value)
    {
        long v = 0;
        var shift = 0;
        while (true)
        {
            if (p >= payload.Length) throw new QwpDecodeException("truncated varint");
            var b = payload[p++];
            // Byte 10: only bit 0 of the data nibble fits without overflowing bit 63.
            if (shift == 63 && (b & 0x7E) != 0)
            {
                throw new QwpDecodeException("varint overflow");
            }
            v |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw new QwpDecodeException("varint overflow");
        }
        value = v;
    }

    private int ParseColumn(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int rowCount, int p)
    {
        p = ParseNullSection(layout, payload, rowCount, p);
        var wt = layout.Info!.WireType;
        switch (wt)
        {
            case QwpConstants.TYPE_BOOLEAN:
            {
                layout.ValuesOffset = p;
                var bytes = (layout.NonNullCount + 7) >> 3;
                if (p + bytes > payload.Length) throw new QwpDecodeException("truncated BOOLEAN");
                return p + bytes;
            }
            case QwpConstants.TYPE_BYTE:
                return AdvanceFixed(layout, payload, p, sizeBytes: 1);
            case QwpConstants.TYPE_SHORT:
            case QwpConstants.TYPE_CHAR:
                return AdvanceFixed(layout, payload, p, sizeBytes: 2);
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_FLOAT:
            case QwpConstants.TYPE_IPv4:
                return AdvanceFixed(layout, payload, p, sizeBytes: 4);
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_DOUBLE:
                return AdvanceFixed(layout, payload, p, sizeBytes: 8);
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
                // §3.2b — TIMESTAMP/TIMESTAMP_NANOS/DATE columns are Gorilla-eligible
                // when the message has FLAG_GORILLA set; raw-only otherwise.
                return ParseTimestampColumn(layout, payload, p);
            case QwpConstants.TYPE_UUID:
                return AdvanceFixed(layout, payload, p, sizeBytes: 16);
            case QwpConstants.TYPE_LONG256:
                return AdvanceFixed(layout, payload, p, sizeBytes: 32);
            case QwpConstants.TYPE_DECIMAL64:
                return ParseDecimalColumn(layout, payload, p, sizeBytes: 8);
            case QwpConstants.TYPE_DECIMAL128:
                return ParseDecimalColumn(layout, payload, p, sizeBytes: 16);
            case QwpConstants.TYPE_DECIMAL256:
                return ParseDecimalColumn(layout, payload, p, sizeBytes: 32);
            case QwpConstants.TYPE_VARCHAR:
            case QwpConstants.TYPE_BINARY:
                return ParseStringOrBinaryColumn(layout, payload, p);
            case QwpConstants.TYPE_GEOHASH:
                return ParseGeohashColumn(layout, payload, p);
            case QwpConstants.TYPE_SYMBOL:
                return ParseSymbolColumn(layout, payload, rowCount, p);
            case QwpConstants.TYPE_DOUBLE_ARRAY:
            case QwpConstants.TYPE_LONG_ARRAY:
                return ParseArrayColumn(layout, payload, rowCount, p);
            default:
                throw new QwpDecodeException($"unsupported wire type 0x{wt:x2}");
        }
    }

    private static int AdvanceFixed(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int p, int sizeBytes)
    {
        layout.ValuesOffset = p;
        var total = (long)sizeBytes * layout.NonNullCount;
        if (p + total > payload.Length) throw new QwpDecodeException("truncated fixed-width column");
        return (int)(p + total);
    }

    private static int ParseDecimalColumn(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int p, int sizeBytes)
    {
        if (p >= payload.Length) throw new QwpDecodeException("truncated DECIMAL scale byte");
        layout.Info!.DecimalScale = (sbyte)payload[p++];
        return AdvanceFixed(layout, payload, p, sizeBytes);
    }

    private static int ParseStringOrBinaryColumn(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int p)
    {
        var nonNull = layout.NonNullCount;
        var offsetsSize = 4L * (nonNull + 1);
        if (p + offsetsSize > payload.Length) throw new QwpDecodeException("truncated string offsets");
        layout.ValuesOffset = p;
        layout.StringBytesOffset = (int)(p + offsetsSize);
        var totalBytes = nonNull == 0
            ? 0
            : BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p + 4 * nonNull, 4));
        if (totalBytes < 0 || layout.StringBytesOffset + totalBytes > payload.Length)
        {
            throw new QwpDecodeException($"invalid string column total bytes: {totalBytes}");
        }
        // Validate per-row offsets: each must lie in [0, totalBytes] and be monotonically
        // non-decreasing. Catches a hostile server that ships an out-of-order or negative
        // intermediate offset which would otherwise turn into a backwards or negative-length
        // read in QwpColumnBatch.GetString / GetBinary.
        var prev = 0;
        for (var i = 0; i < nonNull; i++)
        {
            var off = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p + 4 * i, 4));
            if (off < prev || off > totalBytes)
            {
                throw new QwpDecodeException(
                    $"invalid string column offset[{i}]={off} (prev={prev}, total={totalBytes})");
            }
            prev = off;
        }
        return layout.StringBytesOffset + totalBytes;
    }

    private static int ParseGeohashColumn(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int p)
    {
        ReadVarint(payload, ref p, out var precision);
        if (precision < 1 || precision > 60)
        {
            throw new QwpDecodeException($"GEOHASH precision bits out of range (1..60): {precision}");
        }
        layout.Info!.GeohashPrecisionBits = (int)precision;
        var bytesPerValue = ((int)precision + 7) >> 3;
        return AdvanceFixed(layout, payload, p, bytesPerValue);
    }

    /// <summary>
    ///     §3.2f — TYPE_DOUBLE_ARRAY / TYPE_LONG_ARRAY column parser. Per non-null row:
    ///     <list type="number">
    ///         <item><c>n_dims:u8</c> in [1, <see cref="QuestDB.Utils.SenderOptions.ARRAY_MAX_DIMENSIONS"/>]</item>
    ///         <item><c>dim_size_0:i32 .. dim_size_{n_dims-1}:i32</c> — each ≥1</item>
    ///         <item>8-byte elements (double or long; same wire size either way) —
    ///             count = <c>product(dim_sizes)</c></item>
    ///     </list>
    ///     ArrayRowOffsets / ArrayRowLengths on the layout track per-row payload byte
    ///     ranges (offsets into <see cref="QwpBatchBuffer.Payload"/>) so ColumnView
    ///     accessors (§3.1) can read values + dims/shape per row in O(1).
    ///     Hostile-server guards: rank out of range, dim ≤ 0, element-count overflow,
    ///     truncated payload.
    /// </summary>
    private static int ParseArrayColumn(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int rowCount, int p)
    {
        layout.ValuesOffset = p;

        // Per-row offset / length tables. Sized to the row count so consumer-side
        // dense-index lookup is O(1).
        if (layout.ArrayRowOffsets is null || layout.ArrayRowOffsets.Length < rowCount)
        {
            layout.ArrayRowOffsets = new int[Math.Max(rowCount, 16)];
        }
        if (layout.ArrayRowLengths is null || layout.ArrayRowLengths.Length < rowCount)
        {
            layout.ArrayRowLengths = new int[Math.Max(rowCount, 16)];
        }

        const long maxArrayElements = (int.MaxValue - 1024) / 8L;
        var noNulls = layout.NullBitmapOffset < 0;
        var nonNullIdx = layout.NonNullIdx;

        for (var i = 0; i < rowCount; i++)
        {
            if (!noNulls && nonNullIdx![i] < 0)
            {
                layout.ArrayRowOffsets[i] = 0;
                layout.ArrayRowLengths[i] = 0;
                continue;
            }
            if (p + 1 > payload.Length) throw new QwpDecodeException("truncated ARRAY header");
            int nDims = payload[p];
            if (nDims < 1 || nDims > QuestDB.Utils.SenderOptions.ARRAY_MAX_DIMENSIONS)
            {
                throw new QwpDecodeException(
                    $"invalid array dimensions: {nDims} (must be 1-{QuestDB.Utils.SenderOptions.ARRAY_MAX_DIMENSIONS})");
            }
            var headerEnd = p + 1 + 4 * nDims;
            if (headerEnd > payload.Length) throw new QwpDecodeException("truncated ARRAY dims");
            long elements = 1;
            for (var d = 0; d < nDims; d++)
            {
                var dl = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p + 1 + 4 * d, 4));
                if (dl < 1)
                {
                    throw new QwpDecodeException($"ARRAY dim {d} must be >= 1: {dl}");
                }
                elements *= dl;
                if (elements > maxArrayElements)
                {
                    throw new QwpDecodeException(
                        $"ARRAY element count exceeds limit ({elements} > {maxArrayElements})");
                }
            }
            var rowEnd = headerEnd + 8 * elements;
            if (rowEnd > payload.Length) throw new QwpDecodeException("truncated ARRAY payload");
            layout.ArrayRowOffsets[i] = p;
            layout.ArrayRowLengths[i] = (int)(rowEnd - p);
            p = (int)rowEnd;
        }
        return p;
    }

    /// <summary>
    ///     §3.2b — TIMESTAMP / TIMESTAMP_NANOS / DATE column parser. With FLAG_GORILLA
    ///     unset, the column is plain 8-byte fixed-width. With FLAG_GORILLA set, the
    ///     column prefixes with a 1-byte encoding discriminator:
    ///     <list type="bullet">
    ///         <item><c>0x00</c> — raw 8-byte values inline (encoder fell back when row
    ///             count &lt; 3 or compression worsened the stream).</item>
    ///         <item><c>0x01</c> — Gorilla DoD bitstream. First two timestamps land
    ///             uncompressed (16 bytes); the bit-packed delta-of-deltas follow.
    ///             We decode into a per-column managed buffer (TimestampDecodeBuffer)
    ///             and point ValuesOffset at it via the negative-offset sentinel
    ///             pattern so ColumnView can read the long values uniformly.</item>
    ///     </list>
    /// </summary>
    private int ParseTimestampColumn(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int p)
    {
        var nonNull = layout.NonNullCount;
        if (!_gorillaMode)
        {
            return AdvanceFixed(layout, payload, p, sizeBytes: 8);
        }
        if (p >= payload.Length)
        {
            throw new QwpDecodeException("truncated TIMESTAMP encoding byte");
        }
        var encoding = payload[p++];
        layout.Info!.TimestampEncoding = encoding;
        if (encoding == 0x00)
        {
            // Raw inline.
            return AdvanceFixed(layout, payload, p, sizeBytes: 8);
        }
        if (encoding != 0x01)
        {
            throw new QwpDecodeException(
                $"unknown TIMESTAMP encoding 0x{encoding:x2}");
        }
        // Gorilla. Encoder shortcuts nonNull < 3 to the 0x00 branch, so by the time
        // we see 0x01 there are at least 3 values: two raw + ≥1 in the bitstream.
        if (nonNull < 3)
        {
            throw new QwpDecodeException(
                $"Gorilla-encoded column with nonNull<3: {nonNull}");
        }
        if (p + 16 > payload.Length)
        {
            throw new QwpDecodeException("truncated Gorilla prefix");
        }
        var firstTs = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(p, 8));
        var secondTs = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(p + 8, 8));
        var bitstreamStart = p + 16;
        var decodeBuf = layout.EnsureTimestampDecodeBuffer(nonNull * 8);
        BinaryPrimitives.WriteInt64LittleEndian(decodeBuf.AsSpan(0, 8), firstTs);
        BinaryPrimitives.WriteInt64LittleEndian(decodeBuf.AsSpan(8, 8), secondTs);
        var bitstreamLen = payload.Length - bitstreamStart;
        if (bitstreamLen < 0) throw new QwpDecodeException("Gorilla bitstream past payload end");
        // The decoder takes ReadOnlyMemory; copy the bitstream slice into an owned
        // buffer so the ReadOnlyMemory aliases stable bytes for the decode loop.
        // (QwpBatchBuffer.Payload is itself stable for the call duration, but
        // ReadOnlyMemory<byte> from a span isn't directly constructible — use the
        // payload-backed array slice explicitly.)
        // Instead of allocating, build an Array-backed ReadOnlyMemory by walking back
        // through the buffer's owning array via QwpBatchBuffer.PayloadMemory. We
        // don't have direct access to that here; use a one-shot copy to a column-
        // owned buffer for now. Allocation is per-batch, not per-row, so acceptable.
        var bitstreamCopy = new byte[bitstreamLen];
        payload.Slice(bitstreamStart, bitstreamLen).CopyTo(bitstreamCopy);
        _gorillaDecoder.Reset(firstTs, secondTs, bitstreamCopy);
        for (var i = 2; i < nonNull; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(
                decodeBuf.AsSpan(i * 8, 8),
                _gorillaDecoder.DecodeNext());
        }
        // Park ValuesOffset at the sentinel (-1) so ColumnView dispatches to the
        // managed timestamp-decode buffer rather than the payload offset.
        layout.ValuesOffset = -1;
        var bitPos = _gorillaDecoder.BitPosition;
        var bitstreamBytes = (int)((bitPos + 7) >> 3);
        var end = bitstreamStart + bitstreamBytes;
        if (end > payload.Length)
        {
            throw new QwpDecodeException("truncated Gorilla bitstream");
        }
        return end;
    }

    /// <summary>
    ///     §3.3c — clears the connection-scoped SYMBOL dict when CACHE_RESET arrives
    ///     with the dict-clear bit set. Caller is the IO thread on a CACHE_RESET frame
    ///     (wiring lands in §3.3); the decoder is the right place because it owns the
    ///     dict state.
    /// </summary>
    public void ApplyCacheReset(byte resetMask)
    {
        if ((resetMask & QwpEgressMsgKind.RESET_MASK_DICT) != 0)
        {
            _connDictSize = 0;
            _connDictHeapLength = 0;
            // Capacity stays — server is expected to start refilling immediately
            // and we'd just realloc back up otherwise.
        }
    }

    /// <summary>
    ///     §3.2c — appends new dict entries from the message's delta section to the
    ///     connection-scoped SYMBOL dict. Wire layout:
    ///     <list type="number">
    ///         <item><c>delta_start: varint</c> — must equal current <see cref="_connDictSize"/>.</item>
    ///         <item><c>delta_count: varint</c></item>
    ///         <item>per new entry: <c>len:varint, utf8_bytes</c></item>
    ///     </list>
    ///     Bounds checks reject hostile (delta_start, delta_count) pairs that would
    ///     overflow the int sample count or the configured heap cap.
    /// </summary>
    private int ParseDeltaSymbolDict(ReadOnlySpan<byte> payload, int p)
    {
        ReadVarint(payload, ref p, out var deltaStart);
        ReadVarint(payload, ref p, out var deltaCount);
        if (deltaStart < 0 || deltaCount < 0
            || deltaStart > MAX_CONN_DICT_SIZE
            || deltaCount > MAX_CONN_DICT_SIZE - deltaStart)
        {
            throw new QwpDecodeException(
                $"delta symbol section out of range: start={deltaStart}, count={deltaCount}");
        }
        if (deltaStart != _connDictSize)
        {
            throw new QwpDecodeException(
                $"delta symbol dict out of sync: expected start={_connDictSize}, got={deltaStart}");
        }
        var newSize = _connDictSize + (int)deltaCount;
        EnsureConnDictEntriesCapacity(newSize * 8);
        for (long i = 0; i < deltaCount; i++)
        {
            ReadVarint(payload, ref p, out var entryLen);
            if (entryLen < 0 || entryLen > int.MaxValue || p + entryLen > payload.Length)
            {
                throw new QwpDecodeException("truncated delta symbol entry");
            }
            var len = (int)entryLen;
            var newHeapPos = (long)_connDictHeapLength + len;
            if (newHeapPos > MAX_CONN_DICT_HEAP_BYTES)
            {
                throw new QwpDecodeException(
                    $"connection SYMBOL dict heap exceeds cap ({MAX_CONN_DICT_HEAP_BYTES} bytes); " +
                    "server must emit CACHE_RESET");
            }
            EnsureConnDictHeapCapacity((int)newHeapPos);
            payload.Slice(p, len).CopyTo(_connDictHeapBytes.AsSpan(_connDictHeapLength, len));
            var packed = (long)((uint)_connDictHeapLength) | ((long)len << 32);
            BinaryPrimitives.WriteInt64LittleEndian(_connDictEntries.AsSpan(_connDictSize * 8, 8), packed);
            _connDictSize++;
            _connDictHeapLength = (int)newHeapPos;
            p += len;
        }
        return p;
    }

    private void EnsureConnDictEntriesCapacity(int requiredBytes)
    {
        if (_connDictEntries.Length >= requiredBytes) return;
        var newCap = Math.Max(_connDictEntries.Length * 2, Math.Max(512 * 8, requiredBytes));
        var bigger = new byte[newCap];
        Array.Copy(_connDictEntries, bigger, _connDictSize * 8);
        _connDictEntries = bigger;
    }

    private void EnsureConnDictHeapCapacity(int required)
    {
        if (_connDictHeapBytes.Length >= required) return;
        var newCap = Math.Max(_connDictHeapBytes.Length * 2, Math.Max(4096, required));
        var bigger = new byte[newCap];
        Array.Copy(_connDictHeapBytes, bigger, _connDictHeapLength);
        _connDictHeapBytes = bigger;
    }

    /// <summary>
    ///     §3.2e — parses a non-delta SYMBOL column. Wire format:
    ///     <list type="number">
    ///         <item><c>dict_size:varint</c></item>
    ///         <item>per dict entry: <c>len:varint, utf8_bytes</c></item>
    ///         <item>per non-null row: <c>id:varint</c></item>
    ///     </list>
    ///     Dict entries are unpacked into the column's owned-entries buffer as
    ///     <c>(offset:i32 | length:i32&lt;&lt;32)</c> packed longs so per-row symbol
    ///     lookup is one packed-long load + a UTF-8 decode against the inline
    ///     dict-heap region of the payload. Per-row ids are materialised into
    ///     <see cref="QwpColumnLayout.SymbolRowIds"/> for O(1) random access.
    ///     <para/>
    ///     Delta-mode (<c>FLAG_DELTA_SYMBOL_DICT</c>) and the connection-scoped dict
    ///     are §3.2c — this parser handles only per-message local dicts.
    /// </summary>
    private int ParseSymbolColumn(
        QwpColumnLayout layout,
        ReadOnlySpan<byte> payload,
        int rowCount,
        int p)
    {
        int dictSize;
        if (_deltaMode)
        {
            // §3.2c — delta mode: no per-column dict; ids reference the connection
            // dict already populated by ParseDeltaSymbolDict. Layout aliases the
            // decoder's connection buffers so ColumnView reads dict entries / heap
            // through them.
            dictSize = _connDictSize;
            layout.SymbolHeapBuffer = _connDictHeapBytes;
            layout.SymbolHeapBufferLength = _connDictHeapLength;
            layout.SymbolEntriesBuffer = _connDictEntries;
            layout.SymbolDictSize = dictSize;
            // SymbolDictHeapOffset is meaningless in delta mode; clear to a sentinel
            // so a stale value from a previous non-delta batch doesn't mislead readers.
            layout.SymbolDictHeapOffset = -1;
            layout.SymbolDictEntriesOffset = -1;
        }
        else
        {
            ReadVarint(payload, ref p, out var dictSizeVarint);
            if (dictSizeVarint < 0 || dictSizeVarint > rowCount)
            {
                throw new QwpDecodeException(
                    $"SYMBOL dict size out of range: {dictSizeVarint} (rowCount={rowCount})");
            }
            dictSize = (int)dictSizeVarint;

            // Dict entries live inline in the payload starting here. Capture the base
            // offset so per-entry offsets can be stored relative to it.
            var dictBase = p;
            layout.SymbolDictHeapOffset = dictBase;
            // Non-delta — clear delta-mode buffer aliases left over from a prior batch.
            layout.SymbolHeapBuffer = null;
            layout.SymbolHeapBufferLength = 0;

            var entries = layout.EnsureOwnedEntries(dictSize * 8);
            for (var e = 0; e < dictSize; e++)
            {
                ReadVarint(payload, ref p, out var entryLenVarint);
                if (entryLenVarint < 0 || entryLenVarint > int.MaxValue || p + entryLenVarint > payload.Length)
                {
                    throw new QwpDecodeException("truncated SYMBOL dict entry");
                }
                var lenBytes = (int)entryLenVarint;
                var offset = p - dictBase;
                // Pack (offset:i32 | length:i32<<32) into one 8-byte slot. ColumnView's
                // GetSymbol (when §3.1 wires it) reads the slot, splits, and decodes the
                // dict-heap UTF-8 region.
                var packed = (long)((uint)offset) | ((long)lenBytes << 32);
                BinaryPrimitives.WriteInt64LittleEndian(entries.AsSpan(e * 8, 8), packed);
                p += lenBytes;
            }
            layout.SymbolDictSize = dictSize;
            layout.SymbolEntriesBuffer = entries;
            // SymbolDictEntriesOffset stays unset — entry storage lives in OwnedEntries
            // (managed) rather than at a payload offset, since the wire format encodes
            // entries as varints rather than as fixed 8-byte packed longs.
            layout.SymbolDictEntriesOffset = -1;
        }

        // Per-row ids. NULL rows are skipped — their slot in SymbolRowIds stays at
        // the previous value, but the IsNull check on the layout's null bitmap
        // gates lookups.
        if (layout.SymbolRowIds is null || layout.SymbolRowIds.Length < rowCount)
        {
            layout.SymbolRowIds = new int[Math.Max(rowCount, 16)];
        }
        var noNulls = layout.NullBitmapOffset < 0;
        var nonNullIdx = layout.NonNullIdx;
        for (var i = 0; i < rowCount; i++)
        {
            if (!noNulls && nonNullIdx![i] < 0) continue;
            ReadVarint(payload, ref p, out var idVarint);
            if (idVarint < 0 || idVarint >= dictSize)
            {
                throw new QwpDecodeException($"symbol index out of range: {idVarint}");
            }
            layout.SymbolRowIds[i] = (int)idVarint;
        }

        // The accessor path uses SymbolDictHeapOffset/Buffer + SymbolEntriesBuffer
        // + SymbolRowIds; ValuesOffset is unused for SYMBOL columns.
        layout.ValuesOffset = 0;
        return p;
    }

    private static int ParseNullSection(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int rowCount, int p)
    {
        if (p >= payload.Length) throw new QwpDecodeException("truncated null flag");
        var flag = payload[p++];
        if (flag == 0)
        {
            // No nulls: skip the per-row dense-index fill. Accessors detect this via
            // NullBitmapOffset == -1 and treat the dense index as the row index.
            layout.NullBitmapOffset = -1;
            layout.NonNullIdx = null;
            layout.NonNullCount = rowCount;
            return p;
        }
        var bitmapBytes = (rowCount + 7) >> 3;
        if (p + bitmapBytes > payload.Length) throw new QwpDecodeException("truncated null bitmap");
        layout.NullBitmapOffset = p;
        if (layout.NonNullIdx is null || layout.NonNullIdx.Length < rowCount)
        {
            var nextLen = layout.NonNullIdx is null
                ? Math.Max(rowCount, 16)
                : Math.Max(rowCount, layout.NonNullIdx.Length * 2);
            layout.NonNullIdx = new int[nextLen];
        }
        var denseIdx = 0;
        for (var i = 0; i < rowCount; i++)
        {
            var bi = i >> 3;
            var bit = i & 7;
            var bm = payload[p + bi];
            if ((bm & (1 << bit)) != 0)
            {
                layout.NonNullIdx[i] = -1;
            }
            else
            {
                layout.NonNullIdx[i] = denseIdx++;
            }
        }
        layout.NonNullCount = denseIdx;
        return p + bitmapBytes;
    }
}
