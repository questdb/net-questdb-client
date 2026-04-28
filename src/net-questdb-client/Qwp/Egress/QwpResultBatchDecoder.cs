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
        if ((flags & QwpConstants.FLAG_GORILLA) != 0)
        {
            throw new QwpDecodeException("FLAG_GORILLA not yet supported by this decoder");
        }
        if ((flags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0)
        {
            throw new QwpDecodeException("FLAG_DELTA_SYMBOL_DICT not yet supported by this decoder");
        }

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

    private static int ParseColumn(QwpColumnLayout layout, ReadOnlySpan<byte> payload, int rowCount, int p)
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
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
                return AdvanceFixed(layout, payload, p, sizeBytes: 8);
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
                throw new QwpDecodeException("ARRAY columns not yet supported by this decoder");
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
    private static int ParseSymbolColumn(
        QwpColumnLayout layout,
        ReadOnlySpan<byte> payload,
        int rowCount,
        int p)
    {
        ReadVarint(payload, ref p, out var dictSizeVarint);
        if (dictSizeVarint < 0 || dictSizeVarint > rowCount)
        {
            throw new QwpDecodeException(
                $"SYMBOL dict size out of range: {dictSizeVarint} (rowCount={rowCount})");
        }
        var dictSize = (int)dictSizeVarint;

        // Dict entries live inline in the payload starting here. Capture the base
        // offset so per-entry offsets can be stored relative to it.
        var dictBase = p;
        layout.SymbolDictHeapOffset = dictBase;

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
        // SymbolDictEntriesOffset stays unset — entry storage lives in OwnedEntries
        // (managed) rather than at a payload offset, since the wire format encodes
        // entries as varints rather than as fixed 8-byte packed longs.
        layout.SymbolDictEntriesOffset = -1;

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

        // The accessor path uses SymbolDictHeapOffset + OwnedEntries + SymbolRowIds;
        // ValuesOffset is unused for SYMBOL columns.
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
