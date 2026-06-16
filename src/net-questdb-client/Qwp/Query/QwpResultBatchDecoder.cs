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
using System.Text;
using QuestDB.Enums;

namespace QuestDB.Qwp.Query;

/// <summary>
///     Decodes <c>RESULT_BATCH</c> payloads into a reusable <see cref="QwpColumnBatch" />.
/// </summary>
/// <remarks>
///     Caller has already parsed the 12-byte QWP1 frame header and routed the message kind.
///     Caller passes the post-header payload (starting at <c>msg_kind=0x11</c>) and the header
///     <c>flags</c> byte so the decoder can interpret <see cref="QwpConstants.FlagGorilla" /> and
///     <see cref="QwpConstants.FlagDeltaSymbolDict" />.
/// </remarks>
internal sealed class QwpResultBatchDecoder
{
    private static readonly UTF8Encoding StrictUtf8 = QwpConstants.StrictUtf8;

    private readonly QwpEgressConnState _state;

    // Per-query column schema captured from the first batch (batch_seq == 0). Continuation
    // batches bind rows to it. Reused across queries by reassignment; ResetQuerySchema()
    // invalidates it so a stray continuation can't bind to a stale schema after a new
    // query starts. Single slot is correct because the query client serialises queries
    // (one in-flight at a time); if pipelining is ever added this must become per-request_id.
    private EgressSchema? _querySchema;
    private bool _querySchemaValid;

    public QwpResultBatchDecoder(QwpEgressConnState state)
    {
        _state = state;
    }

    /// <summary>
    ///     Invalidates the schema captured from the last <c>batch_seq == 0</c>. The query client
    ///     calls this when a new query starts so the next query's continuation batches can't bind
    ///     rows to a stale schema.
    /// </summary>
    public void ResetQuerySchema()
    {
        _querySchemaValid = false;
        _querySchema = null;
    }

    public void Decode(ReadOnlySpan<byte> payload, byte headerFlags, QwpColumnBatch batch)
    {
        var p = 0;
        if (payload.Length < 1 + 8)
        {
            throw new QwpDecodeException("RESULT_BATCH payload too short for prelude");
        }

        var msgKind = payload[p++];
        if (msgKind != QwpConstants.MsgKindResultBatch)
        {
            throw new QwpDecodeException($"expected RESULT_BATCH (0x11), got 0x{msgKind:X2}");
        }

        var requestId = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(p, 8));
        p += 8;
        var batchSeq = ReadVarint(payload, ref p);

        var preDictSize = _state.SymbolDict.Size;
        var preSchemaValid = _querySchemaValid;
        var preSchema = _querySchema;
        var commit = false;
        try
        {
            if ((headerFlags & QwpConstants.FlagDeltaSymbolDict) != 0)
            {
                DecodeDeltaSymbolDict(payload, ref p);
            }

            batch.Reset();
            batch.RequestId = requestId;
            batch.BatchSeq = (long)batchSeq;

            DecodeTableBlock(payload, ref p, headerFlags, batch, (long)batchSeq);

            if (p != payload.Length)
            {
                throw new QwpDecodeException($"trailing bytes after RESULT_BATCH: consumed {p}, payload {payload.Length}");
            }

            commit = true;
        }
        finally
        {
            if (!commit)
            {
                // Symbols already appended into the dict; rewind to the pre-batch cursor on failure.
                _state.SymbolDict.TruncateTo(preDictSize);
                // Same for the per-query schema slot: a partial parse of a batch_seq==0 must not
                // leave the decoder with a half-built schema bound for subsequent continuation batches.
                _querySchemaValid = preSchemaValid;
                _querySchema = preSchema;
            }
        }
    }

    private void DecodeDeltaSymbolDict(ReadOnlySpan<byte> payload, ref int p)
    {
        var deltaStart = ReadBoundedVarintAsInt(payload, ref p, "symbol dict deltaStart");
        var deltaCount = ReadBoundedVarintAsInt(payload, ref p, "symbol dict deltaCount");

        if (deltaStart != _state.SymbolDict.Size)
        {
            throw new QwpDecodeException(
                $"symbol dict deltaStart={deltaStart} disagrees with client cursor {_state.SymbolDict.Size}");
        }

        if ((long)deltaStart + deltaCount > int.MaxValue)
        {
            throw new QwpDecodeException(
                $"symbol dict deltaStart+deltaCount overflows: {deltaStart}+{deltaCount}");
        }

        for (var i = 0; i < deltaCount; i++)
        {
            var len = ReadBoundedVarintAsInt(payload, ref p, "symbol dict entry length");
            if (len > QwpConstants.MaxResultBatchWireBytes)
            {
                throw new QwpDecodeException($"symbol dict entry length out of range: {len}");
            }
            if (len > payload.Length - p)
            {
                throw new QwpDecodeException("truncated symbol dict entry");
            }
            _state.SymbolDict.AppendEntry(payload.Slice(p, len));
            p += len;
        }
    }

    private static int ReadBoundedVarintAsInt(ReadOnlySpan<byte> payload, ref int p, string field)
    {
        var v = ReadVarint(payload, ref p);
        if (v > int.MaxValue)
        {
            throw new QwpDecodeException($"{field} varint exceeds int.MaxValue: {v}");
        }
        return (int)v;
    }

    private void DecodeTableBlock(
        ReadOnlySpan<byte> payload, ref int p, byte headerFlags, QwpColumnBatch batch, long batchSeq)
    {
        var nameLen = ReadBoundedVarintAsInt(payload, ref p, "table name length");
        if (nameLen > QwpConstants.MaxNameLengthBytes)
        {
            throw new QwpDecodeException($"table name length out of range: {nameLen}");
        }
        if (nameLen > payload.Length - p)
        {
            throw new QwpDecodeException("truncated table name");
        }
        p += nameLen;

        var rowCount = ReadBoundedVarintAsInt(payload, ref p, "row_count");
        if (rowCount > QwpConstants.MaxRowsPerTable)
        {
            throw new QwpDecodeException($"row_count out of range: {rowCount}");
        }

        EgressSchema schema;
        int colCount;
        if (batchSeq == 0)
        {
            // First batch of a query carries the inline schema: col_count then one
            // (name_length, name, wire_type) descriptor per column.
            colCount = ReadBoundedVarintAsInt(payload, ref p, "col_count");
            if (colCount > QwpConstants.MaxColumnsPerTable)
            {
                throw new QwpDecodeException($"col_count out of range: {colCount}");
            }

            var defs = new EgressColumnDef[colCount];
            for (var i = 0; i < colCount; i++)
            {
                var cnLen = ReadBoundedVarintAsInt(payload, ref p, "column name length");
                if (cnLen > QwpConstants.MaxNameLengthBytes)
                {
                    throw new QwpDecodeException($"column name length out of range: {cnLen}");
                }
                if (cnLen > payload.Length - p)
                {
                    throw new QwpDecodeException("truncated column name");
                }
                var name = StrictUtf8.GetString(payload.Slice(p, cnLen));
                p += cnLen;
                if (p >= payload.Length)
                {
                    throw new QwpDecodeException("truncated before column type code");
                }
                var typeCode = (QwpTypeCode)payload[p++];
                defs[i] = new EgressColumnDef(name, typeCode);
            }
            schema = new EgressSchema(defs);
            _querySchema = schema;
            _querySchemaValid = true;
        }
        else
        {
            // Continuation batch: bind rows to the schema delivered on batch_seq == 0.
            // A continuation arriving before any schema-bearing batch (malformed or hostile
            // server) must not be allowed to bind rows to a stale schema.
            if (!_querySchemaValid || _querySchema is null)
            {
                throw new QwpDecodeException(
                    $"RESULT_BATCH batch_seq={batchSeq} arrived before the schema-bearing batch_seq=0");
            }
            schema = _querySchema;
            colCount = schema.Columns.Length;
        }

        batch.RowCount = rowCount;
        batch.TrimToColumnCount(colCount);
        for (var i = 0; i < colCount; i++)
        {
            var def = schema.Columns[i];
            batch.ConfigureColumn(i, def.Name, def.TypeCode, scale: 0, precisionBits: 0);
        }

        var gorillaEnabled = (headerFlags & QwpConstants.FlagGorilla) != 0;
        for (var i = 0; i < colCount; i++)
        {
            DecodeColumnData(payload, ref p, batch.GetColumn(i), rowCount, gorillaEnabled);
        }
    }

    private void DecodeColumnData(
        ReadOnlySpan<byte> payload, ref int p, ColumnView col, int rowCount, bool gorillaEnabled)
    {
        if (p >= payload.Length)
        {
            throw new QwpDecodeException("truncated before null_flag");
        }

        var nullFlag = payload[p++];
        int nonNull;
        int[]? nonNullIndex = null;
        if (nullFlag == 0)
        {
            nonNull = rowCount;
        }
        else
        {
            var bitmapBytes = (rowCount + 7) >> 3;
            if (bitmapBytes > payload.Length - p)
            {
                throw new QwpDecodeException("truncated null bitmap");
            }
            if (col.NonNullIndexBuf.Length < rowCount)
            {
                col.NonNullIndexBuf = new int[Math.Max(rowCount, Math.Max(64, col.NonNullIndexBuf.Length * 2))];
            }
            nonNull = BuildNonNullIndex(payload.Slice(p, bitmapBytes), rowCount, col.NonNullIndexBuf);
            nonNullIndex = col.NonNullIndexBuf;
            p += bitmapBytes;
        }

        col.NonNullIndex = nonNullIndex;

        switch (col.TypeCode)
        {
            case QwpTypeCode.Boolean:
                CopyFixed(payload, ref p, col, (nonNull + 7) >> 3);
                break;

            case QwpTypeCode.Byte:
                CopyFixed(payload, ref p, col, nonNull);
                break;

            case QwpTypeCode.Short:
            case QwpTypeCode.Char:
                CopyFixed(payload, ref p, col, nonNull * 2);
                break;

            case QwpTypeCode.Int:
            case QwpTypeCode.IPv4:
            case QwpTypeCode.Float:
                CopyFixed(payload, ref p, col, nonNull * 4);
                break;

            case QwpTypeCode.Long:
            case QwpTypeCode.Double:
                CopyFixed(payload, ref p, col, nonNull * 8);
                break;

            case QwpTypeCode.Uuid:
                CopyFixed(payload, ref p, col, nonNull * 16);
                break;

            case QwpTypeCode.Long256:
                CopyFixed(payload, ref p, col, nonNull * QwpConstants.Long256SizeBytes);
                break;

            case QwpTypeCode.Date:
            case QwpTypeCode.Timestamp:
            case QwpTypeCode.TimestampNanos:
                DecodeTimestampColumn(payload, ref p, col, nonNull, gorillaEnabled);
                break;

            case QwpTypeCode.Varchar:
            case QwpTypeCode.Binary:
                DecodeStringColumn(payload, ref p, col, nonNull);
                break;

            case QwpTypeCode.Symbol:
                DecodeSymbolColumn(payload, ref p, col, nonNull);
                break;

            case QwpTypeCode.Decimal64:
                DecodeDecimalColumn(payload, ref p, col, nonNull, valueBytes: QwpConstants.Decimal64SizeBytes);
                break;

            case QwpTypeCode.Decimal128:
                DecodeDecimalColumn(payload, ref p, col, nonNull, valueBytes: QwpConstants.Decimal128SizeBytes);
                break;

            case QwpTypeCode.Decimal256:
                DecodeDecimalColumn(payload, ref p, col, nonNull, valueBytes: QwpConstants.Decimal256SizeBytes);
                break;

            case QwpTypeCode.Geohash:
                DecodeGeohashColumn(payload, ref p, col, nonNull);
                break;

            case QwpTypeCode.DoubleArray:
            case QwpTypeCode.LongArray:
                DecodeArrayColumn(payload, ref p, col, nonNull, elementBytes: 8);
                break;

            default:
                throw new QwpDecodeException($"unsupported column type 0x{(byte)col.TypeCode:X2}");
        }
    }

    private void DecodeTimestampColumn(
        ReadOnlySpan<byte> payload, ref int p, ColumnView col, int nonNull, bool gorillaEnabled)
    {
        if (!gorillaEnabled)
        {
            CopyFixed(payload, ref p, col, nonNull * 8);
            return;
        }

        var rawBytes = nonNull * 8;
        col.ValueBytes = RentScratch(col.ValueBytes, Math.Max(rawBytes, 0));

        if (nonNull == 0)
        {
            if (p >= payload.Length)
            {
                throw new QwpDecodeException("truncated before timestamp encoding flag");
            }
            var flag = payload[p++];
            if (flag != 0x00)
            {
                throw new QwpDecodeException(
                    $"timestamp encoding flag 0x{flag:X2} invalid for nonNull=0; only 0x00 (raw) is valid");
            }
            return;
        }

        // BE-safe: writes LE bytes; the Span<long> overload would byte-swap on big-endian readers.
        var consumed = QwpGorilla.DecodeToBytes(payload.Slice(p), col.ValueBytes.AsSpan(0, rawBytes), nonNull);
        p += consumed;
    }

    private void DecodeStringColumn(ReadOnlySpan<byte> payload, ref int p, ColumnView col, int nonNull)
    {
        var offsetBytes = (nonNull + 1) * 4;
        if (offsetBytes > payload.Length - p)
        {
            throw new QwpDecodeException("truncated varchar offsets");
        }

        if (col.StringOffsetsBuf.Length < nonNull + 1)
        {
            col.StringOffsetsBuf = new int[Math.Max(nonNull + 1, Math.Max(64, col.StringOffsetsBuf.Length * 2))];
        }
        var offsets = col.StringOffsetsBuf;
        for (var i = 0; i <= nonNull; i++)
        {
            offsets[i] = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p, 4));
            p += 4;
        }

        if (offsets[0] != 0)
        {
            throw new QwpDecodeException($"varchar offsets[0] must be 0, got {offsets[0]}");
        }

        var heapLen = nonNull > 0 ? offsets[nonNull] : 0;
        if (heapLen < 0 || heapLen > payload.Length - p)
        {
            throw new QwpDecodeException("truncated varchar heap");
        }

        var prev = 0;
        for (var i = 1; i <= nonNull; i++)
        {
            var off = offsets[i];
            if (off < prev || off > heapLen)
            {
                throw new QwpDecodeException(
                    $"varchar offsets non-monotonic or out of range at index {i}: prev={prev} off={off} heapLen={heapLen}");
            }
            prev = off;
        }

        col.StringOffsets = offsets;
        col.StringHeap = RentScratch(col.StringHeap, heapLen);
        if (heapLen > 0)
        {
            payload.Slice(p, heapLen).CopyTo(col.StringHeap);
            p += heapLen;
        }
    }

    private void DecodeSymbolColumn(ReadOnlySpan<byte> payload, ref int p, ColumnView col, int nonNull)
    {
        if (col.SymbolIdsBuf.Length < Math.Max(nonNull, 1))
        {
            col.SymbolIdsBuf = new int[Math.Max(nonNull, Math.Max(64, col.SymbolIdsBuf.Length * 2))];
        }
        col.SymbolIds = col.SymbolIdsBuf;
        col.SymbolDict = _state.SymbolDict;

        var dictSize = _state.SymbolDict.Size;
        for (var i = 0; i < nonNull; i++)
        {
            var id = ReadBoundedVarintAsInt(payload, ref p, "symbol id");
            if ((uint)id >= (uint)dictSize)
            {
                throw new QwpDecodeException(
                    $"symbol id {id} out of range [0, {dictSize})");
            }
            col.SymbolIdsBuf[i] = id;
        }
    }

    private void DecodeDecimalColumn(
        ReadOnlySpan<byte> payload, ref int p, ColumnView col, int nonNull, int valueBytes)
    {
        if (p >= payload.Length)
        {
            throw new QwpDecodeException("truncated before decimal scale prefix");
        }
        var scale = payload[p++];
        SetScale(col, scale);
        CopyFixed(payload, ref p, col, nonNull * valueBytes);
    }

    private void DecodeGeohashColumn(ReadOnlySpan<byte> payload, ref int p, ColumnView col, int nonNull)
    {
        var precisionBits = (int)ReadVarint(payload, ref p);
        if (precisionBits < QwpConstants.MinGeohashPrecisionBits
            || precisionBits > QwpConstants.MaxGeohashPrecisionBits)
        {
            throw new QwpDecodeException($"geohash precision out of range: {precisionBits}");
        }
        SetPrecision(col, (byte)precisionBits);
        var stride = (precisionBits + 7) >> 3;
        CopyFixed(payload, ref p, col, nonNull * stride);
    }

    private void CopyFixed(ReadOnlySpan<byte> payload, ref int p, ColumnView col, int byteCount)
    {
        if (byteCount < 0 || byteCount > payload.Length - p)
        {
            throw new QwpDecodeException(
                $"truncated column data: need {byteCount} bytes, payload has {payload.Length - p}");
        }
        col.ValueBytes = RentScratch(col.ValueBytes, byteCount);
        if (byteCount > 0)
        {
            payload.Slice(p, byteCount).CopyTo(col.ValueBytes);
            p += byteCount;
        }
    }

    // Plain arrays, not ArrayPool: the per-column scratch is grown-and-reused across every batch,
    // so pooling buys almost nothing — and the final buffers are never returned, which would
    // otherwise drain ArrayPool.Shared of large arrays for each disposed query client.
    private byte[] RentScratch(byte[] existing, int needed)
    {
        if (existing.Length >= needed) return existing;
        var cap = Math.Max(needed, Math.Max(64, existing.Length * 2));
        return new byte[cap];
    }

    private static int BuildNonNullIndex(ReadOnlySpan<byte> bitmap, int rowCount, int[] index)
    {
        var nonNull = 0;
        for (var r = 0; r < rowCount; r++)
        {
            var bit = (bitmap[r >> 3] >> (r & 7)) & 1;
            if (bit == 1)
            {
                index[r] = -1;
            }
            else
            {
                index[r] = nonNull++;
            }
        }
        return nonNull;
    }

    private static ulong ReadVarint(ReadOnlySpan<byte> payload, ref int p)
    {
        if (p >= payload.Length)
        {
            throw new QwpDecodeException("truncated varint");
        }
        var v = QwpVarint.Read(payload.Slice(p), out var consumed);
        p += consumed;
        return v;
    }

    private static void SetScale(ColumnView col, byte scale) => col.Scale = scale;
    private static void SetPrecision(ColumnView col, byte precisionBits) => col.PrecisionBits = precisionBits;

    private void DecodeArrayColumn(ReadOnlySpan<byte> payload, ref int p, ColumnView col, int nonNull, int elementBytes)
    {
        if (col.StringOffsetsBuf.Length < nonNull + 1)
        {
            col.StringOffsetsBuf = new int[Math.Max(nonNull + 1, Math.Max(64, col.StringOffsetsBuf.Length * 2))];
        }
        var offsets = col.StringOffsetsBuf;
        var heapStart = p;
        for (var i = 0; i < nonNull; i++)
        {
            offsets[i] = p - heapStart;
            if (p >= payload.Length)
            {
                throw new QwpDecodeException("truncated array row: missing nDims");
            }
            int nDims = payload[p];
            p++;
            if (nDims < 1 || nDims > QwpConstants.MaxArrayDimensions)
            {
                throw new QwpDecodeException(
                    $"array nDims out of range: {nDims} (must be in [1, {QwpConstants.MaxArrayDimensions}])");
            }

            var dimsBytes = nDims * 4;
            if (dimsBytes > payload.Length - p)
            {
                throw new QwpDecodeException("truncated array row: dim header overflow");
            }

            long elementCount = 1;
            var maxElements = (long)((payload.Length - p - dimsBytes) / elementBytes);
            for (var d = 0; d < nDims; d++)
            {
                var dim = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p + d * 4, 4));
                if (dim < 0)
                {
                    throw new QwpDecodeException($"array dim {d} negative ({dim})");
                }
                if (dim != 0 && elementCount > maxElements / dim)
                {
                    throw new QwpDecodeException(
                        $"array shape exceeds remaining payload: dim {d} = {dim}");
                }
                elementCount *= dim;
            }
            p += dimsBytes;

            var valueBytes = elementCount * elementBytes;
            if (valueBytes < 0 || valueBytes > payload.Length - p)
            {
                throw new QwpDecodeException("truncated array row: values overflow");
            }
            p += (int)valueBytes;

            // The span accessors read [u8 nDims][i32 dims][values]; assert the row carries at
            // least the nDims byte + dim header so a later MemoryMarshal.Cast can't underflow.
            var rowLen = (p - heapStart) - offsets[i];
            if (rowLen < 1 + nDims * 4)
            {
                throw new QwpDecodeException(
                    $"array row {i} shorter than its header: {rowLen} bytes, need {1 + nDims * 4}");
            }
        }
        offsets[nonNull] = p - heapStart;

        var heapLen = p - heapStart;
        col.ValueBytes = RentScratch(col.ValueBytes, heapLen);
        if (heapLen > 0)
        {
            payload.Slice(heapStart, heapLen).CopyTo(col.ValueBytes);
        }
        col.StringOffsets = offsets;
    }
}
