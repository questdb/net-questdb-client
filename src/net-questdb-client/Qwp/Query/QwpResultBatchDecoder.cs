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
    private static readonly UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);

    private readonly QwpEgressConnState _state;

    public QwpResultBatchDecoder(QwpEgressConnState state)
    {
        _state = state;
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

        if ((headerFlags & QwpConstants.FlagDeltaSymbolDict) != 0)
        {
            DecodeDeltaSymbolDict(payload, ref p);
        }

        batch.Reset();
        batch.RequestId = requestId;
        batch.BatchSeq = (long)batchSeq;

        DecodeTableBlock(payload, ref p, headerFlags, batch);

        if (p != payload.Length)
        {
            throw new QwpDecodeException($"trailing bytes after RESULT_BATCH: consumed {p}, payload {payload.Length}");
        }
    }

    private void DecodeDeltaSymbolDict(ReadOnlySpan<byte> payload, ref int p)
    {
        var deltaStart = (int)ReadVarint(payload, ref p);
        var deltaCount = (int)ReadVarint(payload, ref p);

        if (deltaStart != _state.SymbolDict.Size)
        {
            throw new QwpDecodeException(
                $"symbol dict deltaStart={deltaStart} disagrees with client cursor {_state.SymbolDict.Size}");
        }

        for (var i = 0; i < deltaCount; i++)
        {
            var len = (int)ReadVarint(payload, ref p);
            if (p + len > payload.Length)
            {
                throw new QwpDecodeException("truncated symbol dict entry");
            }
            _state.SymbolDict.AppendEntry(payload.Slice(p, len));
            p += len;
        }
    }

    private void DecodeTableBlock(ReadOnlySpan<byte> payload, ref int p, byte headerFlags, QwpColumnBatch batch)
    {
        var nameLen = (int)ReadVarint(payload, ref p);
        if (p + nameLen > payload.Length)
        {
            throw new QwpDecodeException("truncated table name");
        }
        p += nameLen;

        var rowCount = (int)ReadVarint(payload, ref p);
        var colCount = (int)ReadVarint(payload, ref p);
        if (rowCount < 0 || rowCount > QwpConstants.MaxRowsPerTable)
        {
            throw new QwpDecodeException($"row_count out of range: {rowCount}");
        }

        if (colCount < 0 || colCount > QwpConstants.MaxColumnsPerTable)
        {
            throw new QwpDecodeException($"col_count out of range: {colCount}");
        }

        if (p >= payload.Length)
        {
            throw new QwpDecodeException("truncated before schema_mode");
        }
        var schemaMode = payload[p++];
        var schemaId = ReadVarint(payload, ref p);

        EgressSchema schema;
        if (schemaMode == QwpConstants.SchemaModeFull)
        {
            var defs = new EgressColumnDef[colCount];
            for (var i = 0; i < colCount; i++)
            {
                var cnLen = (int)ReadVarint(payload, ref p);
                if (p + cnLen > payload.Length)
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
            _state.RegisterSchema(schemaId, schema);
        }
        else if (schemaMode == QwpConstants.SchemaModeReference)
        {
            if (!_state.TryGetSchema(schemaId, out schema))
            {
                throw new QwpDecodeException($"unknown schema_id {schemaId} in REFERENCE mode");
            }
            if (schema.Columns.Length != colCount)
            {
                throw new QwpDecodeException(
                    $"schema_id {schemaId} has {schema.Columns.Length} cols but RESULT_BATCH says {colCount}");
            }
        }
        else
        {
            throw new QwpDecodeException($"unknown schema_mode 0x{schemaMode:X2}");
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
            if (p + bitmapBytes > payload.Length)
            {
                throw new QwpDecodeException("truncated null bitmap");
            }
            if (col.NonNullIndexBuf.Length < rowCount)
            {
                col.NonNullIndexBuf = new int[Math.Max(rowCount, 64)];
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
                DecodeDecimalColumn(payload, ref p, col, nonNull, valueBytes: 8);
                break;

            case QwpTypeCode.Decimal128:
                DecodeDecimalColumn(payload, ref p, col, nonNull, valueBytes: 16);
                break;

            case QwpTypeCode.Decimal256:
                DecodeDecimalColumn(payload, ref p, col, nonNull, valueBytes: 32);
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

        var dest = nonNull > 0
            ? MemoryMarshal.Cast<byte, long>(col.ValueBytes.AsSpan(0, rawBytes))
            : Span<long>.Empty;
        var consumed = QwpGorilla.Decode(payload.Slice(p), dest, nonNull);

        if (nonNull == 0)
        {
            if (p >= payload.Length)
            {
                throw new QwpDecodeException("truncated before timestamp encoding flag");
            }
            // Server emits the discriminator even with zero non-nulls (FLAG_GORILLA contract).
            p++;
            return;
        }

        p += consumed;
    }

    private void DecodeStringColumn(ReadOnlySpan<byte> payload, ref int p, ColumnView col, int nonNull)
    {
        var offsetBytes = (nonNull + 1) * 4;
        if (p + offsetBytes > payload.Length)
        {
            throw new QwpDecodeException("truncated varchar offsets");
        }

        if (col.StringOffsetsBuf.Length < nonNull + 1)
        {
            col.StringOffsetsBuf = new int[Math.Max(nonNull + 1, 64)];
        }
        var offsets = col.StringOffsetsBuf;
        for (var i = 0; i <= nonNull; i++)
        {
            offsets[i] = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p, 4));
            p += 4;
        }

        var heapLen = nonNull > 0 ? offsets[nonNull] - offsets[0] : 0;
        if (heapLen < 0 || p + heapLen > payload.Length)
        {
            throw new QwpDecodeException("truncated varchar heap");
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
        col.ValueBytes = RentScratch(col.ValueBytes, nonNull * 4);
        col.SymbolDict = _state.SymbolDict;

        for (var i = 0; i < nonNull; i++)
        {
            var id = (int)ReadVarint(payload, ref p);
            BinaryPrimitives.WriteInt32LittleEndian(col.ValueBytes.AsSpan(i * 4, 4), id);
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
        if (p + byteCount > payload.Length)
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

    private byte[] RentScratch(byte[] existing, int needed)
    {
        if (existing.Length >= needed) return existing;
        var cap = Math.Max(needed, 64);
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
            col.StringOffsetsBuf = new int[Math.Max(nonNull + 1, 64)];
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

            var dimsBytes = nDims * 4;
            if (p + dimsBytes > payload.Length)
            {
                throw new QwpDecodeException("truncated array row: dim header overflow");
            }

            long elementCount = 1;
            for (var d = 0; d < nDims; d++)
            {
                var dim = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p + d * 4, 4));
                if (dim < 0)
                {
                    throw new QwpDecodeException($"array dim {d} negative ({dim})");
                }
                elementCount *= dim;
            }
            p += dimsBytes;

            var valueBytes = elementCount * elementBytes;
            if (valueBytes < 0 || p + valueBytes > payload.Length)
            {
                throw new QwpDecodeException("truncated array row: values overflow");
            }
            p += (int)valueBytes;
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
