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

using System.Runtime.InteropServices;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Transport-agnostic column encoder for QWP v1 table data. The .NET counterpart of
///     Java's <c>QwpColumnWriter</c> on java-questdb-client main 64b7ee69. Reads from a
///     <see cref="QwpTableBuffer"/> and writes encoded bytes to an
///     <see cref="IQwpBufferWriter"/> sink — pinned or segmented.
/// </summary>
/// <remarks>
///     Experimental. Both <c>QwpWebSocketEncoder</c> (PR 6) and <c>QwpUdpSender</c> (PR 6)
///     will delegate to this class for the column-level emission.
/// </remarks>
internal sealed class QwpColumnWriter
{
    private const byte ENCODING_UNCOMPRESSED = 0x00;
    private const byte ENCODING_GORILLA = 0x01;

    private readonly QwpGorillaEncoder _gorillaEncoder = new();
    private IQwpBufferWriter? _buffer;

    /// <summary>Binds the writer to a buffer sink. Must be called before <see cref="EncodeTable"/>.</summary>
    public void SetBuffer(IQwpBufferWriter buffer) => _buffer = buffer;

    /// <summary>
    ///     Encodes the entire table — header + all columns — to the bound buffer.
    /// </summary>
    public void EncodeTable(
        QwpTableBuffer table,
        bool useSchemaRef,
        bool useGlobalSymbols,
        bool useGorilla)
    {
        var schemaId = table.SchemaId < 0 ? 0 : table.SchemaId;
        EncodeTable(table, table.RowCount, useSchemaRef, useGlobalSymbols, useGorilla, schemaId);
    }

    /// <summary>
    ///     Overload allowing the caller to pin a row count (used by the microbatch path
    ///     that ships only a prefix of the in-memory rows).
    /// </summary>
    public void EncodeTable(
        QwpTableBuffer table,
        int rowCount,
        bool useSchemaRef,
        bool useGlobalSymbols,
        bool useGorilla,
        int schemaId)
    {
        var buffer = _buffer ?? throw new InvalidOperationException(
            "QwpColumnWriter: call SetBuffer before EncodeTable");

        var columnDefs = table.GetColumnDefs();
        if (useSchemaRef)
        {
            WriteTableHeaderWithSchemaRef(buffer, table.TableName, rowCount, schemaId, columnDefs.Length);
        }
        else
        {
            WriteTableHeaderWithSchema(buffer, table.TableName, rowCount, schemaId, columnDefs);
        }

        for (var i = 0; i < table.ColumnCount; i++)
        {
            var col = table.GetColumn(i);
            EncodeColumn(buffer, col, rowCount, col.ValueCount, useGorilla, useGlobalSymbols);
        }
    }

    private void EncodeColumn(
        IQwpBufferWriter buffer,
        QwpTableBuffer.ColumnBuffer col,
        int rowCount,
        int valueCount,
        bool useGorilla,
        bool useGlobalSymbols)
    {
        WriteNullHeader(buffer, col, rowCount, rowCount - valueCount);

        switch (col.Type)
        {
            case QwpConstants.TYPE_BOOLEAN:
                WriteBoolean(buffer, col.DataMemory.Span, valueCount);
                break;
            case QwpConstants.TYPE_BYTE:
                buffer.PutBlockOfBytes(col.DataMemory.Slice(0, valueCount));
                break;
            case QwpConstants.TYPE_SHORT:
            case QwpConstants.TYPE_CHAR:
                buffer.PutBlockOfBytes(col.DataMemory.Slice(0, valueCount * 2));
                break;
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_FLOAT:
                buffer.PutBlockOfBytes(col.DataMemory.Slice(0, valueCount * 4));
                break;
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_DOUBLE:
                buffer.PutBlockOfBytes(col.DataMemory.Slice(0, valueCount * 8));
                break;
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
                WriteTimestampColumn(buffer, col.DataMemory, valueCount, useGorilla);
                break;
            case QwpConstants.TYPE_GEOHASH:
                WriteGeoHashColumn(buffer, col.DataMemory.Span, valueCount, col.GeoHashPrecision);
                break;
            case QwpConstants.TYPE_VARCHAR:
            case QwpConstants.TYPE_BINARY:
                WriteStringColumn(buffer, col, valueCount);
                break;
            case QwpConstants.TYPE_SYMBOL:
                if (useGlobalSymbols) WriteSymbolColumnWithGlobalIds(buffer, col, valueCount);
                else WriteSymbolColumn(buffer, col, valueCount);
                break;
            case QwpConstants.TYPE_UUID:
                buffer.PutBlockOfBytes(col.DataMemory.Slice(0, valueCount * 16));
                break;
            case QwpConstants.TYPE_LONG256:
                buffer.PutBlockOfBytes(col.DataMemory.Slice(0, valueCount * 32));
                break;
            case QwpConstants.TYPE_DOUBLE_ARRAY:
                WriteDoubleArrayColumn(buffer, col, valueCount);
                break;
            case QwpConstants.TYPE_LONG_ARRAY:
                WriteLongArrayColumn(buffer, col, valueCount);
                break;
            case QwpConstants.TYPE_DECIMAL64:
                WriteDecimal64Column(buffer, (byte)col.DecimalScale, col.DataMemory.Span, valueCount);
                break;
            case QwpConstants.TYPE_DECIMAL128:
                WriteDecimal128Column(buffer, (byte)col.DecimalScale, col.DataMemory.Span, valueCount);
                break;
            case QwpConstants.TYPE_DECIMAL256:
                WriteDecimal256Column(buffer, (byte)col.DecimalScale, col.DataMemory.Span, valueCount);
                break;
            default:
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    $"QwpColumnWriter: unknown column type 0x{col.Type:X2}");
        }
    }

    private static void WriteTableHeaderWithSchema(
        IQwpBufferWriter buffer,
        string tableName,
        int rowCount,
        int schemaId,
        QwpColumnDef[] columns)
    {
        buffer.PutString(tableName);
        buffer.PutVarint(rowCount);
        buffer.PutVarint(columns.Length);
        buffer.PutByte(QwpConstants.SCHEMA_MODE_FULL);
        buffer.PutVarint(schemaId);
        foreach (var col in columns)
        {
            buffer.PutString(col.Name);
            buffer.PutByte(col.WireTypeCode);
        }
    }

    private static void WriteTableHeaderWithSchemaRef(
        IQwpBufferWriter buffer,
        string tableName,
        int rowCount,
        int schemaId,
        int columnCount)
    {
        buffer.PutString(tableName);
        buffer.PutVarint(rowCount);
        buffer.PutVarint(columnCount);
        buffer.PutByte(QwpConstants.SCHEMA_MODE_REFERENCE);
        buffer.PutVarint(schemaId);
    }

    private static void WriteNullHeader(
        IQwpBufferWriter buffer,
        QwpTableBuffer.ColumnBuffer col,
        int rowCount,
        int nullCount)
    {
        if (nullCount > 0)
        {
            buffer.PutByte(1);
            var bitmapBytes = (rowCount + 7) / 8;
            buffer.PutBlockOfBytes(col.NullBitmapMemory(bitmapBytes));
        }
        else
        {
            buffer.PutByte(0);
        }
    }

    private static void WriteBoolean(IQwpBufferWriter buffer, ReadOnlySpan<byte> data, int count)
    {
        var packedSize = (count + 7) / 8;
        for (var i = 0; i < packedSize; i++)
        {
            byte b = 0;
            for (var bit = 0; bit < 8; bit++)
            {
                var idx = i * 8 + bit;
                if (idx < count && data[idx] != 0) b |= (byte)(1 << bit);
            }
            buffer.PutByte(b);
        }
    }

    private static void WriteGeoHashColumn(
        IQwpBufferWriter buffer,
        ReadOnlySpan<byte> data,
        int count,
        int precision)
    {
        if (precision < 1) precision = 1;
        buffer.PutVarint(precision);
        var valueSize = (precision + 7) / 8;
        var longs = MemoryMarshal.Cast<byte, long>(data);
        for (var i = 0; i < count; i++)
        {
            var value = (ulong)longs[i];
            for (var b = 0; b < valueSize; b++)
            {
                buffer.PutByte((byte)(value >> (b * 8)));
            }
        }
    }

    private static void WriteStringColumn(
        IQwpBufferWriter buffer,
        QwpTableBuffer.ColumnBuffer col,
        int valueCount)
    {
        buffer.PutBlockOfBytes(col.StringOffsetsMemory.Slice(0, (valueCount + 1) * 4));
        buffer.PutBlockOfBytes(col.StringDataMemory.Slice(0, col.StringDataSize));
    }

    private static void WriteSymbolColumn(
        IQwpBufferWriter buffer,
        QwpTableBuffer.ColumnBuffer col,
        int count)
    {
        var dict = col.SymbolList;
        buffer.PutVarint(dict.Count);
        for (var i = 0; i < dict.Count; i++) buffer.PutString(dict[i]);

        var data = MemoryMarshal.Cast<byte, int>(col.DataMemory.Span);
        for (var i = 0; i < count; i++) buffer.PutVarint(data[i]);
    }

    private static void WriteSymbolColumnWithGlobalIds(
        IQwpBufferWriter buffer,
        QwpTableBuffer.ColumnBuffer col,
        int count)
    {
        var auxBytes = col.AuxDataMemory;
        if (auxBytes.IsEmpty)
        {
            // global-IDs-only mode: data buffer holds the global IDs directly.
            var data = MemoryMarshal.Cast<byte, int>(col.DataMemory.Span);
            for (var i = 0; i < count; i++) buffer.PutVarint(data[i]);
        }
        else
        {
            // mixed mode: aux buffer holds the per-row global IDs in parallel.
            var aux = MemoryMarshal.Cast<byte, int>(auxBytes.Span);
            for (var i = 0; i < count; i++) buffer.PutVarint(aux[i]);
        }
    }

    private void WriteTimestampColumn(
        IQwpBufferWriter buffer,
        ReadOnlyMemory<byte> dataMemory,
        int count,
        bool useGorilla)
    {
        if (useGorilla && count > 2)
        {
            var timestamps = MemoryMarshal.Cast<byte, long>(dataMemory.Span).Slice(0, count);
            var encodedSize = QwpGorillaEncoder.CalculateEncodedSizeIfSupported(timestamps);
            if (encodedSize >= 0)
            {
                buffer.PutByte(ENCODING_GORILLA);
                buffer.EnsureCapacity(encodedSize);
                var (array, offset) = GetWritableArray(buffer);
                var bytesWritten = _gorillaEncoder.EncodeTimestamps(array, offset, timestamps);
                buffer.Skip(bytesWritten);
                return;
            }
            buffer.PutByte(ENCODING_UNCOMPRESSED);
            buffer.PutBlockOfBytes(dataMemory.Slice(0, count * 8));
            return;
        }

        if (useGorilla) buffer.PutByte(ENCODING_UNCOMPRESSED);
        buffer.PutBlockOfBytes(dataMemory.Slice(0, count * 8));
    }

    private static void WriteDoubleArrayColumn(
        IQwpBufferWriter buffer,
        QwpTableBuffer.ColumnBuffer col,
        int count)
    {
        var dims = col.ArrayDimsSpan;
        var shapes = col.ArrayShapesSpan;
        var data = col.DoubleArrayDataSpan;

        var shapeIdx = 0;
        var dataIdx = 0;
        for (var row = 0; row < count; row++)
        {
            var nDims = dims[row];
            buffer.PutByte(nDims);

            var elemCount = 1;
            for (var d = 0; d < nDims; d++)
            {
                var dimLen = shapes[shapeIdx++];
                buffer.PutInt(dimLen);
                elemCount = checked(elemCount * dimLen);
            }
            for (var e = 0; e < elemCount; e++) buffer.PutDouble(data[dataIdx++]);
        }
    }

    private static void WriteLongArrayColumn(
        IQwpBufferWriter buffer,
        QwpTableBuffer.ColumnBuffer col,
        int count)
    {
        var dims = col.ArrayDimsSpan;
        var shapes = col.ArrayShapesSpan;
        var data = col.LongArrayDataSpan;

        var shapeIdx = 0;
        var dataIdx = 0;
        for (var row = 0; row < count; row++)
        {
            var nDims = dims[row];
            buffer.PutByte(nDims);

            var elemCount = 1;
            for (var d = 0; d < nDims; d++)
            {
                var dimLen = shapes[shapeIdx++];
                buffer.PutInt(dimLen);
                elemCount = checked(elemCount * dimLen);
            }
            for (var e = 0; e < elemCount; e++) buffer.PutLong(data[dataIdx++]);
        }
    }

    private static void WriteDecimal64Column(IQwpBufferWriter buffer, byte scale, ReadOnlySpan<byte> data, int count)
    {
        buffer.PutByte(scale);
        var longs = MemoryMarshal.Cast<byte, long>(data);
        for (var i = 0; i < count; i++) buffer.PutLong(longs[i]);
    }

    private static void WriteDecimal128Column(IQwpBufferWriter buffer, byte scale, ReadOnlySpan<byte> data, int count)
    {
        buffer.PutByte(scale);
        var longs = MemoryMarshal.Cast<byte, long>(data);
        for (var i = 0; i < count; i++)
        {
            // Storage order is (high, low); wire order swaps to (low, high).
            var hi = longs[i * 2];
            var lo = longs[i * 2 + 1];
            buffer.PutLong(lo);
            buffer.PutLong(hi);
        }
    }

    private static void WriteDecimal256Column(IQwpBufferWriter buffer, byte scale, ReadOnlySpan<byte> data, int count)
    {
        buffer.PutByte(scale);
        var longs = MemoryMarshal.Cast<byte, long>(data);
        for (var i = 0; i < count; i++)
        {
            var baseIdx = i * 4;
            // Wire order writes the four longs in reverse storage order (Java parity).
            buffer.PutLong(longs[baseIdx + 3]);
            buffer.PutLong(longs[baseIdx + 2]);
            buffer.PutLong(longs[baseIdx + 1]);
            buffer.PutLong(longs[baseIdx]);
        }
    }

    private static (byte[] array, int offset) GetWritableArray(IQwpBufferWriter buffer)
    {
        return buffer switch
        {
            QwpPinnedBufferWriter pinned => (pinned.UnderlyingArray, pinned.Position),
            QwpSegmentedBufferWriter segmented => (segmented.CurrentChunkUnderlyingArray, segmented.CurrentChunkPosition),
            _ => throw new InvalidOperationException(
                "QwpColumnWriter requires QwpPinnedBufferWriter or QwpSegmentedBufferWriter for in-place encoders"),
        };
    }
}
