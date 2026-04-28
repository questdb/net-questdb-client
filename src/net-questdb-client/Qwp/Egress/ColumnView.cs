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
///     Reusable flyweight reader over a single column of a <see cref="QwpColumnBatch"/>.
///     The .NET counterpart of Java's <c>ColumnView</c> on java-questdb-client main
///     64b7ee69 — PR 9c ships typed accessors for the simple fixed-width types and the
///     varchar/binary/string accessors; the long tail (decimals, long256, arrays,
///     symbols-with-delta-dict) is parked behind <see cref="NotImplementedException"/>
///     until PR 9d / PR 11 wire it.
/// </summary>
/// <remarks>
///     Experimental. Bind to a column via <see cref="BindToColumn"/>, then call the
///     typed accessor matching the column's wire type. Always check
///     <see cref="IsNull"/> first; the typed accessors return zero / default for
///     null rows but won't reflect the absence.
/// </remarks>
internal sealed class ColumnView
{
    private QwpColumnBatch? _batch;
    private int _columnIndex;
    private QwpColumnLayout? _layout;

    public QwpColumnBatch? Batch => _batch;

    public int ColumnIndex => _columnIndex;

    public byte ColumnWireType => _layout?.Info?.WireType ?? 0;

    /// <summary>Binds the view to <paramref name="columnIndex"/> within <paramref name="batch"/>.</summary>
    public void BindToColumn(QwpColumnBatch batch, int columnIndex)
    {
        _batch = batch ?? throw new ArgumentNullException(nameof(batch));
        _columnIndex = columnIndex;
        _layout = batch.GetLayout(columnIndex);
    }

    public bool IsNull(int row) => _batch!.IsNull(_columnIndex, row);

    public bool GetBoolValue(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return _batch!.Payload[_layout.ValuesOffset + idx] != 0;
    }

    public byte GetByteValue(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return _batch!.Payload[_layout.ValuesOffset + idx];
    }

    public short GetShortValue(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadInt16LittleEndian(_batch!.Payload.Slice(_layout.ValuesOffset + idx * 2, 2));
    }

    public char GetCharValue(int row) => (char)GetShortValue(row);

    public int GetIntValue(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadInt32LittleEndian(_batch!.Payload.Slice(_layout.ValuesOffset + idx * 4, 4));
    }

    public long GetLongValue(int row)
    {
        var idx = _layout!.DenseIndex(row);
        // §3.2b — Gorilla-decoded timestamps live in a managed buffer instead of
        // the payload; ValuesOffset == -1 is the sentinel.
        if (_layout.ValuesOffset < 0 && _layout.TimestampDecodeBuffer is not null)
        {
            return BinaryPrimitives.ReadInt64LittleEndian(
                _layout.TimestampDecodeBuffer.AsSpan(idx * 8, 8));
        }
        return BinaryPrimitives.ReadInt64LittleEndian(_batch!.Payload.Slice(_layout.ValuesOffset + idx * 8, 8));
    }

    public float GetFloatValue(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadSingleLittleEndian(_batch!.Payload.Slice(_layout.ValuesOffset + idx * 4, 4));
    }

    public double GetDoubleValue(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadDoubleLittleEndian(_batch!.Payload.Slice(_layout.ValuesOffset + idx * 8, 8));
    }

    public long GetGeohashValue(int row) => GetLongValue(row);

    /// <summary>
    ///     Returns the VARCHAR string for <paramref name="row"/>, or null if the row is
    ///     null. The dense-index → cumulative-offset lookup is the same shape the
    ///     encoder uses on the write side.
    /// </summary>
    public string? GetString(int row)
    {
        if (IsNull(row)) return null;
        var idx = _layout!.DenseIndex(row);
        var payload = _batch!.Payload;
        var offsetsBase = _layout.ValuesOffset;
        var startOffset = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offsetsBase + idx * 4, 4));
        var endOffset = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offsetsBase + (idx + 1) * 4, 4));
        var length = endOffset - startOffset;
        if (length == 0) return string.Empty;
        return Encoding.UTF8.GetString(payload.Slice(_layout.StringBytesOffset + startOffset, length));
    }

    /// <summary>Returns the raw VARCHAR / BINARY bytes for the row, copied into a managed array.</summary>
    public byte[]? GetBinary(int row)
    {
        if (IsNull(row)) return null;
        var idx = _layout!.DenseIndex(row);
        var payload = _batch!.Payload;
        var offsetsBase = _layout.ValuesOffset;
        var startOffset = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offsetsBase + idx * 4, 4));
        var endOffset = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offsetsBase + (idx + 1) * 4, 4));
        return payload.Slice(_layout.StringBytesOffset + startOffset, endOffset - startOffset).ToArray();
    }

    // ---- §3.1 — accessors for the long-tail wire types (decimal128/256, long256,
    // arrays, symbols, uuid). Wired against the layouts populated by §3.2.b/c/d/e/f. ----

    /// <summary>
    ///     Returns the high 64 bits of a Decimal128 row. Storage layout is (hi, lo) so
    ///     the high half sits at the row's first 8 bytes; <see cref="GetDecimal128Low"/>
    ///     reads the second 8.
    /// </summary>
    public long GetDecimal128High(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadInt64LittleEndian(
            _batch!.Payload.Slice(_layout.ValuesOffset + idx * 16, 8));
    }

    public long GetDecimal128Low(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadInt64LittleEndian(
            _batch!.Payload.Slice(_layout.ValuesOffset + idx * 16 + 8, 8));
    }

    /// <summary>
    ///     Returns one 64-bit word of a LONG256 row. <paramref name="wordIndex"/> is in
    ///     [0, 4). The wire stores the four words in storage order; consumers needing
    ///     the full 256-bit value walk index 0..3.
    /// </summary>
    public long GetLong256Word(int row, int wordIndex)
    {
        if ((uint)wordIndex >= 4)
        {
            throw new ArgumentOutOfRangeException(nameof(wordIndex), "must be in [0, 4)");
        }
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadInt64LittleEndian(
            _batch!.Payload.Slice(_layout.ValuesOffset + idx * 32 + wordIndex * 8, 8));
    }

    /// <summary>UUID high 64 bits (the second 8 bytes of the row's 16-byte payload).</summary>
    public long GetUuidHi(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadInt64LittleEndian(
            _batch!.Payload.Slice(_layout.ValuesOffset + idx * 16 + 8, 8));
    }

    /// <summary>UUID low 64 bits (the first 8 bytes of the row's 16-byte payload).</summary>
    public long GetUuidLo(int row)
    {
        var idx = _layout!.DenseIndex(row);
        return BinaryPrimitives.ReadInt64LittleEndian(
            _batch!.Payload.Slice(_layout.ValuesOffset + idx * 16, 8));
    }

    /// <summary>
    ///     Returns the SYMBOL local-dict id for the row, or -1 when the row is null.
    ///     The id resolves against the column's local dict (or, in delta mode, the
    ///     decoder's connection-scoped dict aliased through
    ///     <see cref="QwpColumnLayout.SymbolEntriesBuffer"/>) — see <see cref="GetSymbol"/>
    ///     for the resolved string.
    /// </summary>
    public int GetSymbolId(int row)
    {
        if (IsNull(row)) return -1;
        return _layout!.SymbolRowIds![row];
    }

    /// <summary>
    ///     Resolves a SYMBOL row to its UTF-8 string. Returns null on null rows.
    ///     Dispatches on <see cref="QwpColumnLayout.SymbolHeapBuffer"/>: when non-null
    ///     (delta mode), reads from the decoder's connection-scoped dict; when null
    ///     (non-delta), reads inline from the payload at
    ///     <see cref="QwpColumnLayout.SymbolDictHeapOffset"/>.
    /// </summary>
    public string? GetSymbol(int row)
    {
        if (IsNull(row)) return null;
        var id = _layout!.SymbolRowIds![row];
        // Packed entry: low 32 = offset, high 32 = length.
        var entriesBuffer = _layout.SymbolEntriesBuffer
            ?? throw new InvalidOperationException("SYMBOL column has no entries buffer");
        var packed = BinaryPrimitives.ReadInt64LittleEndian(entriesBuffer.AsSpan(id * 8, 8));
        var offset = (int)(uint)packed;
        var length = (int)(packed >> 32);
        if (length == 0) return string.Empty;

        if (_layout.SymbolHeapBuffer is { } heap)
        {
            // Delta-mode: heap aliases the decoder's connection dict.
            return Encoding.UTF8.GetString(heap.AsSpan(offset, length));
        }
        // Non-delta: heap is inline in the payload.
        return Encoding.UTF8.GetString(
            _batch!.Payload.Slice(_layout.SymbolDictHeapOffset + offset, length));
    }

    /// <summary>Returns the rank (number of dimensions) of the array at <paramref name="row"/>.</summary>
    public int GetArrayNDims(int row)
    {
        if (IsNull(row)) return 0;
        var rowOffset = _layout!.ArrayRowOffsets![row];
        return _batch!.Payload[rowOffset];
    }

    /// <summary>
    ///     Returns the row's array elements as a freshly-allocated <c>double[]</c>
    ///     (flattened by C-order traversal). Returns an empty array on null rows.
    /// </summary>
    public double[] GetDoubleArrayElements(int row)
    {
        if (IsNull(row)) return Array.Empty<double>();
        var rowOffset = _layout!.ArrayRowOffsets![row];
        var rowLength = _layout.ArrayRowLengths![row];
        if (rowLength == 0) return Array.Empty<double>();
        var payload = _batch!.Payload;
        var nDims = payload[rowOffset];
        var elementsStart = rowOffset + 1 + 4 * nDims;
        var elementsBytes = rowOffset + rowLength - elementsStart;
        var count = elementsBytes / 8;
        var arr = new double[count];
        for (var i = 0; i < count; i++)
        {
            arr[i] = BinaryPrimitives.ReadDoubleLittleEndian(payload.Slice(elementsStart + i * 8, 8));
        }
        return arr;
    }

    /// <summary>
    ///     Returns the row's array elements as a freshly-allocated <c>long[]</c>.
    ///     Same flattening contract as <see cref="GetDoubleArrayElements"/>.
    /// </summary>
    public long[] GetLongArrayElements(int row)
    {
        if (IsNull(row)) return Array.Empty<long>();
        var rowOffset = _layout!.ArrayRowOffsets![row];
        var rowLength = _layout.ArrayRowLengths![row];
        if (rowLength == 0) return Array.Empty<long>();
        var payload = _batch!.Payload;
        var nDims = payload[rowOffset];
        var elementsStart = rowOffset + 1 + 4 * nDims;
        var elementsBytes = rowOffset + rowLength - elementsStart;
        var count = elementsBytes / 8;
        var arr = new long[count];
        for (var i = 0; i < count; i++)
        {
            arr[i] = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(elementsStart + i * 8, 8));
        }
        return arr;
    }
}
