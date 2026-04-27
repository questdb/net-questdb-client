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

    // ---- PR 9d / 11 long tail (decimal128/256, long256, arrays, symbols, uuid) ----

    public long GetDecimal128High(int row) => throw NotYetImplemented(nameof(GetDecimal128High));
    public long GetDecimal128Low(int row) => throw NotYetImplemented(nameof(GetDecimal128Low));
    public long GetLong256Word(int row, int wordIndex) => throw NotYetImplemented(nameof(GetLong256Word));
    public double[] GetDoubleArrayElements(int row) => throw NotYetImplemented(nameof(GetDoubleArrayElements));
    public long[] GetLongArrayElements(int row) => throw NotYetImplemented(nameof(GetLongArrayElements));
    public int GetArrayNDims(int row) => throw NotYetImplemented(nameof(GetArrayNDims));
    public int GetSymbolId(int row) => throw NotYetImplemented(nameof(GetSymbolId));
    public string? GetSymbol(int row) => throw NotYetImplemented(nameof(GetSymbol));
    public long GetUuidHi(int row) => throw NotYetImplemented(nameof(GetUuidHi));
    public long GetUuidLo(int row) => throw NotYetImplemented(nameof(GetUuidLo));

    private static NotImplementedException NotYetImplemented(string member) =>
        new($"ColumnView.{member} not yet implemented (lands in PR 9d / 11)");
}
