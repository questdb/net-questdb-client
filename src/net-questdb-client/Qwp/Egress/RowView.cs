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

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Reusable flyweight reader pivoted to a single row. The .NET counterpart of
///     Java's <c>RowView</c> on java-questdb-client main 64b7ee69. Internally
///     reuses a single <see cref="ColumnView"/> per access — the typed accessors
///     bind, fetch, and return.
/// </summary>
/// <remarks>
///     Experimental. Bind to a row via <see cref="BindToRow"/>, then call typed
///     accessors with column indices. The view is a flyweight valid only for the
///     duration of the per-row callback.
/// </remarks>
internal sealed class RowView
{
    private readonly ColumnView _scratch = new();
    private QwpColumnBatch? _batch;
    private int _rowIndex;

    public QwpColumnBatch? Batch => _batch;

    public int RowIndex => _rowIndex;

    public void BindToRow(QwpColumnBatch batch, int rowIndex)
    {
        _batch = batch ?? throw new ArgumentNullException(nameof(batch));
        _rowIndex = rowIndex;
    }

    public bool IsNull(int col) => _batch!.IsNull(col, _rowIndex);

    public bool GetBoolValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetBoolValue(_rowIndex);
    }

    public byte GetByteValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetByteValue(_rowIndex);
    }

    public short GetShortValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetShortValue(_rowIndex);
    }

    public char GetCharValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetCharValue(_rowIndex);
    }

    public int GetIntValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetIntValue(_rowIndex);
    }

    public long GetLongValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetLongValue(_rowIndex);
    }

    public float GetFloatValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetFloatValue(_rowIndex);
    }

    public double GetDoubleValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetDoubleValue(_rowIndex);
    }

    public long GetGeohashValue(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetGeohashValue(_rowIndex);
    }

    public string? GetString(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetString(_rowIndex);
    }

    public byte[]? GetBinary(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetBinary(_rowIndex);
    }

    // ---- PR 9d / 11 long tail (decimal128/256, long256, arrays, symbols, uuid) ----

    public long GetDecimal128High(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetDecimal128High(_rowIndex);
    }

    public long GetDecimal128Low(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetDecimal128Low(_rowIndex);
    }

    public long GetLong256Word(int col, int wordIndex)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetLong256Word(_rowIndex, wordIndex);
    }

    public int GetSymbolId(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetSymbolId(_rowIndex);
    }

    public string? GetSymbol(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetSymbol(_rowIndex);
    }

    public long GetUuidHi(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetUuidHi(_rowIndex);
    }

    public long GetUuidLo(int col)
    {
        _scratch.BindToColumn(_batch!, col);
        return _scratch.GetUuidLo(_rowIndex);
    }
}
