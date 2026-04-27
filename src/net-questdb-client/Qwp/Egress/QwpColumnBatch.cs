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
///     Column-major view over an inbound RESULT_BATCH frame. The .NET counterpart of
///     Java's <c>QwpColumnBatch</c> on java-questdb-client main 64b7ee69 — PR 9c ships
///     the binding API (layouts list, row count, column lookup); PR 9d adds the wire
///     parser that populates the layouts from a freshly-received frame, plus
///     <c>ForEachRow</c> + <c>ForEachColumn</c> iteration.
/// </summary>
/// <remarks>
///     Experimental. The view is a flyweight valid only for the duration of the
///     handler callback that received it; copy out any values you need to retain.
/// </remarks>
internal sealed class QwpColumnBatch
{
    private readonly List<QwpColumnLayout> _columns = new();
    // Per-column ColumnView cache. Lazy — Column(idx) populates on first call. Slots
    // are reused across batches; the list grows but never shrinks so a wide-then-narrow
    // query sequence keeps the same instances.
    private readonly List<ColumnView?> _columnViews = new();
    private RowView? _rowView;
    private QwpBatchBuffer? _buffer;

    /// <summary>The batch buffer this view reads from. Set by the decoder before delivery.</summary>
    public QwpBatchBuffer? Buffer
    {
        get => _buffer;
        internal set => _buffer = value;
    }

    /// <summary>Number of rows in this batch.</summary>
    public int RowCount { get; internal set; }

    /// <summary>Number of columns in this batch.</summary>
    public int ColumnCount => _columns.Count;

    /// <summary>Returns the layout for the given column index.</summary>
    public QwpColumnLayout GetLayout(int columnIndex) => _columns[columnIndex];

    /// <summary>Direct access to the per-column layout list — used by the decoder during fill.</summary>
    internal List<QwpColumnLayout> Layouts => _columns;

    /// <summary>Read-only span over the batch's payload bytes (the underlying buffer).</summary>
    public ReadOnlySpan<byte> Payload =>
        _buffer is null ? ReadOnlySpan<byte>.Empty : _buffer.Payload;

    /// <summary>
    ///     Returns true when the row at <paramref name="rowIndex"/> is null in column
    ///     <paramref name="columnIndex"/>. Reads the bitmap LSB-first.
    /// </summary>
    public bool IsNull(int columnIndex, int rowIndex)
    {
        var layout = _columns[columnIndex];
        if (layout.NullBitmapOffset < 0) return false;
        var byteIdx = rowIndex >> 3;
        var bitIdx = rowIndex & 7;
        return (Payload[layout.NullBitmapOffset + byteIdx] & (1 << bitIdx)) != 0;
    }

    /// <summary>Resets the view for a fresh batch — called by the decoder before each fill.</summary>
    internal void Reset(QwpBatchBuffer buffer, int rowCount)
    {
        _buffer = buffer;
        RowCount = rowCount;
        // Cached ColumnView instances stay; their next Column() lookup will call
        // BindToColumn which refreshes the layout pointer in place. Reusing the
        // instances matches the Java behaviour and avoids per-batch GC churn.
    }

    /// <summary>
    ///     Returns a <see cref="ColumnView"/> pinned to <paramref name="columnIndex"/>.
    ///     Views are cached per index — calling <c>Column(0)</c> + <c>Column(1)</c>
    ///     yields two distinct instances safe to hold side-by-side. A second call for
    ///     the same index returns the same instance with its layout pointer refreshed.
    /// </summary>
    public ColumnView Column(int columnIndex)
    {
        while (_columnViews.Count <= columnIndex) _columnViews.Add(null);
        var view = _columnViews[columnIndex];
        if (view is null)
        {
            view = new ColumnView();
            _columnViews[columnIndex] = view;
        }
        view.BindToColumn(this, columnIndex);
        return view;
    }

    /// <summary>Returns the lazily-allocated <see cref="RowView"/> bound to <paramref name="rowIndex"/>.</summary>
    public RowView Row(int rowIndex)
    {
        _rowView ??= new RowView();
        _rowView.BindToRow(this, rowIndex);
        return _rowView;
    }

    /// <summary>
    ///     Iterates rows in this batch and invokes <paramref name="callback"/> for each.
    ///     The <see cref="RowView"/> is a flyweight reused across iterations — do not
    ///     retain it past the call. Throwing from the callback aborts iteration and
    ///     propagates the exception out of <c>ForEachRow</c>.
    /// </summary>
    public void ForEachRow(RowCallback callback)
    {
        _rowView ??= new RowView();
        for (var r = 0; r < RowCount; r++)
        {
            _rowView.BindToRow(this, r);
            callback(_rowView);
        }
    }

    /// <summary>Returns the wire-type byte for <paramref name="columnIndex"/>.</summary>
    public byte GetColumnWireType(int columnIndex) =>
        _columns[columnIndex].Info?.WireType ?? 0;

    // ---- Column-major (col, row) convenience pass-throughs ----

    public bool GetBoolValue(int col, int row) => Column(col).GetBoolValue(row);
    public byte GetByteValue(int col, int row) => Column(col).GetByteValue(row);
    public short GetShortValue(int col, int row) => Column(col).GetShortValue(row);
    public char GetCharValue(int col, int row) => Column(col).GetCharValue(row);
    public int GetIntValue(int col, int row) => Column(col).GetIntValue(row);
    public long GetLongValue(int col, int row) => Column(col).GetLongValue(row);
    public float GetFloatValue(int col, int row) => Column(col).GetFloatValue(row);
    public double GetDoubleValue(int col, int row) => Column(col).GetDoubleValue(row);
    public string? GetString(int col, int row) => Column(col).GetString(row);
    public byte[]? GetBinary(int col, int row) => Column(col).GetBinary(row);
}
