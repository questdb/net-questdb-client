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
    }
}
