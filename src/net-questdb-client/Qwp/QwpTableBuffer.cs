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

using System.Numerics;
using System.Text;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Per-table columnar buffer used by the WebSocket sender.
/// </summary>
/// <remarks>
///     Owns an ordered list of <see cref="QwpColumn" /> instances, the table's row count, and the
///     schema id slot used for the cached vs. full schema decision.
///     <para />
///     Per-row state machine:
///     <list type="number">
///         <item>User code calls <c>AppendXxx</c> to set values for the current row, in any order.</item>
///         <item>User code calls one of the <c>At*</c> methods to commit the row. Untouched columns are
///               null-padded; the designated-timestamp column receives the supplied timestamp.</item>
///     </list>
///     <para />
///     A <see cref="SchemaId" /> of <c>-1</c> means "needs allocation on next flush". The id is reset
///     to <c>-1</c> whenever a new column is added, forcing the next frame to send the schema in
///     full mode.
/// </remarks>
internal sealed class QwpTableBuffer
{
    private readonly Dictionary<string, int> _columnIndex = new(StringComparer.Ordinal);
    private readonly List<QwpColumn> _columns = new();

    private bool[] _touchedInCurrentRow = Array.Empty<bool>();

    /// <summary>
    ///     Constructs a new empty buffer.
    /// </summary>
    /// <param name="tableName">UTF-8 byte length must be ≤ <see cref="QwpConstants.MaxNameLengthBytes" />.</param>
    /// <param name="maxNameLengthBytes">Override for the name-length limit; defaults to the spec value.</param>
    public QwpTableBuffer(string tableName, int maxNameLengthBytes = QwpConstants.MaxNameLengthBytes)
    {
        if (string.IsNullOrEmpty(tableName))
        {
            throw new IngressError(ErrorCode.InvalidName, "table name must not be empty");
        }

        var nameByteCount = Encoding.UTF8.GetByteCount(tableName);
        if (nameByteCount > maxNameLengthBytes)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"table name exceeds {maxNameLengthBytes} UTF-8 bytes (got {nameByteCount})");
        }

        TableName = tableName;
    }

    /// <summary>Table name as it appears on the wire.</summary>
    public string TableName { get; }

    /// <summary>
    ///     Number of fully-committed rows. Increments on each <c>At*</c> call.
    /// </summary>
    public int RowCount { get; private set; }

    /// <summary>
    ///     Per-connection schema id; <c>-1</c> means "not yet allocated". The encoder assigns
    ///     a fresh id on the next flush when this is <c>-1</c>, and emits the schema in full mode
    ///     (otherwise reference mode is used).
    /// </summary>
    public int SchemaId { get; set; } = -1;

    /// <summary>
    ///     User-declared data columns in declaration order. The designated-timestamp column is
    ///     <em>not</em> included here; access it via <see cref="DesignatedTimestampColumn" />.
    /// </summary>
    public IReadOnlyList<QwpColumn> Columns => _columns;

    /// <summary>
    ///     The designated-timestamp column (empty wire name + TIMESTAMP / TIMESTAMP_NANOS type),
    ///     or <c>null</c> if no <c>At*</c> has been called yet. Always emitted last when encoding.
    /// </summary>
    public QwpColumn? DesignatedTimestampColumn { get; private set; }

    /// <summary>Total column count, including the designated-timestamp column if present.</summary>
    public int TotalColumnCount => _columns.Count + (DesignatedTimestampColumn is null ? 0 : 1);

    /// <summary>True when at least one column has been touched in the current row.</summary>
    public bool HasPendingRow { get; private set; }

    // -- Append API for non-designated columns -----------------------------------

    /// <summary>Append a boolean value to the named column.</summary>
    public void AppendBool(string columnName, bool value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendBool(value);
    }

    /// <summary>Append a signed byte.</summary>
    public void AppendByte(string columnName, sbyte value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendByte(value);
    }

    /// <summary>Append a 16-bit signed integer.</summary>
    public void AppendShort(string columnName, short value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendShort(value);
    }

    /// <summary>Append a 32-bit signed integer.</summary>
    public void AppendInt(string columnName, int value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendInt(value);
    }

    /// <summary>Append a 64-bit signed integer.</summary>
    public void AppendLong(string columnName, long value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendLong(value);
    }

    /// <summary>Append a single-precision float.</summary>
    public void AppendFloat(string columnName, float value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendFloat(value);
    }

    /// <summary>Append a double-precision float.</summary>
    public void AppendDouble(string columnName, double value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendDouble(value);
    }

    /// <summary>Append a TIMESTAMP value (microseconds since epoch) to a non-designated column.</summary>
    public void AppendTimestampMicros(string columnName, long micros)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendTimestampMicros(micros);
    }

    /// <summary>Append a TIMESTAMP_NANOS value (nanoseconds since epoch) to a non-designated column.</summary>
    public void AppendTimestampNanos(string columnName, long nanos)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendTimestampNanos(nanos);
    }

    /// <summary>Append a DATE value (milliseconds since epoch).</summary>
    public void AppendDateMillis(string columnName, long millis)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendDateMillis(millis);
    }

    /// <summary>Append a UUID.</summary>
    public void AppendUuid(string columnName, Guid value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendUuid(value);
    }

    /// <summary>Append a single UTF-16 code unit.</summary>
    public void AppendChar(string columnName, char value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendChar(value);
    }

    /// <summary>Append a length-prefixed UTF-8 string.</summary>
    public void AppendVarchar(string columnName, ReadOnlySpan<char> value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendVarchar(value);
    }

    /// <summary>Append a SYMBOL value as a global dictionary id.</summary>
    public void AppendSymbol(string columnName, int globalId)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendSymbol(globalId);
    }

    /// <summary>Append a DECIMAL128 value. The first call locks the column scale.</summary>
    public void AppendDecimal128(string columnName, decimal value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendDecimal128(value);
    }

    /// <summary>Append a non-negative LONG256 value (≤ 256 bits).</summary>
    public void AppendLong256(string columnName, BigInteger value)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendLong256(value);
    }

    /// <summary>Append a GEOHASH value. The first call locks the column precision (in bits).</summary>
    public void AppendGeohash(string columnName, ulong hash, int precisionBits)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendGeohash(hash, precisionBits);
    }

    /// <summary>Append a DOUBLE_ARRAY row with the given shape.</summary>
    public void AppendDoubleArray(string columnName, ReadOnlySpan<double> values, ReadOnlySpan<int> shape)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendDoubleArray(values, shape);
    }

    /// <summary>Append a LONG_ARRAY row with the given shape.</summary>
    public void AppendLongArray(string columnName, ReadOnlySpan<long> values, ReadOnlySpan<int> shape)
    {
        var col = GetOrCreateColumn(columnName);
        col.AppendLongArray(values, shape);
    }

    // -- Reset -------------------------------------------------------------------

    /// <summary>
    ///     Drops row data while preserving the table's name, columns, and schema id. Used by the
    ///     sender to recycle the buffer between batches without losing the cached schema id —
    ///     subsequent encodes will emit <see cref="QwpConstants.SchemaModeReference" />.
    /// </summary>
    public void Clear()
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            _columns[i].Clear();
        }

        DesignatedTimestampColumn?.Clear();

        RowCount = 0;
        HasPendingRow = false;

        if (_touchedInCurrentRow.Length > 0)
        {
            Array.Clear(_touchedInCurrentRow, 0, _touchedInCurrentRow.Length);
        }
    }

    // -- Row finalisers ----------------------------------------------------------

    /// <summary>
    ///     Commit the current row with a TIMESTAMP (microseconds-since-epoch) designated value.
    /// </summary>
    public void At(long timestampMicros)
    {
        var ts = EnsureDesignatedTimestampColumn();
        ts.AppendTimestampMicros(timestampMicros);
        FinaliseRow();
    }

    /// <summary>
    ///     Commit the current row with a TIMESTAMP_NANOS (nanoseconds-since-epoch) designated value.
    /// </summary>
    public void AtNanos(long timestampNanos)
    {
        var ts = EnsureDesignatedTimestampColumn();
        ts.AppendTimestampNanos(timestampNanos);
        FinaliseRow();
    }

    // -- Internal helpers --------------------------------------------------------

    /// <summary>
    ///     Look up an existing column or create a new one.
    /// </summary>
    /// <remarks>
    ///     A new column resets <see cref="SchemaId" /> to -1, which forces the next encoded frame to
    ///     emit a fresh full-schema block. The new column is back-filled with nulls for the
    ///     <see cref="RowCount" /> rows that came before it.
    /// </remarks>
    private QwpColumn GetOrCreateColumn(string columnName)
    {
        if (columnName is null)
        {
            throw new ArgumentNullException(nameof(columnName));
        }

        // The designated-timestamp slot uses the empty string; no user data column may share the slot.
        if (columnName.Length == 0)
        {
            throw new IngressError(ErrorCode.InvalidName, "column name must not be empty");
        }

        if (_columnIndex.TryGetValue(columnName, out var idx))
        {
            MarkTouched(idx);
            return _columns[idx];
        }

        var nameByteCount = Encoding.UTF8.GetByteCount(columnName);
        if (nameByteCount > QwpConstants.MaxNameLengthBytes)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"column name exceeds {QwpConstants.MaxNameLengthBytes} UTF-8 bytes (got {nameByteCount})");
        }

        if (_columns.Count >= QwpConstants.MaxColumnsPerTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"table '{TableName}' exceeds the {QwpConstants.MaxColumnsPerTable}-column limit");
        }

        var col = new QwpColumn(columnName, RowCount);
        idx = _columns.Count;
        _columns.Add(col);
        _columnIndex[columnName] = idx;
        SchemaId = -1; // adding a column invalidates any cached schema id.

        EnsureTouchedCapacity(idx + 1);
        MarkTouched(idx);

        return col;
    }

    /// <summary>
    /// 
    ///     Lazily creates the designated-timestamp column. The first <c>AppendTimestamp*</c> call
    ///     on the returned column locks its type code (TIMESTAMP vs. TIMESTAMP_NANOS); subsequent
    ///     mismatched calls throw via <see cref="QwpColumn" />'s type guard.
    /// </summary>
    private QwpColumn EnsureDesignatedTimestampColumn()
    {
        if (DesignatedTimestampColumn is null)
        {
            DesignatedTimestampColumn = new QwpColumn(string.Empty, RowCount);
            SchemaId = -1;
        }

        return DesignatedTimestampColumn;
    }

    private void FinaliseRow()
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            if (!_touchedInCurrentRow[i])
            {
                _columns[i].AppendNull();
            }

            _touchedInCurrentRow[i] = false;
        }

        RowCount++;
        HasPendingRow = false;

        if (RowCount > QwpConstants.MaxRowsPerTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"table '{TableName}' exceeds the {QwpConstants.MaxRowsPerTable}-row limit");
        }
    }

    private void MarkTouched(int columnIndex)
    {
        EnsureTouchedCapacity(columnIndex + 1);
        _touchedInCurrentRow[columnIndex] = true;
        HasPendingRow = true;
    }

    private void EnsureTouchedCapacity(int required)
    {
        if (_touchedInCurrentRow.Length >= required)
        {
            return;
        }

        var newSize = Math.Max(8, _touchedInCurrentRow.Length);
        while (newSize < required)
        {
            newSize *= 2;
        }

        Array.Resize(ref _touchedInCurrentRow, newSize);
    }
}
