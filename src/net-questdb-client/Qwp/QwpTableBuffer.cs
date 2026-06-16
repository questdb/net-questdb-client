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

internal static class QwpStrictUtf8
{
    internal static readonly UTF8Encoding Encoding =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
}

/// <summary>
///     Per-table columnar buffer used by the WebSocket sender.
/// </summary>
/// <remarks>
///     Owns an ordered list of <see cref="QwpColumn" /> instances and the table's row count.
///     <para />
///     Per-row state machine:
///     <list type="number">
///         <item>User code calls <c>AppendXxx</c> to set values for the current row, in any order.</item>
///         <item>User code calls one of the <c>At*</c> methods to commit the row. Untouched columns are
///               null-padded; the designated-timestamp column receives the supplied timestamp.</item>
///     </list>
///     <para />
///     Schemas always travel inline on the wire (no schema-id reference mechanism), so the buffer
///     no longer carries a schema-id slot.
/// </remarks>
internal sealed class QwpTableBuffer
{
    private readonly Dictionary<string, int> _columnIndex = new(StringComparer.OrdinalIgnoreCase);
#if NET9_0_OR_GREATER
    private readonly Dictionary<string, int>.AlternateLookup<ReadOnlySpan<char>> _columnIndexLookup;
#endif
    private readonly List<QwpColumn> _columns = new();
    private readonly int _maxNameLengthBytes;

    private bool[] _touchedInCurrentRow = new bool[8];

    private int _committedColumnCount;
    private QwpColumn.Savepoint[] _rowSavepoints = new QwpColumn.Savepoint[8];
    private QwpColumn.Savepoint? _designatedSavepoint;
    private bool _designatedCreatedInCurrentRow;

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

        int nameByteCount;
        try
        {
            nameByteCount = QwpStrictUtf8.Encoding.GetByteCount(tableName);
        }
        catch (EncoderFallbackException ex)
        {
            throw new IngressError(ErrorCode.InvalidName,
                "table name is not valid UTF-8 (lone surrogate)", ex);
        }
        if (nameByteCount > maxNameLengthBytes)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"table name exceeds {maxNameLengthBytes} UTF-8 bytes (got {nameByteCount})");
        }

        TableName = tableName;
        _maxNameLengthBytes = maxNameLengthBytes;
#if NET9_0_OR_GREATER
        _columnIndexLookup = _columnIndex.GetAlternateLookup<ReadOnlySpan<char>>();
#endif
    }

    /// <summary>Table name as it appears on the wire.</summary>
    public string TableName { get; }

    /// <summary>
    ///     Number of fully-committed rows. Increments on each <c>At*</c> call.
    /// </summary>
    public int RowCount { get; private set; }

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

    /// <summary>Raw bytes accumulated across every column's backing buffers, including the designated timestamp.</summary>
    public long GetBufferedBytes()
    {
        long bytes = 0;
        for (var i = 0; i < _columns.Count; i++)
        {
            bytes += _columns[i].BufferedBytes;
        }

        if (DesignatedTimestampColumn is not null)
        {
            bytes += DesignatedTimestampColumn.BufferedBytes;
        }

        return bytes;
    }

    /// <summary>Append a boolean value to the named column.</summary>
    public void AppendBool(ReadOnlySpan<char> columnName, bool value)
    {
        try { GetOrCreateColumn(columnName)?.AppendBool(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a signed byte.</summary>
    public void AppendByte(ReadOnlySpan<char> columnName, sbyte value)
    {
        try { GetOrCreateColumn(columnName)?.AppendByte(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a 16-bit signed integer.</summary>
    public void AppendShort(ReadOnlySpan<char> columnName, short value)
    {
        try { GetOrCreateColumn(columnName)?.AppendShort(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a 32-bit signed integer.</summary>
    public void AppendInt(ReadOnlySpan<char> columnName, int value)
    {
        try { GetOrCreateColumn(columnName)?.AppendInt(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a 64-bit signed integer.</summary>
    public void AppendLong(ReadOnlySpan<char> columnName, long value)
    {
        try { GetOrCreateColumn(columnName)?.AppendLong(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a single-precision float.</summary>
    public void AppendFloat(ReadOnlySpan<char> columnName, float value)
    {
        try { GetOrCreateColumn(columnName)?.AppendFloat(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a double-precision float.</summary>
    public void AppendDouble(ReadOnlySpan<char> columnName, double value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDouble(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a TIMESTAMP value (microseconds since epoch) to a non-designated column.</summary>
    public void AppendTimestampMicros(ReadOnlySpan<char> columnName, long micros)
    {
        try { GetOrCreateColumn(columnName)?.AppendTimestampMicros(micros); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a TIMESTAMP_NANOS value (nanoseconds since epoch) to a non-designated column.</summary>
    public void AppendTimestampNanos(ReadOnlySpan<char> columnName, long nanos)
    {
        try { GetOrCreateColumn(columnName)?.AppendTimestampNanos(nanos); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DATE value (milliseconds since epoch).</summary>
    public void AppendDateMillis(ReadOnlySpan<char> columnName, long millis)
    {
        try { GetOrCreateColumn(columnName)?.AppendDateMillis(millis); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a UUID.</summary>
    public void AppendUuid(ReadOnlySpan<char> columnName, Guid value)
    {
        try { GetOrCreateColumn(columnName)?.AppendUuid(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a single UTF-16 code unit.</summary>
    public void AppendChar(ReadOnlySpan<char> columnName, char value)
    {
        try { GetOrCreateColumn(columnName)?.AppendChar(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a length-prefixed UTF-8 string.</summary>
    public void AppendVarchar(ReadOnlySpan<char> columnName, ReadOnlySpan<char> value)
    {
        try { GetOrCreateColumn(columnName)?.AppendVarchar(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a SYMBOL value as a global dictionary id.</summary>
    public void AppendSymbol(ReadOnlySpan<char> columnName, int globalId)
    {
        try { GetOrCreateColumn(columnName)?.AppendSymbol(globalId); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DECIMAL64 value. The first call locks the column scale.</summary>
    public void AppendDecimal64(ReadOnlySpan<char> columnName, decimal value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal64(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DECIMAL128 value. The first call locks the column scale.</summary>
    public void AppendDecimal128(ReadOnlySpan<char> columnName, decimal value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal128(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DECIMAL256 value. The first call locks the column scale.</summary>
    public void AppendDecimal256(ReadOnlySpan<char> columnName, decimal value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal256(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DECIMAL64 value as the unscaled int64 with explicit scale.</summary>
    public void AppendDecimal64(ReadOnlySpan<char> columnName, long unscaledValue, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal64(unscaledValue, scale); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DECIMAL128 value: <c>lo</c> = unsigned low 64 bits, <c>hi</c> = signed high 64 bits.</summary>
    public void AppendDecimal128(ReadOnlySpan<char> columnName, long lo, long hi, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal128(lo, hi, scale); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DECIMAL256 value: <c>l0</c>–<c>l2</c> unsigned low limbs, <c>l3</c> signed high limb.</summary>
    public void AppendDecimal256(ReadOnlySpan<char> columnName, long l0, long l1, long l2, long l3, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal256(l0, l1, l2, l3, scale); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a BINARY value as opaque bytes (no UTF-8 contract).</summary>
    public void AppendBinary(ReadOnlySpan<char> columnName, ReadOnlySpan<byte> value)
    {
        try { GetOrCreateColumn(columnName)?.AppendBinary(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append an IPv4 address as 4 bytes little-endian.</summary>
    public void AppendIPv4(ReadOnlySpan<char> columnName, uint addr)
    {
        try { GetOrCreateColumn(columnName)?.AppendIPv4(addr); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a non-negative LONG256 value (≤ 256 bits).</summary>
    public void AppendLong256(ReadOnlySpan<char> columnName, BigInteger value)
    {
        try { GetOrCreateColumn(columnName)?.AppendLong256(value); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a GEOHASH value. The first call locks the column precision (in bits).</summary>
    public void AppendGeohash(ReadOnlySpan<char> columnName, ulong hash, int precisionBits)
    {
        try { GetOrCreateColumn(columnName)?.AppendGeohash(hash, precisionBits); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a DOUBLE_ARRAY row with the given shape.</summary>
    public void AppendDoubleArray(ReadOnlySpan<char> columnName, ReadOnlySpan<double> values, ReadOnlySpan<int> shape)
    {
        try { GetOrCreateColumn(columnName)?.AppendDoubleArray(values, shape); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>Append a LONG_ARRAY row with the given shape.</summary>
    public void AppendLongArray(ReadOnlySpan<char> columnName, ReadOnlySpan<long> values, ReadOnlySpan<int> shape)
    {
        try { GetOrCreateColumn(columnName)?.AppendLongArray(values, shape); } catch { CancelCurrentRow(); throw; }
    }

    /// <summary>
    ///     Drops row data while preserving the table's name and column declarations. Used by the
    ///     sender to recycle the buffer between batches.
    /// </summary>
    public void Clear()
    {
        if (HasPendingRow)
        {
            CancelCurrentRow();
        }

        for (var i = 0; i < _columns.Count; i++)
        {
            _columns[i].Clear();
        }

        DesignatedTimestampColumn?.Clear();

        RowCount = 0;
        HasPendingRow = false;
        _committedColumnCount = _columns.Count;
        _designatedSavepoint = null;

        if (_touchedInCurrentRow.Length > 0)
        {
            Array.Clear(_touchedInCurrentRow, 0, _touchedInCurrentRow.Length);
        }
    }

    public void TrimToCurrent()
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            _columns[i].TrimToCurrent();
        }

        DesignatedTimestampColumn?.TrimToCurrent();
    }

    /// <summary>
    ///     Commit the current row with a TIMESTAMP (microseconds-since-epoch) designated value.
    /// </summary>
    public void At(long timestampMicros)
    {
        try
        {
            EnsureCanAppendRow();
            var ts = EnsureDesignatedTimestampColumn();
            _designatedSavepoint ??= ts.Snapshot();
            ts.AppendTimestampMicros(timestampMicros);
            FinaliseRow();
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>
    ///     Commit the current row with a TIMESTAMP_NANOS (nanoseconds-since-epoch) designated value.
    /// </summary>
    public void AtNanos(long timestampNanos)
    {
        try
        {
            EnsureCanAppendRow();
            var ts = EnsureDesignatedTimestampColumn();
            _designatedSavepoint ??= ts.Snapshot();
            ts.AppendTimestampNanos(timestampNanos);
            FinaliseRow();
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    private void EnsureCanAppendRow()
    {
        if (RowCount >= QwpConstants.MaxRowsPerTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"table '{TableName}' exceeds the {QwpConstants.MaxRowsPerTable}-row limit");
        }
    }

    /// <summary>
    ///     Look up an existing column or create a new one.
    /// </summary>
    /// <remarks>
    ///     The new column is back-filled with nulls for the <see cref="RowCount" /> rows that came before it.
    /// </remarks>
    private QwpColumn? GetOrCreateColumn(ReadOnlySpan<char> columnName)
    {
        if (columnName.Length == 0)
        {
            throw new IngressError(ErrorCode.InvalidName, "column name must not be empty");
        }

        int idx;
#if NET9_0_OR_GREATER
        if (_columnIndexLookup.TryGetValue(columnName, out idx))
        {
            SnapshotOnFirstTouch(idx, _columns[idx]);
            return MarkTouched(idx) ? _columns[idx] : null;
        }
#else
        var probeKey = columnName.ToString();
        if (_columnIndex.TryGetValue(probeKey, out idx))
        {
            SnapshotOnFirstTouch(idx, _columns[idx]);
            return MarkTouched(idx) ? _columns[idx] : null;
        }
#endif

        int nameByteCount;
        try
        {
            nameByteCount = QwpStrictUtf8.Encoding.GetByteCount(columnName);
        }
        catch (EncoderFallbackException ex)
        {
            throw new IngressError(ErrorCode.InvalidName,
                "column name is not valid UTF-8 (lone surrogate)", ex);
        }
        if (nameByteCount > _maxNameLengthBytes)
        {
            throw new IngressError(ErrorCode.InvalidName,
                $"column name exceeds {_maxNameLengthBytes} UTF-8 bytes (got {nameByteCount})");
        }

        if (_columns.Count >= QwpConstants.MaxColumnsPerTable)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"table '{TableName}' exceeds the {QwpConstants.MaxColumnsPerTable}-column limit");
        }

        var name = columnName.ToString();
        var col = new QwpColumn(name, RowCount);
        idx = _columns.Count;
        _columns.Add(col);
        _columnIndex[name] = idx;

        EnsureTouchedCapacity(idx + 1);
        MarkTouched(idx);
        return col;
    }

    private void SnapshotOnFirstTouch(int index, QwpColumn col)
    {
        if (index < _touchedInCurrentRow.Length && _touchedInCurrentRow[index])
        {
            return;
        }

        if (_rowSavepoints.Length <= index)
        {
            Array.Resize(ref _rowSavepoints, Math.Max(4, index + 1));
        }
        _rowSavepoints[index] = col.Snapshot();
    }

    internal void CancelCurrentRow()
    {
        for (var i = 0; i < _committedColumnCount && i < _touchedInCurrentRow.Length; i++)
        {
            if (_touchedInCurrentRow[i])
            {
                _columns[i].Restore(_rowSavepoints[i]);
            }
        }

        while (_columns.Count > _committedColumnCount)
        {
            var last = _columns.Count - 1;
            _columnIndex.Remove(_columns[last].Name);
            _columns.RemoveAt(last);
        }

        if (_designatedCreatedInCurrentRow)
        {
            DesignatedTimestampColumn = null;
        }
        else if (_designatedSavepoint.HasValue && DesignatedTimestampColumn is not null)
        {
            DesignatedTimestampColumn.Restore(_designatedSavepoint.Value);
        }
        _designatedSavepoint = null;
        _designatedCreatedInCurrentRow = false;

        if (_touchedInCurrentRow.Length > 0)
        {
            Array.Clear(_touchedInCurrentRow, 0, _touchedInCurrentRow.Length);
        }
        HasPendingRow = false;
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
            _designatedCreatedInCurrentRow = true;
        }

        return DesignatedTimestampColumn;
    }

    private void FinaliseRow()
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            if (!_touchedInCurrentRow[i])
            {
                SnapshotOnFirstTouch(i, _columns[i]);
                MarkTouched(i);
                _columns[i].AppendNull();
            }
        }

        for (var i = 0; i < _columns.Count; i++)
        {
            _touchedInCurrentRow[i] = false;
        }

        RowCount++;
        HasPendingRow = false;
        _committedColumnCount = _columns.Count;
        _designatedSavepoint = null;
        _designatedCreatedInCurrentRow = false;
    }

    /// <summary>Returns false when the column has already been written in this row (ILP first-value-wins).</summary>
    private bool MarkTouched(int columnIndex)
    {
        EnsureTouchedCapacity(columnIndex + 1);
        if (_touchedInCurrentRow[columnIndex])
        {
            return false;
        }
        _touchedInCurrentRow[columnIndex] = true;
        HasPendingRow = true;
        return true;
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
