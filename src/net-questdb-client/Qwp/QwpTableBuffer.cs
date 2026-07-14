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

    // Positional resolver cursor: the index in _columns the next append is expected to target,
    // exploiting that rows repeat the same columns in the same order. Reset to 0 at each row
    // boundary. Lets GetOrCreateColumn skip the case-insensitive dictionary lookup on the hot path.
    private int _appendCursor;

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

        TableName           = tableName;
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
        var  c     = _columns.Count;
        for (var i = 0; i < c; i++)
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
        try
        {
            GetOrCreateColumn(columnName)?.AppendBool(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a signed byte.</summary>
    public void AppendByte(ReadOnlySpan<char> columnName, sbyte value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendByte(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a 16-bit signed integer.</summary>
    public void AppendShort(ReadOnlySpan<char> columnName, short value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendShort(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a 32-bit signed integer.</summary>
    public void AppendInt(ReadOnlySpan<char> columnName, int value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendInt(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a 64-bit signed integer.</summary>
    public void AppendLong(ReadOnlySpan<char> columnName, long value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendLong(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a single-precision float.</summary>
    public void AppendFloat(ReadOnlySpan<char> columnName, float value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendFloat(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a double-precision float.</summary>
    public void AppendDouble(ReadOnlySpan<char> columnName, double value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDouble(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a TIMESTAMP value (microseconds since epoch) to a non-designated column.</summary>
    public void AppendTimestampMicros(ReadOnlySpan<char> columnName, long micros)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendTimestampMicros(micros);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a TIMESTAMP_NANOS value (nanoseconds since epoch) to a non-designated column.</summary>
    public void AppendTimestampNanos(ReadOnlySpan<char> columnName, long nanos)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendTimestampNanos(nanos);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DATE value (milliseconds since epoch).</summary>
    public void AppendDateMillis(ReadOnlySpan<char> columnName, long millis)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDateMillis(millis);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a UUID.</summary>
    public void AppendUuid(ReadOnlySpan<char> columnName, Guid value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendUuid(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a single UTF-16 code unit.</summary>
    public void AppendChar(ReadOnlySpan<char> columnName, char value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendChar(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a length-prefixed UTF-8 string.</summary>
    public void AppendVarchar(ReadOnlySpan<char> columnName, ReadOnlySpan<char> value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendVarchar(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a SYMBOL value as a global dictionary id.</summary>
    public void AppendSymbol(ReadOnlySpan<char> columnName, int globalId)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendSymbol(globalId);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    // String-name overloads of the hot scalar appends: they route through the string GetOrCreateColumn
    // so a stable column-name instance is resolved by reference equality on the positional fast path.
    public void AppendBool(string columnName, bool value)
    {
        try { GetOrCreateColumn(columnName)?.AppendBool(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendInt(string columnName, int value)
    {
        try { GetOrCreateColumn(columnName)?.AppendInt(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendLong(string columnName, long value)
    {
        try { GetOrCreateColumn(columnName)?.AppendLong(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDouble(string columnName, double value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDouble(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendChar(string columnName, char value)
    {
        try { GetOrCreateColumn(columnName)?.AppendChar(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendSymbol(string columnName, int globalId)
    {
        try { GetOrCreateColumn(columnName)?.AppendSymbol(globalId); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendByte(string columnName, sbyte value)
    {
        try { GetOrCreateColumn(columnName)?.AppendByte(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendShort(string columnName, short value)
    {
        try { GetOrCreateColumn(columnName)?.AppendShort(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendFloat(string columnName, float value)
    {
        try { GetOrCreateColumn(columnName)?.AppendFloat(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendTimestampMicros(string columnName, long micros)
    {
        try { GetOrCreateColumn(columnName)?.AppendTimestampMicros(micros); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendTimestampNanos(string columnName, long nanos)
    {
        try { GetOrCreateColumn(columnName)?.AppendTimestampNanos(nanos); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDateMillis(string columnName, long millis)
    {
        try { GetOrCreateColumn(columnName)?.AppendDateMillis(millis); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendUuid(string columnName, Guid value)
    {
        try { GetOrCreateColumn(columnName)?.AppendUuid(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendVarchar(string columnName, ReadOnlySpan<char> value)
    {
        try { GetOrCreateColumn(columnName)?.AppendVarchar(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal64(string columnName, decimal value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal64(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal128(string columnName, decimal value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal128(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal256(string columnName, decimal value)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal256(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal64(string columnName, decimal value, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal64(value, scale); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal128(string columnName, decimal value, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal128(value, scale); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal256(string columnName, decimal value, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal256(value, scale); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal128(string columnName, long lo, long hi, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal128(lo, hi, scale); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDecimal256(string columnName, long l0, long l1, long l2, long l3, byte scale)
    {
        try { GetOrCreateColumn(columnName)?.AppendDecimal256(l0, l1, l2, l3, scale); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendBinary(string columnName, ReadOnlySpan<byte> value)
    {
        try { GetOrCreateColumn(columnName)?.AppendBinary(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendIPv4(string columnName, uint addr)
    {
        try { GetOrCreateColumn(columnName)?.AppendIPv4(addr); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendLong256(string columnName, BigInteger value)
    {
        try { GetOrCreateColumn(columnName)?.AppendLong256(value); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendGeohash(string columnName, ulong hash, int precisionBits)
    {
        try { GetOrCreateColumn(columnName)?.AppendGeohash(hash, precisionBits); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendDoubleArray(string columnName, ReadOnlySpan<double> values, ReadOnlySpan<int> shape)
    {
        try { GetOrCreateColumn(columnName)?.AppendDoubleArray(values, shape); }
        catch { CancelCurrentRow(); throw; }
    }

    public void AppendLongArray(string columnName, ReadOnlySpan<long> values, ReadOnlySpan<int> shape)
    {
        try { GetOrCreateColumn(columnName)?.AppendLongArray(values, shape); }
        catch { CancelCurrentRow(); throw; }
    }


    /// <summary>Append a DECIMAL64 value. The first call locks the column scale.</summary>
    public void AppendDecimal64(ReadOnlySpan<char> columnName, decimal value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal64(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DECIMAL128 value. The first call locks the column scale.</summary>
    public void AppendDecimal128(ReadOnlySpan<char> columnName, decimal value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal128(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DECIMAL256 value. The first call locks the column scale.</summary>
    public void AppendDecimal256(ReadOnlySpan<char> columnName, decimal value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal256(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DECIMAL64 value coerced to the explicit scale (round half away from zero).</summary>
    public void AppendDecimal64(ReadOnlySpan<char> columnName, decimal value, byte scale)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal64(value, scale);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DECIMAL128 value coerced to the explicit scale (round half away from zero).</summary>
    public void AppendDecimal128(ReadOnlySpan<char> columnName, decimal value, byte scale)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal128(value, scale);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DECIMAL256 value coerced to the explicit scale (round half away from zero).</summary>
    public void AppendDecimal256(ReadOnlySpan<char> columnName, decimal value, byte scale)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal256(value, scale);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DECIMAL128 value: <c>lo</c> = unsigned low 64 bits, <c>hi</c> = signed high 64 bits.</summary>
    public void AppendDecimal128(ReadOnlySpan<char> columnName, long lo, long hi, byte scale)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal128(lo, hi, scale);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DECIMAL256 value: <c>l0</c>–<c>l2</c> unsigned low limbs, <c>l3</c> signed high limb.</summary>
    public void AppendDecimal256(ReadOnlySpan<char> columnName, long l0, long l1, long l2, long l3, byte scale)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDecimal256(l0, l1, l2, l3, scale);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a BINARY value as opaque bytes (no UTF-8 contract).</summary>
    public void AppendBinary(ReadOnlySpan<char> columnName, ReadOnlySpan<byte> value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendBinary(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append an IPv4 address as 4 bytes little-endian.</summary>
    public void AppendIPv4(ReadOnlySpan<char> columnName, uint addr)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendIPv4(addr);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a non-negative LONG256 value (≤ 256 bits).</summary>
    public void AppendLong256(ReadOnlySpan<char> columnName, BigInteger value)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendLong256(value);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a GEOHASH value. The first call locks the column precision (in bits).</summary>
    public void AppendGeohash(ReadOnlySpan<char> columnName, ulong hash, int precisionBits)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendGeohash(hash, precisionBits);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a DOUBLE_ARRAY row with the given shape.</summary>
    public void AppendDoubleArray(ReadOnlySpan<char> columnName, ReadOnlySpan<double> values, ReadOnlySpan<int> shape)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendDoubleArray(values, shape);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    /// <summary>Append a LONG_ARRAY row with the given shape.</summary>
    public void AppendLongArray(ReadOnlySpan<char> columnName, ReadOnlySpan<long> values, ReadOnlySpan<int> shape)
    {
        try
        {
            GetOrCreateColumn(columnName)?.AppendLongArray(values, shape);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
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

        RowCount              = 0;
        HasPendingRow         = false;
        _appendCursor         = 0;
        _committedColumnCount = _columns.Count;
        _designatedSavepoint  = null;

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
            // Happy path: ts column already exists (steady state) and the row limit is not hit. The
            // savepoint is per-row (reset in FinaliseRow) so it is always taken here — it is what lets
            // CancelCurrentRow roll the ts column back if a later append/FinaliseRow throws. Row-1
            // creation and the row-limit throw are the cold AtSlow.
            var ts = DesignatedTimestampColumn;
            if (ts != null && RowCount < QwpConstants.MaxRowsPerTable)
            {
                _designatedSavepoint = ts.NewSnapshot();
                ts.AppendTimestampMicros(timestampMicros);
                FinaliseRow();
                return;
            }

            AtSlow(timestampMicros);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    private void AtSlow(long timestampMicros)
    {
        EnsureCanAppendRow();
        var ts = DesignatedTimestampColumn ?? CreateDesignatedTimestampColumn();
        _designatedSavepoint = ts.NewSnapshot();
        ts.AppendTimestampMicros(timestampMicros);
        FinaliseRow();
    }

    /// <summary>
    ///     Commit the current row with a TIMESTAMP_NANOS (nanoseconds-since-epoch) designated value.
    /// </summary>
    public void AtNanos(long timestampNanos)
    {
        try
        {
            var ts = DesignatedTimestampColumn;
            if (ts != null && RowCount < QwpConstants.MaxRowsPerTable)
            {
                _designatedSavepoint = ts.NewSnapshot();
                ts.AppendTimestampNanos(timestampNanos);
                FinaliseRow();
                return;
            }

            AtNanosSlow(timestampNanos);
        }
        catch
        {
            CancelCurrentRow();
            throw;
        }
    }

    private void AtNanosSlow(long timestampNanos)
    {
        EnsureCanAppendRow();
        var ts = DesignatedTimestampColumn ?? CreateDesignatedTimestampColumn();
        _designatedSavepoint = ts.NewSnapshot();
        ts.AppendTimestampNanos(timestampNanos);
        FinaliseRow();
    }

    private void EnsureCanAppendRow()
    {
        // Kept tiny so the JIT inlines the check into At/AtNanos; the throwing string interpolation
        // lives in a cold helper to stay under the inline budget.
        if (RowCount >= QwpConstants.MaxRowsPerTable)
        {
            ThrowRowLimitExceeded();
        }
    }

    private void ThrowRowLimitExceeded() =>
        throw new IngressError(ErrorCode.InvalidApiCall,
                               $"table '{TableName}' exceeds the {QwpConstants.MaxRowsPerTable}-row limit");

    /// <summary>
    ///     Look up an existing column or create a new one.
    /// </summary>
    /// <remarks>
    ///     The new column is back-filled with nulls for the <see cref="RowCount" /> rows that came before it.
    /// </remarks>
    private QwpColumn? GetOrCreateColumn(ReadOnlySpan<char> columnName)
    {
        // Positional fast path: rows repeat the same columns in the same order, so the next append
        // almost always lands on _columns[_appendCursor]. An ordinal name match here skips the
        // case-insensitive dictionary hash+probe, which is the dominant per-cell cost. Row-1
        // creation, reordered / renamed / case-only-different columns, and duplicates fall to the
        // cold OrCreateColumn, which resolves via the dictionary and realigns the cursor. Both
        // _touchedInCurrentRow and _rowSavepoints are grown to the column count at creation, so no
        // bounds check beyond cursor < _columns.Count is needed.
        var cursor = _appendCursor;
        if ((uint)cursor < (uint)_columns.Count)
        {
            var col = _columns[cursor];
            if (!_touchedInCurrentRow[cursor] && columnName.SequenceEqual(col.Name))
            {
                TouchAtCursor(cursor, col);
                return col;
            }
        }

        return OrCreateColumn(columnName);
    }

    /// <summary>
    ///     String overload of <see cref="GetOrCreateColumn(ReadOnlySpan{char})" />. When the caller
    ///     passes a stable string instance (interned literal / cached name), the positional match is a
    ///     single <see cref="object.ReferenceEquals(object,object)" /> pointer compare instead of the
    ///     char-by-char <c>SequenceEqual</c> — a span can't carry the string identity, hence the
    ///     dedicated overload. Columns created via this path store the caller's exact string as their
    ///     <see cref="QwpColumn.Name" />, so later same-instance appends hit the reference compare;
    ///     equal-but-distinct instances fall back to SequenceEqual, then the dictionary.
    /// </summary>
    private QwpColumn? GetOrCreateColumn(string columnName)
    {
        var cursor = _appendCursor;
        if ((uint)cursor < (uint)_columns.Count)
        {
            var col = _columns[cursor];
            if (!_touchedInCurrentRow[cursor] &&
                (ReferenceEquals(columnName, col.Name) || columnName.AsSpan().SequenceEqual(col.Name)))
            {
                TouchAtCursor(cursor, col);
                return col;
            }
        }

        return OrCreateColumn(columnName);
    }

    // Commits the positional-fast-path bookkeeping once the column at the cursor is confirmed.
    private void TouchAtCursor(int cursor, QwpColumn col)
    {
        _appendCursor                = cursor + 1;
        col.Snapshot(ref _rowSavepoints[cursor]);
        _touchedInCurrentRow[cursor] = true;
        HasPendingRow                = true;
    }

    private QwpColumn? OrCreateColumn(ReadOnlySpan<char> columnName)
    {
        // Cold path: the positional cursor missed. Resolve through the dictionary — the column may
        // exist (first touch out of order) or already be written this row (duplicate), or be new.
#if NET9_0_OR_GREATER
        if (_columnIndexLookup.TryGetValue(columnName, out var existing))
        {
            return TouchExisting(existing);
        }
#else
        if (_columnIndex.TryGetValue(columnName.ToString(), out var existing))
        {
            return TouchExisting(existing);
        }
#endif
        if (columnName.Length == 0)
        {
            throw new IngressError(ErrorCode.InvalidName, "column name must not be empty");
        }

        return AddColumn(columnName.ToString());
    }

    private QwpColumn? OrCreateColumn(string columnName)
    {
        // String cold path: same resolution as the span overload, but a brand-new column stores the
        // caller's exact string as its Name so the reference-equality fast path can hit next time.
        if (_columnIndex.TryGetValue(columnName, out var existing))
        {
            return TouchExisting(existing);
        }

        if (columnName.Length == 0)
        {
            throw new IngressError(ErrorCode.InvalidName, "column name must not be empty");
        }

        return AddColumn(columnName);
    }

    private QwpColumn? TouchExisting(int existing)
    {
        if (existing < _touchedInCurrentRow.Length && _touchedInCurrentRow[existing])
        {
            // Same column appended earlier in this row — keep the first value (cursor unchanged).
            return null;
        }

        SnapshotOnFirstTouch(existing, _columns[existing]);
        var resolved = MarkTouched(existing) ? _columns[existing] : null;
        _appendCursor = existing + 1;
        return resolved;
    }

    private QwpColumn AddColumn(string name)
    {
        int nameByteCount;
        try
        {
            nameByteCount = QwpStrictUtf8.Encoding.GetByteCount(name);
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

        var col = new QwpColumn(name, RowCount);
        var idx = _columns.Count;
        _columns.Add(col);
        _columnIndex[name] = idx;

        EnsureTouchedCapacity(idx + 1);
        // Grow the savepoint array alongside the column count so the positional fast path can write
        // _rowSavepoints[cursor] without its own bounds check.
        EnsureSavepointCapacity(idx + 1);
        MarkTouched(idx);
        _appendCursor = idx + 1;
        return col;
    }

    private void EnsureSavepointCapacity(int required)
    {
        if (_rowSavepoints.Length < required)
        {
            Array.Resize(ref _rowSavepoints, Math.Max(required, _rowSavepoints.Length * 2));
        }
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

        _rowSavepoints[index] = col.NewSnapshot();
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

        _designatedSavepoint           = null;
        _designatedCreatedInCurrentRow = false;

        if (_touchedInCurrentRow.Length > 0)
        {
            Array.Clear(_touchedInCurrentRow, 0, _touchedInCurrentRow.Length);
        }

        HasPendingRow = false;
        _appendCursor = 0;
    }

    /// <summary>
    ///     Lazily creates the designated-timestamp column (cold path of At/AtNanos, inlined there).
    ///     The first <c>AppendTimestamp*</c> call on the returned column locks its type code
    ///     (TIMESTAMP vs. TIMESTAMP_NANOS); subsequent mismatched calls throw via
    ///     <see cref="QwpColumn" />'s type guard.
    /// </summary>
    private QwpColumn CreateDesignatedTimestampColumn()
    {
        DesignatedTimestampColumn      = new QwpColumn(string.Empty, RowCount);
        _designatedCreatedInCurrentRow = true;
        return DesignatedTimestampColumn;
    }

    private void FinaliseRow()
    {
        var columnsCount = _columns.Count;
        for (var i = 0; i < columnsCount; i++)
        {
            if (!_touchedInCurrentRow[i])
            {
                SnapshotOnFirstTouch(i, _columns[i]);
                MarkTouched(i);
                _columns[i].AppendNull();
            }
        }

        for (var i = 0; i < columnsCount; i++)
        {
            _touchedInCurrentRow[i] = false;
        }

        RowCount++;
        HasPendingRow                  = false;
        _appendCursor                  = 0;
        _committedColumnCount          = columnsCount;
        _designatedSavepoint           = null;
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
        HasPendingRow                     = true;
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