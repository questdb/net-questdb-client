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
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Buffers rows for a single QWP table in columnar layout. The .NET counterpart of
///     Java's <c>QwpTableBuffer</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Experimental. PR 3a scope: the <see cref="ColumnBuffer"/> skeleton supports the
///     fixed-width simple types (Boolean..Double), VARCHAR, GEOHASH (precision tracking),
///     plus the lookup / get-or-create / cancel-row state machine. Symbol, decimal, and
///     array column support land in PR 3b–3d.
///     <para/>
///     Column-name lookup is case-insensitive (via the same lookup ordering as Java's
///     <c>LowerCaseCharSequenceIntHashMap</c>). The hot path uses a sequential cursor to
///     skip the hash lookup when columns are accessed in row order; out-of-order access
///     falls through to the dictionary.
/// </remarks>
internal sealed class QwpTableBuffer
{
    private readonly Dictionary<string, int> _columnIndexByLowerName =
        new(StringComparer.Ordinal);
    private readonly List<ColumnBuffer> _columns = new();
    private readonly string _tableName;

    private int _columnAccessCursor;
    private int _committedColumnCount;
    private int _inProgressColumnCount;
    private int _rowCount;
    private int _schemaId = -1;

    public QwpTableBuffer(string tableName)
    {
        if (string.IsNullOrEmpty(tableName))
        {
            throw new IngressError(Enums.ErrorCode.InvalidName, "table name cannot be empty");
        }
        if (tableName.Length > QwpConstants.MAX_TABLE_NAME_LENGTH)
        {
            throw new IngressError(Enums.ErrorCode.InvalidName,
                $"table name too long [maxLength={QwpConstants.MAX_TABLE_NAME_LENGTH}]");
        }
        _tableName = tableName;
    }

    public string TableName => _tableName;

    public int RowCount => _rowCount;

    public int ColumnCount => _columns.Count;

    public int SchemaId => _schemaId;

    public bool HasInProgressRow => _inProgressColumnCount > 0;

    public ColumnBuffer GetColumn(int index) => _columns[index];

    public void SetSchemaId(int schemaId) => _schemaId = schemaId;

    /// <summary>
    ///     Returns an existing column with the given name and type, or <c>null</c> if absent.
    ///     Hits the same sequential-cursor fast path as <see cref="GetOrCreateColumn"/>; on
    ///     a type mismatch — fast path or hash path — throws an
    ///     <see cref="IngressError"/>.
    /// </summary>
    public ColumnBuffer? GetExistingColumn(string name, byte type) => LookupColumn(name, type);

    /// <summary>
    ///     Returns the column with the given name, creating it if absent. Returns <c>null</c>
    ///     when the column has already received a value in the current (uncommitted) row —
    ///     callers must treat <c>null</c> as "duplicate column in this row, skip the write"
    ///     to match ILP first-value-wins semantics.
    /// </summary>
    public ColumnBuffer? GetOrCreateColumn(string name, byte type, bool useNullBitmap)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new IngressError(Enums.ErrorCode.InvalidName, "column name cannot be empty");
        }

        var existing = LookupColumn(name, type);
        if (existing is not null)
        {
            // size > rowCount means this column was already written in the in-progress row.
            // Silently ignore the duplicate (first value wins, same as ILP).
            if (existing.Size > _rowCount) return null;
            _inProgressColumnCount++;
            return existing;
        }

        if (name.Length > QwpConstants.MAX_COLUMN_NAME_LENGTH)
        {
            throw new IngressError(Enums.ErrorCode.InvalidName,
                $"column name too long [maxLength={QwpConstants.MAX_COLUMN_NAME_LENGTH}]");
        }

        var col = CreateColumn(name, type, useNullBitmap);
        _inProgressColumnCount++;
        return col;
    }

    /// <summary>
    ///     Advances to the next row. Pads any column whose size lags the new row count
    ///     with nulls so every column ends with the same row count.
    /// </summary>
    public void NextRow()
    {
        _columnAccessCursor = 0;
        _inProgressColumnCount = 0;
        var target = _rowCount + 1;
        for (var i = 0; i < _columns.Count; i++)
        {
            var col = _columns[i];
            while (col.Size < target) col.AddNull();
        }
        _rowCount++;
        _committedColumnCount = _columns.Count;
    }

    /// <summary>
    ///     Cancels the in-progress row. Late-added columns (created during the in-progress
    ///     row) are fully cleared; pre-existing columns whose size advanced past
    ///     <see cref="RowCount"/> are truncated back to the committed row count.
    /// </summary>
    public void CancelCurrentRow()
    {
        _columnAccessCursor = 0;
        _inProgressColumnCount = 0;
        for (var i = 0; i < _columns.Count; i++)
        {
            var col = _columns[i];
            if (i >= _committedColumnCount)
            {
                col.TruncateTo(0);
            }
            else if (col.Size > _rowCount)
            {
                col.TruncateTo(_rowCount);
            }
        }
    }

    /// <summary>Resets row state for reuse, keeping column definitions and storage allocations.</summary>
    public void Reset()
    {
        for (var i = 0; i < _columns.Count; i++) _columns[i].Reset();
        _columnAccessCursor = 0;
        _committedColumnCount = _columns.Count;
        _inProgressColumnCount = 0;
        _rowCount = 0;
    }

    /// <summary>Clears the buffer completely, including column definitions.</summary>
    public void Clear()
    {
        _columns.Clear();
        _columnIndexByLowerName.Clear();
        _columnAccessCursor = 0;
        _committedColumnCount = 0;
        _inProgressColumnCount = 0;
        _rowCount = 0;
        _schemaId = -1;
    }

    private ColumnBuffer? LookupColumn(string name, byte type)
    {
        // Fast path: the next column is the one the caller usually wants. A single
        // case-insensitive name comparison saves the dictionary lookup.
        var n = _columns.Count;
        if (_columnAccessCursor < n)
        {
            var candidate = _columns[_columnAccessCursor];
            if (string.Equals(candidate.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                _columnAccessCursor++;
                AssertColumnType(name, type, candidate);
                return candidate;
            }
        }

        // Slow path: dictionary lookup. The dictionary keys are lowercase to match the
        // case-insensitive lookup contract.
        if (_columnIndexByLowerName.TryGetValue(LowerInvariant(name), out var idx))
        {
            var existing = _columns[idx];
            AssertColumnType(name, type, existing);
            return existing;
        }
        return null;
    }

    private ColumnBuffer CreateColumn(string name, byte type, bool useNullBitmap)
    {
        if (_columns.Count >= QwpConstants.MAX_COLUMNS_PER_TABLE)
        {
            throw new IngressError(Enums.ErrorCode.InvalidName,
                $"column count exceeds maximum: {_columns.Count + 1} (max {QwpConstants.MAX_COLUMNS_PER_TABLE})");
        }

        var col = new ColumnBuffer(name, type, useNullBitmap) { Index = _columns.Count };
        _columns.Add(col);
        _columnIndexByLowerName[LowerInvariant(name)] = col.Index;

        // Pre-pad with nulls for already-committed rows so the next value the caller adds
        // lands at the correct row position.
        for (var r = 0; r < _rowCount; r++) col.AddNull();

        _schemaId = -1;
        return col;
    }

    private static string LowerInvariant(string s) => s.ToLowerInvariant();

    private static void AssertColumnType(string name, byte type, ColumnBuffer column)
    {
        if (column.Type != type)
        {
            throw new IngressError(Enums.ErrorCode.InvalidName,
                $"Column type mismatch for column '{name}': columnType={column.Type}, sentType={type}");
        }
    }

    /// <summary>
    ///     Per-column data buffer for <see cref="QwpTableBuffer"/>. Fixed-width values are
    ///     stored in a <see cref="PinnedAppendBuffer"/> data buffer; VARCHAR uses a pair
    ///     of buffers (offsets + data); a per-row null bitmap sits alongside when
    ///     <see cref="UseNullBitmap"/> is true.
    /// </summary>
    public sealed class ColumnBuffer
    {
        private readonly PinnedAppendBuffer? _dataBuffer;
        private readonly PinnedAppendBuffer? _stringData;
        private readonly PinnedAppendBuffer? _stringOffsets;

        // Per-row null bitmap. 1 bit per row, packed LSB-first per byte.
        private byte[] _nullBitmap = Array.Empty<byte>();
        private int _nullBitmapCapBits;

        private int _geohashPrecision = -1;
        private bool _hasNulls;
        private int _size;
        private int _valueCount;

        internal ColumnBuffer(string name, byte type, bool useNullBitmap)
        {
            Name = name;
            Type = type;
            UseNullBitmap = useNullBitmap;
            ElementSize = ElementSizeInBuffer(type);

            switch (type)
            {
                case QwpConstants.TYPE_BOOLEAN:
                case QwpConstants.TYPE_BYTE:
                    _dataBuffer = new PinnedAppendBuffer(16);
                    break;
                case QwpConstants.TYPE_SHORT:
                case QwpConstants.TYPE_CHAR:
                    _dataBuffer = new PinnedAppendBuffer(32);
                    break;
                case QwpConstants.TYPE_INT:
                case QwpConstants.TYPE_FLOAT:
                case QwpConstants.TYPE_SYMBOL:
                    _dataBuffer = new PinnedAppendBuffer(64);
                    break;
                case QwpConstants.TYPE_LONG:
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                case QwpConstants.TYPE_DATE:
                case QwpConstants.TYPE_DOUBLE:
                case QwpConstants.TYPE_GEOHASH:
                case QwpConstants.TYPE_DECIMAL64:
                    _dataBuffer = new PinnedAppendBuffer(128);
                    break;
                case QwpConstants.TYPE_UUID:
                case QwpConstants.TYPE_DECIMAL128:
                    _dataBuffer = new PinnedAppendBuffer(256);
                    break;
                case QwpConstants.TYPE_LONG256:
                case QwpConstants.TYPE_DECIMAL256:
                    _dataBuffer = new PinnedAppendBuffer(512);
                    break;
                case QwpConstants.TYPE_VARCHAR:
                case QwpConstants.TYPE_BINARY:
                    _stringOffsets = new PinnedAppendBuffer(64);
                    _stringOffsets.PutInt(0); // seed initial 0 offset
                    _stringData = new PinnedAppendBuffer(256);
                    break;
                default:
                    throw new IngressError(Enums.ErrorCode.InvalidName,
                        $"unsupported column type for QwpTableBuffer: 0x{type:X2}");
            }
        }

        public string Name { get; }

        public byte Type { get; }

        public bool UseNullBitmap { get; }

        public int ElementSize { get; }

        public int Index { get; internal set; }

        /// <summary>Total row count (including nulls).</summary>
        public int Size => _size;

        /// <summary>Number of stored non-null values.</summary>
        public int ValueCount => _valueCount;

        public bool HasNulls => _hasNulls;

        public bool IsNull(int rowIndex)
        {
            if (rowIndex < 0 || rowIndex >= _nullBitmapCapBits) return false;
            var byteIdx = rowIndex >> 3;
            var bitIdx = rowIndex & 7;
            return (_nullBitmap[byteIdx] & (1 << bitIdx)) != 0;
        }

        public void AddBoolean(bool value)
        {
            _dataBuffer!.PutByte(value ? (byte)1 : (byte)0);
            _valueCount++;
            _size++;
        }

        public void AddByte(byte value)
        {
            _dataBuffer!.PutByte(value);
            _valueCount++;
            _size++;
        }

        public void AddShort(short value)
        {
            _dataBuffer!.PutShort(value);
            _valueCount++;
            _size++;
        }

        public void AddInt(int value)
        {
            _dataBuffer!.PutInt(value);
            _valueCount++;
            _size++;
        }

        public void AddLong(long value)
        {
            _dataBuffer!.PutLong(value);
            _valueCount++;
            _size++;
        }

        public void AddFloat(float value)
        {
            _dataBuffer!.PutFloat(value);
            _valueCount++;
            _size++;
        }

        public void AddDouble(double value)
        {
            _dataBuffer!.PutDouble(value);
            _valueCount++;
            _size++;
        }

        public void AddGeoHash(long value, int precision)
        {
            if (precision < 1 || precision > 60)
            {
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    $"invalid GeoHash precision: {precision} (must be 1-60)");
            }
            if (_geohashPrecision == -1)
            {
                _geohashPrecision = precision;
            }
            else if (_geohashPrecision != precision)
            {
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    $"GeoHash precision mismatch: column has {_geohashPrecision} bits, got {precision}");
            }
            _dataBuffer!.PutLong(value);
            _valueCount++;
            _size++;
        }

        public void AddString(string? value)
        {
            if (value is null && UseNullBitmap)
            {
                EnsureNullBitmapCapacity(_size + 1);
                MarkNull(_size);
            }
            else
            {
                if (value is not null) _stringData!.PutUtf8(value);
                _stringOffsets!.PutInt(CheckedStringOffset(_stringData!.Length));
                _valueCount++;
            }
            _size++;
        }

        public void AddNull()
        {
            if (UseNullBitmap)
            {
                EnsureNullBitmapCapacity(_size + 1);
                MarkNull(_size);
            }
            else
            {
                // Non-nullable columns store a sentinel/default to keep the fixed-stride
                // buffer aligned with row indices.
                switch (Type)
                {
                    case QwpConstants.TYPE_BOOLEAN:
                    case QwpConstants.TYPE_BYTE:
                        _dataBuffer!.PutByte(0);
                        break;
                    case QwpConstants.TYPE_SHORT:
                    case QwpConstants.TYPE_CHAR:
                        _dataBuffer!.PutShort(0);
                        break;
                    case QwpConstants.TYPE_INT:
                        _dataBuffer!.PutInt(0);
                        break;
                    case QwpConstants.TYPE_SYMBOL:
                        _dataBuffer!.PutInt(-1);
                        break;
                    case QwpConstants.TYPE_FLOAT:
                        _dataBuffer!.PutFloat(float.NaN);
                        break;
                    case QwpConstants.TYPE_DOUBLE:
                        _dataBuffer!.PutDouble(double.NaN);
                        break;
                    case QwpConstants.TYPE_GEOHASH:
                        _dataBuffer!.PutLong(-1L);
                        break;
                    case QwpConstants.TYPE_LONG:
                    case QwpConstants.TYPE_TIMESTAMP:
                    case QwpConstants.TYPE_TIMESTAMP_NANOS:
                    case QwpConstants.TYPE_DATE:
                        _dataBuffer!.PutLong(long.MinValue);
                        break;
                    case QwpConstants.TYPE_UUID:
                        _dataBuffer!.PutLong(long.MinValue);
                        _dataBuffer!.PutLong(long.MinValue);
                        break;
                    case QwpConstants.TYPE_LONG256:
                        _dataBuffer!.PutLong(long.MinValue);
                        _dataBuffer!.PutLong(long.MinValue);
                        _dataBuffer!.PutLong(long.MinValue);
                        _dataBuffer!.PutLong(long.MinValue);
                        break;
                    case QwpConstants.TYPE_VARCHAR:
                    case QwpConstants.TYPE_BINARY:
                        _stringOffsets!.PutInt(CheckedStringOffset(_stringData!.Length));
                        break;
                    default:
                        throw new IngressError(Enums.ErrorCode.InvalidName,
                            $"AddNull: unsupported column type 0x{Type:X2}");
                }
                _valueCount++;
            }
            _size++;
        }

        internal void TruncateTo(int newSize)
        {
            if (newSize >= _size) return;

            int newValueCount;
            if (UseNullBitmap)
            {
                newValueCount = 0;
                for (var i = 0; i < newSize; i++)
                {
                    if (!IsNull(i)) newValueCount++;
                }
                // Clear null bits beyond the new range so a subsequent IsNull(j) for
                // j ∈ [newSize, oldSize) reports false.
                var clearTo = Math.Min(_size, _nullBitmapCapBits);
                for (var i = newSize; i < clearTo; i++)
                {
                    var byteIdx = i >> 3;
                    var bitIdx = i & 7;
                    _nullBitmap[byteIdx] &= (byte)~(1 << bitIdx);
                }
                _hasNulls = false;
                for (var i = 0; i < newSize && !_hasNulls; i++)
                {
                    if (IsNull(i)) _hasNulls = true;
                }
            }
            else
            {
                newValueCount = newSize;
            }

            _size = newSize;
            _valueCount = newValueCount;

            // Rewind fixed-width data buffer
            if (_dataBuffer is not null && ElementSize > 0)
            {
                _dataBuffer.JumpTo(newValueCount * ElementSize);
            }

            // Rewind variable-width string buffers using the kept offsets table.
            if (_stringOffsets is not null && _stringData is not null)
            {
                var dataOffset = BinaryPrimitives.ReadInt32LittleEndian(
                    _stringOffsets.AsReadOnlySpan(newValueCount * 4, 4));
                _stringData.JumpTo(dataOffset);
                _stringOffsets.JumpTo((newValueCount + 1) * 4);
            }

            // Type-specific metadata reverts to "freshly created" when truncated to zero.
            if (newValueCount == 0)
            {
                _geohashPrecision = -1;
            }
        }

        internal void Reset()
        {
            _size = 0;
            _valueCount = 0;
            _hasNulls = false;
            _dataBuffer?.Truncate();
            if (_stringOffsets is not null)
            {
                _stringOffsets.Truncate();
                _stringOffsets.PutInt(0); // re-seed initial 0 offset
            }
            _stringData?.Truncate();
            if (_nullBitmap.Length > 0) Array.Clear(_nullBitmap, 0, _nullBitmap.Length);
            _geohashPrecision = -1;
        }

        private void EnsureNullBitmapCapacity(int requiredBits)
        {
            if (requiredBits <= _nullBitmapCapBits) return;
            // Grow in 64-bit increments to match Java's bitmap layout.
            var newBits = Math.Max(64, _nullBitmapCapBits * 2);
            while (newBits < requiredBits) newBits *= 2;
            var newBytes = newBits >> 3;
            if (_nullBitmap.Length < newBytes)
            {
                var bigger = new byte[newBytes];
                Array.Copy(_nullBitmap, bigger, _nullBitmap.Length);
                _nullBitmap = bigger;
            }
            _nullBitmapCapBits = newBits;
        }

        private void MarkNull(int rowIndex)
        {
            var byteIdx = rowIndex >> 3;
            var bitIdx = rowIndex & 7;
            _nullBitmap[byteIdx] |= (byte)(1 << bitIdx);
            _hasNulls = true;
        }

        private static int CheckedStringOffset(int offset)
        {
            // 32-bit cap per Java's checkedStringOffset; PinnedAppendBuffer.Length is
            // already int, but the cap is documented for parity.
            if (offset < 0)
            {
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    "string column data exceeds 2 GiB per batch, flush more frequently");
            }
            return offset;
        }

        private static int ElementSizeInBuffer(byte type) => type switch
        {
            QwpConstants.TYPE_BOOLEAN or QwpConstants.TYPE_BYTE => 1,
            QwpConstants.TYPE_SHORT or QwpConstants.TYPE_CHAR => 2,
            QwpConstants.TYPE_INT or QwpConstants.TYPE_SYMBOL or QwpConstants.TYPE_FLOAT => 4,
            QwpConstants.TYPE_GEOHASH or QwpConstants.TYPE_LONG or QwpConstants.TYPE_TIMESTAMP
                or QwpConstants.TYPE_TIMESTAMP_NANOS or QwpConstants.TYPE_DATE
                or QwpConstants.TYPE_DECIMAL64 or QwpConstants.TYPE_DOUBLE => 8,
            QwpConstants.TYPE_UUID or QwpConstants.TYPE_DECIMAL128 => 16,
            QwpConstants.TYPE_LONG256 or QwpConstants.TYPE_DECIMAL256 => 32,
            _ => 0,
        };
    }
}
