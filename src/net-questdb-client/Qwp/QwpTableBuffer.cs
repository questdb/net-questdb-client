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
using System.Numerics;
using System.Text;
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
    private readonly IQwpGlobalSymbolSink? _globalSymbolSink;
    private readonly string _tableName;

    private int _columnAccessCursor;
    private int _committedColumnCount;
    private int _inProgressColumnCount;
    private int _rowCount;
    private int _schemaId = -1;

    public QwpTableBuffer(string tableName) : this(tableName, null) { }

    /// <summary>
    ///     Constructs a table buffer optionally bound to a global symbol dictionary sink.
    ///     When <paramref name="globalSymbolSink"/> is non-null, <c>AddSymbol</c> calls flow
    ///     through it for global ID allocation; otherwise the column maintains a
    ///     per-batch local dictionary.
    /// </summary>
    public QwpTableBuffer(string tableName, IQwpGlobalSymbolSink? globalSymbolSink)
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
        _globalSymbolSink = globalSymbolSink;
    }

    public string TableName => _tableName;

    public int RowCount => _rowCount;

    public int ColumnCount => _columns.Count;

    public int SchemaId => _schemaId;

    public bool HasInProgressRow => _inProgressColumnCount > 0;

    public ColumnBuffer GetColumn(int index) => _columns[index];

    public void SetSchemaId(int schemaId) => _schemaId = schemaId;

    /// <summary>
    ///     Returns a snapshot of the per-column (name, wireTypeCode) pairs in column order.
    ///     Used by <see cref="QwpColumnWriter"/> to emit the FULL-schema table header.
    /// </summary>
    public QwpColumnDef[] GetColumnDefs()
    {
        var defs = new QwpColumnDef[_columns.Count];
        for (var i = 0; i < _columns.Count; i++)
        {
            defs[i] = new QwpColumnDef(_columns[i].Name, _columns[i].Type);
        }
        return defs;
    }

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
    /// <summary>
    ///     Gets or creates the designated-timestamp column. Java uses an empty-name slot
    ///     for the designated timestamp; this entry point bypasses the name validation
    ///     that <see cref="GetOrCreateColumn"/> applies for regular columns.
    /// </summary>
    public ColumnBuffer GetOrCreateDesignatedTimestampColumn(byte type)
    {
        var existing = LookupColumn("", type);
        if (existing is not null)
        {
            if (existing.Size > _rowCount) return existing; // duplicate write within the row → first wins
            _inProgressColumnCount++;
            return existing;
        }
        var col = CreateColumn("", type, useNullBitmap: true);
        _inProgressColumnCount++;
        return col;
    }

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
        // §1.4 — flush-and-batch headroom tracking is per-batch state; clears with the buffer.
        _committedDatagramEstimate = 0;
        _headroomCommittedSampleCount = 0;
        _headroomEwmaRowGrowth = 0;
        _headroomLastRowGrowth = 0;
        _headroomSchemaColumnCount = -1;
    }

    // §1.4 — flush-and-batch headroom tracking. Owned by QwpTableBuffer so per-table
    // state survives across queries on the same buffer; only the UDP sender reads /
    // writes these fields today (WS sender doesn't pre-flush at row commit time).
    //
    // The shift-by-2 EWMA matches Java's ADAPTIVE_HEADROOM_EWMA_SHIFT = 2 (α = 1/4):
    //   ewma += (rowGrowth - ewma) >> 2.
    // PredictNextRowGrowth returns max(lastGrowth, ewma) once at least two samples
    // have been observed — see Java's TableHeadroomState.predictNextRowGrowth.
    private long _committedDatagramEstimate;
    private int _headroomCommittedSampleCount;
    private long _headroomEwmaRowGrowth;
    private long _headroomLastRowGrowth;
    private int _headroomSchemaColumnCount = -1;

    /// <summary>Running estimate of the encoded datagram size for the currently-committed rows.</summary>
    public long CommittedDatagramEstimate => _committedDatagramEstimate;

    /// <summary>
    ///     Records that one more row was committed, advancing the running datagram-size
    ///     estimate and the headroom predictor's EWMA. <paramref name="newDatagramSize"/>
    ///     is the full estimated encoded size after this row was committed (typically
    ///     produced by <see cref="EstimateEncodedDatagramSize"/>(<see cref="RowCount"/>)).
    /// </summary>
    public void RecordCommittedRow(int newDatagramSize)
    {
        var growth = newDatagramSize - _committedDatagramEstimate;
        _committedDatagramEstimate = newDatagramSize;

        var currentColumnCount = _columns.Count;
        if (_headroomSchemaColumnCount != currentColumnCount)
        {
            // Schema added/removed columns mid-batch — reset the predictor so stale
            // row sizes from the prior schema don't bias new predictions.
            _headroomCommittedSampleCount = 0;
            _headroomEwmaRowGrowth = 0;
            _headroomLastRowGrowth = 0;
            _headroomSchemaColumnCount = currentColumnCount;
        }

        _headroomLastRowGrowth = growth;
        if (_headroomCommittedSampleCount == 0)
        {
            _headroomEwmaRowGrowth = growth;
        }
        else
        {
            _headroomEwmaRowGrowth += (growth - _headroomEwmaRowGrowth) >> 2;
        }
        if (_headroomCommittedSampleCount < int.MaxValue) _headroomCommittedSampleCount++;
    }

    /// <summary>
    ///     Returns the predicted byte growth of the next row, or <c>0</c> while there
    ///     aren't enough samples to estimate. Mirrors Java's
    ///     <c>TableHeadroomState.predictNextRowGrowth</c>: <c>max(lastGrowth, ewma)</c>
    ///     once two or more rows have committed.
    /// </summary>
    public long PredictNextRowGrowth()
    {
        if (_headroomCommittedSampleCount < 2) return 0;
        return Math.Max(_headroomLastRowGrowth, _headroomEwmaRowGrowth);
    }

    /// <summary>
    ///     Returns an exact (or safe over-) estimate of the wire-byte size that
    ///     <see cref="QwpWebSocketEncoder.Encode"/> would produce for a datagram
    ///     containing the first <paramref name="rowCount"/> rows of this table in
    ///     SCHEMA_MODE_FULL. Used by <c>QwpUdpSender</c>'s flush-and-batch path to
    ///     decide whether the next row commit would push the datagram over
    ///     <c>max_datagram_size</c>.
    /// </summary>
    /// <remarks>
    ///     Mirrors the byte counts emitted by <see cref="QwpColumnWriter"/> for the
    ///     non-Gorilla, non-symbol-delta path (the only path UDP uses today). The
    ///     estimate accounts for: 12-byte QWP1 envelope, table-block headers,
    ///     per-column schema entries, per-column null section + per-type body. SYMBOL
    ///     and ARRAY column estimates walk the per-column lists since their wire size
    ///     is data-dependent; everything else closes form.
    /// </remarks>
    public int EstimateEncodedDatagramSize(int rowCount)
    {
        if (rowCount == 0) return 0;

        var size = QwpConstants.HEADER_SIZE; // QWP1 envelope.

        // Table block headers.
        var tableNameBytes = Encoding.UTF8.GetByteCount(TableName);
        size += VarintSize((long)tableNameBytes) + tableNameBytes;
        size += VarintSize(rowCount);
        size += VarintSize(_columns.Count);
        size += 1; // schema_mode byte
        size += VarintSize(_schemaId < 0 ? 0 : _schemaId);

        // Per-column schema entries (FULL mode).
        for (var i = 0; i < _columns.Count; i++)
        {
            var col = _columns[i];
            var colNameBytes = Encoding.UTF8.GetByteCount(col.Name);
            size += VarintSize((long)colNameBytes) + colNameBytes;
            size += 1; // wire-type byte
        }

        // Per-column data sections.
        for (var i = 0; i < _columns.Count; i++)
        {
            size += _columns[i].EstimateWireBytes(rowCount);
        }

        return size;
    }

    private static int VarintSize(long value)
    {
        var v = (ulong)value;
        var bytes = 1;
        while (v >= 0x80) { v >>= 7; bytes++; }
        return bytes;
    }

    /// <summary>
    ///     Drops every committed row from each column while preserving the in-progress
    ///     row's per-column data. Used by <c>QwpUdpSender</c>'s flush-and-batch path:
    ///     when a freshly-staged row would overflow <c>max_datagram_size</c>, the
    ///     committed prefix flushes as one datagram and the in-progress row becomes
    ///     the seed of the next datagram. After this call the table reports
    ///     <see cref="RowCount"/> = 0 with each touched column carrying exactly one
    ///     pre-committed value at index 0; the next <see cref="NextRow"/> commits it
    ///     as row 0 of the new datagram.
    /// </summary>
    /// <remarks>
    ///     Type-specific column metadata (decimal scale, geohash precision, symbol
    ///     dictionary) resets per-column to "freshly created" — appropriate because
    ///     UDP datagrams are stand-alone QWP1 messages and each carries its own full
    ///     schema header. The in-progress row's value re-establishes any per-type
    ///     state (e.g. the first AddDecimal64 after the trim re-latches the scale).
    /// </remarks>
    public void RetainInProgressRow()
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            _columns[i].RetainInProgressRow(_rowCount);
        }
        _rowCount = 0;
        // Cursor + in-progress counters survive: the user is still in the middle of
        // building the row, so AtNow's NextRow pass and any subsequent Column lookups
        // need to keep behaving as if no flush had happened.
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

        var col = new ColumnBuffer(name, type, useNullBitmap, _globalSymbolSink)
        {
            Index = _columns.Count,
        };
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
    ///     Allocation-free snapshot of an in-progress row's per-column data, captured
    ///     by <see cref="ColumnBuffer.CaptureInProgressRowState"/> and replayed via
    ///     <see cref="ColumnBuffer.RestoreInProgressRowState"/> on the
    ///     <see cref="QwpTableBuffer.RetainInProgressRow"/> path. Fixed-width column
    ///     types capture entirely in the <c>long</c> payload slots — the struct is a
    ///     pure value type with no boxing on the per-row commit path. Reference fields
    ///     stay null for primitives and only allocate for VARCHAR / SYMBOL strings or
    ///     array data, which always need heap storage anyway.
    /// </summary>
    /// <remarks>
    ///     <see cref="WireType"/> doubles as a discriminator: <c>0</c> means "no
    ///     in-progress data on this column" and <see cref="HasValue"/> reflects that.
    ///     The payload field reuse per wire type:
    ///     <list type="bullet">
    ///         <item>BOOL/BYTE/SHORT/CHAR/INT/LONG/TIMESTAMP[_NANOS]/DATE — Long0 only.</item>
    ///         <item>FLOAT/DOUBLE — Long0 carries the IEEE bits.</item>
    ///         <item>UUID/DECIMAL128 — Long0 = lo, Long1 = hi (decimal scale on IntAux).</item>
    ///         <item>LONG256 — Long0..Long3 = words 0..3.</item>
    ///         <item>DECIMAL64 — Long0 = unscaled value, IntAux = scale.</item>
    ///         <item>GEOHASH — Long0 = packed value, IntAux = precision bits.</item>
    ///         <item>VARCHAR/BINARY/SYMBOL — StringRef carries the UTF-16 string.</item>
    ///         <item>DOUBLE_ARRAY/LONG_ARRAY — IntAux = nDims, ArrayShape + one of
    ///             DoubleArrayData / LongArrayData = the per-row payload.</item>
    ///     </list>
    /// </remarks>
    internal readonly struct InProgressSnapshot
    {
        /// <summary>Wire type discriminator. <c>0</c> means "no in-progress data".</summary>
        public byte WireType { get; init; }

        /// <summary>Primary 8-byte payload — see remarks for per-type meaning.</summary>
        public long Long0 { get; init; }

        /// <summary>Second long slot — UUID/DECIMAL128 hi, LONG256 word 1.</summary>
        public long Long1 { get; init; }

        /// <summary>LONG256 word 2.</summary>
        public long Long2 { get; init; }

        /// <summary>LONG256 word 3.</summary>
        public long Long3 { get; init; }

        /// <summary>Scale (decimal), precision (geohash), or rank (array).</summary>
        public int IntAux { get; init; }

        /// <summary>VARCHAR / BINARY / SYMBOL string payload.</summary>
        public string? StringRef { get; init; }

        /// <summary>Array shape vector (length = <see cref="IntAux"/>).</summary>
        public int[]? ArrayShape { get; init; }

        /// <summary>TYPE_DOUBLE_ARRAY data; null otherwise.</summary>
        public double[]? DoubleArrayData { get; init; }

        /// <summary>TYPE_LONG_ARRAY data; null otherwise.</summary>
        public long[]? LongArrayData { get; init; }

        /// <summary>True when the snapshot carries a captured in-progress value.</summary>
        public bool HasValue => WireType != 0;
    }

    /// <summary>
    ///     Per-column data buffer for <see cref="QwpTableBuffer"/>. Fixed-width values are
    ///     stored in a <see cref="PinnedAppendBuffer"/> data buffer; VARCHAR uses a pair
    ///     of buffers (offsets + data); a per-row null bitmap sits alongside when
    ///     <see cref="UseNullBitmap"/> is true.
    /// </summary>
    public sealed class ColumnBuffer
    {
        // Strict UTF-8 decoder — throws on invalid byte sequences (used by AddSymbolUtf8).
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);

        private readonly PinnedAppendBuffer? _dataBuffer;
        private readonly IQwpGlobalSymbolSink? _globalSymbolSink;
        private readonly PinnedAppendBuffer? _stringData;
        private readonly PinnedAppendBuffer? _stringOffsets;

        // Per-row null bitmap. 1 bit per row, packed LSB-first per byte.
        private byte[] _nullBitmap = Array.Empty<byte>();
        private int _nullBitmapCapBits;

        // Per-column symbol state (TYPE_SYMBOL only). Null when unused.
        private PinnedAppendBuffer? _auxBuffer;
        private Dictionary<string, int>? _symbolDict;
        private List<string>? _symbolList;
        private bool _storeGlobalSymbolIdsOnly;
        private int _maxGlobalSymbolId = -1;

        // Per-column array state (TYPE_DOUBLE_ARRAY / TYPE_LONG_ARRAY only).
        // Java uses growable native byte[] / int[]; .NET uses List<T> for simplicity —
        // arrays are not the hot path for QWP and the GC overhead is dominated by the
        // value boxing avoided here.
        private List<byte>? _arrayDims;
        private List<int>? _arrayShapes;
        private List<double>? _doubleArrayData;
        private List<long>? _longArrayData;

        private int _decimalScale = -1;
        private int _geohashPrecision = -1;
        private bool _hasNulls;
        private int _size;
        private int _valueCount;

        // §1.3 — incremental wire-byte cache for SYMBOL + ARRAY paths. Other column
        // types' wire size is closed-form (valueCount * elementSize, etc.); SYMBOL
        // and ARRAY had to walk the dict / per-row lists each estimate, which made
        // EstimateEncodedDatagramSize O(rows × cols × dict_size + arrays). These
        // running totals collapse the inner loops to O(1) per estimate.
        private int _symbolDictRunningBytes;  // sum over dict entries: varintSize(utf8Len) + utf8Len
        private int _symbolIdxRunningBytes;   // sum over per-row stored idx values: varintSize(idx)
        private int _arrayBodyRunningBytes;   // sum over rows: 1 + 4*nDims + 8*element_count

        internal ColumnBuffer(string name, byte type, bool useNullBitmap, IQwpGlobalSymbolSink? globalSymbolSink = null)
        {
            Name = name;
            Type = type;
            UseNullBitmap = useNullBitmap;
            ElementSize = ElementSizeInBuffer(type);
            _globalSymbolSink = globalSymbolSink;

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
                case QwpConstants.TYPE_DOUBLE_ARRAY:
                    _arrayDims = new List<byte>();
                    _arrayShapes = new List<int>();
                    _doubleArrayData = new List<double>();
                    break;
                case QwpConstants.TYPE_LONG_ARRAY:
                    _arrayDims = new List<byte>();
                    _arrayShapes = new List<int>();
                    _longArrayData = new List<long>();
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

        /// <summary>Latched decimal scale (-1 when no decimal value has been added).</summary>
        public int DecimalScale => _decimalScale;

        /// <summary>Latched geohash precision in bits (-1 when no geohash value has been added).</summary>
        public int GeoHashPrecision => _geohashPrecision;

        /// <summary>Total bytes of UTF-8 data accumulated by <c>AddString</c> for VARCHAR / BINARY columns.</summary>
        public int StringDataSize => _stringData?.Length ?? 0;

        // Internal memory accessors used by QwpColumnWriter. Each returns a view backed by
        // the column's POH-pinned storage; the views are valid until the next mutation.
        internal ReadOnlyMemory<byte> DataMemory =>
            _dataBuffer is null ? ReadOnlyMemory<byte>.Empty : _dataBuffer.AsReadOnlyMemory(0, _dataBuffer.Length);

        internal ReadOnlyMemory<byte> StringOffsetsMemory =>
            _stringOffsets is null ? ReadOnlyMemory<byte>.Empty : _stringOffsets.AsReadOnlyMemory(0, _stringOffsets.Length);

        internal ReadOnlyMemory<byte> StringDataMemory =>
            _stringData is null ? ReadOnlyMemory<byte>.Empty : _stringData.AsReadOnlyMemory(0, _stringData.Length);

        internal ReadOnlyMemory<byte> AuxDataMemory =>
            _auxBuffer is null ? ReadOnlyMemory<byte>.Empty : _auxBuffer.AsReadOnlyMemory(0, _auxBuffer.Length);

        internal ReadOnlyMemory<byte> NullBitmapMemory(int bitmapBytes)
        {
            EnsureNullBitmapCapacity(bitmapBytes * 8);
            return _nullBitmap.AsMemory(0, bitmapBytes);
        }

        internal ReadOnlySpan<byte> ArrayDimsSpan =>
            _arrayDims is null ? ReadOnlySpan<byte>.Empty : System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_arrayDims);

        internal ReadOnlySpan<int> ArrayShapesSpan =>
            _arrayShapes is null ? ReadOnlySpan<int>.Empty : System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_arrayShapes);

        internal ReadOnlySpan<double> DoubleArrayDataSpan =>
            _doubleArrayData is null ? ReadOnlySpan<double>.Empty : System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_doubleArrayData);

        internal ReadOnlySpan<long> LongArrayDataSpan =>
            _longArrayData is null ? ReadOnlySpan<long>.Empty : System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_longArrayData);

        internal IReadOnlyList<string> SymbolList =>
            (IReadOnlyList<string>?)_symbolList ?? Array.Empty<string>();

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

        /// <summary>
        ///     Adds a Decimal64 value with the given scale. The first <c>AddDecimal64</c>
        ///     call latches the column's scale; subsequent calls at a different scale are
        ///     rescaled to match. Throws on precision loss (downscaling with a non-zero
        ///     fractional remainder) or 64-bit overflow (upscaling that exceeds long range).
        /// </summary>
        public void AddDecimal64(long unscaledValue, int scale)
        {
            long stored;
            if (_decimalScale == -1)
            {
                _decimalScale = scale;
                stored = unscaledValue;
            }
            else if (_decimalScale != scale)
            {
                stored = Rescale64(unscaledValue, scale, _decimalScale);
            }
            else
            {
                stored = unscaledValue;
            }
            _dataBuffer!.PutLong(stored);
            _valueCount++;
            _size++;
        }

        /// <summary>
        ///     Adds a Decimal128 value with the given scale. The first <c>AddDecimal128</c>
        ///     call latches the column's scale; subsequent calls at a different scale are
        ///     rescaled to match. Throws on precision loss or 128-bit overflow.
        /// </summary>
        public void AddDecimal128(long high, long low, int scale)
        {
            long storedHigh, storedLow;
            if (_decimalScale == -1)
            {
                _decimalScale = scale;
                storedHigh = high;
                storedLow = low;
            }
            else if (_decimalScale != scale)
            {
                (storedHigh, storedLow) = Rescale128(high, low, scale, _decimalScale);
            }
            else
            {
                storedHigh = high;
                storedLow = low;
            }
            _dataBuffer!.PutLong(storedHigh);
            _dataBuffer!.PutLong(storedLow);
            _valueCount++;
            _size++;
        }

        private long Rescale64(long unscaled, int fromScale, int toScale)
        {
            var diff = toScale - fromScale;
            BigInteger result;
            if (diff > 0)
            {
                result = (BigInteger)unscaled * BigInteger.Pow(10, diff);
            }
            else
            {
                var divisor = BigInteger.Pow(10, -diff);
                var quotient = BigInteger.DivRem((BigInteger)unscaled, divisor, out var remainder);
                if (!remainder.IsZero)
                {
                    throw new IngressError(Enums.ErrorCode.InvalidName,
                        $"column '{Name}' cannot rescale decimal from scale {fromScale} to {toScale} without precision loss");
                }
                result = quotient;
            }
            if (result < long.MinValue || result > long.MaxValue)
            {
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    $"Decimal64 overflow: rescaling from scale {fromScale} to {toScale} exceeds 64-bit capacity");
            }
            return (long)result;
        }

        private (long high, long low) Rescale128(long high, long low, int fromScale, int toScale)
        {
            var input = Pack128(high, low);
            var diff = toScale - fromScale;
            BigInteger result;
            if (diff > 0)
            {
                result = input * BigInteger.Pow(10, diff);
            }
            else
            {
                var divisor = BigInteger.Pow(10, -diff);
                var quotient = BigInteger.DivRem(input, divisor, out var remainder);
                if (!remainder.IsZero)
                {
                    throw new IngressError(Enums.ErrorCode.InvalidName,
                        $"column '{Name}' cannot rescale decimal from scale {fromScale} to {toScale} without precision loss");
                }
                result = quotient;
            }
            var max128 = (BigInteger.One << 127) - 1;
            var min128 = -(BigInteger.One << 127);
            if (result < min128 || result > max128)
            {
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    $"Decimal128 overflow: rescaling from scale {fromScale} to {toScale} exceeds 128-bit capacity");
            }
            return Unpack128(result);
        }

        private static BigInteger Pack128(long high, long low) =>
            ((BigInteger)high << 64) | (BigInteger)(ulong)low;

        private static (long high, long low) Unpack128(BigInteger value)
        {
            var lowMask = (BigInteger.One << 64) - 1;
            var lowBig = value & lowMask;
            var high = (long)(value >> 64);
            var low = (long)(ulong)lowBig;
            return (high, low);
        }

        /// <summary>Snapshot of per-row dimensionality entries for an array column.</summary>
        public byte[] GetArrayDims() => _arrayDims?.ToArray() ?? Array.Empty<byte>();

        /// <summary>Snapshot of accumulated shape ints for an array column.</summary>
        public int[] GetArrayShapes() => _arrayShapes?.ToArray() ?? Array.Empty<int>();

        /// <summary>Snapshot of the flat double-array column data.</summary>
        public double[] GetDoubleArrayData() => _doubleArrayData?.ToArray() ?? Array.Empty<double>();

        /// <summary>Snapshot of the flat long-array column data.</summary>
        public long[] GetLongArrayData() => _longArrayData?.ToArray() ?? Array.Empty<long>();

        public void AddDoubleArray(double[]? values)
        {
            if (values is null) { AddNull(); return; }
            _arrayDims!.Add(1);
            _arrayShapes!.Add(values.Length);
            for (var i = 0; i < values.Length; i++) _doubleArrayData!.Add(values[i]);
            // §1.3 — per-row wire cost: 1 byte ndims + 4*ndims shape + 8*element_count.
            _arrayBodyRunningBytes += 1 + 4 * 1 + 8 * values.Length;
            _valueCount++;
            _size++;
        }

        public void AddDoubleArray(double[,]? values)
        {
            if (values is null) { AddNull(); return; }
            var dim0 = values.GetLength(0);
            var dim1 = values.GetLength(1);
            _arrayDims!.Add(2);
            _arrayShapes!.Add(dim0);
            _arrayShapes!.Add(dim1);
            for (var i = 0; i < dim0; i++)
            {
                for (var j = 0; j < dim1; j++) _doubleArrayData!.Add(values[i, j]);
            }
            _arrayBodyRunningBytes += 1 + 4 * 2 + 8 * dim0 * dim1;
            _valueCount++;
            _size++;
        }

        public void AddLongArray(long[]? values)
        {
            if (values is null) { AddNull(); return; }
            _arrayDims!.Add(1);
            _arrayShapes!.Add(values.Length);
            for (var i = 0; i < values.Length; i++) _longArrayData!.Add(values[i]);
            _arrayBodyRunningBytes += 1 + 4 * 1 + 8 * values.Length;
            _valueCount++;
            _size++;
        }

        public void AddLongArray(long[,]? values)
        {
            if (values is null) { AddNull(); return; }
            var dim0 = values.GetLength(0);
            var dim1 = values.GetLength(1);
            _arrayDims!.Add(2);
            _arrayShapes!.Add(dim0);
            _arrayShapes!.Add(dim1);
            for (var i = 0; i < dim0; i++)
            {
                for (var j = 0; j < dim1; j++) _longArrayData!.Add(values[i, j]);
            }
            _arrayBodyRunningBytes += 1 + 4 * 2 + 8 * dim0 * dim1;
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

        /// <summary>
        ///     Number of entries in the per-column local symbol dictionary (zero when in
        ///     global-IDs-only mode or when the column is empty).
        /// </summary>
        public int SymbolDictionarySize => _symbolList?.Count ?? 0;

        /// <summary>Largest global symbol ID seen on this column, or -1 if no global IDs were written.</summary>
        public int MaxGlobalSymbolId => _maxGlobalSymbolId;

        /// <summary>Whether the column is currently in "global IDs only" mode (no local dict).</summary>
        public bool StoreGlobalSymbolIdsOnly => _storeGlobalSymbolIdsOnly;

        /// <summary>
        ///     Snapshot of the per-column local symbol dictionary in insertion order.
        ///     Returns an empty array when in global-IDs-only mode or when the column is empty.
        /// </summary>
        public string[] GetSymbolDictionary() =>
            _symbolList is null ? Array.Empty<string>() : _symbolList.ToArray();

        /// <summary>Read-only span over the column's data buffer (the bytes written so far).</summary>
        public ReadOnlySpan<byte> GetDataReadOnlySpan() =>
            _dataBuffer is null ? ReadOnlySpan<byte>.Empty : _dataBuffer.AsReadOnlySpan(0, _dataBuffer.Length);

        /// <summary>
        ///     Read-only span over the per-column auxiliary buffer (used to record global IDs
        ///     when the column is in mixed local+global mode). Empty when the column is in
        ///     local-only or global-only mode.
        /// </summary>
        public ReadOnlySpan<byte> GetAuxDataReadOnlySpan() =>
            _auxBuffer is null ? ReadOnlySpan<byte>.Empty : _auxBuffer.AsReadOnlySpan(0, _auxBuffer.Length);

        public void AddSymbol(string? value)
        {
            if (value is null)
            {
                AddNull();
                return;
            }
            if (_globalSymbolSink is not null)
            {
                var globalId = _globalSymbolSink.GetOrAddGlobalSymbol(value);
                AddSymbolWithGlobalId(value, globalId);
                return;
            }
            if (_storeGlobalSymbolIdsOnly)
            {
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    $"column '{Name}' cannot mix global symbol IDs with local symbol dictionary values");
            }
            var idx = GetOrAddLocalSymbol(value);
            _dataBuffer!.PutInt(idx);
            _symbolIdxRunningBytes += VarintSizeStatic(idx);
            _valueCount++;
            _size++;
        }

        public void AddSymbolUtf8(ReadOnlySpan<byte> utf8Bytes)
        {
            if (utf8Bytes.IsEmpty)
            {
                // Java treats negative length as null, otherwise empty string is a real value.
                // .NET's empty span is unambiguous — treat it as a real (zero-length) symbol.
                AddSymbol(string.Empty);
                return;
            }

            string decoded;
            try
            {
                decoded = StrictUtf8.GetString(utf8Bytes);
            }
            catch (DecoderFallbackException ex)
            {
                throw new IngressError(Enums.ErrorCode.InvalidName,
                    $"cannot convert invalid UTF-8 sequence to UTF-16 for column '{Name}'", ex);
            }
            AddSymbol(decoded);
        }

        public void AddSymbolWithGlobalId(string? value, int globalId)
        {
            if (value is null)
            {
                AddNull();
                return;
            }

            if (!_storeGlobalSymbolIdsOnly)
            {
                if (_symbolList is { Count: > 0 })
                {
                    // Mixed mode: record the local index in dataBuffer + the global ID in
                    // the aux buffer, so the encoder can ship the local dict + a mapping
                    // from local IDs to global IDs.
                    var localIdx = GetOrAddLocalSymbol(value);
                    _dataBuffer!.PutInt(localIdx);
                    _symbolIdxRunningBytes += VarintSizeStatic(localIdx);
                    _auxBuffer ??= new PinnedAppendBuffer(64);
                    _auxBuffer.PutInt(globalId);
                    if (globalId > _maxGlobalSymbolId) _maxGlobalSymbolId = globalId;
                    _valueCount++;
                    _size++;
                    return;
                }
                _storeGlobalSymbolIdsOnly = true;
            }
            _dataBuffer!.PutInt(globalId);
            // In global-only mode the wire emits varint(globalId) per row from the
            // aux/data buffer; track the running varint byte total accordingly.
            _symbolIdxRunningBytes += VarintSizeStatic(globalId);
            if (globalId > _maxGlobalSymbolId) _maxGlobalSymbolId = globalId;
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
                    case QwpConstants.TYPE_DECIMAL64:
                        _dataBuffer!.PutLong(long.MinValue); // DECIMAL64_NULL
                        break;
                    case QwpConstants.TYPE_DECIMAL128:
                        _dataBuffer!.PutLong(long.MinValue); // DECIMAL128_HI_NULL
                        _dataBuffer!.PutLong(0L);            // DECIMAL128_LO_NULL
                        break;
                    case QwpConstants.TYPE_DECIMAL256:
                        _dataBuffer!.PutLong(long.MinValue); // DECIMAL256_HH_NULL
                        _dataBuffer!.PutLong(0L);
                        _dataBuffer!.PutLong(0L);
                        _dataBuffer!.PutLong(0L);
                        break;
                    case QwpConstants.TYPE_VARCHAR:
                    case QwpConstants.TYPE_BINARY:
                        _stringOffsets!.PutInt(CheckedStringOffset(_stringData!.Length));
                        break;
                    case QwpConstants.TYPE_DOUBLE_ARRAY:
                    case QwpConstants.TYPE_LONG_ARRAY:
                        // Empty 1D array sentinel: 1 dim, shape=0, no data.
                        _arrayDims!.Add(1);
                        _arrayShapes!.Add(0);
                        break;
                    default:
                        throw new IngressError(Enums.ErrorCode.InvalidName,
                            $"AddNull: unsupported column type 0x{Type:X2}");
                }
                _valueCount++;
            }
            _size++;
        }

        /// <summary>
        ///     Returns the wire-byte size this column would emit when encoded into a
        ///     <paramref name="rowCount"/>-row datagram. Mirrors the byte counts in
        ///     <see cref="QwpColumnWriter"/>'s non-Gorilla / non-global-symbol path.
        /// </summary>
        internal int EstimateWireBytes(int rowCount)
        {
            // Null header: 1 flag byte + (rowCount + 7) / 8 bytes of bitmap when any
            // nulls exist. When called before NextRow with rowCount > _size, the
            // padding NextRow would apply turns the column nullable in flight, so
            // anticipate that by treating "_size < rowCount" as "will have nulls".
            var size = 1;
            var willHaveNulls = _hasNulls || _size < rowCount;
            if (UseNullBitmap && willHaveNulls)
            {
                size += (rowCount + 7) / 8;
            }

            var nonNullCount = UseNullBitmap ? _valueCount : rowCount;

            switch (Type)
            {
                case QwpConstants.TYPE_BOOLEAN:
                    size += (nonNullCount + 7) / 8; // bit-packed on the wire
                    break;
                case QwpConstants.TYPE_BYTE:
                    size += nonNullCount;
                    break;
                case QwpConstants.TYPE_SHORT:
                case QwpConstants.TYPE_CHAR:
                    size += nonNullCount * 2;
                    break;
                case QwpConstants.TYPE_INT:
                case QwpConstants.TYPE_FLOAT:
                    size += nonNullCount * 4;
                    break;
                case QwpConstants.TYPE_LONG:
                case QwpConstants.TYPE_DOUBLE:
                case QwpConstants.TYPE_DATE:
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                    size += nonNullCount * 8;
                    break;
                case QwpConstants.TYPE_UUID:
                    size += nonNullCount * 16;
                    break;
                case QwpConstants.TYPE_LONG256:
                    size += nonNullCount * 32;
                    break;
                case QwpConstants.TYPE_DECIMAL64:
                    size += 1 + nonNullCount * 8; // scale byte + values
                    break;
                case QwpConstants.TYPE_DECIMAL128:
                    size += 1 + nonNullCount * 16;
                    break;
                case QwpConstants.TYPE_DECIMAL256:
                    size += 1 + nonNullCount * 32;
                    break;
                case QwpConstants.TYPE_GEOHASH:
                {
                    var precision = _geohashPrecision > 0 ? _geohashPrecision : 1;
                    size += VarintSizeStatic(precision);
                    size += nonNullCount * ((precision + 7) / 8);
                    break;
                }
                case QwpConstants.TYPE_VARCHAR:
                case QwpConstants.TYPE_BINARY:
                    // (nonNullCount + 1) int32 offsets + actual UTF-8 / opaque bytes.
                    size += 4 * (nonNullCount + 1) + (_stringData?.Length ?? 0);
                    break;
                case QwpConstants.TYPE_SYMBOL:
                    size += EstimateSymbolWireBytes(nonNullCount);
                    break;
                case QwpConstants.TYPE_DOUBLE_ARRAY:
                case QwpConstants.TYPE_LONG_ARRAY:
                    size += EstimateArrayWireBytes();
                    break;
                default:
                    // Unknown type — be safe by reporting a large estimate so the caller
                    // pre-flushes rather than under-counting.
                    size += nonNullCount * 32;
                    break;
            }
            return size;
        }

        private int EstimateSymbolWireBytes(int nonNullCount)
        {
            // §1.3 — uses running totals maintained on dict/Add: O(1) per call. Was
            // previously O(dict_size + nonNullCount).
            // Wire layout: dict_size_varint + per-entry(varint_len + utf8_bytes)
            //            + per-value varint id (one per non-null row).
            // _symbolDictRunningBytes covers the per-entry block; _symbolIdxRunningBytes
            // covers the per-row varints. The dict_size_varint is computed inline since
            // it depends only on the cached dict count.
            //
            // nonNullCount is the row-count parameter. The running idx total tracks
            // per-Add bytes, which equals nonNullCount-worth of bytes when called
            // post-NextRow (the common case). Pre-NextRow with nonNullCount > _valueCount
            // is the non-null-bitmap edge case (rare; see CommitCurrentRow note above).
            return VarintSizeStatic(_symbolList?.Count ?? 0)
                   + _symbolDictRunningBytes
                   + _symbolIdxRunningBytes;
        }

        private int EstimateArrayWireBytes()
        {
            // §1.3 — running total maintained on each AddDoubleArray/AddLongArray.
            return _arrayBodyRunningBytes;
        }

        private static int VarintSizeStatic(long value)
        {
            var v = (ulong)value;
            var bytes = 1;
            while (v >= 0x80) { v >>= 7; bytes++; }
            return bytes;
        }

        /// <summary>
        ///     Captures the in-progress row's per-column data into an
        ///     <see cref="InProgressSnapshot"/> without mutating the column. The
        ///     returned struct's <see cref="InProgressSnapshot.HasValue"/> is false when
        ///     no in-progress data exists. Pair with
        ///     <see cref="RestoreInProgressRowState"/> to re-apply on a freshly-reset
        ///     column on the flush-and-batch path.
        /// </summary>
        /// <param name="committedRowCount">
        ///     Current <see cref="QwpTableBuffer.RowCount"/>. Stored values at index
        ///     <paramref name="committedRowCount"/> belong to the in-progress row.
        /// </param>
        internal InProgressSnapshot CaptureInProgressRowState(int committedRowCount)
        {
            if (_size <= committedRowCount) return default;
            // _size > committedRowCount means the user's current row wrote a value to
            // this column. By construction (first-value-wins on duplicate writes) the
            // in-progress row writes at most one value per column, so _size is exactly
            // committedRowCount + 1 here.
            switch (Type)
            {
                case QwpConstants.TYPE_BOOLEAN:
                {
                    var raw = _dataBuffer!.AsReadOnlySpan(committedRowCount, 1)[0];
                    return new InProgressSnapshot { WireType = Type, Long0 = raw != 0 ? 1L : 0L };
                }
                case QwpConstants.TYPE_BYTE:
                {
                    var raw = _dataBuffer!.AsReadOnlySpan(committedRowCount, 1)[0];
                    return new InProgressSnapshot { WireType = Type, Long0 = raw };
                }
                case QwpConstants.TYPE_SHORT:
                case QwpConstants.TYPE_CHAR:
                {
                    var v = BinaryPrimitives.ReadInt16LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 2, 2));
                    return new InProgressSnapshot { WireType = Type, Long0 = v };
                }
                case QwpConstants.TYPE_INT:
                {
                    var v = BinaryPrimitives.ReadInt32LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 4, 4));
                    return new InProgressSnapshot { WireType = Type, Long0 = v };
                }
                case QwpConstants.TYPE_FLOAT:
                {
                    // Carry float bits in the long slot to keep the snapshot allocation-free.
                    var bits = BinaryPrimitives.ReadInt32LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 4, 4));
                    return new InProgressSnapshot { WireType = Type, Long0 = bits };
                }
                case QwpConstants.TYPE_LONG:
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                case QwpConstants.TYPE_DATE:
                {
                    var v = BinaryPrimitives.ReadInt64LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 8, 8));
                    return new InProgressSnapshot { WireType = Type, Long0 = v };
                }
                case QwpConstants.TYPE_DOUBLE:
                {
                    var bits = BinaryPrimitives.ReadInt64LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 8, 8));
                    return new InProgressSnapshot { WireType = Type, Long0 = bits };
                }
                case QwpConstants.TYPE_UUID:
                {
                    var lo = BinaryPrimitives.ReadInt64LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 16, 8));
                    var hi = BinaryPrimitives.ReadInt64LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 16 + 8, 8));
                    return new InProgressSnapshot { WireType = Type, Long0 = lo, Long1 = hi };
                }
                case QwpConstants.TYPE_LONG256:
                {
                    var off = committedRowCount * 32;
                    return new InProgressSnapshot
                    {
                        WireType = Type,
                        Long0 = BinaryPrimitives.ReadInt64LittleEndian(_dataBuffer!.AsReadOnlySpan(off, 8)),
                        Long1 = BinaryPrimitives.ReadInt64LittleEndian(_dataBuffer.AsReadOnlySpan(off + 8, 8)),
                        Long2 = BinaryPrimitives.ReadInt64LittleEndian(_dataBuffer.AsReadOnlySpan(off + 16, 8)),
                        Long3 = BinaryPrimitives.ReadInt64LittleEndian(_dataBuffer.AsReadOnlySpan(off + 24, 8)),
                    };
                }
                case QwpConstants.TYPE_DECIMAL64:
                {
                    var v = BinaryPrimitives.ReadInt64LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 8, 8));
                    return new InProgressSnapshot { WireType = Type, Long0 = v, IntAux = _decimalScale };
                }
                case QwpConstants.TYPE_DECIMAL128:
                {
                    var off = committedRowCount * 16;
                    return new InProgressSnapshot
                    {
                        WireType = Type,
                        Long0 = BinaryPrimitives.ReadInt64LittleEndian(_dataBuffer!.AsReadOnlySpan(off, 8)),       // hi
                        Long1 = BinaryPrimitives.ReadInt64LittleEndian(_dataBuffer.AsReadOnlySpan(off + 8, 8)),    // lo
                        IntAux = _decimalScale,
                    };
                }
                case QwpConstants.TYPE_GEOHASH:
                {
                    var v = BinaryPrimitives.ReadInt64LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 8, 8));
                    return new InProgressSnapshot { WireType = Type, Long0 = v, IntAux = _geohashPrecision };
                }
                case QwpConstants.TYPE_VARCHAR:
                case QwpConstants.TYPE_BINARY:
                {
                    var startOff = BinaryPrimitives.ReadInt32LittleEndian(
                        _stringOffsets!.AsReadOnlySpan(committedRowCount * 4, 4));
                    var endOff = BinaryPrimitives.ReadInt32LittleEndian(
                        _stringOffsets.AsReadOnlySpan((committedRowCount + 1) * 4, 4));
                    var s = endOff > startOff
                        ? Encoding.UTF8.GetString(_stringData!.AsReadOnlySpan(startOff, endOff - startOff))
                        : string.Empty;
                    return new InProgressSnapshot { WireType = Type, StringRef = s };
                }
                case QwpConstants.TYPE_SYMBOL:
                {
                    var idx = BinaryPrimitives.ReadInt32LittleEndian(
                        _dataBuffer!.AsReadOnlySpan(committedRowCount * 4, 4));
                    return new InProgressSnapshot { WireType = Type, StringRef = _symbolList![idx] };
                }
                case QwpConstants.TYPE_DOUBLE_ARRAY:
                case QwpConstants.TYPE_LONG_ARRAY:
                    return CaptureArrayInProgressRow();
                default:
                    throw new IngressError(Enums.ErrorCode.InvalidName,
                        $"CaptureInProgressRowState: unsupported column type 0x{Type:X2} on column '{Name}'");
            }
        }

        /// <summary>
        ///     Re-applies a captured in-progress-row snapshot onto a freshly-reset column.
        ///     No-op when <see cref="InProgressSnapshot.HasValue"/> is false. Re-establishes
        ///     per-type metadata that <see cref="TruncateTo">TruncateTo(0)</see> reset
        ///     (decimal scale, geohash precision, symbol dict).
        /// </summary>
        internal void RestoreInProgressRowState(in InProgressSnapshot snapshot)
        {
            if (!snapshot.HasValue) return;
            switch (snapshot.WireType)
            {
                case QwpConstants.TYPE_BOOLEAN:
                    AddBoolean(snapshot.Long0 != 0);
                    break;
                case QwpConstants.TYPE_BYTE:
                    AddByte((byte)snapshot.Long0);
                    break;
                case QwpConstants.TYPE_SHORT:
                case QwpConstants.TYPE_CHAR:
                    AddShort((short)snapshot.Long0);
                    break;
                case QwpConstants.TYPE_INT:
                    AddInt((int)snapshot.Long0);
                    break;
                case QwpConstants.TYPE_FLOAT:
                {
                    Span<byte> tmp = stackalloc byte[4];
                    BinaryPrimitives.WriteInt32LittleEndian(tmp, (int)snapshot.Long0);
                    AddFloat(BinaryPrimitives.ReadSingleLittleEndian(tmp));
                    break;
                }
                case QwpConstants.TYPE_LONG:
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                case QwpConstants.TYPE_DATE:
                    AddLong(snapshot.Long0);
                    break;
                case QwpConstants.TYPE_DOUBLE:
                {
                    Span<byte> tmp = stackalloc byte[8];
                    BinaryPrimitives.WriteInt64LittleEndian(tmp, snapshot.Long0);
                    AddDouble(BinaryPrimitives.ReadDoubleLittleEndian(tmp));
                    break;
                }
                case QwpConstants.TYPE_UUID:
                    AddLong(snapshot.Long0); AddLong(snapshot.Long1);
                    break;
                case QwpConstants.TYPE_LONG256:
                    AddLong(snapshot.Long0); AddLong(snapshot.Long1);
                    AddLong(snapshot.Long2); AddLong(snapshot.Long3);
                    break;
                case QwpConstants.TYPE_DECIMAL64:
                    AddDecimal64(snapshot.Long0, snapshot.IntAux);
                    break;
                case QwpConstants.TYPE_DECIMAL128:
                    AddDecimal128(snapshot.Long0, snapshot.Long1, snapshot.IntAux);
                    break;
                case QwpConstants.TYPE_GEOHASH:
                    AddGeoHash(snapshot.Long0, snapshot.IntAux);
                    break;
                case QwpConstants.TYPE_VARCHAR:
                case QwpConstants.TYPE_BINARY:
                    AddString(snapshot.StringRef);
                    break;
                case QwpConstants.TYPE_SYMBOL:
                    AddSymbol(snapshot.StringRef);
                    break;
                case QwpConstants.TYPE_DOUBLE_ARRAY:
                    RestoreDoubleArrayInProgressRow(in snapshot);
                    break;
                case QwpConstants.TYPE_LONG_ARRAY:
                    RestoreLongArrayInProgressRow(in snapshot);
                    break;
                default:
                    throw new IngressError(Enums.ErrorCode.InvalidName,
                        $"RestoreInProgressRowState: unsupported wire type 0x{snapshot.WireType:X2} on column '{Name}'");
            }
        }

        private InProgressSnapshot CaptureArrayInProgressRow()
        {
            // The in-progress array entry is the last one in the dims/shapes/data lists.
            var nDims = _arrayDims![_arrayDims.Count - 1];
            var shapeStart = 0;
            for (var i = 0; i < _arrayDims.Count - 1; i++) shapeStart += _arrayDims[i];
            var shape = _arrayShapes!.GetRange(shapeStart, nDims).ToArray();
            var elemCount = 1;
            for (var d = 0; d < nDims; d++) elemCount *= shape[d];
            if (Type == QwpConstants.TYPE_DOUBLE_ARRAY)
            {
                var dataStart = _doubleArrayData!.Count - elemCount;
                return new InProgressSnapshot
                {
                    WireType = Type,
                    IntAux = nDims,
                    ArrayShape = shape,
                    DoubleArrayData = _doubleArrayData.GetRange(dataStart, elemCount).ToArray(),
                };
            }
            {
                var dataStart = _longArrayData!.Count - elemCount;
                return new InProgressSnapshot
                {
                    WireType = Type,
                    IntAux = nDims,
                    ArrayShape = shape,
                    LongArrayData = _longArrayData.GetRange(dataStart, elemCount).ToArray(),
                };
            }
        }

        private void RestoreDoubleArrayInProgressRow(in InProgressSnapshot snapshot)
        {
            switch (snapshot.IntAux)
            {
                case 1:
                    AddDoubleArray(snapshot.DoubleArrayData);
                    break;
                case 2:
                {
                    var shape = snapshot.ArrayShape!;
                    var data = snapshot.DoubleArrayData!;
                    var arr = new double[shape[0], shape[1]];
                    var idx = 0;
                    for (var i = 0; i < shape[0]; i++)
                    for (var j = 0; j < shape[1]; j++)
                        arr[i, j] = data[idx++];
                    AddDoubleArray(arr);
                    break;
                }
                default:
                    throw new IngressError(Enums.ErrorCode.InvalidName,
                        $"RestoreInProgressRowState: unsupported array rank {snapshot.IntAux} on column '{Name}'");
            }
        }

        private void RestoreLongArrayInProgressRow(in InProgressSnapshot snapshot)
        {
            switch (snapshot.IntAux)
            {
                case 1:
                    AddLongArray(snapshot.LongArrayData);
                    break;
                case 2:
                {
                    var shape = snapshot.ArrayShape!;
                    var data = snapshot.LongArrayData!;
                    var arr = new long[shape[0], shape[1]];
                    var idx = 0;
                    for (var i = 0; i < shape[0]; i++)
                    for (var j = 0; j < shape[1]; j++)
                        arr[i, j] = data[idx++];
                    AddLongArray(arr);
                    break;
                }
                default:
                    throw new IngressError(Enums.ErrorCode.InvalidName,
                        $"RestoreInProgressRowState: unsupported array rank {snapshot.IntAux} on column '{Name}'");
            }
        }

        /// <summary>
        ///     Drops every committed row from this column while preserving the
        ///     in-progress row's value (if any). Convenience wrapper around
        ///     <see cref="CaptureInProgressRowState"/> + <see cref="TruncateTo">TruncateTo(0)</see>
        ///     + <see cref="RestoreInProgressRowState"/>. Used by the
        ///     <see cref="QwpTableBuffer.RetainInProgressRow"/> table-level helper that
        ///     callers (and tests) keep using as a single op.
        /// </summary>
        internal void RetainInProgressRow(int committedRowCount)
        {
            // No in-progress data to preserve — fast path.
            if (_size <= committedRowCount)
            {
                TruncateTo(0);
                return;
            }

            // §1.9: capture-truncate-restore replaces the per-type switch body that
            // earlier PRs duplicated alongside the public Capture/Restore methods.
            // Both code paths produce identical state; the switch-based body has
            // been removed so future column types only need to be added in one place.
            var snapshot = CaptureInProgressRowState(committedRowCount);
            TruncateTo(0);
            RestoreInProgressRowState(in snapshot);
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

            // Rewind aux buffer (mixed-mode global IDs).
            if (_auxBuffer is not null)
            {
                _auxBuffer.JumpTo(newValueCount * 4);
            }

            // Rewind array state by walking the retained values to recompute shape and
            // data offsets, then truncating the lists.
            if (_arrayDims is not null)
            {
                var newShapeCount = 0;
                var newDataCount = 0;
                for (var i = 0; i < newValueCount; i++)
                {
                    var nDims = _arrayDims[i];
                    var elemCount = 1;
                    for (var d = 0; d < nDims; d++)
                    {
                        elemCount *= _arrayShapes![newShapeCount++];
                    }
                    newDataCount += elemCount;
                }
                if (_arrayDims.Count > newValueCount)
                {
                    _arrayDims.RemoveRange(newValueCount, _arrayDims.Count - newValueCount);
                }
                if (_arrayShapes!.Count > newShapeCount)
                {
                    _arrayShapes.RemoveRange(newShapeCount, _arrayShapes.Count - newShapeCount);
                }
                if (_doubleArrayData is not null && _doubleArrayData.Count > newDataCount)
                {
                    _doubleArrayData.RemoveRange(newDataCount, _doubleArrayData.Count - newDataCount);
                }
                if (_longArrayData is not null && _longArrayData.Count > newDataCount)
                {
                    _longArrayData.RemoveRange(newDataCount, _longArrayData.Count - newDataCount);
                }
            }

            // Type-specific metadata reverts to "freshly created" when truncated to zero.
            if (newValueCount == 0)
            {
                _decimalScale = -1;
                _geohashPrecision = -1;
                _storeGlobalSymbolIdsOnly = false;
                _maxGlobalSymbolId = -1;
                _symbolDict?.Clear();
                _symbolList?.Clear();
                _symbolDictRunningBytes = 0;
                _symbolIdxRunningBytes = 0;
                _arrayBodyRunningBytes = 0;
            }
            else
            {
                // §1.3 — partial truncate: rebuild the running totals by walking the
                // retained slice. TruncateTo is uncommon (only PR D's flush path uses
                // it); the O(retained) cost is acceptable.
                RecomputeRunningWireBytes();
            }
        }

        // Walks the retained per-row state to refresh the §1.3 running totals after
        // a partial truncate. Reset() and the truncate-to-zero path skip this in
        // favour of clearing the totals to 0 directly.
        private void RecomputeRunningWireBytes()
        {
            // Symbol dict bytes don't change on truncate (the dict survives), so the
            // running total is unchanged — a partial truncate keeps the same
            // _symbolList. Per-row idx bytes need a re-walk.
            if (Type == QwpConstants.TYPE_SYMBOL && _dataBuffer is not null)
            {
                _symbolIdxRunningBytes = 0;
                var bytes = _dataBuffer.AsReadOnlySpan(0, _valueCount * 4);
                for (var i = 0; i < _valueCount; i++)
                {
                    var id = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(i * 4, 4));
                    _symbolIdxRunningBytes += VarintSizeStatic(id);
                }
            }
            else if ((Type == QwpConstants.TYPE_DOUBLE_ARRAY || Type == QwpConstants.TYPE_LONG_ARRAY)
                     && _arrayDims is not null && _arrayShapes is not null)
            {
                _arrayBodyRunningBytes = 0;
                var shapeCursor = 0;
                for (var i = 0; i < _arrayDims.Count; i++)
                {
                    var nDims = _arrayDims[i];
                    var elemCount = 1;
                    for (var d = 0; d < nDims; d++) elemCount *= _arrayShapes[shapeCursor++];
                    _arrayBodyRunningBytes += 1 + 4 * nDims + 8 * elemCount;
                }
            }
        }

        internal void Reset()
        {
            _size = 0;
            _valueCount = 0;
            _hasNulls = false;
            _dataBuffer?.Truncate();
            _auxBuffer?.Truncate();
            if (_stringOffsets is not null)
            {
                _stringOffsets.Truncate();
                _stringOffsets.PutInt(0); // re-seed initial 0 offset
            }
            _stringData?.Truncate();
            if (_nullBitmap.Length > 0) Array.Clear(_nullBitmap, 0, _nullBitmap.Length);
            _arrayDims?.Clear();
            _arrayShapes?.Clear();
            _doubleArrayData?.Clear();
            _longArrayData?.Clear();
            _decimalScale = -1;
            _geohashPrecision = -1;
            _storeGlobalSymbolIdsOnly = false;
            _maxGlobalSymbolId = -1;
            _symbolDict?.Clear();
            _symbolList?.Clear();
            _symbolDictRunningBytes = 0;
            _symbolIdxRunningBytes = 0;
            _arrayBodyRunningBytes = 0;
        }

        private int GetOrAddLocalSymbol(string value)
        {
            _symbolDict ??= new Dictionary<string, int>(StringComparer.Ordinal);
            _symbolList ??= new List<string>();
            if (_symbolDict.TryGetValue(value, out var existing)) return existing;
            var idx = _symbolList.Count;
            _symbolDict[value] = idx;
            _symbolList.Add(value);
            // §1.3 — track the new dict entry's wire-byte cost so the estimator can
            // skip the per-call dict walk.
            var entryBytes = Encoding.UTF8.GetByteCount(value);
            _symbolDictRunningBytes += VarintSizeStatic(entryBytes) + entryBytes;
            return idx;
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
