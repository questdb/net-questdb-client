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

#if NET7_0_OR_GREATER

using QuestDB.Enums;
using QuestDB.Pooling;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace QuestDB.Senders;

/// <summary>
///     ISender implementation backed by the WebSocket transport and QWP v1 columnar binary protocol.
/// </summary>
/// <remarks>
///     The sender always routes frames through <see cref="QwpCursorSendEngine" /> regardless of
///     whether <c>sf_dir</c> is set. With <c>sf_dir</c>, segments are mmap'd files and survive
///     restarts; without, segments are RAM only. Both modes pipeline async I/O, replay un-acked
///     frames on transient WS failures, and only terminate on permanent errors (auth, upgrade
///     reject, protocol violation, or reconnect-budget exhaustion).
/// </remarks>
internal sealed class QwpWebSocketSender : IQwpWebSocketSender, IPooledSlotSender, IPooledTransactionalSender
{
    private const long TicksPerMicrosecond = 10L;
    private const int EncoderInitialCapacity = 1 << 16;

    private readonly Dictionary<string, QwpTableBuffer> _tables = new(StringComparer.Ordinal);
#if NET9_0_OR_GREATER
    private readonly Dictionary<string, QwpTableBuffer>.AlternateLookup<ReadOnlySpan<char>> _tablesLookup;
#endif
    private readonly QwpSymbolDictionary _symbolDictionary;
    private readonly List<QwpTableBuffer> _flushBatch = new();
    private readonly QwpEncoder.FrameBuilder _encoderBuffer;

    private readonly QwpSlotLock? _slotLock;
    private readonly QwpCursorSendEngine _engine;
    private readonly QwpBackgroundDrainerPool? _drainerPool;
    private readonly QwpSenderErrorDispatcher? _errorDispatcher;
    private readonly QwpConnectionEventDispatcher? _connectionEventDispatcher;
    private readonly QwpTlsAuth.CertificateValidator? _certValidator;

    private readonly Dictionary<string, long> _committedSeqTxn = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _durableSeqTxn = new(StringComparer.Ordinal);
    private readonly object _seqTxnLock = new();

    private readonly bool _convertLocalToUtc;

    private QwpTableBuffer? _currentTable;
    private int _disposed;
    private int _runningRowCount;
    private long _pendingBytes;
    private long _currentTableSnapshotBytes;
    private int _currentBatchMaxSymbolId = -1;

    // Transactional (defer-commit) mode: auto-flush frames carry FLAG_DEFER_COMMIT; an explicit
    // commit ships a non-deferred frame. _hasDeferredMessages tracks whether a commit is owed.
    private readonly bool _transactional;
    private bool _hasDeferredMessages;

    public QwpWebSocketSender(SenderOptions options)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        if (!options.IsWebSocket())
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"protocol must be ws or wss for {nameof(QwpWebSocketSender)}, got {options.protocol}");
        }

        _convertLocalToUtc = options.convert_local_to_utc;
        _transactional = options.transaction;

        _symbolDictionary = new QwpSymbolDictionary();
#if NET9_0_OR_GREATER
        _tablesLookup = _tables.GetAlternateLookup<ReadOnlySpan<char>>();
#endif
        _encoderBuffer = new QwpEncoder.FrameBuilder(EncoderInitialCapacity);

        (_slotLock, _engine, _drainerPool, _errorDispatcher, _connectionEventDispatcher, _certValidator) = BuildEngineStack(options);
        _engine.SetTableEntryHandler(UpdateSeqTxnFromAck);
    }

    /// <summary>
    ///     True when there is no slot lock (RAM mode), or the slot lock the engine owns has been
    ///     released. The pool reads this after disposing this sender to decide whether the slot index
    ///     may be reused. See <see cref="IPooledSlotSender" />.
    /// </summary>
    public bool IsSlotLockReleased => _slotLock is null || _slotLock.IsReleased;

    /// <summary>
    ///     True while auto-flushed transactional rows are staged server-side awaiting a commit frame.
    ///     The pool reads this on return: staged rows cannot be rolled back, so the entry must be
    ///     discarded rather than re-pooled. See <see cref="IPooledTransactionalSender" />.
    /// </summary>
    public bool HasUncommittedDeferredRows => _hasDeferredMessages;

    private static (QwpSlotLock? slotLock, QwpCursorSendEngine engine, QwpBackgroundDrainerPool? pool, QwpSenderErrorDispatcher? dispatcher, QwpConnectionEventDispatcher? eventDispatcher, QwpTlsAuth.CertificateValidator? certValidator)
        BuildEngineStack(SenderOptions options)
    {
        var sfMode = !string.IsNullOrEmpty(options.sf_dir);
        QwpSlotLock? slotLock = null;
        QwpSegmentRing? ring = null;
        QwpAckWatermark? ackWatermark = null;
        QwpCursorSendEngine? engine = null;
        QwpBackgroundDrainerPool? pool = null;
        QwpSenderErrorDispatcher? dispatcher = null;
        QwpConnectionEventDispatcher? eventDispatcher = null;
        QwpTlsAuth.CertificateValidator? certValidator = null;

        try
        {
            if (sfMode)
            {
                var sfRoot = options.sf_dir!;
                var slotDir = Path.Combine(sfRoot, options.sender_id);
                slotLock = QwpSlotLock.Acquire(slotDir);
                ring = QwpSegmentRing.Open(slotDir, segmentCapacity: options.sf_max_bytes);
                if (ring.NextFsn == 0)
                {
                    // Clear any stale watermark from a prior session that left no segments behind.
                    QwpAckWatermark.RemoveOrphan(slotDir);
                }
                ackWatermark = QwpAckWatermark.Open(slotDir);
            }
            else
            {
                ring = QwpSegmentRing.OpenMemoryBacked(segmentCapacity: options.sf_max_bytes);
            }

            var authHeader = BuildAuthHeader(options);
            certValidator = BuildCertificateValidator(options);
            var certCallback = certValidator?.Callback;
            var tracker = new QwpHostHealthTracker(options.addresses, clientZone: options.zone, targetIsPrimary: false);
            var transportFactory = BuildHostRotatingFactory(options, tracker, authHeader, certCallback);

            var policy = new QwpReconnectPolicy(
                options.reconnect_initial_backoff_millis,
                options.reconnect_max_backoff_millis,
                options.reconnect_max_duration_millis,
                jitter: QwpReconnectPolicy.EqualJitter);

            dispatcher = new QwpSenderErrorDispatcher(options.error_handler, options.error_inbox_capacity);

            if (options.ConnectionListener is not null)
            {
                eventDispatcher = new QwpConnectionEventDispatcher(
                    options.ConnectionListener,
                    options.connection_listener_inbox_capacity);
            }

            var capturedSink = eventDispatcher;
            engine = new QwpCursorSendEngine(
                slotLock,
                ring,
                transportFactory,
                policy,
                options.sf_append_deadline_millis,
                options.initial_connect_mode,
                maxTotalBytes: options.sf_max_total_bytes,
                skipBackoffPredicate: () => !tracker.IsRoundExhausted,
                errorDispatcher: dispatcher,
                policyResolver: options.BuildEffectivePolicyResolver(),
                durableAckMode: options.request_durable_ack,
                ackWatermark: ackWatermark,
                connectionEventSink: capturedSink is null ? null : (Action<SenderConnectionEvent>)(evt => capturedSink.Offer(evt)));

            engine.Start();

            if (options.initial_connect_mode != InitialConnectMode.async)
            {
                try
                {
                    engine.FirstConnectTask.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    var code = (ex as IngressError)?.code ?? ErrorCode.SocketError;
                    throw new IngressError(code,
                        $"first connect failed against all {options.AddressCount} configured endpoint(s): {ex.Message}", ex);
                }
            }

            if (sfMode && options.drain_orphans)
            {
                var drainer = new QwpBackgroundDrainer(
                    contextBuilder: () =>
                    {
                        var drainerTracker = new QwpHostHealthTracker(options.addresses, clientZone: options.zone, targetIsPrimary: false);
                        var drainerFactory = BuildHostRotatingFactory(
                            options, drainerTracker, authHeader, certCallback);
                        return new Qwp.Sf.DrainContext(
                            drainerFactory,
                            () => !drainerTracker.IsRoundExhausted);
                    },
                    policy,
                    segmentCapacity: options.sf_max_bytes,
                    drainTimeout: options.reconnect_max_duration_millis,
                    durableAckMode: options.request_durable_ack);
                // Orphan-drainer shutdown uses the pool's own small fixed grace; do NOT pass
                // close_flush_timeout here, or a wedged drainer adds the full flush budget to Dispose().
                pool = new QwpBackgroundDrainerPool(
                    options.max_background_drainers,
                    drainer);
                var orphans = QwpOrphanScanner.ClaimOrphans(
                    options.sf_dir!, options.sender_id,
                    options.OrphanExcludeManagedBase, options.OrphanExcludeManagedCount);
                var enqueued = 0;
                try
                {
                    for (; enqueued < orphans.Count; enqueued++)
                    {
                        pool.Enqueue(orphans[enqueued]);
                    }
                }
                catch
                {
                    for (var i = enqueued; i < orphans.Count; i++)
                    {
                        SfCleanup.Dispose(orphans[i]);
                    }
                    throw;
                }
            }

            return (slotLock, engine, pool, dispatcher, eventDispatcher, certValidator);
        }
        catch (Exception)
        {
            SfCleanup.Dispose(pool);
            SfCleanup.Dispose(engine);
            SfCleanup.Dispose(dispatcher);
            SfCleanup.Dispose(eventDispatcher);
            SfCleanup.Dispose(ring);
            SfCleanup.Dispose(ackWatermark);
            SfCleanup.Dispose(slotLock);
            SfCleanup.Dispose(certValidator);
            throw;
        }
    }

    /// <inheritdoc />
    public SenderOptions Options { get; }

    /// <inheritdoc />
    public int Length => (int)Math.Min(_pendingBytes, int.MaxValue);

    /// <inheritdoc />
    public int RowCount => _runningRowCount;

    /// <inheritdoc />
    public bool WithinTransaction => _transactional;

    /// <inheritdoc />
    public DateTime LastFlush { get; private set; } = DateTime.MinValue;
    private long _lastFlushTickCount;

    /// <inheritdoc />
    public ISender Transaction(ReadOnlySpan<char> tableName)
    {
        // WS transactions are a connection-level mode, not a per-table begin: enable `transaction=on`
        // and commit with Commit()/Send().
        throw new IngressError(ErrorCode.InvalidApiCall,
            "explicit per-table Transaction() is not supported on the WebSocket transport; set `transaction=on` and use Commit()/Send()");
    }

    /// <inheritdoc />
    public void Rollback()
    {
        // Deferred rows are already streamed to the server (appended to the WAL, awaiting commit), so
        // there is nothing to roll back client-side.
        throw new IngressError(ErrorCode.InvalidApiCall,
            "Rollback() is not supported on the WebSocket transport; deferred rows already shipped to the server cannot be rolled back");
    }

    /// <inheritdoc />
    public Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfTerminal();
        RequireTransactionalMode();
        EnsureNoRowInProgress();
        return FlushAsyncCore(deferCommit: false, ct).AsTask();
    }

    /// <inheritdoc />
    public void Commit(CancellationToken ct = default)
    {
        ThrowIfTerminal();
        RequireTransactionalMode();
        EnsureNoRowInProgress();
        FlushSync(deferCommit: false, ct);
    }

    private void RequireTransactionalMode()
    {
        if (!_transactional)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "no transaction to commit; set `transaction=on` to enable WebSocket transactional mode");
        }
    }

    /// <inheritdoc />
    public ISender Table(ReadOnlySpan<char> name)
    {
        ThrowIfTerminal();
#if NET9_0_OR_GREATER
        if (!_tablesLookup.TryGetValue(name, out var t))
        {
            var key = name.ToString();
            t = new QwpTableBuffer(key, Options.max_name_len);
            _tables[key] = t;
        }
#else
        var key = name.ToString();
        if (!_tables.TryGetValue(key, out var t))
        {
            t = new QwpTableBuffer(key, Options.max_name_len);
            _tables[key] = t;
        }
#endif
        _currentTable = t;
        _currentTableSnapshotBytes = t.GetBufferedBytes();
        return this;
    }

    /// <inheritdoc />
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        ThrowIfTerminal();
        var preCount = _symbolDictionary.Count;
        try
        {
            // Add validates a first-seen value's UTF-8 (a lone surrogate would otherwise throw from
            // the encoder on every self-sufficient flush and wedge the sender). Repeated values take
            // the dictionary's fast path, so the hot path stays free of any extra scan.
            var globalId = _symbolDictionary.Add(value);
            EnsureCurrentTable().AppendSymbol(name, globalId);
            if (globalId > _currentBatchMaxSymbolId)
            {
                _currentBatchMaxSymbolId = globalId;
            }
        }
        catch
        {
            // Drop any dict entry Add committed and abandon the in-progress row, so a rejected
            // value/name never leaves the buffer half-written. CancelCurrentRow is idempotent, so it
            // is safe even when AppendSymbol already cancelled on a bad name.
            if (_symbolDictionary.Count > preCount)
            {
                _symbolDictionary.RollbackTo(preCount);
            }
            _currentTable?.CancelCurrentRow();
            throw;
        }
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendVarchar(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendLong(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, int value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendInt(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendBool(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDouble(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendTimestampMicros(name, DateTimeToMicros(value));
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendTimestampMicros(name, DateTimeToMicros(value.UtcDateTime));
        return this;
    }

    /// <inheritdoc />
    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendTimestampNanos(name, timestampNanos);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, decimal value)
    {
        // QWP carries an explicit per-column decimal width + scale, so the scale-inferring overload is
        // not supported here — the caller must choose the type and scale.
        throw new IngressError(ErrorCode.InvalidApiCall,
            "QWP requires an explicit decimal type and scale; use ColumnDecimal64/128/256(name, value, scale)");
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, Guid value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendUuid(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, char value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendChar(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        // Reuse the shape-aware overload with a 1D shape; lets the array path do the type dispatch.
        Span<int> shape = stackalloc int[1];
        shape[0] = value.Length;
        AppendArrayDispatch(name, value, shape);
        return this;
    }

    /// <inheritdoc />
    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct
    {
        ThrowIfTerminal();
        var arr = value as T[] ?? value.ToArray();
        var shapeArr = shape as int[] ?? shape.ToArray();
        AppendArrayDispatch<T>(name, arr.AsSpan(), shapeArr.AsSpan());
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, Array value)
    {
        ThrowIfTerminal();

        var rank = value.Rank;
        if (rank < 1 || rank > QwpConstants.MaxArrayDimensions)
        {
            throw new IngressError(ErrorCode.InvalidArrayShapeError,
                $"array rank {rank} not supported; must be in [1, {QwpConstants.MaxArrayDimensions}]");
        }

        Span<int> shape = stackalloc int[rank];
        for (var i = 0; i < rank; i++)
        {
            shape[i] = value.GetLength(i);
        }

        var elementType = value.GetType().GetElementType();
        if (elementType == typeof(double))
        {
            ref var head = ref System.Runtime.InteropServices.MemoryMarshal.GetArrayDataReference(value);
            var bytes = System.Runtime.InteropServices.MemoryMarshal.CreateSpan(ref head, value.Length * sizeof(double));
            var flat = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, double>(bytes);
            EnsureCurrentTable().AppendDoubleArray(name, flat, shape);
        }
        else if (elementType == typeof(long))
        {
            ref var head = ref System.Runtime.InteropServices.MemoryMarshal.GetArrayDataReference(value);
            var bytes = System.Runtime.InteropServices.MemoryMarshal.CreateSpan(ref head, value.Length * sizeof(long));
            var flat = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes);
            EnsureCurrentTable().AppendLongArray(name, flat, shape);
        }
        else
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"array element type {elementType} not supported; only double and long");
        }

        return this;
    }

    /// <inheritdoc />
    public ISender ColumnDecimal64(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDecimal64(name, value, scale);
        return this;
    }

    /// <inheritdoc />
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDecimal128(name, value, scale);
        return this;
    }

    /// <inheritdoc />
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDecimal256(name, value, scale);
        return this;
    }

    /// <inheritdoc />
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, long lo, long hi, byte scale)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDecimal128(name, lo, hi, scale);
        return this;
    }

    /// <inheritdoc />
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, long l0, long l1, long l2, long l3, byte scale)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDecimal256(name, l0, l1, l2, l3, scale);
        return this;
    }

    /// <summary>Appends a BINARY value (opaque bytes; same wire layout as VARCHAR but no UTF-8 contract).</summary>
    public IQwpWebSocketSender ColumnBinary(ReadOnlySpan<char> name, ReadOnlySpan<byte> value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendBinary(name, value);
        return this;
    }

    /// <summary>Appends an IPv4 address. Throws if <paramref name="addr" /> is not <see cref="System.Net.Sockets.AddressFamily.InterNetwork" />.</summary>
    public IQwpWebSocketSender ColumnIPv4(ReadOnlySpan<char> name, System.Net.IPAddress addr)
    {
        ArgumentNullException.ThrowIfNull(addr);
        if (addr.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"IPv4 column requires an InterNetwork address, got {addr.AddressFamily} (`{addr}`)");
        }

        ThrowIfTerminal();
        Span<byte> octets = stackalloc byte[4];
        if (!addr.TryWriteBytes(octets, out var written) || written != 4)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, $"failed to serialise IPv4 address `{addr}`");
        }

        var packed = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(octets);
        EnsureCurrentTable().AppendIPv4(name, packed);
        return this;
    }

    /// <inheritdoc />
    public IQwpWebSocketSender ColumnByte(ReadOnlySpan<char> name, sbyte value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendByte(name, value);
        return this;
    }

    /// <inheritdoc />
    public IQwpWebSocketSender ColumnShort(ReadOnlySpan<char> name, short value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendShort(name, value);
        return this;
    }

    /// <inheritdoc />
    public IQwpWebSocketSender ColumnFloat(ReadOnlySpan<char> name, float value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendFloat(name, value);
        return this;
    }

    /// <inheritdoc />
    public IQwpWebSocketSender ColumnDate(ReadOnlySpan<char> name, long millisSinceEpoch)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDateMillis(name, millisSinceEpoch);
        return this;
    }

    /// <inheritdoc />
    public IQwpWebSocketSender ColumnGeohash(ReadOnlySpan<char> name, ulong hash, int precisionBits)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendGeohash(name, hash, precisionBits);
        return this;
    }

    /// <inheritdoc />
    public IQwpWebSocketSender ColumnLong256(ReadOnlySpan<char> name, System.Numerics.BigInteger value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendLong256(name, value);
        return this;
    }

    private void AppendArrayDispatch<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> values, ReadOnlySpan<int> shape)
        where T : struct
    {
        ThrowIfTerminal();
        var col = EnsureCurrentTable();
        if (typeof(T) == typeof(double))
        {
            col.AppendDoubleArray(name, System.Runtime.InteropServices.MemoryMarshal.Cast<T, double>(values), shape);
        }
        else if (typeof(T) == typeof(long))
        {
            col.AppendLongArray(name, System.Runtime.InteropServices.MemoryMarshal.Cast<T, long>(values), shape);
        }
        else
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"array element type {typeof(T)} not supported; only double and long");
        }
    }

    /// <inheritdoc />
    public ValueTask AtAsync(DateTime value, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        var t = EnsureCurrentTable();
        GuardRowSize(t);
        t.At(DateTimeToMicros(value));
        OnRowCommitted(t);
        return FlushIfNecessaryAsyncCore(ct);
    }

    /// <inheritdoc />
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default)
        => AtAsync(value.UtcDateTime, ct);

    /// <inheritdoc />
    public ValueTask AtAsync(long value, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        var t = EnsureCurrentTable();
        GuardRowSize(t);
        t.At(value);
        OnRowCommitted(t);
        return FlushIfNecessaryAsyncCore(ct);
    }

    /// <inheritdoc />
    public ValueTask AtNowAsync(CancellationToken ct = default)
        => AtAsync(DateTime.UtcNow, ct);

    /// <inheritdoc />
    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        var t = EnsureCurrentTable();
        GuardRowSize(t);
        t.AtNanos(timestampNanos);
        OnRowCommitted(t);
        return FlushIfNecessaryAsyncCore(ct);
    }

    /// <inheritdoc />
    public void At(DateTime value, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        var t = EnsureCurrentTable();
        GuardRowSize(t);
        t.At(DateTimeToMicros(value));
        OnRowCommitted(t);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void At(DateTimeOffset value, CancellationToken ct = default)
    {
        At(value.UtcDateTime, ct);
    }

    /// <inheritdoc />
    public void At(long value, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        var t = EnsureCurrentTable();
        GuardRowSize(t);
        t.At(value);
        OnRowCommitted(t);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void AtNow(CancellationToken ct = default)
    {
        At(DateTime.UtcNow, ct);
    }

    /// <inheritdoc />
    public void AtNanos(long timestampNanos, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        var t = EnsureCurrentTable();
        GuardRowSize(t);
        t.AtNanos(timestampNanos);
        OnRowCommitted(t);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public Task SendAsync(CancellationToken ct = default)
    {
        // Standalone (SendAwaitsAck): Send drains (flush + await ACK) so "Send() before Dispose" delivers
        // even though Dispose does not drain. Pooled: fast flush-to-ring; the pool ships async and
        // delivery is confirmed via IQuestDBClient.Flush(). close_flush_timeout_millis=0 is the documented
        // "fast, don't wait" opt-out — a 0ms drain can never observe an ACK, so treat it as flush-to-ring
        // rather than a spurious timeout.
        if (Options.SendAwaitsAck && Options.close_flush_timeout_millis > TimeSpan.Zero)
        {
            return SendDrainAsync(ct);
        }

        ThrowIfTerminal();
        EnsureNoRowInProgress();
        return FlushAsyncCore(deferCommit: false, ct).AsTask();
    }

    private async Task SendDrainAsync(CancellationToken ct)
    {
        if (!await FlushAsync(Options.close_flush_timeout_millis, ct).ConfigureAwait(false))
        {
            ThrowSendDrainTimeout();
        }
    }

    /// <inheritdoc />
    public void Send(CancellationToken ct = default)
    {
        if (Options.SendAwaitsAck && Options.close_flush_timeout_millis > TimeSpan.Zero)
        {
            if (!Flush(Options.close_flush_timeout_millis, ct))
            {
                ThrowSendDrainTimeout();
            }

            return;
        }

        ThrowIfTerminal();
        EnsureNoRowInProgress();
        FlushSync(deferCommit: false, ct);
    }

    private void ThrowSendDrainTimeout() =>
        throw new IngressError(ErrorCode.ServerFlushError,
            $"Send timed out after {Options.close_flush_timeout_millis.TotalMilliseconds:F0}ms waiting for the " +
            "server to acknowledge; data may not have been delivered");

    /// <inheritdoc />
    public bool Flush(TimeSpan timeout, CancellationToken ct = default)
        => FlushCore(timeout, ct).GetAwaiter().GetResult();

    /// <inheritdoc />
    public Task<bool> FlushAsync(TimeSpan timeout, CancellationToken ct = default)
        => FlushCore(timeout, ct);

    // Drain = flush buffered rows into the ring (publishing them) then wait for the ACK watermark to reach
    // that published frame. Reuses FlushAndGetSequenceAsync so the deferred-commit / commit-frame handling
    // stays in one place.
    private async Task<bool> FlushCore(TimeSpan timeout, CancellationToken ct)
    {
        ThrowIfTerminal();
        EnsureNoRowInProgress();
        var publishedFsn = await FlushAndGetSequenceAsync(ct).ConfigureAwait(false);
        return await AwaitAckedFsnAsync(publishedFsn, timeout, ct).ConfigureAwait(false);
    }

    private void EnsureNoRowInProgress()
    {
        foreach (var t in _tables.Values)
        {
            if (t.HasPendingRow)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    $"row in progress on table `{t.TableName}` — call At()/AtNow() to commit or CancelRow() to abandon before flushing");
            }
        }
    }

    private int EncodeBatch(bool deferCommit)
    {
        _flushBatch.Clear();
        foreach (var t in _tables.Values)
        {
            if (t.RowCount > 0)
            {
                _flushBatch.Add(t);
            }
        }

        if (_flushBatch.Count == 0)
        {
            return 0;
        }

        return QwpEncoder.EncodeInto(
            _encoderBuffer, _flushBatch, _symbolDictionary,
            selfSufficient: true,
            symbolDeltaCount: _currentBatchMaxSymbolId + 1,
            deferCommit: deferCommit);
    }

    // An empty (zero-table) non-deferred frame that triggers the server-side commit of rows shipped by
    // earlier deferred auto-flushes when an explicit commit has no residual rows to send. Mirrors the
    // Java client's sendCommitMessage (beginMessage with tableCount=0).
    private int EncodeCommitFrame()
    {
        return QwpEncoder.EncodeInto(
            _encoderBuffer, Array.Empty<QwpTableBuffer>(), _symbolDictionary,
            selfSufficient: true,
            symbolDeltaCount: _currentBatchMaxSymbolId + 1,
            deferCommit: false);
    }

    private void FlushSync(bool deferCommit, CancellationToken ct)
    {
        var len = EncodeBatch(deferCommit);
        if (len > 0)
        {
            GuardBatchSize(len);
            _engine.AppendBlocking(_encoderBuffer.AsSpan(0, len), ct);
            OnFlushSucceeded();
            _hasDeferredMessages = deferCommit;
            return;
        }

        // No buffered rows. If this is a commit and earlier auto-flushes deferred, send the commit frame.
        if (!deferCommit && _hasDeferredMessages)
        {
            var commitLen = EncodeCommitFrame();
            _engine.AppendBlocking(_encoderBuffer.AsSpan(0, commitLen), ct);
            OnFlushSucceeded();
            _hasDeferredMessages = false;
        }
    }

    private async ValueTask FlushAsyncCore(bool deferCommit, CancellationToken ct)
    {
        var len = EncodeBatch(deferCommit);
        if (len > 0)
        {
            GuardBatchSize(len);
            await _engine.AppendAsync(_encoderBuffer.WrittenMemory, ct).ConfigureAwait(false);
            OnFlushSucceeded();
            _hasDeferredMessages = deferCommit;
            return;
        }

        if (!deferCommit && _hasDeferredMessages)
        {
            EncodeCommitFrame();
            await _engine.AppendAsync(_encoderBuffer.WrittenMemory, ct).ConfigureAwait(false);
            OnFlushSucceeded();
            _hasDeferredMessages = false;
        }
    }

    /// <inheritdoc />
    public long AckedFsn => _engine.AckedFsn - 1;

    /// <inheritdoc />
    public async Task<long> FlushAndGetSequenceAsync(CancellationToken ct = default)
    {
        ThrowIfTerminal();
        // An explicit, sequence-returning flush is a commit point (non-deferred).
        var len = EncodeBatch(deferCommit: false);
        if (len == 0)
        {
            if (!_hasDeferredMessages) return _engine.NextFsn - 1;
            EncodeCommitFrame();
            await _engine.AppendAsync(_encoderBuffer.WrittenMemory, ct).ConfigureAwait(false);
            var committedFsn = _engine.NextFsn - 1;
            OnFlushSucceeded();
            _hasDeferredMessages = false;
            return committedFsn;
        }
        GuardBatchSize(len);
        await _engine.AppendAsync(_encoderBuffer.WrittenMemory, ct).ConfigureAwait(false);
        var publishedFsn = _engine.NextFsn - 1;
        OnFlushSucceeded();
        _hasDeferredMessages = false;
        return publishedFsn;
    }

    /// <inheritdoc />
    public Task<bool> AwaitAckedFsnAsync(long targetFsn, TimeSpan timeout, CancellationToken ct = default)
    {
        if (targetFsn < 0) return Task.FromResult(true);
        ThrowIfTerminal();
        return _engine.AwaitAckedFsnAsync(targetFsn + 1, timeout, ct);
    }

    private void OnFlushSucceeded()
    {
        ResetPendingState();
        LastFlush = DateTime.UtcNow;
        _lastFlushTickCount = Environment.TickCount64;
    }

    private void ResetPendingState()
    {
        // Symbol ids must stay stable for the connection's lifetime; resetting the
        // dictionary per flush makes the server serve stale symbol-cache hits.
        _currentBatchMaxSymbolId = -1;
        foreach (var t in _flushBatch)
        {
            t.Clear();
        }

        _flushBatch.Clear();
        _runningRowCount = 0;
        _pendingBytes = 0;
        _currentTableSnapshotBytes = 0;
        _currentTable = null;
    }

    /// <inheritdoc />
    public void Truncate()
    {
        ThrowIfTerminal();
        foreach (var t in _tables.Values)
        {
            t.TrimToCurrent();
        }
    }

    /// <inheritdoc />
    public void CancelRow()
    {
        ThrowIfTerminal();
        _currentTable?.CancelCurrentRow();
    }

    /// <inheritdoc />
    public void Clear()
    {
        ThrowIfTerminal();
        foreach (var t in _tables.Values)
        {
            t.Clear();
        }

        _currentBatchMaxSymbolId = -1;
        _currentTable = null;
        _runningRowCount = 0;
        _pendingBytes = 0;
        _currentTableSnapshotBytes = 0;
    }

    /// <inheritdoc />
    public long GetHighestAckedSeqTxn(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ThrowIfTerminal();
        lock (_seqTxnLock)
        {
            return _committedSeqTxn.TryGetValue(tableName, out var v) ? v : -1L;
        }
    }

    /// <inheritdoc />
    public long GetHighestDurableSeqTxn(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ThrowIfTerminal();
        lock (_seqTxnLock)
        {
            return _durableSeqTxn.TryGetValue(tableName, out var v) ? v : -1L;
        }
    }

    /// <inheritdoc />
    public long DroppedErrorNotifications => _errorDispatcher?.DroppedNotifications ?? 0L;

    /// <inheritdoc />
    public long DroppedConnectionNotifications => _connectionEventDispatcher?.DroppedNotifications ?? 0L;

    /// <inheritdoc />
    public long TotalErrorNotificationsDelivered => _errorDispatcher?.TotalDelivered ?? 0L;

    /// <inheritdoc />
    public long TotalFramesSent => _engine.TotalFramesSent;

    /// <inheritdoc />
    public long TotalAcks => _engine.TotalAcks;

    /// <inheritdoc />
    public long TotalServerErrors => _engine.TotalServerErrors;

    /// <inheritdoc />
    public long TotalReconnectAttempts => _engine.TotalReconnectAttempts;

    /// <inheritdoc />
    public long TotalReconnectsSucceeded => _engine.TotalReconnectsSucceeded;

    private void UpdateSeqTxnFromAck(QwpTableEntry entry, bool isDurable)
    {
        lock (_seqTxnLock)
        {
            if (isDurable)
            {
                if (!_durableSeqTxn.TryGetValue(entry.TableName, out var prev) || entry.SeqTxn > prev)
                {
                    _durableSeqTxn[entry.TableName] = entry.SeqTxn;
                }
            }
            else
            {
                if (!_committedSeqTxn.TryGetValue(entry.TableName, out var prev) || entry.SeqTxn > prev)
                {
                    _committedSeqTxn[entry.TableName] = entry.SeqTxn;
                }
            }
        }
    }

    /// <inheritdoc />
    public void Ping(CancellationToken ct = default)
        => PingAsyncCore(ct).GetAwaiter().GetResult();

    /// <inheritdoc />
    public ValueTask PingAsync(CancellationToken ct = default)
        => PingAsyncCore(ct);

    private async ValueTask PingAsyncCore(CancellationToken ct)
    {
        ThrowIfTerminal();
        await _engine.FlushAsync(Options.ping_timeout, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        DisposeStackSync();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        await DisposeStackAsync().ConfigureAwait(false);
    }

    // Dispose is pure resource release: no flush, no ACK wait, no throw. Buffered-but-unsent rows are
    // discarded. Callers wanting delivery must Send() (hand rows to the ring) and, for confirmation,
    // Flush(timeout) (await ACK) BEFORE disposing. Already-sent frames are unaffected: in SF mode they
    // persist in the mmap ring and replay on the next run; in RAM mode they ship best-effort until
    // teardown. Sync and async teardown are identical — the cursor engine's own Dispose is synchronous
    // (it defers a wedged pump-join past its budget), so there is nothing to await.
    private void DisposeStackSync()
    {
        SfCleanup.Dispose(_drainerPool);
        SfCleanup.Dispose(_engine);
        SfCleanup.Dispose(_errorDispatcher);
        SfCleanup.Dispose(_connectionEventDispatcher);
        SfCleanup.Dispose(_certValidator);
    }

    private ValueTask DisposeStackAsync()
    {
        DisposeStackSync();
        return default;
    }

    private QwpTableBuffer EnsureCurrentTable()
    {
        if (_currentTable is null)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table(...) must be called before adding columns or symbols");
        }

        return _currentTable;
    }

    private void ThrowIfTerminal()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(QwpWebSocketSender));
        }

        if (_engine.IsTerminallyFailed)
        {
            var inner = _engine.TerminalError;
            var code = (inner as IngressError)?.code ?? ErrorCode.ServerFlushError;
            var msg = inner?.Message ?? "QWP cursor engine failed terminally";
            throw inner is null
                ? new IngressError(code, msg)
                : new IngressError(code, msg, inner);
        }
    }

    private void GuardLastFlushNotSet()
    {
        if (LastFlush == DateTime.MinValue)
        {
            LastFlush = DateTime.UtcNow;
            _lastFlushTickCount = Environment.TickCount64;
        }
    }

    private void FlushIfNecessary(CancellationToken ct)
    {
        if (!ShouldAutoFlush()) return;
        // In transactional mode auto-flush stages rows with FLAG_DEFER_COMMIT; the explicit
        // Send()/Commit() commits. Outside transactional mode every frame commits immediately.
        FlushSync(deferCommit: _transactional, ct);
    }

    private ValueTask FlushIfNecessaryAsyncCore(CancellationToken ct)
    {
        if (!ShouldAutoFlush()) return ValueTask.CompletedTask;
        return FlushAsyncCore(deferCommit: _transactional, ct);
    }

    private bool ShouldAutoFlush()
    {
        if (Options.auto_flush != AutoFlushType.on) return false;

        var effectiveBytes = EffectiveAutoFlushBytes();
        var rowsTrigger = Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows;
        var bytesTrigger = effectiveBytes > 0 && _pendingBytes >= effectiveBytes;
        var timeTrigger = Options.auto_flush_interval > TimeSpan.Zero
                          && Environment.TickCount64 - _lastFlushTickCount >= (long)Options.auto_flush_interval.TotalMilliseconds;
        return rowsTrigger || bytesTrigger || timeTrigger;
    }

    // The size ceiling to clamp against: the server-advertised cap once known, otherwise the
    // protocol hard limit. Until the first successful connect the server cap is 0 (unknown) —
    // notably in initial_connect_mode=async, where rows are appended before connecting — so without
    // this fallback the row/batch guards would be bypassed entirely in that window.
    private int EffectiveMaxBatchCap()
    {
        var cap = _engine.NegotiatedMaxBatchSize;
        return cap > 0 ? cap : QwpConstants.MaxBatchBytes;
    }

    // Clamps the configured byte budget to fit under the batch cap minus a 10% margin for schema /
    // dict-delta / framing overhead. auto_flush_bytes=off stays off even when a cap is advertised.
    // Before the first connect the cap falls back to the protocol limit so auto-flush can't
    // accumulate a batch the encoder will later reject (and leave stuck in the buffer).
    private int EffectiveAutoFlushBytes()
    {
        var configured = Options.auto_flush_bytes;
        if (configured <= 0) return 0;
        var safeBudget = (long)EffectiveMaxBatchCap() * 9 / 10;
        return configured < safeBudget ? configured : (int)safeBudget;
    }

    private void GuardRowSize(QwpTableBuffer t)
    {
        var cap = _engine.NegotiatedMaxBatchSize;
        var effectiveCap = cap > 0 ? cap : QwpConstants.MaxBatchBytes;
        var rowBytes = t.GetBufferedBytes() - _currentTableSnapshotBytes;
        // 10% margin matches EffectiveAutoFlushBytes: framing overhead pushes a raw-cap-sized row over.
        var safeBudget = (long)effectiveCap * 9 / 10;
        if (rowBytes <= safeBudget) return;
        t.CancelCurrentRow();
        throw new IngressError(ErrorCode.InvalidApiCall, cap > 0
            ? $"row too large for server batch cap [rowBytes={rowBytes}, serverMaxBatchSize={cap}]"
            : $"row too large for protocol batch limit [rowBytes={rowBytes}, "
              + $"protocolMaxBatchSize={QwpConstants.MaxBatchBytes}, server cap not yet negotiated]");
    }

    private void OnRowCommitted(QwpTableBuffer t)
    {
        _runningRowCount++;
        var bufferedNow = t.GetBufferedBytes();
        _pendingBytes += bufferedNow - _currentTableSnapshotBytes;
        _currentTableSnapshotBytes = bufferedNow;
    }

    private void GuardBatchSize(int messageSize)
    {
        var cap = _engine.NegotiatedMaxBatchSize;
        if (cap <= 0 || messageSize <= cap) return;
        var droppedRows = _runningRowCount;
        ResetPendingState();
        throw new IngressError(ErrorCode.InvalidApiCall,
            $"batch too large for server batch cap [messageSize={messageSize}, serverMaxBatchSize={cap}, droppedRows={droppedRows}]");
    }

    private long DateTimeToMicros(DateTime value)
    {
        var utc = _convertLocalToUtc && value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;
        return (utc - DateTime.UnixEpoch).Ticks / TicksPerMicrosecond;
    }

    // Same authHeader is forwarded to every failover host. Safe because ErrorCode.AuthError is
    // terminal in QwpCursorSendEngine; auth rejection never reaches the host-rotation path.
    private static Func<IQwpCursorTransport> BuildHostRotatingFactory(
        SenderOptions options,
        QwpHostHealthTracker tracker,
        string? authHeader,
        System.Net.Security.RemoteCertificateValidationCallback? certValidator)
    {
        var proxy = ResolveProxy(options.proxy);
        return () =>
        {
            var idx = tracker.PickNext();
            if (idx < 0)
            {
                tracker.BeginRound(forgetClassifications: true);
                idx = tracker.PickNext();
            }

            var keepalive = options.request_durable_ack
                && options.durable_ack_keepalive_interval_millis > TimeSpan.Zero
                ? options.durable_ack_keepalive_interval_millis
                : TimeSpan.Zero;

            var transportOpts = new QwpWebSocketTransportOptions
            {
                Uri = options.BuildUri(idx, QwpConstants.WritePath),
                AuthorizationHeader = authHeader,
                RequestDurableAck = options.request_durable_ack,
                RemoteCertificateValidationCallback = certValidator,
                Proxy = proxy,
                KeepAliveInterval = keepalive,
            };

            return new QwpTrackedCursorTransport(new QwpWebSocketTransport(transportOpts), tracker, idx,
                options.EffectiveConnectTimeout);
        };
    }

    private static System.Net.IWebProxy? ResolveProxy(string? proxy)
    {
        if (string.IsNullOrEmpty(proxy) || string.Equals(proxy, "disable", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }
        if (string.Equals(proxy, "system", StringComparison.OrdinalIgnoreCase))
        {
            return System.Net.WebRequest.DefaultWebProxy;
        }
        if (!Uri.TryCreate(proxy, UriKind.Absolute, out var uri))
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"`proxy` must be `disable`, `system`, or an absolute URI; got `{proxy}`");
        }
        return new System.Net.WebProxy(uri);
    }

    private static string? BuildAuthHeader(SenderOptions options) =>
        QwpTlsAuth.BuildAuthHeader(options.username, options.password, options.token);

    private static QwpTlsAuth.CertificateValidator? BuildCertificateValidator(SenderOptions options) =>
        QwpTlsAuth.BuildCertificateValidator(options.tls_verify, options.tls_roots, options.tls_roots_password);
}

#endif
