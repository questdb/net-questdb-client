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

using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace QuestDB.Senders;

/// <summary>
///     ISender implementation backed by the WebSocket transport and QWP v1 columnar binary protocol.
/// </summary>
/// <remarks>
///     Pipelined async I/O: producer encodes into a double buffer, a send-loop drains the channel,
///     and a receive-loop matches ACKs against an in-flight window of size <c>in_flight_window</c>.
///     <para />
///     <b>Terminal-failure model:</b> any wire error, server error frame, or ACK timeout sets a
///     sticky <c>_terminalError</c> that subsequent calls re-throw. Recovery is to dispose the
///     sender and create a new one — there is no automatic reconnect.
/// </remarks>
internal sealed class QwpWebSocketSender : IQwpWebSocketSender
{
    private const long TicksPerMicrosecond = 10L;

    private readonly Dictionary<string, QwpTableBuffer> _tables = new(StringComparer.Ordinal);
#if NET9_0_OR_GREATER
    private readonly Dictionary<string, QwpTableBuffer>.AlternateLookup<ReadOnlySpan<char>> _tablesLookup;
#endif
    private readonly QwpSchemaCache _schemaCache;
    private readonly QwpSymbolDictionary _symbolDictionary;
    private readonly QwpInFlightWindow _inFlightWindow = new();
    private readonly QwpWebSocketTransport? _transport;
    private byte[] _receiveBuffer;
    private const int MaxReceiveBufferBytes = 16 * 1024 * 1024;
    private readonly List<QwpTableBuffer> _flushBatch = new();

    private readonly SemaphoreSlim? _slot;
    private readonly Channel<AsyncBatch>? _sendChannel;
    private readonly Task? _sendLoopTask;
    private readonly Task? _receiveLoopTask;
    private readonly CancellationTokenSource? _ioCts;

    // Double-buffered encoder: producer encodes into one buffer while SendLoop is sending the
    // other; ready signals gate buffer reuse. Sync/SF take the index-0 fast path.
    private readonly QwpEncoder.FrameBuilder[] _encoderBuffers;
    private readonly SemaphoreSlim[] _encoderReady;
    private int _encoderIndex;
    private const int EncoderInitialCapacity = 1 << 16;

    // Store-and-forward — populated only when sf_dir is set on Options.
    private readonly bool _sfMode;
    private readonly QwpCursorSendEngine? _sfEngine;
    private readonly QwpBackgroundDrainerPool? _sfDrainerPool;

    // Per-table seqTxn watermarks. Accessed by both the producer thread (read via Get*) and the
    // receive loop (write on ACK frames); guarded by _seqTxnLock.
    private readonly Dictionary<string, long> _committedSeqTxn = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _durableSeqTxn = new(StringComparer.Ordinal);
    private readonly object _seqTxnLock = new();

    private QwpTableBuffer? _currentTable;
    private long _nextSequence;
    private IngressError? _terminalError;
    private int _disposed;
    private int _runningRowCount;

    private readonly record struct AsyncBatch(int BufferIndex, ReadOnlyMemory<byte> Frame);

    public QwpWebSocketSender(SenderOptions options)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        if (!options.IsWebSocket())
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"protocol must be ws or wss for {nameof(QwpWebSocketSender)}, got {options.protocol}");
        }

        if (options.in_flight_window < 2)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"WebSocket transport requires in_flight_window > 1, got {options.in_flight_window}");
        }

        _schemaCache = new QwpSchemaCache(options.max_schemas_per_connection);
        _symbolDictionary = new QwpSymbolDictionary();
        _receiveBuffer = new byte[QwpConstants.ErrorAckHeaderSize + QwpConstants.MaxErrorMessageBytes];
        _sfMode = !string.IsNullOrEmpty(options.sf_dir);
#if NET9_0_OR_GREATER
        _tablesLookup = _tables.GetAlternateLookup<ReadOnlySpan<char>>();
#endif

        // Two encoder buffers + two ready signals (one per buffer). Async mode toggles between 0/1
        // while pipelined batches are in flight; sync and SF only use index 0.
        _encoderBuffers = new[]
        {
            new QwpEncoder.FrameBuilder(EncoderInitialCapacity),
            new QwpEncoder.FrameBuilder(EncoderInitialCapacity),
        };
        _encoderReady = new[]
        {
            new SemaphoreSlim(1, 1),
            new SemaphoreSlim(1, 1),
        };

        if (_sfMode)
        {
            (_sfEngine, _sfDrainerPool) = BuildSfStack(options);
            _sfEngine.SetTableEntryHandler(UpdateSeqTxnFromAck);
            return;
        }

        var authHeader = BuildAuthHeader(options);
        var certValidator = BuildCertificateValidator(options);
        var tracker = new QwpHostHealthTracker(options.addresses);

        QwpWebSocketTransport? transport = null;
        SemaphoreSlim? slot = null;
        Channel<AsyncBatch>? sendChannel;
        CancellationTokenSource? ioCts = null;
        try
        {
            transport = ConnectInitialTransport(options, tracker, authHeader, certValidator);

            slot = new SemaphoreSlim(options.in_flight_window, options.in_flight_window);
            sendChannel = Channel.CreateBounded<AsyncBatch>(new BoundedChannelOptions(options.in_flight_window)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });
            ioCts = new CancellationTokenSource();
        }
        catch
        {
            ioCts?.Dispose();
            slot?.Dispose();
            transport?.Dispose();
            throw;
        }

        _transport = transport;
        _slot = slot;
        _sendChannel = sendChannel;
        _ioCts = ioCts;
        _sendLoopTask = Task.Run(() => SendLoop(_ioCts.Token));
        _receiveLoopTask = Task.Run(() => ReceiveLoop(_ioCts.Token));
    }

    private static (QwpCursorSendEngine engine, QwpBackgroundDrainerPool? pool) BuildSfStack(SenderOptions options)
    {
        var sfRoot = options.sf_dir!;
        var slotDir = Path.Combine(sfRoot, options.sender_id);
        var slotLock = QwpSlotLock.Acquire(slotDir);
        QwpSegmentRing? ring = null;
        QwpCursorSendEngine? engine = null;
        QwpBackgroundDrainerPool? pool = null;

        try
        {
            ring = QwpSegmentRing.Open(
                slotDir,
                segmentCapacity: options.sf_max_bytes);

            var authHeader = BuildAuthHeader(options);
            var certValidator = BuildCertificateValidator(options);
            var tracker = new QwpHostHealthTracker(options.addresses);
            var transportFactory = BuildHostRotatingFactory(options, tracker, authHeader, certValidator);

            var policy = new QwpReconnectPolicy(
                options.reconnect_initial_backoff_millis,
                options.reconnect_max_backoff_millis,
                options.reconnect_max_duration_millis,
                jitter: QwpReconnectPolicy.UniformDoubleJitter);

            engine = new QwpCursorSendEngine(
                slotLock,
                ring,
                transportFactory,
                policy,
                options.sf_append_deadline_millis,
                options.initial_connect_retry,
                maxTotalBytes: options.sf_max_total_bytes,
                skipBackoffPredicate: () => !tracker.IsRoundExhausted);

            engine.Start();

            if (options.drain_orphans)
            {
                var drainer = new QwpBackgroundDrainer(
                    transportFactory,
                    policy,
                    segmentCapacity: options.sf_max_bytes,
                    drainTimeout: options.reconnect_max_duration_millis,
                    skipBackoffPredicate: () => !tracker.IsRoundExhausted);
                pool = new QwpBackgroundDrainerPool(
                    options.max_background_drainers,
                    drainer,
                    shutdownWait: options.close_flush_timeout_millis);
                var orphans = QwpOrphanScanner.ClaimOrphans(sfRoot, options.sender_id);
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

            return (engine, pool);
        }
        catch (Exception)
        {
            SfCleanup.Dispose(pool);
            SfCleanup.Dispose(engine);
            SfCleanup.Dispose(ring);
            SfCleanup.Dispose(slotLock);
            throw;
        }
    }

    /// <inheritdoc />
    public SenderOptions Options { get; }

    /// <inheritdoc />
    public int Length
    {
        get
        {
            var total = 0;
            foreach (var t in _tables.Values)
            {
                total += EstimateTableSize(t);
            }

            return total;
        }
    }

    /// <inheritdoc />
    public int RowCount => _runningRowCount;

    /// <inheritdoc />
    public bool WithinTransaction => false;

    /// <inheritdoc />
    public DateTime LastFlush { get; private set; } = DateTime.MinValue;
    private long _lastFlushTickCount;

    /// <inheritdoc />
    public ISender Transaction(ReadOnlySpan<char> tableName)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, "transactions are not supported on the WebSocket transport");
    }

    /// <inheritdoc />
    public void Rollback()
    {
        throw new IngressError(ErrorCode.InvalidApiCall, "transactions are not supported on the WebSocket transport");
    }

    /// <inheritdoc />
    public ValueTask CommitAsync(CancellationToken ct = default)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, "transactions are not supported on the WebSocket transport");
    }

    /// <inheritdoc />
    public void Commit(CancellationToken ct = default)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, "transactions are not supported on the WebSocket transport");
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
        return this;
    }

    /// <inheritdoc />
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        ThrowIfTerminal();
        var preCount = _symbolDictionary.Count;
        var globalId = _symbolDictionary.Add(value);
        try
        {
            EnsureCurrentTable().AppendSymbol(name, globalId);
        }
        catch
        {
            // CancelCurrentRow rolls back column savepoints but not the dict.
            if (_symbolDictionary.Count > preCount)
            {
                _symbolDictionary.RollbackTo(preCount);
            }
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
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDecimal128(name, value);
        return this;
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
        AppendArrayDispatch(name, arr.AsSpan(), shapeArr.AsSpan());
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
        EnsureCurrentTable().At(DateTimeToMicros(value));
        _runningRowCount++;
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
        EnsureCurrentTable().At(value);
        _runningRowCount++;
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
        EnsureCurrentTable().AtNanos(timestampNanos);
        _runningRowCount++;
        return FlushIfNecessaryAsyncCore(ct);
    }

    /// <inheritdoc />
    public void At(DateTime value, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        EnsureCurrentTable().At(DateTimeToMicros(value));
        _runningRowCount++;
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
        EnsureCurrentTable().At(value);
        _runningRowCount++;
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
        EnsureCurrentTable().AtNanos(timestampNanos);
        _runningRowCount++;
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public ValueTask SendAsync(CancellationToken ct = default)
    {
        ThrowIfTerminal();
        EnsureNoRowInProgress();
        if (_sfMode)
        {
            return FlushToSfEngineAsyncCore(ct);
        }

        return EnqueueAsyncCore(ct, awaitDrain: true);
    }

    /// <inheritdoc />
    public void Send(CancellationToken ct = default)
    {
        ThrowIfTerminal();
        EnsureNoRowInProgress();
        if (_sfMode)
        {
            FlushToSfEngineSync(ct);
            return;
        }

        EnqueueSync(ct, awaitDrain: true);
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

    private int EncodeSfBatch()
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

        try
        {
            return QwpEncoder.EncodeInto(
                _encoderBuffers[0], _flushBatch, _schemaCache, _symbolDictionary,
                selfSufficient: true,
                gorillaEnabled: Options.gorilla);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailTerminal(ex);
            throw LoadTerminal()!;
        }
    }

    private void FlushToSfEngineSync(CancellationToken ct)
    {
        var len = EncodeSfBatch();
        if (len == 0) return;

        try
        {
            _sfEngine!.AppendBlocking(_encoderBuffers[0].AsSpan(0, len), ct);
        }
        catch (IngressError)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailTerminal(ex);
            throw LoadTerminal()!;
        }

        OnFlushSucceeded();
    }

    private async ValueTask FlushToSfEngineAsyncCore(CancellationToken ct)
    {
        var len = EncodeSfBatch();
        if (len == 0) return;

        try
        {
            await _sfEngine!.AppendAsync(_encoderBuffers[0].WrittenMemory, ct).ConfigureAwait(false);
        }
        catch (IngressError)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailTerminal(ex);
            throw LoadTerminal()!;
        }

        OnFlushSucceeded();
    }

    /// <summary>
    ///     Encodes the current pending tables into <paramref name="bufferIndex" />'s shared
    ///     encoder buffer. Returns the encoded length, or 0 if there were no pending rows.
    /// </summary>
    private int EncodeFrameInto(int bufferIndex)
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

        try
        {
            return QwpEncoder.EncodeInto(
                _encoderBuffers[bufferIndex], _flushBatch, _schemaCache, _symbolDictionary,
                gorillaEnabled: Options.gorilla);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailTerminal(ex);
            throw LoadTerminal()!;
        }
    }

    private void ProcessTableEntries(IReadOnlyList<QwpTableEntry> entries, bool isDurable)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var target = isDurable ? _durableSeqTxn : _committedSeqTxn;
        lock (_seqTxnLock)
        {
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                if (target.TryGetValue(entry.TableName, out var existing))
                {
                    if (entry.SeqTxn > existing)
                    {
                        target[entry.TableName] = entry.SeqTxn;
                    }
                }
                else
                {
                    target[entry.TableName] = entry.SeqTxn;
                }
            }
        }
    }

    /// <summary>
    ///     Encodes the pending tables, hands the resulting frame to the send loop, and (if requested)
    ///     waits for the in-flight window to drain. Truly async: every wait uses <c>WaitAsync</c>.
    /// </summary>
    private ValueTask EnqueueAsyncCore(CancellationToken ct, bool awaitDrain)
        => EnqueueAsyncCore(ct, awaitDrain, drainTimeout: Timeout.InfiniteTimeSpan);

    private async ValueTask EnqueueAsyncCore(CancellationToken ct, bool awaitDrain, TimeSpan drainTimeout)
    {
        var linked = ct.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(_ioCts!.Token, ct)
            : null;
        try
        {
            await EnqueueAsyncBody(linked?.Token ?? _ioCts!.Token, awaitDrain, drainTimeout).ConfigureAwait(false);
        }
        finally
        {
            linked?.Dispose();
        }
    }

    private async ValueTask EnqueueAsyncBody(CancellationToken linkedCt, bool awaitDrain, TimeSpan drainTimeout)
    {
        var idx = _encoderIndex;
        _encoderIndex = (idx + 1) & 1;
        var ownsReady = false;
        var ownsSlot = false;
        Exception? wrapAsTerminal = null;
        var drainedSuccessfully = false;

        try
        {
            try
            {
                await _encoderReady[idx].WaitAsync(linkedCt).ConfigureAwait(false);
                ownsReady = true;

                var len = EncodeFrameInto(idx);
                if (len > 0)
                {
                    await _slot!.WaitAsync(linkedCt).ConfigureAwait(false);
                    ownsSlot = true;

                    var seq = _nextSequence++;
                    var frame = _encoderBuffers[idx].WrittenMemory;
                    _inFlightWindow.Add(seq);
                    if (!_sendChannel!.Writer.TryWrite(new AsyncBatch(idx, frame)))
                    {
                        wrapAsTerminal = new IngressError(
                            ErrorCode.ServerFlushError,
                            "internal: in-flight channel was full after reserving a slot");
                    }
                    else
                    {
                        ownsSlot = false;
                        ownsReady = false;
                        OnFlushSucceeded();
                    }
                }
                else
                {
                    _encoderReady[idx].Release();
                    ownsReady = false;
                }

                if (wrapAsTerminal is null && awaitDrain)
                {
                    try
                    {
                        await _inFlightWindow.AwaitEmptyAsync(drainTimeout, linkedCt).ConfigureAwait(false);
                        drainedSuccessfully = true;
                    }
                    catch (OperationCanceledException) when (LoadTerminal() is not null)
                    {
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException && ex is not IngressError)
                    {
                        wrapAsTerminal = ex;
                    }
                }
            }
            catch (OperationCanceledException) when (LoadTerminal() is not null)
            {
            }
        }
        finally
        {
            if (ownsSlot) SfCleanup.Run(() => _slot!.Release());
            if (ownsReady) SfCleanup.Run(() => _encoderReady[idx].Release());
        }

        if (wrapAsTerminal is not null)
        {
            FailTerminal(wrapAsTerminal);
        }

        if (!drainedSuccessfully)
        {
            ThrowIfTerminal();
        }
    }

    private void EnqueueSync(CancellationToken ct, bool awaitDrain)
    {
        EnqueueAsyncCore(ct, awaitDrain, drainTimeout: Timeout.InfiniteTimeSpan).GetAwaiter().GetResult();
    }

    private void EnqueueSync(CancellationToken ct, bool awaitDrain, TimeSpan drainTimeout)
    {
        EnqueueAsyncCore(ct, awaitDrain, drainTimeout).GetAwaiter().GetResult();
    }

    private void OnFlushSucceeded()
    {
        if (_sfMode)
        {
            // SF frames are self-sufficient — Reset, not Commit, so the dict can't grow unbounded.
            _symbolDictionary.Reset();
            foreach (var t in _flushBatch) t.SchemaId = -1;
        }
        else
        {
            _symbolDictionary.Commit();
        }

        foreach (var t in _flushBatch)
        {
            t.Clear();
        }

        _flushBatch.Clear();
        _runningRowCount = 0;
        LastFlush = DateTime.UtcNow;
        _lastFlushTickCount = Environment.TickCount64;
    }

    private async Task SendLoop(CancellationToken ct)
    {
        try
        {
            await foreach (var batch in _sendChannel!.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    await _transport!.SendBinaryAsync(batch.Frame, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    FailTerminal(ex);
                    return;
                }
                finally
                {
                    // The receiver releases _slot on ACK; here we only return the encoder buffer.
                    _encoderReady[batch.BufferIndex].Release();
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ReceiveLoop(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                int read;
                try
                {
                    (read, _receiveBuffer) = await _transport!.ReceiveFrameAsync(_receiveBuffer, MaxReceiveBufferBytes, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    FailTerminal(ex);
                    return;
                }

                QwpResponse response;
                try
                {
                    response = QwpResponse.Parse(_receiveBuffer.AsSpan(0, read));
                }
                catch (Exception ex)
                {
                    FailTerminal(ex);
                    return;
                }

                try
                {
                    if (response.IsDurableAck)
                    {
                        // Informational watermark; doesn't advance the in-flight window.
                        ProcessTableEntries(response.TableEntries, isDurable: true);
                        continue;
                    }

                    if (!response.IsOk)
                    {
                        FailTerminal(response.ToException());
                        return;
                    }

                    var prevAcked = _inFlightWindow.AckedSequence;
                    _inFlightWindow.AcknowledgeUpTo(response.Sequence);
                    var newAcked = _inFlightWindow.AckedSequence;
                    var freed = (int)(newAcked - prevAcked);
                    if (freed > 0)
                    {
                        _slot!.Release(freed);
                    }

                    ProcessTableEntries(response.TableEntries, isDurable: false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Malformed ACK must terminalise; otherwise producers block until close_flush_timeout_millis.
                    FailTerminal(ex);
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
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

        _currentTable = null;
        _runningRowCount = 0;
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

        if (_sfMode)
        {
            await _sfEngine!.FlushAsync(Options.ping_timeout, ct).ConfigureAwait(false);
            return;
        }

        // Race-safe: capture _ioCts under the disposed-check window. If Dispose set _disposed = 1
        // and disposed _ioCts already, ThrowIfDisposed() above re-throws on next call; here we
        // tolerate the late-Dispose case by catching ObjectDisposedException from the linked CTS.
        CancellationTokenSource? linked = null;
        try
        {
            linked = CancellationTokenSource.CreateLinkedTokenSource(_ioCts!.Token, ct);
            await _inFlightWindow.AwaitEmptyAsync(Options.ping_timeout, linked.Token).ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            ThrowIfDisposed();
            throw;
        }
        catch (OperationCanceledException) when (LoadTerminal() is not null)
        {
            ThrowIfTerminal();
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException && ex is not IngressError)
        {
            FailTerminal(ex);
            throw LoadTerminal()!;
        }
        finally
        {
            linked?.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(QwpWebSocketSender));
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        if (_sfMode) DisposeSfStackSync();
        else DisposeWsStackSync();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        if (_sfMode) await DisposeSfStackAsync().ConfigureAwait(false);
        else await DisposeWsStackAsync().ConfigureAwait(false);
    }

    private void DisposeWsStackSync()
    {
        try
        {
            if (LoadTerminal() is null)
            {
                using var flushCts = new CancellationTokenSource(Options.close_flush_timeout_millis);
                EnqueueSync(flushCts.Token, awaitDrain: true, drainTimeout: Options.close_flush_timeout_millis);
            }
        }
        catch (Exception)
        {
        }

        var ioJoined = false;
        try
        {
            _sendChannel!.Writer.TryComplete();
            _ioCts!.Cancel();
            ioJoined = Task.WhenAll(_sendLoopTask!, _receiveLoopTask!).Wait(Options.close_flush_timeout_millis);
        }
        catch (Exception)
        {
        }

        FinalizeWsTeardown(ioJoined);

        if (ioJoined)
        {
            try
            {
                using var closeCts = new CancellationTokenSource(Options.close_flush_timeout_millis);
                _transport!.CloseAsync(WebSocketCloseStatus.NormalClosure, null, closeCts.Token).GetAwaiter().GetResult();
            }
            catch (Exception)
            {
            }
        }

        SfCleanup.Dispose(_transport);
    }

    private async ValueTask DisposeWsStackAsync()
    {
        try
        {
            if (LoadTerminal() is null)
            {
                using var flushCts = new CancellationTokenSource(Options.close_flush_timeout_millis);
                await EnqueueAsyncCore(flushCts.Token, awaitDrain: true, drainTimeout: Options.close_flush_timeout_millis).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
        }

        var ioJoined = false;
        try
        {
            _sendChannel!.Writer.TryComplete();
            _ioCts!.Cancel();
            await Task.WhenAll(_sendLoopTask!, _receiveLoopTask!)
                .WaitAsync(Options.close_flush_timeout_millis).ConfigureAwait(false);
            ioJoined = true;
        }
        catch (Exception)
        {
        }

        FinalizeWsTeardown(ioJoined);

        if (ioJoined)
        {
            try
            {
                using var closeCts = new CancellationTokenSource(Options.close_flush_timeout_millis);
                await _transport!.CloseAsync(WebSocketCloseStatus.NormalClosure, null, closeCts.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        SfCleanup.Dispose(_transport);
    }

    private void FinalizeWsTeardown(bool ioJoined)
    {
        // On wedge, leak the semaphores: SendLoop's finally still calls Release on them.
        if (!ioJoined) return;

        SfCleanup.Dispose(_ioCts);
        SfCleanup.Dispose(_slot);
        foreach (var sem in _encoderReady)
        {
            SfCleanup.Dispose(sem);
        }
    }

    private void DisposeSfStackSync()
    {
        try
        {
            if (LoadTerminal() is null && Options.close_flush_timeout_millis.TotalMilliseconds > 0)
            {
                FlushToSfEngineSync(CancellationToken.None);
                _sfEngine!.FlushAsync(Options.close_flush_timeout_millis).GetAwaiter().GetResult();
            }
        }
        catch (Exception)
        {
        }

        SfCleanup.Dispose(_sfDrainerPool);
        SfCleanup.Dispose(_sfEngine);

        foreach (var sem in _encoderReady)
        {
            SfCleanup.Dispose(sem);
        }
    }

    private async ValueTask DisposeSfStackAsync()
    {
        try
        {
            if (LoadTerminal() is null && Options.close_flush_timeout_millis.TotalMilliseconds > 0)
            {
                await FlushToSfEngineAsyncCore(CancellationToken.None).ConfigureAwait(false);
                await _sfEngine!.FlushAsync(Options.close_flush_timeout_millis).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
        }

        SfCleanup.Dispose(_sfDrainerPool);
        SfCleanup.Dispose(_sfEngine);

        foreach (var sem in _encoderReady)
        {
            SfCleanup.Dispose(sem);
        }
    }

    private QwpTableBuffer EnsureCurrentTable()
    {
        if (_currentTable is null)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "Table(...) must be called before adding columns or symbols");
        }

        return _currentTable;
    }

    private IngressError? LoadTerminal() => Volatile.Read(ref _terminalError);

    private void ThrowIfTerminal()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(QwpWebSocketSender));
        }

        var terminal = LoadTerminal();
        if (terminal is not null)
        {
            // Re-wrap so the user sees a fresh stack trace pointing to their call site, but
            // preserves the original failure as the inner exception.
            throw new IngressError(terminal.code, terminal.Message, terminal);
        }

        if (_sfMode && _sfEngine!.IsTerminallyFailed)
        {
            var inner = _sfEngine.TerminalError;
            var code = (inner as IngressError)?.code ?? ErrorCode.ServerFlushError;
            var msg = inner?.Message ?? "QWP cursor engine failed terminally";
            throw inner is null
                ? new IngressError(code, msg)
                : new IngressError(code, msg, inner);
        }
    }

    private void FailTerminal(Exception ex)
    {
        var failure = ex as IngressError ?? new IngressError(ErrorCode.SocketError, ex.Message, ex);
        // Race with concurrent producer / send / receive loops; only the first writer wins so that
        // FailAll and the I/O cancellation each fire exactly once.
        if (Interlocked.CompareExchange(ref _terminalError, failure, null) is not null)
        {
            return;
        }

        _inFlightWindow.FailAll(failure);
        _ioCts?.Cancel();
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

        if (_sfMode)
        {
            FlushToSfEngineSync(ct);
            return;
        }

        EnqueueSync(ct, awaitDrain: false);
    }

    private ValueTask FlushIfNecessaryAsyncCore(CancellationToken ct)
    {
        if (!ShouldAutoFlush()) return ValueTask.CompletedTask;

        if (_sfMode)
        {
            return FlushToSfEngineAsyncCore(ct);
        }

        return EnqueueAsyncCore(ct, awaitDrain: false);
    }

    private bool ShouldAutoFlush()
    {
        if (Options.auto_flush != AutoFlushType.on) return false;

        var rowsTrigger = Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows;
        var bytesTrigger = Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes;
        var timeTrigger = Options.auto_flush_interval > TimeSpan.Zero
                          && Environment.TickCount64 - _lastFlushTickCount >= (long)Options.auto_flush_interval.TotalMilliseconds;
        return rowsTrigger || bytesTrigger || timeTrigger;
    }

    private static int EstimateTableSize(QwpTableBuffer t)
    {
        // Rough byte budget per table for auto_flush_bytes accounting. We don't recompute the
        // exact wire size on every row — that would be O(N) per append. Sum FixedLen + StrLen
        // over all columns instead, which is a tight upper bound on the row-data portion.
        var total = 0;
        foreach (var col in t.Columns)
        {
            total += col.FixedLen + col.StrLen;
        }

        if (t.DesignatedTimestampColumn is not null)
        {
            total += t.DesignatedTimestampColumn.FixedLen + t.DesignatedTimestampColumn.StrLen;
        }

        return total;
    }

    private static long DateTimeToMicros(DateTime value)
    {
        var utc = value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;
        return (utc - DateTime.UnixEpoch).Ticks / TicksPerMicrosecond;
    }

    private static QwpWebSocketTransport ConnectInitialTransport(
        SenderOptions options,
        QwpHostHealthTracker tracker,
        string? authHeader,
        System.Net.Security.RemoteCertificateValidationCallback? certValidator)
    {
        var proxy = ResolveProxy(options.proxy);
        Exception? lastFailure = null;
        var hostCount = tracker.Count;
        for (var attempt = 0; attempt < hostCount; attempt++)
        {
            var idx = tracker.PickNext();
            if (idx < 0) break;

            var transportOpts = new QwpWebSocketTransportOptions
            {
                Uri = options.BuildUri(idx, QwpConstants.WritePath),
                AuthorizationHeader = authHeader,
                RequestDurableAck = options.request_durable_ack,
                RemoteCertificateValidationCallback = certValidator,
                Proxy = proxy,
            };

            QwpWebSocketTransport? candidate = null;
            try
            {
                candidate = new QwpWebSocketTransport(transportOpts);
                using var connectCts = new CancellationTokenSource(options.auth_timeout);
                try
                {
                    candidate.ConnectAsync(connectCts.Token).GetAwaiter().GetResult();
                }
                catch (OperationCanceledException) when (connectCts.IsCancellationRequested)
                {
                    throw new IngressError(ErrorCode.SocketError,
                        $"WebSocket upgrade to {transportOpts.Uri} exceeded auth_timeout={options.auth_timeout.TotalMilliseconds}ms");
                }

                tracker.RecordSuccess(idx);
                return candidate;
            }
            catch (IngressError ex) when (ex.code == ErrorCode.AuthError)
            {
                candidate?.Dispose();
                throw;
            }
            catch (QwpIngressRoleRejectedException ex)
            {
                candidate?.Dispose();
                tracker.RecordRoleReject(idx, ex.IsTransient);
                lastFailure = ex;
            }
            catch (Exception ex)
            {
                candidate?.Dispose();
                tracker.RecordTransportError(idx);
                lastFailure = ex;
            }
        }

        throw new IngressError(ErrorCode.SocketError,
            $"WebSocket ingress failed against all {tracker.Count} configured endpoint(s): {lastFailure?.Message}",
            lastFailure);
    }

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

            var transportOpts = new QwpWebSocketTransportOptions
            {
                Uri = options.BuildUri(idx, QwpConstants.WritePath),
                AuthorizationHeader = authHeader,
                RequestDurableAck = options.request_durable_ack,
                RemoteCertificateValidationCallback = certValidator,
                Proxy = proxy,
            };

            return new QwpTrackedCursorTransport(new QwpWebSocketTransport(transportOpts), tracker, idx);
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
        QwpTlsAuth.BuildAuthHeader(options.username, options.password, options.token, rawAuth: null);

    private static System.Net.Security.RemoteCertificateValidationCallback? BuildCertificateValidator(SenderOptions options) =>
        QwpTlsAuth.BuildCertificateValidator(options.tls_verify, options.tls_roots, options.tls_roots_password);
}

#endif
