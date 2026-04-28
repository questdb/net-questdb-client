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
///     Synchronous mode only at present (<c>in_flight_window=1</c>): every flush sends one frame
///     and blocks for one ACK.
///     <para />
///     <b>Terminal-failure model:</b> any wire error, server error frame, or ACK timeout sets a
///     sticky <c>_terminalError</c> that subsequent calls re-throw. Recovery is to dispose the
///     sender and create a new one — there is no automatic reconnect.
/// </remarks>
internal sealed class QwpWebSocketSender : IQwpWebSocketSender
{
    private const long TicksPerMicrosecond = 10L;

    private readonly Dictionary<string, QwpTableBuffer> _tables = new(StringComparer.Ordinal);
    private readonly QwpSchemaCache _schemaCache;
    private readonly QwpSymbolDictionary _symbolDictionary = new();
    private readonly QwpInFlightWindow _inFlightWindow = new();
    private readonly QwpWebSocketTransport? _transport;
    private readonly byte[] _receiveBuffer;
    private readonly List<QwpTableBuffer> _flushBatch = new();

    // Async I/O — populated only when in_flight_window > 1.
    private readonly bool _asyncMode;
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
    private bool _disposed;

    private readonly record struct AsyncBatch(long Sequence, int BufferIndex, ReadOnlyMemory<byte> Frame);

    public QwpWebSocketSender(SenderOptions options)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        if (!options.IsWebSocket())
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"protocol must be ws or wss for {nameof(QwpWebSocketSender)}, got {options.protocol}");
        }

        if (options.in_flight_window < 1)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"in_flight_window must be >= 1, got {options.in_flight_window}");
        }

        _schemaCache = new QwpSchemaCache(options.max_schemas_per_connection);
        _receiveBuffer = new byte[QwpConstants.ErrorAckHeaderSize + QwpConstants.MaxErrorMessageBytes];
        _sfMode = !string.IsNullOrEmpty(options.sf_dir);

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
            return;
        }

        _asyncMode = options.in_flight_window > 1;

        var transportOpts = new QwpWebSocketTransportOptions
        {
            Uri = BuildUri(options),
            AuthorizationHeader = BuildAuthHeader(options),
            RequestDurableAck = options.request_durable_ack,
            RemoteCertificateValidationCallback = BuildCertificateValidator(options),
        };

        _transport = new QwpWebSocketTransport(transportOpts);

        try
        {
            _transport.ConnectAsync(CancellationToken.None).GetAwaiter().GetResult();
        }
        catch (Exception)
        {
            _transport.Dispose();
            throw;
        }

        if (_asyncMode)
        {
            _slot = new SemaphoreSlim(options.in_flight_window, options.in_flight_window);
            // Bounded to the in-flight window: producer back-pressure happens at the slot
            // semaphore, the channel just hands off the encoded frame.
            _sendChannel = Channel.CreateBounded<AsyncBatch>(new BoundedChannelOptions(options.in_flight_window)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            });
            _ioCts = new CancellationTokenSource();
            _sendLoopTask = Task.Run(() => SendLoop(_ioCts.Token));
            _receiveLoopTask = Task.Run(() => ReceiveLoop(_ioCts.Token));
        }
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

            var transportOpts = new QwpWebSocketTransportOptions
            {
                Uri = BuildUri(options),
                AuthorizationHeader = BuildAuthHeader(options),
                RequestDurableAck = options.request_durable_ack,
                RemoteCertificateValidationCallback = BuildCertificateValidator(options),
            };

            var policy = new QwpReconnectPolicy(
                options.reconnect_initial_backoff_millis,
                options.reconnect_max_backoff_millis,
                options.reconnect_max_duration_millis,
                jitter: QwpReconnectPolicy.UniformDoubleJitter);

            engine = new QwpCursorSendEngine(
                slotLock,
                ring,
                () => new QwpWebSocketTransport(transportOpts),
                policy,
                options.sf_append_deadline_millis,
                options.initial_connect_retry,
                maxTotalBytes: options.sf_max_total_bytes);

            engine.Start();

            if (options.drain_orphans)
            {
                var drainer = new QwpBackgroundDrainer(
                    () => new QwpWebSocketTransport(transportOpts),
                    policy,
                    segmentCapacity: options.sf_max_bytes,
                    drainTimeout: options.reconnect_max_duration_millis);
                pool = new QwpBackgroundDrainerPool(
                    options.max_background_drainers,
                    drainer,
                    shutdownWait: options.close_flush_timeout_millis);
                foreach (var orphanLock in QwpOrphanScanner.ClaimOrphans(sfRoot, options.sender_id))
                {
                    pool.Enqueue(orphanLock);
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
    public int RowCount
    {
        get
        {
            var total = 0;
            foreach (var t in _tables.Values)
            {
                total += t.RowCount;
            }

            return total;
        }
    }

    /// <inheritdoc />
    public bool WithinTransaction => false;

    /// <inheritdoc />
    public DateTime LastFlush { get; private set; } = DateTime.MinValue;

    // -- Transactions are not supported on WebSocket --------------------------

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
    public Task CommitAsync(CancellationToken ct = default)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, "transactions are not supported on the WebSocket transport");
    }

    /// <inheritdoc />
    public void Commit(CancellationToken ct = default)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, "transactions are not supported on the WebSocket transport");
    }

    // -- Row API -------------------------------------------------------------

    /// <inheritdoc />
    public ISender Table(ReadOnlySpan<char> name)
    {
        ThrowIfTerminal();
        var key = name.ToString();
        if (!_tables.TryGetValue(key, out var t))
        {
            t = new QwpTableBuffer(key, Options.max_name_len);
            _tables[key] = t;
        }

        _currentTable = t;
        return this;
    }

    /// <inheritdoc />
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        ThrowIfTerminal();
        var globalId = _symbolDictionary.Add(value.ToString());
        EnsureCurrentTable().AppendSymbol(name.ToString(), globalId);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendVarchar(name.ToString(), value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendLong(name.ToString(), value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, int value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendInt(name.ToString(), value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendBool(name.ToString(), value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDouble(name.ToString(), value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendTimestampMicros(name.ToString(), DateTimeToMicros(value));
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendTimestampMicros(name.ToString(), DateTimeToMicros(value.UtcDateTime));
        return this;
    }

    /// <inheritdoc />
    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendTimestampNanos(name.ToString(), timestampNanos);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, decimal value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendDecimal128(name.ToString(), value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, Guid value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendUuid(name.ToString(), value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, char value)
    {
        ThrowIfTerminal();
        EnsureCurrentTable().AppendChar(name.ToString(), value);
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
            var flat = new double[value.Length];
            Array.Copy(value, flat, value.Length);
            EnsureCurrentTable().AppendDoubleArray(name.ToString(), flat, shape);
        }
        else if (elementType == typeof(long))
        {
            var flat = new long[value.Length];
            Array.Copy(value, flat, value.Length);
            EnsureCurrentTable().AppendLongArray(name.ToString(), flat, shape);
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
            col.AppendDoubleArray(name.ToString(), System.Runtime.InteropServices.MemoryMarshal.Cast<T, double>(values), shape);
        }
        else if (typeof(T) == typeof(long))
        {
            col.AppendLongArray(name.ToString(), System.Runtime.InteropServices.MemoryMarshal.Cast<T, long>(values), shape);
        }
        else
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"array element type {typeof(T)} not supported; only double and long");
        }
    }

    // -- At / commit row -----------------------------------------------------

    /// <inheritdoc />
    public ValueTask AtAsync(DateTime value, CancellationToken ct = default)
    {
        At(value, ct);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default)
    {
        At(value, ct);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask AtAsync(long value, CancellationToken ct = default)
    {
        At(value, ct);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask AtNowAsync(CancellationToken ct = default)
    {
        AtNow(ct);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default)
    {
        AtNanos(timestampNanos, ct);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public void At(DateTime value, CancellationToken ct = default)
    {
        ThrowIfTerminal();
        GuardLastFlushNotSet();
        EnsureCurrentTable().At(DateTimeToMicros(value));
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
        FlushIfNecessary(ct);
    }

    // -- Send / flush --------------------------------------------------------

    /// <inheritdoc />
    public Task SendAsync(CancellationToken ct = default)
    {
        Send(ct);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Send(CancellationToken ct = default)
    {
        ThrowIfTerminal();
        if (_sfMode)
        {
            FlushToSfEngine(ct);
            return;
        }

        if (_asyncMode)
        {
            EnqueueAsync(ct, awaitDrain: true);
        }
        else
        {
            FlushAndAwaitAck(ct);
        }
    }

    private void FlushToSfEngine(CancellationToken ct)
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
            return;
        }

        // SF flushes are synchronous (AppendBlocking copies into mmap before returning), so a
        // single shared buffer is enough — no double-buffering needed.
        var builder = _encoderBuffers[0];
        int len;
        try
        {
            len = QwpEncoder.EncodeInto(
                builder, _flushBatch, _schemaCache, _symbolDictionary,
                selfSufficient: true,
                gorillaEnabled: Options.gorilla);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailTerminal(ex);
            throw _terminalError!;
        }

        try
        {
            _sfEngine!.AppendBlocking(builder.AsSpan(0, len), ct);
        }
        catch (IngressError)
        {
            // Engine surfaces its own terminal errors; bubble up unchanged.
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailTerminal(ex);
            throw _terminalError!;
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
            throw _terminalError!;
        }
    }

    private void FlushAndAwaitAck(CancellationToken ct)
    {
        // Sync mode runs single-buffered: index 0 is encoded into and sent before the next call.
        var len = EncodeFrameInto(0);
        if (len == 0)
        {
            return;
        }

        var frame = _encoderBuffers[0].WrittenMemory;
        var sequence = _nextSequence++;

        try
        {
            _transport!.SendBinaryAsync(frame, ct).GetAwaiter().GetResult();
            _inFlightWindow.Add(sequence);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailTerminal(ex);
            throw _terminalError!;
        }

        QwpResponse response;
        while (true)
        {
            try
            {
                var read = _transport.ReceiveFrameAsync(_receiveBuffer, ct).GetAwaiter().GetResult();
                response = QwpResponse.Parse(_receiveBuffer.AsSpan(0, read));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                FailTerminal(ex);
                throw _terminalError!;
            }

            if (response.IsDurableAck)
            {
                // Informational; absorb and keep waiting for the OK that closes the round-trip.
                ProcessTableEntries(response.TableEntries, isDurable: true);
                continue;
            }

            break;
        }

        if (!response.IsOk)
        {
            FailTerminal(response.ToException());
            throw _terminalError!;
        }

        // Stale ACK absorption: tolerate ACKs from earlier batches still in flight on this connection.
        // Anything covered by a higher cumulative ACK is silently absorbed by InFlightWindow.AcknowledgeUpTo.
        _inFlightWindow.AcknowledgeUpTo(response.Sequence);
        ProcessTableEntries(response.TableEntries, isDurable: false);
        OnFlushSucceeded();
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

    private void EnqueueAsync(CancellationToken ct, bool awaitDrain)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_ioCts!.Token, ct);
        var linkedCt = linked.Token;

        // Pick the next encoder buffer; ping-pong between two builders to overlap encode/send.
        // Acquire the matching ready signal before encoding so we don't overwrite a frame that the
        // SendLoop is still reading.
        var idx = _encoderIndex;
        _encoderIndex = (idx + 1) & 1;
        var releasedReady = false;
        try
        {
            _encoderReady[idx].Wait(linkedCt);
        }
        catch (OperationCanceledException) when (_terminalError is not null)
        {
            ThrowIfTerminal();
            throw;
        }

        try
        {
            var len = EncodeFrameInto(idx);
            if (len == 0)
            {
                _encoderReady[idx].Release();
                releasedReady = true;
            }
            else
            {
                // Commit symbol delta and clear tables eagerly so the next flush builds on new state.
                OnFlushSucceeded();

                try
                {
                    _slot!.Wait(linkedCt);
                }
                catch (OperationCanceledException) when (_terminalError is not null)
                {
                    _encoderReady[idx].Release();
                    releasedReady = true;
                    ThrowIfTerminal();
                    throw;
                }

                var seq = _nextSequence++;
                try
                {
                    // Mark the sequence as in-flight before handoff so AwaitEmpty sees the pending
                    // batch. Doing this in the SendLoop instead would race: the producer's
                    // AwaitEmpty could see an "empty" window and return prematurely.
                    _inFlightWindow.Add(seq);
                    var frame = _encoderBuffers[idx].WrittenMemory;
                    _sendChannel!.Writer.WriteAsync(new AsyncBatch(seq, idx, frame), linkedCt)
                        .AsTask().GetAwaiter().GetResult();
                }
                catch (OperationCanceledException) when (_terminalError is not null)
                {
                    _slot.Release();
                    _encoderReady[idx].Release();
                    releasedReady = true;
                    ThrowIfTerminal();
                    throw;
                }
                catch (Exception)
                {
                    _slot.Release();
                    _encoderReady[idx].Release();
                    releasedReady = true;
                    throw;
                }
            }
        }
        catch when (!releasedReady)
        {
            // Last-resort safety net: if anything else escapes, free the buffer's ready signal so
            // the next encode can proceed (the failed frame was never enqueued).
            SfCleanup.Run(() => _encoderReady[idx].Release());
            throw;
        }

        if (awaitDrain)
        {
            try
            {
                _inFlightWindow.AwaitEmpty(Options.close_timeout, linkedCt);
            }
            catch (OperationCanceledException) when (_terminalError is not null)
            {
                ThrowIfTerminal();
                throw;
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not IngressError)
            {
                FailTerminal(ex);
                throw _terminalError!;
            }
        }
    }

    private void OnFlushSucceeded()
    {
        _symbolDictionary.Commit();
        foreach (var t in _flushBatch)
        {
            t.Clear();
        }

        _flushBatch.Clear();
        LastFlush = DateTime.UtcNow;
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
                    // The buffer's ready slot is leaked; the sender is terminal anyway and any
                    // pending Wait on this signal will unblock once _ioCts cancels.
                    return;
                }
                finally
                {
                    // Release the buffer for the next encoder use. Receiver still owes us an ACK
                    // that frees the in-flight slot (_slot) — the two signals are independent.
                    if ((uint)batch.BufferIndex < (uint)_encoderReady.Length)
                    {
                        _encoderReady[batch.BufferIndex].Release();
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
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
                    read = await _transport!.ReceiveFrameAsync(_receiveBuffer, ct).ConfigureAwait(false);
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
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
        }
    }

    // -- Misc ----------------------------------------------------------------

    /// <inheritdoc />
    public void Truncate()
    {
        // Buffers self-grow; nothing to trim today. Match HTTP/TCP signatures so callers can swap.
    }

    /// <inheritdoc />
    public void CancelRow()
    {
        // Untouched columns get null-padded on At*, so a pending row that isn't At'd is invisible
        // to the wire. CancelRow without a pending row is a no-op.
    }

    /// <inheritdoc />
    public void Clear()
    {
        foreach (var t in _tables.Values)
        {
            t.Clear();
        }

        _currentTable = null;
    }

    // -- IQwpWebSocketSender ----------------------------------------------------

    /// <inheritdoc />
    public long GetHighestAckedSeqTxn(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        lock (_seqTxnLock)
        {
            return _committedSeqTxn.TryGetValue(tableName, out var v) ? v : -1L;
        }
    }

    /// <inheritdoc />
    public long GetHighestDurableSeqTxn(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        lock (_seqTxnLock)
        {
            return _durableSeqTxn.TryGetValue(tableName, out var v) ? v : -1L;
        }
    }

    /// <inheritdoc />
    public void Ping(CancellationToken ct = default)
    {
        ThrowIfTerminal();

        if (_sfMode)
        {
            // SF mode: "everything sent so far is acknowledged" maps directly to engine.FlushAsync.
            _sfEngine!.FlushAsync(Options.close_flush_timeout_millis, ct).GetAwaiter().GetResult();
            return;
        }

        // We don't drive WS-level PING (ClientWebSocket exposes no public API for it). Instead we
        // expose the user-observable contract: "after Ping returns, every batch sent so far has been
        // acknowledged and the per-table seqTxn watermarks reflect that."
        using var linked = _ioCts is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(_ioCts.Token, ct);
        var waitCt = linked?.Token ?? ct;
        try
        {
            _inFlightWindow.AwaitEmpty(Options.close_timeout, waitCt);
        }
        catch (OperationCanceledException) when (_terminalError is not null)
        {
            ThrowIfTerminal();
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException && ex is not IngressError)
        {
            FailTerminal(ex);
            throw _terminalError!;
        }
    }

    /// <inheritdoc />
    public Task PingAsync(CancellationToken ct = default)
    {
        Ping(ct);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_sfMode)
        {
            DisposeSfStack();
            return;
        }

        try
        {
            if (_terminalError is null)
            {
                if (_asyncMode)
                {
                    EnqueueAsync(CancellationToken.None, awaitDrain: true);
                }
                else
                {
                    FlushAndAwaitAck(CancellationToken.None);
                }
            }
        }
        catch (Exception)
        {
            // best-effort flush on close
        }

        if (_asyncMode)
        {
            try
            {
                _sendChannel!.Writer.TryComplete();
                _ioCts!.Cancel();
                Task.WhenAll(_sendLoopTask!, _receiveLoopTask!).Wait(TimeSpan.FromSeconds(2));
            }
            catch (Exception)
            {
                // best-effort shutdown
            }
            finally
            {
                _ioCts!.Dispose();
                _slot!.Dispose();
            }
        }

        foreach (var sem in _encoderReady)
        {
            try { sem.Dispose(); } catch { /* best-effort */ }
        }

        try
        {
            _transport!.CloseAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch (Exception)
        {
            // best-effort close
        }

        _transport!.Dispose();
    }

    private void DisposeSfStack()
    {
        try
        {
            if (_terminalError is null)
            {
                FlushToSfEngine(CancellationToken.None);
                _sfEngine!.FlushAsync(Options.close_flush_timeout_millis).GetAwaiter().GetResult();
            }
        }
        catch (Exception)
        {
            // best-effort flush on close
        }

        SfCleanup.Dispose(_sfDrainerPool);
        SfCleanup.Dispose(_sfEngine);

        foreach (var sem in _encoderReady)
        {
            try { sem.Dispose(); } catch { /* best-effort */ }
        }
    }

    // -- Internals -----------------------------------------------------------

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
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpWebSocketSender));
        }

        if (_terminalError is not null)
        {
            // Re-wrap so the user sees a fresh stack trace pointing to their call site, but
            // preserves the original failure as the inner exception.
            throw new IngressError(_terminalError.code, _terminalError.Message, _terminalError);
        }

        if (_sfMode && _sfEngine!.IsTerminallyFailed)
        {
            var inner = _sfEngine.TerminalError;
            var code = (inner as IngressError)?.code ?? ErrorCode.ServerFlushError;
            var msg = inner?.Message ?? "QWP cursor engine failed terminally";
            throw new IngressError(code, msg, inner ?? new InvalidOperationException(msg));
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
        }
    }

    private void FlushIfNecessary(CancellationToken ct)
    {
        if (Options.auto_flush != AutoFlushType.on)
        {
            return;
        }

        var rowsTrigger = Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows;
        var bytesTrigger = Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes;
        var timeTrigger = Options.auto_flush_interval > TimeSpan.Zero
                          && DateTime.UtcNow - LastFlush >= Options.auto_flush_interval;

        if (!(rowsTrigger || bytesTrigger || timeTrigger))
        {
            return;
        }

        if (_sfMode)
        {
            FlushToSfEngine(ct);
            return;
        }

        if (_asyncMode)
        {
            // Auto-flush enqueues but does not block the producer on ACK drain.
            EnqueueAsync(ct, awaitDrain: false);
        }
        else
        {
            FlushAndAwaitAck(ct);
        }
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
        // Treat DateTime as UTC. .NET ticks are 100 ns; QWP TIMESTAMP is microseconds.
        var utc = value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;
        return (utc - DateTime.UnixEpoch).Ticks / TicksPerMicrosecond;
    }

    private static Uri BuildUri(SenderOptions options)
    {
        var scheme = options.protocol == ProtocolType.wss ? "wss" : "ws";
        var host = options.Host;
        var port = options.Port;
        return new Uri($"{scheme}://{host}:{port}{QwpConstants.WritePath}");
    }

    private static string? BuildAuthHeader(SenderOptions options)
    {
        if (!string.IsNullOrEmpty(options.username) && !string.IsNullOrEmpty(options.password))
        {
            var pair = $"{options.username}:{options.password}";
            return "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(pair));
        }

        if (!string.IsNullOrEmpty(options.token))
        {
            return "Bearer " + options.token;
        }

        return null;
    }

    private static System.Net.Security.RemoteCertificateValidationCallback? BuildCertificateValidator(SenderOptions options)
    {
        if (options.tls_verify == TlsVerifyType.unsafe_off)
        {
            return (_, _, _, _) => true;
        }

        return null;
    }
}

#endif
