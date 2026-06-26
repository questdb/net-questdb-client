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

using System.Buffers;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     SF send engine: owns a slot's segment ring and a background I/O loop that drains envelopes
///     to the server, reconnecting on transient failure. Send and receive run as concurrent pumps —
///     send walks the cursor and ships frames; receive consumes cumulative ACKs and trims the ring.
///     On reconnect the cursor rewinds to the first un-acked FSN so in-flight frames are replayed.
/// </summary>
internal sealed class QwpCursorSendEngine : IDisposable
{
    private const int AckBufferSize = 4096;

    // A valid OK ACK is 11 + N*(10 + nameBytes); with up to 65535 touched tables and 127-byte
    // names it can reach ~9 MiB, so the receive buffer grows on demand. The cap stays well above
    // that worst case while still rejecting a genuinely malformed oversized frame.
    private const int AckBufferMaxBytes = QwpConstants.MaxResultBatchWireBytes;

    private readonly QwpSlotLock? _slotLock;
    private readonly QwpSegmentRing _ring;
    private readonly QwpSegmentManager _segmentManager;
    private readonly Func<IQwpCursorTransport> _transportFactory;
    private readonly QwpReconnectPolicy _reconnectPolicy;
    private readonly TimeSpan _appendDeadline;
    private readonly InitialConnectMode _initialConnectMode;
    private readonly TaskCompletionSource<bool> _firstConnectGate =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Func<bool>? _skipBackoffPredicate;
    private readonly QwpSenderErrorDispatcher? _errorDispatcher;
    private readonly Action<QuestDB.Senders.SenderConnectionEvent>? _connectionEventSink;
    private (string Host, int Port)? _liveEndpoint;
    private long _currentRoundSeq;
    private readonly SenderErrorPolicyResolver? _policyResolver;
    private readonly QwpAckWatermark? _ackWatermark;
    private readonly object _stateLock = new();
    private readonly byte[] _sendBuffer;
    // Grows on demand for oversized-but-valid ACKs; reused across receive-pump iterations.
    private byte[] _ackBuffer;
    private readonly bool _durableAckMode;
    private readonly Queue<PendingDurable> _pendingDurable = new();
    private readonly Dictionary<string, long> _durableTableWatermarks = new(StringComparer.Ordinal);

    private long _cursorFsn;
    private long _ackedFsn;
    private long _sentFsnHighWatermark;
    private long _totalAcks;
    private long _totalFramesSent;
    private long _totalServerErrors;
    private long _totalReconnectAttempts;
    private long _totalReconnectsSucceeded;
    private int _negotiatedMaxBatchSize;
    private bool _terminal;
    private Exception? _terminalError;
    private bool _seenFirstConnect;
    private bool _disposed;
    private bool _started;

    private TaskCompletionSource<bool> _appendSignal = NewSignal();
    private TaskCompletionSource<bool> _ackSignal = NewSignal();

    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;
    private Action<QwpTableEntry, bool>? _tableEntryHandler;

    /// <param name="slotLock">
    ///     The slot lock to dispose alongside the engine. Pass <c>null</c> when the caller (e.g.
    ///     <see cref="QwpBackgroundDrainerPool" />) owns the lock externally — the engine will
    ///     drive the wire but leave the lock alone on dispose.
    /// </param>
    /// <param name="ring">Segment ring backing the engine's frames; the engine owns its disposal.</param>
    /// <param name="transportFactory">Factory that produces a fresh transport on each (re)connect.</param>
    /// <param name="reconnectPolicy">Backoff policy applied between transient wire failures.</param>
    /// <param name="appendDeadline">Max time <see cref="AppendBlocking" /> will wait when the disk cap is hit.</param>
    /// <param name="initialConnectMode">First-connect retry policy. See <see cref="InitialConnectMode" />.</param>
    /// <param name="maxTotalBytes">Disk cap forwarded to the engine's <see cref="QwpSegmentManager" />.</param>
    /// <param name="skipBackoffPredicate">
    ///     When non-null and returns <c>true</c> after a connect failure, the engine retries
    ///     immediately instead of waiting the reconnect backoff. Lets multi-host failover walk
    ///     the full address list before paying the backoff cost.
    /// </param>
    /// <param name="errorDispatcher">
    ///     Optional dispatcher that delivers <see cref="SenderError" /> notifications off the
    ///     I/O thread. The engine never invokes user code directly.
    /// </param>
    /// <param name="policyResolver">
    ///     Optional override for the per-<see cref="SenderErrorCategory" /> default policy.
    /// </param>
    /// <param name="durableAckMode">
    ///     When <c>true</c>, trim is driven by <see cref="QwpStatusCode.DurableAck" /> frames
    ///     covering the longest pending-OK prefix, not by <see cref="QwpStatusCode.Ok" />
    ///     frames directly. The caller must have verified the server echoed
    ///     <c>X-QWP-Durable-Ack: enabled</c> on the upgrade.
    /// </param>
    /// <param name="ackWatermark">
    ///     Optional persisted durable-ack high-water mark. When supplied, refines the startup
    ///     ackedFsn seed past frames the previous session already had durable-acks for,
    ///     avoiding row-level re-replay inside the lowest surviving sealed segment. The engine
    ///     takes ownership and disposes the watermark on shutdown.
    /// </param>
    public QwpCursorSendEngine(
        QwpSlotLock? slotLock,
        QwpSegmentRing ring,
        Func<IQwpCursorTransport> transportFactory,
        QwpReconnectPolicy reconnectPolicy,
        TimeSpan appendDeadline,
        InitialConnectMode initialConnectMode,
        long maxTotalBytes = long.MaxValue,
        Func<bool>? skipBackoffPredicate = null,
        QwpSenderErrorDispatcher? errorDispatcher = null,
        SenderErrorPolicyResolver? policyResolver = null,
        bool durableAckMode = false,
        QwpAckWatermark? ackWatermark = null,
        Action<QuestDB.Senders.SenderConnectionEvent>? connectionEventSink = null)
    {
        ArgumentNullException.ThrowIfNull(ring);
        ArgumentNullException.ThrowIfNull(transportFactory);
        ArgumentNullException.ThrowIfNull(reconnectPolicy);
        if (appendDeadline <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(appendDeadline), "must be positive");
        }

        _slotLock = slotLock;
        _ring = ring;
        _transportFactory = transportFactory;
        _reconnectPolicy = reconnectPolicy;
        _appendDeadline = appendDeadline;
        _initialConnectMode = initialConnectMode;
        _skipBackoffPredicate = skipBackoffPredicate;
        _errorDispatcher = errorDispatcher;
        _policyResolver = policyResolver;
        _durableAckMode = durableAckMode;
        _ackWatermark = ackWatermark;
        _connectionEventSink = connectionEventSink;

        var baseSeed = ring.OldestFsn;
        if (ackWatermark is not null)
        {
            var wm = ackWatermark.Read();
            if (wm != QwpAckWatermark.Invalid)
            {
                // max() absorbs either ordering of the manager's persist-then-trim tick.
                var candidate = Math.Max(baseSeed, wm + 1);
                // candidate > NextFsn means corruption: a clean prior session can't produce one.
                if (candidate <= ring.NextFsn)
                {
                    baseSeed = candidate;
                }
            }
        }

        _cursorFsn = baseSeed;
        _ackedFsn = baseSeed;
        if (baseSeed > ring.OldestFsn)
        {
            ring.Acknowledge(baseSeed - 1);
        }

        _segmentManager = new QwpSegmentManager(ring, maxTotalBytes);
        _segmentManager.SetAckWatermark(ackWatermark);
        _sendBuffer = new byte[ring.SegmentCapacity];
        _ackBuffer = new byte[AckBufferSize];
        // Spare arrival from the manager wakes any producer parked in AppendBlocking.
        ring.SetSpareInstalledCallback(() =>
        {
            lock (_stateLock) FireAckSignalLocked();
        });
    }

    /// <summary>FSN of the next frame to be appended (== <see cref="QwpSegmentRing.NextFsn" />).</summary>
    public long NextFsn
    {
        get
        {
            lock (_stateLock)
            {
                return _ring.NextFsn;
            }
        }
    }

    /// <summary>First un-acked FSN. <c>NextFsn == AckedFsn</c> means the slot is fully drained.</summary>
    public long AckedFsn
    {
        get
        {
            lock (_stateLock)
            {
                return _ackedFsn;
            }
        }
    }

    public long TotalFramesSent => Volatile.Read(ref _totalFramesSent);
    public long TotalAcks => Volatile.Read(ref _totalAcks);
    public long TotalServerErrors => Volatile.Read(ref _totalServerErrors);
    public long TotalReconnectAttempts => Volatile.Read(ref _totalReconnectAttempts);
    public long TotalReconnectsSucceeded => Volatile.Read(ref _totalReconnectsSucceeded);

    /// <summary>
    ///     Server-advertised hard cap on QWP ingest payload bytes, refreshed on every successful
    ///     (re)connect. <c>0</c> when the current server did not advertise it.
    /// </summary>
    public int NegotiatedMaxBatchSize => Volatile.Read(ref _negotiatedMaxBatchSize);

    /// <summary>True once the engine has hit a terminal failure.</summary>
    public bool IsTerminallyFailed
    {
        get
        {
            lock (_stateLock)
            {
                return _terminal;
            }
        }
    }

    /// <summary>The terminal error, if any.</summary>
    public Exception? TerminalError
    {
        get
        {
            lock (_stateLock)
            {
                return _terminalError;
            }
        }
    }

    /// <summary>
    ///     Optional callback invoked for every per-table entry the server returns in OK / DurableAck
    ///     frames. The boolean argument is <c>true</c> when the source was a DurableAck.
    /// </summary>
    public void SetTableEntryHandler(Action<QwpTableEntry, bool>? handler)
    {
        Volatile.Write(ref _tableEntryHandler, handler);
    }

    /// <summary>Launches the I/O loop. Idempotent; subsequent calls are no-ops.</summary>
    public void Start()
    {
        lock (_stateLock)
        {
            EnsureNotDisposed();
            if (_started)
            {
                return;
            }

            _started = true;
            _loopCts = new CancellationTokenSource();
        }

        if (_slotLock is not null)
        {
            _segmentManager.SetHeartbeatCallback(_slotLock.RefreshHeartbeat);
        }
        _segmentManager.Start();
        _loopTask = Task.Run(() => RunLoopAsync(_loopCts!.Token));
    }

    /// <summary>
    ///     Persists <paramref name="frame" /> to disk. Blocks the calling thread on backpressure
    ///     until the ring drains or <c>sf_append_deadline</c> elapses.
    /// </summary>
    public void AppendBlocking(ReadOnlySpan<byte> frame, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        if (frame.Length == 0)
        {
            throw new ArgumentException("empty frames are not permitted", nameof(frame));
        }

        lock (_stateLock)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
            if (_terminal) throw WrapTerminalForProducer();

            if (_ring.TryAppend(frame))
            {
                FireAppendSignalLocked();
                return;
            }
        }

        AppendBlockingSlow(frame, cancellationToken);
    }

    /// <summary>
    ///     Async counterpart of <see cref="AppendBlocking" />: the returned task completes once the
    ///     frame has been persisted to the ring or throws on terminal failure / deadline / cancellation.
    /// </summary>
    public ValueTask AppendAsync(ReadOnlyMemory<byte> frame, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        if (frame.Length == 0)
        {
            throw new ArgumentException("empty frames are not permitted", nameof(frame));
        }

        lock (_stateLock)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
            if (_terminal) throw WrapTerminalForProducer();

            if (_ring.TryAppend(frame.Span))
            {
                FireAppendSignalLocked();
                return ValueTask.CompletedTask;
            }
        }

        return AppendAsyncSlow(frame, cancellationToken);
    }

    private void AppendBlockingSlow(ReadOnlySpan<byte> frame, CancellationToken cancellationToken)
    {
        var rented = ArrayPool<byte>.Shared.Rent(frame.Length);
        var len = frame.Length;
        frame.CopyTo(rented);
        try
        {
            var deadlineMs = Environment.TickCount64 + (long)_appendDeadline.TotalMilliseconds;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Task waitTask;
                lock (_stateLock)
                {
                    if (_disposed) throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
                    if (_terminal) throw WrapTerminalForProducer();

                    if (_ring.TryAppend(rented.AsSpan(0, len)))
                    {
                        FireAppendSignalLocked();
                        return;
                    }

                    waitTask = _ackSignal.Task;
                }

                var remainingMs = deadlineMs - Environment.TickCount64;
                if (remainingMs <= 0)
                {
                    var svcErr = _segmentManager.LastServiceError;
                    var suffix = svcErr is not null
                        ? $"; last segment-manager error: {svcErr.GetType().Name}: {svcErr.Message}"
                        : string.Empty;
                    throw new IngressError(
                        ErrorCode.ServerFlushError,
                        $"sf_append_deadline ({_appendDeadline.TotalMilliseconds:F0} ms) expired with the ring full{suffix}");
                }

                try
                {
                    // Bound the wait so a missed signal (e.g. manager's first heartbeat installing
                    // the initial spare) cannot stall the producer for the full deadline.
                    var slice = (int)Math.Min(remainingMs, 200);
                    waitTask.Wait(slice, cancellationToken);
                }
                catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)
                {
                    throw ex.InnerException;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private async ValueTask AppendAsyncSlow(ReadOnlyMemory<byte> frame, CancellationToken cancellationToken)
    {
        var deadlineMs = Environment.TickCount64 + (long)_appendDeadline.TotalMilliseconds;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Task waitTask;
            lock (_stateLock)
            {
                if (_disposed) throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
                if (_terminal) throw WrapTerminalForProducer();

                if (_ring.TryAppend(frame.Span))
                {
                    FireAppendSignalLocked();
                    return;
                }

                waitTask = _ackSignal.Task;
            }

            var remainingMs = deadlineMs - Environment.TickCount64;
            if (remainingMs <= 0)
            {
                var svcErr = _segmentManager.LastServiceError;
                var suffix = svcErr is not null
                    ? $"; last segment-manager error: {svcErr.GetType().Name}: {svcErr.Message}"
                    : string.Empty;
                throw new IngressError(
                    ErrorCode.ServerFlushError,
                    $"sf_append_deadline ({_appendDeadline.TotalMilliseconds:F0} ms) expired with the ring full{suffix}");
            }

            var slice = TimeSpan.FromMilliseconds(Math.Min(remainingMs, 200));
            try
            {
                await waitTask.WaitAsync(slice, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
        }
    }

    /// <summary>
    ///     Blocks until <see cref="AckedFsn" /> reaches <paramref name="targetFsn" /> or
    ///     <paramref name="timeout" /> elapses. Returns <c>true</c> on success, <c>false</c> on
    ///     timeout. Throws on terminal failure or cancellation.
    /// </summary>
    public async Task<bool> AwaitAckedFsnAsync(long targetFsn, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        var infiniteTimeout = timeout == Timeout.InfiniteTimeSpan;
        var deadlineMs = infiniteTimeout
            ? long.MaxValue
            : Environment.TickCount64 + (long)timeout.TotalMilliseconds;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Task waitTask;
            lock (_stateLock)
            {
                if (_disposed) throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
                if (_terminal) throw WrapTerminalForProducer();
                if (_ackedFsn >= targetFsn) return true;
                waitTask = _ackSignal.Task;
            }

            if (infiniteTimeout)
            {
                await waitTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            var remainingMs = deadlineMs - Environment.TickCount64;
            if (remainingMs <= 0) return false;

            try
            {
                await waitTask.WaitAsync(TimeSpan.FromMilliseconds(remainingMs), cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
        }
    }

    /// <summary>Returns once every appended frame is acked, or throws on timeout / terminal failure.</summary>
    public async Task FlushAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        var infiniteTimeout = timeout == Timeout.InfiniteTimeSpan;
        var deadlineMs = infiniteTimeout
            ? long.MaxValue
            : Environment.TickCount64 + (long)timeout.TotalMilliseconds;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Task waitTask;
            lock (_stateLock)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
                }

                if (_terminal)
                {
                    throw WrapTerminalForProducer();
                }

                if (_ackedFsn >= _ring.NextFsn)
                {
                    return;
                }

                waitTask = _ackSignal.Task;
            }

            if (infiniteTimeout)
            {
                // Task.WaitAsync(TimeSpan, ct) rejects timeouts above ~49.7 days; route infinite waits
                // through the no-timeout overload to avoid ArgumentOutOfRangeException.
                await waitTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            var remainingMs = deadlineMs - Environment.TickCount64;
            if (remainingMs <= 0)
            {
                throw new TimeoutException(
                    $"close_flush_timeout ({timeout.TotalMilliseconds:F0} ms) expired with un-acked frames pending");
            }

            try
            {
                await waitTask.WaitAsync(TimeSpan.FromMilliseconds(remainingMs), cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        CancellationTokenSource? cts;
        Task? loop;
        bool fullyDrained;
        string? slotDir;
        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }

            Volatile.Write(ref _disposed, true);
            cts = _loopCts;
            loop = _loopTask;
            // Capture drain state BEFORE disposing the ring — once disposed, NextFsn isn't safe to read.
            fullyDrained = _ackedFsn >= _ring.NextFsn;
            slotDir = _ring.Directory;
            // Wake blocked producers so they can observe _disposed and throw.
            FireAckSignalLocked();
            FireAppendSignalLocked();
        }

        SfCleanup.Run(() => cts?.Cancel());
        _segmentManager.RequestShutdown();

        // Defer ring/lock release if either pump is still alive: tearing down shared state would
        // crash on disposed mmaps or let another sender poach the slot mid-send.
        var pending = new[] { loop, _segmentManager.WorkerTask }
            .Where(t => t is not null)
            .Cast<Task>()
            .ToArray();
        var allJoined = pending.Length == 0 || SafeWaitAll(pending, TimeSpan.FromSeconds(5));

        if (!allJoined)
        {
            // Pumps still alive; leak _loopCts rather than risk ObjectDisposedException on a late
            // token read. The continuation disposes it once both pumps actually exit.
            Task.WhenAll(pending).ContinueWith(
                _ => { ReleaseSharedResources(fullyDrained, slotDir); SfCleanup.Dispose(cts); },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return;
        }

        SfCleanup.Dispose(cts);
        ReleaseSharedResources(fullyDrained, slotDir);
    }

    private static bool SafeWaitAll(Task[] tasks, TimeSpan timeout)
    {
        try { return Task.WaitAll(tasks, timeout); }
        catch { return true; }
    }

    private void ReleaseSharedResources(bool fullyDrained, string? slotDir)
    {
        SfCleanup.Dispose(_segmentManager);
        SfCleanup.Dispose(_ring);
        SfCleanup.Dispose(_ackWatermark);

        if (fullyDrained && slotDir is not null)
        {
            UnlinkSegmentFiles(slotDir);
            QwpAckWatermark.RemoveOrphan(slotDir);
        }

        SfCleanup.Dispose(_slotLock);
    }

    private static void UnlinkSegmentFiles(string slotDirectory)
    {
        try
        {
            foreach (var path in QwpFiles.EnumerateFiles(slotDirectory, "sf-*.sfa"))
            {
                SfCleanup.DeleteFile(path);
            }
        }
        catch (Exception)
        {
            // Best-effort — slot dir may have been removed externally.
        }
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        try
        {
            await RunLoopBodyAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            SetTerminal(ex);
        }
        finally
        {
            bool needFallback;
            lock (_stateLock) { needFallback = !_terminal; }
            if (needFallback)
            {
                FireFirstConnectFailed(
                    new IngressError(ErrorCode.SocketError, "SF engine loop exited before first connect"));
            }
        }
    }

    private async Task RunLoopBodyAsync(CancellationToken ct)
    {
        var backoff = new BackoffState();

        while (!ct.IsCancellationRequested)
        {
            IQwpCursorTransport? transport;
            try
            {
                transport = _transportFactory();
            }
            catch (Exception ex)
            {
                SetTerminal(ex);
                return;
            }

            try
            {
                Interlocked.Increment(ref _totalReconnectAttempts);
                try
                {
                    await transport.ConnectAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (IngressError ex) when (
                    ex.code is ErrorCode.AuthError or ErrorCode.DurableAckNotSupported)
                {
                    if (ex.code is ErrorCode.AuthError)
                    {
                        EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.AuthFailed, cause: ex);
                    }
                    SetTerminal(ex);
                    return;
                }
                catch (QwpIngressRoleRejectedException ex)
                {
                    EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.EndpointAttemptFailed,
                        cause: ex, endpoint: transport.Endpoint);
                    backoff.ResetAttempt();
                    backoff.OutageStartTickMs ??= Environment.TickCount64;
                    var elapsed = TimeSpan.FromMilliseconds(
                        Environment.TickCount64 - backoff.OutageStartTickMs.Value);
                    if (elapsed >= _reconnectPolicy.MaxOutageDuration)
                    {
                        EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.ReconnectBudgetExhausted,
                            cause: ex, endpoint: transport.Endpoint);
                        SetTerminal(ex);
                        return;
                    }

                    if (_skipBackoffPredicate?.Invoke() == true)
                    {
                        continue;
                    }

                    EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.AllEndpointsUnreachable,
                        cause: ex, endpoint: transport.Endpoint);
                    Interlocked.Increment(ref _currentRoundSeq);

                    var remaining = _reconnectPolicy.MaxOutageDuration - elapsed;
                    var jittered = _reconnectPolicy.ComputeBackoff(0);
                    var sleep = remaining < jittered ? remaining : jittered;
                    try
                    {
                        await Task.Delay(sleep, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }

                    elapsed = TimeSpan.FromMilliseconds(
                        Environment.TickCount64 - backoff.OutageStartTickMs.Value);
                    if (elapsed >= _reconnectPolicy.MaxOutageDuration)
                    {
                        EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.ReconnectBudgetExhausted,
                            cause: ex, endpoint: transport.Endpoint);
                        SetTerminal(ex);
                        return;
                    }
                    continue;
                }
                catch (Exception ex)
                {
                    EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.EndpointAttemptFailed,
                        cause: ex, endpoint: transport.Endpoint);

                    if (_skipBackoffPredicate?.Invoke() == true)
                    {
                        continue;
                    }

                    EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.AllEndpointsUnreachable,
                        cause: ex, endpoint: transport.Endpoint);
                    Interlocked.Increment(ref _currentRoundSeq);

                    if (!_seenFirstConnect && _initialConnectMode == InitialConnectMode.off)
                    {
                        SetTerminal(ex);
                        return;
                    }

                    if (!await BackoffOrGiveUpAsync(ex, backoff, ct).ConfigureAwait(false))
                    {
                        return;
                    }

                    continue;
                }

                var wasFirst = !_seenFirstConnect;
                var newEndpoint = transport.Endpoint;
                var prevEndpoint = _liveEndpoint;
                _seenFirstConnect = true;
                _liveEndpoint = newEndpoint;
                Volatile.Write(ref _negotiatedMaxBatchSize, transport.NegotiatedMaxBatchSize);
                Interlocked.Increment(ref _totalReconnectsSucceeded);
                backoff.Reset();
                FireFirstConnectSucceeded();

                QuestDB.Senders.SenderConnectionEventKind successKind;
                if (wasFirst)
                {
                    successKind = QuestDB.Senders.SenderConnectionEventKind.Connected;
                }
                else if (newEndpoint is not null && prevEndpoint is not null
                                                 && (newEndpoint.Value.Host != prevEndpoint.Value.Host
                                                     || newEndpoint.Value.Port != prevEndpoint.Value.Port))
                {
                    successKind = QuestDB.Senders.SenderConnectionEventKind.FailedOver;
                }
                else
                {
                    successKind = QuestDB.Senders.SenderConnectionEventKind.Reconnected;
                }
                EmitConnectionEvent(successKind, cause: null, endpoint: newEndpoint);

                long fsnAtZero;
                lock (_stateLock)
                {
                    // Invariant: the manager only trims segments at or below ackedFsn-1, so the
                    // ring's oldest FSN must never sit ahead of the cursor's resume point.
                    // A violation means trim and ack are out of sync — surface it as terminal
                    // rather than silently skip un-acked frames at the ring head.
                    if (_ackedFsn < _ring.OldestFsn)
                    {
                        throw new InvalidDataException(
                            $"cursor invariant violation: ackedFsn={_ackedFsn} < ring.OldestFsn={_ring.OldestFsn}");
                    }
                    _cursorFsn = _ackedFsn;
                    fsnAtZero = _cursorFsn;
                    _sentFsnHighWatermark = _cursorFsn - 1;
                    // Pending durable state is per-connection (wireSeq is reset on every upgrade);
                    // the new server replays cumulative watermarks from scratch.
                    _pendingDurable.Clear();
                    _durableTableWatermarks.Clear();
                }

                try
                {
                    await RunConnectionAsync(transport, fsnAtZero, ct).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (HaltCarrier hc)
                {
                    SetTerminal(hc.Wire, hc.SenderError);
                    return;
                }
                catch (QwpProtocolViolationException ex)
                {
                    SetTerminal(ex, BuildProtocolViolationError(ex));
                    return;
                }
                catch (Exception ex) when (IsTerminalServerError(ex))
                {
                    SetTerminal(ex);
                    return;
                }
                catch (Exception ex)
                {
                    EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.Disconnected,
                        cause: ex, endpoint: _liveEndpoint);
                    _liveEndpoint = null;
                    if (!await BackoffOrGiveUpAsync(ex, backoff, ct).ConfigureAwait(false))
                    {
                        return;
                    }
                }
            }
            finally
            {
                SfCleanup.Dispose(transport);
            }
        }
    }

    private sealed class BackoffState
    {
        public int Attempt;
        public long? OutageStartTickMs;

        public void Reset()
        {
            Attempt = 0;
            OutageStartTickMs = null;
        }

        // Role-reject path resets attempt counter but preserves the outage clock so the
        // wall-clock budget still bounds a stuck PRIMARY_CATCHUP topology.
        public void ResetAttempt()
        {
            Attempt = 0;
        }
    }

    // Pipelined send + receive: two pumps share state under _stateLock. A linked CTS means a fault
    // in either pump cancels the other; the connection then closes and the outer loop reconnects.
    private async Task RunConnectionAsync(IQwpCursorTransport transport, long fsnAtZero, CancellationToken ct)
    {
        using var connCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var connToken = connCts.Token;
        var sendTask = Task.Run(() => SendPumpAsync(transport, connToken));
        var recvTask = Task.Run(() => ReceivePumpAsync(transport, fsnAtZero, connToken));

        try
        {
            await Task.WhenAny(sendTask, recvTask).ConfigureAwait(false);
        }
        finally
        {
            connCts.Cancel();
        }

        try
        {
            await Task.WhenAll(sendTask, recvTask).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }

        // Both pumps can fault simultaneously when the server closes the socket: recv sees the
        // CLOSE frame while send's in-flight write fails with a generic transport error. Prefer
        // the terminal fault so QwpProtocolViolationException / QwpException don't get masked
        // by the concurrent send-side WebSocketException, which would otherwise route through
        // the transient reconnect path.
        var sendFault = sendTask.Exception?.GetBaseException();
        var recvFault = recvTask.Exception?.GetBaseException();
        var fault = PickTerminalFault(sendFault, recvFault) ?? sendFault ?? recvFault;
        if (fault is not null)
        {
            throw fault;
        }

        if (sendTask.IsCanceled || recvTask.IsCanceled)
        {
            throw new OperationCanceledException(connCts.Token);
        }

        // Both pumps returned without throwing OCE: graceful shutdown via outer ct or terminal.
    }

    private async Task SendPumpAsync(IQwpCursorTransport transport, CancellationToken ct)
    {
        var sendBuffer = _sendBuffer;

        while (!ct.IsCancellationRequested)
        {
            long readFsn;
            int frameLen;

            while (true)
            {
                Task wait;
                lock (_stateLock)
                {
                    if (_cursorFsn < _ring.NextFsn)
                    {
                        readFsn = _cursorFsn;
                        frameLen = _ring.TryReadFrame(readFsn, sendBuffer);
                        if (frameLen < 0)
                        {
                            // Terminal (not transient): an unreadable published FSN is corruption
                            // that replaying cannot recover, so don't reconnect-loop over it.
                            throw new InvalidDataException(
                                $"QWP segment ring: published FSN {readFsn} is no longer readable " +
                                "(corrupt or truncated segment)");
                        }

                        // Watermark must bump atomically with cursor; a fast server ACK racing ahead
                        // of SendBinaryAsync completion would otherwise be dropped by the receive pump.
                        _cursorFsn = readFsn + 1;
                        if (readFsn > _sentFsnHighWatermark)
                        {
                            _sentFsnHighWatermark = readFsn;
                        }
                        break;
                    }

                    wait = _appendSignal.Task;
                }

                await wait.WaitAsync(ct).ConfigureAwait(false);
            }

            await transport.SendBinaryAsync(sendBuffer.AsMemory(0, frameLen), ct).ConfigureAwait(false);
            Interlocked.Increment(ref _totalFramesSent);
        }
    }

    private async Task ReceivePumpAsync(IQwpCursorTransport transport, long fsnAtZero, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var (ackLen, ackBuffer) = await transport
                .ReceiveFrameAsync(_ackBuffer, AckBufferMaxBytes, ct).ConfigureAwait(false);
            // Keep a grown buffer so the next iteration doesn't reallocate on a recurring wide ACK.
            _ackBuffer = ackBuffer;
            var response = QwpResponse.Parse(ackBuffer.AsSpan(0, ackLen));

            if (!response.IsOk && !response.IsDurableAck)
            {
                Interlocked.Increment(ref _totalServerErrors);
                HandleServerRejection(response, fsnAtZero);
                continue;
            }

            DispatchTableEntries(response);

            if (response.IsDurableAck)
            {
                if (_durableAckMode) HandleDurableAck(response, fsnAtZero);
                continue;
            }

            Interlocked.Increment(ref _totalAcks);

            var ackedSeq = response.Sequence;
            if (ackedSeq < 0)
            {
                throw new IngressError(
                    ErrorCode.ServerFlushError,
                    $"server returned negative ack sequence {ackedSeq}");
            }

            if (_durableAckMode)
            {
                HandleOkInDurableMode(response, ackedSeq, fsnAtZero);
                continue;
            }

            lock (_stateLock)
            {
                var highestSentWireSeq = _sentFsnHighWatermark - fsnAtZero;
                if (highestSentWireSeq < 0)
                {
                    continue;
                }

                if (ackedSeq > highestSentWireSeq)
                {
                    ackedSeq = highestSentWireSeq;
                }

                var newAcked = checked(fsnAtZero + ackedSeq + 1);
                if (newAcked > _ackedFsn)
                {
                    _ackedFsn = newAcked;
                    _ring.Acknowledge(_ackedFsn - 1);
                    FireAckSignalLocked();
                }
            }
        }
    }

    private void HandleOkInDurableMode(in QwpResponse response, long ackedSeq, long fsnAtZero)
    {
        var entries = response.TableEntries.Count == 0
            ? Array.Empty<QwpTableEntry>()
            : response.TableEntries.ToArray();

        lock (_stateLock)
        {
            var highestSentWireSeq = _sentFsnHighWatermark - fsnAtZero;
            if (highestSentWireSeq < 0) return;

            // Cap the wire seq instead of dropping the frame: cumulative ACK semantics mean a
            // sequence beyond our highest sent is still a valid commitment of everything we've
            // sent, and the queued per-table watermarks must still drive TrimCoveredPrefix.
            if (ackedSeq > highestSentWireSeq) ackedSeq = highestSentWireSeq;

            _pendingDurable.Enqueue(new PendingDurable(ackedSeq, entries));
            TrimCoveredPrefixLocked(fsnAtZero);
        }
    }

    private void HandleDurableAck(in QwpResponse response, long fsnAtZero)
    {
        lock (_stateLock)
        {
            foreach (var entry in response.TableEntries)
            {
                if (!_durableTableWatermarks.TryGetValue(entry.TableName, out var prev) || entry.SeqTxn > prev)
                {
                    _durableTableWatermarks[entry.TableName] = entry.SeqTxn;
                }
            }
            TrimCoveredPrefixLocked(fsnAtZero);
        }
    }

    // Drain pendingDurable head while head is covered (empty entries, or every (table, seqTxn)
    // ≤ watermark). Caller holds _stateLock.
    private void TrimCoveredPrefixLocked(long fsnAtZero)
    {
        var maxPoppedWireSeq = -1L;
        while (_pendingDurable.Count > 0)
        {
            var head = _pendingDurable.Peek();
            if (!IsCoveredLocked(head)) break;
            _pendingDurable.Dequeue();
            maxPoppedWireSeq = head.WireSeq;
        }

        if (maxPoppedWireSeq < 0) return;

        var newAcked = checked(fsnAtZero + maxPoppedWireSeq + 1);
        if (newAcked > _ackedFsn)
        {
            _ackedFsn = newAcked;
            _ring.Acknowledge(_ackedFsn - 1);
            FireAckSignalLocked();
        }
    }

    private bool IsCoveredLocked(PendingDurable entry)
    {
        // Empty entries are trivially durable — no per-table commit to wait for.
        if (entry.Entries.Length == 0) return true;
        foreach (var t in entry.Entries)
        {
            if (!_durableTableWatermarks.TryGetValue(t.TableName, out var w) || w < t.SeqTxn)
            {
                return false;
            }
        }
        return true;
    }

    private readonly record struct PendingDurable(long WireSeq, QwpTableEntry[] Entries);

    private void HandleServerRejection(in QwpResponse response, long fsnAtZero)
    {
        var category = QwpErrorClassifier.Classify(response.Status);
        var policy = QwpErrorClassifier.ResolvePolicy(category, _policyResolver);

        var wireSeq = response.Sequence;
        if (wireSeq < 0)
        {
            // Symmetry with the OK path's negative-seq rejection: error frames also follow the
            // non-negative-sequence wire contract.
            throw new IngressError(
                ErrorCode.ServerFlushError,
                $"server returned negative error sequence {wireSeq}");
        }
        long fromFsn, toFsn;
        long highestSentWireSeq;
        lock (_stateLock)
        {
            highestSentWireSeq = _sentFsnHighWatermark - fsnAtZero;
        }

        if (highestSentWireSeq < 0)
        {
            // Pre-send reject (server hit us before any frame went out on this connection):
            // the wire seq doesn't map to a real FSN, so skip the watermark advance.
            fromFsn = -1L;
            toFsn = -1L;
        }
        else
        {
            var capped = Math.Min(wireSeq, highestSentWireSeq);
            fromFsn = checked(fsnAtZero + capped);
            toFsn = fromFsn;
        }

        var tableName = response.TableEntries.Count == 1 ? response.TableEntries[0].TableName : null;
        var senderError = new SenderError(
            category,
            policy,
            (byte)response.Status,
            response.Message,
            wireSeq,
            fromFsn,
            toFsn,
            tableName,
            DateTime.UtcNow);

        if (policy == SenderErrorPolicy.Halt)
        {
            throw new HaltCarrier(senderError, new LineSenderServerException(senderError));
        }

        if (fromFsn >= 0L)
        {
            lock (_stateLock)
            {
                if (_durableAckMode)
                {
                    var cappedSeq = fromFsn - fsnAtZero;
                    _pendingDurable.Enqueue(new PendingDurable(cappedSeq, Array.Empty<QwpTableEntry>()));
                    TrimCoveredPrefixLocked(fsnAtZero);
                }
                else
                {
                    var newAcked = checked(fromFsn + 1L);
                    if (newAcked > _ackedFsn)
                    {
                        _ackedFsn = newAcked;
                        _ring.Acknowledge(_ackedFsn - 1);
                        FireAckSignalLocked();
                    }
                }
            }
        }

        _errorDispatcher?.Offer(senderError);
    }

    private void DispatchTableEntries(in QwpResponse response)
    {
        var handler = Volatile.Read(ref _tableEntryHandler);
        if (handler is null || response.TableEntries.Count == 0)
        {
            return;
        }

        var isDurable = response.IsDurableAck;
        for (var i = 0; i < response.TableEntries.Count; i++)
        {
            handler(response.TableEntries[i], isDurable);
        }
    }

    private async Task<bool> BackoffOrGiveUpAsync(Exception lastError, BackoffState state, CancellationToken ct)
    {
        state.OutageStartTickMs ??= Environment.TickCount64;
        var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - state.OutageStartTickMs.Value);
        var next = _reconnectPolicy.NextBackoffOrGiveUp(state.Attempt, elapsed);
        if (next is null)
        {
            EmitConnectionEvent(QuestDB.Senders.SenderConnectionEventKind.ReconnectBudgetExhausted, cause: lastError);
            SetTerminal(lastError);
            return false;
        }

        try
        {
            await Task.Delay(next.Value, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }

        state.Attempt++;
        return true;
    }

    private void SetTerminal(Exception error, SenderError? senderError = null)
    {
        bool isInitialConnect;
        lock (_stateLock)
        {
            if (_terminal)
            {
                return;
            }

            _terminal = true;
            _terminalError = error;
            isInitialConnect = !_seenFirstConnect;
            FireAckSignalLocked();
            FireAppendSignalLocked();
        }
        FireFirstConnectFailed(error);
        if (_errorDispatcher is null) return;
        _errorDispatcher.Offer(senderError ?? BuildEngineError(error, isInitialConnect));
    }

    private static SenderError BuildEngineError(Exception error, bool isInitialConnect) =>
        new(
            category: SenderErrorCategory.Unknown,
            appliedPolicy: SenderErrorPolicy.Halt,
            serverStatusByte: SenderError.NoStatusByte,
            serverMessage: error.Message,
            messageSequence: SenderError.NoMessageSequence,
            fromFsn: -1L,
            toFsn: -1L,
            tableName: null,
            detectedAtUtc: DateTime.UtcNow,
            exception: error,
            isInitialConnect: isInitialConnect);

    private SenderError BuildProtocolViolationError(QwpProtocolViolationException error)
    {
        long fromFsn, toFsn;
        lock (_stateLock)
        {
            // _ackedFsn is the first un-acked FSN; the highest published FSN is NextFsn - 1.
            // A fully-drained slot leaves fromFsn > toFsn — by convention, an empty span.
            fromFsn = _ackedFsn;
            toFsn = _ring.NextFsn - 1L;
        }

        return new SenderError(
            category: SenderErrorCategory.ProtocolViolation,
            appliedPolicy: SenderErrorPolicy.Halt,
            serverStatusByte: SenderError.NoStatusByte,
            serverMessage: error.Message,
            messageSequence: SenderError.NoMessageSequence,
            fromFsn: fromFsn,
            toFsn: toFsn,
            tableName: null,
            detectedAtUtc: DateTime.UtcNow,
            exception: error,
            isInitialConnect: !_seenFirstConnect);
    }

    /// <summary>
    ///     Completes when the engine has either successfully established its first connection or
    ///     reached a terminal state. Used by SF mode <see cref="InitialConnectMode.off" /> and
    ///     <see cref="InitialConnectMode.on" /> to gate the user-facing constructor on first
    ///     connect; <see cref="InitialConnectMode.async" /> simply doesn't await it.
    /// </summary>
    public Task FirstConnectTask => _firstConnectGate.Task;

    private static readonly Action<object?> FireSignalCallback =
        static state => ((TaskCompletionSource<bool>)state!).TrySetResult(true);

    private void FireAppendSignalLocked()
    {
        var prev = _appendSignal;
        _appendSignal = NewSignal();
        _ = Task.Factory.StartNew(FireSignalCallback, prev,
            CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
    }

    private void FireAckSignalLocked()
    {
        var prev = _ackSignal;
        _ackSignal = NewSignal();
        _ = Task.Factory.StartNew(FireSignalCallback, prev,
            CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
    }

    private void FireFirstConnectSucceeded()
    {
        var gate = _firstConnectGate;
        _ = Task.Factory.StartNew(static s => ((TaskCompletionSource<bool>)s!).TrySetResult(true),
            gate, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
    }

    private void EmitConnectionEvent(
        QuestDB.Senders.SenderConnectionEventKind kind,
        Exception? cause,
        (string Host, int Port)? endpoint = null,
        long? attemptNumberOverride = null,
        long? roundNumberOverride = null)
    {
        var sink = _connectionEventSink;
        if (sink is null) return;
        var attempt = attemptNumberOverride ?? Volatile.Read(ref _totalReconnectAttempts);
        var round = roundNumberOverride ?? Volatile.Read(ref _currentRoundSeq);
        var evt = new QuestDB.Senders.SenderConnectionEvent(
            kind,
            host: endpoint?.Host,
            port: endpoint?.Port ?? QuestDB.Senders.SenderConnectionEvent.NoPort,
            attemptNumber: attempt,
            roundNumber: round,
            cause: cause,
            timestamp: DateTimeOffset.UtcNow);
        try { sink(evt); } catch { /* dispatcher absorbs listener exceptions */ }
    }

    private void FireFirstConnectFailed(Exception error)
    {
        var gate = _firstConnectGate;
        var captured = error;
        _ = Task.Factory.StartNew(static s =>
            {
                var (g, e) = ((TaskCompletionSource<bool>, Exception))s!;
                g.TrySetException(e);
            },
            (gate, captured), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
    }

    /// <remarks>
    ///     <see cref="TaskCreationOptions.RunContinuationsAsynchronously" /> means async
    ///     continuations are queued to <see cref="TaskScheduler.Default" /> rather than running
    ///     inline on the completing thread, so callers can safely fire the signal under
    ///     <c>_stateLock</c> without deadlocking awaiters that re-enter the same lock.
    /// </remarks>
    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private void EnsureNotDisposed()
    {
        if (Volatile.Read(ref _disposed))
        {
            throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
        }
    }

    private IngressError WrapTerminalForProducer()
    {
        var inner = _terminalError;
        if (inner is LineSenderServerException lse) return lse;
        var code = inner is IngressError ie ? ie.code : ErrorCode.ServerFlushError;
        var message = inner?.Message ?? "QWP cursor engine has terminally failed";
        return inner is null
            ? new IngressError(code, message)
            : new IngressError(code, "QWP cursor engine has terminally failed; see inner exception", inner);
    }

    // Also consulted by QwpBackgroundDrainerPool to decide whether an orphan-drain fault is
    // deterministic (quarantine the slot) or transient (leave it for re-adoption).
    internal static bool IsTerminalServerError(Exception ex)
    {
        return ex is QwpException
            || ex is QwpProtocolViolationException
            || ex is HaltCarrier
            || ex is InvalidDataException
            || (ex is IngressError ie && ie.code is ErrorCode.AuthError);
    }

    private static Exception? PickTerminalFault(Exception? a, Exception? b)
    {
        if (a is not null && IsTerminalServerError(a)) return a;
        if (b is not null && IsTerminalServerError(b)) return b;
        return null;
    }

    private sealed class HaltCarrier : Exception
    {
        public HaltCarrier(SenderError err, LineSenderServerException wire)
            : base(wire.Message, wire)
        {
            SenderError = err;
            Wire = wire;
        }

        public SenderError SenderError { get; }
        public LineSenderServerException Wire { get; }
    }
}
