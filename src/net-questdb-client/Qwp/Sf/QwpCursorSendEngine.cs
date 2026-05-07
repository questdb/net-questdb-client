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
    private readonly SenderErrorPolicyResolver? _policyResolver;
    private readonly object _stateLock = new();
    private readonly byte[] _sendBuffer;
    private readonly byte[] _ackBuffer;

    private long _cursorFsn;
    private long _ackedFsn;
    private long _sentFsnHighWatermark;
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
        SenderErrorPolicyResolver? policyResolver = null)
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
        _cursorFsn = ring.OldestFsn;
        _ackedFsn = ring.OldestFsn;
        _segmentManager = new QwpSegmentManager(ring, maxTotalBytes);
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
        string slotDir;
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

    private void ReleaseSharedResources(bool fullyDrained, string slotDir)
    {
        SfCleanup.Dispose(_segmentManager);
        SfCleanup.Dispose(_ring);

        if (fullyDrained)
        {
            UnlinkSegmentFiles(slotDir);
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
            FireFirstConnectFailed(
                new IngressError(ErrorCode.SocketError, "SF engine loop exited before first connect"));
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
                try
                {
                    await transport.ConnectAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (IngressError ex) when (
                    ex.code is ErrorCode.AuthError or ErrorCode.ProtocolVersionError
                    && ex is not QwpIngressRoleRejectedException)
                {
                    SetTerminal(ex);
                    return;
                }
                catch (QwpIngressRoleRejectedException ex)
                {
                    // Role-reject retries indefinitely; don't accumulate elapsed against the give-up budget.
                    backoff.Reset();
                    if (_skipBackoffPredicate?.Invoke() == true)
                    {
                        continue;
                    }

                    if (!_seenFirstConnect && _initialConnectMode == InitialConnectMode.off)
                    {
                        SetTerminal(ex);
                        return;
                    }

                    try
                    {
                        await Task.Delay(_reconnectPolicy.InitialBackoff, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    continue;
                }
                catch (Exception ex)
                {
                    if (_skipBackoffPredicate?.Invoke() == true)
                    {
                        continue;
                    }

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

                _seenFirstConnect = true;
                backoff.Reset();
                FireFirstConnectSucceeded();

                long fsnAtZero;
                lock (_stateLock)
                {
                    // Rewind cursor to first un-acked, clamped to the ring's oldest available FSN
                    // so we never rewind past frames already trimmed off the ring head.
                    _cursorFsn = Math.Max(_ackedFsn, _ring.OldestFsn);
                    fsnAtZero = _cursorFsn;
                    _sentFsnHighWatermark = _cursorFsn - 1;
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
                catch (Exception ex) when (IsTerminalServerError(ex))
                {
                    SetTerminal(ex);
                    return;
                }
                catch (Exception ex)
                {
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

        // Prefer a faulted task over WhenAny's winner: if recv faulted with QwpException and send
        // completed via cancellation, IsTerminalServerError must see the recv exception.
        var fault = sendTask.Exception?.GetBaseException() ?? recvTask.Exception?.GetBaseException();
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
                            throw new IngressError(
                                ErrorCode.ServerFlushError,
                                $"internal: cursor at FSN {readFsn} fell out of segment range");
                        }

                        // Advance the cursor before the await so the receiver's clamp
                        // (`_cursorFsn - fsnAtZero - 1`) reflects the in-flight frame. Failure
                        // tears down the connection and the reconnect path rewinds via
                        // `_cursorFsn = _ackedFsn`, so optimistic advance is safe.
                        _cursorFsn = readFsn + 1;
                        break;
                    }

                    wait = _appendSignal.Task;
                }

                await wait.WaitAsync(ct).ConfigureAwait(false);
            }

            await transport.SendBinaryAsync(sendBuffer.AsMemory(0, frameLen), ct).ConfigureAwait(false);
            lock (_stateLock)
            {
                if (readFsn > _sentFsnHighWatermark)
                {
                    _sentFsnHighWatermark = readFsn;
                }
            }
        }
    }

    private async Task ReceivePumpAsync(IQwpCursorTransport transport, long fsnAtZero, CancellationToken ct)
    {
        var ackBuffer = _ackBuffer;

        while (!ct.IsCancellationRequested)
        {
            var ackLen = await transport.ReceiveFrameAsync(ackBuffer, ct).ConfigureAwait(false);
            var response = QwpResponse.Parse(ackBuffer.AsSpan(0, ackLen));

            if (!response.IsOk && !response.IsDurableAck)
            {
                HandleServerRejection(response, fsnAtZero);
                continue;
            }

            DispatchTableEntries(response);

            if (!response.IsOk)
            {
                continue;
            }

            var ackedSeq = response.Sequence;
            if (ackedSeq < 0)
            {
                throw new IngressError(
                    ErrorCode.ServerFlushError,
                    $"server returned negative ack sequence {ackedSeq}");
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
                    // Manager polls AckedFsn and trims off the I/O critical path. Acknowledge takes
                    // the highest acked FSN, so subtract 1 from our "first un-acked" semantics.
                    _ring.Acknowledge(_ackedFsn - 1);
                    FireAckSignalLocked();
                }
            }
        }
    }

    private void HandleServerRejection(in QwpResponse response, long fsnAtZero)
    {
        var category = QwpErrorClassifier.Classify(response.Status);
        var policy = QwpErrorClassifier.ResolvePolicy(category, _policyResolver);

        var wireSeq = response.Sequence;
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
            var capped = Math.Max(0L, Math.Min(wireSeq, highestSentWireSeq));
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
                var newAcked = checked(fromFsn + 1L);
                if (newAcked > _ackedFsn)
                {
                    _ackedFsn = newAcked;
                    _ring.Acknowledge(_ackedFsn - 1);
                    FireAckSignalLocked();
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

    // QwpException carries a server status code; per spec these are application-layer rejects
    // that replay cannot fix. AuthError / ProtocolVersionError are likewise non-retryable.
    private static bool IsTerminalServerError(Exception ex)
    {
        return ex is QwpException
            || ex is HaltCarrier
            || (ex is IngressError ie && ie.code is ErrorCode.AuthError or ErrorCode.ProtocolVersionError);
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
