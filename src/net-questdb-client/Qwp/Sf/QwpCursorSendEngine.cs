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
    private readonly bool _initialConnectRetry;
    private readonly object _stateLock = new();

    private long _cursorFsn;
    private long _ackedFsn;
    private bool _terminal;
    private Exception? _terminalError;
    private bool _disposed;
    private bool _started;

    private TaskCompletionSource<bool> _appendSignal = NewSignal();
    private TaskCompletionSource<bool> _ackSignal = NewSignal();

    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;

    /// <param name="slotLock">
    ///     The slot lock to dispose alongside the engine. Pass <c>null</c> when the caller (e.g.
    ///     <see cref="QwpBackgroundDrainerPool" />) owns the lock externally — the engine will
    ///     drive the wire but leave the lock alone on dispose.
    /// </param>
    /// <param name="ring">Segment ring backing the engine's frames; the engine owns its disposal.</param>
    /// <param name="transportFactory">Factory that produces a fresh transport on each (re)connect.</param>
    /// <param name="reconnectPolicy">Backoff policy applied between transient wire failures.</param>
    /// <param name="appendDeadline">Max time <see cref="AppendBlocking" /> will wait when the disk cap is hit.</param>
    /// <param name="initialConnectRetry">
    ///     If <c>true</c>, the first connect honours the reconnect backoff loop; if <c>false</c>,
    ///     a failed initial connect immediately marks the engine terminal.
    /// </param>
    /// <param name="maxTotalBytes">Disk cap forwarded to the engine's <see cref="QwpSegmentManager" />.</param>
    public QwpCursorSendEngine(
        QwpSlotLock? slotLock,
        QwpSegmentRing ring,
        Func<IQwpCursorTransport> transportFactory,
        QwpReconnectPolicy reconnectPolicy,
        TimeSpan appendDeadline,
        bool initialConnectRetry,
        long maxTotalBytes = long.MaxValue)
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
        _initialConnectRetry = initialConnectRetry;
        _cursorFsn = ring.OldestFsn;
        _ackedFsn = ring.OldestFsn;
        _segmentManager = new QwpSegmentManager(ring, maxTotalBytes);
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

        // Span cannot escape into the wait/loop below — copy first.
        var copy = frame.ToArray();
        var deadline = DateTime.UtcNow + _appendDeadline;

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

                if (_ring.TryAppend(copy))
                {
                    FireAppendSignalLocked();
                    return;
                }

                waitTask = _ackSignal.Task;
            }

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                throw new IngressError(
                    ErrorCode.ServerFlushError,
                    $"sf_append_deadline ({_appendDeadline.TotalMilliseconds:F0} ms) expired with the ring full");
            }

            try
            {
                // Cap per-iteration wait so missed signals (e.g. manager's first heartbeat tick to
                // install the initial spare) don't stall us for the full deadline.
                var slice = Math.Min((int)Math.Min(int.MaxValue, remaining.TotalMilliseconds), 200);
                waitTask.Wait(slice, cancellationToken);
            }
            catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)
            {
                throw ex.InnerException;
            }
        }
    }

    /// <summary>Returns once every appended frame is acked, or throws on timeout / terminal failure.</summary>
    public async Task FlushAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        var deadline = timeout == Timeout.InfiniteTimeSpan
            ? DateTime.MaxValue
            : DateTime.UtcNow + timeout;

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

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                throw new IngressError(
                    ErrorCode.ServerFlushError,
                    $"close_flush_timeout ({timeout.TotalMilliseconds:F0} ms) expired with un-acked frames pending");
            }

            try
            {
                await waitTask.WaitAsync(remaining, cancellationToken).ConfigureAwait(false);
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

            _disposed = true;
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
        // Bounded wait — never hang Dispose on a wedged loop. Loop errors surface via TerminalError.
        if (loop is not null) SfCleanup.Run(() => loop.Wait(TimeSpan.FromSeconds(5)));
        SfCleanup.Dispose(cts);
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
        var backoff = new BackoffState();
        var seenFirstConnect = false;

        while (!ct.IsCancellationRequested)
        {
            IQwpCursorTransport? transport = null;
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
                catch (IngressError ex) when (ex.code == ErrorCode.AuthError)
                {
                    SetTerminal(ex);
                    return;
                }
                catch (Exception ex)
                {
                    if (!seenFirstConnect && !_initialConnectRetry)
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

                seenFirstConnect = true;
                backoff.Reset();

                long fsnAtZero;
                lock (_stateLock)
                {
                    // Rewind cursor to first un-acked: anything past _ackedFsn was in flight on the
                    // dropped connection and may not have actually reached the server.
                    _cursorFsn = _ackedFsn;
                    fsnAtZero = _ackedFsn;
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
        public DateTime? OutageStart;

        public void Reset()
        {
            Attempt = 0;
            OutageStart = null;
        }
    }

    // Pipelined send + receive: two pumps share state under _stateLock. A linked CTS means a fault
    // in either pump cancels the other; the connection then closes and the outer loop reconnects.
    private async Task RunConnectionAsync(IQwpCursorTransport transport, long fsnAtZero, CancellationToken ct)
    {
        using var connCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var sendTask = Task.Run(() => SendPumpAsync(transport, connCts.Token), connCts.Token);
        var recvTask = Task.Run(() => ReceivePumpAsync(transport, fsnAtZero, connCts.Token), connCts.Token);

        Task firstFinished;
        try
        {
            firstFinished = await Task.WhenAny(sendTask, recvTask).ConfigureAwait(false);
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
            // The first-finished branch below surfaces the meaningful exception.
        }

        if (firstFinished.IsFaulted)
        {
            throw firstFinished.Exception!.GetBaseException();
        }

        if (firstFinished.IsCanceled)
        {
            throw new OperationCanceledException(connCts.Token);
        }

        throw new InvalidOperationException("cursor pump returned without error or cancellation");
    }

    private async Task SendPumpAsync(IQwpCursorTransport transport, CancellationToken ct)
    {
        var sendBuffer = new byte[_ring.SegmentCapacity];

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

                        break;
                    }

                    wait = _appendSignal.Task;
                }

                await wait.WaitAsync(ct).ConfigureAwait(false);
            }

            await transport.SendBinaryAsync(sendBuffer.AsMemory(0, frameLen), ct).ConfigureAwait(false);

            // Cursor advances on send completion. The receiver clamps against (cursor - fsnAtZero - 1)
            // when applying server acks so it can never trim past what's truly in flight.
            lock (_stateLock)
            {
                if (_cursorFsn == readFsn)
                {
                    _cursorFsn = readFsn + 1;
                }
            }
        }
    }

    private async Task ReceivePumpAsync(IQwpCursorTransport transport, long fsnAtZero, CancellationToken ct)
    {
        var ackBuffer = new byte[AckBufferSize];

        while (!ct.IsCancellationRequested)
        {
            var ackLen = await transport.ReceiveFrameAsync(ackBuffer, ct).ConfigureAwait(false);
            var response = QwpResponse.Parse(ackBuffer.AsSpan(0, ackLen));

            if (!response.IsOk && !response.IsDurableAck)
            {
                throw response.ToException();
            }

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
                // Clamp against highest wire-seq actually sent on this connection so a malformed
                // server ack can't trim segments past what's truly safe.
                var highestSentWireSeq = _cursorFsn - fsnAtZero - 1;
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

    private async Task<bool> BackoffOrGiveUpAsync(Exception lastError, BackoffState state, CancellationToken ct)
    {
        state.OutageStart ??= DateTime.UtcNow;
        var elapsed = DateTime.UtcNow - state.OutageStart.Value;
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

    private void SetTerminal(Exception error)
    {
        lock (_stateLock)
        {
            if (_terminal)
            {
                return;
            }

            _terminal = true;
            _terminalError = error;
            FireAckSignalLocked();
            FireAppendSignalLocked();
        }
    }

    private void FireAppendSignalLocked()
    {
        var prev = _appendSignal;
        _appendSignal = NewSignal();
        Task.Run(() => prev.TrySetResult(true));
    }

    private void FireAckSignalLocked()
    {
        var prev = _ackSignal;
        _ackSignal = NewSignal();
        Task.Run(() => prev.TrySetResult(true));
    }

    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpCursorSendEngine));
        }
    }

    private IngressError WrapTerminalForProducer()
    {
        var inner = _terminalError ?? new InvalidOperationException("engine terminated");
        var code = inner is IngressError ie ? ie.code : ErrorCode.ServerFlushError;
        return new IngressError(code, "QWP cursor engine has terminally failed; see inner exception", inner);
    }

    // QwpException carries a server status code; per spec these are application-layer rejects
    // that replay cannot fix. AuthError / ProtocolVersionError are likewise non-retryable.
    private static bool IsTerminalServerError(Exception ex)
    {
        return ex is QwpException
            || (ex is IngressError ie && ie.code is ErrorCode.AuthError or ErrorCode.ProtocolVersionError);
    }
}
