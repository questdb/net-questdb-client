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

using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Listener invoked once per queue when the IO pump latches a terminal failure.
///     The argument is the underlying error.
/// </summary>
internal delegate void ConnectionFailureListener(Exception error);

/// <summary>
///     Asynchronous IO pump for QWP WebSocket batches. The .NET counterpart of Java's
///     <c>WebSocketSendQueue</c> on java-questdb-client main 64b7ee69, redesigned around
///     <see cref="Channel{T}"/> + two background <see cref="Task"/>s instead of Java's
///     volatile + <c>synchronized</c> + <c>wait</c>/<c>notify</c> pattern.
/// </summary>
/// <remarks>
///     Experimental. Architecture:
///     <list type="bullet">
///         <item>A capacity-1 <see cref="Channel{T}"/> holds the next sealed
///             <see cref="MicrobatchBuffer"/>. Producer (user thread) writes; the send
///             task reads.</item>
///         <item>Send task: dequeues, calls
///             <see cref="IWebSocketChannel.SendBinaryAsync"/>, transitions buffer to
///             RECYCLED, optionally registers with <see cref="InFlightWindow"/>.</item>
///         <item>Receive task: reads frames, parses via <see cref="WebSocketResponse"/>,
///             routes ACKs to <see cref="InFlightWindow.AcknowledgeUpTo"/>, advances
///             per-table seqTxn watermarks for the originating table.</item>
///         <item>Ping is a binary echo: a <see cref="TaskCompletionSource{TResult}"/>
///             is set when the matching pong arrives.</item>
///     </list>
///     First failure latches; subsequent operations rethrow. The
///     <see cref="ConnectionFailureListener"/> is invoked exactly once.
/// </remarks>
internal sealed class WebSocketSendQueue : IDisposable
{
    public const long DEFAULT_ENQUEUE_TIMEOUT_MS = 30_000;
    public const long DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;
    private const int RECEIVE_BUFFER_SIZE = 16 * 1024;
    private static readonly byte[] PING_PAYLOAD = { 0xFF, 0xFF, 0xFF, 0xFF };

    private readonly IWebSocketChannel _channel;
    private readonly InFlightWindow? _inFlightWindow;
    private readonly ConnectionFailureListener? _failureListener;
    private readonly long _enqueueTimeoutMs;
    private readonly long _shutdownTimeoutMs;

    private readonly Channel<MicrobatchBuffer> _slot;
    private readonly ConcurrentDictionary<string, long> _committedSeqTxns = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _durableSeqTxns = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly ManualResetEventSlim _flushIdle = new(true); // initially idle

    private readonly Task _sendTask;
    private readonly Task _receiveTask;

    private long _totalBatchesSent;
    private long _totalBytesSent;
    private long _totalAcks;
    private long _nextBatchSequence;
    private int _undelivered; // batches enqueued but not yet finished sending
    private TaskCompletionSource<bool>? _pingTcs;
    private Exception? _lastError;
    private int _failureNotified;
    private int _closeCalled;

    public WebSocketSendQueue(
        IWebSocketChannel channel,
        InFlightWindow? inFlightWindow = null,
        ConnectionFailureListener? failureListener = null,
        long enqueueTimeoutMs = DEFAULT_ENQUEUE_TIMEOUT_MS,
        long shutdownTimeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS)
    {
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _inFlightWindow = inFlightWindow;
        _failureListener = failureListener;
        _enqueueTimeoutMs = enqueueTimeoutMs;
        _shutdownTimeoutMs = shutdownTimeoutMs;
        _slot = Channel.CreateBounded<MicrobatchBuffer>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

        _sendTask = Task.Run(SendLoopAsync);
        _receiveTask = Task.Run(ReceiveLoopAsync);
    }

    public bool IsRunning => !_shutdownCts.IsCancellationRequested && Volatile.Read(ref _lastError) is null;

    public Exception? LastError => Volatile.Read(ref _lastError);

    public long TotalBatchesSent => Interlocked.Read(ref _totalBatchesSent);

    public long TotalBytesSent => Interlocked.Read(ref _totalBytesSent);

    public long TotalAcks => Interlocked.Read(ref _totalAcks);

    public long GetCommittedSeqTxn(string tableName) =>
        _committedSeqTxns.TryGetValue(tableName, out var v) ? v : 0;

    public long GetDurableSeqTxn(string tableName) =>
        _durableSeqTxns.TryGetValue(tableName, out var v) ? v : 0;

    /// <summary>
    ///     Enqueues a sealed buffer for sending. Blocks if the slot is occupied, up to
    ///     the configured enqueue timeout. Throws if the queue has latched an error.
    /// </summary>
    public void Enqueue(MicrobatchBuffer buffer)
    {
        if (buffer is null) throw new ArgumentNullException(nameof(buffer));
        if (!buffer.IsSealed)
        {
            throw new IngressError(ErrorCode.ServerFlushError,
                $"buffer must be sealed before enqueue, state={MicrobatchBuffer.StateName(buffer.State)}");
        }
        ThrowIfErrorOrShuttingDown();

        // Increment _undelivered BEFORE the channel write so Flush observes the pending
        // work even if the send loop hasn't yet picked the slot up.
        Interlocked.Increment(ref _undelivered);
        _flushIdle.Reset();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_shutdownCts.Token);
        cts.CancelAfter(TimeSpan.FromMilliseconds(_enqueueTimeoutMs));
        try
        {
            _slot.Writer.WriteAsync(buffer, cts.Token).AsTask().GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
            // Compensate the counter we pre-incremented.
            if (Interlocked.Decrement(ref _undelivered) == 0) _flushIdle.Set();
            ThrowIfErrorOrShuttingDown();
            throw new IngressError(ErrorCode.ServerFlushError,
                $"enqueue timeout after {_enqueueTimeoutMs}ms");
        }
        catch
        {
            if (Interlocked.Decrement(ref _undelivered) == 0) _flushIdle.Set();
            throw;
        }
    }

    /// <summary>
    ///     Blocks until the slot is empty and no batch is mid-send. Throws on a latched error.
    /// </summary>
    public void Flush()
    {
        ThrowIfError();
        var deadline = Environment.TickCount64 + _enqueueTimeoutMs;
        while (Volatile.Read(ref _undelivered) > 0)
        {
            ThrowIfError();
            var remaining = deadline - Environment.TickCount64;
            if (remaining <= 0)
            {
                throw new IngressError(ErrorCode.ServerFlushError,
                    $"flush timeout after {_enqueueTimeoutMs}ms");
            }
            // _flushIdle is set when _undelivered drops to 0; periodic poll guards against
            // a missed signal during a tight enqueue/decrement race.
            _flushIdle.Wait(TimeSpan.FromMilliseconds(Math.Min(remaining, 50)));
        }
        ThrowIfError();
    }

    /// <summary>
    ///     Blocks until <see cref="InFlightWindow.IsEmpty"/> if the queue is bound to one;
    ///     otherwise no-op. Throws on a latched error.
    /// </summary>
    public void AwaitPendingAcks()
    {
        ThrowIfError();
        _inFlightWindow?.AwaitEmpty();
        ThrowIfError();
    }

    /// <summary>
    ///     Sends an application-level ping (a binary frame the server is expected to echo)
    ///     and blocks until the matching pong arrives.
    /// </summary>
    public void Ping(TimeSpan? timeout = null)
    {
        ThrowIfError();
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        // Latch a single in-flight ping at a time. If a previous ping is still pending,
        // wait for it before issuing this one.
        var existing = Interlocked.CompareExchange(ref _pingTcs, tcs, null);
        if (existing is not null)
        {
            try { existing.Task.GetAwaiter().GetResult(); }
            finally { Interlocked.CompareExchange(ref _pingTcs, tcs, null); }
        }

        try
        {
            _channel.SendPingAsync(PING_PAYLOAD, _shutdownCts.Token).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            LatchFailure(new IngressError(ErrorCode.ServerFlushError, "ping send failed", ex));
            tcs.TrySetException(ex);
            Interlocked.CompareExchange(ref _pingTcs, null, tcs);
            ThrowIfError();
        }

        var actualTimeout = timeout ?? TimeSpan.FromMilliseconds(_enqueueTimeoutMs);
        if (!tcs.Task.Wait(actualTimeout))
        {
            Interlocked.CompareExchange(ref _pingTcs, null, tcs);
            throw new IngressError(ErrorCode.ServerFlushError, "ping timeout");
        }
        Interlocked.CompareExchange(ref _pingTcs, null, tcs);
        ThrowIfError();
    }

    public void Close()
    {
        if (Interlocked.CompareExchange(ref _closeCalled, 1, 0) != 0) return;

        // Stop accepting new work — but let the slot drain naturally first.
        try
        {
            // Wait for in-flight to finish, with timeout.
            var deadline = Environment.TickCount64 + _shutdownTimeoutMs;
            while (Environment.TickCount64 < deadline)
            {
                if (Volatile.Read(ref _undelivered) == 0) break;
                _flushIdle.Wait(50);
            }
        }
        catch
        {
            // best-effort drain
        }

        _slot.Writer.TryComplete();
        _shutdownCts.Cancel();
        // Force-disconnect the channel to unwind the receive loop (it would otherwise
        // be stuck on ReceiveAsync waiting for the next frame).
        try { _channel.ForceDisconnect(); } catch { }

        try { Task.WhenAll(_sendTask, _receiveTask).Wait(TimeSpan.FromMilliseconds(_shutdownTimeoutMs)); }
        catch { /* swallow on shutdown */ }
    }

    public void Dispose()
    {
        Close();
        _shutdownCts.Dispose();
        _flushIdle.Dispose();
    }

    private async Task SendLoopAsync()
    {
        try
        {
            await foreach (var batch in _slot.Reader.ReadAllAsync(_shutdownCts.Token).ConfigureAwait(false))
            {
                if (_shutdownCts.IsCancellationRequested) break;

                // Wait for in-flight window space if a window is bound. The producer thread
                // already reserves space before enqueue when the sender wraps this queue
                // for sync mode — async mode tolerates a brief block here.
                _inFlightWindow?.AddInFlight(Interlocked.Increment(ref _nextBatchSequence) - 1);

                try
                {
                    if (batch.IsSealed) batch.MarkSending();
                    var bytes = batch.AsReadOnlyMemory();
                    await _channel.SendBinaryAsync(bytes, _shutdownCts.Token).ConfigureAwait(false);
                    Interlocked.Increment(ref _totalBatchesSent);
                    Interlocked.Add(ref _totalBytesSent, bytes.Length);
                    batch.MarkRecycled();
                }
                catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
                {
                    // graceful shutdown — leave the buffer in its current state for the user
                    if (Interlocked.Decrement(ref _undelivered) == 0) _flushIdle.Set();
                    break;
                }
                catch (Exception ex)
                {
                    LatchFailure(new IngressError(ErrorCode.ServerFlushError, "WebSocket send failed", ex));
                    if (Interlocked.Decrement(ref _undelivered) == 0) _flushIdle.Set();
                    break;
                }

                if (Interlocked.Decrement(ref _undelivered) == 0) _flushIdle.Set();
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        catch (Exception ex) { LatchFailure(new IngressError(ErrorCode.ServerFlushError, "send loop crashed", ex)); }
    }

    private async Task ReceiveLoopAsync()
    {
        var receive = new byte[RECEIVE_BUFFER_SIZE];
        var assembled = new byte[RECEIVE_BUFFER_SIZE];
        var assembledLen = 0;
        var response = new WebSocketResponse();

        while (!_shutdownCts.IsCancellationRequested && _channel.IsConnected)
        {
            WebSocketChannelReceiveResult result;
            try
            {
                result = await _channel.ReceiveAsync(receive, _shutdownCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                LatchFailure(new IngressError(ErrorCode.ServerFlushError, "WebSocket receive failed", ex));
                break;
            }

            if (result.MessageType == WebSocketMessageType.Close) break;

            // Accumulate into the assembly buffer to handle fragmentation. ClientWebSocket
            // delivers frames in chunks; we wait for EndOfMessage before parsing.
            if (assembledLen + result.Count > assembled.Length)
            {
                var bigger = new byte[Math.Max(assembled.Length * 2, assembledLen + result.Count)];
                Array.Copy(assembled, bigger, assembledLen);
                assembled = bigger;
            }
            Array.Copy(receive, 0, assembled, assembledLen, result.Count);
            assembledLen += result.Count;

            if (!result.EndOfMessage) continue;

            var frame = new ReadOnlySpan<byte>(assembled, 0, assembledLen);
            assembledLen = 0;

            // Treat a 4-byte echo of the ping payload as a pong.
            if (frame.Length == PING_PAYLOAD.Length && frame.SequenceEqual(PING_PAYLOAD))
            {
                var tcs = Interlocked.Exchange(ref _pingTcs, null);
                tcs?.TrySetResult(true);
                continue;
            }

            if (!response.ReadFrom(frame))
            {
                LatchFailure(new IngressError(ErrorCode.ServerFlushError,
                    "received malformed WebSocket response frame"));
                break;
            }

            HandleResponse(response);
        }
    }

    private void HandleResponse(WebSocketResponse response)
    {
        if (response.IsSuccess)
        {
            _inFlightWindow?.AcknowledgeUpTo(response.Sequence);
            Interlocked.Increment(ref _totalAcks);
            for (var i = 0; i < response.TableEntryCount; i++)
            {
                UpdateMonotonic(_committedSeqTxns, response.GetTableName(i), response.GetTableSeqTxn(i));
            }
            return;
        }
        if (response.IsDurableAck)
        {
            for (var i = 0; i < response.TableEntryCount; i++)
            {
                UpdateMonotonic(_durableSeqTxns, response.GetTableName(i), response.GetTableSeqTxn(i));
            }
            return;
        }
        // Error frame — latch the error and surface to InFlightWindow.
        var err = new IngressError(ErrorCode.ServerFlushError,
            $"server returned {response.GetStatusName()}: {response.ErrorMessage ?? string.Empty}");
        _inFlightWindow?.Fail(response.Sequence, err);
        LatchFailure(err);
    }

    private static void UpdateMonotonic(ConcurrentDictionary<string, long> dict, string key, long value)
    {
        dict.AddOrUpdate(key, value, (_, existing) => Math.Max(existing, value));
    }

    private void LatchFailure(Exception error)
    {
        // Only the FIRST failure is published — preserves the root cause and avoids
        // listener spam.
        if (Interlocked.CompareExchange(ref _lastError, error, null) is null)
        {
            _inFlightWindow?.FailAll(error);
            if (Interlocked.Exchange(ref _failureNotified, 1) == 0)
            {
                try { _failureListener?.Invoke(error); } catch { /* swallow */ }
            }
        }
        // Wake any pending ping waiter.
        var tcs = Interlocked.Exchange(ref _pingTcs, null);
        tcs?.TrySetException(error);
    }

    private void ThrowIfError()
    {
        var error = Volatile.Read(ref _lastError);
        if (error is not null) throw error;
    }

    private void ThrowIfErrorOrShuttingDown()
    {
        ThrowIfError();
        if (_shutdownCts.IsCancellationRequested)
        {
            throw new IngressError(ErrorCode.ServerFlushError, "send queue is shutting down");
        }
    }
}
