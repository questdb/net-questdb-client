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

using System.Net.WebSockets;
using System.Threading.Channels;

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Dedicated IO loop that owns the egress <see cref="IWebSocketChannel"/> and drives
///     receive + decode off the user thread. The .NET counterpart of Java's
///     <c>QwpEgressIoThread</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Experimental. PR 11b ports the foundation: submit a query (pre-encoded frame),
///     send it, drive the receive loop, dispatch RESULT_BATCH / RESULT_END /
///     EXEC_DONE / QUERY_ERROR to the events queue, and gracefully unwind on shutdown
///     or transport failure. CANCEL frames, CREDIT replenish, and CACHE_RESET are
///     deferred to a follow-up sub-PR (the decoder also doesn't support the symbol
///     delta dictionary yet, so CACHE_RESET arriving early would be a server bug).
///     <para/>
///     Concurrency model: <see cref="SubmitQuery"/> / <see cref="TakeEvent"/> /
///     <see cref="ReleaseBuffer"/> / <see cref="ReleaseEvent"/> are user-thread; the
///     run loop runs on a dedicated background <see cref="Task"/>. The pinned scratch
///     copy in <see cref="QwpBatchBuffer.CopyFromPayload"/> means the user's batch
///     view points into the buffer's POH array rather than the WebSocket recv buffer,
///     so there's no need for the Java side's release-latch park.
/// </remarks>
internal sealed class QwpEgressIoThread : IDisposable
{
    private const int DEFAULT_BUFFER_CAPACITY = 64 * 1024;

    private readonly IWebSocketChannel _channel;
    private readonly QwpResultBatchDecoder _decoder = new();
    private readonly QwpSpscQueue<QueryEvent> _eventPool;
    private readonly QwpSpscQueue<QueryEvent> _events;
    private readonly Channel<QwpBatchBuffer> _freeBuffers;
    private readonly Channel<QueryRequest> _requests;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly Action<string>? _terminalFailureListener;
    private byte[] _frameAccumulator = new byte[DEFAULT_BUFFER_CAPACITY];
    private Task? _runTask;
    private volatile bool _closed;

    public QwpEgressIoThread(
        IWebSocketChannel channel,
        int bufferPoolSize = 4,
        Action<string>? terminalFailureListener = null)
    {
        if (bufferPoolSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferPoolSize), "must be >= 1");
        }
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _terminalFailureListener = terminalFailureListener;

        // +2 reserves slots for a trailing RESULT_END plus a synthetic transport-error
        // event the IO thread may emit on shutdown — matches Java's headroom.
        _events = new QwpSpscQueue<QueryEvent>(bufferPoolSize + 2);
        var poolCap = bufferPoolSize + 4;
        _eventPool = new QwpSpscQueue<QueryEvent>(poolCap);
        for (var i = 0; i < poolCap; i++) _eventPool.Offer(new QueryEvent());

        _freeBuffers = Channel.CreateBounded<QwpBatchBuffer>(bufferPoolSize);
        for (var i = 0; i < bufferPoolSize; i++)
        {
            _freeBuffers.Writer.TryWrite(new QwpBatchBuffer(DEFAULT_BUFFER_CAPACITY));
        }
        _requests = Channel.CreateBounded<QueryRequest>(1);
    }

    /// <summary>True once <see cref="Shutdown"/> has been called.</summary>
    public bool IsClosed => _closed;

    /// <summary>Starts the receive loop on a long-running background task.</summary>
    public void Start()
    {
        if (_runTask is not null) throw new InvalidOperationException("already started");
        _runTask = Task.Factory.StartNew(
            RunAsync,
            CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default).Unwrap();
    }

    /// <summary>Submits a fully-encoded QUERY_REQUEST frame for the IO thread to send.</summary>
    public Task SubmitQueryAsync(QueryRequest request, CancellationToken ct = default) =>
        _requests.Writer.WriteAsync(request, ct).AsTask();

    /// <summary>Blocks until the next <see cref="QueryEvent"/> arrives from the IO thread.</summary>
    public QueryEvent TakeEvent(CancellationToken ct = default) => _events.Take(ct);

    /// <summary>Returns a buffer to the pool. Idempotent on shutdown — abandoned buffers self-close.</summary>
    public void ReleaseBuffer(QwpBatchBuffer buffer)
    {
        if (buffer is null) throw new ArgumentNullException(nameof(buffer));
        if (_closed)
        {
            buffer.Close();
            return;
        }
        if (!_freeBuffers.Writer.TryWrite(buffer))
        {
            // Pool full — invariant violation (we never hand out more than poolSize buffers).
            // Close the buffer in place so an invariant break surfaces as a broken buffer
            // rather than a slow native-memory leak.
            buffer.Close();
        }
    }

    /// <summary>Returns a consumed event to the pool for reuse. No-op on null.</summary>
    public void ReleaseEvent(QueryEvent? ev)
    {
        if (ev is null) return;
        ev.Reset();
        _eventPool.Offer(ev);
    }

    /// <summary>Signals shutdown. The run task drains and exits cleanly.</summary>
    public void Shutdown()
    {
        _closed = true;
        _shutdownCts.Cancel();
        _requests.Writer.TryComplete();
    }

    /// <summary>Awaits the run task. Call <see cref="Shutdown"/> first.</summary>
    public Task WaitForCompletionAsync() => _runTask ?? Task.CompletedTask;

    public void Dispose()
    {
        Shutdown();
        try
        {
            _runTask?.GetAwaiter().GetResult();
        }
        catch
        {
            // Disposal swallows; the run task already wrote a transport-error event.
        }
        _shutdownCts.Dispose();
    }

    private QueryEvent BorrowEvent()
    {
        if (_eventPool.TryPoll(out var ev)) return ev;
        // Pool drained — fall back to fresh allocation. The pool is sized above the
        // events-queue capacity so the fallback only fires under accounting drift.
        return new QueryEvent();
    }

    private async Task RunAsync()
    {
        var ct = _shutdownCts.Token;
        var currentQueryDone = true;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                QueryRequest req;
                try
                {
                    req = await _requests.Reader.ReadAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ChannelClosedException)
                {
                    break;
                }

                currentQueryDone = false;
                try
                {
                    await _channel.SendBinaryAsync(req.EncodedFrame, ct).ConfigureAwait(false);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    EmitTransportError($"send failed: {e.Message}");
                    currentQueryDone = true;
                    continue;
                }

                while (!currentQueryDone && !ct.IsCancellationRequested)
                {
                    int frameLen;
                    try
                    {
                        frameLen = await ReadCompleteFrameAsync(ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        EmitTransportError($"recv failed: {e.Message}");
                        currentQueryDone = true;
                        break;
                    }

                    if (frameLen < 0)
                    {
                        EmitTransportError("server closed connection");
                        currentQueryDone = true;
                        break;
                    }

                    currentQueryDone = await DispatchFrameAsync(
                        new ArraySegment<byte>(_frameAccumulator, 0, frameLen), ct).ConfigureAwait(false);
                }
            }
        }
        catch (Exception e)
        {
            EmitTransportError($"IO thread failure: {e.Message}");
        }
        finally
        {
            if (!currentQueryDone)
            {
                EmitTransportError("IO thread shut down with query in flight");
            }
        }
    }

    private async ValueTask<int> ReadCompleteFrameAsync(CancellationToken ct)
    {
        var pos = 0;
        while (true)
        {
            if (pos == _frameAccumulator.Length)
            {
                Array.Resize(ref _frameAccumulator, _frameAccumulator.Length * 2);
            }
            var result = await _channel.ReceiveAsync(_frameAccumulator.AsMemory(pos), ct).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return -1;
            }
            pos += result.Count;
            if (result.EndOfMessage) return pos;
        }
    }

    /// <summary>Returns true when the current query has reached a terminal frame.</summary>
    private async ValueTask<bool> DispatchFrameAsync(ArraySegment<byte> payload, CancellationToken ct)
    {
        if (payload.Count < QwpConstants.HEADER_SIZE + 1)
        {
            EmitTransportError("server sent truncated frame");
            return true;
        }
        var msgKind = payload[QwpConstants.HEADER_SIZE];
        switch (msgKind)
        {
            case QwpEgressMsgKind.RESULT_BATCH:
                return await HandleResultBatchAsync(payload, ct).ConfigureAwait(false);
            case QwpEgressMsgKind.RESULT_END:
                HandleResultEnd(payload);
                return true;
            case QwpEgressMsgKind.EXEC_DONE:
                HandleExecDone(payload);
                return true;
            case QwpEgressMsgKind.QUERY_ERROR:
                HandleQueryError(payload);
                return true;
            case QwpEgressMsgKind.CACHE_RESET:
                // Decoder doesn't need to apply the reset until FLAG_DELTA_SYMBOL_DICT
                // lands; ignore for the PR 11b subset. Frame is consumed silently.
                return false;
            default:
                EmitTransportError($"unknown msg_kind 0x{msgKind:x2}");
                return true;
        }
    }

    private async ValueTask<bool> HandleResultBatchAsync(ArraySegment<byte> payload, CancellationToken ct)
    {
        QwpBatchBuffer buf;
        try
        {
            buf = await _freeBuffers.Reader.ReadAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return true;
        }
        catch (ChannelClosedException)
        {
            return true;
        }

        buf.CopyFromPayload(payload);
        try
        {
            _decoder.Decode(buf);
        }
        catch (QwpDecodeException e)
        {
            ReleaseBuffer(buf);
            // A decode failure leaves the protocol cursor unrecoverable; the next frame
            // would alias bytes the decoder hasn't validated. Tear down the query.
            EmitTransportError($"decode failure: {e.Message}");
            return true;
        }

        _events.Offer(BorrowEvent().AsBatch(buf));
        return false;
    }

    private void HandleResultEnd(ArraySegment<byte> payload)
    {
        // Body: msg_kind(1) + requestId(8) + final_seq(varint) + total_rows(varint)
        var p = QwpConstants.HEADER_SIZE + 1 + 8;
        if (!TrySkipVarint(payload, ref p, out var seqOk) || !seqOk)
        {
            EmitTransportError("RESULT_END frame truncated mid final_seq varint");
            return;
        }
        if (!TryReadVarint(payload, ref p, out var totalRows))
        {
            EmitTransportError("RESULT_END frame truncated mid total_rows varint");
            return;
        }
        _events.Offer(BorrowEvent().AsEnd(totalRows));
    }

    private void HandleExecDone(ArraySegment<byte> payload)
    {
        // Body: msg_kind(1) + requestId(8) + op_type(1) + rows_affected(varint)
        var p = QwpConstants.HEADER_SIZE + 1 + 8;
        if (p + 1 > payload.Count)
        {
            EmitTransportError("EXEC_DONE frame truncated before op_type");
            return;
        }
        var opType = (sbyte)payload[p++];
        if (!TryReadVarint(payload, ref p, out var rowsAffected))
        {
            EmitTransportError("EXEC_DONE frame truncated mid rows_affected varint");
            return;
        }
        _events.Offer(BorrowEvent().AsExecDone(opType, rowsAffected));
    }

    private void HandleQueryError(ArraySegment<byte> payload)
    {
        // Body: msg_kind(1) + requestId(8) + status(1) + msg_len(u16) + msg(utf8)
        var p = QwpConstants.HEADER_SIZE + 1 + 8;
        if (p + 1 + 2 > payload.Count)
        {
            EmitError(QwpConstants.STATUS_INTERNAL_ERROR, "QUERY_ERROR frame truncated before msg_len");
            return;
        }
        var status = payload[p++];
        var msgLen = payload[p] | (payload[p + 1] << 8);
        p += 2;
        if (p + msgLen > payload.Count)
        {
            EmitError(QwpConstants.STATUS_INTERNAL_ERROR,
                $"QUERY_ERROR msg_len {msgLen} exceeds frame remainder {payload.Count - p}");
            return;
        }
        var message = msgLen == 0
            ? string.Empty
            : System.Text.Encoding.UTF8.GetString(payload.Array!, payload.Offset + p, msgLen);
        _events.Offer(BorrowEvent().AsError(status, message));
    }

    private void EmitError(byte status, string message)
    {
        _events.Offer(BorrowEvent().AsError(status, message));
    }

    private void EmitTransportError(string message)
    {
        _terminalFailureListener?.Invoke(message);
        _events.Offer(BorrowEvent().AsTransportError(QwpConstants.STATUS_INTERNAL_ERROR, message));
    }

    /// <summary>
    ///     Reads an LEB128 varint from <paramref name="payload"/> at offset <paramref name="p"/>.
    ///     Returns false on truncation; throws nothing — caller surfaces a transport error.
    /// </summary>
    private static bool TryReadVarint(ArraySegment<byte> payload, ref int p, out long value)
    {
        long v = 0;
        var shift = 0;
        while (p < payload.Count)
        {
            var b = payload[p++];
            if (shift == 63 && (b & 0x7E) != 0) { value = 0; return false; }
            v |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) { value = v; return true; }
            shift += 7;
            if (shift > 63) { value = 0; return false; }
        }
        value = 0;
        return false;
    }

    /// <summary>Skips a varint without retaining its value. <paramref name="terminated"/> reflects whether the varint ended cleanly.</summary>
    private static bool TrySkipVarint(ArraySegment<byte> payload, ref int p, out bool terminated)
    {
        var bytes = 0;
        while (p < payload.Count)
        {
            var b = payload[p++];
            if ((b & 0x80) == 0) { terminated = true; return true; }
            if (++bytes > 9) { terminated = false; return false; }
        }
        terminated = false;
        return true;
    }
}
