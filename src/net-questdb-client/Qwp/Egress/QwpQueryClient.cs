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
using System.Text;

namespace QuestDB.Qwp.Egress;

/// <summary>
///     QWP egress (query results) client. The .NET counterpart of Java's
///     <c>QwpQueryClient</c> on java-questdb-client main 64b7ee69 — wires up
///     <see cref="QwpBindValues"/>, the QUERY_REQUEST frame encoder, and
///     <see cref="QwpEgressIoThread"/>, with optional multi-endpoint failover (§3.4).
/// </summary>
/// <remarks>
///     Experimental. Two construction modes:
///     <list type="bullet">
///         <item>
///             Single-endpoint with a pre-connected channel — used by tests and the
///             pre-failover code path. No reconnect on transport failure.
///         </item>
///         <item>
///             Multi-endpoint with a channel factory and <see cref="QwpFailoverOptions"/>.
///             On a transport failure during <see cref="Execute(string, IQwpColumnBatchHandler)"/>,
///             the client tears down the dead IO thread, walks the endpoint list with
///             exponential backoff, applies the role filter against the new connection's
///             SERVER_INFO frame, fires <see cref="IQwpColumnBatchHandler.OnFailoverReset"/>,
///             and replays the query.
///         </item>
///     </list>
///     Connection-string parsing (<c>addr=</c>, <c>target=</c>, etc.) is deferred —
///     callers build a <see cref="QwpEndpoint"/> list and channel factory directly.
///     <para/>
///     Thread safety: not thread-safe for concurrent queries on the same client.
/// </remarks>
internal sealed class QwpQueryClient : IDisposable
{
    private readonly QwpBindValues _bindValues = new();
    private readonly int _bufferPoolSize;
    private readonly Func<QwpEndpoint, CancellationToken, Task<IWebSocketChannel>>? _channelFactory;
    private readonly IReadOnlyList<QwpEndpoint> _endpoints;
    private readonly QwpFailoverOptions _failoverOptions;
    private readonly QwpPinnedBufferWriter _frameScratch = new();
    private QwpEgressIoThread _ioThread;
    private int _currentEndpointIndex = -1;
    private long _nextRequestId;
    private bool _closed;

    /// <summary>
    ///     Single-endpoint constructor: takes a pre-connected channel. No failover —
    ///     a transport failure surfaces as <see cref="IQwpColumnBatchHandler.OnError"/>.
    /// </summary>
    public QwpQueryClient(IWebSocketChannel channel, int bufferPoolSize = 4)
    {
        if (channel is null) throw new ArgumentNullException(nameof(channel));
        _bufferPoolSize = bufferPoolSize;
        _endpoints = Array.Empty<QwpEndpoint>();
        _channelFactory = null;
        _failoverOptions = QwpFailoverOptions.Disabled;
        _ioThread = new QwpEgressIoThread(channel, bufferPoolSize);
        _ioThread.Start();
    }

    /// <summary>
    ///     §3.4 — multi-endpoint constructor. Connects to the first endpoint that the
    ///     <paramref name="channelFactory"/> succeeds against and whose SERVER_INFO
    ///     role passes <see cref="QwpFailoverOptions.TargetRole"/>. On a transport
    ///     failure inside <see cref="Execute(string, IQwpColumnBatchHandler)"/> the
    ///     client transparently re-walks the endpoint list per the failover knobs.
    /// </summary>
    /// <exception cref="QwpRoleMismatchException">
    ///     Thrown when no endpoint matches the configured role within the first
    ///     <see cref="QwpFailoverOptions.MaxAttempts"/> attempts.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when every endpoint's <paramref name="channelFactory"/> call failed
    ///     within the failover budget.
    /// </exception>
    public QwpQueryClient(
        IReadOnlyList<QwpEndpoint> endpoints,
        Func<QwpEndpoint, CancellationToken, Task<IWebSocketChannel>> channelFactory,
        QwpFailoverOptions failoverOptions,
        int bufferPoolSize = 4)
    {
        if (endpoints is null) throw new ArgumentNullException(nameof(endpoints));
        if (endpoints.Count == 0) throw new ArgumentException("must contain at least one endpoint", nameof(endpoints));
        _bufferPoolSize = bufferPoolSize;
        _endpoints = endpoints;
        _channelFactory = channelFactory ?? throw new ArgumentNullException(nameof(channelFactory));
        _failoverOptions = failoverOptions.MaxAttempts <= 0 ? QwpFailoverOptions.Default : failoverOptions;
        // Connect synchronously to the first endpoint that passes the role filter.
        _ioThread = ConnectAsync(skipExistingIndex: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>Number of QUERY_REQUEST frames the client has submitted since construction.</summary>
    public long QueriesSubmitted => Interlocked.Read(ref _nextRequestId);

    /// <summary>The endpoint the current IO thread is bound to, or <c>null</c> for single-endpoint mode.</summary>
    public QwpEndpoint? CurrentEndpoint =>
        _endpoints.Count > 0 && _currentEndpointIndex >= 0 ? _endpoints[_currentEndpointIndex] : (QwpEndpoint?)null;

    /// <summary>Executes <paramref name="sql"/> with no binds.</summary>
    public void Execute(string sql, IQwpColumnBatchHandler handler) =>
        Execute(sql, binds: null, handler);

    /// <summary>
    ///     Executes <paramref name="sql"/> with optional bind parameters supplied via
    ///     <paramref name="binds"/>. Drives the supplied <paramref name="handler"/>
    ///     synchronously until the query reaches a terminal frame (RESULT_END,
    ///     EXEC_DONE, QUERY_ERROR, or — when failover is exhausted — TRANSPORT_ERROR).
    /// </summary>
    public void Execute(string sql, QwpBindSetter? binds, IQwpColumnBatchHandler handler)
    {
        if (_closed) throw new InvalidOperationException("QwpQueryClient is closed");
        if (sql is null) throw new ArgumentNullException(nameof(sql));
        if (handler is null) throw new ArgumentNullException(nameof(handler));

        // 1. Encode binds on the user thread.
        _bindValues.Reset();
        binds?.Invoke(_bindValues);

        // 2. Build the QUERY_REQUEST frame. Reused across failover replays.
        var requestId = Interlocked.Increment(ref _nextRequestId);
        var frame = BuildQueryFrame(sql, requestId, _bindValues);

        // 3. Submit + drain. On transport error and with failover enabled, reconnect
        //    and replay. Each reconnect counts toward MaxAttempts.
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                _ioThread.SubmitQueryAsync(new QueryRequest(requestId, frame)).GetAwaiter().GetResult();
            }
            catch (Exception e) when (_failoverOptions.Enabled && attempt < _failoverOptions.MaxAttempts && e is not OperationCanceledException)
            {
                if (TryFailover(handler, $"submit failed: {e.Message}", CancellationToken.None)) continue;
                handler.OnError(QwpConstants.STATUS_INTERNAL_ERROR, $"submit failed: {e.Message}");
                return;
            }

            var outcome = DrainEvents(handler);
            switch (outcome.Kind)
            {
                case DrainOutcomeKind.Terminated:
                    return;
                case DrainOutcomeKind.TransportError:
                    if (_failoverOptions.Enabled && attempt < _failoverOptions.MaxAttempts &&
                        TryFailover(handler, outcome.Message, CancellationToken.None))
                    {
                        continue;
                    }
                    handler.OnError(QwpConstants.STATUS_INTERNAL_ERROR, outcome.Message);
                    return;
            }
        }
    }

    public void Dispose()
    {
        if (_closed) return;
        _closed = true;
        try { _ioThread.Dispose(); } catch { /* swallow on close */ }
    }

    /// <summary>
    ///     §3.4 — tears down the current IO thread and walks the endpoint list with
    ///     exponential backoff. Returns true on success and fires
    ///     <see cref="IQwpColumnBatchHandler.OnFailoverReset"/> with the new
    ///     SERVER_INFO. Returns false when all attempts are exhausted.
    /// </summary>
    private bool TryFailover(IQwpColumnBatchHandler handler, string reason, CancellationToken ct)
    {
        if (_endpoints.Count == 0 || _channelFactory is null) return false;
        try { _ioThread.Dispose(); } catch { /* swallow */ }
        try
        {
            _ioThread = ConnectAsync(skipExistingIndex: true, ct).GetAwaiter().GetResult();
        }
        catch
        {
            return false;
        }
        handler.OnFailoverReset(_ioThread.LastServerInfo);
        return true;
    }

    /// <summary>
    ///     §3.4 — opens a new IO thread against the next endpoint that matches the
    ///     role filter. Walks the endpoint list with exponential backoff capped at
    ///     <see cref="QwpFailoverOptions.MaxBackoffMs"/>. Throws if no endpoint
    ///     succeeds within the budget.
    /// </summary>
    private async Task<QwpEgressIoThread> ConnectAsync(bool skipExistingIndex, CancellationToken ct)
    {
        Exception? lastException = null;
        QwpServerInfo? lastObservedServer = null;
        var sawAnyMatchingRole = false;
        var backoff = _failoverOptions.InitialBackoffMs;
        var startIndex = skipExistingIndex
            ? (_currentEndpointIndex + 1) % _endpoints.Count
            : 0;

        for (var attempt = 0; attempt < _failoverOptions.MaxAttempts; attempt++)
        {
            for (var i = 0; i < _endpoints.Count; i++)
            {
                var idx = (startIndex + i) % _endpoints.Count;
                var endpoint = _endpoints[idx];
                IWebSocketChannel? channel = null;
                try
                {
                    channel = await _channelFactory!(endpoint, ct).ConfigureAwait(false);
                    var observed = await ReadInitialServerInfoAsync(channel, ct).ConfigureAwait(false);
                    if (observed is not null)
                    {
                        lastObservedServer = observed;
                    }
                    if (!RoleMatches(observed, _failoverOptions.TargetRole))
                    {
                        await CloseChannelQuietlyAsync(channel, ct).ConfigureAwait(false);
                        continue;
                    }
                    sawAnyMatchingRole = true;
                    var io = new QwpEgressIoThread(channel, _bufferPoolSize);
                    if (observed is not null) io.SeedServerInfo(observed);
                    io.Start();
                    _currentEndpointIndex = idx;
                    return io;
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    lastException = e;
                    if (channel is not null)
                    {
                        await CloseChannelQuietlyAsync(channel, ct).ConfigureAwait(false);
                    }
                }
            }
            if (attempt + 1 < _failoverOptions.MaxAttempts && backoff > 0)
            {
                await Task.Delay(backoff, ct).ConfigureAwait(false);
                backoff = Math.Min(backoff * 2, _failoverOptions.MaxBackoffMs);
            }
        }

        if (lastObservedServer is not null && !sawAnyMatchingRole && _failoverOptions.TargetRole != QwpTargetRole.Any)
        {
            throw new QwpRoleMismatchException(
                _failoverOptions.TargetRole.ToString(),
                lastObservedServer,
                $"no endpoint matched target role {_failoverOptions.TargetRole}");
        }
        throw new InvalidOperationException(
            $"failed to connect to any of {_endpoints.Count} endpoint(s) within {_failoverOptions.MaxAttempts} attempts",
            lastException);
    }

    /// <summary>
    ///     §3.4 — reads the initial SERVER_INFO frame off a freshly opened channel.
    ///     Returns null if the first frame isn't SERVER_INFO (e.g., a v1 server) or
    ///     the channel closes before any frame arrives. The bootstrapped frame is
    ///     consumed off the wire — the IO thread won't see it again, so we
    ///     <see cref="QwpEgressIoThread.SeedServerInfo"/> the parsed value before the
    ///     thread starts.
    /// </summary>
    private static async Task<QwpServerInfo?> ReadInitialServerInfoAsync(IWebSocketChannel channel, CancellationToken ct)
    {
        var buffer = new byte[2048];
        var pos = 0;
        while (true)
        {
            if (pos == buffer.Length) Array.Resize(ref buffer, buffer.Length * 2);
            var result = await channel.ReceiveAsync(buffer.AsMemory(pos), ct).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close) return null;
            pos += result.Count;
            if (result.EndOfMessage) break;
        }
        if (pos < QwpConstants.HEADER_SIZE + 1 ||
            buffer[QwpConstants.HEADER_SIZE] != QwpEgressMsgKind.SERVER_INFO)
        {
            return null;
        }
        return QwpServerInfoDecoder.Decode(buffer.AsSpan(0, pos));
    }

    private static async Task CloseChannelQuietlyAsync(IWebSocketChannel channel, CancellationToken ct)
    {
        try
        {
            await channel.CloseAsync(WebSocketCloseStatus.NormalClosure, null, ct).ConfigureAwait(false);
        }
        catch
        {
            channel.ForceDisconnect();
        }
    }

    private static bool RoleMatches(QwpServerInfo? observed, QwpTargetRole target)
    {
        if (target == QwpTargetRole.Any) return true;
        if (observed is null) return false;
        return target switch
        {
            QwpTargetRole.Primary => observed.Role == QwpEgressMsgKind.ROLE_PRIMARY ||
                                    observed.Role == QwpEgressMsgKind.ROLE_STANDALONE,
            QwpTargetRole.Replica => observed.Role == QwpEgressMsgKind.ROLE_REPLICA,
            _ => true,
        };
    }

    private DrainOutcome DrainEvents(IQwpColumnBatchHandler handler)
    {
        while (true)
        {
            var ev = _ioThread.TakeEvent();
            try
            {
                switch (ev.Kind)
                {
                    case QueryEvent.KIND_BATCH:
                        var buf = ev.Buffer!;
                        try { handler.OnBatch(buf.Batch); }
                        finally { _ioThread.ReleaseBuffer(buf); }
                        break;
                    case QueryEvent.KIND_END:
                        handler.OnEnd(ev.TotalRows);
                        return DrainOutcome.OfTerminated();
                    case QueryEvent.KIND_ERROR:
                        handler.OnError(ev.ErrorStatus, ev.ErrorMessage);
                        return DrainOutcome.OfTerminated();
                    case QueryEvent.KIND_EXEC_DONE:
                        handler.OnExecDone(ev.OpType, ev.RowsAffected);
                        return DrainOutcome.OfTerminated();
                    case QueryEvent.KIND_TRANSPORT_ERROR:
                        return DrainOutcome.OfTransportError(ev.ErrorMessage ?? "transport error");
                    default:
                        throw new InvalidOperationException(
                            $"unexpected QueryEvent.Kind={ev.Kind} from IO thread");
                }
            }
            finally
            {
                _ioThread.ReleaseEvent(ev);
            }
        }
    }

    private ReadOnlyMemory<byte> BuildQueryFrame(string sql, long requestId, QwpBindValues binds)
    {
        // Wire layout: msg_kind(1B) | request_id(8B LE) | sql_len(varint) | sql(UTF-8)
        // | initial_credit(varint, 0=unbounded) | bind_count(varint) | bind_payload(...).
        // No 12-byte QWP1 envelope: the egress send path is asymmetric (responses
        // carry the envelope, queries don't — matches Java's sendQueryRequest).
        _frameScratch.Reset();
        _frameScratch.PutByte(QwpEgressMsgKind.QUERY_REQUEST);
        _frameScratch.PutLong(requestId);
        var sqlBytes = Encoding.UTF8.GetBytes(sql);
        _frameScratch.PutVarint(sqlBytes.Length);
        _frameScratch.PutBlockOfBytes(sqlBytes);
        _frameScratch.PutVarint(0);            // initial_credit
        _frameScratch.PutVarint(binds.Count);
        if (binds.Count > 0)
        {
            _frameScratch.PutBlockOfBytes(binds.BufferSpan.ToArray());
        }
        // Snapshot to a fresh array so the caller can safely retain it past the
        // next Execute (the IO thread copies on send, but cheap to be defensive).
        return _frameScratch.AsReadOnlySpan().ToArray();
    }

    private enum DrainOutcomeKind { Terminated, TransportError }

    private readonly record struct DrainOutcome(DrainOutcomeKind Kind, string Message)
    {
        public static DrainOutcome OfTerminated() => new(DrainOutcomeKind.Terminated, string.Empty);
        public static DrainOutcome OfTransportError(string message) => new(DrainOutcomeKind.TransportError, message);
    }
}
