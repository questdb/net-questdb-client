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

using System.Buffers.Binary;
using System.Net.WebSockets;
using System.Text;
using QuestDB.Enums;
using QuestDB.Qwp.Sf;
using QuestDB.Senders;
using QuestDB.Utils;

namespace QuestDB.Qwp.Query;

internal sealed class QwpQueryWebSocketClient : IQwpQueryClient
{
    private static readonly UTF8Encoding StrictUtf8 = QwpConstants.StrictUtf8;
    private const int InitialReceiveBufferBytes = 64 * 1024;
    private static readonly TimeSpan ServerInfoReadTimeout = TimeSpan.FromSeconds(5);

    private readonly QueryOptions _options;
    private readonly QwpEgressConnState _connState = new();
    private readonly QwpResultBatchDecoder _decoder;
    private readonly QwpColumnBatch _batch = new();
    private readonly SemaphoreSlim _executeLock = new(1, 1);
    // ClientWebSocket.SendAsync is not safe under concurrent senders; Cancel() is foreign-thread.
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private QwpWebSocketTransport? _transport;
    private readonly QwpHostHealthTracker _hostTracker;
    private int _activeAddressIndex = -1;
    private byte[] _receiveBuffer = new byte[InitialReceiveBufferBytes];
    private byte[] _decompressBuffer = Array.Empty<byte>();
    private byte[] _queryRequestBuf = Array.Empty<byte>();
    private readonly byte[] _cancelFrameBuf = new byte[1 + 8];
    private readonly byte[] _creditFrameBuf = new byte[1 + 8 + QwpVarint.MaxBytes];
    private ZstdSharp.Decompressor? _decompressor;
    private long _nextRequestId;
    private long _currentRequestId = -1;
    private long _pendingCreditBytes;
    private long _expectedBatchSeq;
    private int _disposed;
    private int _terminal;
    private long _cancelTargetRid = -1;
    // Either flag suppresses MarkTerminal in finally: cleanly = wire-side terminator or user-cancelled;
    // drainOk = user callback threw but the connection is still recoverable.
    private bool _executeFinishedCleanly;
    private bool _drainOkAfterHandlerThrow;
    private int _lastCloseTimedOut;

    private QwpQueryWebSocketClient(QueryOptions options)
    {
        _options = options;
        _decoder = new QwpResultBatchDecoder(_connState);
        _hostTracker = new QwpHostHealthTracker(
            options.addresses,
            clientZone: options.zone,
            targetIsPrimary: options.target == TargetType.primary);
    }

    internal static async Task<QwpQueryWebSocketClient> CreateAsync(QueryOptions options, CancellationToken ct)
    {
        var client = new QwpQueryWebSocketClient(options);
        try
        {
            await client.ConnectInitialAsync(ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        return client;
    }

    public QwpServerInfo? ServerInfo { get; private set; }

    public int NegotiatedVersion => _transport?.NegotiatedVersion ?? 0;

    public string? NegotiatedCompression => _transport?.NegotiatedContentEncoding;

    public bool WasLastCloseTimedOut => Volatile.Read(ref _lastCloseTimedOut) != 0;

    public void Execute(string sql, QwpColumnBatchHandler handler) =>
        Task.Run(() => ExecuteCoreAsync(sql, binds: null, handler, CancellationToken.None))
            .GetAwaiter().GetResult();

    public void Execute(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler) =>
        Task.Run(() => ExecuteCoreAsync(sql, binds, handler, CancellationToken.None))
            .GetAwaiter().GetResult();

    public Task ExecuteAsync(string sql, QwpColumnBatchHandler handler, CancellationToken ct = default) =>
        ExecuteCoreAsync(sql, binds: null, handler, ct);

    public Task ExecuteAsync(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler,
        CancellationToken cancellationToken = default) =>
        ExecuteCoreAsync(sql, binds, handler, cancellationToken);

    private async Task ExecuteCoreAsync(
        string sql, QwpBindSetter? binds, QwpColumnBatchHandler handler, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(handler);
        ThrowIfDisposed();
        ThrowIfTerminal();

        var sqlByteCount = StrictUtf8.GetByteCount(sql);
        if (sqlByteCount > QwpConstants.MaxSqlLengthBytes)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"SQL exceeds {QwpConstants.MaxSqlLengthBytes} byte limit (got {sqlByteCount})");
        }

        var bindBlob = ReadOnlyMemory<byte>.Empty;
        var bindCount = 0;
        if (binds is not null)
        {
            var bindValues = new QwpBindValues();
            binds(bindValues);
            bindBlob = bindValues.AsMemory();
            bindCount = bindValues.Count;
        }

        if (!await _executeLock.WaitAsync(0, ct).ConfigureAwait(false))
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "Execute is in flight; one query at a time per client");
        }

        _executeFinishedCleanly = false;
        _drainOkAfterHandlerThrow = false;
        _hostTracker.BeginRound(forgetClassifications: false);
        var requestId = Interlocked.Increment(ref _nextRequestId);
        Interlocked.Exchange(ref _currentRequestId, requestId);
        try
        {
            var attempt = 0;
            var backoffPolicy = new QwpReconnectPolicy(
                _options.failover_backoff_initial_ms,
                _options.failover_backoff_max_ms,
                _options.failover_max_duration_ms > TimeSpan.Zero
                    ? _options.failover_max_duration_ms
                    : _options.failover_backoff_max_ms,
                QwpReconnectPolicy.FullJitter);
            var failoverDeadline = _options.failover_max_duration_ms > TimeSpan.Zero
                ? Environment.TickCount64 + (long)_options.failover_max_duration_ms.TotalMilliseconds
                : long.MaxValue;
            while (true)
            {
                _pendingCreditBytes = 0;
                _expectedBatchSeq = 0;
                // The schema rides only batch_seq == 0; invalidate any schema left over from the
                // prior query so a continuation batch can't bind rows to a stale schema.
                _decoder.ResetQuerySchema();
                try
                {
                    await SendQueryRequestAsync(requestId, sql, sqlByteCount, _options.initial_credit, bindBlob, bindCount, ct)
                        .ConfigureAwait(false);
                    await DriveQueryLoopAsync(handler, ct).ConfigureAwait(false);
                    _executeFinishedCleanly = true;
                    return;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    // ClientWebSocket.ReceiveAsync aborts the socket before throwing on token-cancel,
                    // so SendCancel is not deliverable. The connection is unrecoverable; finally
                    // MarkTerminal()s and the caller must build a fresh client. Use Cancel() for
                    // graceful cancellation that gives the server a CANCEL frame.
                    throw;
                }
                catch (Exception ex) when (
                    _options.failover
                    && attempt + 1 < _options.failover_max_attempts
                    && Environment.TickCount64 < failoverDeadline
                    && IsTransportError(ex)
                    && !ct.IsCancellationRequested
                    && Interlocked.Read(ref _cancelTargetRid) != requestId
                    && Volatile.Read(ref _disposed) == 0)
                {
                    if (_activeAddressIndex >= 0) _hostTracker.RecordMidStreamFailure(_activeAddressIndex);
                    var sleep = backoffPolicy.ComputeBackoff(attempt);
                    var remainingMs = failoverDeadline - Environment.TickCount64;
                    if (remainingMs <= 0) throw;
                    if (sleep.TotalMilliseconds > remainingMs)
                        sleep = TimeSpan.FromMilliseconds(remainingMs);
                    await Task.Delay(sleep, ct).ConfigureAwait(false);
                    if (Interlocked.Read(ref _cancelTargetRid) == requestId)
                    {
                        throw new OperationCanceledException("query cancelled during failover");
                    }
                    attempt++;
                    await ReconnectAsync(attempt, ct).ConfigureAwait(false);
                    if (Interlocked.Read(ref _cancelTargetRid) == requestId)
                    {
                        throw new OperationCanceledException("query cancelled during failover");
                    }
                    // An OnFailoverReset throw is terminal: the query was abandoned by the reconnect,
                    // so leave _drainOkAfterHandlerThrow false and let the finally MarkTerminal().
                    handler.OnFailoverReset(ServerInfo);
                }
            }
        }
        finally
        {
            if (!_executeFinishedCleanly && !_drainOkAfterHandlerThrow) MarkTerminal();
            Interlocked.Exchange(ref _currentRequestId, -1);
            // The decompressor is reused across queries, so reclaim it only on the disposal path:
            // when Dispose() races an in-flight Execute it can't take _executeLock and skips
            // DisposeDecompressor, so this query — its last user — frees the native context as it
            // exits. Done under _executeLock (like the Dispose paths) so disposal stays serialised
            // with the decode loop's use of the decompressor. Idempotent via Interlocked.Exchange.
            if (Volatile.Read(ref _disposed) != 0) DisposeDecompressor();
            _executeLock.Release();
        }
    }

    private static bool IsTransportError(Exception ex)
    {
        // ProtocolVersionError stays retryable (another endpoint may run a compatible version);
        // ProtocolViolation is structural frame corruption — terminal, never retried.
        return ex switch
        {
            IngressError ie => ie.code is ErrorCode.SocketError or ErrorCode.ProtocolVersionError,
            System.Net.WebSockets.WebSocketException => true,
            IOException => true,
            ObjectDisposedException => true,
            _ => false,
        };
    }

    private async Task ReconnectAsync(int attempt, CancellationToken ct)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(QwpQueryWebSocketClient));
        }
        Interlocked.Exchange(ref _transport, null)?.Dispose();
        _hostTracker.BeginRound(forgetClassifications: false);

        var (info, lastError, anyRoleMismatch) = await WalkTrackerAsync(ct).ConfigureAwait(false);
        if (_transport is not null)
        {
            ServerInfo = info;
            _connState.ResetSymbolDict();
            // No schema reset needed here: the per-query schema lives on the decoder and is
            // invalidated by the next Execute() call's ResetQuerySchema().
            return;
        }

        var addrCount = _hostTracker.Count;
        if (!anyRoleMismatch && lastError is not null)
        {
            throw new IngressError(ErrorCode.SocketError,
                $"failover exhausted after {attempt} attempt(s) across {addrCount} endpoint(s): {lastError.Message}",
                lastError);
        }

        throw new QwpRoleMismatchException(_options.target, info,
            lastError is null
                ? $"failover exhausted after {attempt} attempt(s) across {addrCount} endpoint(s): no endpoint matched target={_options.target}"
                : $"failover exhausted after {attempt} attempt(s) across {addrCount} endpoint(s): {lastError.Message}");
    }

    private async Task<(QwpServerInfo? LastInfo, Exception? LastError, bool AnyRoleMismatch)>
        WalkTrackerAsync(CancellationToken ct)
    {
        QwpServerInfo? lastInfo = null;
        Exception? lastError = null;
        var anyRoleMismatch = false;
        var retriedAfterReset = false;
        while (true)
        {
            var idx = _hostTracker.PickNext();
            if (idx < 0)
            {
                if (!retriedAfterReset)
                {
                    _hostTracker.BeginRound(forgetClassifications: true);
                    retriedAfterReset = true;
                    continue;
                }
                return (lastInfo, lastError, anyRoleMismatch);
            }

            var addr = _hostTracker.GetHost(idx);
            QwpWebSocketTransport? candidate = null;
            try
            {
                candidate = BuildTransport(addr);

                QwpServerInfo? info = null;
                using (var upgradeCts = CancellationTokenSource.CreateLinkedTokenSource(ct))
                {
                    upgradeCts.CancelAfter(_options.auth_timeout_ms);
                    try
                    {
                        await candidate.ConnectAsync(upgradeCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (upgradeCts.IsCancellationRequested && !ct.IsCancellationRequested)
                    {
                        throw new IngressError(ErrorCode.SocketError,
                            $"WebSocket upgrade for {addr} exceeded auth_timeout={_options.auth_timeout_ms.TotalMilliseconds}ms");
                    }
                }

                // SERVER_INFO is unconditionally delivered post-upgrade — no v2 gate any more.
                using (var serverInfoCts = CancellationTokenSource.CreateLinkedTokenSource(ct))
                {
                    serverInfoCts.CancelAfter(ServerInfoReadTimeout);
                    try
                    {
                        info = await ReadServerInfoFrameAsync(candidate, serverInfoCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (serverInfoCts.IsCancellationRequested && !ct.IsCancellationRequested)
                    {
                        throw new IngressError(ErrorCode.SocketError,
                            $"SERVER_INFO read for {addr} exceeded {ServerInfoReadTimeout.TotalMilliseconds}ms");
                    }
                }

                // info is non-null here: ReadServerInfoFrameAsync returns non-null or throws.
                if ((info!.Capabilities & QwpConstants.CapZone) != 0)
                {
                    _hostTracker.RecordZone(idx, info.ZoneId);
                }

                lastInfo = info;
                if (RoleMatchesTarget(info.Role, _options.target))
                {
                    if (Volatile.Read(ref _disposed) != 0)
                    {
                        candidate.Dispose();
                        throw new ObjectDisposedException(nameof(QwpQueryWebSocketClient));
                    }
                    Interlocked.Exchange(ref _transport, candidate)?.Dispose();
                    _activeAddressIndex = idx;
                    _hostTracker.RecordSuccess(idx);
                    return (lastInfo, lastError, anyRoleMismatch);
                }

                anyRoleMismatch = true;
                _hostTracker.RecordRoleReject(idx, transient: info.Role == QwpRole.PrimaryCatchup);
                lastError = new IngressError(ErrorCode.ConfigError,
                    $"endpoint {addr} role {info.RoleName} does not match target={_options.target}");
                candidate.Dispose();
                candidate = null;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                candidate?.Dispose();
                throw;
            }
            catch (IngressError ex) when (ex.code is ErrorCode.AuthError)
            {
                candidate?.Dispose();
                throw;
            }
            catch (QwpIngressRoleRejectedException ex)
            {
                anyRoleMismatch = true;
                lastInfo = SynthesiseRoleRejectInfo(ex);
                lastError = ex;
                _hostTracker.RecordZone(idx, ex.Zone);
                _hostTracker.RecordRoleReject(idx, ex.IsTransient);
                candidate?.Dispose();
            }
            catch (Exception ex)
            {
                lastError = ex;
                _hostTracker.RecordTransportError(idx);
                candidate?.Dispose();
            }
        }
    }

    public void Cancel()
    {
        var rid = Interlocked.Read(ref _currentRequestId);
        if (rid < 0) return;
        // Record the exact rid being cancelled. If this thread is pre-empted between the read above
        // and here while query `rid` finishes and the next query starts, the stale rid no longer
        // matches the running requestId, so the failover-path checks ignore it.
        Interlocked.Exchange(ref _cancelTargetRid, rid);
        if (Volatile.Read(ref _disposed) != 0) return;

        try
        {
            // Bounded so a wedged I/O loop can't deadlock a foreign-thread Cancel.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            SendCancelAsync(rid, cts.Token).GetAwaiter().GetResult();
        }
        catch
        {
            // Best-effort cancel; the connection is being torn down regardless.
        }
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        CloseAndDisposeTransport(Interlocked.Exchange(ref _transport, null));
        var locked = _executeLock.Wait(TimeSpan.FromSeconds(5));
        Volatile.Write(ref _lastCloseTimedOut, locked ? 0 : 1);
        CloseAndDisposeTransport(Interlocked.Exchange(ref _transport, null));
        if (locked)
        {
            DisposeDecompressor();
            _executeLock.Release();
            // !locked path leaks _executeLock/_sendLock so a foreign in-flight Execute thread
            // doesn't hit ObjectDisposedException on its finally Release.
            try { _executeLock.Dispose(); } catch { }
            try { _sendLock.Dispose(); } catch { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        await CloseAndDisposeTransportAsync(Interlocked.Exchange(ref _transport, null))
            .ConfigureAwait(false);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var locked = false;
        try
        {
            await _executeLock.WaitAsync(cts.Token).ConfigureAwait(false);
            locked = true;
        }
        catch (OperationCanceledException) { }
        Volatile.Write(ref _lastCloseTimedOut, locked ? 0 : 1);
        await CloseAndDisposeTransportAsync(Interlocked.Exchange(ref _transport, null))
            .ConfigureAwait(false);
        if (locked)
        {
            DisposeDecompressor();
            _executeLock.Release();
            try { _executeLock.Dispose(); } catch { }
            try { _sendLock.Dispose(); } catch { }
        }
    }

    private static void CloseAndDisposeTransport(QwpWebSocketTransport? transport)
    {
        if (transport is null) return;
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            transport.CloseAsync(WebSocketCloseStatus.NormalClosure, null, cts.Token)
                .GetAwaiter().GetResult();
        }
        catch { }
        transport.Dispose();
    }

    private static async Task CloseAndDisposeTransportAsync(QwpWebSocketTransport? transport)
    {
        if (transport is null) return;
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await transport.CloseAsync(WebSocketCloseStatus.NormalClosure, null, cts.Token)
                .ConfigureAwait(false);
        }
        catch { }
        transport.Dispose();
    }

    private void DisposeDecompressor()
    {
        var d = Interlocked.Exchange(ref _decompressor, null);
        try { d?.Dispose(); } catch { }
    }

    private static Uri BuildUri(QueryOptions options, string addr)
    {
        // addr is host[:port] only; '/' '@' or whitespace would smuggle a path or userinfo
        // straight into the composed URI string.
        if (string.IsNullOrEmpty(addr)
            || addr.IndexOfAny(new[] { '/', '@' }) >= 0
            || addr.AsSpan().IndexOfAny(' ', '\t') >= 0
            || addr.IndexOf('\n') >= 0 || addr.IndexOf('\r') >= 0)
        {
            throw new IngressError(ErrorCode.ConfigError,
                $"malformed addr '{addr}': expected host[:port] with no path, userinfo, or whitespace");
        }
        var scheme = options.protocol == ProtocolType.wss ? "wss" : "ws";
        var path = options.path;
        if (!path.StartsWith('/')) path = "/" + path;
        return new Uri($"{scheme}://{addr}{path}");
    }

    private QwpWebSocketTransport BuildTransport(string addr)
    {
        var extras = new Dictionary<string, string>(StringComparer.Ordinal);
        var accept = BuildAcceptEncoding(_options);
        if (accept is not null) extras[QwpConstants.HeaderAcceptEncoding] = accept;
        if (_options.max_batch_rows > 0)
        {
            extras[QwpConstants.HeaderMaxBatchRows] =
                _options.max_batch_rows.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        var transportOpts = new QwpWebSocketTransportOptions
        {
            Uri = BuildUri(_options, addr),
            ClientMaxVersion = QwpConstants.SupportedVersion,
            ClientId = _options.client_id,
            AuthorizationHeader = QwpTlsAuth.BuildAuthHeader(
                _options.username, _options.password, _options.token),
            RemoteCertificateValidationCallback = QwpTlsAuth.BuildCertificateValidator(
                _options.tls_verify, _options.tls_roots, _options.tls_roots_password),
            ExtraRequestHeaders = extras.Count > 0 ? extras : null,
        };
        return new QwpWebSocketTransport(transportOpts);
    }

    private static string? BuildAcceptEncoding(QueryOptions options)
    {
        return options.compression switch
        {
            CompressionType.raw => null,
            CompressionType.zstd => $"zstd;level={options.compression_level},raw",
            CompressionType.auto => $"zstd;level={options.compression_level},raw",
            _ => throw new InvalidOperationException(
                $"unknown CompressionType {options.compression}"),
        };
    }

    private async Task ConnectInitialAsync(CancellationToken ct)
    {
        var (info, lastError, anyRoleMismatch) = await WalkTrackerAsync(ct).ConfigureAwait(false);
        if (_transport is not null)
        {
            ServerInfo = info;
            return;
        }

        if (!anyRoleMismatch && lastError is not null)
        {
            throw new IngressError(ErrorCode.SocketError,
                $"connect failed against every endpoint: {lastError.Message}",
                lastError);
        }

        throw new QwpRoleMismatchException(_options.target, info,
            lastError is null
                ? $"no endpoint matched target={_options.target} (last observed role: {info?.RoleName ?? "<none>"})"
                : $"connect failed against every endpoint: {lastError.Message}");
    }

    internal static QwpServerInfo SynthesiseRoleRejectInfo(QwpIngressRoleRejectedException ex) => new()
    {
        // Role-header strings are case-insensitive per the failover spec; align here with the
        // RoleRejected.IsTransient check rather than the case-sensitive switch.
        Role   = MapRoleName(ex.Role),
        ZoneId = ex.Zone,
    };

    internal static QwpRole MapRoleName(string? role)
    {
        if (string.IsNullOrEmpty(role)) return QwpRole.Undefined;
        if (string.Equals(role, QwpConstants.RoleStandaloneName, StringComparison.OrdinalIgnoreCase))
            return QwpRole.Standalone;
        if (string.Equals(role, QwpConstants.RolePrimaryName, StringComparison.OrdinalIgnoreCase))
            return QwpRole.Primary;
        if (string.Equals(role, QwpConstants.RoleReplicaName, StringComparison.OrdinalIgnoreCase))
            return QwpRole.Replica;
        if (string.Equals(role, QwpConstants.RolePrimaryCatchupName, StringComparison.OrdinalIgnoreCase))
            return QwpRole.PrimaryCatchup;
        return QwpRole.Undefined;
    }

    internal static bool RoleMatchesTarget(QwpRole role, TargetType target)
    {
        // target=any only accepts the spec-defined roles; unknown values (including the Undefined
        // sentinel) must topology-reject so a future or buggy server is rejected loudly rather than
        // masked as "matches anything".
        if (role is not (QwpRole.Standalone or QwpRole.Primary
            or QwpRole.Replica or QwpRole.PrimaryCatchup))
        {
            return false;
        }
        return target switch
        {
            TargetType.any => true,
            TargetType.primary => role is QwpRole.Standalone or QwpRole.Primary
                or QwpRole.PrimaryCatchup,
            TargetType.replica => role == QwpRole.Replica,
            _ => false,
        };
    }

    private async Task<QwpServerInfo> ReadServerInfoFrameAsync(QwpWebSocketTransport transport, CancellationToken ct)
    {
        var recv = await transport
            .ReceiveFrameAsync(_receiveBuffer, QwpConstants.MaxResultBatchWireBytes, ct)
            .ConfigureAwait(false);
        _receiveBuffer = recv.Buffer;
        var (kind, payload, _) = SliceFrame(recv.Buffer, recv.Read);
        if (kind != QwpEgressMsgKind.ServerInfo)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"server must send SERVER_INFO as the first frame, got 0x{(byte)kind:X2}");
        }
        return DecodeServerInfo(payload);
    }

    // Bounds a server that floods stale-request / CACHE_RESET frames so a sync Execute
    // (CancellationToken.None) cannot hang forever; reset on every active-request frame.
    private const int MaxConsecutiveNonProgressFrames = 1024;

    private async Task DriveQueryLoopAsync(QwpColumnBatchHandler handler, CancellationToken ct)
    {
        var activeRid = Volatile.Read(ref _currentRequestId);
        var consecutiveNonProgress = 0;
        while (true)
        {
            var (kind, payload, headerFlags) = await ReadFrameAsync(ct).ConfigureAwait(false);
            switch (kind)
            {
                case QwpEgressMsgKind.ResultBatch:
                    var batchBytes = payload.Length;
                    var decoded = MaybeDecompressResultBatch(payload, headerFlags);
                    try
                    {
                        _decoder.Decode(decoded.Span, headerFlags, _batch);
                    }
                    catch (QwpDecodeException ex)
                    {
                        throw new IngressError(ErrorCode.ProtocolViolation, ex.Message, ex);
                    }
                    var batchRid = _batch.RequestId;
                    if (batchRid != activeRid)
                    {
                        // Return credit for a stale request_id directly so the server's
                        // per-request window for that id is not leaked.
                        if (_options.initial_credit > 0)
                        {
                            await SendCreditAsync(batchRid, batchBytes + QwpConstants.HeaderSize, ct)
                                .ConfigureAwait(false);
                        }
                        ThrowIfNonProgressExceeded(++consecutiveNonProgress, activeRid);
                        continue;
                    }
                    if (_batch.BatchSeq != _expectedBatchSeq)
                    {
                        throw new IngressError(ErrorCode.ProtocolViolation,
                            $"out-of-order RESULT_BATCH for request_id={batchRid}: expected batch_seq={_expectedBatchSeq}, got {_batch.BatchSeq}");
                    }
                    consecutiveNonProgress = 0;
                    _expectedBatchSeq++;
                    try
                    {
                        handler.OnBatch(_batch);
                    }
                    catch
                    {
                        _drainOkAfterHandlerThrow = await CancelAndDrainAsync(batchRid)
                            .ConfigureAwait(false);
                        throw;
                    }
                    if (_options.initial_credit > 0)
                    {
                        _pendingCreditBytes += batchBytes + QwpConstants.HeaderSize;
                        var threshold = Math.Max(1L, _options.initial_credit / 2);
                        if (_pendingCreditBytes >= threshold)
                        {
                            var toReturn = _pendingCreditBytes;
                            _pendingCreditBytes = 0;
                            await SendCreditAsync(batchRid, toReturn, ct).ConfigureAwait(false);
                        }
                    }
                    break;

                case QwpEgressMsgKind.ResultEnd:
                    var (endRid, endTotal) = DecodeResultEnd(payload);
                    if (endRid != activeRid)
                    {
                        ThrowIfNonProgressExceeded(++consecutiveNonProgress, activeRid);
                        continue;
                    }
                    _executeFinishedCleanly = true;
                    handler.OnEnd(endTotal);
                    return;

                case QwpEgressMsgKind.ExecDone:
                    var (execRid, opType, rowsAffected) = DecodeExecDone(payload);
                    if (execRid != activeRid)
                    {
                        ThrowIfNonProgressExceeded(++consecutiveNonProgress, activeRid);
                        continue;
                    }
                    _executeFinishedCleanly = true;
                    handler.OnExecDone((QwpOpType)opType, rowsAffected);
                    return;

                case QwpEgressMsgKind.QueryError:
                    var (errRid, status, message) = DecodeQueryError(payload);
                    if (errRid != activeRid && errRid != QwpConstants.RequestIdWildcard)
                    {
                        ThrowIfNonProgressExceeded(++consecutiveNonProgress, activeRid);
                        continue;
                    }
                    if (errRid == QwpConstants.RequestIdWildcard)
                    {
                        Interlocked.Exchange(ref _transport, null)?.Dispose();
                        MarkTerminal();
                    }
                    else
                    {
                        _executeFinishedCleanly = true;
                    }
                    handler.OnError((QwpStatusCode)status, message);
                    return;

                case QwpEgressMsgKind.CacheReset:
                    DecodeCacheReset(payload);
                    ThrowIfNonProgressExceeded(++consecutiveNonProgress, activeRid);
                    break;

                case QwpEgressMsgKind.ServerInfo:
                    throw new IngressError(ErrorCode.ProtocolViolation,
                        "unexpected SERVER_INFO mid-query");

                default:
                    throw new IngressError(ErrorCode.ProtocolViolation,
                        $"unknown egress frame 0x{(byte)kind:X2}");
            }
        }
    }

    private static void ThrowIfNonProgressExceeded(int consecutiveNonProgress, long activeRid)
    {
        if (consecutiveNonProgress > MaxConsecutiveNonProgressFrames)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"server sent {consecutiveNonProgress} consecutive frames not advancing request_id={activeRid}");
        }
    }

    private async Task<(QwpEgressMsgKind Kind, ReadOnlyMemory<byte> Payload, byte HeaderFlags)>
        ReadFrameAsync(CancellationToken ct)
    {
        var transport = _transport ?? throw new ObjectDisposedException(nameof(QwpQueryWebSocketClient));
        var (read, buffer) = await transport.ReceiveFrameAsync(_receiveBuffer, QwpConstants.MaxResultBatchWireBytes, ct)
            .ConfigureAwait(false);
        _receiveBuffer = buffer;
        return SliceFrame(buffer, read);
    }

    private static (QwpEgressMsgKind Kind, ReadOnlyMemory<byte> Payload, byte HeaderFlags)
        SliceFrame(byte[] buffer, int read)
    {
        if (read < QwpConstants.HeaderSize)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, $"frame shorter than QWP1 header: {read} bytes");
        }

        var hdr = buffer.AsSpan(0, QwpConstants.HeaderSize);
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(hdr.Slice(QwpConstants.OffsetMagic, 4));
        if (magic != QwpConstants.Magic)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, $"bad QWP1 magic 0x{magic:X8}");
        }

        var version = hdr[QwpConstants.OffsetVersion];
        if (version != QwpConstants.SupportedVersion)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, $"unsupported QWP version {version}");
        }

        var flags = hdr[QwpConstants.OffsetFlags];
        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(hdr.Slice(QwpConstants.OffsetTableCount, 2));
        var payloadLenU32 = BinaryPrimitives.ReadUInt32LittleEndian(hdr.Slice(QwpConstants.OffsetPayloadLength, 4));
        if (payloadLenU32 > (uint)QwpConstants.MaxResultBatchWireBytes)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"payload_length {payloadLenU32} exceeds {QwpConstants.MaxResultBatchWireBytes}");
        }
        var payloadLen = (int)payloadLenU32;
        if (QwpConstants.HeaderSize + payloadLen != read)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"payload_length mismatch: header says {payloadLen}, frame is {read - QwpConstants.HeaderSize}");
        }

        if (payloadLen < 1)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "frame payload missing msg_kind byte");
        }

        var payloadMem = buffer.AsMemory(QwpConstants.HeaderSize, payloadLen);
        var kind = (QwpEgressMsgKind)payloadMem.Span[0];
        var expectedTableCount = kind == QwpEgressMsgKind.ResultBatch ? 1 : 0;
        if (tableCount != expectedTableCount)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"table_count={tableCount} for msg_kind 0x{(byte)kind:X2}, expected {expectedTableCount}");
        }
        return (kind, payloadMem, flags);
    }

    private async Task SendQueryRequestAsync(
        long requestId, string sql, int sqlByteCount, long initialCredit,
        ReadOnlyMemory<byte> bindBlob, int bindCount, CancellationToken ct)
    {
        var bindBlobSpan = bindBlob.Span;
        var len = 1 + 8 + QwpVarint.GetByteCount((ulong)sqlByteCount) + sqlByteCount
            + QwpVarint.GetByteCount((ulong)initialCredit)
            + QwpVarint.GetByteCount((ulong)bindCount) + bindBlobSpan.Length;

        if (_queryRequestBuf.Length < len)
        {
            _queryRequestBuf = new byte[Math.Max(len, Math.Max(256, _queryRequestBuf.Length * 2))];
        }
        var frame = _queryRequestBuf;
        var p = 0;
        frame[p++] = QwpConstants.MsgKindQueryRequest;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(p, 8), requestId);
        p += 8;
        p += QwpVarint.Write(frame.AsSpan(p), (ulong)sqlByteCount);
        StrictUtf8.GetBytes(sql, frame.AsSpan(p, sqlByteCount));
        p += sqlByteCount;
        p += QwpVarint.Write(frame.AsSpan(p), (ulong)initialCredit);
        p += QwpVarint.Write(frame.AsSpan(p), (ulong)bindCount);
        if (bindBlobSpan.Length > 0)
        {
            bindBlobSpan.CopyTo(frame.AsSpan(p));
        }

        await SendFrameAsync(frame.AsMemory(0, len), ct).ConfigureAwait(false);
    }

    private async Task SendFrameAsync(ReadOnlyMemory<byte> frame, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var transport = _transport ?? throw new ObjectDisposedException(nameof(QwpQueryWebSocketClient));
            await transport.SendBinaryAsync(frame, ct).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private ReadOnlyMemory<byte> MaybeDecompressResultBatch(ReadOnlyMemory<byte> payload, byte headerFlags)
    {
        if ((headerFlags & QwpConstants.FlagZstd) == 0) return payload;

        var span = payload.Span;
        if (span.Length < 1 + 8 + 1)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "compressed RESULT_BATCH missing prelude");
        }

        int seqBytes;
        try
        {
            QwpVarint.Read(span.Slice(9), out seqBytes);
        }
        catch (Exception ex)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                "compressed RESULT_BATCH: malformed batch_seq varint", ex);
        }
        var preludeLen = 1 + 8 + seqBytes;
        if (preludeLen > span.Length)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "compressed RESULT_BATCH prelude truncated");
        }

        var compressed = span.Slice(preludeLen);
        if (compressed.IsEmpty)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "zstd RESULT_BATCH has empty compressed body");
        }
        var declaredSize = ZstdSharp.Decompressor.GetDecompressedSize(compressed);
        const ulong ContentSizeError = unchecked((ulong)-2L);
        const ulong ContentSizeUnknown = unchecked((ulong)-1L);
        if (declaredSize == ContentSizeError)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "zstd frame: malformed content-size");
        }

        int attemptSize;
        if (declaredSize == ContentSizeUnknown)
        {
            // No declared size: decompress once into a worst-case destination. Oversized output
            // overruns it and is rejected — no message-string-driven realloc retry loop.
            attemptSize = QwpConstants.MaxResultBatchWireBytes;
        }
        else if (declaredSize > (ulong)QwpConstants.MaxResultBatchWireBytes)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"zstd frame reports decompressed size {declaredSize}, exceeds {QwpConstants.MaxResultBatchWireBytes}");
        }
        else
        {
            attemptSize = (int)declaredSize;
        }

        var needed = preludeLen + attemptSize;
        if (_decompressBuffer.Length < needed)
        {
            _decompressBuffer = new byte[needed];
        }
        span.Slice(0, preludeLen).CopyTo(_decompressBuffer);

        var decompressor = _decompressor ??= new ZstdSharp.Decompressor();
        int written;
        try
        {
            written = decompressor.Unwrap(compressed, _decompressBuffer.AsSpan(preludeLen, attemptSize));
        }
        catch (Exception ex)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"zstd RESULT_BATCH decompression failed (attemptSize={attemptSize}): {ex.Message}", ex);
        }

        return _decompressBuffer.AsMemory(0, preludeLen + written);
    }

    private async Task SendCreditAsync(long requestId, long additionalBytes, CancellationToken ct)
    {
        _creditFrameBuf[0] = QwpConstants.MsgKindCredit;
        BinaryPrimitives.WriteInt64LittleEndian(_creditFrameBuf.AsSpan(1, 8), requestId);
        var varintLen = QwpVarint.Write(_creditFrameBuf.AsSpan(9), (ulong)additionalBytes);
        var len = 1 + 8 + varintLen;

        if (_transport is null) return;
        await SendFrameAsync(_creditFrameBuf.AsMemory(0, len), ct).ConfigureAwait(false);
    }

    private async Task<bool> CancelAndDrainAsync(long requestId)
    {
        try { await SendCancelAsync(requestId, CancellationToken.None).ConfigureAwait(false); }
        catch { return false; }

        const int maxDrainFrames = 1024;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        for (var i = 0; i < maxDrainFrames; i++)
        {
            QwpEgressMsgKind kind;
            ReadOnlyMemory<byte> payload;
            byte headerFlags;
            try
            {
                (kind, payload, headerFlags) = await ReadFrameAsync(cts.Token).ConfigureAwait(false);
            }
            catch
            {
                return false;
            }

            if (kind is QwpEgressMsgKind.ResultEnd
                or QwpEgressMsgKind.QueryError
                or QwpEgressMsgKind.ExecDone)
            {
                return true;
            }

            if (kind == QwpEgressMsgKind.CacheReset)
            {
                DecodeCacheReset(payload);
                continue;
            }

            if (kind == QwpEgressMsgKind.ResultBatch)
            {
                // Fully decode so dict/schema cursors stay in sync with the server; skip OnBatch.
                try
                {
                    var decoded = MaybeDecompressResultBatch(payload, headerFlags);
                    _decoder.Decode(decoded.Span, headerFlags, _batch);
                }
                catch
                {
                    return false;
                }
                continue;
            }

            return false;
        }
        return false;
    }

    private async Task SendCancelAsync(long requestId, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Interlocked.Read(ref _currentRequestId) != requestId) return;
            var transport = _transport;
            if (transport is null) return;
            _cancelFrameBuf[0] = QwpConstants.MsgKindCancel;
            BinaryPrimitives.WriteInt64LittleEndian(_cancelFrameBuf.AsSpan(1, 8), requestId);
            await transport.SendBinaryAsync(_cancelFrameBuf, ct).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private static QwpServerInfo DecodeServerInfo(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 1 + 8 + 4 + 8 + 2)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"SERVER_INFO too short: {s.Length} bytes");
        }

        if (s[0] != QwpConstants.MsgKindServerInfo)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"SERVER_INFO has wrong msg_kind 0x{s[0]:X2}");
        }

        var role = s[1];
        var epoch = BinaryPrimitives.ReadUInt64LittleEndian(s.Slice(2, 8));
        var capabilities = BinaryPrimitives.ReadUInt32LittleEndian(s.Slice(10, 4));
        var serverWallNs = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(14, 8));

        var clusterIdLen = BinaryPrimitives.ReadUInt16LittleEndian(s.Slice(22, 2));
        if (s.Length < 24 + clusterIdLen + 2)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "SERVER_INFO truncated at cluster_id");
        }
        var clusterId = StrictUtf8.GetString(s.Slice(24, clusterIdLen));
        var nodeIdLenOffset = 24 + clusterIdLen;
        var nodeIdLen = BinaryPrimitives.ReadUInt16LittleEndian(s.Slice(nodeIdLenOffset, 2));
        var nodeIdStart = nodeIdLenOffset + 2;
        if (s.Length < nodeIdStart + nodeIdLen)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "SERVER_INFO truncated at node_id");
        }
        var nodeId = StrictUtf8.GetString(s.Slice(nodeIdStart, nodeIdLen));
        var consumed = nodeIdStart + nodeIdLen;

        string? zoneId = null;
        if ((capabilities & QwpConstants.CapZone) != 0)
        {
            if (s.Length < consumed + 2)
            {
                throw new IngressError(ErrorCode.ProtocolViolation,
                    "SERVER_INFO truncated at zone_id_len (CAP_ZONE set)");
            }
            var zoneIdLen = BinaryPrimitives.ReadUInt16LittleEndian(s.Slice(consumed, 2));
            consumed += 2;
            if (s.Length < consumed + zoneIdLen)
            {
                throw new IngressError(ErrorCode.ProtocolViolation, "SERVER_INFO truncated at zone_id");
            }
            zoneId = StrictUtf8.GetString(s.Slice(consumed, zoneIdLen));
            consumed += zoneIdLen;
        }

        if (s.Length < consumed)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"SERVER_INFO truncated: consumed {consumed}, payload {s.Length}");
        }

        return new QwpServerInfo
        {
            Role = (QwpRole)role,
            Epoch = epoch,
            Capabilities = capabilities,
            ServerWallNs = serverWallNs,
            ClusterId = clusterId,
            NodeId = nodeId,
            ZoneId = zoneId,
        };
    }

    private static (long RequestId, long TotalRows) DecodeResultEnd(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 8) throw new IngressError(ErrorCode.ProtocolViolation, "RESULT_END too short");
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(1, 8));
        var p = 9;
        QwpVarint.Read(s.Slice(p), out var consumed1); // final_seq is informational; not surfaced.
        p += consumed1;
        var totalRows = (long)QwpVarint.Read(s.Slice(p), out var consumed2);
        p += consumed2;
        if (p != s.Length)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"RESULT_END trailing bytes: consumed {p}, payload {s.Length}");
        }
        return (requestId, totalRows);
    }

    private static (long RequestId, byte OpType, long RowsAffected) DecodeExecDone(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 8 + 1 + 1) throw new IngressError(ErrorCode.ProtocolViolation, "EXEC_DONE too short");
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(1, 8));
        byte opType = s[9];
        var rowsAffectedRaw = QwpVarint.Read(s.Slice(10), out var consumed);
        if (rowsAffectedRaw > long.MaxValue)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"EXEC_DONE rows_affected {rowsAffectedRaw} exceeds Int64 range");
        }
        var rowsAffected = (long)rowsAffectedRaw;
        var p = 10 + consumed;
        if (p != s.Length)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"EXEC_DONE trailing bytes: consumed {p}, payload {s.Length}");
        }
        return (requestId, opType, rowsAffected);
    }

    private static (long RequestId, byte Status, string Message) DecodeQueryError(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 8 + 1 + 2)
        {
            throw new IngressError(ErrorCode.ProtocolViolation, "QUERY_ERROR too short");
        }
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(1, 8));
        var status = s[9];
        var msgLen = BinaryPrimitives.ReadUInt16LittleEndian(s.Slice(10, 2));
        if (s.Length != 12 + msgLen)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"QUERY_ERROR length mismatch: msgLen={msgLen} payload={s.Length}");
        }
        var msg = StrictUtf8.GetString(s.Slice(12, msgLen));
        return (requestId, status, msg);
    }

    private void DecodeCacheReset(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length != 2)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"CACHE_RESET length mismatch: payload={s.Length}, expected 2");
        }
        if (s[0] != QwpConstants.MsgKindCacheReset)
        {
            throw new IngressError(ErrorCode.ProtocolViolation,
                $"CACHE_RESET has wrong msg_kind 0x{s[0]:X2}");
        }
        var mask = s[1];
        // Only bit 0 (SYMBOL dict) is defined; other bits are reserved and silently ignored.
        if ((mask & QwpConstants.ResetMaskDict) != 0) _connState.ResetSymbolDict();
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(QwpQueryWebSocketClient));
        }
    }

    private void ThrowIfTerminal()
    {
        if (Volatile.Read(ref _terminal) != 0)
        {
            throw new IngressError(ErrorCode.SocketError,
                "client is in a terminal state after a prior failure; create a new QueryClient");
        }
    }

    private void MarkTerminal() => Volatile.Write(ref _terminal, 1);
}

#endif
