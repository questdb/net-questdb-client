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
using System.Text;
using QuestDB.Enums;
using QuestDB.Senders;
using QuestDB.Utils;

namespace QuestDB.Qwp.Query;

internal sealed class QwpQueryWebSocketClient : IQwpQueryClient
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);
    private const int InitialReceiveBufferBytes = 64 * 1024;

    private readonly QueryOptions _options;
    private readonly QwpEgressConnState _connState = new();
    private readonly QwpResultBatchDecoder _decoder;
    private readonly QwpColumnBatch _batch = new();
    private readonly SemaphoreSlim _executeLock = new(1, 1);
    // ClientWebSocket.SendAsync is not safe under concurrent senders; Cancel() is foreign-thread.
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private QwpWebSocketTransport? _transport;
    private int _activeAddressIndex;
    private byte[] _receiveBuffer = new byte[InitialReceiveBufferBytes];
    private byte[] _decompressBuffer = Array.Empty<byte>();
    private byte[] _queryRequestBuf = Array.Empty<byte>();
    private readonly byte[] _cancelFrameBuf = new byte[1 + 8];
    private readonly byte[] _creditFrameBuf = new byte[1 + 8 + QwpVarint.MaxBytes];
    private ZstdSharp.Decompressor? _decompressor;
    private long _nextRequestId;
    private long _currentRequestId = -1;
    private int _disposed;
    private int _terminal;
    private bool _drainOkAfterHandlerThrow;
    private bool _lastCloseTimedOut;

    private QwpQueryWebSocketClient(QueryOptions options)
    {
        _options = options;
        _decoder = new QwpResultBatchDecoder(_connState);
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
            // Connect failed; release the half-built transport before rethrowing.
            client._transport?.Dispose();
            throw;
        }
        return client;
    }

    public QwpServerInfo? ServerInfo { get; private set; }

    public int NegotiatedVersion => _transport?.NegotiatedVersion ?? 0;

    public string? NegotiatedCompression => _transport?.NegotiatedContentEncoding;

    public bool WasLastCloseTimedOut => _lastCloseTimedOut;

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

        var sqlBytes = StrictUtf8.GetByteCount(sql);
        if (sqlBytes > QwpConstants.MaxSqlLengthBytes)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"SQL exceeds {QwpConstants.MaxSqlLengthBytes} byte limit (got {sqlBytes})");
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

        var graceful = false;
        _drainOkAfterHandlerThrow = false;
        try
        {
            var attempt = 0;
            var backoffMs = _options.failover_backoff_initial_ms.TotalMilliseconds;
            while (true)
            {
                var requestId = Interlocked.Increment(ref _nextRequestId);
                Interlocked.Exchange(ref _currentRequestId, requestId);
                try
                {
                    await SendQueryRequestAsync(requestId, sql, _options.initial_credit, bindBlob, bindCount, ct)
                        .ConfigureAwait(false);
                    await DriveQueryLoopAsync(handler, ct).ConfigureAwait(false);
                    graceful = true;
                    return;
                }
                catch (Exception ex) when (
                    _options.failover
                    && attempt + 1 < _options.failover_max_attempts
                    && IsTransportError(ex)
                    && !ct.IsCancellationRequested)
                {
                    Interlocked.Exchange(ref _currentRequestId, -1);
                    await Task.Delay(TimeSpan.FromMilliseconds(backoffMs), ct).ConfigureAwait(false);
                    backoffMs = Math.Min(backoffMs * 2, _options.failover_backoff_max_ms.TotalMilliseconds);
                    attempt++;
                    await ReconnectAsync(attempt, ct).ConfigureAwait(false);
                    handler.OnFailoverReset(ServerInfo);
                }
            }
        }
        finally
        {
            if (!graceful && !_drainOkAfterHandlerThrow) MarkTerminal();
            Interlocked.Exchange(ref _currentRequestId, -1);
            _executeLock.Release();
        }
    }

    private static bool IsTransportError(Exception ex)
    {
        // ProtocolVersionError is intentionally excluded — frame-decode corruption is permanent;
        // failing over to the next endpoint masks server bugs and burns retry budget.
        return ex switch
        {
            IngressError ie => ie.code == ErrorCode.SocketError,
            System.Net.WebSockets.WebSocketException => true,
            IOException => true,
            ObjectDisposedException => true,
            _ => false,
        };
    }

    private async Task ReconnectAsync(int attempt, CancellationToken ct)
    {
        _transport?.Dispose();
        _transport = null;

        var totalAddresses = _options.AddressCount;
        QwpServerInfo? lastInfo = null;
        Exception? lastTransportError = null;
        var anyRoleMismatch = false;
        for (var step = 1; step <= totalAddresses; step++)
        {
            var idx = (_activeAddressIndex + step) % totalAddresses;
            var addr = _options.addresses[idx];
            QwpWebSocketTransport? candidate = null;
            try
            {
                candidate = BuildTransport(addr);
                await candidate.ConnectAsync(ct).ConfigureAwait(false);

                QwpServerInfo? info = null;
                if (candidate.NegotiatedVersion >= 2)
                {
                    info = await ReadServerInfoFrameAsync(candidate, ct).ConfigureAwait(false);
                }

                lastInfo = info;
                if (EndpointMatchesTarget(info))
                {
                    _transport = candidate;
                    _activeAddressIndex = idx;
                    ServerInfo = info;
                    _connState.ResetSymbolDict();
                    _connState.ResetSchemas();
                    return;
                }

                anyRoleMismatch = true;
                candidate.Dispose();
                candidate = null;
            }
            catch (IngressError ex) when (ex.code is ErrorCode.ConfigError or ErrorCode.AuthError)
            {
                candidate?.Dispose();
                throw;
            }
            catch (Exception ex)
            {
                lastTransportError = ex;
                candidate?.Dispose();
            }
        }

        if (!anyRoleMismatch && lastTransportError is not null)
        {
            throw new IngressError(ErrorCode.SocketError,
                $"failover exhausted after {attempt} attempt(s) across {totalAddresses} endpoint(s): {lastTransportError.Message}",
                lastTransportError);
        }

        throw new QwpRoleMismatchException(_options.target, lastInfo,
            lastTransportError is null
                ? $"failover exhausted after {attempt} attempt(s) across {totalAddresses} endpoint(s): no endpoint matched target={_options.target}"
                : $"failover exhausted after {attempt} attempt(s) across {totalAddresses} endpoint(s): {lastTransportError.Message}");
    }

    public void Cancel()
    {
        var rid = Interlocked.Read(ref _currentRequestId);
        if (rid < 0) return;
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
        _transport?.Dispose();
        var locked = _executeLock.Wait(TimeSpan.FromSeconds(5));
        _lastCloseTimedOut = !locked;
        DisposeDecompressor();
        if (locked) _executeLock.Release();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        _transport?.Dispose();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var locked = false;
        try
        {
            await _executeLock.WaitAsync(cts.Token).ConfigureAwait(false);
            locked = true;
        }
        catch (OperationCanceledException) { }
        _lastCloseTimedOut = !locked;
        DisposeDecompressor();
        if (locked) _executeLock.Release();
    }

    private void DisposeDecompressor()
    {
        var d = Interlocked.Exchange(ref _decompressor, null);
        try { d?.Dispose(); } catch { }
    }

    private static Uri BuildUri(QueryOptions options, string addr)
    {
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
            ClientMaxVersion = QwpConstants.SupportedEgressVersion,
            ClientId = _options.client_id,
            AuthorizationHeader = QwpTlsAuth.BuildAuthHeader(
                _options.username, _options.password, _options.token, _options.auth),
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
            _ => null,
        };
    }

    private async Task ConnectInitialAsync(CancellationToken ct)
    {
        QwpServerInfo? lastInfo = null;
        Exception? lastTransportError = null;
        var anyRoleMismatch = false;
        for (var i = 0; i < _options.AddressCount; i++)
        {
            var addr = _options.addresses[i];
            QwpWebSocketTransport? candidate = null;
            try
            {
                candidate = BuildTransport(addr);
                await candidate.ConnectAsync(ct).ConfigureAwait(false);

                QwpServerInfo? info = null;
                if (candidate.NegotiatedVersion >= 2)
                {
                    info = await ReadServerInfoFrameAsync(candidate, ct).ConfigureAwait(false);
                }

                lastInfo = info;
                if (EndpointMatchesTarget(info))
                {
                    _transport = candidate;
                    _activeAddressIndex = i;
                    ServerInfo = info;
                    return;
                }

                anyRoleMismatch = true;
                candidate.Dispose();
                candidate = null;
            }
            catch (IngressError ex) when (ex.code is ErrorCode.ConfigError or ErrorCode.AuthError)
            {
                candidate?.Dispose();
                throw;
            }
            catch (Exception ex)
            {
                lastTransportError = ex;
                candidate?.Dispose();
            }
        }

        if (!anyRoleMismatch && lastTransportError is not null)
        {
            throw new IngressError(ErrorCode.SocketError,
                $"connect failed against every endpoint: {lastTransportError.Message}",
                lastTransportError);
        }

        throw new QwpRoleMismatchException(_options.target, lastInfo,
            lastTransportError is null
                ? $"no endpoint matched target={_options.target} (last observed role: {lastInfo?.RoleName ?? "<none>"})"
                : $"connect failed against every endpoint: {lastTransportError.Message}");
    }

    // v1 server (no SERVER_INFO) only matches target=any; primary/replica must skip it.
    private bool EndpointMatchesTarget(QwpServerInfo? info)
    {
        if (info is not null) return RoleMatchesTarget(info.Role, _options.target);
        return _options.target == TargetType.any;
    }

    internal static bool RoleMatchesTarget(byte role, TargetType target)
    {
        return target switch
        {
            TargetType.any => true,
            TargetType.primary => role is QwpConstants.RoleStandalone or QwpConstants.RolePrimary
                or QwpConstants.RolePrimaryCatchup,
            TargetType.replica => role == QwpConstants.RoleReplica,
            _ => false,
        };
    }

    private async Task<QwpServerInfo> ReadServerInfoFrameAsync(QwpWebSocketTransport transport, CancellationToken ct)
    {
        var (read, buffer) = await transport
            .ReceiveFrameAsync(_receiveBuffer, QwpConstants.MaxResultBatchWireBytes, ct)
            .ConfigureAwait(false);
        _receiveBuffer = buffer;
        var (kind, payload, _) = SliceFrame(buffer, read);
        if (kind != QwpEgressMsgKind.ServerInfo)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"v2 server must send SERVER_INFO as the first frame, got 0x{(byte)kind:X2}");
        }
        return DecodeServerInfo(payload);
    }

    private async Task DriveQueryLoopAsync(QwpColumnBatchHandler handler, CancellationToken ct)
    {
        var activeRid = Volatile.Read(ref _currentRequestId);
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
                        throw new IngressError(ErrorCode.SocketError, ex.Message, ex);
                    }
                    if (_batch.RequestId != activeRid) continue;
                    var requestIdAtBatch = _batch.RequestId;
                    try
                    {
                        handler.OnBatch(_batch);
                    }
                    catch
                    {
                        _drainOkAfterHandlerThrow = await CancelAndDrainAsync(requestIdAtBatch)
                            .ConfigureAwait(false);
                        throw;
                    }
                    if (_options.initial_credit > 0)
                    {
                        await SendCreditAsync(_batch.RequestId, batchBytes + QwpConstants.HeaderSize, ct)
                            .ConfigureAwait(false);
                    }
                    break;

                case QwpEgressMsgKind.ResultEnd:
                    var (endRid, endTotal) = DecodeResultEnd(payload);
                    if (endRid != activeRid) continue;
                    handler.OnEnd(endTotal);
                    return;

                case QwpEgressMsgKind.ExecDone:
                    var (execRid, opType, rowsAffected) = DecodeExecDone(payload);
                    if (execRid != activeRid) continue;
                    handler.OnExecDone(opType, rowsAffected);
                    return;

                case QwpEgressMsgKind.QueryError:
                    var (errRid, status, message) = DecodeQueryError(payload);
                    if (errRid != activeRid && errRid != QwpConstants.RequestIdWildcard) continue;
                    handler.OnError(status, message);
                    return;

                case QwpEgressMsgKind.CacheReset:
                    DecodeCacheReset(payload);
                    break;

                case QwpEgressMsgKind.ServerInfo:
                    throw new IngressError(ErrorCode.ProtocolVersionError,
                        "unexpected SERVER_INFO mid-query");

                default:
                    throw new IngressError(ErrorCode.ProtocolVersionError,
                        $"unknown egress frame 0x{(byte)kind:X2}");
            }
        }
    }

    private async Task<(QwpEgressMsgKind Kind, ReadOnlyMemory<byte> Payload, byte HeaderFlags)>
        ReadFrameAsync(CancellationToken ct)
    {
        var transport = _transport ?? throw new InvalidOperationException("transport is not connected");
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
            throw new IngressError(ErrorCode.ProtocolVersionError, $"frame shorter than QWP1 header: {read} bytes");
        }

        var hdr = buffer.AsSpan(0, QwpConstants.HeaderSize);
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(hdr.Slice(QwpConstants.OffsetMagic, 4));
        if (magic != QwpConstants.Magic)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, $"bad QWP1 magic 0x{magic:X8}");
        }

        var version = hdr[QwpConstants.OffsetVersion];
        if (version > QwpConstants.SupportedEgressVersion)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, $"unsupported QWP version {version}");
        }

        var flags = hdr[QwpConstants.OffsetFlags];
        var payloadLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(hdr.Slice(QwpConstants.OffsetPayloadLength, 4));
        if (QwpConstants.HeaderSize + payloadLen != read)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"payload_length mismatch: header says {payloadLen}, frame is {read - QwpConstants.HeaderSize}");
        }

        if (payloadLen < 1)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "frame payload missing msg_kind byte");
        }

        var payloadMem = buffer.AsMemory(QwpConstants.HeaderSize, payloadLen);
        return ((QwpEgressMsgKind)payloadMem.Span[0], payloadMem, flags);
    }

    private async Task SendQueryRequestAsync(
        long requestId, string sql, long initialCredit,
        ReadOnlyMemory<byte> bindBlob, int bindCount, CancellationToken ct)
    {
        var sqlByteCount = StrictUtf8.GetByteCount(sql);
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
            var transport = _transport ?? throw new InvalidOperationException("transport is not connected");
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
            throw new IngressError(ErrorCode.ProtocolVersionError, "compressed RESULT_BATCH missing prelude");
        }

        QwpVarint.Read(span.Slice(9), out var seqBytes);
        var preludeLen = 1 + 8 + seqBytes;
        if (preludeLen > span.Length)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "compressed RESULT_BATCH prelude truncated");
        }

        var compressed = span.Slice(preludeLen);
        if (compressed.IsEmpty)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "zstd RESULT_BATCH has empty compressed body");
        }
        // ulong.MaxValue / -1 are zstd's "unknown size" / "error" sentinels — must reject before truncating to int.
        var declaredSize = ZstdSharp.Decompressor.GetDecompressedSize(compressed);
        if (declaredSize >= unchecked((ulong)-2L)
            || declaredSize > (ulong)QwpConstants.MaxResultBatchWireBytes)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"zstd frame reports decompressed size {declaredSize}, exceeds {QwpConstants.MaxResultBatchWireBytes}");
        }
        var decompressedSize = (int)declaredSize;

        var needed = preludeLen + decompressedSize;
        if (_decompressBuffer.Length < needed)
        {
            _decompressBuffer = new byte[needed];
        }

        span.Slice(0, preludeLen).CopyTo(_decompressBuffer);

        _decompressor ??= new ZstdSharp.Decompressor();
        var written = _decompressor.Unwrap(compressed, _decompressBuffer.AsSpan(preludeLen, decompressedSize));
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
        _cancelFrameBuf[0] = QwpConstants.MsgKindCancel;
        BinaryPrimitives.WriteInt64LittleEndian(_cancelFrameBuf.AsSpan(1, 8), requestId);

        if (_transport is null) return;
        await SendFrameAsync(_cancelFrameBuf, ct).ConfigureAwait(false);
    }

    private static QwpServerInfo DecodeServerInfo(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 1 + 8 + 4 + 8 + 2)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"SERVER_INFO too short: {s.Length} bytes");
        }

        if (s[0] != QwpConstants.MsgKindServerInfo)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"SERVER_INFO has wrong msg_kind 0x{s[0]:X2}");
        }

        var role = s[1];
        var epoch = BinaryPrimitives.ReadUInt64LittleEndian(s.Slice(2, 8));
        var capabilities = BinaryPrimitives.ReadUInt32LittleEndian(s.Slice(10, 4));
        var serverWallNs = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(14, 8));

        var clusterIdLen = BinaryPrimitives.ReadUInt16LittleEndian(s.Slice(22, 2));
        if (s.Length < 24 + clusterIdLen + 2)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "SERVER_INFO truncated at cluster_id");
        }
        var clusterId = StrictUtf8.GetString(s.Slice(24, clusterIdLen));
        var nodeIdLenOffset = 24 + clusterIdLen;
        var nodeIdLen = BinaryPrimitives.ReadUInt16LittleEndian(s.Slice(nodeIdLenOffset, 2));
        var nodeIdStart = nodeIdLenOffset + 2;
        if (s.Length < nodeIdStart + nodeIdLen)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "SERVER_INFO truncated at node_id");
        }
        var nodeId = StrictUtf8.GetString(s.Slice(nodeIdStart, nodeIdLen));

        return new QwpServerInfo
        {
            Role = role,
            Epoch = epoch,
            Capabilities = capabilities,
            ServerWallNs = serverWallNs,
            ClusterId = clusterId,
            NodeId = nodeId,
        };
    }

    private static (long RequestId, long TotalRows) DecodeResultEnd(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 8) throw new IngressError(ErrorCode.ProtocolVersionError, "RESULT_END too short");
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(1, 8));
        var p = 9;
        QwpVarint.Read(s.Slice(p), out var consumed1); // final_seq is informational; not surfaced.
        p += consumed1;
        var totalRows = (long)QwpVarint.Read(s.Slice(p), out var consumed2);
        p += consumed2;
        if (p != s.Length)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"RESULT_END trailing bytes: consumed {p}, payload {s.Length}");
        }
        return (requestId, totalRows);
    }

    private static (long RequestId, byte OpType, long RowsAffected) DecodeExecDone(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 8 + 1 + 1) throw new IngressError(ErrorCode.ProtocolVersionError, "EXEC_DONE too short");
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(1, 8));
        var opType = s[9];
        var rowsAffected = (long)QwpVarint.Read(s.Slice(10), out var consumed);
        var p = 10 + consumed;
        if (p != s.Length)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"EXEC_DONE trailing bytes: consumed {p}, payload {s.Length}");
        }
        return (requestId, opType, rowsAffected);
    }

    private static (long RequestId, byte Status, string Message) DecodeQueryError(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 1 + 8 + 1 + 2)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "QUERY_ERROR too short");
        }
        var requestId = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(1, 8));
        var status = s[9];
        var msgLen = BinaryPrimitives.ReadUInt16LittleEndian(s.Slice(10, 2));
        if (msgLen > QwpConstants.MaxErrorMessageBytes)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QUERY_ERROR message length {msgLen} exceeds {QwpConstants.MaxErrorMessageBytes}");
        }
        if (s.Length != 12 + msgLen)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QUERY_ERROR length mismatch: msgLen={msgLen} payload={s.Length}");
        }
        var msg = StrictUtf8.GetString(s.Slice(12, msgLen));
        return (requestId, status, msg);
    }

    private void DecodeCacheReset(ReadOnlyMemory<byte> payload)
    {
        var s = payload.Span;
        if (s.Length < 2) throw new IngressError(ErrorCode.ProtocolVersionError, "CACHE_RESET too short");
        var mask = s[1];
        if ((mask & QwpConstants.ResetMaskDict) != 0) _connState.ResetSymbolDict();
        if ((mask & QwpConstants.ResetMaskSchemas) != 0) _connState.ResetSchemas();
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
