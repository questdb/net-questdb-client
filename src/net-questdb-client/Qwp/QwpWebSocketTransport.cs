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
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.WebSockets;
using System.Reflection;
using QuestDB.Enums;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Thin wrapper over <see cref="ClientWebSocket" /> that handles QWP-specific upgrade
///     headers, version negotiation, single-frame binary I/O, optional dump-mode capture, and
///     graceful close.
/// </summary>
/// <remarks>
///     <b>Why net7.0+ only.</b> The version-negotiation handshake reads the server's
///     <c>X-QWP-Version</c> response header via <see cref="ClientWebSocket.HttpResponseHeaders" />,
///     which was added in .NET 7. Older targets get HTTP / TCP transports only.
///     <para />
///     <b>KeepAliveInterval = 0</b>: we manage connection liveness via QWP ACK timeouts in the
///     sender; built-in WebSocket pings would only complicate that.
///     <para />
///     <b>Dump format</b>: when <see cref="QwpWebSocketTransportOptions.DumpStream" /> is set, the
///     transport tees both directions of binary traffic to that stream as
///     <c>[direction byte 'S'/'R'][uint32 LE length][payload]</c> records. The format is internal
///     and may change between client versions; useful for tests and bug reports.
/// </remarks>
internal sealed class QwpWebSocketTransport : IQwpCursorTransport
{
    private const int DumpHeaderSize = 5;
    private static readonly string DefaultClientId = BuildDefaultClientId();

    private readonly QwpWebSocketTransportOptions _options;
    private readonly ClientWebSocket _client = new();
    private readonly object _dumpLock = new();

    private bool _disposed;
    private int _negotiatedVersion;

    /// <summary>Constructs a transport. <see cref="ConnectAsync" /> must be called before any I/O.</summary>
    public QwpWebSocketTransport(QwpWebSocketTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Uri is null)
        {
            throw new ArgumentException("Uri is required", nameof(options));
        }

        _options = options;

        var ws = _client.Options;
        ws.KeepAliveInterval = TimeSpan.Zero;
        ws.CollectHttpResponseDetails = true; // expose response headers for X-QWP-Version negotiation
        ws.Proxy = options.Proxy;
        ws.SetRequestHeader(QwpConstants.HeaderMaxVersion, options.ClientMaxVersion.ToString());
        ws.SetRequestHeader(QwpConstants.HeaderClientId, options.ClientId ?? DefaultClientId);

        if (!string.IsNullOrEmpty(options.AuthorizationHeader))
        {
            ws.SetRequestHeader("Authorization", options.AuthorizationHeader);
        }

        if (options.RequestDurableAck)
        {
            ws.SetRequestHeader(QwpConstants.HeaderRequestDurableAck, "true");
        }

        if (options.RemoteCertificateValidationCallback is not null)
        {
            ws.RemoteCertificateValidationCallback = options.RemoteCertificateValidationCallback;
        }

        if (options.ExtraRequestHeaders is { } extra)
        {
            foreach (var kv in extra)
            {
                ws.SetRequestHeader(kv.Key, kv.Value);
            }
        }
    }

    /// <summary>True once the upgrade has succeeded and the WebSocket is open.</summary>
    public bool IsConnected => _client.State == WebSocketState.Open;

    /// <summary>
    ///     QWP version chosen by the server during the upgrade. <c>0</c> until <see cref="ConnectAsync" />
    ///     has run; always <c>1</c> in v1.
    /// </summary>
    public int NegotiatedVersion => _negotiatedVersion;

    /// <summary>
    ///     Server-selected <c>X-QWP-Content-Encoding</c> from the upgrade response (e.g.
    ///     <c>"zstd;level=3"</c>); <c>null</c> when omitted (= raw) or before <see cref="ConnectAsync" />.
    /// </summary>
    public string? NegotiatedContentEncoding { get; private set; }

    /// <summary>
    ///     Opens the TCP/TLS connection, performs the WebSocket upgrade, and validates that the
    ///     server selected a version this client speaks.
    /// </summary>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();

        try
        {
            await _client.ConnectAsync(_options.Uri!, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            var status = (int)_client.HttpStatusCode;
            if (status is 401 or 403)
            {
                throw new IngressError(ErrorCode.AuthError,
                    $"WebSocket upgrade rejected with HTTP {status} for {_options.Uri}", ex);
            }

            if (status == 421)
            {
                var role = ReadOptionalHeader(QwpConstants.HeaderQuestDbRole);
                if (!string.IsNullOrEmpty(role))
                {
                    throw new QwpIngressRoleRejectedException(role!, _options.Uri!, ex);
                }
            }

            throw new IngressError(ErrorCode.SocketError, $"failed to connect to {_options.Uri}", ex);
        }

        _negotiatedVersion = ReadNegotiatedVersion();
        if (_negotiatedVersion < 1 || _negotiatedVersion > _options.ClientMaxVersion)
        {
            await TryCloseAsync(WebSocketCloseStatus.ProtocolError, "unsupported QWP version", ct)
                .ConfigureAwait(false);
            throw new IngressError(
                ErrorCode.ProtocolVersionError,
                $"server negotiated QWP version {_negotiatedVersion}; this client supports v1..v{_options.ClientMaxVersion}");
        }
        NegotiatedContentEncoding = ReadOptionalHeader(QwpConstants.HeaderContentEncoding);
    }

    private string? ReadOptionalHeader(string name)
    {
        var headers = _client.HttpResponseHeaders;
        if (headers is null || !headers.TryGetValue(name, out var values)) return null;
        foreach (var v in values)
        {
            if (!string.IsNullOrEmpty(v)) return v;
        }
        return null;
    }

    /// <summary>Sends one QWP frame as a single WebSocket BINARY message.</summary>
    public async Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureOpen();

        DumpFrame((byte)'S', data.Span);
        await _client.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     Receives one QWP response frame, aggregating fragments until <c>EndOfMessage</c>.
    /// </summary>
    /// <param name="destination">Caller-owned buffer; must be large enough for the entire message.</param>
    /// <param name="ct">Cancellation token; cancellation aborts the in-progress receive.</param>
    /// <returns>Number of bytes written into <paramref name="destination" />.</returns>
    /// <exception cref="IngressError">
    ///     If the message exceeds <paramref name="destination" />, the server closes the connection,
    ///     or the message is not a binary frame.
    /// </exception>
    public async Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureOpen();

        var totalRead = 0;
        while (true)
        {
            var slice = destination.Slice(totalRead);
            if (slice.Length == 0)
            {
                throw new IngressError(
                    ErrorCode.SocketError,
                    $"incoming WebSocket frame exceeds the {destination.Length}-byte receive buffer; " +
                    "decrease batch size or increase the buffer");
            }

            var result = await _client.ReceiveAsync(slice, ct).ConfigureAwait(false);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                var ec = _client.CloseStatus == WebSocketCloseStatus.PolicyViolation
                    ? ErrorCode.AuthError
                    : ErrorCode.SocketError;
                throw new IngressError(
                    ec,
                    $"server closed the WebSocket: {_client.CloseStatus} {_client.CloseStatusDescription}");
            }

            if (result.MessageType != WebSocketMessageType.Binary)
            {
                throw new IngressError(
                    ErrorCode.ProtocolVersionError,
                    $"unexpected WebSocket message type {result.MessageType}; QWP uses binary frames");
            }

            totalRead += result.Count;

            if (result.EndOfMessage)
            {
                DumpFrame((byte)'R', destination.Span.Slice(0, totalRead));
                return totalRead;
            }
        }
    }

    /// <summary>
    ///     Like <see cref="ReceiveFrameAsync(Memory{byte}, CancellationToken)" /> but doubles
    ///     <paramref name="initial" /> when the incoming frame would otherwise overflow, up to
    ///     <paramref name="maxBytes" />. Returns the (possibly grown) buffer along with the byte
    ///     count. Frames exceeding the cap raise a SocketError.
    /// </summary>
    public async Task<(int Read, byte[] Buffer)> ReceiveFrameAsync(
        byte[] initial,
        int maxBytes,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureOpen();

        var buffer = initial;
        var totalRead = 0;
        while (true)
        {
            if (totalRead == buffer.Length)
            {
                if (buffer.Length >= maxBytes)
                {
                    throw new IngressError(
                        ErrorCode.SocketError,
                        $"incoming WebSocket frame exceeds the {maxBytes}-byte receive cap");
                }

                var newSize = Math.Min(maxBytes, Math.Max(buffer.Length * 2, totalRead + 1));
                Array.Resize(ref buffer, newSize);
            }

            var slice = buffer.AsMemory(totalRead);
            var result = await _client.ReceiveAsync(slice, ct).ConfigureAwait(false);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                var ec = _client.CloseStatus == WebSocketCloseStatus.PolicyViolation
                    ? ErrorCode.AuthError
                    : ErrorCode.SocketError;
                throw new IngressError(
                    ec,
                    $"server closed the WebSocket: {_client.CloseStatus} {_client.CloseStatusDescription}");
            }

            if (result.MessageType != WebSocketMessageType.Binary)
            {
                throw new IngressError(
                    ErrorCode.ProtocolVersionError,
                    $"unexpected WebSocket message type {result.MessageType}; QWP uses binary frames");
            }

            totalRead += result.Count;

            if (result.EndOfMessage)
            {
                DumpFrame((byte)'R', buffer.AsSpan(0, totalRead));
                return (totalRead, buffer);
            }
        }
    }

    /// <summary>
    ///     Sends a graceful WebSocket CLOSE frame and waits for the server's acknowledgement.
    ///     Idempotent and exception-tolerant: a transport in any non-closed state can call this.
    /// </summary>
    public async Task CloseAsync(
        WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure,
        string? description = null,
        CancellationToken ct = default)
    {
        if (_disposed)
        {
            return;
        }

        await TryCloseAsync(status, description, ct).ConfigureAwait(false);
    }

    Task IQwpCursorTransport.CloseAsync(CancellationToken cancellationToken) =>
        CloseAsync(ct: cancellationToken);

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        SfCleanup.Dispose(_client);
    }

    private async Task TryCloseAsync(WebSocketCloseStatus status, string? description, CancellationToken ct)
    {
        // Only attempt a CLOSE when the channel is still in a state that accepts one.
        var state = _client.State;
        if (state is not WebSocketState.Open and not WebSocketState.CloseReceived)
        {
            return;
        }

        try
        {
            await _client.CloseAsync(status, description, ct).ConfigureAwait(false);
        }
        catch
        {
            // Best-effort close; we already have the user's primary error in flight.
        }
    }

    private int ReadNegotiatedVersion()
    {
        // The version header proves we're talking to a QWP server, not an arbitrary WS service.
        var headers = _client.HttpResponseHeaders;
        if (headers is null || !headers.TryGetValue(QwpConstants.HeaderVersion, out var values))
        {
            throw new IngressError(
                ErrorCode.ProtocolVersionError,
                $"server did not return a {QwpConstants.HeaderVersion} header on the upgrade response; " +
                "endpoint is not a QWP server");
        }

        foreach (var value in values)
        {
            if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
            {
                return v;
            }
        }

        throw new IngressError(
            ErrorCode.ProtocolVersionError,
            $"server returned invalid {QwpConstants.HeaderVersion} header value");
    }

    private void DumpFrame(byte direction, ReadOnlySpan<byte> bytes)
    {
        var dump = _options.DumpStream;
        if (dump is null)
        {
            return;
        }

        Span<byte> header = stackalloc byte[DumpHeaderSize];
        header[0] = direction;
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(1, 4), bytes.Length);

        // SendBinaryAsync and ReceiveFrameAsync run concurrently; serialise so records don't tear.
        lock (_dumpLock)
        {
            dump.Write(header);
            dump.Write(bytes);
        }
    }

    private void EnsureOpen()
    {
        if (_client.State != WebSocketState.Open)
        {
            throw new IngressError(
                ErrorCode.SocketError,
                $"WebSocket is not open (state={_client.State}); call ConnectAsync first");
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpWebSocketTransport));
        }
    }

    private static string BuildDefaultClientId()
    {
        var version = typeof(QwpWebSocketTransport).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion
            ?? typeof(QwpWebSocketTransport).Assembly.GetName().Version?.ToString()
            ?? "unknown";
        return $"dotnet/{version}";
    }
}

/// <summary>Construction options for <see cref="QwpWebSocketTransport" />.</summary>
internal sealed class QwpWebSocketTransportOptions
{
    /// <summary>The endpoint URI, including scheme (<c>ws</c> or <c>wss</c>), host, port, and path.</summary>
    public Uri? Uri { get; init; }

    /// <summary>Optional <c>Authorization</c> header (e.g. "Basic abc..." or "Bearer xyz...").</summary>
    public string? AuthorizationHeader { get; init; }

    /// <summary>Whether to opt in to STATUS_DURABLE_ACK frames. Maps to the upgrade header.</summary>
    public bool RequestDurableAck { get; init; }

    /// <summary>Optional dump-mode stream; the transport tees both directions of binary traffic here.</summary>
    public Stream? DumpStream { get; init; }

    /// <summary>Maximum QWP version this client speaks. The wire pins ingest to 1; exposing the knob is for forward-compat tests.</summary>
    public int ClientMaxVersion { get; init; } = QwpConstants.SupportedIngestVersion;

    /// <summary>Free-form client identifier used for the <c>X-QWP-Client-Id</c> header.</summary>
    public string? ClientId { get; init; }

    /// <summary>Optional callback for TLS certificate validation; bypassed when null.</summary>
    public RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; init; }

    /// <summary>Extra HTTP request headers to set on the WebSocket upgrade.</summary>
    public IReadOnlyDictionary<string, string>? ExtraRequestHeaders { get; init; }

    /// <summary>
    ///     Proxy override for the underlying <see cref="ClientWebSocket" />. <c>null</c> = no proxy
    ///     (default; long-lived WS rarely survives HTTP proxies). Set to <see cref="WebRequest.DefaultWebProxy" />
    ///     for system proxy, or a fresh <see cref="WebProxy" /> for an explicit URI.
    /// </summary>
    public IWebProxy? Proxy { get; init; }
}

#endif
