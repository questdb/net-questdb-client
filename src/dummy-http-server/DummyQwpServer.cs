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

using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace dummy_http_server;

/// <summary>
///     Lightweight Kestrel-based <c>/write/v4</c> WebSocket endpoint for QWP transport tests.
/// </summary>
/// <remarks>
///     Spins up on <c>http://127.0.0.1:0</c> (a random ephemeral port). The bound URI is exposed
///     via <see cref="Uri" /> after <see cref="StartAsync" /> completes. A test-supplied
///     <see cref="DummyQwpServerOptions.FrameHandler" /> decides what response (if any) to send for
///     each incoming binary frame.
///     <para />
///     Captures everything received in <see cref="ReceivedFrames" /> and exposes the upgrade
///     request headers via <see cref="LastUpgradeHeaders" /> so tests can verify that the client
///     sent the expected <c>X-QWP-*</c> headers.
/// </remarks>
public sealed class DummyQwpServer : IAsyncDisposable
{
    private readonly DummyQwpServerOptions _options;
    private readonly IHost _host;
    private readonly ConcurrentQueue<byte[]> _received = new();

    private string? _baseUri;

    public DummyQwpServer(DummyQwpServerOptions? options = null)
    {
        _options = options ?? new DummyQwpServerOptions();

        _host = new HostBuilder()
            .ConfigureWebHost(webHost =>
            {
                webHost.UseKestrel(kestrel =>
                {
                    kestrel.Listen(IPAddress.Loopback, 0, listen =>
                    {
                        listen.Protocols = HttpProtocols.Http1;
                        if (_options.TlsCertificate is not null)
                        {
                            listen.UseHttps(_options.TlsCertificate);
                        }
                    });
                });
                webHost.Configure(app =>
                {
                    app.UseWebSockets();
                    app.Run(async ctx =>
                    {
                        if (!ctx.Request.Path.Equals(_options.Path, StringComparison.Ordinal))
                        {
                            ctx.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return;
                        }

                        await HandleWriteV4(ctx).ConfigureAwait(false);
                    });
                });
            })
            .Build();
    }

    /// <summary>The URI clients should connect to. Available after <see cref="StartAsync" /> returns.</summary>
    public Uri Uri
    {
        get
        {
            if (_baseUri is null)
            {
                throw new InvalidOperationException("Server has not started yet");
            }

            var ws = _baseUri
                .Replace("https://", "wss://", StringComparison.OrdinalIgnoreCase)
                .Replace("http://", "ws://", StringComparison.OrdinalIgnoreCase);
            return new Uri(ws + _options.Path);
        }
    }

    /// <summary>All binary frames received, in arrival order.</summary>
    public IReadOnlyCollection<byte[]> ReceivedFrames => _received;

    /// <summary>Headers from the last WebSocket upgrade request, set after the upgrade completes.</summary>
    public IDictionary<string, string>? LastUpgradeHeaders { get; private set; }

    /// <summary>Starts the host and binds to its random port.</summary>
    public async Task StartAsync(CancellationToken ct = default)
    {
        await _host.StartAsync(ct).ConfigureAwait(false);

        var server = _host.Services.GetRequiredService<IServer>();
        var addresses = server.Features.Get<IServerAddressesFeature>()?.Addresses
            ?? throw new InvalidOperationException("server addresses unavailable");
        _baseUri = addresses.First();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _host.StopAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
        }
        catch
        {
            // best-effort
        }

        _host.Dispose();
        await Task.CompletedTask;
    }

    private async Task HandleWriteV4(HttpContext ctx)
    {
        if (!ctx.WebSockets.IsWebSocketRequest)
        {
            ctx.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            await ctx.Response.WriteAsync("WebSocket upgrade required").ConfigureAwait(false);
            return;
        }

        // Capture the upgrade headers before the handshake runs.
        LastUpgradeHeaders = ctx.Request.Headers
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToString(), StringComparer.OrdinalIgnoreCase);

        // The X-QWP-Version response header is set on Response.Headers BEFORE accepting; the
        // accept call writes the 101 response.
        if (_options.NegotiatedVersion is not null)
        {
            ctx.Response.Headers["X-QWP-Version"] = _options.NegotiatedVersion;
        }

        if (_options.RejectUpgradeWith is { } rejectStatus)
        {
            ctx.Response.StatusCode = (int)rejectStatus;
            await ctx.Response.WriteAsync("rejected by test").ConfigureAwait(false);
            return;
        }

        using var ws = await ctx.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);

        var receiveBuf = new byte[_options.ReceiveBufferSize];
        var framesHandled = 0;
        while (ws.State == WebSocketState.Open)
        {
            var totalRead = 0;
            WebSocketReceiveResult result;
            do
            {
                if (totalRead >= receiveBuf.Length)
                {
                    return; // overflow; bail.
                }

                result = await ws.ReceiveAsync(
                        new ArraySegment<byte>(receiveBuf, totalRead, receiveBuf.Length - totalRead),
                        ctx.RequestAborted)
                    .ConfigureAwait(false);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, null, ctx.RequestAborted)
                        .ConfigureAwait(false);
                    return;
                }

                totalRead += result.Count;
            } while (!result.EndOfMessage);

            var frame = new byte[totalRead];
            Buffer.BlockCopy(receiveBuf, 0, frame, 0, totalRead);
            _received.Enqueue(frame);

            // Multi-frame handler takes precedence so individual tests can switch behaviour.
            var multi = _options.FrameHandlerMulti?.Invoke(frame);
            if (multi is not null)
            {
                foreach (var response in multi)
                {
                    if (response is not null && response.Length > 0)
                    {
                        await ws.SendAsync(response, WebSocketMessageType.Binary, endOfMessage: true, ctx.RequestAborted)
                            .ConfigureAwait(false);
                    }
                }
            }
            else
            {
                var response = _options.FrameHandler?.Invoke(frame);
                if (response is not null && response.Length > 0)
                {
                    await ws.SendAsync(response, WebSocketMessageType.Binary, endOfMessage: true, ctx.RequestAborted)
                        .ConfigureAwait(false);
                }
            }

            framesHandled++;
            if (_options.CloseAfterFrameCount is { } cap && framesHandled >= cap)
            {
                await ws.CloseAsync(_options.CloseStatus, _options.CloseReason, ctx.RequestAborted)
                    .ConfigureAwait(false);
                return;
            }
        }
    }
}

/// <summary>Configuration knobs for <see cref="DummyQwpServer" />.</summary>
public sealed class DummyQwpServerOptions
{
    /// <summary>HTTP path to bind. Defaults to <c>/write/v4</c>.</summary>
    public string Path { get; init; } = "/write/v4";

    /// <summary>Value to return in the <c>X-QWP-Version</c> response header. Set to <c>null</c> to omit.</summary>
    public string? NegotiatedVersion { get; init; } = "1";

    /// <summary>If set, the server returns this HTTP status during the upgrade and never opens the WebSocket.</summary>
    public HttpStatusCode? RejectUpgradeWith { get; init; }

    /// <summary>If set, Kestrel binds an HTTPS listener using this certificate; <see cref="DummyQwpServer.Uri" /> returns a <c>wss://</c> URI.</summary>
    public X509Certificate2? TlsCertificate { get; init; }

    /// <summary>If set, the server emits its (optional) response then sends a WebSocket CLOSE with this status after handling the Nth frame (1-based).</summary>
    public int? CloseAfterFrameCount { get; init; }

    /// <summary>Close status to send when <see cref="CloseAfterFrameCount" /> triggers.</summary>
    public WebSocketCloseStatus CloseStatus { get; init; } = WebSocketCloseStatus.InternalServerError;

    /// <summary>Optional close reason text accompanying <see cref="CloseStatus" />.</summary>
    public string? CloseReason { get; init; } = "test injected close";

    /// <summary>Per-frame response generator. Return null/empty to suppress a response.</summary>
    public Func<byte[], byte[]?>? FrameHandler { get; init; }

    /// <summary>
    ///     Per-frame multi-response generator. Each call may return multiple response frames; the
    ///     server emits them one after another as separate WebSocket binary messages.
    ///     Use this when the response sequence has out-of-band frames (e.g. <c>DURABLE_ACK</c>
    ///     interleaved with the request's <c>OK</c>).
    /// </summary>
    public Func<byte[], IReadOnlyList<byte[]>?>? FrameHandlerMulti { get; init; }

    /// <summary>Buffer size for reading incoming WebSocket messages.</summary>
    public int ReceiveBufferSize { get; init; } = 64 * 1024;
}
