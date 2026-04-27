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
using System.Threading;
using System.Threading.Tasks;

namespace QuestDB.Qwp;

/// <summary>
///     <see cref="IWebSocketChannel"/> adapter over <see cref="ClientWebSocket"/>.
///     The instance owns the underlying socket — callers who need direct access
///     should construct the <see cref="ClientWebSocket"/> separately and hand it
///     to this constructor.
/// </summary>
/// <remarks>
///     Experimental. <see cref="ClientWebSocket.SendAsync(ReadOnlyMemory{byte}, WebSocketMessageType, bool, CancellationToken)"/>
///     and the matching receive overload do all the framing work — Java's bespoke
///     <c>WebSocketFrameParser</c> / <c>WebSocketFrameWriter</c> aren't needed.
///     Ping / pong are handled implicitly by the runtime (PONGs arrive via
///     <see cref="ReceiveAsync"/> with <see cref="WebSocketMessageType.Binary"/>
///     or as keep-alive pings the runtime handles internally — applications
///     read <see cref="ClientWebSocket.SendAsync(ReadOnlyMemory{byte}, WebSocketMessageType, bool, CancellationToken)"/>
///     and pretend they don't exist).
/// </remarks>
internal sealed class ClientWebSocketChannel : IWebSocketChannel, IDisposable
{
    private readonly ClientWebSocket _socket;

    public ClientWebSocketChannel(ClientWebSocket socket)
    {
        _socket = socket ?? throw new ArgumentNullException(nameof(socket));
    }

    public bool IsConnected => _socket.State == WebSocketState.Open;

    public Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        return _socket.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, cancellationToken).AsTask();
    }

    public async Task<WebSocketChannelReceiveResult> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var result = await _socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
        return new WebSocketChannelReceiveResult(result.Count, result.MessageType, result.EndOfMessage);
    }

    public Task SendPingAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        // ClientWebSocket doesn't expose an explicit ping API — keep-alive is handled by
        // the runtime via WebSocketKeepAliveInterval. For QWP's application-level ping,
        // the convention is to send a binary ping payload that the server echoes; the
        // server's QWP protocol layer recognises it as a ping and replies with the same
        // payload (or an empty binary frame). Treat it as a regular binary send.
        return SendBinaryAsync(payload, cancellationToken);
    }

    public Task CloseAsync(WebSocketCloseStatus status, string? statusDescription, CancellationToken cancellationToken)
    {
        if (_socket.State == WebSocketState.Open || _socket.State == WebSocketState.CloseReceived)
        {
            return _socket.CloseAsync(status, statusDescription, cancellationToken);
        }
        return Task.CompletedTask;
    }

    public void ForceDisconnect() => _socket.Abort();

    public void Dispose() => _socket.Dispose();
}
