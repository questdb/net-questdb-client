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
///     Async WebSocket channel abstraction used by <c>WebSocketSendQueue</c> and the
///     QWP WebSocket sender. The .NET equivalent of Java's
///     <c>WebSocketClient</c> + <c>WebSocketFrameHandler</c> pair, except that
///     framing is handled by <see cref="System.Net.WebSockets.ClientWebSocket"/>
///     (or any other test mock) rather than ported from Java.
/// </summary>
internal interface IWebSocketChannel
{
    /// <summary>
    ///     Whether the channel is in an open state. Drops to false on graceful close,
    ///     remote close, or transport failure.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>Sends a single binary frame.</summary>
    Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken);

    /// <summary>
    ///     Receives bytes into <paramref name="buffer"/>. Returns the number of bytes filled,
    ///     the message type (Binary, Text, Close), and whether this is the final fragment of
    ///     the current message (matches <see cref="WebSocketReceiveResult"/>).
    /// </summary>
    Task<WebSocketChannelReceiveResult> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken);

    /// <summary>Sends a PING frame. The peer's PONG arrives via <see cref="ReceiveAsync"/>.</summary>
    Task SendPingAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);

    /// <summary>Closes the channel gracefully with the given status.</summary>
    Task CloseAsync(WebSocketCloseStatus status, string? statusDescription, CancellationToken cancellationToken);

    /// <summary>Forcibly aborts the underlying transport. Used to unwind a stuck IO loop on shutdown.</summary>
    void ForceDisconnect();
}

/// <summary>
///     Result of <see cref="IWebSocketChannel.ReceiveAsync"/>. Mirrors
///     <see cref="WebSocketReceiveResult"/> but with the count plus message-type plus
///     final-fragment indicator the queue actually needs (no close-status detail).
/// </summary>
internal readonly record struct WebSocketChannelReceiveResult(
    int Count,
    WebSocketMessageType MessageType,
    bool EndOfMessage);
