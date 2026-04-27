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
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Programmable <see cref="IWebSocketChannel"/> for unit tests. Records every binary
///     frame sent to the channel; serves a scripted sequence of inbound frames to
///     <see cref="ReceiveAsync"/>. Replaces Java's <c>FakeWebSocketClient</c>.
/// </summary>
internal sealed class FakeWebSocketChannel : IWebSocketChannel, IDisposable
{
    private readonly Channel<InboundFrame> _inbound = Channel.CreateUnbounded<InboundFrame>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    private readonly ConcurrentQueue<byte[]> _sentFrames = new();

    private bool _connected = true;
    private bool _disposed;

    public bool IsConnected => _connected && !_disposed;

    public IReadOnlyCollection<byte[]> SentFrames => _sentFrames;

    /// <summary>Pushes <paramref name="bytes"/> as the next inbound frame seen by <see cref="ReceiveAsync"/>.</summary>
    public void EnqueueInboundBinary(ReadOnlySpan<byte> bytes)
    {
        var copy = bytes.ToArray();
        _inbound.Writer.TryWrite(new InboundFrame(copy, WebSocketMessageType.Binary, EndOfMessage: true));
    }

    /// <summary>Pushes a Close frame so the next ReceiveAsync surfaces a graceful disconnect.</summary>
    public void EnqueueInboundClose()
    {
        _inbound.Writer.TryWrite(new InboundFrame(Array.Empty<byte>(), WebSocketMessageType.Close, EndOfMessage: true));
    }

    public Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        if (!_connected) throw new InvalidOperationException("channel is closed");
        _sentFrames.Enqueue(data.ToArray());
        return Task.CompletedTask;
    }

    public async Task<WebSocketChannelReceiveResult> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (!_connected) throw new InvalidOperationException("channel is closed");

        var frame = await _inbound.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        if (frame.MessageType == WebSocketMessageType.Close) _connected = false;

        var copyLength = Math.Min(frame.Bytes.Length, buffer.Length);
        frame.Bytes.AsSpan(0, copyLength).CopyTo(buffer.Span);
        // If the inbound frame was longer than the receive buffer, push the remainder back
        // for the next read — mirrors ClientWebSocket's fragmenting behaviour.
        if (copyLength < frame.Bytes.Length)
        {
            _inbound.Writer.TryWrite(new InboundFrame(
                frame.Bytes.AsSpan(copyLength).ToArray(),
                frame.MessageType,
                EndOfMessage: frame.EndOfMessage));
            return new WebSocketChannelReceiveResult(copyLength, frame.MessageType, EndOfMessage: false);
        }
        return new WebSocketChannelReceiveResult(copyLength, frame.MessageType, frame.EndOfMessage);
    }

    public Task SendPingAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        => SendBinaryAsync(payload, cancellationToken);

    public Task CloseAsync(WebSocketCloseStatus status, string? statusDescription, CancellationToken cancellationToken)
    {
        _connected = false;
        _inbound.Writer.TryComplete();
        return Task.CompletedTask;
    }

    public void ForceDisconnect()
    {
        _connected = false;
        _inbound.Writer.TryComplete();
    }

    public void Dispose()
    {
        _disposed = true;
        _connected = false;
        _inbound.Writer.TryComplete();
    }

    private readonly record struct InboundFrame(byte[] Bytes, WebSocketMessageType MessageType, bool EndOfMessage);
}
