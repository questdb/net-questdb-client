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
using NUnit.Framework;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Smoke tests for <see cref="FakeWebSocketChannel"/> — the unit-test mock used by
///     PR 5b2's WebSocketSendQueue tests. Verifies the script + record + close model
///     behaves before we build dependent tests on top.
/// </summary>
[TestFixture]
public class FakeWebSocketChannelTests
{
    [Test]
    public async Task RecordsSentFrames()
    {
        using var ch = new FakeWebSocketChannel();
        await ch.SendBinaryAsync(new byte[] { 1, 2, 3 }, CancellationToken.None);
        await ch.SendBinaryAsync(new byte[] { 4, 5 }, CancellationToken.None);

        var sent = ch.SentFrames.ToArray();
        Assert.That(sent.Length, Is.EqualTo(2));
        Assert.That(sent[0], Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(sent[1], Is.EqualTo(new byte[] { 4, 5 }));
    }

    [Test]
    public async Task ReplaysScriptedInboundFrames()
    {
        using var ch = new FakeWebSocketChannel();
        ch.EnqueueInboundBinary(new byte[] { 0x10, 0x20 });
        ch.EnqueueInboundBinary(new byte[] { 0x30 });

        var buf = new byte[16];
        var first = await ch.ReceiveAsync(buf, CancellationToken.None);
        Assert.That(first.Count, Is.EqualTo(2));
        Assert.That(first.MessageType, Is.EqualTo(WebSocketMessageType.Binary));
        Assert.That(first.EndOfMessage, Is.True);
        Assert.That(buf.AsSpan(0, 2).ToArray(), Is.EqualTo(new byte[] { 0x10, 0x20 }));

        var second = await ch.ReceiveAsync(buf, CancellationToken.None);
        Assert.That(second.Count, Is.EqualTo(1));
        Assert.That(buf[0], Is.EqualTo((byte)0x30));
    }

    [Test]
    public async Task FragmentsLargeFramesToBufferSize()
    {
        // Simulates how ClientWebSocket fragments oversize messages: caller's buffer is too
        // small, so the channel returns EndOfMessage=false and pushes the remainder back.
        using var ch = new FakeWebSocketChannel();
        ch.EnqueueInboundBinary(new byte[] { 1, 2, 3, 4, 5 });

        var buf = new byte[3];
        var first = await ch.ReceiveAsync(buf, CancellationToken.None);
        Assert.That(first.Count, Is.EqualTo(3));
        Assert.That(first.EndOfMessage, Is.False);
        Assert.That(buf, Is.EqualTo(new byte[] { 1, 2, 3 }));

        var second = await ch.ReceiveAsync(buf, CancellationToken.None);
        Assert.That(second.Count, Is.EqualTo(2));
        Assert.That(second.EndOfMessage, Is.True);
        Assert.That(buf.AsSpan(0, 2).ToArray(), Is.EqualTo(new byte[] { 4, 5 }));
    }

    [Test]
    public async Task EnqueueCloseMakesReceiveReturnCloseFrame()
    {
        using var ch = new FakeWebSocketChannel();
        ch.EnqueueInboundClose();

        var result = await ch.ReceiveAsync(new byte[16], CancellationToken.None);
        Assert.That(result.MessageType, Is.EqualTo(WebSocketMessageType.Close));
        Assert.That(ch.IsConnected, Is.False);
    }

    [Test]
    public void ForceDisconnectFlipsIsConnected()
    {
        using var ch = new FakeWebSocketChannel();
        Assert.That(ch.IsConnected, Is.True);
        ch.ForceDisconnect();
        Assert.That(ch.IsConnected, Is.False);
        Assert.That(() => ch.SendBinaryAsync(new byte[1], CancellationToken.None),
                    Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public async Task ReceiveBlocksUntilFrameAvailable()
    {
        using var ch = new FakeWebSocketChannel();
        var receiveTask = Task.Run(async () =>
        {
            var buf = new byte[8];
            return await ch.ReceiveAsync(buf, CancellationToken.None);
        });
        await Task.Delay(20);
        Assert.That(receiveTask.IsCompleted, Is.False, "should block while no inbound frame");

        ch.EnqueueInboundBinary(new byte[] { 0xCA, 0xFE });
        var result = await receiveTask.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.That(result.Count, Is.EqualTo(2));
    }

    [Test]
    public async Task SendPingRecordsAsBinaryFrame()
    {
        // ClientWebSocketChannel routes SendPingAsync through the same SendAsync as binary;
        // FakeWebSocketChannel records both as plain sent frames.
        using var ch = new FakeWebSocketChannel();
        await ch.SendPingAsync(new byte[] { 0x99 }, CancellationToken.None);
        Assert.That(ch.SentFrames.Count, Is.EqualTo(1));
        Assert.That(ch.SentFrames.First(), Is.EqualTo(new byte[] { 0x99 }));
    }
}
