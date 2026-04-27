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

using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Smoke + state-machine coverage for <see cref="WebSocketSendQueue"/>. The Java
///     <c>WebSocketSendQueueTest.java</c> is much larger; .NET ships a focused suite
///     covering the essential paths since the queue's design itself diverges
///     (Channel&lt;T&gt; + Tasks vs volatile + wait/notify).
/// </summary>
[TestFixture]
public class WebSocketSendQueueTests
{
    [Test]
    public void EnqueueRequiresSealedBuffer()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);
        using var buffer = new MicrobatchBuffer(64);
        // Buffer is in FILLING state — must be sealed first.
        Assert.That(() => queue.Enqueue(buffer), Throws.TypeOf<IngressError>());
    }

    [Test]
    public void EnqueueRejectsNullBuffer()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);
        Assert.That(() => queue.Enqueue(null!), Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void EnqueueSendsBytesThroughChannel()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);
        using var buffer = new MicrobatchBuffer(64);
        buffer.Write(new byte[] { 1, 2, 3, 4 });
        buffer.IncrementRowCount();
        buffer.Seal();
        queue.Enqueue(buffer);

        // Send happens on a background task — wait briefly.
        Assert.That(WaitFor(() => queue.TotalBatchesSent == 1, TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(queue.TotalBytesSent, Is.EqualTo(4L));
        Assert.That(fake.SentFrames.Count, Is.EqualTo(1));
        Assert.That(fake.SentFrames.First(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
        Assert.That(buffer.IsRecycled, Is.True);
    }

    [Test]
    public void OkResponseAdvancesPerTableSeqTxn()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);

        // Enqueue a sealed buffer so the receive task has something to react to.
        using var buffer = new MicrobatchBuffer(64);
        buffer.WriteByte(0xFF);
        buffer.IncrementRowCount();
        buffer.Seal();
        queue.Enqueue(buffer);
        Assert.That(WaitFor(() => queue.TotalBatchesSent == 1, TimeSpan.FromSeconds(2)), Is.True);

        // Server replies with an OK frame including a per-table seqTxn entry.
        var reply = WebSocketResponse.Success(0);
        reply.AddTableEntry("trades", 100);
        var bytes = new byte[reply.SerializedSize()];
        var written = reply.WriteTo(bytes);
        fake.EnqueueInboundBinary(bytes.AsSpan(0, written));

        Assert.That(WaitFor(() => queue.GetCommittedSeqTxn("trades") == 100, TimeSpan.FromSeconds(2)),
                    Is.True);
        Assert.That(queue.TotalAcks, Is.EqualTo(1L));
        // Higher seqTxn replaces; lower does not regress.
        var reply2 = WebSocketResponse.Success(1);
        reply2.AddTableEntry("trades", 50); // earlier value, should NOT win
        var bytes2 = new byte[reply2.SerializedSize()];
        reply2.WriteTo(bytes2);
        fake.EnqueueInboundBinary(bytes2);
        Assert.That(WaitFor(() => queue.TotalAcks == 2, TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(queue.GetCommittedSeqTxn("trades"), Is.EqualTo(100L), "monotonic — must not regress");
    }

    [Test]
    public void DurableAckAdvancesDurableSeqTxn()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);

        var reply = WebSocketResponse.DurableAck("trades", 42);
        var bytes = new byte[reply.SerializedSize()];
        reply.WriteTo(bytes);
        fake.EnqueueInboundBinary(bytes);

        Assert.That(WaitFor(() => queue.GetDurableSeqTxn("trades") == 42, TimeSpan.FromSeconds(2)),
                    Is.True);
        // Durable acks don't bump TotalAcks (the queue counts STATUS_OK responses only).
        Assert.That(queue.TotalAcks, Is.EqualTo(0L));
    }

    [Test]
    public void ErrorResponseLatchesFailureAndNotifiesListener()
    {
        using var fake = new FakeWebSocketChannel();
        Exception? caught = null;
        using var queue = new WebSocketSendQueue(
            fake,
            failureListener: ex => caught = ex);

        var reply = WebSocketResponse.Error(0, WebSocketResponse.STATUS_PARSE_ERROR, "bad request");
        var bytes = new byte[reply.SerializedSize()];
        reply.WriteTo(bytes);
        fake.EnqueueInboundBinary(bytes);

        // LatchFailure sets LastError then invokes the listener — wait for both so we
        // don't observe the in-between state under load.
        Assert.That(WaitFor(() => queue.LastError is not null && caught is not null,
                            TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(queue.LastError, Is.TypeOf<IngressError>());
        Assert.That(queue.LastError!.Message, Does.Contain("PARSE_ERROR"));
        Assert.That(caught, Is.SameAs(queue.LastError));
    }

    [Test]
    public void EnqueueAfterFailureThrows()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);
        var reply = WebSocketResponse.Error(0, WebSocketResponse.STATUS_INTERNAL_ERROR, "boom");
        var bytes = new byte[reply.SerializedSize()];
        reply.WriteTo(bytes);
        fake.EnqueueInboundBinary(bytes);

        Assert.That(WaitFor(() => queue.LastError is not null, TimeSpan.FromSeconds(2)), Is.True);

        using var buffer = new MicrobatchBuffer(8);
        buffer.WriteByte(1);
        buffer.IncrementRowCount();
        buffer.Seal();
        Assert.That(() => queue.Enqueue(buffer), Throws.TypeOf<IngressError>());
    }

    [Test]
    public void FlushReturnsImmediatelyWhenIdle()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        queue.Flush();
        sw.Stop();
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(100), "should return quickly when idle");
    }

    [Test]
    public void FlushBlocksUntilSendCompletes()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);
        using var buffer = new MicrobatchBuffer(64);
        buffer.WriteByte(0xAB);
        buffer.IncrementRowCount();
        buffer.Seal();
        queue.Enqueue(buffer);
        queue.Flush();
        Assert.That(queue.TotalBatchesSent, Is.EqualTo(1L));
        Assert.That(fake.SentFrames.Count, Is.EqualTo(1));
    }

    [Test]
    public void PingRoundTripsThroughEcho()
    {
        // The fake channel echoes any binary frame it receives back to the queue when
        // we call EnqueueInboundBinary with the same payload. The ping payload is the
        // 4-byte 0xFF marker the queue uses internally.
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);

        var pingTask = Task.Run(() => queue.Ping(TimeSpan.FromSeconds(2)));
        // Wait for the ping to be sent, then echo it back.
        Assert.That(WaitFor(() => fake.SentFrames.Count > 0, TimeSpan.FromSeconds(2)), Is.True);
        fake.EnqueueInboundBinary(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });
        Assert.That(pingTask.Wait(TimeSpan.FromSeconds(2)), Is.True);
    }

    [Test]
    public void PingTimesOutWithoutPong()
    {
        using var fake = new FakeWebSocketChannel();
        using var queue = new WebSocketSendQueue(fake);
        Assert.That(() => queue.Ping(TimeSpan.FromMilliseconds(100)),
                    Throws.TypeOf<IngressError>().With.Message.Contains("ping timeout"));
    }

    [Test]
    public void CloseStopsBackgroundTasks()
    {
        var fake = new FakeWebSocketChannel();
        var queue = new WebSocketSendQueue(fake);

        // Enqueue a batch + drain it.
        using var buffer = new MicrobatchBuffer(64);
        buffer.WriteByte(1);
        buffer.IncrementRowCount();
        buffer.Seal();
        queue.Enqueue(buffer);
        queue.Flush();

        queue.Close();
        // After close, IsRunning must be false.
        Assert.That(queue.IsRunning, Is.False);
        queue.Dispose();
        fake.Dispose();
    }

    private static bool WaitFor(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        while (Environment.TickCount64 < deadline)
        {
            if (condition()) return true;
            Thread.Sleep(10);
        }
        return condition();
    }
}
