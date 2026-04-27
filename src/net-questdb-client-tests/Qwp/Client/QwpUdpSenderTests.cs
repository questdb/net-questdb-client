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

using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using NUnit.Framework;
using QuestDB;
using QuestDB.Qwp;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Round-trip + behavioural smoke tests for <see cref="QwpUdpSender"/> (PR 6b skeleton).
///     The full Java <c>QwpUdpSenderTest.java</c> is 2709 LoC and exercises far more types
///     than PR 6b ships; PR 6c will fill in the long tail (decimals, arrays, datagram
///     fragmentation, multicast). For now we verify the wire envelope matches what
///     <see cref="QwpWebSocketEncoder"/> emits for a single-table batch.
/// </summary>
[TestFixture]
public class QwpUdpSenderTests
{
    [Test]
    public void RequiresTableBeforeColumns()
    {
        using var sender = (QwpUdpSender)new SenderOptions("udp::addr=127.0.0.1:9007;").Build();
        Assert.That(() => sender.Column("v", 1L), Throws.TypeOf<IngressError>()
                                                        .With.Message.Contains("Table"));
    }

    [Test]
    public void TransactionsAreUnsupported()
    {
        using var sender = (QwpUdpSender)new SenderOptions("udp::addr=127.0.0.1:9007;").Build();
        Assert.That(() => sender.Transaction("x"), Throws.TypeOf<IngressError>());
        Assert.That(() => sender.Commit(), Throws.TypeOf<IngressError>());
        Assert.That(() => sender.Rollback(), Throws.TypeOf<IngressError>());
    }

    [Test]
    public async Task RoundTripDatagramReachesUdpReceiver()
    {
        // Bind a UDP socket on a free port, configure a sender to target it, send a
        // single batch + assert the receiver gets a QWP1-prefixed datagram.
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("trades")
              .Symbol("sym", "AAPL")
              .Column("price", 123.45)
              .Column("volume", 1000L)
              .At(DateTime.UtcNow);
        await sender.SendAsync();

        var receiveTask = receiver.ReceiveAsync();
        var completed = await Task.WhenAny(receiveTask, Task.Delay(2000));
        Assert.That(completed, Is.SameAs(receiveTask), "datagram must arrive within 2s");

        var bytes = receiveTask.Result.Buffer;
        Assert.That(bytes.Length, Is.GreaterThanOrEqualTo(QwpConstants.HEADER_SIZE));
        Assert.That(bytes[0], Is.EqualTo((byte)'Q'));
        Assert.That(bytes[1], Is.EqualTo((byte)'W'));
        Assert.That(bytes[2], Is.EqualTo((byte)'P'));
        Assert.That(bytes[3], Is.EqualTo((byte)'1'));
    }

    [Test]
    public void OversizeDatagramThrowsWhenMaxSet()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        // Cap max_datagram_size very low so even one row exceeds it.
        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};max_datagram_size=8;").Build();
        sender.Table("trades")
              .Column("v", 1L)
              .AtNow();
        Assert.That(() => sender.Send(),
                    Throws.TypeOf<IngressError>().With.Message.Contains("max_datagram_size"));
    }

    [Test]
    public void ResetsBetweenSends()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("t").Column("v", 1L).AtNow();
        sender.Send();
        Assert.That(sender.RowCount, Is.EqualTo(0));

        // Drain the receiver so the next send has a clean inbox.
        receiver.ReceiveAsync().Wait(500);

        sender.Table("t").Column("v", 2L).AtNow();
        Assert.That(sender.RowCount, Is.EqualTo(1));
    }
}
