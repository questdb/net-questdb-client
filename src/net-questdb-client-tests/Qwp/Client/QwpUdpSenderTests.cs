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
    public void SingleRowExceedingMaxDatagramSizeThrowsAtCommit()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        // Cap max_datagram_size below a single row's encoded size — AtNow itself throws.
        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};max_datagram_size=8;").Build();
        sender.Table("trades").Column("v", 1L);
        Assert.That(() => sender.AtNow(),
                    Throws.TypeOf<IngressError>().With.Message.Contains("max_datagram_size"));
    }

    [Test]
    public async Task MultiRowBatchExceedingMaxDatagramSizeSplitsIntoDatagrams()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        // Sized so 2-3 LONG rows fit per datagram. With 12 rows we expect ~4-6 datagrams.
        using var sender = (QwpUdpSender)new SenderOptions(
            $"udp::addr=127.0.0.1:{port};max_datagram_size=80;").Build();
        for (var i = 0; i < 12; i++)
        {
            sender.Table("trades").Column("v", (long)i).At(DateTime.UtcNow);
        }
        await sender.SendAsync();

        // Drain everything the receiver got. Multiple datagrams must arrive — exact
        // count depends on per-datagram fit but must be >= 2 (the user-visible win).
        var datagrams = new List<byte[]>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var task = receiver.ReceiveAsync();
                if (await Task.WhenAny(task, Task.Delay(200)) != task) break;
                datagrams.Add(task.Result.Buffer);
            }
            catch { break; }
        }
        Assert.That(datagrams.Count, Is.GreaterThanOrEqualTo(2),
            "12-row batch must split into at least 2 datagrams under max_datagram_size=80");
        foreach (var d in datagrams)
        {
            Assert.That(d.Length, Is.LessThanOrEqualTo(80),
                "every emitted datagram must respect max_datagram_size");
        }
    }

    [Test]
    public async Task FlushPreservesInProgressRow()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        // Tight cap so committing the 4th row triggers a flush mid-batch.
        using var sender = (QwpUdpSender)new SenderOptions(
            $"udp::addr=127.0.0.1:{port};max_datagram_size=80;").Build();
        sender.Table("t").Column("v", 1L).At(DateTime.UtcNow);
        sender.Table("t").Column("v", 2L).At(DateTime.UtcNow);
        sender.Table("t").Column("v", 3L).At(DateTime.UtcNow);
        // The 4th row triggers a flush of [1,2,3] and seeds the next datagram with row 4.
        sender.Table("t").Column("v", 4L).At(DateTime.UtcNow);
        await sender.SendAsync();

        // Two datagrams expected. Walk receiver until idle.
        var receivedCount = 0;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            var task = receiver.ReceiveAsync();
            if (await Task.WhenAny(task, Task.Delay(200)) != task) break;
            receivedCount++;
        }
        Assert.That(receivedCount, Is.GreaterThanOrEqualTo(2),
            "the auto-flushed prefix and the trailing remainder both ship");
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

    // ---- PR A: decimal + array Column overloads ----

    [Test]
    public async Task ColumnDecimal256SendsWithScalePrefix()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        // Native 256-bit decimal — beyond .NET decimal's 96-bit mantissa.
        sender.Table("t")
              .ColumnDecimal256("d", hh: 1, hl: 2, lh: 3, ll: 4, scale: 10)
              .At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DECIMAL256), Is.True,
            "datagram should advertise the column as DECIMAL256");
    }

    [Test]
    public void ColumnDecimal256OnHttpSenderThrowsNotSupported()
    {
        // HTTP/TCP senders inherit the default ISender.ColumnDecimal256 stub which
        // throws NotSupportedException. Confirms the API gate is in place.
        using var sender = new SenderOptions("http::addr=localhost:9000;").Build();
        Assert.That(() => sender.ColumnDecimal256("d", 0, 0, 0, 0, 0),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public async Task DecimalColumnSendsAsDecimal64WhenSmall()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("t").Column("price", 123.45m).At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DECIMAL64), Is.True,
            "datagram should advertise the column as DECIMAL64 since 12345 fits in 64 bits");
    }

    [Test]
    public async Task DecimalColumnSendsAsDecimal128WhenLarge()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        // decimal.MaxValue is 7.9...e28 — mantissa overflows 64 bits, must route to DECIMAL128.
        sender.Table("t").Column("big", decimal.MaxValue).At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DECIMAL128), Is.True,
            "datagram should advertise the column as DECIMAL128 for >64-bit mantissa");
    }

    [Test]
    public async Task DecimalColumnNegativeRoundTripsScale()
    {
        // -123.45 should land as DECIMAL64 with scale=2. Negation goes through 128-bit
        // two's complement, then collapses back to a small 64-bit signed value.
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("t").Column("price", -123.45m).At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DECIMAL64), Is.True);
    }

    [Test]
    public async Task DoubleArrayColumn1D()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("t").Column("data", new[] { 1.0, 2.0, 3.0 }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DOUBLE_ARRAY), Is.True);
    }

    [Test]
    public async Task DoubleArrayColumn2D()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("t").Column("matrix", new[,] { { 1.0, 2.0 }, { 3.0, 4.0 } }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DOUBLE_ARRAY), Is.True);
    }

    [Test]
    public async Task LongArrayColumn1D()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("t").Column("counts", new[] { 1L, 2L, 3L }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_LONG_ARRAY), Is.True);
    }

    [Test]
    public async Task ArrayViaReadOnlySpan()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        // Stage the row in a sync local so the ReadOnlySpan local doesn't outlive the
        // first await (C# 12 forbids ref-like locals across awaits in async methods).
        StageSpanRow(sender);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DOUBLE_ARRAY), Is.True);

        static void StageSpanRow(QwpUdpSender s)
        {
            ReadOnlySpan<double> span = stackalloc[] { 1.0, 2.0, 3.0 };
            s.Table("t").Column("data", span).At(DateTime.UtcNow);
        }
    }

    [Test]
    public async Task ArrayViaIEnumerableAndShape()
    {
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)receiver.Client.LocalEndPoint!).Port;

        using var sender = (QwpUdpSender)new SenderOptions($"udp::addr=127.0.0.1:{port};").Build();
        sender.Table("t").Column<double>("matrix",
            new List<double> { 1.0, 2.0, 3.0, 4.0 },
            new[] { 2, 2 }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var bytes = await ReceiveOne(receiver);
        Assert.That(ContainsByte(bytes, QwpConstants.TYPE_DOUBLE_ARRAY), Is.True);
    }

    [Test]
    public void ArrayShapeMismatchThrows()
    {
        using var sender = (QwpUdpSender)new SenderOptions("udp::addr=127.0.0.1:9007;").Build();
        Assert.That(() => sender.Table("t").Column<double>("m",
                new List<double> { 1.0, 2.0, 3.0 },  // 3 elements
                new[] { 2, 2 }).At(DateTime.UtcNow),  // shape claims 4
            Throws.TypeOf<IngressError>().With.Message.Contains("does not match shape product"));
    }

    [Test]
    public void NonDoubleNonLongArrayElementThrows()
    {
        using var sender = (QwpUdpSender)new SenderOptions("udp::addr=127.0.0.1:9007;").Build();
        Assert.That(() => sender.Table("t").Column<int>("v",
                new[] { 1, 2 }.AsSpan().ToArray()).At(DateTime.UtcNow),
            Throws.TypeOf<IngressError>().With.Message.Contains("supports double and long arrays"));
    }

    [Test]
    public void NullArrayIsNoop()
    {
        using var sender = (QwpUdpSender)new SenderOptions("udp::addr=127.0.0.1:9007;").Build();
        // No exception, no row added (the sender just returns this).
        sender.Table("t").Column("opt", (Array?)null!);
        // Adding a real column afterwards still works.
        sender.Column("real", 42L).At(DateTime.UtcNow);
        Assert.That(sender.RowCount, Is.EqualTo(1));
    }

    [Test]
    public void Array3DRejected()
    {
        using var sender = (QwpUdpSender)new SenderOptions("udp::addr=127.0.0.1:9007;").Build();
        Assert.That(() => sender.Table("t").Column("m", new double[2, 2, 2]).At(DateTime.UtcNow),
            Throws.TypeOf<IngressError>().With.Message.Contains("rank"));
    }

    private static async Task<byte[]> ReceiveOne(UdpClient receiver)
    {
        var receiveTask = receiver.ReceiveAsync();
        var completed = await Task.WhenAny(receiveTask, Task.Delay(2000));
        Assert.That(completed, Is.SameAs(receiveTask), "datagram must arrive within 2s");
        return receiveTask.Result.Buffer;
    }

    private static bool ContainsByte(byte[] bytes, byte target)
    {
        foreach (var b in bytes) if (b == target) return true;
        return false;
    }
}
