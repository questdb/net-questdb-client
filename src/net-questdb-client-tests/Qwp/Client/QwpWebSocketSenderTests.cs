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
using System.Net.WebSockets;
using System.Threading.Tasks;
using NUnit.Framework;
using QuestDB;
using QuestDB.Qwp;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Minimal smoke tests for <see cref="QwpWebSocketSender"/> (PR 7 sync subset).
///     Spins up a tiny <see cref="HttpListener"/>-based echo WebSocket server per test
///     so the construct + send path is exercised end-to-end. The full Java
///     <c>QwpWebSocketSenderTest.java</c> + <c>QwpWebSocketSenderStateTest.java</c>
///     coverage (1209 LoC combined) lands with PR 8's QwpTestWebSocketServer
///     infrastructure.
/// </summary>
[TestFixture]
public class QwpWebSocketSenderTests
{
    [Test]
    public async Task BuildsAgainstLocalhostServer()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};");
        using var sender = options.Build();
        Assert.That(sender, Is.TypeOf<QwpWebSocketSender>());
    }

    [Test]
    public async Task SendsQwp1FrameAndQueueObservesIt()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("trades")
              .Symbol("sym", "AAPL")
              .Column("price", 123.45)
              .At(DateTime.UtcNow);
        await sender.SendAsync();

        // The server records the first frame's first 4 bytes — must be QWP1 magic.
        var prefix = await server.WaitForFirstFramePrefixAsync(TimeSpan.FromSeconds(2));
        Assert.That(prefix, Is.EqualTo(new byte[] { (byte)'Q', (byte)'W', (byte)'P', (byte)'1' }));
    }

    [Test]
    public async Task TransactionsAreUnsupported()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};");
        using var sender = options.Build();
        Assert.That(() => sender.Transaction("x"), Throws.TypeOf<QuestDB.Utils.IngressError>());
    }

    // ---- PR 8 — async pipelining + watermark accessors + durable-ack header ----

    [Test]
    public async Task AsyncModeAcceptsMultipleSendsBeforeAcksDrain()
    {
        // Window=4 lets up to 4 batches sit in-flight. Send 4 in rapid succession
        // without waiting for individual acks; AwaitPendingAcks at the end drains.
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=4;");
        using var sender = (QwpWebSocketSender)options.Build();

        for (var i = 0; i < 4; i++)
        {
            sender.Table("trades").Column("v", (long)i).At(DateTime.UtcNow);
            await sender.SendAsync();
        }
        sender.AwaitPendingAcks();
        Assert.That(server.FrameCount, Is.GreaterThanOrEqualTo(4));
    }

    [Test]
    public async Task PingRoundTripsAgainstEchoServer()
    {
        // The echo server replies to any binary frame with a STATUS_OK ack. The
        // sender's ping payload is the 4-byte 0xFF marker; the queue treats any
        // byte-for-byte echo of that payload as a pong. Configure the server to
        // echo the exact bytes back instead of sending an ack — this verifies the
        // ping round-trip path. We use a server flag for this.
        await using var server = await EchoWebSocketServer.StartAsync(echoExactly: true);
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();
        // Ping with a generous timeout; the echo server returns the payload immediately.
        Assert.That(() => sender.Ping(TimeSpan.FromSeconds(2)), Throws.Nothing);
    }

    [Test]
    public async Task GetHighestAckedSeqTxnReflectsServerOkFrame()
    {
        await using var server = await EchoWebSocketServer.StartAsync(seqTxnPerTable: 100);
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("trades").Column("v", 1L).At(DateTime.UtcNow);
        await sender.SendAsync();
        sender.AwaitPendingAcks();

        Assert.That(WaitFor(() => sender.GetHighestAckedSeqTxn("trades") == 100,
                            TimeSpan.FromSeconds(2)), Is.True);
    }

    [Test]
    public async Task DurableAckHeaderIsSetWhenRequested()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = new QwpWebSocketSender(options, requestDurableAck: true);
        Assert.That(server.UpgradeHeaderDurableAck, Is.EqualTo("true"));
    }

    // ---- PR A: decimal + array Column overloads ----

    [Test]
    public async Task DecimalColumnRoutesToDecimal64WhenSmall()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("trades").Column("price", 123.45m).At(DateTime.UtcNow);
        await sender.SendAsync();

        var frame = await server.WaitForFirstFrameAsync(TimeSpan.FromSeconds(2));
        Assert.That(ContainsByte(frame, QwpConstants.TYPE_DECIMAL64), Is.True);
    }

    [Test]
    public async Task DecimalColumnRoutesToDecimal128WhenLarge()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("t").Column("big", decimal.MaxValue).At(DateTime.UtcNow);
        await sender.SendAsync();

        var frame = await server.WaitForFirstFrameAsync(TimeSpan.FromSeconds(2));
        Assert.That(ContainsByte(frame, QwpConstants.TYPE_DECIMAL128), Is.True);
    }

    [Test]
    public async Task DoubleArrayColumn1D()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("t").Column("data", new[] { 1.0, 2.0, 3.0 }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var frame = await server.WaitForFirstFrameAsync(TimeSpan.FromSeconds(2));
        Assert.That(ContainsByte(frame, QwpConstants.TYPE_DOUBLE_ARRAY), Is.True);
    }

    [Test]
    public async Task DoubleArrayColumn2D()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("t").Column("matrix", new[,] { { 1.0, 2.0 }, { 3.0, 4.0 } }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var frame = await server.WaitForFirstFrameAsync(TimeSpan.FromSeconds(2));
        Assert.That(ContainsByte(frame, QwpConstants.TYPE_DOUBLE_ARRAY), Is.True);
    }

    [Test]
    public async Task LongArrayColumn1D()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("t").Column("counts", new[] { 1L, 2L, 3L }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var frame = await server.WaitForFirstFrameAsync(TimeSpan.FromSeconds(2));
        Assert.That(ContainsByte(frame, QwpConstants.TYPE_LONG_ARRAY), Is.True);
    }

    [Test]
    public async Task ArrayViaIEnumerableAndShape()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};in_flight_window=1;");
        using var sender = (QwpWebSocketSender)options.Build();

        sender.Table("t").Column<double>("matrix",
            new List<double> { 1.0, 2.0, 3.0, 4.0 }, new[] { 2, 2 }).At(DateTime.UtcNow);
        await sender.SendAsync();

        var frame = await server.WaitForFirstFrameAsync(TimeSpan.FromSeconds(2));
        Assert.That(ContainsByte(frame, QwpConstants.TYPE_DOUBLE_ARRAY), Is.True);
    }

    [Test]
    public async Task NonDoubleNonLongArrayElementThrows()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};");
        using var sender = (QwpWebSocketSender)options.Build();
        Assert.That(() => sender.Table("t").Column<int>("v", new[] { 1, 2 }, new[] { 2 }).At(DateTime.UtcNow),
            Throws.TypeOf<IngressError>().With.Message.Contains("supports double and long arrays"));
    }

    [Test]
    public async Task NullArrayIsNoop()
    {
        await using var server = await EchoWebSocketServer.StartAsync();
        var options = new SenderOptions($"ws::addr=127.0.0.1:{server.Port};");
        using var sender = (QwpWebSocketSender)options.Build();
        // Adding a null array column followed by a real column should leave the row valid.
        sender.Table("t").Column("opt", (Array?)null!).Column("real", 1L).At(DateTime.UtcNow);
        await sender.SendAsync();
        var frame = await server.WaitForFirstFrameAsync(TimeSpan.FromSeconds(2));
        Assert.That(ContainsByte(frame, QwpConstants.TYPE_LONG), Is.True);
    }

    private static bool ContainsByte(byte[] bytes, byte target)
    {
        foreach (var b in bytes) if (b == target) return true;
        return false;
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

    /// <summary>
    ///     Minimal HttpListener-backed WebSocket server used by the smoke tests above.
    ///     Accepts one connection, records incoming frames, and stays alive until the
    ///     test disposes it. Replaces a separate test harness file for now; a richer
    ///     QwpTestWebSocketServer lands with PR 8 ack-flow tests.
    /// </summary>
    private sealed class EchoWebSocketServer : IAsyncDisposable
    {
        private readonly HttpListener _listener;
        private readonly TaskCompletionSource<byte[]> _firstFramePrefix = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<byte[]> _firstFrameFull = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenSource _cts = new();
        private Task? _acceptTask;
        private long _ackSequence;
        private bool _echoExactly;
        private long _seqTxnPerTable;
        private int _frameCount;

        private EchoWebSocketServer(HttpListener listener) => _listener = listener;

        public int Port { get; private set; }

        public int FrameCount => System.Threading.Volatile.Read(ref _frameCount);

        /// <summary>Captures the X-QWP-Request-Durable-Ack upgrade header from the client.</summary>
        public string? UpgradeHeaderDurableAck { get; private set; }

        public static async Task<EchoWebSocketServer> StartAsync(bool echoExactly = false, long seqTxnPerTable = 0)
        {
            // Bind to an ephemeral port via the kernel by starting at a free socket.
            using var probe = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork,
                System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            var port = ((IPEndPoint)probe.LocalEndPoint!).Port;
            probe.Close();

            var listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            listener.Start();

            var server = new EchoWebSocketServer(listener)
            {
                Port = port,
                _echoExactly = echoExactly,
                _seqTxnPerTable = seqTxnPerTable,
            };
            server._acceptTask = Task.Run(server.AcceptLoop);
            await Task.Yield();
            return server;
        }

        public Task<byte[]> WaitForFirstFramePrefixAsync(TimeSpan timeout) =>
            _firstFramePrefix.Task.WaitAsync(timeout);

        /// <summary>Returns the full bytes of the first frame received (not just the 4-byte prefix).</summary>
        public Task<byte[]> WaitForFirstFrameAsync(TimeSpan timeout) =>
            _firstFrameFull.Task.WaitAsync(timeout);

        private async Task AcceptLoop()
        {
            try
            {
                var ctx = await _listener.GetContextAsync().ConfigureAwait(false);
                UpgradeHeaderDurableAck = ctx.Request.Headers["X-QWP-Request-Durable-Ack"];
                var wsCtx = await ctx.AcceptWebSocketAsync(subProtocol: null).ConfigureAwait(false);
                using var ws = wsCtx.WebSocket;

                var buf = new byte[16 * 1024];
                while (!_cts.IsCancellationRequested && ws.State == WebSocketState.Open)
                {
                    WebSocketReceiveResult result;
                    try
                    {
                        result = await ws.ReceiveAsync(new ArraySegment<byte>(buf), _cts.Token).ConfigureAwait(false);
                    }
                    catch { break; }

                    if (result.MessageType == WebSocketMessageType.Close) break;
                    System.Threading.Interlocked.Increment(ref _frameCount);
                    if (result.Count >= 4 && !_firstFramePrefix.Task.IsCompleted)
                    {
                        _firstFramePrefix.TrySetResult(new[] { buf[0], buf[1], buf[2], buf[3] });
                    }
                    if (!_firstFrameFull.Task.IsCompleted)
                    {
                        var copy = new byte[result.Count];
                        Array.Copy(buf, copy, result.Count);
                        _firstFrameFull.TrySetResult(copy);
                    }

                    if (_echoExactly)
                    {
                        // Echo path: send back the exact incoming bytes (used by ping tests).
                        var echo = new byte[result.Count];
                        Array.Copy(buf, echo, result.Count);
                        await ws.SendAsync(echo, WebSocketMessageType.Binary,
                                           endOfMessage: true, _cts.Token).ConfigureAwait(false);
                        continue;
                    }

                    // Reply with a STATUS_OK frame so the sender's InFlightWindow drains.
                    var ack = WebSocketResponse.Success(_ackSequence++);
                    if (_seqTxnPerTable > 0) ack.AddTableEntry("trades", _seqTxnPerTable);
                    var ackBuf = new byte[ack.SerializedSize()];
                    ack.WriteTo(ackBuf);
                    await ws.SendAsync(ackBuf, WebSocketMessageType.Binary,
                                       endOfMessage: true, _cts.Token).ConfigureAwait(false);
                }
            }
            catch { /* shutdown */ }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            try { _listener.Stop(); } catch { }
            try { _listener.Close(); } catch { }
            if (_acceptTask is not null)
            {
                try { await _acceptTask.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
            }
            _cts.Dispose();
            _firstFramePrefix.TrySetCanceled();
        }
    }
}
