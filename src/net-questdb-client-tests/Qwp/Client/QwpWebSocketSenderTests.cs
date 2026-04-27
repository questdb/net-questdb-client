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
        private readonly CancellationTokenSource _cts = new();
        private Task? _acceptTask;
        private long _ackSequence;

        private EchoWebSocketServer(HttpListener listener) => _listener = listener;

        public int Port { get; private set; }

        public static async Task<EchoWebSocketServer> StartAsync()
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

            var server = new EchoWebSocketServer(listener) { Port = port };
            server._acceptTask = Task.Run(server.AcceptLoop);
            await Task.Yield();
            return server;
        }

        public Task<byte[]> WaitForFirstFramePrefixAsync(TimeSpan timeout) =>
            _firstFramePrefix.Task.WaitAsync(timeout);

        private async Task AcceptLoop()
        {
            try
            {
                var ctx = await _listener.GetContextAsync().ConfigureAwait(false);
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
                    if (result.Count >= 4 && !_firstFramePrefix.Task.IsCompleted)
                    {
                        _firstFramePrefix.TrySetResult(new[] { buf[0], buf[1], buf[2], buf[3] });
                    }
                    // Reply with a STATUS_OK frame so the sender's InFlightWindow drains.
                    // Sequence numbers issued by the queue start at 0 (per WebSocketSendQueue
                    // SendLoopAsync's Interlocked.Increment then -1).
                    var ack = WebSocketResponse.Success(_ackSequence++);
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
