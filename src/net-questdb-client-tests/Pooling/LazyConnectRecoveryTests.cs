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

#if NET7_0_OR_GREATER

using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using dummy_http_server;

namespace net_questdb_client_tests.Pooling;

/// <summary>
///     End-to-end <c>lazy_connect=true</c> recovery lifecycle against an in-process QWP server whose
///     lifetime the test controls: start the client with the server down (writes buffer), bring the
///     server up (buffered writes drain), and restart the server mid-stream (the client reconnects to a
///     fresh instance on the same port and replays un-acked frames). Mirrors the Java client's
///     QuestDBServerRecoveryTest.
///     <para />
///     The server ACKs each frame with a per-connection 0-based wire sequence (a fresh instance restarts
///     at 0), matching the cursor engine's <c>fsnAtZero</c> mapping so a restarted server replays cleanly.
/// </summary>
[TestFixture]
public class LazyConnectRecoveryTests
{
    // Single reused pooled sender so both batches flow through the same reconnecting engine/ring, and
    // small reconnect backoffs so recovery is quick. auto_flush=off keeps flushing under test control.
    private static string ConfFor(int port) =>
        $"ws::addr=127.0.0.1:{port};lazy_connect=true;auto_flush=off;" +
        "sender_pool_min=1;sender_pool_max=1;" +
        "reconnect_initial_backoff_millis=50;reconnect_max_backoff_millis=200;" +
        "reconnect_max_duration_millis=60000;";

    [Test]
    public async Task StartClientBeforeServer_BufferedWriteDeliversOnceServerIsUp()
    {
        var port = FreeLoopbackPort();

        // The server is DOWN. lazy_connect makes Build() return promptly (ingest connects async) and the
        // default sender_pool_min=1 pre-warm does not block/throw on the missing server.
        using var client = QuestDBClient.Connect(ConfFor(port));

        // Buffer a write while the server is down: Send() flushes to the ring (non-blocking); nothing is
        // on the wire yet.
        using (var s = client.BorrowSender())
        {
            s.Table("recovery").Column("v", 1L).AtNow();
            s.Send();
        }

        // The server appears.
        await using var server = await StartAckingServerAsync(port);

        // Flush drains: the engine connects to the now-up server, replays the buffered frame and awaits
        // its ACK.
        Assert.That(client.Flush(TimeSpan.FromSeconds(30)), Is.True,
            "the write buffered while the server was down must drain once it is up");
        await WaitFor(() => server.ReceivedFrames.Count >= 1, 5000);
        Assert.That(server.ReceivedFrames.Count, Is.EqualTo(1),
            "the server received exactly the one buffered frame (no duplicate delivery)");
    }

    [Test]
    public async Task StartBeforeServerThenRestartMidStream_ClientBuffersReconnectsAndReplays()
    {
        var port = FreeLoopbackPort();

        // Start the client with NO server present (lazy_connect).
        using var client = QuestDBClient.Connect(ConfFor(port));

        // Buffer batch 1 while the server is still down.
        using (var s = client.BorrowSender())
        {
            s.Table("recovery").Column("v", 1L).AtNow();
            s.Send();
        }

        // Bring the server up: the buffered batch 1 drains.
        var server1 = await StartAckingServerAsync(port);
        Assert.That(client.Flush(TimeSpan.FromSeconds(30)), Is.True, "batch 1 drains once the server is up");
        await WaitFor(() => server1.ReceivedFrames.Count >= 1, 5000);
        Assert.That(server1.ReceivedFrames.Count, Is.EqualTo(1), "first server received exactly batch 1");

        // Restart the server in the middle: stop it, buffer a write while it is down, then bring a fresh
        // instance up on the SAME port.
        await server1.DisposeAsync();

        using (var s = client.BorrowSender())
        {
            s.Table("recovery").Column("v", 2L).AtNow();
            s.Send();
        }

        await using var server2 = await StartAckingServerAsync(port);

        // The engine reconnects to the fresh server and replays only the un-acked batch 2 (batch 1 was
        // already acked, so the cursor does not rewind past it).
        Assert.That(client.Flush(TimeSpan.FromSeconds(30)), Is.True,
            "batch 2 drains to the restarted server after reconnect");
        await WaitFor(() => server2.ReceivedFrames.Count >= 1, 5000);
        Assert.That(server2.ReceivedFrames.Count, Is.EqualTo(1),
            "the restarted server received exactly the replayed batch 2 (the acked batch 1 is not re-sent)");
    }

    // ---- helpers ----

    private static async Task<DummyQwpServer> StartAckingServerAsync(int port)
    {
        // Per-instance 0-based wire sequence. Each phase uses a fresh instance serving one stable
        // connection, so this equals the per-connection sequence the cursor engine's fsnAtZero expects.
        long nextWireSeq = 0;
        var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Port = port,
            FrameHandler = _ => BuildOkAck(Interlocked.Increment(ref nextWireSeq) - 1),
        });
        await server.StartAsync();
        return server;
    }

    private static byte[] BuildOkAck(long sequence)
    {
        var bytes = new byte[QwpConstants.OffsetTableCountInOkAck + 2];
        bytes[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(QwpConstants.OffsetTableCountInOkAck, 2), 0);
        return bytes;
    }

    // Bind :0 to grab a free loopback port, then release it so the client can be configured with the
    // address before the server exists and so a restarted server can re-bind it.
    private static int FreeLoopbackPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!predicate() && Environment.TickCount64 < deadline)
        {
            await Task.Delay(20);
        }
    }
}
#endif
