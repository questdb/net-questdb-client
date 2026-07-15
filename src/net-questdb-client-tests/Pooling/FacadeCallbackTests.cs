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
using System.Collections.Concurrent;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Senders;
using QuestDB.Utils;
using dummy_http_server;

namespace net_questdb_client_tests.Pooling;

// Ingest error/connection callbacks exposed on the pooled QuestDBClient facade and propagated to
// every pooled ws sender. The per-slot connect-string re-parse in SenderPool drops programmatic
// delegates, so the facade re-applies them per created sender.
[TestFixture]
public class FacadeCallbackTests
{
    // A facade errorHandler receives the async auth-terminal SenderError from a pooled sender against
    // a 401-rejecting server. (Under Invariant B a plain connection error retries forever and never
    // surfaces — only a genuine terminal like auth does, so we drive a 401.)
    [Test]
    public async Task FacadeErrorHandler_ReceivesAsyncAuthTerminal()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            RejectUpgradeWith = System.Net.HttpStatusCode.Unauthorized,
        });
        await server.StartAsync();
        var port = server.Uri.Port;

        var fired = new ManualResetEventSlim();
        SenderError? captured = null;
        await using var client = QuestDBClient.Builder()
            .FromConfig($"ws::addr=127.0.0.1:{port};initial_connect_retry=async;auto_flush=off;sender_pool_min=1;query_pool_min=0;")
            .ErrorHandler(e => { captured = e; fired.Set(); })
            .Build();

        Assert.That(fired.Wait(TimeSpan.FromSeconds(5)), Is.True,
            "the facade errorHandler must receive the async auth terminal from the pooled sender");
        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.Exception, Is.InstanceOf<IngressError>());
        Assert.That(((IngressError)captured.Exception!).code, Is.EqualTo(ErrorCode.AuthError));
    }

    // A facade connectionListener observes the pooled sender's connection-state transitions.
    [Test]
    public async Task FacadeConnectionListener_ObservesConnect()
    {
        await using var server = await StartAckingServerAsync();
        var port = server.Uri.Port;

        var listener = new RecordingListener();
        await using var client = QuestDBClient.Builder()
            .FromConfig($"ws::addr=127.0.0.1:{port};auto_flush=off;sender_pool_min=1;query_pool_min=0;")
            .ConnectionListener(listener)
            .Build();

        await WaitFor(() => listener.Kinds.Contains(SenderConnectionEventKind.Connected), 5000);
        Assert.That(listener.Kinds, Does.Contain(SenderConnectionEventKind.Connected),
            "the facade connectionListener must observe the pooled sender connecting");
    }

    // Callbacks reach every pooled slot, not just the first — guards the per-slot connect-string
    // re-parse regression that would silently drop programmatic delegates.
    [Test]
    public async Task FacadeCallbacks_PropagateToEveryPooledSender()
    {
        await using var server = await StartAckingServerAsync();
        var port = server.Uri.Port;

        const int min = 3;
        var listener = new RecordingListener();
        await using var client = QuestDBClient.Builder()
            .FromConfig($"ws::addr=127.0.0.1:{port};auto_flush=off;sender_pool_min={min};sender_pool_max={min};query_pool_min=0;")
            .ConnectionListener(listener)
            .Build();

        // Every one of the `min` pre-warmed senders must fire a Connected event through the listener.
        await WaitFor(() => listener.CountOf(SenderConnectionEventKind.Connected) >= min, 5000);
        Assert.That(listener.CountOf(SenderConnectionEventKind.Connected), Is.GreaterThanOrEqualTo(min),
            $"every one of the {min} pooled senders must reach the shared connectionListener");
    }

    // A facade drainerListener observes a pooled ws+sf sender adopting and draining a crashed sibling's
    // orphan slot (drain_orphans=on): SlotAdopted then DrainCompleted must both reach the shared listener.
    [Test]
    public async Task FacadeDrainerListener_ReceivesDrainerEvents()
    {
        var root = Path.Combine(Path.GetTempPath(), "qwp-facade-drain-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            // Seed an out-of-family orphan slot (not "default-{i}", the pool's managed family) with one
            // un-acked frame left behind by a "crashed" sibling. Dispose the ring so the segment file is
            // free for the orphan scanner to adopt.
            var orphanSlot = Path.Combine(root, "crashed-sibling");
            using (var ring = QwpSegmentRing.Open(orphanSlot, segmentCapacity: 4096))
            {
                Assert.That(ring.TryAppend(new byte[] { 1 }), Is.True);
            }

            await using var server = await StartAckingServerAsync();
            var port = server.Uri.Port;

            var listener = new RecordingDrainerListener();
            await using var client = QuestDBClient.Builder()
                .FromConfig(
                    $"ws::addr=127.0.0.1:{port};sf_dir={root};drain_orphans=on;auto_flush=off;" +
                    "sender_pool_min=1;sender_pool_max=1;query_pool_min=0;")
                .DrainerListener(listener)
                .Build();

            await WaitFor(() => listener.Contains(BackgroundDrainerEventKind.DrainCompleted), 10_000);

            Assert.That(listener.Contains(BackgroundDrainerEventKind.SlotAdopted), Is.True,
                "the facade drainerListener must observe the orphan slot being adopted");
            Assert.That(listener.Contains(BackgroundDrainerEventKind.DrainCompleted), Is.True,
                "the facade drainerListener must observe the orphan slot draining to completion");
            Assert.That(listener.LastSlotDirectory, Does.EndWith("crashed-sibling"),
                "the event must carry the adopted sibling's slot directory");
            Assert.That(Directory.GetFiles(orphanSlot, "sf-*.sfa"), Is.Empty,
                "the drained orphan's segment files must be unlinked");
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    // ---- helpers ----

    private sealed class RecordingDrainerListener : IBackgroundDrainerListener
    {
        private readonly ConcurrentQueue<BackgroundDrainerEventKind> _kinds = new();
        private volatile string? _lastSlotDirectory;
        public string? LastSlotDirectory => _lastSlotDirectory;
        public bool Contains(BackgroundDrainerEventKind kind) => _kinds.Contains(kind);

        public void OnEvent(BackgroundDrainerEvent evt)
        {
            _lastSlotDirectory = evt.SlotDirectory;
            _kinds.Enqueue(evt.Kind);
        }
    }

    private sealed class RecordingListener : ISenderConnectionListener
    {
        private readonly ConcurrentQueue<SenderConnectionEventKind> _kinds = new();
        public IReadOnlyCollection<SenderConnectionEventKind> Kinds => _kinds.ToArray();
        public int CountOf(SenderConnectionEventKind kind) => _kinds.Count(k => k == kind);
        public void OnEvent(SenderConnectionEvent evt) => _kinds.Enqueue(evt.Kind);
    }

    private static async Task<DummyQwpServer> StartAckingServerAsync()
    {
        long nextWireSeq = 0;
        var server = new DummyQwpServer(new DummyQwpServerOptions
        {
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

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!predicate() && Environment.TickCount64 < deadline)
        {
            await Task.Delay(25);
        }
    }
}

#endif
