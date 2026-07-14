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

using System.Buffers.Binary;
using System.Threading.Channels;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpCursorSendEngineTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-engine-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); }
            catch { /* mmap on Windows can hold the file briefly */ }
        }
    }

    [Test]
    public void Constructor_NullArgs_Throws()
    {
        var slotLock = QwpSlotLock.Acquire(Path.Combine(_root, "s"));
        try
        {
            var ring = QwpSegmentRing.Open(slotLock.SlotDirectory, segmentCapacity: 4096);
            var policy = new QwpReconnectPolicy(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(1));

            // slotLock=null is permitted (drain mode — pool owns the lock externally).
            Assert.Throws<ArgumentNullException>(() =>
                new QwpCursorSendEngine(slotLock, null!, () => null!, policy, TimeSpan.FromSeconds(5), InitialConnectMode.off));
            Assert.Throws<ArgumentNullException>(() =>
                new QwpCursorSendEngine(slotLock, ring, null!, policy, TimeSpan.FromSeconds(5), InitialConnectMode.off));
            Assert.Throws<ArgumentNullException>(() =>
                new QwpCursorSendEngine(slotLock, ring, () => null!, null!, TimeSpan.FromSeconds(5), InitialConnectMode.off));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new QwpCursorSendEngine(slotLock, ring, () => null!, policy, TimeSpan.Zero, InitialConnectMode.off));

            ring.Dispose();
        }
        finally
        {
            slotLock.Dispose();
        }
    }

    [Test]
    public async Task AppendAndDrain_HappyPath_AllFramesAcked()
    {
        var stubs = new List<StubTransport>();
        using var engine = NewEngine(out var slotDir, factory: () =>
        {
            var s = new StubTransport();
            stubs.Add(s);
            return s;
        });
        engine.Start();

        engine.AppendBlocking(new byte[] { 1 });
        engine.AppendBlocking(new byte[] { 2 });
        engine.AppendBlocking(new byte[] { 3 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(engine.AckedFsn, Is.EqualTo(3L));
        Assert.That(engine.NextFsn, Is.EqualTo(3L));
        Assert.That(stubs, Has.Count.EqualTo(1));
        Assert.That(stubs[0].Sent.Select(b => b[0]).ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task AppendAsync_HappyPath_FrameLandsOnWire()
    {
        var stubs = new List<StubTransport>();
        using var engine = NewEngine(out _, factory: () =>
        {
            var s = new StubTransport();
            stubs.Add(s);
            return s;
        });
        engine.Start();

        await engine.AppendAsync(new byte[] { 1, 2, 3 });
        await engine.AppendAsync(new byte[] { 4 });
        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(engine.AckedFsn, Is.EqualTo(2L));
        Assert.That(stubs[0].Sent.Select(b => b[0]).ToArray(), Is.EqualTo(new byte[] { 1, 4 }));
    }

    [Test]
    public async Task AppendAsync_DeadlineExpired_Throws()
    {
        using var sendGate = new SemaphoreSlim(0, int.MaxValue);
        using var engine = NewEngine(out _,
            segmentCapacity: QwpMmapSegment.HeaderSize + 64,
            maxTotalBytes: QwpMmapSegment.HeaderSize + 64,
            appendDeadline: TimeSpan.FromMilliseconds(100),
            factory: () => new StubTransport
            {
                OnSendGate = ct => sendGate.WaitAsync(ct)
            });
        engine.Start();

        await engine.AppendAsync(new byte[24]);
        await engine.AppendAsync(new byte[24]);

        var ex = Assert.ThrowsAsync<IngressError>(async () => await engine.AppendAsync(new byte[24]));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ServerFlushError));
        Assert.That(ex.Message, Does.Contain("sf_append_deadline"));

        sendGate.Release(8);
    }

    [Test]
    public async Task AppendAsync_DoesNotBlockCallingThread()
    {
        using var sendGate = new SemaphoreSlim(0, int.MaxValue);
        using var engine = NewEngine(out _,
            segmentCapacity: QwpMmapSegment.HeaderSize + 64,
            maxTotalBytes: QwpMmapSegment.HeaderSize + 64,
            appendDeadline: TimeSpan.FromSeconds(10),
            factory: () => new StubTransport
            {
                OnSendGate = ct => sendGate.WaitAsync(ct)
            });
        engine.Start();

        await engine.AppendAsync(new byte[24]);
        await engine.AppendAsync(new byte[24]);

        var pending = engine.AppendAsync(new byte[24]);
        Assert.That(pending.IsCompleted, Is.False, "third append must be pending while ring is full");

        sendGate.Release(2);
        await pending.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        sendGate.Release(8);
    }

    [Test]
    public async Task FullDrain_OnDispose_UnlinksSegmentFiles()
    {
        var slotDir = Path.Combine(_root, "drain-cleanup");

        {
            using var engine = NewEngine(out _, slotDirectoryOverride: slotDir);
            engine.Start();
            engine.AppendBlocking(new byte[] { 1 });
            engine.AppendBlocking(new byte[] { 2 });
            await engine.FlushAsync(TimeSpan.FromSeconds(5));
            Assert.That(engine.AckedFsn, Is.EqualTo(engine.NextFsn));
        }

        var residual = Directory.GetFiles(slotDir, "sf-*.sfa");
        Assert.That(residual, Is.Empty, "fully-drained slot must have no segment files left after dispose");
    }

    [Test]
    public async Task PartialDrain_OnDispose_PreservesSegmentFiles()
    {
        var slotDir = Path.Combine(_root, "drain-partial");
        var sendGate = new SemaphoreSlim(0, int.MaxValue);

        using (var engine = NewEngine(out _,
                   slotDirectoryOverride: slotDir,
                   factory: () => new StubTransport { OnSendGate = ct => sendGate.WaitAsync(ct) }))
        {
            engine.Start();
            engine.AppendBlocking(new byte[] { 1 });
            engine.AppendBlocking(new byte[] { 2 });
            await Task.Delay(100);
        }

        var residual = Directory.GetFiles(slotDir, "sf-*.sfa");
        Assert.That(residual, Is.Not.Empty, "un-acked data must survive engine dispose");
        sendGate.Release(8);
    }

    [Test]
    public async Task Recovery_ReopenSlotWithExistingSegments_ReplaysFromOldestFsn()
    {
        var slotDir = Path.Combine(_root, "recovery");

        var sendGate = new SemaphoreSlim(0, int.MaxValue);
        using (var first = NewEngine(out _,
                   slotDirectoryOverride: slotDir,
                   factory: () => new StubTransport { OnSendGate = ct => sendGate.WaitAsync(ct) }))
        {
            first.Start();
            first.AppendBlocking(new byte[] { 10 });
            first.AppendBlocking(new byte[] { 20 });
            first.AppendBlocking(new byte[] { 30 });
            await Task.Delay(100);
        }
        sendGate.Release(int.MaxValue / 2);

        var stubs = new List<StubTransport>();
        using (var second = NewEngine(out _,
                   slotDirectoryOverride: slotDir,
                   factory: () => { var s = new StubTransport(); stubs.Add(s); return s; }))
        {
            second.Start();
            await second.FlushAsync(TimeSpan.FromSeconds(5));
            Assert.That(second.AckedFsn, Is.EqualTo(3L), "all 3 recovered envelopes must be acked");
        }

        Assert.That(Directory.GetFiles(slotDir, "sf-*.sfa"), Is.Empty);

        var sentBytes = stubs.SelectMany(s => s.Sent).Select(f => f[^1]).ToHashSet();
        Assert.That(sentBytes.IsSupersetOf(new byte[] { 10, 20, 30 }), "recovered payloads must reach the wire");
    }

    [Test]
    public async Task AwaitAckedFsnAsync_TargetReached_ReturnsTrue()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport());
        engine.Start();

        engine.AppendBlocking(new byte[] { 1 });
        engine.AppendBlocking(new byte[] { 2 });

        var ok = await engine.AwaitAckedFsnAsync(targetFsn: 2L, TimeSpan.FromSeconds(5));
        Assert.That(ok, Is.True);
        Assert.That(engine.AckedFsn, Is.GreaterThanOrEqualTo(2L));
    }

    [Test]
    public async Task AwaitAckedFsnAsync_TargetUnreached_ReturnsFalseOnTimeout()
    {
        using var sendGate = new SemaphoreSlim(0, int.MaxValue);
        using var engine = NewEngine(out _,
            factory: () => new StubTransport { OnSendGate = ct => sendGate.WaitAsync(ct) });
        engine.Start();

        engine.AppendBlocking(new byte[] { 1 });

        var ok = await engine.AwaitAckedFsnAsync(targetFsn: 1L, TimeSpan.FromMilliseconds(200));
        Assert.That(ok, Is.False);

        sendGate.Release(8);
    }

    [Test]
    public void AwaitAckedFsnAsync_Terminal_Throws()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnConnect = _ => throw new IngressError(ErrorCode.AuthError, "401")
        });
        engine.Start();
        AssertEventually(() => engine.IsTerminallyFailed, "engine never marked terminal");

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await engine.AwaitAckedFsnAsync(targetFsn: 1L, TimeSpan.FromSeconds(1)));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.AuthError));
    }

    [Test]
    public void AuthError_OnConnect_MarksTerminalAndThrowsOnAppend()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnConnect = _ => throw new IngressError(ErrorCode.AuthError, "401 unauthorized")
        });
        engine.Start();

        AssertEventually(() => engine.IsTerminallyFailed, "engine never observed AuthError");
        var ex = Assert.Catch<IngressError>(() => engine.AppendBlocking(new byte[] { 1 }));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.AuthError));
    }

    [Test]
    public async Task ReceivePump_ServerLiesAboutAckSequence_ClampsToHighestStaged()
    {
        using var engine = NewEngine(out _, factory: () =>
            new StubTransport
            {
                OnSendAsync = _ => Task.FromResult(OkResponse(sequence: 1_000_000)),
            });
        engine.Start();

        engine.AppendBlocking(new byte[] { 1 });
        engine.AppendBlocking(new byte[] { 2 });
        engine.AppendBlocking(new byte[] { 3 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(engine.AckedFsn, Is.EqualTo(3L));
    }

    [Test]
    public void InitialConnectFailure_NoRetry_Terminal()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnConnect = _ => throw new IngressError(ErrorCode.SocketError, "connection refused")
        }, initialConnectMode: InitialConnectMode.off);
        engine.Start();

        AssertEventually(() => engine.IsTerminallyFailed, "engine never marked terminal after initial connect failure");
        Assert.That(engine.TerminalError, Is.InstanceOf<IngressError>());
    }

    [Test]
    public async Task InitialConnectFailure_WithRetry_RecoversAndDrains()
    {
        var attempts = 0;
        using var engine = NewEngine(out _, factory: () =>
        {
            attempts++;
            return new StubTransport
            {
                OnConnect = _ => attempts < 3
                    ? throw new IngressError(ErrorCode.SocketError, $"attempt {attempts} refused")
                    : Task.CompletedTask
            };
        }, initialConnectMode: InitialConnectMode.on);
        engine.Start();

        engine.AppendBlocking(new byte[] { 7 });
        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(attempts, Is.GreaterThanOrEqualTo(3));
        Assert.That(engine.AckedFsn, Is.EqualTo(1L));
    }

    [Test]
    public async Task WireFailureMidStream_ReconnectsAndReplays()
    {
        var stubs = new List<StubTransport>();
        var connectCount = 0;
        using var engine = NewEngine(out _, factory: () =>
        {
            connectCount++;
            var idx = connectCount;
            var s = new StubTransport();
            if (idx == 1)
            {
                // First stub: ack the first frame, then throw on the second send.
                var sent = 0;
                s.OnSend = _ =>
                {
                    sent++;
                    if (sent == 1) return OkResponse(0);
                    throw new IngressError(ErrorCode.SocketError, "broken pipe");
                };
            }

            stubs.Add(s);
            return s;
        });
        engine.Start();

        engine.AppendBlocking(new byte[] { 0 });
        engine.AppendBlocking(new byte[] { 1 });
        engine.AppendBlocking(new byte[] { 2 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(connectCount, Is.GreaterThanOrEqualTo(2), "reconnect did not occur");
        // Frame at FSN 0 was acked on the first connection; FSN 1 + 2 replayed on the second.
        Assert.That(engine.AckedFsn, Is.EqualTo(3L));

        var perTransport = stubs
            .Select(s => (IReadOnlyList<int>)s.Sent.Select(b => (int)b[0]).ToList())
            .ToList();
        AssertAtLeastOnceDelivery(perTransport, expectedCount: 3);
    }

    // ---- NACK policy v2: retriable rejections replay from ackedFsn, nothing is dropped ----

    [TestCase(QwpStatusCode.WriteError)]
    [TestCase(QwpStatusCode.InternalError)]
    [TestCase((QwpStatusCode)0xFE)]
    public async Task RetriableNack_ReplaysFrame_NotDropped(QwpStatusCode status)
    {
        var connectCount = 0;
        var stubs = new System.Collections.Concurrent.ConcurrentBag<StubTransport>();
        using var engine = NewEngine(out _, factory: () =>
        {
            var idx = Interlocked.Increment(ref connectCount);
            var s = idx == 1
                ? new StubTransport { OnSend = _ => ErrorResponse(status, sequence: 0, "retriable") }
                : new StubTransport();
            stubs.Add(s);
            return s;
        });
        engine.Start();
        engine.AppendBlocking(new byte[] { 42 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(connectCount, Is.GreaterThanOrEqualTo(2), "a retriable NACK must recycle the connection");
        Assert.That(engine.AckedFsn, Is.EqualTo(1L), "the replayed frame is acked — not dropped");
        Assert.That(engine.IsTerminallyFailed, Is.False, "a retriable NACK must not terminalise the engine");
        Assert.That(stubs.SelectMany(s => s.Sent).Any(b => b.Length > 0 && b[0] == 42), Is.True,
            "the rejected frame must reach the wire again on replay");
    }

    [Test]
    public void Nack_DoesNotAdvanceAckedFsn()
    {
        var connectCount = 0;
        using var engine = NewEngine(out _,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(5), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                Interlocked.Increment(ref connectCount);
                return new StubTransport { OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, 0, "still bad") };
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => Volatile.Read(ref connectCount) >= 2, "retriable NACK must recycle + replay");
        Assert.That(engine.AckedFsn, Is.EqualTo(0L), "a NACKed frame must never advance the ack watermark");
        Assert.That(engine.NextFsn, Is.EqualTo(1L), "the frame stays buffered for replay");
        Assert.That(engine.IsTerminallyFailed, Is.False);
    }

    [Test]
    public void SchemaMismatchNack_LatchesTerminal_DataPreservedOnDisk()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnSend = _ => ErrorResponse(QwpStatusCode.SchemaMismatch, 0, "type clash")
        });
        engine.Start();
        engine.AppendBlocking(new byte[] { 7 });

        AssertEventually(() => engine.IsTerminallyFailed, "schema mismatch is deterministic → terminal");
        Assert.That(engine.AckedFsn, Is.EqualTo(0L), "the rejected frame is not acked");
        Assert.That(engine.NextFsn, Is.EqualTo(1L), "the rejected frame stays in the ring (preserved on disk)");
        Assert.That(engine.TerminalError, Is.InstanceOf<LineSenderServerException>());
    }

    [Test]
    public async Task RetriableNack_ThenServerOk_AdvancesWatermarkOnce()
    {
        var connectCount = 0;
        using var engine = NewEngine(out _, factory: () =>
        {
            var idx = Interlocked.Increment(ref connectCount);
            return idx == 1
                ? new StubTransport { OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, 0, "retry me") }
                : new StubTransport();
        });
        engine.Start();
        engine.AppendBlocking(new byte[] { 5 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(engine.AckedFsn, Is.EqualTo(1L), "one frame acked exactly once after replay — no double advance");
        Assert.That(engine.NextFsn, Is.EqualTo(1L));
    }

    [Test]
    public async Task ConcurrentDrainerWalks_DoNotBlockForegroundConnect()
    {
        // The foreground engine and each drainer engine own independent transports/trackers, and the
        // reconnect loop never holds a shared lock across ConnectAsync. Stalled drainer connects must
        // not delay the foreground engine's own connect.
        using var drainerGate = new SemaphoreSlim(0);
        var drainers = new List<QwpCursorSendEngine>();
        try
        {
            for (var i = 0; i < 4; i++)
            {
                var d = NewEngine(out _, initialConnectMode: InitialConnectMode.async,
                    factory: () => new StubTransport { OnConnect = ct => drainerGate.WaitAsync(ct) });
                d.Start();
                d.AppendBlocking(new byte[] { 1 });
                drainers.Add(d);
            }

            using var foreground = NewEngine(out _, initialConnectMode: InitialConnectMode.async,
                factory: () => new StubTransport());
            var startTick = Environment.TickCount64;
            foreground.Start();
            foreground.AppendBlocking(new byte[] { 9 });

            // If a shared lock existed, the stalled drainer connects would block this indefinitely and
            // WaitAsync would time out. A generous bound distinguishes "not blocked" from "blocked".
            await foreground.FirstConnectTask.WaitAsync(TimeSpan.FromSeconds(10));
            var elapsedMs = Environment.TickCount64 - startTick;
            Assert.That(elapsedMs, Is.LessThan(5000),
                $"foreground connect must not be blocked by stalled drainer connects; took {elapsedMs}ms");
        }
        finally
        {
            drainerGate.Release(100);
            foreach (var d in drainers) d.Dispose();
        }
    }

    // ---- Poison-frame detector: consecutive same-head rejections/closes escalate to terminal ----

    [Test]
    public void PoisonFrame_EscalatesToTerminalAfterMaxRejections()
    {
        var connectCount = 0;
        using var engine = NewEngine(out _,
            maxFrameRejections: 3,
            poisonMinEscalationWindow: TimeSpan.Zero,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                Interlocked.Increment(ref connectCount);
                return new StubTransport { OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, 0, "poison") };
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => engine.IsTerminallyFailed, "same-frame poison must escalate to terminal", 5000);
        var lse = (LineSenderServerException)engine.TerminalError!;
        Assert.That(lse.Error.Category, Is.EqualTo(SenderErrorCategory.ProtocolViolation),
            "escalation must be a ProtocolViolation, not a data drop");
    }

    [Test]
    public async Task PoisonFrame_BelowThreshold_KeepsRetrying()
    {
        var connectCount = 0;
        using var engine = NewEngine(out _,
            maxFrameRejections: 5,
            poisonMinEscalationWindow: TimeSpan.Zero,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                var idx = Interlocked.Increment(ref connectCount);
                return idx <= 3
                    ? new StubTransport { OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, 0, "transient") }
                    : new StubTransport();
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));
        Assert.That(engine.IsTerminallyFailed, Is.False, "3 strikes < threshold(5) must not escalate");
        Assert.That(engine.AckedFsn, Is.EqualTo(1L));
    }

    [Test]
    public void PoisonFrame_DurableAckMode_OkLevelProgressResetsDetector()
    {
        // M1 regression / java-questdb-client parity. In durable-ack mode the trim watermark
        // (AckedFsn) only advances on DURABLE_ACK coverage, so a plain OK never moves it. The poison
        // detector must therefore measure progress against OK-level acceptance, not the trim
        // watermark: a server that OKs one more frame each connection before dropping (a replica-lag
        // window with a flapping wire) is making genuine progress and must NOT be escalated to a
        // poison terminal. Before the fix the detector keyed on AckedFsn (pinned at 0), so every
        // close struck the same head frame and escalated after maxFrameRejections closes — here that
        // would be by connection 2, terminalising a purely transient outage (Invariant B violation,
        // and RAM-mode data loss).
        var connectCount = 0;
        using var engine = NewEngine(out _,
            durableAckMode: true,
            maxFrameRejections: 2,
            poisonMinEscalationWindow: TimeSpan.Zero,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                var conn = Interlocked.Increment(ref connectCount);
                var seq = -1;
                return new StubTransport
                {
                    // Cumulative OK per shipped frame, each carrying an uncovered per-table entry so the
                    // trim watermark stays pinned at 0 (no DURABLE_ACK ever raises the table watermark).
                    OnSend = _ => OkResponseWithEntry(Interlocked.Increment(ref seq), "t", seqTxn: 1000),
                    // Connection N OKs N frames, then drops — one more frame of OK-level progress each
                    // cycle, so the suspect head advances and the detector resets on every reconnect.
                    FailReceiveAfterAcks = conn,
                };
            });
        engine.Start();
        for (var i = 0; i < 12; i++)
        {
            engine.AppendBlocking(new byte[] { (byte)i });
        }

        // Ride out several OK-then-close cycles. Without OK-level reset this escalates by connection 2;
        // with it the sender keeps reconnecting through the transient window.
        AssertEventually(() => Volatile.Read(ref connectCount) >= 5,
            "the sender must keep reconnecting through the OK-then-close window, not latch terminal", 5000);
        Assert.That(engine.IsTerminallyFailed, Is.False,
            "OK-level progress each cycle must reset the poison detector in durable-ack mode — a " +
            "progressing transient must never escalate to a terminal ProtocolViolation");
    }

    [Test]
    public void RetriableNack_WithConcurrentSendFault_StrikesOnce_NotTwice()
    {
        // Regression for the double-counted poison strike on NACK-before-close: the server NACKs the
        // head-of-line frame (recv pump strikes in HandleServerRejection and throws RetriableNack)
        // while the same in-flight send faults on the torn-down socket. If PickTerminalOrRetriableFault
        // lets the non-terminal send-side transport fault mask the RetriableNack, the reconnect catch
        // strikes the SAME head-of-line frame a second time (RegisterHeadOfLineStrike) — halving the
        // poison budget. With maxFrameRejections=2 the double strike escalates to terminal after a
        // single episode; a single strike (< 2) must replay and ack on reconnect instead.
        var connectCount = 0;
        using var engine = NewEngine(out _,
            maxFrameRejections: 2,
            poisonMinEscalationWindow: TimeSpan.Zero,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                var conn = Interlocked.Increment(ref connectCount);
                return new NackThenConcurrentSendFaultTransport(faultThisConnection: conn == 1);
            });
        engine.Start();
        // Two frames: frame0's send returns with a NACK (strike #1), frame1's send faults concurrently.
        engine.AppendBlocking(new byte[] { 0 });
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => engine.AckedFsn == 2L,
            "one NACK+concurrent-send-fault episode is a single strike (< threshold 2); the frames must " +
            "replay and ack on a fresh connection, not escalate to terminal from a double-counted strike",
            5000);
        Assert.That(engine.IsTerminallyFailed, Is.False,
            "a single retriable-NACK episode must not latch the sender terminal");
        Assert.That(connectCount, Is.GreaterThanOrEqualTo(2),
            "the poisoned connection must be recycled and the frame replayed on a fresh connection");
    }

    [Test]
    public void TerminalNack_WithConcurrentSendFault_LatchesTerminal_NotMaskedIntoRetry()
    {
        // Companion to RetriableNack_WithConcurrentSendFault_StrikesOnce_NotTwice, exercising the OTHER
        // branch of PickTerminalOrRetriableFault: its first tier prefers a terminal server fault over a
        // concurrent non-terminal send-side transport fault. Here the server NACKs the head-of-line
        // frame with SCHEMA_MISMATCH (recv pump throws HaltCarrier — a terminal, IsTerminalServerError
        // fault) while the same in-flight send faults on the torn-down socket. If the non-terminal
        // send-side SocketError masked the HaltCarrier, RunConnectionAsync would surface the socket
        // fault, the reconnect catch would route through the generic transient branch, and the sender
        // would silently retry a deterministic schema clash *forever* (Invariant B) instead of latching
        // terminal. The terminal fault must win: the sender stops on the SchemaMismatch and never
        // recycles the connection.
        var connectCount = 0;
        using var engine = NewEngine(out _,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                Interlocked.Increment(ref connectCount);
                return new NackThenConcurrentSendFaultTransport(
                    faultThisConnection: true, nackStatus: QwpStatusCode.SchemaMismatch);
            });
        engine.Start();
        // frame0's send returns normally (marks _sentOnCurrentConnection) and its ack is a terminal
        // SCHEMA_MISMATCH; frame1's send faults concurrently on the same torn-down socket.
        engine.AppendBlocking(new byte[] { 0 });
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => engine.IsTerminallyFailed,
            "a terminal SCHEMA_MISMATCH NACK must latch terminal even when a concurrent send-side socket " +
            "fault could mask it into the transient reconnect path", 5000);
        var lse = (LineSenderServerException)engine.TerminalError!;
        Assert.That(lse.Error.Category, Is.EqualTo(SenderErrorCategory.SchemaMismatch),
            "the terminal fault, not the concurrent send-side socket error, must be the surfaced cause");
        Assert.That(engine.AckedFsn, Is.EqualTo(0L), "the rejected frame is never acked");
        Assert.That(Volatile.Read(ref connectCount), Is.EqualTo(1),
            "a masked-then-retried terminal fault would recycle the connection; the terminal path must not");
    }

    [Test]
    public async Task OkAtOrBeyondSuspect_ResetsPoisonStrikes()
    {
        // Threshold is 2, but each NACK is on a distinct head-of-line frame separated by progress,
        // so strikes never accumulate to 2 — proving an ack past the suspect resets the detector.
        var attempts = new System.Collections.Concurrent.ConcurrentDictionary<byte, int>();
        using var engine = NewEngine(out _,
            segmentCapacity: 64 * 1024,
            maxFrameRejections: 2,
            poisonMinEscalationWindow: TimeSpan.Zero,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                var wireSeq = -1;
                return new StubTransport
                {
                    OnSend = frame =>
                    {
                        wireSeq++;
                        var payload = frame[0];
                        var n = attempts.AddOrUpdate(payload, 1, (_, c) => c + 1);
                        // NACK payloads 0 and 5 on their first delivery only (one strike each),
                        // then OK on replay — progress lands between the two strikes.
                        return (payload is 0 or 5) && n == 1
                            ? ErrorResponse(QwpStatusCode.WriteError, wireSeq, "flap")
                            : OkResponse(wireSeq);
                    }
                };
            });
        engine.Start();
        for (var i = 0; i < 10; i++) engine.AppendBlocking(new byte[] { (byte)i });

        await engine.FlushAsync(TimeSpan.FromSeconds(10));
        Assert.That(engine.IsTerminallyFailed, Is.False,
            "single NACKs separated by progress must reset strikes and never escalate");
        Assert.That(engine.AckedFsn, Is.EqualTo(10L));
    }

    [Test]
    public void PoisonDwell_HoldsEscalationUntilWindowElapses()
    {
        const int dwellMs = 500;
        using var engine = NewEngine(out _,
            maxFrameRejections: 2,
            poisonMinEscalationWindow: TimeSpan.FromMilliseconds(dwellMs),
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () => new StubTransport { OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, 0, "poison") });

        var startTick = Environment.TickCount64;
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        // Strikes reach the threshold within a few ms, but escalation is held until the dwell elapses.
        AssertEventually(() => engine.IsTerminallyFailed, "must escalate once the dwell window elapses", 3000);
        var elapsedMs = Environment.TickCount64 - startTick;
        Assert.That(elapsedMs, Is.GreaterThanOrEqualTo(dwellMs - 100),
            $"escalation must be held for ~the dwell window ({dwellMs}ms), not fire at the strike threshold; took {elapsedMs}ms");
    }

    [Test]
    public void PoisonDwell_Zero_EscalatesImmediatelyAtThreshold()
    {
        var connectCount = 0;
        using var engine = NewEngine(out _,
            maxFrameRejections: 3,
            poisonMinEscalationWindow: TimeSpan.Zero,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                Interlocked.Increment(ref connectCount);
                return new StubTransport { OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, 0, "poison") };
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => engine.IsTerminallyFailed, "dwell=0 escalates at the strike threshold", 3000);
        Assert.That(connectCount, Is.LessThanOrEqualTo(10),
            $"escalation must be prompt at the threshold, not after a storm; saw {connectCount}");
    }

    [Test]
    public void AcceptThenClose_RecycleIsPaced_NotStrikeStorm()
    {
        var connectCount = 0;
        using var engine = NewEngine(out _,
            maxFrameRejections: 4,
            poisonMinEscalationWindow: TimeSpan.FromMilliseconds(200),
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(30)),
            factory: () =>
            {
                Interlocked.Increment(ref connectCount);
                return new StubTransport { FailReceiveWith = new IngressError(ErrorCode.SocketError, "middlebox close") };
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => engine.IsTerminallyFailed,
            "accept-then-close on the same frame must escalate", 5000);
        Assert.That(connectCount, Is.LessThanOrEqualTo(30),
            $"accept-then-close recycles must be paced, not a storm; saw {connectCount}");
        Assert.That(((LineSenderServerException)engine.TerminalError!).Error.Category,
            Is.EqualTo(SenderErrorCategory.ProtocolViolation));
    }

    [Test]
    public void ReconnectBudgetExhausted_Terminal()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromMilliseconds(150));
        using var engine = NewEngine(out _,
            factory: () => new StubTransport
            {
                OnConnect = _ => throw new IngressError(ErrorCode.SocketError, "always refused")
            },
            policy: policy,
            initialConnectMode: InitialConnectMode.on);
        engine.Start();

        AssertEventually(() => engine.IsTerminallyFailed, "budget never exhausted", timeoutMs: 2000);
        Assert.That(engine.TerminalError, Is.InstanceOf<IngressError>());
    }

    // ---- Invariant B: a store-and-forward sender never terminates on a connection error ----

    [Test]
    public void AsyncInitialConnect_DeadEndpoint_RetriesForever_NoTerminal()
    {
        var attempts = 0;
        using var engine = NewEngine(out _,
            initialConnectMode: InitialConnectMode.async,
            // Short budget — async initial connect must ignore it and keep retrying.
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(20), TimeSpan.FromMilliseconds(100)),
            factory: () =>
            {
                Interlocked.Increment(ref attempts);
                return new StubTransport { OnConnect = _ => throw new IngressError(ErrorCode.SocketError, "dead") };
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 }); // buffers without blocking

        AssertEventually(() => Volatile.Read(ref attempts) >= 2, "async initial connect must keep retrying");
        var attemptsAtBudget = Volatile.Read(ref attempts);
        Thread.Sleep(300); // well past the 100ms budget
        Assert.That(engine.IsTerminallyFailed, Is.False,
            "async initial connect must never terminalise on the reconnect budget");
        Assert.That(Volatile.Read(ref attempts), Is.GreaterThan(attemptsAtBudget),
            "the loop must keep retrying past the old wall-clock budget");
    }

    [Test]
    public void MidStreamReconnect_NeverGivesUp_NoBudgetTerminal()
    {
        var connectCount = 0;
        using var engine = NewEngine(out _,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(20), TimeSpan.FromMilliseconds(100)),
            factory: () =>
            {
                var idx = Interlocked.Increment(ref connectCount);
                if (idx == 1)
                {
                    var sent = 0;
                    return new StubTransport
                    {
                        OnSend = _ =>
                        {
                            sent++;
                            return sent == 1
                                ? OkResponse(0)
                                : throw new IngressError(ErrorCode.SocketError, "drop");
                        }
                    };
                }

                // Every later connection refuses — a permanent mid-stream outage.
                return new StubTransport { OnConnect = _ => throw new IngressError(ErrorCode.SocketError, "outage") };
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 0 });
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => Volatile.Read(ref connectCount) >= 3, "mid-stream reconnect must keep retrying");
        var attemptsAtBudget = Volatile.Read(ref connectCount);
        Thread.Sleep(300); // well past the 100ms budget
        Assert.That(engine.IsTerminallyFailed, Is.False,
            "mid-stream reconnect must never terminalise on the reconnect budget (Invariant B)");
        Assert.That(Volatile.Read(ref connectCount), Is.GreaterThan(attemptsAtBudget),
            "the loop must keep retrying past the old wall-clock budget");
        Assert.That(engine.AckedFsn, Is.EqualTo(1L), "the acked prefix is preserved across the outage");
    }

    [Test]
    public void SfExhaustion_SurfacesAsAppendBackpressure_NotTerminal()
    {
        using var engine = NewEngine(out _,
            segmentCapacity: QwpMmapSegment.HeaderSize + 64,
            maxTotalBytes: QwpMmapSegment.HeaderSize + 64,
            appendDeadline: TimeSpan.FromMilliseconds(200),
            initialConnectMode: InitialConnectMode.async,
            factory: () => new StubTransport { OnConnect = _ => throw new IngressError(ErrorCode.SocketError, "down") });
        engine.Start();

        // The wire is down, so the tiny store fills and AppendBlocking eventually throws append
        // backpressure — but the engine must stay non-terminal (it keeps retrying under Invariant B).
        Assert.Throws<IngressError>(() =>
        {
            for (var i = 0; i < 200; i++)
            {
                engine.AppendBlocking(new byte[24]);
            }
        });
        Assert.That(engine.IsTerminallyFailed, Is.False,
            "SF exhaustion is producer backpressure, not a terminal sender");
    }

    [Test]
    public void ErrorHandler_FiresOnSyncInitialConnectExhaustion()
    {
        // Only the blocking SYNC initial connect (initial_connect_retry=on) is bounded by the
        // reconnect budget; its exhaustion is a genuine terminal that fires the error handler.
        // (Async initial connect retries forever under Invariant B — see AsyncInitialConnect_* tests.)
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromMilliseconds(150));
        SenderError? captured = null;
        var fired = new ManualResetEventSlim();
        using var engine = NewEngine(out _,
            factory: () => new StubTransport
            {
                OnConnect = _ => throw new IngressError(ErrorCode.SocketError, "always refused")
            },
            policy: policy,
            initialConnectMode: InitialConnectMode.on,
            errorHandler: e => { captured = e; fired.Set(); });
        engine.Start();

        Assert.That(fired.Wait(TimeSpan.FromSeconds(2)), Is.True, "error_handler never fired");
        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.IsInitialConnect, Is.True);
        Assert.That(captured.Exception, Is.InstanceOf<IngressError>());
    }

    [Test]
    public void ErrorHandler_FiresOnceForServerReject_AfterConnect()
    {
        var fireCount = 0;
        var fired = new ManualResetEventSlim();
        SenderError? captured = null;
        using var engine = NewEngine(out _,
            factory: () => new StubTransport
            {
                OnSend = _ => ErrorResponse(QwpStatusCode.ParseError, sequence: 0, "boom")
            },
            errorHandler: e =>
            {
                Interlocked.Increment(ref fireCount);
                captured = e;
                fired.Set();
            });
        engine.Start();
        engine.AppendBlocking(new byte[] { 9 });

        Assert.That(fired.Wait(TimeSpan.FromSeconds(2)), Is.True, "error_handler never fired");
        AssertEventually(() => engine.IsTerminallyFailed, "engine should mark terminal");
        Thread.Sleep(100);
        Assert.That(fireCount, Is.EqualTo(1));
        Assert.That(captured!.Category, Is.EqualTo(SenderErrorCategory.ParseError));
        Assert.That(captured.AppliedPolicy, Is.EqualTo(SenderErrorPolicy.Terminal));
        Assert.That(captured.IsInitialConnect, Is.False);
    }

    [Test]
    public void ErrorHandler_ThrowsAreSwallowed()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnSend = _ => ErrorResponse(QwpStatusCode.ParseError, sequence: 0, "boom")
        }, errorHandler: _ => throw new InvalidOperationException("user bug"));
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        AssertEventually(() => engine.IsTerminallyFailed, "engine should mark terminal");
        Assert.That(engine.TerminalError, Is.InstanceOf<LineSenderServerException>());
    }

    [Test]
    public void PolicyResolver_UpgradesRetriableToTerminal()
    {
        SenderError? captured = null;
        var fired = new ManualResetEventSlim();
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, sequence: 0, "write error")
        }, errorHandler: e => { captured = e; fired.Set(); },
           policyResolver: _ => SenderErrorPolicy.Terminal);
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        Assert.That(fired.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(captured!.AppliedPolicy, Is.EqualTo(SenderErrorPolicy.Terminal));
        AssertEventually(() => engine.IsTerminallyFailed, "terminal override must make engine terminal");
        Assert.That(engine.TerminalError, Is.InstanceOf<LineSenderServerException>());
    }

    [Test]
    public void Senderror_Fields_PopulatedFromResponse()
    {
        SenderError? captured = null;
        var fired = new ManualResetEventSlim();
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnSend = _ => ErrorResponse(QwpStatusCode.ParseError, sequence: 7, "boom")
        }, errorHandler: e => { captured = e; fired.Set(); });
        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        Assert.That(fired.Wait(TimeSpan.FromSeconds(2)), Is.True);
        Assert.That(captured!.ServerStatusByte, Is.EqualTo((int)QwpStatusCode.ParseError));
        Assert.That(captured.ServerMessage, Is.EqualTo("boom"));
        Assert.That(captured.MessageSequence, Is.EqualTo(7));
        Assert.That(captured.FromFsn, Is.EqualTo(0L));
        Assert.That(captured.ToFsn, Is.EqualTo(0L));
    }

    [Test]
    public void ErrorInbox_Overflow_BumpsDroppedCounter()
    {
        var release = new ManualResetEventSlim();
        var seen = 0;
        var sendCount = 0;
        var slotDir = Path.Combine(_root, "sender-" + Guid.NewGuid().ToString("N"));
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(40), TimeSpan.FromSeconds(2));
        using var dispatcher = new QwpSenderErrorDispatcher(_ =>
        {
            Interlocked.Increment(ref seen);
            release.Wait();
        }, capacity: 1);
        using var engine = new QwpCursorSendEngine(
            slotLock, ring,
            () => new StubTransport
            {
                OnSend = _ => ErrorResponse(QwpStatusCode.WriteError, sequence: sendCount++, "retriable")
            },
            policy,
            TimeSpan.FromSeconds(5),
            InitialConnectMode.off,
            errorDispatcher: dispatcher);
        engine.Start();
        for (var i = 0; i < 64; i++)
        {
            engine.AppendBlocking(new byte[] { (byte)i });
        }
        AssertEventually(() => Volatile.Read(ref seen) >= 1, "first notification never delivered");
        AssertEventually(() => dispatcher.DroppedNotifications > 0,
            "no dropped notifications recorded despite the parked handler");
        release.Set();
    }

    [Test]
    public void ServerErrorResponse_Terminal_OnTerminalCategory()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnSend = _ => ErrorResponse(QwpStatusCode.ParseError, sequence: 0, "boom")
        });
        engine.Start();

        engine.AppendBlocking(new byte[] { 9 });
        AssertEventually(() => engine.IsTerminallyFailed, "engine should mark terminal on Terminal-policy reject");
        Assert.That(engine.TerminalError, Is.InstanceOf<LineSenderServerException>());
        var lse = (LineSenderServerException)engine.TerminalError!;
        Assert.That(lse.Error.Category, Is.EqualTo(SenderErrorCategory.ParseError));
        Assert.That(lse.Error.AppliedPolicy, Is.EqualTo(SenderErrorPolicy.Terminal));
    }

    [Test]
    public async Task FlushAsync_TimesOut_WhenWireDoesNotDrain()
    {
        var gate = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnSendAsync = async _ => await gate.Task.ConfigureAwait(false)
        });
        engine.Start();

        engine.AppendBlocking(new byte[] { 1 });

        Assert.CatchAsync<TimeoutException>(
            async () => await engine.FlushAsync(TimeSpan.FromMilliseconds(150)));

        gate.TrySetResult(OkResponse(0));
        await engine.FlushAsync(TimeSpan.FromSeconds(5));
    }

    [Test]
    public void Backpressure_DeadlineExpired_Throws()
    {
        // 64-byte segment fits exactly two 24-byte payloads (envelope = 8 header + 24 body = 32). The
        // third append would rotate; with maxTotalBytes=64 the new segment can't be allocated → backpressure.
        using var sendGate = new SemaphoreSlim(0, int.MaxValue);
        using var engine = NewEngine(out _,
            segmentCapacity: QwpMmapSegment.HeaderSize + 64,
            maxTotalBytes: QwpMmapSegment.HeaderSize + 64,
            appendDeadline: TimeSpan.FromMilliseconds(100),
            factory: () => new StubTransport
            {
                OnSendGate = ct => sendGate.WaitAsync(ct)
            });
        engine.Start();

        engine.AppendBlocking(new byte[24]);
        engine.AppendBlocking(new byte[24]);

        var ex = Assert.Catch<IngressError>(() => engine.AppendBlocking(new byte[24]));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ServerFlushError));
        Assert.That(ex.Message, Does.Contain("sf_append_deadline"));

        sendGate.Release(8); // unblock loop so Dispose joins promptly
    }

    [Test]
    public async Task Backpressure_BlocksUntilTrim()
    {
        using var sendGate = new SemaphoreSlim(0, int.MaxValue);
        using var engine = NewEngine(out _,
            segmentCapacity: QwpMmapSegment.HeaderSize + 64,
            maxTotalBytes: QwpMmapSegment.HeaderSize + 64,
            appendDeadline: TimeSpan.FromSeconds(10),
            factory: () => new StubTransport
            {
                OnSendGate = ct => sendGate.WaitAsync(ct)
            });
        engine.Start();

        engine.AppendBlocking(new byte[24]);
        engine.AppendBlocking(new byte[24]);

        var producer = Task.Run(() => engine.AppendBlocking(new byte[24]));
        await Task.Delay(100);
        Assert.That(producer.IsCompleted, Is.False, "third append should still be blocked");

        // Release both queued sends — once FSN=1 is acked, seg0 (FSNs 0,1) is fully covered and
        // gets trimmed, freeing space for FSN=2.
        sendGate.Release(2);

        await producer.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(producer.IsCompletedSuccessfully, Is.True);
        sendGate.Release(8);
    }

    [Test]
    public async Task Dispose_WakesBlockedProducerWithDisposedException()
    {
        using var sendGate = new SemaphoreSlim(0, int.MaxValue);
        var engine = NewEngine(out _,
            segmentCapacity: QwpMmapSegment.HeaderSize + 64,
            maxTotalBytes: QwpMmapSegment.HeaderSize + 64,
            appendDeadline: TimeSpan.FromSeconds(30),
            factory: () => new StubTransport
            {
                OnSendGate = ct => sendGate.WaitAsync(ct)
            });
        engine.Start();

        engine.AppendBlocking(new byte[24]);
        engine.AppendBlocking(new byte[24]);

        var producer = Task.Run<Exception?>(() =>
        {
            try
            {
                engine.AppendBlocking(new byte[24]);
                return null;
            }
            catch (ObjectDisposedException ex)
            {
                return ex;
            }
        });
        await Task.Delay(100);
        Assert.That(producer.IsCompleted, Is.False);

        engine.Dispose();
        sendGate.Release(8);

        var thrown = await producer.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(thrown, Is.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public async Task ReconnectCycles_DisposeEveryTransport_NoLeak()
    {
        var stubs = new System.Collections.Concurrent.ConcurrentBag<StubTransport>();
        var connectCount = 0;

        using var engine = NewEngine(out _,
            factory: () =>
            {
                var idx = Interlocked.Increment(ref connectCount);
                var s = new StubTransport();
                if (idx <= 5)
                {
                    var sent = 0;
                    s.OnSend = _ =>
                    {
                        sent++;
                        if (sent == 1) return OkResponse(0);
                        throw new IngressError(ErrorCode.SocketError, $"connection {idx} drops on second send");
                    };
                }

                stubs.Add(s);
                return s;
            });
        engine.Start();

        for (var i = 0; i < 12; i++)
        {
            engine.AppendBlocking(new byte[] { (byte)i });
        }

        await engine.FlushAsync(TimeSpan.FromSeconds(10));
        engine.Dispose();

        Assert.That(stubs, Has.Count.GreaterThanOrEqualTo(2));
        Assert.That(stubs.All(s => s.Disposed), Is.True,
            $"every transport must be disposed; leaked {stubs.Count(s => !s.Disposed)} of {stubs.Count}");
    }

    [Test]
    public async Task MultipleProducers_AllFramesEventuallyDrained()
    {
        const int producerCount = 4;
        const int framesPerProducer = 250;
        const int totalFrames = producerCount * framesPerProducer;

        using var engine = NewEngine(out _, segmentCapacity: 64 * 1024);
        engine.Start();

        var producerTasks = Enumerable.Range(0, producerCount).Select(p => Task.Run(() =>
        {
            for (var i = 0; i < framesPerProducer; i++)
            {
                engine.AppendBlocking(new byte[] { (byte)p, (byte)(i & 0xFF) });
            }
        })).ToArray();

        await Task.WhenAll(producerTasks).WaitAsync(TimeSpan.FromSeconds(20));
        await engine.FlushAsync(TimeSpan.FromSeconds(20));

        Assert.That(engine.NextFsn, Is.EqualTo((long)totalFrames));
        Assert.That(engine.AckedFsn, Is.EqualTo((long)totalFrames));
    }

    [Test]
    public async Task StressReconnects_NoFramesLost()
    {
        const int totalFrames = 200;
        var failureRate = 7;
        var stubs = new System.Collections.Concurrent.ConcurrentBag<StubTransport>();

        using var engine = NewEngine(out _,
            segmentCapacity: 64 * 1024,
            policy: new QwpReconnectPolicy(
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(5), TimeSpan.FromMinutes(2)),
            factory: () =>
            {
                var s = new StubTransport();
                var sendCount = 0;
                s.OnSend = _ =>
                {
                    var n = Interlocked.Increment(ref sendCount);
                    if (n % failureRate == 0)
                    {
                        throw new IngressError(ErrorCode.SocketError, "synthetic flap");
                    }

                    return OkResponse(n - 1);
                };
                stubs.Add(s);
                return s;
            });
        engine.Start();

        for (var i = 0; i < totalFrames; i++)
        {
            var payload = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(payload, i);
            engine.AppendBlocking(payload);
        }

        await engine.FlushAsync(TimeSpan.FromSeconds(60));
        Assert.That(engine.AckedFsn, Is.EqualTo((long)totalFrames));
        Assert.That(stubs.Count, Is.GreaterThan(1), "synthetic flaps must have triggered at least one reconnect");

        var perTransport = stubs
            .Select(s => (IReadOnlyList<int>)s.Sent.Select(b => BinaryPrimitives.ReadInt32LittleEndian(b)).ToList())
            .ToList();
        AssertAtLeastOnceDelivery(perTransport, expectedCount: totalFrames);
    }

    // At-least-once: each connection sends frames in strictly ascending FSN order (no in-connection
    // reorder), and the union across all connections covers every sequence 0..expectedCount-1 with
    // no extras. Cross-connection duplicates are tolerated — replay is at-least-once.
    private static void AssertAtLeastOnceDelivery(IEnumerable<IReadOnlyList<int>> perTransport, int expectedCount)
    {
        var union = new HashSet<int>();
        foreach (var sent in perTransport)
        {
            for (var i = 1; i < sent.Count; i++)
            {
                Assert.That(sent[i], Is.GreaterThan(sent[i - 1]),
                    "a single connection must send frames in strictly ascending FSN order");
            }

            foreach (var seq in sent)
            {
                union.Add(seq);
            }
        }

        Assert.That(union, Is.EquivalentTo(Enumerable.Range(0, expectedCount)),
            "every expected sequence number must appear at least once across all connections, with no extras");
    }


    private QwpCursorSendEngine NewEngine(
        out string slotDir,
        Func<IQwpCursorTransport>? factory = null,
        QwpReconnectPolicy? policy = null,
        TimeSpan? appendDeadline = null,
        InitialConnectMode initialConnectMode = InitialConnectMode.off,
        long segmentCapacity = 4096,
        long maxTotalBytes = long.MaxValue,
        string? slotDirectoryOverride = null,
        SenderErrorHandler? errorHandler = null,
        SenderErrorPolicyResolver? policyResolver = null,
        int errorInboxCapacity = 256,
        int maxFrameRejections = 4,
        TimeSpan? poisonMinEscalationWindow = null,
        bool durableAckMode = false)
    {
        slotDir = slotDirectoryOverride ?? Path.Combine(_root, "sender-" + Guid.NewGuid().ToString("N"));
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: segmentCapacity);
        policy ??= new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromSeconds(2));
        factory ??= () => new StubTransport();
        var dispatcher = errorHandler is null && policyResolver is null
            ? null
            : new QwpSenderErrorDispatcher(errorHandler, errorInboxCapacity);
        return new QwpCursorSendEngine(
            slotLock, ring, factory, policy,
            appendDeadline ?? TimeSpan.FromSeconds(5),
            initialConnectMode,
            maxTotalBytes: maxTotalBytes,
            errorDispatcher: dispatcher,
            policyResolver: policyResolver,
            durableAckMode: durableAckMode,
            maxFrameRejections: maxFrameRejections,
            poisonMinEscalationWindow: poisonMinEscalationWindow);
    }

    private static byte[] OkResponse(long sequence)
    {
        var buf = new byte[11];
        buf[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), 0);
        return buf;
    }

    // OK carrying a single per-table seqTxn entry. In durable-ack mode such an OK is NOT trivially
    // durable (an empty-entry OK is), so it does not advance the trim watermark until a matching
    // DURABLE_ACK arrives — which is exactly what pins AckedFsn during a replica-lag window.
    private static byte[] OkResponseWithEntry(long sequence, string table, long seqTxn)
    {
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(table);
        var buf = new byte[11 + 2 + nameBytes.Length + 8];
        buf[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), 1);
        var pos = 11;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos, 2), (ushort)nameBytes.Length);
        pos += 2;
        nameBytes.CopyTo(buf.AsSpan(pos));
        pos += nameBytes.Length;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos, 8), seqTxn);
        return buf;
    }

    private static byte[] ErrorResponse(QwpStatusCode status, long sequence, string message)
    {
        var msgBytes = System.Text.Encoding.UTF8.GetBytes(message);
        var buf = new byte[11 + msgBytes.Length];
        buf[0] = (byte)status;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), (ushort)msgBytes.Length);
        msgBytes.CopyTo(buf.AsSpan(11));
        return buf;
    }

    private static void AssertEventually(Func<bool> condition, string message, int timeoutMs = 1000, int pollMs = 10)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            Thread.Sleep(pollMs);
        }

        Assert.Fail(message);
    }

    private sealed class StubTransport : IQwpCursorTransport
    {
        public Func<CancellationToken, Task>? OnConnect;
        public Func<byte[], byte[]>? OnSend;
        public Func<byte[], Task<byte[]>>? OnSendAsync;
        public Func<CancellationToken, Task>? OnSendGate;
        // Models an accept-then-close middlebox: once a frame has been shipped, the receive side
        // faults with this exception (the frame is never acked), driving the poison detector.
        public Exception? FailReceiveWith;
        // Models "server ack'd N frames on this connection, then dropped": deliver this many acks
        // from the channel, then fault the receive as a non-orderly close. Unlike FailReceiveWith,
        // the acks ARE processed first (so OK-level progress is recorded before the close).
        public int? FailReceiveAfterAcks;
        public List<byte[]> Sent { get; } = new();
        public (string Host, int Port)? Endpoint { get; set; } = ("stub", 0);

        private readonly Channel<byte[]> _acks = Channel.CreateUnbounded<byte[]>();
        private readonly object _sentLock = new();
        private int _autoSeq;
        private int _acksDelivered;
        public bool Disposed { get; private set; }

        public Task ConnectAsync(CancellationToken cancellationToken)
        {
            return OnConnect is null ? Task.CompletedTask : OnConnect(cancellationToken);
        }

        public async Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            var copy = data.ToArray();
            lock (_sentLock) { Sent.Add(copy); }

            if (OnSendGate is not null)
            {
                await OnSendGate(cancellationToken).ConfigureAwait(false);
            }

            byte[] ack;
            if (OnSendAsync is not null)
            {
                ack = await OnSendAsync(copy).ConfigureAwait(false);
            }
            else if (OnSend is not null)
            {
                ack = OnSend(copy);
            }
            else
            {
                ack = DefaultOk(Interlocked.Increment(ref _autoSeq) - 1);
            }

            await _acks.Writer.WriteAsync(ack, cancellationToken).ConfigureAwait(false);
        }

        public async Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken)
        {
            if (FailReceiveAfterAcks is int limit && _acksDelivered >= limit)
            {
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
                throw FailReceiveWith ?? new IngressError(ErrorCode.SocketError, "server closed after acks");
            }

            if (FailReceiveWith is not null)
            {
                // Wait until a frame has actually shipped so the engine has marked this connection
                // as having sent (the frame is left un-acked), then fault as a server close.
                while (true)
                {
                    lock (_sentLock) { if (Sent.Count > 0) break; }
                    await Task.Delay(5, cancellationToken).ConfigureAwait(false);
                }

                await Task.Delay(25, cancellationToken).ConfigureAwait(false);
                throw FailReceiveWith;
            }

            var ack = await _acks.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            _acksDelivered++;
            ack.CopyTo(destination.Span);
            return ack.Length;
        }

        public Task CloseAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose()
        {
            Disposed = true;
            _acks.Writer.TryComplete();
        }

        private static byte[] DefaultOk(int seq)
        {
            var buf = new byte[11];
            buf[0] = (byte)QwpStatusCode.Ok;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), seq);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), 0);
            return buf;
        }
    }

    // Reproduces the NACK-before-close pipeline race the fix targets. On the poisoned connection two
    // frames are sent:
    //   frame0 send returns normally (so the engine marks _sentOnCurrentConnection) and its ack is a
    //     retriable NACK — the recv pump strikes the head-of-line frame in HandleServerRejection
    //     (strike #1) and throws RetriableNackException.
    //   frame1 send concurrently faults with a non-terminal socket error (the torn-down connection).
    // Ordering is made deterministic without sleeps racing the strike: the NACK for frame0 is only
    // enqueued once frame1's send is in flight, so the connection is never cancelled before frame1 is
    // sent; and frame1's fault is withheld until the recv pump has consumed the NACK, so strike #1 is
    // always registered before both pumps fault together. On a non-poisoned connection every frame is
    // acked OK, so the replayed frames land on the fresh connection.
    private sealed class NackThenConcurrentSendFaultTransport : IQwpCursorTransport
    {
        private readonly bool _faultThisConnection;
        private readonly QwpStatusCode _nackStatus;
        private readonly Channel<byte[]> _acks = Channel.CreateUnbounded<byte[]>();
        private readonly TaskCompletionSource _nackConsumed =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _sends;

        // nackStatus selects which side of PickTerminalOrRetriableFault's precedence is exercised:
        // a retriable status (WriteError) makes frame0's recv pump throw RetriableNackException; a
        // terminal status (SchemaMismatch) makes it throw HaltCarrier. Either must win over the
        // concurrent non-terminal send-side socket fault below.
        public NackThenConcurrentSendFaultTransport(
            bool faultThisConnection, QwpStatusCode nackStatus = QwpStatusCode.WriteError)
        {
            _faultThisConnection = faultThisConnection;
            _nackStatus = nackStatus;
        }

        public (string Host, int Port)? Endpoint => ("stub", 0);

        public Task ConnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            var seq = Interlocked.Increment(ref _sends) - 1;
            if (!_faultThisConnection)
            {
                await _acks.Writer.WriteAsync(OkResponse(seq), cancellationToken).ConfigureAwait(false);
                return;
            }

            if (seq == 0)
            {
                // Return normally so the engine records _sentOnCurrentConnection, but withhold the NACK
                // until frame1 is in flight (below) so the recv-side rejection can't cancel the
                // connection before frame1 is even sent.
                return;
            }

            // frame1: now that a second frame is being sent, release frame0's NACK so the recv pump
            // strikes + throws while this send is in flight...
            await _acks.Writer
                .WriteAsync(ErrorResponse(_nackStatus, 0, "poison"), cancellationToken)
                .ConfigureAwait(false);
            // ...wait until the recv pump has actually consumed the NACK (strike #1 registered), then
            // fault this send with the concurrent socket error that must NOT re-strike the same frame.
            await _nackConsumed.Task.ConfigureAwait(false);
            throw new IngressError(ErrorCode.SocketError, "socket closed under concurrent send");
        }

        public async Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken)
        {
            var ack = await _acks.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (_faultThisConnection && ack[0] != (byte)QwpStatusCode.Ok)
            {
                _nackConsumed.TrySetResult();
            }

            ack.CopyTo(destination.Span);
            return ack.Length;
        }

        public Task CloseAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose()
        {
            _nackConsumed.TrySetResult();
            _acks.Writer.TryComplete();
        }
    }
}
