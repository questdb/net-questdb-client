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
                new QwpCursorSendEngine(slotLock, null!, () => null!, policy, TimeSpan.FromSeconds(5), false));
            Assert.Throws<ArgumentNullException>(() =>
                new QwpCursorSendEngine(slotLock, ring, null!, policy, TimeSpan.FromSeconds(5), false));
            Assert.Throws<ArgumentNullException>(() =>
                new QwpCursorSendEngine(slotLock, ring, () => null!, null!, TimeSpan.FromSeconds(5), false));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new QwpCursorSendEngine(slotLock, ring, () => null!, policy, TimeSpan.Zero, false));

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
    public void InitialConnectFailure_NoRetry_Terminal()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnConnect = _ => throw new IngressError(ErrorCode.SocketError, "connection refused")
        }, initialConnectRetry: false);
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
        }, initialConnectRetry: true);
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

        engine.AppendBlocking(new byte[] { 1 });
        engine.AppendBlocking(new byte[] { 2 });
        engine.AppendBlocking(new byte[] { 3 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(connectCount, Is.GreaterThanOrEqualTo(2), "reconnect did not occur");
        // Frame at FSN 0 was acked on the first connection; FSN 1 + 2 replayed on the second.
        Assert.That(engine.AckedFsn, Is.EqualTo(3L));
        var allSent = stubs.SelectMany(s => s.Sent.Select(b => b[0])).ToArray();
        Assert.That(allSent, Does.Contain((byte)1));
        Assert.That(allSent, Does.Contain((byte)2));
        Assert.That(allSent, Does.Contain((byte)3));
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
            initialConnectRetry: true);
        engine.Start();

        AssertEventually(() => engine.IsTerminallyFailed, "budget never exhausted", timeoutMs: 2000);
        Assert.That(engine.TerminalError, Is.InstanceOf<IngressError>());
    }

    [Test]
    public void ServerErrorResponse_Terminal()
    {
        using var engine = NewEngine(out _, factory: () => new StubTransport
        {
            OnSend = _ => ErrorResponse(QwpStatusCode.SchemaMismatch, sequence: 0, "bad schema")
        });
        engine.Start();

        engine.AppendBlocking(new byte[] { 9 });
        AssertEventually(() => engine.IsTerminallyFailed, "engine should mark terminal on server reject");
        Assert.That(engine.TerminalError, Is.InstanceOf<QwpException>());
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

        var ex = Assert.CatchAsync<IngressError>(
            async () => await engine.FlushAsync(TimeSpan.FromMilliseconds(150)));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ServerFlushError));

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
            segmentCapacity: 64,
            maxTotalBytes: 64,
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
            segmentCapacity: 64,
            maxTotalBytes: 64,
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
            segmentCapacity: 64,
            maxTotalBytes: 64,
            appendDeadline: TimeSpan.FromSeconds(30),
            factory: () => new StubTransport
            {
                OnSendGate = ct => sendGate.WaitAsync(ct)
            });
        engine.Start();

        engine.AppendBlocking(new byte[24]);
        engine.AppendBlocking(new byte[24]);

        var producer = Task.Run(() =>
        {
            try
            {
                engine.AppendBlocking(new byte[24]);
                return (Exception?)null;
            }
            catch (Exception ex)
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

        await Task.WhenAll(producerTasks).WaitAsync(TimeSpan.FromSeconds(10));
        await engine.FlushAsync(TimeSpan.FromSeconds(10));

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
                TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(5), TimeSpan.FromSeconds(30)),
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
            engine.AppendBlocking(new byte[] { (byte)(i & 0xFF) });
        }

        await engine.FlushAsync(TimeSpan.FromSeconds(20));
        Assert.That(engine.AckedFsn, Is.EqualTo((long)totalFrames));
        Assert.That(stubs.Count, Is.GreaterThan(1), "synthetic flaps must have triggered at least one reconnect");
    }

    // -- helpers ----------------------------------------------------------------

    private QwpCursorSendEngine NewEngine(
        out string slotDir,
        Func<IQwpCursorTransport>? factory = null,
        QwpReconnectPolicy? policy = null,
        TimeSpan? appendDeadline = null,
        bool initialConnectRetry = false,
        long segmentCapacity = 4096,
        long maxTotalBytes = long.MaxValue,
        string? slotDirectoryOverride = null)
    {
        slotDir = slotDirectoryOverride ?? Path.Combine(_root, "sender-" + Guid.NewGuid().ToString("N"));
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: segmentCapacity);
        policy ??= new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromSeconds(2));
        factory ??= () => new StubTransport();
        return new QwpCursorSendEngine(
            slotLock, ring, factory, policy,
            appendDeadline ?? TimeSpan.FromSeconds(5),
            initialConnectRetry,
            maxTotalBytes: maxTotalBytes);
    }

    private static byte[] OkResponse(long sequence)
    {
        var buf = new byte[9];
        buf[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), sequence);
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
        public List<byte[]> Sent { get; } = new();

        private readonly Channel<byte[]> _acks = Channel.CreateUnbounded<byte[]>();
        private readonly object _sentLock = new();
        private int _autoSeq;
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
            var ack = await _acks.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
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
            var buf = new byte[9];
            buf[0] = (byte)QwpStatusCode.Ok;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), seq);
            return buf;
        }
    }
}
