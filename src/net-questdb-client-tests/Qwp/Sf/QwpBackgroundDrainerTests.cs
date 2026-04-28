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
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpBackgroundDrainerTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-bgdrain-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); } catch { }
        }
    }

    [Test]
    public async Task DrainAsync_SeededSlot_FullyDrainsAndCleansUp()
    {
        var slotDir = Path.Combine(_root, "orphan");
        SeedSlot(slotDir, payloads: new byte[][] { new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 } });

        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(40), TimeSpan.FromSeconds(2));
        var drainer = new QwpBackgroundDrainer(
            transportFactory: () => new StubTransport(),
            reconnectPolicy: policy,
            segmentCapacity: 4096,
            drainTimeout: TimeSpan.FromSeconds(5));

        await drainer.DrainAsync(slotDir, CancellationToken.None);

        Assert.That(Directory.GetFiles(slotDir, "sf-*.sfa"), Is.Empty,
            "drainer must unlink fully-acked segment files before returning");
    }

    [Test]
    public async Task DrainAsync_EmptySlot_NoOp()
    {
        var slotDir = Path.Combine(_root, "empty");
        Directory.CreateDirectory(slotDir);

        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(40), TimeSpan.FromSeconds(2));
        var drainer = new QwpBackgroundDrainer(
            transportFactory: () => new StubTransport(),
            reconnectPolicy: policy,
            segmentCapacity: 4096,
            drainTimeout: TimeSpan.FromSeconds(5));

        Assert.DoesNotThrowAsync(async () => await drainer.DrainAsync(slotDir, CancellationToken.None));
    }

    [Test]
    public async Task DrainAsync_EmptyOrphanSegmentFiles_AreUnlinkedToPreventScannerChurn()
    {
        var slotDir = Path.Combine(_root, "empty-files");
        Directory.CreateDirectory(slotDir);

        var fakeSegment = Path.Combine(slotDir, "sf-0000000000000000.sfa");
        File.WriteAllBytes(fakeSegment, new byte[64]);

        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(40), TimeSpan.FromSeconds(2));
        var drainer = new QwpBackgroundDrainer(
            transportFactory: () => new StubTransport(),
            reconnectPolicy: policy,
            segmentCapacity: 4096,
            drainTimeout: TimeSpan.FromSeconds(5));

        await drainer.DrainAsync(slotDir, CancellationToken.None);

        Assert.That(Directory.GetFiles(slotDir, "sf-*.sfa"), Is.Empty,
            "empty segment files must be unlinked so the scanner doesn't loop adopting them");
    }

    [Test]
    public async Task EndToEnd_ScannerPoolDrainer_AdoptsAndCleansMultipleOrphans()
    {
        var slotA = Path.Combine(_root, "crashed-a");
        var slotB = Path.Combine(_root, "crashed-b");
        var slotC = Path.Combine(_root, "crashed-c");
        SeedSlot(slotA, payloads: new byte[][] { new byte[] { 10 } });
        SeedSlot(slotB, payloads: new byte[][] { new byte[] { 20 }, new byte[] { 21 } });
        SeedSlot(slotC, payloads: new byte[][] { new byte[] { 30 } });

        // .failed sentinel marks a slot the scanner must skip.
        var slotFailed = Path.Combine(_root, "crashed-failed");
        SeedSlot(slotFailed, payloads: new byte[][] { new byte[] { 99 } });
        await File.WriteAllTextAsync(Path.Combine(slotFailed, ".failed"), "prior crash");

        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(40), TimeSpan.FromSeconds(2));
        var drainer = new QwpBackgroundDrainer(
            transportFactory: () => new StubTransport(),
            reconnectPolicy: policy,
            segmentCapacity: 4096,
            drainTimeout: TimeSpan.FromSeconds(5));

        using var pool = new QwpBackgroundDrainerPool(maxConcurrent: 2, drainer);
        foreach (var l in QwpOrphanScanner.ClaimOrphans(_root, ourSenderId: "live-sender"))
        {
            pool.Enqueue(l);
        }

        await pool.WaitForAllAsync();

        Assert.That(Directory.GetFiles(slotA, "sf-*.sfa"), Is.Empty);
        Assert.That(Directory.GetFiles(slotB, "sf-*.sfa"), Is.Empty);
        Assert.That(Directory.GetFiles(slotC, "sf-*.sfa"), Is.Empty);
        Assert.That(File.Exists(Path.Combine(slotFailed, ".failed")), Is.True);
        Assert.That(Directory.GetFiles(slotFailed, "sf-*.sfa"), Is.Not.Empty);
    }

    [Test]
    public async Task DrainAsync_TransientWireFailure_ReconnectsAndCompletes()
    {
        var slotDir = Path.Combine(_root, "flaky");
        SeedSlot(slotDir, payloads: new byte[][] { new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 } });

        var stubsBuilt = 0;
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(40), TimeSpan.FromSeconds(2));
        var drainer = new QwpBackgroundDrainer(
            transportFactory: () =>
            {
                var idx = Interlocked.Increment(ref stubsBuilt);
                var s = new StubTransport();
                if (idx == 1)
                {
                    // First connection succeeds, but the second send throws — engine reconnects.
                    var sendCount = 0;
                    s.OnSend = _ =>
                    {
                        sendCount++;
                        if (sendCount == 1) return DefaultOk(0);
                        throw new IngressError(ErrorCode.SocketError, "broken pipe");
                    };
                }
                return s;
            },
            reconnectPolicy: policy,
            segmentCapacity: 4096,
            drainTimeout: TimeSpan.FromSeconds(5));

        await drainer.DrainAsync(slotDir, CancellationToken.None);

        Assert.That(stubsBuilt, Is.GreaterThanOrEqualTo(2),
            "drainer must reconnect after transient wire failure");
        Assert.That(Directory.GetFiles(slotDir, "sf-*.sfa"), Is.Empty);
    }

    private static void SeedSlot(string slotDir, byte[][] payloads)
    {
        Directory.CreateDirectory(slotDir);
        using var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        foreach (var p in payloads)
        {
            Assert.That(ring.TryAppend(p), Is.True);
        }
    }

    private static byte[] DefaultOk(long seq)
    {
        var buf = new byte[9];
        buf[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), seq);
        return buf;
    }

    private sealed class StubTransport : IQwpCursorTransport
    {
        public Func<byte[], byte[]>? OnSend;
        private readonly Channel<byte[]> _acks = Channel.CreateUnbounded<byte[]>();
        private int _autoSeq;

        public Task ConnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            byte[] ack;
            if (OnSend is not null)
            {
                ack = OnSend(data.ToArray());
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

        public void Dispose() => _acks.Writer.TryComplete();
    }
}
