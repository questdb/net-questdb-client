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

using NUnit.Framework;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpSegmentManagerTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-mgr-" + Guid.NewGuid().ToString("N"));
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
    public async Task Provisions_HotSpare_OnFreshRing()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        using var mgr = new QwpSegmentManager(ring, long.MaxValue);
        mgr.Start();

        // Producer asks for spare via NeedsHotSpare → Wake → manager installs.
        await WaitFor(() => mgr.SparesInstalled >= 1, TimeSpan.FromSeconds(2));
        Assert.That(ring.NeedsHotSpare(), Is.False);
        Assert.That(mgr.CommittedBytes, Is.EqualTo(64L));
    }

    [Test]
    public async Task DisksCap_RefusesNewSpareUntilTrim()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        using var mgr = new QwpSegmentManager(ring, maxTotalBytes: 64);
        mgr.Start();

        await WaitFor(() => mgr.SparesInstalled >= 1, TimeSpan.FromSeconds(2));

        // Producer adopts the spare; ring now holds one segment of 64 bytes — at the cap.
        Assert.That(ring.TryAppend(new byte[24]), Is.True);
        Assert.That(ring.TryAppend(new byte[24]), Is.True);

        // Cap reached. The next call would need a rotation. Wake manager — it must NOT install a
        // second spare (committed=64 + capacity=64 > cap=64).
        var sparesBefore = mgr.SparesInstalled;
        Assert.That(ring.TryAppend(new byte[24]), Is.False);
        await Task.Delay(100);
        Assert.That(mgr.SparesInstalled, Is.EqualTo(sparesBefore), "no spare while cap-bound");

        // Acknowledge fully covers segment 0 → manager trims → cap frees → spare installable.
        ring.Acknowledge(1L);
        await WaitFor(() => mgr.SparesInstalled > sparesBefore, TimeSpan.FromSeconds(2));
        Assert.That(mgr.CommittedBytes, Is.EqualTo(64L), "after trim+spare-install, committed back at one segment");
    }

    [Test]
    public async Task Trim_RemovesAckedSegments_AndDecrementsCommittedBytes()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        using var mgr = new QwpSegmentManager(ring, maxTotalBytes: 1024);
        mgr.Start();

        // Fill three segments worth.
        for (var i = 0; i < 6; i++)
        {
            await WaitFor(() => ring.TryAppend(new byte[24]), TimeSpan.FromSeconds(2));
        }

        Assert.That(ring.SealedSegmentCount, Is.GreaterThanOrEqualTo(2));
        var sealedSegmentBytes = (long)ring.SealedSegmentCount * ring.SegmentCapacity;
        var committedBefore = mgr.CommittedBytes;
        var trimsBefore = mgr.TrimCycles;

        ring.Acknowledge(99L);
        await WaitFor(() => mgr.TrimCycles > trimsBefore, TimeSpan.FromSeconds(2));

        Assert.That(committedBefore - mgr.CommittedBytes, Is.GreaterThanOrEqualTo(sealedSegmentBytes));
    }

    [Test]
    public async Task Dispose_ShutsDownCleanly_EvenWhenIdle()
    {
        var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        var mgr = new QwpSegmentManager(ring, long.MaxValue);
        mgr.Start();

        await WaitFor(() => mgr.SparesInstalled >= 1, TimeSpan.FromSeconds(2));

        var sw = System.Diagnostics.Stopwatch.StartNew();
        mgr.Dispose();
        sw.Stop();

        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(2)), "dispose should not block past the heartbeat");
        ring.Dispose();
    }

    [Test]
    public async Task Wake_DrivesProvisioning_Promptly()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        using var mgr = new QwpSegmentManager(ring, long.MaxValue);
        mgr.Start();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await WaitFor(() => mgr.SparesInstalled >= 1, TimeSpan.FromSeconds(2));
        sw.Stop();

        // The first spare is needed because the ring is fresh; the producer's first append (via
        // NeedsHotSpare check) should wake the manager immediately rather than wait the full
        // heartbeat. We give a generous bound to avoid CI flakes.
        Assert.That(sw.Elapsed, Is.LessThan(QwpSegmentManager.HeartbeatInterval),
            "first spare arrives via wake, not heartbeat tick");
    }

    [Test]
    public async Task ConcurrentRotateAndTrim_NoCorruption()
    {
        // Producer hammers TryAppend (lots of rotations); receiver acks aggressively. Manager must
        // service spare provisioning + trim concurrently without corrupting the ring.
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 256);
        using var mgr = new QwpSegmentManager(ring, maxTotalBytes: 4 * 1024);
        mgr.Start();

        var stop = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        long appended = 0;

        var producer = Task.Run(() =>
        {
            var payload = new byte[80];
            while (!stop.IsCancellationRequested)
            {
                if (ring.TryAppend(payload))
                {
                    Interlocked.Increment(ref appended);
                }
                else
                {
                    Thread.SpinWait(10);
                }
            }
        });

        var acker = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                var hwm = ring.PublishedFsn;
                if (hwm >= 0)
                {
                    ring.Acknowledge(hwm);
                }
                Thread.Sleep(1);
            }
        });

        await Task.WhenAll(producer, acker);

        Assert.That(Interlocked.Read(ref appended), Is.GreaterThan(0));
        // Final invariant: published = appended - 1 (assuming 0-based FSN), acked ≤ published.
        Assert.That(ring.PublishedFsn, Is.EqualTo(Interlocked.Read(ref appended) - 1));
        Assert.That(ring.AckedFsn, Is.LessThanOrEqualTo(ring.PublishedFsn));
    }

    private static async Task WaitFor(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(10);
        }

        Assert.Fail($"condition not satisfied within {timeout}");
    }
}
