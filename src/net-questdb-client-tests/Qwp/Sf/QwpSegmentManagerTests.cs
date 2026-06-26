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

using System.Runtime.InteropServices;
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
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: QwpMmapSegment.HeaderSize + 64);
        using var mgr = new QwpSegmentManager(ring, long.MaxValue);
        mgr.Start();

        // Producer asks for spare via NeedsHotSpare → Wake → manager installs.
        await WaitFor(() => mgr.SparesInstalled >= 1, TimeSpan.FromSeconds(2));
        Assert.That(ring.NeedsHotSpare(), Is.False);
        Assert.That(mgr.CommittedBytes, Is.EqualTo((long)QwpMmapSegment.HeaderSize + 64));
    }

    [Test]
    public async Task ProvisionedHotSpare_IsBlockReserved_NotSparse()
    {
        // Guards the ProvisionHotSpare ordering: Reserve must own the SetLength so the macOS
        // F_PREALLOCATE path (which reserves blocks from the physical EOF) actually runs. A SetLength
        // before Reserve leaves a sparse spare whose mmap-write SIGBUSes when the disk is full. The
        // sparseness is only observable on Unix via on-disk block accounting (du); NTFS SetLength
        // always allocates, so there is nothing to verify on Windows.
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            Assert.Ignore("Sparse spares are not observable on Windows; SetLength allocates on NTFS.");
        }

        // Capacity well above a filesystem block so sparse (~0 reserved) vs full (~capacity reserved)
        // is unambiguous.
        const long capacity = QwpMmapSegment.HeaderSize + 4L * 1024 * 1024;
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: capacity);
        using var mgr = new QwpSegmentManager(ring, long.MaxValue);
        mgr.Start();

        await WaitFor(() => mgr.SparesInstalled >= 1, TimeSpan.FromSeconds(5));

        var spares = Directory.GetFiles(
            _root, QwpSegmentRing.SparePrefix + "*" + QwpSegmentRing.SpareSuffix);
        Assert.That(spares, Has.Length.EqualTo(1), "manager installs exactly one hot spare");

        var allocated = GetAllocatedBytes(spares[0]);
        Assert.That(allocated, Is.GreaterThanOrEqualTo(capacity / 2),
            $"hot spare must be block-reserved, not sparse (allocated={allocated}, capacity={capacity})");
    }

    [Test]
    public async Task DisksCap_RefusesNewSpareUntilTrim()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: QwpMmapSegment.HeaderSize + 64);
        using var mgr = new QwpSegmentManager(ring, maxTotalBytes: QwpMmapSegment.HeaderSize + 64);
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
        Assert.That(mgr.CommittedBytes, Is.EqualTo((long)QwpMmapSegment.HeaderSize + 64),
            "after trim+spare-install, committed back at one segment");
    }

    [Test]
    public async Task Trim_RemovesAckedSegments_AndDecrementsCommittedBytes()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: QwpMmapSegment.HeaderSize + 64);
        using var mgr = new QwpSegmentManager(ring, maxTotalBytes: 1024);
        mgr.Start();

        // Fill three segments worth.
        for (var i = 0; i < 6; i++)
        {
            await WaitFor(() => ring.TryAppend(new byte[24]), TimeSpan.FromSeconds(2));
        }

        Assert.That(ring.SealedSegmentCount, Is.GreaterThanOrEqualTo(2));

        ring.Acknowledge(99L);
        // Drain leaves only the active segment + at most one installed hot spare on disk; the
        // manager reconciles _committedBytes to that. Don't capture a "before" snapshot — the
        // spare-install timing races with capture and produces flaky deltas across machines.
        await WaitFor(
            () => ring.SealedSegmentCount == 0 && mgr.CommittedBytes <= 2 * ring.SegmentCapacity,
            TimeSpan.FromSeconds(2));
    }

    [Test]
    public async Task Dispose_ShutsDownCleanly_EvenWhenIdle()
    {
        var ring = QwpSegmentRing.Open(_root, segmentCapacity: QwpMmapSegment.HeaderSize + 64);
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
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: QwpMmapSegment.HeaderSize + 64);
        using var mgr = new QwpSegmentManager(ring, long.MaxValue, heartbeatInterval: TimeSpan.FromSeconds(30));
        mgr.Start();

        // Sample BEFORE consuming the startup spare — TryAllocateNewActive can wake the manager
        // mid-append and bump SparesInstalled, racing with a post-append sample.
        await WaitFor(() => mgr.SparesInstalled >= 1, TimeSpan.FromSeconds(5));
        var sparesBefore = mgr.SparesInstalled;
        Assert.That(ring.TryAppend(new byte[24]), Is.True);
        Assert.That(ring.TryAppend(new byte[24]), Is.True);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await WaitFor(() => mgr.SparesInstalled > sparesBefore, TimeSpan.FromSeconds(5));
        sw.Stop();

        Assert.That(sw.Elapsed, Is.LessThan(mgr.HeartbeatInterval),
            "spare arrives via producer wake, not heartbeat tick");
    }

    [Test]
    public async Task ConcurrentRotateAndTrim_NoCorruption()
    {
        // Producer hammers TryAppend (lots of rotations); receiver acks aggressively. Manager must
        // service spare provisioning + trim concurrently without corrupting the ring.
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: QwpMmapSegment.HeaderSize + 256);
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

    // On-disk bytes actually reserved for the file (st_blocks), as reported by `du -k`. A sparse file
    // reports ~0 regardless of its logical length; a block-reserved file reports ~its length.
    private static long GetAllocatedBytes(string path)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("du")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-k");
        psi.ArgumentList.Add(path);

        using var proc = System.Diagnostics.Process.Start(psi)!;
        var stdout = proc.StandardOutput.ReadToEnd();
        proc.WaitForExit();

        // `du -k` emits "<kib>\t<path>"; the first whitespace-delimited token is 1024-byte blocks.
        var token = stdout.Split(
            new[] { '\t', ' ', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)[0];
        return long.Parse(token, System.Globalization.CultureInfo.InvariantCulture) * 1024;
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
