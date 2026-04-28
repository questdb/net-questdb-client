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
public class QwpSegmentRingTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-ring-" + Guid.NewGuid().ToString("N"));
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Test]
    public void Open_FreshDirectory_StartsEmpty()
    {
        using var ring = QwpSegmentRing.Open(_root);
        Assert.That(ring.SegmentCount, Is.Zero);
        Assert.That(ring.NextFsn, Is.Zero);
        Assert.That(ring.OldestFsn, Is.Zero);
    }

    [Test]
    public void Append_LargerThanSegment_Throws()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        Assert.Throws<ArgumentException>(() => ring.TryAppend(new byte[1024]));
    }

    [Test]
    public void Append_FillsSegmentThenRotates()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);

        // 64-byte segment fits two ~24-byte envelopes (8 header + 24 body) before rotating.
        Assert.That(ring.TryAppend(new byte[24]), Is.True);
        Assert.That(ring.TryAppend(new byte[24]), Is.True);
        // Next append rotates.
        Assert.That(ring.TryAppend(new byte[24]), Is.True);

        Assert.That(ring.SegmentCount, Is.EqualTo(2));
        Assert.That(ring.NextFsn, Is.EqualTo(3L));
    }

    [Test]
    public void TryReadFrame_AcrossSegments_RoundTrips()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);

        var bodies = new[]
        {
            new byte[] { 1, 1, 1 },
            new byte[] { 2, 2, 2, 2 },
            new byte[] { 3, 3 },
            new byte[] { 4, 4, 4, 4, 4 },
            new byte[] { 5 },
        };

        foreach (var b in bodies)
        {
            Assert.That(ring.TryAppend(b), Is.True);
        }

        var dest = new byte[64];
        for (var i = 0; i < bodies.Length; i++)
        {
            var n = ring.TryReadFrame(i, dest);
            Assert.That(n, Is.EqualTo(bodies[i].Length), $"frame {i}");
            Assert.That(dest.AsSpan(0, n).ToArray(), Is.EqualTo(bodies[i]));
        }
    }

    [Test]
    public void TryReadFrame_OutOfRange_ReturnsMinusOne()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        ring.TryAppend(new byte[8]);

        var dest = new byte[64];
        Assert.That(ring.TryReadFrame(-1, dest), Is.EqualTo(-1));
        Assert.That(ring.TryReadFrame(99, dest), Is.EqualTo(-1));
    }

    [Test]
    public void Acknowledge_FollowedByDrainTrimmable_PrecisePerSegment()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);

        // Each envelope is 8 (header) + 24 (body) = 32 bytes. Two fit per 64-byte segment.
        for (var i = 0; i < 6; i++)
        {
            ring.TryAppend(new byte[24]);
        }

        Assert.That(ring.SegmentCount, Is.EqualTo(3));
        Assert.That(ring.OldestFsn, Is.EqualTo(0L));
        // Segment 0: FSNs 0,1. Segment 1: FSNs 2,3. Segment 2 (active): FSNs 4,5.

        // Watermark 0 → segment 0 has last FSN 1 > 0, not fully covered.
        ring.Acknowledge(0L);
        Assert.That(ring.DrainTrimmable(), Is.Null);

        // Watermark 1 → segment 0 fully covered, drains 1.
        ring.Acknowledge(1L);
        var drained = ring.DrainTrimmable();
        Assert.That(drained, Is.Not.Null);
        Assert.That(drained!.Count, Is.EqualTo(1));
        foreach (var s in drained) s.Dispose();
        Assert.That(ring.SegmentCount, Is.EqualTo(2));
        Assert.That(ring.OldestFsn, Is.EqualTo(2L));

        // Watermark 99 → would cover segments 1 and active, but active is never drained.
        ring.Acknowledge(99L);
        drained = ring.DrainTrimmable();
        Assert.That(drained, Is.Not.Null);
        Assert.That(drained!.Count, Is.EqualTo(1));
        foreach (var s in drained) s.Dispose();
        Assert.That(ring.SegmentCount, Is.EqualTo(1));
        Assert.That(ring.OldestFsn, Is.EqualTo(4L));
    }

    [Test]
    public void Acknowledge_IsMonotonic()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        ring.TryAppend(new byte[24]);
        ring.TryAppend(new byte[24]);

        ring.Acknowledge(1L);
        Assert.That(ring.AckedFsn, Is.EqualTo(1L));

        // Older ACK is silently absorbed.
        ring.Acknowledge(0L);
        Assert.That(ring.AckedFsn, Is.EqualTo(1L));
    }

    [Test]
    public void NeedsHotSpare_FreshThenInstall()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        Assert.That(ring.NeedsHotSpare(), Is.True, "fresh ring needs initial active");

        // Initial append creates the active segment; spare not yet needed (under high-water).
        ring.TryAppend(new byte[8]);
        Assert.That(ring.NeedsHotSpare(), Is.False);

        // Cross 75% — needs spare.
        ring.TryAppend(new byte[24]);
        Assert.That(ring.NeedsHotSpare(), Is.True);

        // Install one — no longer needs.
        var spare = Path.Combine(_root, "manual-spare.tmp");
        File.WriteAllBytes(spare, new byte[64]);
        Assert.That(ring.InstallHotSpare(spare), Is.True);
        Assert.That(ring.NeedsHotSpare(), Is.False);
    }

    [Test]
    public void InstallHotSpare_TwiceWithoutConsumption_RejectsSecond()
    {
        using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);
        var spare1 = Path.Combine(_root, "spare1.tmp");
        File.WriteAllBytes(spare1, new byte[64]);
        Assert.That(ring.InstallHotSpare(spare1), Is.True);

        var spare2 = Path.Combine(_root, "spare2.tmp");
        File.WriteAllBytes(spare2, new byte[64]);
        Assert.That(ring.InstallHotSpare(spare2), Is.False);
    }

    [Test]
    public void Reopen_AfterAppends_RecoversWriteHead()
    {
        for (var iter = 0; iter < 2; iter++)
        {
            using var ring = QwpSegmentRing.Open(_root, segmentCapacity: 64);

            if (iter == 0)
            {
                ring.TryAppend(new byte[24]);
                ring.TryAppend(new byte[24]);
                ring.TryAppend(new byte[24]); // rotation, FSN 2 in segment 1.
            }
            else
            {
                Assert.That(ring.SegmentCount, Is.EqualTo(2));
                Assert.That(ring.NextFsn, Is.EqualTo(3L));
                Assert.That(ring.OldestFsn, Is.Zero);
            }
        }
    }
}
