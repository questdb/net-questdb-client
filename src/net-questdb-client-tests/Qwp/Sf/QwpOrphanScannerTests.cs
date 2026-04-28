
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
public class QwpOrphanScannerTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-orphan-" + Guid.NewGuid().ToString("N"));
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
    public void ClaimOrphans_AbsentRoot_ReturnsEmpty()
    {
        var claimed = QwpOrphanScanner.ClaimOrphans(_root, "self");
        Assert.That(claimed, Is.Empty);
    }

    [Test]
    public void ClaimOrphans_SkipsOwnSlot()
    {
        var ownSlot = Path.Combine(_root, "self");
        SetupSlotWithSegment(ownSlot);

        var claimed = QwpOrphanScanner.ClaimOrphans(_root, "self");
        Assert.That(claimed, Is.Empty);
    }

    [Test]
    public void ClaimOrphans_SkipsFailedSentinelSlot()
    {
        var slot = Path.Combine(_root, "crashed");
        SetupSlotWithSegment(slot);
        File.WriteAllText(Path.Combine(slot, ".failed"), "");

        var claimed = QwpOrphanScanner.ClaimOrphans(_root, "self");
        Assert.That(claimed, Is.Empty);
    }

    [Test]
    public void ClaimOrphans_SkipsAlreadyLockedSlot()
    {
        var slot = Path.Combine(_root, "live");
        SetupSlotWithSegment(slot);
        using var ownerLock = QwpSlotLock.Acquire(slot);

        var claimed = QwpOrphanScanner.ClaimOrphans(_root, "self");
        Assert.That(claimed, Is.Empty);
    }

    [Test]
    public void ClaimOrphans_SkipsEmptySlot()
    {
        var slot = Path.Combine(_root, "empty");
        Directory.CreateDirectory(slot);

        var claimed = QwpOrphanScanner.ClaimOrphans(_root, "self");
        Assert.That(claimed, Is.Empty);
    }

    [Test]
    public void ClaimOrphans_ClaimsEligibleOrphan()
    {
        var slot = Path.Combine(_root, "crashed");
        SetupSlotWithSegment(slot);

        var claimed = QwpOrphanScanner.ClaimOrphans(_root, "self");
        try
        {
            Assert.That(claimed, Has.Count.EqualTo(1));
            Assert.That(claimed[0].SlotDirectory, Is.EqualTo(slot));
        }
        finally
        {
            foreach (var c in claimed)
            {
                c.Dispose();
            }
        }
    }

    [Test]
    public void ClaimOrphans_MultipleOrphans_PartialEligibility()
    {
        var ourSlot = Path.Combine(_root, "self");
        var goodA = Path.Combine(_root, "good-a");
        var goodB = Path.Combine(_root, "good-b");
        var failed = Path.Combine(_root, "failed");
        var live = Path.Combine(_root, "live");
        var empty = Path.Combine(_root, "empty");

        SetupSlotWithSegment(ourSlot);
        SetupSlotWithSegment(goodA);
        SetupSlotWithSegment(goodB);
        SetupSlotWithSegment(failed);
        File.WriteAllText(Path.Combine(failed, ".failed"), "x");
        SetupSlotWithSegment(live);
        using var liveLock = QwpSlotLock.Acquire(live);
        Directory.CreateDirectory(empty);

        var claimed = QwpOrphanScanner.ClaimOrphans(_root, "self");
        try
        {
            var dirs = claimed.Select(l => l.SlotDirectory).OrderBy(s => s).ToList();
            Assert.That(dirs, Is.EqualTo(new[] { goodA, goodB }));
        }
        finally
        {
            foreach (var c in claimed)
            {
                c.Dispose();
            }
        }
    }

    [Test]
    public void ClaimOrphans_LockAlreadyReleased_CanBeReclaimedByCaller()
    {
        var slot = Path.Combine(_root, "crashed");
        SetupSlotWithSegment(slot);

        var firstSweep = QwpOrphanScanner.ClaimOrphans(_root, "self");
        Assert.That(firstSweep, Has.Count.EqualTo(1));
        firstSweep[0].Dispose();

        // After releasing, a second scan should re-claim the same slot.
        var secondSweep = QwpOrphanScanner.ClaimOrphans(_root, "self");
        try
        {
            Assert.That(secondSweep, Has.Count.EqualTo(1));
            Assert.That(secondSweep[0].SlotDirectory, Is.EqualTo(slot));
        }
        finally
        {
            foreach (var c in secondSweep)
            {
                c.Dispose();
            }
        }
    }

    private static void SetupSlotWithSegment(string slotDir)
    {
        Directory.CreateDirectory(slotDir);
        // Create a fake segment file. The scanner only checks for the glob match — content is irrelevant.
        File.WriteAllBytes(Path.Combine(slotDir, "sf-0000000000000000.sfa"), new byte[16]);
    }
}
