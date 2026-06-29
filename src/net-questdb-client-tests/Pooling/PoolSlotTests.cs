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

using System.Collections.Concurrent;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Pooling;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

public class PoolSlotTests
{
    private static SenderPool MakeSfPool(string keys, out ConcurrentBag<FakeSender> created)
    {
        var bag = new ConcurrentBag<FakeSender>();
        created = bag;
        // ws + sf_dir => the pool runs in store-and-forward mode and hands out slot indices. The fake
        // factory bypasses the real WS sender, so no sockets / files are touched.
        var options = new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb-pool-test;" + keys);
        return new SenderPool(options, null, slot =>
        {
            var s = new FakeSender(slot);
            bag.Add(s);
            return s;
        });
    }

    [Test]
    public void ApplySlotIdentityStampsSenderIdAndManagedFamily()
    {
        var options = new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb-pool-test;");
        SenderPool.ApplySlotIdentity(options, "default", 2, 4);

        Assert.Multiple(() =>
        {
            Assert.That(options.sender_id, Is.EqualTo("default-2"));
            Assert.That(options.OrphanExcludeManagedBase, Is.EqualTo("default"));
            Assert.That(options.OrphanExcludeManagedCount, Is.EqualTo(4));
        });
    }

    [Test]
    public void SfModeAssignsDistinctSlotIndices()
    {
        var pool = MakeSfPool("sender_pool_min=3;sender_pool_max=4;", out var created);
        try
        {
            var indices = created.Select(s => s.SlotIndex).OrderBy(i => i).ToArray();
            Assert.That(indices, Is.EqualTo(new[] { 0, 1, 2 }));
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void NonSfPoolUsesNoSlotIndex()
    {
        var bag = new ConcurrentBag<FakeSender>();
        var options = new SenderOptions("http::addr=localhost:9000;sender_pool_min=2;sender_pool_max=2;");
        var pool = new SenderPool(options, null, slot =>
        {
            var s = new FakeSender(slot);
            bag.Add(s);
            return s;
        });
        try
        {
            Assert.That(bag.All(s => s.SlotIndex == -1), Is.True, "non-SF senders carry no slot index");
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void FreedSlotIndexIsReusedLowestFirst()
    {
        var pool = MakeSfPool("sender_pool_min=0;sender_pool_max=4;", out var created);
        try
        {
            var a = pool.Borrow(); // slot 0
            var b = pool.Borrow(); // slot 1
            var c = pool.Borrow(); // slot 2
            Assert.That(new[] { a.SlotIndex, b.SlotIndex, c.SlotIndex }, Is.EqualTo(new[] { 0, 1, 2 }));

            // Break the slot-1 sender so it is discarded (lock releases cleanly), freeing index 1.
            created.Single(s => s.SlotIndex == 1).ThrowOnSend = true;
            b.Dispose();

            var d = pool.Borrow();
            Assert.That(d.SlotIndex, Is.EqualTo(1), "lowest free index reused");
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(0));

            a.Dispose();
            c.Dispose();
            d.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void DiscardWithUnreleasedLockRetiresSlotAndShrinksCapacity()
    {
        var pool = MakeSfPool("sender_pool_min=0;sender_pool_max=2;acquire_timeout_ms=150;", out var created);
        try
        {
            var a = pool.Borrow(); // slot 0
            var b = pool.Borrow(); // slot 1

            // Slot-0 sender breaks AND refuses to release its lock -> the index must be retired.
            var fakeA = created.Single(s => s.SlotIndex == 0);
            fakeA.SlotLockReleased = false;
            fakeA.ThrowOnSend = true;
            a.Dispose();

            Assert.That(pool.LeakedSlotCount, Is.EqualTo(1), "leaked slot retired");

            // Effective capacity is now 1 (one permit permanently retained): with b still held, a new
            // borrow must time out.
            Assert.Throws<IngressError>(() => pool.Borrow());

            // Returning b frees the one remaining permit; a single concurrent borrow now succeeds...
            b.Dispose();
            var c = pool.Borrow();
            // ...but a second concurrent borrow still cannot (max shrank to 1).
            Assert.Throws<IngressError>(() => pool.Borrow());
            c.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapRetiringLeakedSlotShrinksEffectiveCapacity()
    {
        // Regression for the reap-path accounting bug: a reaped idle sender that fails to release its
        // slot lock must retire its index AND shrink effective capacity, so the semaphore never
        // advertises more in-use slots than the bitmap can allocate.
        var pool = MakeSfPool("sender_pool_min=0;sender_pool_max=2;idle_timeout_ms=1;acquire_timeout_ms=150;", out var created);
        try
        {
            var a = pool.Borrow(); // slot 0
            var b = pool.Borrow(); // slot 1
            a.Dispose();
            b.Dispose();

            // Slot 0's sender will not release its lock when reaped -> its index is retired.
            created.Single(s => s.SlotIndex == 0).SlotLockReleased = false;
            Thread.Sleep(25);
            pool.ReapIdle();

            Assert.That(pool.LeakedSlotCount, Is.EqualTo(1));

            // Effective max is now 1: one concurrent borrow works; the second fails with the DOCUMENTED
            // PoolExhausted error, not an internal "no free slot index".
            var c = pool.Borrow();
            var ex = Assert.Throws<IngressError>(() => pool.Borrow());
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.PoolExhausted));
            c.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }

    [TestCase("default-0", "default", 4, true)]
    [TestCase("default-3", "default", 4, true)]
    [TestCase("default-4", "default", 4, false)] // out of managed range -> a true orphan
    [TestCase("default", "default", 4, false)] // no -index suffix
    [TestCase("default-x", "default", 4, false)] // non-numeric suffix
    [TestCase("other-1", "default", 4, false)] // different base
    [TestCase("default-1", null, 0, false)] // no managed family configured
    public void OrphanScannerIdentifiesManagedSlots(string senderId, string? managedBase, int managedCount, bool expected)
    {
        Assert.That(QwpOrphanScanner.IsManagedSlot(senderId, managedBase, managedCount), Is.EqualTo(expected));
    }
}
