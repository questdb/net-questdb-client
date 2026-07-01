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
            created.Single(s => s.SlotIndex == 1).ThrowOnClear = true;
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
            fakeA.ThrowOnClear = true;
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

    [Test]
    public void ReclaimRestoresCapacityWhenRetiredSlotLockReleases()
    {
        // Fix for the deferred-teardown capacity-shrink bug: a slot retired because its lock had not yet
        // released at dispose time must be reclaimed (index + permit restored) once the lock releases, so
        // a transient / wedged teardown does not permanently shrink effective max toward PoolExhausted.
        var pool = MakeSfPool("sender_pool_min=0;sender_pool_max=2;acquire_timeout_ms=150;", out var created);
        try
        {
            var a = pool.Borrow(); // slot 0
            var b = pool.Borrow(); // slot 1

            // Slot-0 sender breaks AND has not released its lock yet -> the index is retired and effective
            // max shrinks to 1 (mirrors a deferred engine teardown still holding the slot lock).
            var fakeA = created.Single(s => s.SlotIndex == 0);
            fakeA.SlotLockReleased = false;
            fakeA.ThrowOnClear = true;
            a.Dispose();

            Assert.That(pool.LeakedSlotCount, Is.EqualTo(1), "slot retired while lock held");

            // Reclaim is a no-op while the lock is still held.
            pool.ReclaimRetiredSlots();
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(1), "not reclaimed while lock held");

            // The deferred teardown finally releases the lock; the housekeeper sweep reclaims the slot.
            fakeA.SlotLockReleased = true;
            pool.ReclaimRetiredSlots();
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(0), "retired slot reclaimed once lock released");

            // Effective max is back to 2: with b still held, a fresh borrow now succeeds and reuses the
            // reclaimed lowest-free index 0 — no permanent shrink.
            var c = pool.Borrow();
            Assert.That(c.SlotIndex, Is.EqualTo(0), "reclaimed index reused");
            c.Dispose();
            b.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReclaimRestoresCapacityForReapedRetiredSlot()
    {
        // Same reclaim, but via the reap path (idle sender whose lock was held when reaped). Exercises the
        // leak-debt branch where the retired permit was paid from an available permit (debt settled), so
        // reclaim must Release a real permit rather than just cancel pending debt.
        var pool = MakeSfPool("sender_pool_min=0;sender_pool_max=2;idle_timeout_ms=1;acquire_timeout_ms=150;", out var created);
        try
        {
            var a = pool.Borrow(); // slot 0
            var b = pool.Borrow(); // slot 1
            a.Dispose();
            b.Dispose();

            created.Single(s => s.SlotIndex == 0).SlotLockReleased = false;
            Thread.Sleep(25);
            pool.ReapIdle();
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(1));

            // Lock releases later; reclaim restores effective max to 2 (two concurrent borrows now work).
            created.Single(s => s.SlotIndex == 0).SlotLockReleased = true;
            pool.ReclaimRetiredSlots();
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(0));

            var c = pool.Borrow();
            var d = pool.Borrow();
            Assert.That(new[] { c.SlotIndex, d.SlotIndex }.OrderBy(i => i), Is.EqualTo(new[] { 0, 1 }));
            c.Dispose();
            d.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ConcurrentBorrowDuringReapDisposeWindowDoesNotSpuriouslyThrow()
    {
        // Regression: a reaped idle SF sender is removed from _all/_available under the pool lock, but its
        // slot index is freed only AFTER the (slow WS+SF) DisposeInner runs outside the lock. A reaped
        // sender holds no capacity permit, so during that dispose window the semaphore used to advertise a
        // free permit whose slot index was still locked -> a concurrent Borrow acquired the permit, found no
        // free index (AllocateSlotIndex -> -1), and threw a spurious PoolExhausted. The fix withholds the
        // permit for the whole window, so the borrow waits for real capacity instead of failing.
        var pool = MakeSfPool(
            "sender_pool_min=1;sender_pool_max=2;idle_timeout_ms=1;acquire_timeout_ms=5000;", out var created);
        BorrowedSender? held = null;
        try
        {
            held = pool.Borrow(); // slot 0 stays borrowed (the "min" sender), so reap can't touch it
            var warm = pool.Borrow(); // slot 1
            warm.Dispose(); // slot 1 now idle and reapable

            var slot1 = created.Single(s => s.SlotIndex == 1);
            slot1.DisposeEntered = new ManualResetEventSlim(false);
            slot1.DisposeGate = new ManualResetEventSlim(false);

            Thread.Sleep(25); // exceed idle_timeout so slot 1 is over-idle

            var reap = Task.Run(() => pool.ReapIdle());
            Assert.That(slot1.DisposeEntered.Wait(TimeSpan.FromSeconds(5)), Is.True,
                "reap reached the (blocked) dispose window: slot 1 out of _all/_available, index still held");

            // The window is now open — slot 1's index is reserved while its dispose is parked. Race a borrow.
            var borrow = Task.Run(() => pool.Borrow());

            // Before the fix the borrow threw immediately (faulting the task); after it, the borrow must be
            // parked waiting for real capacity to come back.
            Thread.Sleep(300);
            Assert.That(borrow.IsCompleted, Is.False,
                "borrow waits for capacity instead of spuriously throwing PoolExhausted");

            slot1.DisposeGate.Set(); // let the teardown finish -> slot index + withheld permit reclaimed
            Assert.That(reap.Wait(TimeSpan.FromSeconds(5)), Is.True, "reap completed");
            Assert.That(borrow.Wait(TimeSpan.FromSeconds(5)), Is.True, "borrow completed once capacity freed");

            var s2 = borrow.Result; // throws if the borrow faulted (the pre-fix behaviour)
            Assert.That(s2.SlotIndex, Is.EqualTo(1), "reclaimed slot index reused");
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(0));
            s2.Dispose();
        }
        finally
        {
            held?.Dispose();
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
