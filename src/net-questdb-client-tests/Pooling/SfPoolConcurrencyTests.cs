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
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

/// <summary>
///     Store-and-forward (ws + sf_dir) concurrency stress for the slot-index machinery —
///     <c>AllocateSlotIndex</c> / <c>FreeSlotIndex</c> / <c>RetireSlotIndex</c> /
///     <c>SettleLeakDebtLocked</c> / <c>ReclaimRetiredSlots</c> and the leak-debt branch of
///     <c>ReleaseCapacity</c>. That code is the most race-prone in the pool, yet the rest of the pool's
///     multi-threaded coverage runs only against HTTP pools, which carry no slot machinery at all
///     (<see cref="net_questdb_client_tests.Pooling.PoolSlotTests" /> exercises these paths only
///     single-threaded). These tests drive the same paths under contention and assert the two
///     invariants the machinery must never break: (1) no two concurrently-borrowed senders ever share a
///     slot index, and (2) after a retire/reclaim storm fully quiesces, effective capacity is restored
///     to exactly <c>max</c> — no permanent shrink, no over-release.
/// </summary>
public class SfPoolConcurrencyTests
{
    private const int Max = 8;

    // SF pool over the fake-sender seam: ws + sf_dir flips the pool into store-and-forward mode (slot
    // indices, leak debt) without touching a socket or the filesystem. `ownerByIndex[i]` is the fake
    // currently backing slot i — written by the factory at creation; since a slot index is held
    // continuously by one PooledSender entry until that entry is discarded/reaped, a borrower can map its
    // slot index back to its inner fake (to drive retire) with no race.
    private static SenderPool MakeSfPool(
        string keys, out ConcurrentBag<FakeSender> created, out FakeSender?[] ownerByIndex)
    {
        var bag = new ConcurrentBag<FakeSender>();
        var owners = new FakeSender?[Max];
        created = bag;
        ownerByIndex = owners;
        var options = new SenderOptions("ws::addr=localhost:9000;sf_dir=/tmp/qdb-sf-pool-stress;" + keys);
        return new SenderPool(options, null, slot =>
        {
            var s = new FakeSender(slot);
            bag.Add(s);
            if (slot >= 0 && slot < owners.Length)
            {
                Volatile.Write(ref owners[slot], s);
            }

            return s;
        });
    }

    // Asserts effective capacity is exactly `expected`: that many borrows succeed with distinct indices
    // in [0, Max), and one more times out with the documented PoolExhausted error. Leaves the pool empty.
    private static void AssertEffectiveCapacity(SenderPool pool, int expected)
    {
        var held = new List<BorrowedSender>(expected);
        for (var i = 0; i < expected; i++)
        {
            held.Add(pool.Borrow());
        }

        var indices = held.Select(h => h.SlotIndex).OrderBy(i => i).ToArray();
        Assert.That(indices, Is.EqualTo(Enumerable.Range(0, expected)),
            "every live SF sender holds a distinct lowest-fill slot index");

        var ex = Assert.Throws<IngressError>(() => pool.Borrow());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.PoolExhausted), "capacity is exactly the restored max");

        foreach (var h in held)
        {
            h.Dispose();
        }
    }

    [Test]
    public void SfConcurrentCleanChurnNeverDuplicatesLiveSlotIndex()
    {
        // Pure borrow/return churn (no breaks) hammering AllocateSlotIndex during pool growth and
        // FreeSlotIndex is never hit on a clean return, but the bitmap scan/realloc under TakeOrCreate is.
        // Two concurrently-borrowed senders sharing an index would be a correctness bug.
        var pool = MakeSfPool("sender_pool_min=0;sender_pool_max=8;acquire_timeout_ms=5000;", out _, out _);
        var liveSlot = new int[Max];
        var errors = new ConcurrentQueue<string>();
        try
        {
            Parallel.For(0, 6000, new ParallelOptions { MaxDegreeOfParallelism = 16 }, _ =>
            {
                var ps = pool.Borrow();
                var idx = ps.SlotIndex;
                if (idx < 0 || idx >= Max)
                {
                    errors.Enqueue($"slot index out of range: {idx}");
                }
                else if (Interlocked.Exchange(ref liveSlot[idx], 1) != 0)
                {
                    errors.Enqueue($"two live senders share slot index {idx}");
                }

                Thread.SpinWait(100);

                if (idx >= 0 && idx < Max)
                {
                    Interlocked.Exchange(ref liveSlot[idx], 0);
                }

                ps.Dispose();
            });

            Assert.That(errors, Is.Empty);
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(0));
            Assert.That(pool.TotalSize, Is.LessThanOrEqualTo(Max));

            // Clean churn must not have drifted capacity: exactly Max distinct slots remain borrowable.
            AssertEffectiveCapacity(pool, Max);
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void SfConcurrentRetireAndReclaimKeepsCapacityAccountingConsistent()
    {
        // The core race: workers retire slots (discard with the slot lock still held -> RetireSlotIndex /
        // SettleLeakDebtLocked, shrinking effective capacity) while a housekeeper thread releases those
        // locks and reclaims the indices (ReclaimRetiredSlots, restoring capacity). Retire and reclaim
        // mutate _retired / _leakedSlots / _leakDebt and drain/release the capacity semaphore from
        // different threads continuously. The invariant: once everything quiesces and all retired slots
        // are reclaimed, effective capacity is back to exactly Max — leak debt nets to zero, no permit is
        // lost or double-released.
        var pool = MakeSfPool(
            "sender_pool_min=0;sender_pool_max=8;acquire_timeout_ms=200;idle_timeout_ms=1;",
            out var created, out var ownerByIndex);
        var liveSlot = new int[Max];
        var errors = new ConcurrentQueue<string>();
        var stop = false;

        try
        {
            var keeper = new Thread(() =>
            {
                while (!Volatile.Read(ref stop))
                {
                    foreach (var s in created.ToArray())
                    {
                        s.SlotLockReleased = true; // a deferred teardown finally drops the OS lock
                    }

                    try
                    {
                        pool.ReclaimRetiredSlots();
                        pool.ReapIdle();
                    }
                    catch (Exception e)
                    {
                        errors.Enqueue("keeper: " + e);
                    }

                    Thread.Sleep(1);
                }
            });

            var remaining = 6000;
            var workers = Enumerable.Range(0, 12).Select(_ => new Thread(() =>
            {
                while (Interlocked.Decrement(ref remaining) >= 0)
                {
                    BorrowedSender ps;
                    try
                    {
                        ps = pool.Borrow();
                    }
                    catch (IngressError)
                    {
                        continue; // transient capacity shrink -> PoolExhausted is expected, not a fault
                    }

                    var idx = ps.SlotIndex;
                    if (idx < 0 || idx >= Max)
                    {
                        errors.Enqueue($"slot index out of range: {idx}");
                        continue;
                    }

                    if (Interlocked.Exchange(ref liveSlot[idx], 1) != 0)
                    {
                        errors.Enqueue($"two live senders share slot index {idx}");
                    }

                    // ~1/3 of borrows retire: break the inner and keep its slot lock held so the discard
                    // retires the index (capacity shrink) rather than freeing it.
                    if (idx % 3 == 0)
                    {
                        var inner = Volatile.Read(ref ownerByIndex[idx]);
                        if (inner != null)
                        {
                            inner.SlotLockReleased = false;
                            inner.ThrowOnClear = true;
                        }
                    }

                    Thread.SpinWait(100);
                    Interlocked.Exchange(ref liveSlot[idx], 0);
                    ps.Dispose(); // clean return, or discard->retire when this borrow was marked above
                }
            })).ToArray();

            keeper.Start();
            foreach (var w in workers)
            {
                w.Start();
            }

            foreach (var w in workers)
            {
                Assert.That(w.Join(TimeSpan.FromSeconds(30)), Is.True, "worker finished");
            }

            Volatile.Write(ref stop, true);
            Assert.That(keeper.Join(TimeSpan.FromSeconds(30)), Is.True, "keeper finished");

            // Quiesce: every lock is now releasable, so a final reclaim sweep must drain every retired
            // slot back into circulation.
            foreach (var s in created.ToArray())
            {
                s.SlotLockReleased = true;
            }

            for (var i = 0; i < 16 && pool.LeakedSlotCount > 0; i++)
            {
                pool.ReclaimRetiredSlots();
            }

            Assert.That(errors, Is.Empty);
            Assert.That(pool.LeakedSlotCount, Is.EqualTo(0), "every retired slot reclaimed once locks released");

            // The decisive end-to-end check: capacity is restored to exactly Max. Under-counting leak debt
            // would leave it < Max (PoolExhausted before Max borrows); over-releasing would let a Max+1th
            // borrow through.
            AssertEffectiveCapacity(pool, Max);
        }
        finally
        {
            Volatile.Write(ref stop, true);
            pool.Close();
        }
    }

    [Test]
    public void SfConcurrentBorrowDiscardReapAndCloseStayConsistent()
    {
        // Close racing the full SF slot storm: borrows, retire-discards, reaping and reclaiming all in
        // flight when the handle closes. Close clears _retired while RetireSlotIndex appends to it, and
        // disposes the capacity semaphore under returning permits. Must never corrupt the live-slot set,
        // never surface an unexpected exception, and stay idempotent.
        var pool = MakeSfPool(
            "sender_pool_min=2;sender_pool_max=8;acquire_timeout_ms=2000;idle_timeout_ms=1;",
            out var created, out var ownerByIndex);
        var liveSlot = new int[Max];
        var errors = new ConcurrentQueue<string>();
        var stop = false;

        var keeper = new Thread(() =>
        {
            while (!Volatile.Read(ref stop))
            {
                foreach (var s in created.ToArray())
                {
                    s.SlotLockReleased = true;
                }

                try
                {
                    pool.ReapIdle();
                    pool.ReclaimRetiredSlots();
                }
                catch (Exception e)
                {
                    errors.Enqueue("keeper: " + e);
                }

                Thread.Sleep(1);
            }
        });

        var workers = Enumerable.Range(0, 10).Select(_ => new Thread(() =>
        {
            var n = 0;
            while (!Volatile.Read(ref stop))
            {
                BorrowedSender ps;
                try
                {
                    ps = pool.Borrow();
                }
                catch (IngressError)
                {
                    continue; // exhausted or closed: both expected during the storm / after close
                }

                var idx = ps.SlotIndex;
                if (idx < 0 || idx >= Max)
                {
                    errors.Enqueue($"slot index out of range: {idx}");
                    continue;
                }

                if (Interlocked.Exchange(ref liveSlot[idx], 1) != 0)
                {
                    errors.Enqueue($"two live senders share slot index {idx}");
                }

                if ((n++ & 1) == 0)
                {
                    var inner = Volatile.Read(ref ownerByIndex[idx]);
                    if (inner != null)
                    {
                        inner.SlotLockReleased = false;
                        inner.ThrowOnClear = true;
                    }
                }

                Interlocked.Exchange(ref liveSlot[idx], 0);
                try
                {
                    ps.Dispose();
                }
                catch (Exception e)
                {
                    errors.Enqueue("dispose: " + e);
                }
            }
        })).ToArray();

        keeper.Start();
        foreach (var w in workers)
        {
            w.Start();
        }

        Thread.Sleep(400); // let the storm build up retired slots and in-flight borrows
        pool.Close(); // race the close against the slot machinery
        Volatile.Write(ref stop, true);

        foreach (var w in workers)
        {
            Assert.That(w.Join(TimeSpan.FromSeconds(15)), Is.True, "worker drained after close");
        }

        Assert.That(keeper.Join(TimeSpan.FromSeconds(15)), Is.True, "keeper drained after close");

        Assert.That(errors, Is.Empty, "no slot corruption or unexpected exception under borrow/retire/reap/close");
        Assert.DoesNotThrow(() => pool.Close(), "close is idempotent");
    }
}
