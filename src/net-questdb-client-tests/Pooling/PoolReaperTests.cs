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
using System.Diagnostics;
using NUnit.Framework;
using QuestDB;
using QuestDB.Pooling;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

public class PoolReaperTests
{
    private static SenderPool MakePool(string keys, out ConcurrentBag<FakeSender> created)
    {
        var bag = new ConcurrentBag<FakeSender>();
        created = bag;
        var options = new SenderOptions("http::addr=localhost:9000;" + keys);
        return new SenderPool(options, null, slot =>
        {
            var s = new FakeSender(slot);
            bag.Add(s);
            return s;
        });
    }

    // Borrow `count` senders then immediately return them, leaving them idle in the pool.
    private static void Churn(SenderPool pool, int count)
    {
        var borrowed = new List<ISender>(count);
        for (var i = 0; i < count; i++)
        {
            borrowed.Add(pool.Borrow());
        }

        foreach (var s in borrowed)
        {
            s.Dispose();
        }
    }

    [Test]
    public void ReapIdleShrinksToMin()
    {
        var pool = MakePool("sender_pool_min=1;sender_pool_max=4;idle_timeout_ms=1;", out var created);
        try
        {
            Churn(pool, 4);
            Thread.Sleep(25);
            pool.ReapIdle();

            Assert.Multiple(() =>
            {
                Assert.That(pool.TotalSize, Is.EqualTo(1));
                Assert.That(pool.AvailableSize, Is.EqualTo(1));
                Assert.That(created.Count(s => s.Disposed), Is.EqualTo(created.Count - 1));
            });
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapIdleRespectsMin()
    {
        var pool = MakePool("sender_pool_min=2;sender_pool_max=4;idle_timeout_ms=1;", out _);
        try
        {
            Churn(pool, 4);
            Thread.Sleep(25);
            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(2));
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapIdleKeepsSendersWithinTimeout()
    {
        // idle_timeout defaults to 60s; freshly-returned senders are not reaped.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=4;", out _);
        try
        {
            Churn(pool, 3);
            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(3));
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapIdleNeverTouchesInUseSenders()
    {
        var pool = MakePool("sender_pool_min=0;sender_pool_max=4;idle_timeout_ms=1;", out _);
        try
        {
            var held = pool.Borrow();
            Thread.Sleep(25);
            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(1), "the in-use sender is never reaped");
            held.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapIdleByMaxLifetime()
    {
        // idle_timeout is long, but max_lifetime is tiny: an over-age but recently-returned sender is recycled.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;idle_timeout_ms=600000;max_lifetime_ms=1;", out _);
        try
        {
            Churn(pool, 2);
            Thread.Sleep(25);
            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(0));
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapIdleSurvivesDelegateDisposeError()
    {
        var pool = MakePool("sender_pool_min=0;sender_pool_max=3;idle_timeout_ms=1;", out var created);
        try
        {
            Churn(pool, 3);
            foreach (var s in created)
            {
                s.ThrowOnDispose = true;
            }

            Thread.Sleep(25);
            Assert.DoesNotThrow(() => pool.ReapIdle());
            Assert.That(pool.TotalSize, Is.EqualTo(0), "reap removed all idle senders despite dispose faults");
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapIdleSkipsSendersWithUnAckedData()
    {
        var pool = MakePool("sender_pool_min=0;sender_pool_max=4;idle_timeout_ms=1;", out var created);
        try
        {
            Churn(pool, 3);
            foreach (var s in created)
            {
                s.FullyDrained = false; // ring still holds un-acked frames
            }

            Thread.Sleep(25);
            pool.ReapIdle();

            Assert.Multiple(() =>
            {
                Assert.That(pool.TotalSize, Is.EqualTo(3), "un-drained senders are never reaped, even over idle timeout");
                Assert.That(created.Count(s => s.Disposed), Is.EqualTo(0));
            });

            // Once the rings drain, the next sweep reaps them (idle clock started at drain).
            foreach (var s in created)
            {
                s.FullyDrained = true;
            }

            Thread.Sleep(25);
            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(0), "drained senders reap normally on the next sweep");
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapIdleByMaxLifetimeSkipsUnAckedData()
    {
        // Even the max-lifetime path must not drop un-acked data: an over-age but un-drained sender survives.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;idle_timeout_ms=600000;max_lifetime_ms=1;", out var created);
        try
        {
            Churn(pool, 2);
            foreach (var s in created)
            {
                s.FullyDrained = false;
            }

            Thread.Sleep(25);
            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(2), "over-age senders with un-acked data are not aged out");

            foreach (var s in created)
            {
                s.FullyDrained = true;
            }

            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(0), "once drained, the over-age sender is recycled");
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void ReapStopsAtFirstEntryWithinIdleTimeout()
    {
        // The idle deque is sorted by idle time (borrowers pop/push the hot end), so the sweep scans from
        // the cold end and stops at the first entry inside its timeout: the long-idle sender is reaped,
        // the freshly-returned one behind it survives without being visited. Margins are ~1.5s so a
        // loaded CI runner cannot tip the young entry over the timeout before the sweep runs.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;idle_timeout_ms=1500;", out var created);
        try
        {
            var a = pool.Borrow();
            var b = pool.Borrow();
            var fakes = created.ToArray();
            a.Dispose(); // cold: idles past the timeout below
            Thread.Sleep(2000);
            b.Dispose(); // hot: freshly idle

            pool.ReapIdle();

            Assert.Multiple(() =>
            {
                Assert.That(pool.TotalSize, Is.EqualTo(1), "only the over-idle cold entry is reaped");
                Assert.That(fakes.Count(s => s.Disposed), Is.EqualTo(1));
            });
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void OverAgeSenderReapedDespiteYoungerColdEntryInFront()
    {
        // The deque is idle-sorted, not age-sorted, so an over-age sender at the hot end hides behind
        // the younger-created (but colder) entry the idle sweep stops at. The max_lifetime walk (gated
        // on the oldest parked CreatedAtUtc) must reap it anyway — and leave the young entry alone.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;idle_timeout_ms=600000;max_lifetime_ms=1500;", out var created);
        try
        {
            var a = pool.Borrow(); // created now; held past max_lifetime
            var fakeA = created.Single();
            Thread.Sleep(2000);
            var b = pool.Borrow(); // created young
            var fakeB = created.Single(s => !ReferenceEquals(s, fakeA));
            b.Dispose(); // young entry at the cold end
            a.Dispose(); // over-age, at the hot end behind it

            pool.ReapIdle();

            Assert.Multiple(() =>
            {
                Assert.That(pool.TotalSize, Is.EqualTo(1), "over-age entry reaped despite returning last");
                Assert.That(fakeA.Disposed, Is.True, "the over-age sender was the one reaped");
                Assert.That(fakeB.Disposed, Is.False, "the young sender survives");
            });
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void SenderCrossingMaxLifetimeWhileParkedIsReaped()
    {
        // Regression: an entry that goes over-age only AFTER being returned sits at the hot end where
        // the cold-end idle sweep never inspects it. The _idleOldestCreatedUtc-gated walk must catch it
        // — reaping the aged sender, not the younger cold entry in front of it.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;idle_timeout_ms=600000;max_lifetime_ms=3000;", out var created);
        try
        {
            var a = pool.Borrow(); // A created at t0
            var fakeA = created.Single();
            Thread.Sleep(1200);
            var b = pool.Borrow(); // B created young, at t1200
            var fakeB = created.Single(s => !ReferenceEquals(s, fakeA));
            b.Dispose(); // B parks first: cold end
            a.Dispose(); // A parks under-age (~1.2s < 3s): hot end, NOT over-age yet

            pool.ReapIdle();
            Assert.That(pool.TotalSize, Is.EqualTo(2), "nothing over-age or over-idle yet");

            Thread.Sleep(2400); // t3600: A (3.6s) crossed max_lifetime while parked; B (2.4s) still young

            pool.ReapIdle();

            Assert.Multiple(() =>
            {
                Assert.That(pool.TotalSize, Is.EqualTo(1), "the entry that aged while parked is reaped");
                Assert.That(fakeA.Disposed, Is.True, "A crossed max_lifetime while parked");
                Assert.That(fakeB.Disposed, Is.False, "young B survives at the cold end");
            });
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void HousekeeperReapsInBackground()
    {
        var bag = new ConcurrentBag<FakeSender>();
        var options = new SenderOptions(
            "http::addr=localhost:9000;sender_pool_min=0;sender_pool_max=2;" +
            "idle_timeout_ms=1;housekeeper_interval_ms=100;");
        using var client = new QuestDBClientImpl(options, slot =>
        {
            var s = new FakeSender(slot);
            bag.Add(s);
            return s;
        });

        Churn2(client, 2);
        Assert.That(client.TotalSenderCount, Is.EqualTo(2));

        var sw = Stopwatch.StartNew();
        while (client.TotalSenderCount > 0 && sw.ElapsedMilliseconds < 3000)
        {
            Thread.Sleep(50);
        }

        Assert.That(client.TotalSenderCount, Is.EqualTo(0), "background housekeeper reaped idle senders");
    }

    private static void Churn2(IQuestDBClient client, int count)
    {
        var borrowed = new List<ISender>(count);
        for (var i = 0; i < count; i++)
        {
            borrowed.Add(client.BorrowSender());
        }

        foreach (var s in borrowed)
        {
            s.Dispose();
        }
    }
}
