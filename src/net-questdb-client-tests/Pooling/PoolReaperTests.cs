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
