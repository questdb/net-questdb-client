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

#if NET7_0_OR_GREATER
using System.Collections.Concurrent;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Pooling;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

public class QueryClientPoolTests
{
    private static QueryClientPool MakePool(string keys, out ConcurrentBag<FakeQueryClient> created)
    {
        var bag = new ConcurrentBag<FakeQueryClient>();
        created = bag;
        var options = new SenderOptions("ws::addr=localhost:9000;" + keys);
        return new QueryClientPool(options, null, () =>
        {
            var c = new FakeQueryClient();
            bag.Add(c);
            return new ValueTask<IQwpQueryClient>(c);
        });
    }

    [Test]
    public void PreWarmsMinClients()
    {
        using var g = new PoolGuard(MakePool("query_pool_min=2;query_pool_max=4;", out var created));
        Assert.Multiple(() =>
        {
            Assert.That(g.Pool.TotalSize, Is.EqualTo(2));
            Assert.That(g.Pool.AvailableSize, Is.EqualTo(2));
            Assert.That(created, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void BorrowReturnRecyclesSameDecorator()
    {
        using var g = new PoolGuard(MakePool("query_pool_min=1;query_pool_max=1;", out var created));

        var c1 = g.Pool.Borrow();
        c1.Dispose();
        var c2 = g.Pool.Borrow();

        Assert.That(c2, Is.SameAs(c1));
        Assert.That(created, Has.Count.EqualTo(1), "no new underlying client created on reuse");
        c2.Dispose();
    }

    [Test]
    public void BorrowAndReturnTrackSizes()
    {
        using var g = new PoolGuard(MakePool("query_pool_min=1;query_pool_max=2;", out _));

        var c = g.Pool.Borrow();
        Assert.Multiple(() =>
        {
            Assert.That(g.Pool.AvailableSize, Is.EqualTo(0));
            Assert.That(g.Pool.TotalSize, Is.EqualTo(1));
        });

        c.Dispose();
        Assert.Multiple(() =>
        {
            Assert.That(g.Pool.AvailableSize, Is.EqualTo(1));
            Assert.That(g.Pool.TotalSize, Is.EqualTo(1));
        });
    }

    [Test]
    public void ExhaustedBorrowThrowsWithQueryPoolMaxInMessage()
    {
        using var g = new PoolGuard(MakePool("query_pool_min=0;query_pool_max=1;acquire_timeout_ms=150;", out _));

        var held = g.Pool.Borrow();
        var ex = Assert.Throws<IngressError>(() => g.Pool.Borrow());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.PoolExhausted));
            Assert.That(ex.Message, Does.Contain("query_pool_max=1"));
        });
        held.Dispose();
    }

    [Test]
    public void ClosedPoolBorrowThrows()
    {
        var pool = MakePool("query_pool_min=0;query_pool_max=2;", out _);
        pool.Close();

        var ex = Assert.Throws<IngressError>(() => pool.Borrow());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
    }

    [Test]
    public void DiscardBrokenRemovesAndRestoresCapacity()
    {
        using var g = new PoolGuard(MakePool("query_pool_min=0;query_pool_max=1;acquire_timeout_ms=150;", out var created));

        var c = (PooledQueryClient)g.Pool.Borrow();
        c.MarkBroken();
        c.Dispose(); // broken -> discard

        Assert.Multiple(() =>
        {
            Assert.That(g.Pool.TotalSize, Is.EqualTo(0), "broken client removed from the pool");
            Assert.That(created.Single().Disposed, Is.True, "broken client's inner is disposed");
        });

        // Capacity restored (no leak-debt analog): a fresh borrow succeeds and creates a new client.
        var c2 = g.Pool.Borrow();
        Assert.That(created, Has.Count.EqualTo(2));
        c2.Dispose();
    }

    [Test]
    public void TerminalClientIsDiscardedOnReturn()
    {
        using var g = new PoolGuard(MakePool("query_pool_min=0;query_pool_max=2;", out var created));

        var c = g.Pool.Borrow();
        created.Single().TerminalOrDisposed = true; // becomes terminal without throwing
        c.Dispose(); // belt-and-braces -> discard, not re-pool

        Assert.Multiple(() =>
        {
            Assert.That(g.Pool.AvailableSize, Is.EqualTo(0));
            Assert.That(g.Pool.TotalSize, Is.EqualTo(0));
            Assert.That(created.Single().Disposed, Is.True);
        });
    }

    [Test]
    public void ReapIdleReapsDownToMin()
    {
        using var g = new PoolGuard(MakePool("query_pool_min=1;query_pool_max=3;idle_timeout_ms=1;", out _));

        var a = g.Pool.Borrow();
        var b = g.Pool.Borrow();
        var c = g.Pool.Borrow();
        a.Dispose();
        b.Dispose();
        c.Dispose();
        Assert.That(g.Pool.AvailableSize, Is.EqualTo(3));

        Thread.Sleep(30); // exceed idle_timeout_ms
        g.Pool.ReapIdle();

        Assert.That(g.Pool.TotalSize, Is.EqualTo(1), "reaped idle clients down to min");
    }

    [Test]
    public void CloseDuringCreateDisposesFreshClientAndRestoresCapacity()
    {
        var bag = new ConcurrentBag<FakeQueryClient>();
        var options = new SenderOptions("ws::addr=localhost:9000;query_pool_min=0;query_pool_max=1;");
        QueryClientPool? poolRef = null;
        var pool = new QueryClientPool(options, null, () =>
        {
            // Simulate the pool closing while a client is being created.
            poolRef!.Close();
            var c = new FakeQueryClient();
            bag.Add(c);
            return new ValueTask<IQwpQueryClient>(c);
        });
        poolRef = pool;

        var ex = Assert.Throws<IngressError>(() => pool.Borrow());
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
            Assert.That(bag.Single().Disposed, Is.True, "the client created during the close race is disposed");
        });
    }

    [Test]
    public void CloseCancelsBorrowedClientAndLeavesTeardownToBorrower()
    {
        // Close disposes only idle clients; an in-flight one is merely cancelled (so a blocked
        // ExecuteAsync unwinds) and torn down by its borrower on return — never disposed by Close, which
        // would race the borrower's still-running query on the non-thread-safe client.
        var pool = MakePool("query_pool_min=2;query_pool_max=2;", out var created);

        var borrowed = pool.Borrow(); // 1 in-use, 1 idle

        pool.Close();

        var inUse = created.Single(c => !c.Disposed); // the borrowed client survived close (only cancelled)
        Assert.Multiple(() =>
        {
            Assert.That(created.Count(c => c.Disposed), Is.EqualTo(1), "idle client disposed by close");
            Assert.That(inUse.CancelCount, Is.EqualTo(1), "in-flight client cancelled so a blocked query unwinds");
            Assert.That(inUse.DisposeCount, Is.EqualTo(0), "close did not dispose the in-use client");
        });

        borrowed.Dispose(); // borrower returns post-close: teardown happens here, on this thread

        Assert.That(inUse.DisposeCount, Is.EqualTo(1), "inner disposed exactly once, by the borrower");

        Assert.DoesNotThrow(() => pool.Close(), "second close is idempotent");
    }

    private sealed class PoolGuard : IDisposable
    {
        public PoolGuard(QueryClientPool pool) => Pool = pool;
        public QueryClientPool Pool { get; }
        public void Dispose() => Pool.Close();
    }
}
#endif
