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

public class QueryHandleTests
{
    private static QuestDBClientImpl MakeHandle(string keys, out ConcurrentBag<FakeQueryClient> created)
    {
        var bag = new ConcurrentBag<FakeQueryClient>();
        created = bag;
        var options = new SenderOptions("ws::addr=localhost:9000;sender_pool_min=0;" + keys);
        return new QuestDBClientImpl(
            options,
            slot => new FakeSender(slot),
            () =>
            {
                var c = new FakeQueryClient();
                bag.Add(c);
                return new ValueTask<IQwpQueryClient>(c);
            });
    }

    [Test]
    public async Task NewQueryExecutesAndReturnsClient()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);

        await h.NewQuery().Sql("select 1").Handler(new NoopQueryHandler()).ExecuteAsync();

        var c = created.Single();
        Assert.Multiple(() =>
        {
            Assert.That(c.ExecuteCount, Is.EqualTo(1));
            Assert.That(c.LastSql, Is.EqualTo("select 1"));
            Assert.That(c.Disposed, Is.False);
            Assert.That(h.AvailableQueryClientCount, Is.EqualTo(1), "client returned to the pool");
        });
    }

    [Test]
    public async Task ExecuteSqlAsyncIsEquivalentToNewQuery()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);

        await h.ExecuteSqlAsync("select 2", new NoopQueryHandler());

        Assert.That(created.Single().LastSql, Is.EqualTo("select 2"));
    }

    [Test]
    public void MissingSqlThrows()
    {
        using var h = MakeHandle("query_pool_min=0;query_pool_max=1;", out _);

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await h.NewQuery().Handler(new NoopQueryHandler()).ExecuteAsync());
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
            Assert.That(ex.Message, Does.Contain("sql is required"));
        });
    }

    [Test]
    public void MissingHandlerThrows()
    {
        using var h = MakeHandle("query_pool_min=0;query_pool_max=1;", out _);

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await h.NewQuery().Sql("x").ExecuteAsync());
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
            Assert.That(ex.Message, Does.Contain("handler is required"));
        });
    }

    [Test]
    public async Task SingleFlightOverlapThrows()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);
        var fake = created.Single();
        fake.Gate = new TaskCompletionSource<bool>();

        var q = h.NewQuery().Sql("x").Handler(new NoopQueryHandler());
        var inFlight = q.ExecuteAsync();

        var ex = Assert.ThrowsAsync<IngressError>(async () => await q.ExecuteAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));

        fake.Gate.SetResult(true);
        await inFlight;
    }

    [Test]
    public async Task ThrowingQueryDiscardsClientThenNextBorrowCreatesFresh()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=2;", out var created);
        var first = created.Single();
        first.ThrowOnExecute = true;

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await h.NewQuery().Sql("x").Handler(new NoopQueryHandler()).ExecuteAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));

        Assert.Multiple(() =>
        {
            Assert.That(first.Disposed, Is.True, "failed client is discarded");
            Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
        });

        await h.NewQuery().Sql("y").Handler(new NoopQueryHandler()).ExecuteAsync();
        Assert.That(created, Has.Count.EqualTo(2), "next borrow creates a fresh client");
    }

    [Test]
    public void CtCancelDiscardsClientAndSurfacesOce()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=2;", out var created);
        var first = created.Single();
        first.CancelOnExecute = true;

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await h.NewQuery().Sql("x").Handler(new NoopQueryHandler()).ExecuteAsync());

        Assert.Multiple(() =>
        {
            Assert.That(first.Disposed, Is.True, "hard-cancelled client is discarded");
            Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task CooperativeCancelEndsCleanlyAndRepools()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);
        var fake = created.Single();
        fake.Gate = new TaskCompletionSource<bool>();

        var q = h.NewQuery().Sql("x").Handler(new NoopQueryHandler());
        var inFlight = q.ExecuteAsync();
        q.Cancel();                 // cooperative: posts a CANCEL frame
        fake.Gate.SetResult(true);  // query then completes normally
        await inFlight;

        Assert.Multiple(() =>
        {
            Assert.That(fake.CancelCount, Is.EqualTo(1));
            Assert.That(fake.Disposed, Is.False, "cleanly-ending cooperative cancel re-pools the client");
            Assert.That(h.AvailableQueryClientCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task CancelAfterCompletionDoesNotCancelReborrowedClient()
    {
        // Pool size 1: q2 re-borrows the exact same inner client q1 just returned. A late Cancel() on the
        // already-completed q1 must not forward to that re-borrowed client and abort q2's in-flight query.
        // Guards the lease null-out in Query.ExecuteAsync's finally (the only thing scoping a cancel to its
        // own execution — there is no generation token).
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);
        var fake = created.Single();

        var q1 = h.NewQuery().Sql("a").Handler(new NoopQueryHandler());
        await q1.ExecuteAsync(); // completes cleanly; client re-pooled, q1's lease nulled

        fake.Gate = new TaskCompletionSource<bool>();
        var q2 = h.NewQuery().Sql("b").Handler(new NoopQueryHandler());
        var t2 = q2.ExecuteAsync(); // re-borrows the same inner; parks in flight on the gate

        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (Volatile.Read(ref fake.ExecuteCount) < 2 && sw.ElapsedMilliseconds < 2000)
        {
            await Task.Delay(5);
        }

        Assert.That(fake.ExecuteCount, Is.EqualTo(2), "q2 re-borrowed the same client and is in flight");

        q1.Cancel(); // late cancel on the finished query — must be a no-op, not reach q2's client

        Assert.That(fake.CancelCount, Is.EqualTo(0),
            "a cancel on a completed query must not reach the re-borrowed client running q2");

        fake.Gate.SetResult(true);
        await t2;

        Assert.That(fake.CancelCount, Is.EqualTo(0), "q2 completed without a stray cancel");
    }

    [Test]
    public async Task ConcurrentNewQueriesGetDistinctClients()
    {
        // Shared gate keeps every borrowed client in flight so the pool must hand out distinct ones.
        var gate = new TaskCompletionSource<bool>();
        var bag = new ConcurrentBag<FakeQueryClient>();
        var options = new SenderOptions(
            "ws::addr=localhost:9000;sender_pool_min=0;query_pool_min=0;query_pool_max=3;acquire_timeout_ms=2000;");
        using var h = new QuestDBClientImpl(
            options,
            slot => new FakeSender(slot),
            () =>
            {
                var c = new FakeQueryClient { Gate = gate };
                bag.Add(c);
                return new ValueTask<IQwpQueryClient>(c);
            });

        var tasks = new List<Task>();
        for (var i = 0; i < 3; i++)
        {
            tasks.Add(h.NewQuery().Sql("q").Handler(new NoopQueryHandler()).ExecuteAsync());
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (bag.Count < 3 && sw.ElapsedMilliseconds < 2000)
        {
            await Task.Delay(5);
        }

        Assert.That(h.TotalQueryClientCount, Is.EqualTo(3), "three concurrent queries borrowed three distinct clients");
        gate.SetResult(true);
        await Task.WhenAll(tasks);
    }

    [Test]
    public void DisposeClosesQueryPool()
    {
        var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out _);
        h.Dispose();

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await h.NewQuery().Sql("x").Handler(new NoopQueryHandler()).ExecuteAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
    }

    [Test]
    public async Task QueryReusableAfterPoolExhausted()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;acquire_timeout_ms=100;", out var created);
        var fake = created.Single();
        fake.Gate = new TaskCompletionSource<bool>();

        var q1 = h.NewQuery().Sql("a").Handler(new NoopQueryHandler());
        var t1 = q1.ExecuteAsync(); // holds the only client (gated)

        var q2 = h.NewQuery().Sql("b").Handler(new NoopQueryHandler());
        var ex = Assert.ThrowsAsync<IngressError>(async () => await q2.ExecuteAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.PoolExhausted));

        fake.Gate.SetResult(true);
        await t1;

        // q2 is not poisoned by the earlier exhaustion: a retry succeeds.
        await q2.ExecuteAsync();
        Assert.That(fake.LastSql, Is.EqualTo("b"));
    }
}
#endif
