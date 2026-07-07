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
using QuestDB.Qwp.Query;
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

        var reader = await h.NewQuery().Sql("select 1").ExecuteReaderAsync();
        Assert.That(await reader.ReadBatchAsync(), Is.False);
        await reader.DisposeAsync();

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
    public async Task HandleExecuteReaderAsyncIsEquivalentToNewQuery()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);

        var reader = await h.ExecuteReaderAsync("select 2");
        await reader.DisposeAsync();

        Assert.That(created.Single().LastSql, Is.EqualTo("select 2"));
    }

    [Test]
    public void MissingSqlThrows()
    {
        using var h = MakeHandle("query_pool_min=0;query_pool_max=1;", out _);

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await h.NewQuery().ExecuteReaderAsync());
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
            Assert.That(ex.Message, Does.Contain("sql is required"));
        });
    }

    [Test]
    public async Task SingleFlightOverlapThrows()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out _);

        var q = h.NewQuery().Sql("x");
        var reader = await q.ExecuteReaderAsync(); // reader open → single-flight slot held

        var ex = Assert.ThrowsAsync<IngressError>(async () => await q.ExecuteReaderAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));

        await reader.DisposeAsync();
    }

    [Test]
    public async Task ThrowingQueryDiscardsClientThenNextBorrowCreatesFresh()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=2;", out var created);
        var first = created.Single();
        first.ThrowOnExecute = true;
        first.TerminalOrDisposed = true; // a submit that fails on the wire leaves the client terminal

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await h.NewQuery().Sql("x").ExecuteReaderAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.SocketError));

        Assert.Multiple(() =>
        {
            Assert.That(first.Disposed, Is.True, "failed client is discarded");
            Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
        });

        var reader = await h.NewQuery().Sql("y").ExecuteReaderAsync();
        await reader.DisposeAsync();
        Assert.That(created, Has.Count.EqualTo(2), "next borrow creates a fresh client");
    }

    [Test]
    public void CtCancelDiscardsClientAndSurfacesOce()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=2;", out var created);
        var first = created.Single();
        first.CancelOnExecute = true;
        first.TerminalOrDisposed = true; // a hard CT cancel tears the connection down → terminal

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await h.NewQuery().Sql("x").ExecuteReaderAsync());

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

        var q = h.NewQuery().Sql("x");
        var reader = await q.ExecuteReaderAsync();      // in flight; rid set
        var read = reader.ReadBatchAsync().AsTask();    // parks on the gate
        q.Cancel();                                     // cooperative: posts a CANCEL frame
        fake.Gate.SetResult(true);                      // query then completes normally
        Assert.That(await read, Is.False);
        await reader.DisposeAsync();

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
        // Guards the lease null-out on the reader-dispose path.
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);
        var fake = created.Single();

        var q1 = h.NewQuery().Sql("a");
        var r1 = await q1.ExecuteReaderAsync();
        Assert.That(await r1.ReadBatchAsync(), Is.False);
        await r1.DisposeAsync(); // completes cleanly; client re-pooled, q1's lease nulled

        fake.Gate = new TaskCompletionSource<bool>();
        var q2 = h.NewQuery().Sql("b");
        var r2 = await q2.ExecuteReaderAsync(); // re-borrows the same inner
        var read2 = r2.ReadBatchAsync().AsTask(); // parks in flight on the gate

        Assert.That(fake.ExecuteCount, Is.EqualTo(2), "q2 re-borrowed the same client and is in flight");

        q1.Cancel(); // late cancel on the finished query — must be a no-op, not reach q2's client

        Assert.That(fake.CancelCount, Is.EqualTo(0),
            "a cancel on a completed query must not reach the re-borrowed client running q2");

        fake.Gate.SetResult(true);
        Assert.That(await read2, Is.False);
        await r2.DisposeAsync();

        Assert.That(fake.CancelCount, Is.EqualTo(0), "q2 completed without a stray cancel");
    }

    [Test]
    public async Task CancelRacingWithCompletionAndReborrowDoesNotCancelSuccessorQuery()
    {
        // The TOCTOU window: Cancel() resolves its target while q1 is still in flight, but the CANCEL
        // dispatch lands only after q1 completed, its client was re-pooled, and q2 re-borrowed it.
        // The cancel must be scoped to q1's request id so the late dispatch is dropped rather than
        // cancelling q2. CancelGate parks the dispatch inside the client to pin that interleaving.
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out var created);
        var fake = created.Single();
        fake.Gate = new TaskCompletionSource<bool>();
        fake.CancelGate = new TaskCompletionSource<bool>();

        var q1 = h.NewQuery().Sql("a");
        var r1 = await q1.ExecuteReaderAsync();   // fake rid 1
        var read1 = r1.ReadBatchAsync().AsTask(); // in flight, parked on the gate

        var cancelDispatch = Task.Run(q1.Cancel); // resolves rid 1, then parks on CancelGate
        var resolvedRid = await fake.CancelRequestEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(resolvedRid, Is.EqualTo(1), "cancel resolved q1's own request id");

        fake.Gate.SetResult(true); // q1's read completes
        Assert.That(await read1, Is.False);
        await r1.DisposeAsync();   // its client is returned to the pool

        fake.Gate = new TaskCompletionSource<bool>();
        var q2 = h.NewQuery().Sql("b");
        var r2 = await q2.ExecuteReaderAsync();   // re-borrows the same inner client; fake rid 2
        var read2 = r2.ReadBatchAsync().AsTask();

        Assert.That(fake.ExecuteCount, Is.EqualTo(2), "q2 re-borrowed the same client and is in flight");

        fake.CancelGate.SetResult(true); // stale cancel for rid 1 finally dispatches
        await cancelDispatch;

        Assert.That(fake.CancelCount, Is.EqualTo(0),
            "a cancel resolved against q1 must not cancel q2 on the re-borrowed client");

        fake.Gate.SetResult(true);
        Assert.That(await read2, Is.False);
        await r2.DisposeAsync();

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

        var readers = new List<IQwpQueryReader>();
        var reads = new List<Task>();
        for (var i = 0; i < 3; i++)
        {
            var reader = await h.NewQuery().Sql("q").ExecuteReaderAsync();
            readers.Add(reader);
            reads.Add(reader.ReadBatchAsync().AsTask()); // parks on the shared gate, staying in flight
        }

        Assert.That(h.TotalQueryClientCount, Is.EqualTo(3), "three concurrent queries borrowed three distinct clients");

        gate.SetResult(true);
        await Task.WhenAll(reads);
        foreach (var reader in readers)
        {
            await reader.DisposeAsync();
        }
    }

    [Test]
    public void DisposeClosesQueryPool()
    {
        var h = MakeHandle("query_pool_min=1;query_pool_max=1;", out _);
        h.Dispose();

        var ex = Assert.ThrowsAsync<IngressError>(async () =>
            await h.NewQuery().Sql("x").ExecuteReaderAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidApiCall));
    }

    [Test]
    public async Task QueryReusableAfterPoolExhausted()
    {
        using var h = MakeHandle("query_pool_min=1;query_pool_max=1;acquire_timeout_ms=100;", out var created);
        var fake = created.Single();
        fake.Gate = new TaskCompletionSource<bool>();

        var q1 = h.NewQuery().Sql("a");
        var r1 = await q1.ExecuteReaderAsync();   // holds the only client
        var read1 = r1.ReadBatchAsync().AsTask(); // parks (gated), keeping the client leased

        var q2 = h.NewQuery().Sql("b");
        var ex = Assert.ThrowsAsync<IngressError>(async () => await q2.ExecuteReaderAsync());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.PoolExhausted));

        fake.Gate.SetResult(true);
        Assert.That(await read1, Is.False);
        await r1.DisposeAsync();

        // q2 is not poisoned by the earlier exhaustion: a retry succeeds.
        var r2 = await q2.ExecuteReaderAsync();
        await r2.DisposeAsync();
        Assert.That(fake.LastSql, Is.EqualTo("b"));
    }
}
#endif
