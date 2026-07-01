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
using QuestDB.Enums;
using QuestDB.Pooling;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

public class SenderPoolTests
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

    [Test]
    public void PreWarmsMinSenders()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=2;sender_pool_max=4;", out var created));
        var pool = g.Pool;
        Assert.Multiple(() =>
        {
            Assert.That(pool.TotalSize, Is.EqualTo(2));
            Assert.That(pool.AvailableSize, Is.EqualTo(2));
            Assert.That(created, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void BorrowReturnReusesUnderlyingSenderButHandsOutFreshHandle()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=1;sender_pool_max=1;", out var created));
        var pool = g.Pool;

        var s1 = pool.Borrow();
        s1.Dispose();
        var s2 = pool.Borrow();

        Assert.That(s2, Is.Not.SameAs(s1),
            "each borrow gets a fresh handle so a returned-then-reused reference can't alias the next borrower");
        Assert.That(created, Has.Count.EqualTo(1), "the underlying sender is reused: no new one created on re-borrow");
        s2.Dispose();
    }

    [Test]
    public void BorrowAndReturnTrackSizes()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=1;sender_pool_max=2;", out _));
        var pool = g.Pool;

        var s = pool.Borrow();
        Assert.Multiple(() =>
        {
            Assert.That(pool.AvailableSize, Is.EqualTo(0));
            Assert.That(pool.TotalSize, Is.EqualTo(1));
        });

        s.Dispose();
        Assert.Multiple(() =>
        {
            Assert.That(pool.AvailableSize, Is.EqualTo(1));
            Assert.That(pool.TotalSize, Is.EqualTo(1));
        });
    }

    [Test]
    public void ReturnDiscardsUnsentAndRepools()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=1;sender_pool_max=1;", out var created));
        var pool = g.Pool;

        var s = pool.Borrow();
        s.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(created.Single().SendCount, Is.EqualTo(0), "return does NOT send — Dispose is pure release");
            Assert.That(created.Single().ClearCount, Is.EqualTo(1), "return discards un-sent rows so the entry is clean");
            Assert.That(created.Single().Disposed, Is.False, "return does not dispose the real sender");
        });
    }

    [Test]
    public void BrokenSenderIsDiscardedNotReturned()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=0;sender_pool_max=2;", out var created));
        var pool = g.Pool;

        var s = pool.Borrow();
        created.Single().ThrowOnClear = true;
        s.Dispose(); // the buffer can't be cleared (terminal) -> discard rather than re-pool

        Assert.Multiple(() =>
        {
            Assert.That(pool.AvailableSize, Is.EqualTo(0), "broken sender not re-pooled");
            Assert.That(pool.TotalSize, Is.EqualTo(0), "broken sender removed from pool");
            Assert.That(created.Single().Disposed, Is.True, "broken sender disposed for real");
        });

        // capacity restored: a fresh sender can still be borrowed
        var s2 = pool.Borrow();
        Assert.That(created, Has.Count.EqualTo(2));
        s2.Dispose();
    }

    [Test]
    public void DisposeNeverThrows()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=1;sender_pool_max=1;", out var created));
        var pool = g.Pool;

        var s = pool.Borrow();
        created.Single().ThrowOnClear = true;
        // Even when the inner cannot be cleared AND its teardown throws, Dispose must swallow both.
        created.Single().ThrowOnDispose = true;

        Assert.DoesNotThrow(() => s.Dispose());
    }

    [Test]
    public void FlushDrainsEverySenderInThePool()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=3;sender_pool_max=3;", out var created));
        var pool = g.Pool;

        var drained = pool.Flush(TimeSpan.FromSeconds(1));

        Assert.Multiple(() =>
        {
            Assert.That(drained, Is.True, "all senders drained");
            Assert.That(created, Has.Count.EqualTo(3));
            Assert.That(created.All(s => s.FlushCount == 1), Is.True, "every pooled sender was drained once");
            Assert.That(created.All(s => s.SendCount == 0), Is.True, "Flush drains via Flush(), not Send()");
        });
    }

    [Test]
    public void FlushReturnsFalseWhenASenderDoesNotDrain()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=2;sender_pool_max=2;", out var created));
        var pool = g.Pool;

        created.First().ThrowOnFlush = true;

        Assert.That(pool.Flush(TimeSpan.FromSeconds(1)), Is.False,
            "one sender failing to drain makes the pool-wide Flush report failure");
        Assert.That(created.All(s => s.FlushCount == 1), Is.True, "a failing sender does not abort draining the rest");
    }

    [Test]
    public void GrowsOnDemandUpToMax()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=0;sender_pool_max=3;acquire_timeout_ms=100;", out var created));
        var pool = g.Pool;

        var a = pool.Borrow();
        var b = pool.Borrow();
        var c = pool.Borrow();
        Assert.That(pool.TotalSize, Is.EqualTo(3));
        Assert.That(created, Has.Count.EqualTo(3));

        Assert.Throws<IngressError>(() => pool.Borrow());

        a.Dispose();
        b.Dispose();
        c.Dispose();
    }

    [Test]
    public void ExhaustionThrowsAfterAcquireTimeout()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=0;sender_pool_max=1;acquire_timeout_ms=200;", out _));
        var pool = g.Pool;

        var held = pool.Borrow();
        var sw = Stopwatch.StartNew();
        var ex = Assert.Throws<IngressError>(() => pool.Borrow());
        sw.Stop();

        Assert.That(ex!.code, Is.EqualTo(ErrorCode.PoolExhausted));
        Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(150), "blocked for ~acquire_timeout");
        held.Dispose();
    }

    [Test]
    public void CloseDisposesEverySender()
    {
        var pool = MakePool("sender_pool_min=2;sender_pool_max=4;", out var created);
        pool.Close();

        Assert.Multiple(() =>
        {
            Assert.That(pool.AvailableSize, Is.EqualTo(0));
            Assert.That(pool.TotalSize, Is.EqualTo(0));
            Assert.That(created.All(s => s.Disposed), Is.True);
        });
    }

    [Test]
    public void CloseIsIdempotent()
    {
        var pool = MakePool("sender_pool_min=1;sender_pool_max=2;", out _);
        pool.Close();
        Assert.DoesNotThrow(() => pool.Close());
    }

    [Test]
    public void BorrowAfterCloseThrows()
    {
        var pool = MakePool("sender_pool_min=1;sender_pool_max=2;", out _);
        pool.Close();
        var ex = Assert.Throws<IngressError>(() => pool.Borrow());
        Assert.That(ex!.Message, Does.Contain("closed"));
    }

    [Test]
    public void CloseLeavesBorrowedSenderForBorrowerToTearDown()
    {
        // Close must dispose only idle senders; an in-use one is torn down by its borrower on return,
        // never by Close (which would race the borrower's flush on a non-thread-safe inner).
        var pool = MakePool("sender_pool_min=2;sender_pool_max=2;", out var created);

        var borrowed = pool.Borrow(); // 1 in-use, 1 idle

        pool.Close();

        var inUse = created.Single(s => !s.Disposed); // the borrowed sender survived close untouched
        Assert.Multiple(() =>
        {
            Assert.That(created.Count(s => s.Disposed), Is.EqualTo(1), "idle sender disposed by close");
            Assert.That(inUse.DisposeCount, Is.EqualTo(0), "not torn down yet — borrower hasn't returned");
        });

        borrowed.Dispose(); // borrower returns post-close: discard un-sent rows, then teardown, same thread

        Assert.Multiple(() =>
        {
            Assert.That(inUse.SendCount, Is.EqualTo(0), "return never sends");
            Assert.That(inUse.ClearCount, Is.EqualTo(1), "return discarded un-sent rows exactly once");
            Assert.That(inUse.DisposeCount, Is.EqualTo(1), "inner disposed exactly once, by the borrower");
            Assert.That(inUse.DisposedDuringClear, Is.False, "clear and dispose never overlapped");
        });

        Assert.DoesNotThrow(() => pool.Close(), "second close is idempotent");
    }

    [Test]
    public void CloseDoesNotDisposeASenderWhileItIsReturning()
    {
        // The M1 race, retargeted to the current return path: Close() running concurrently with a
        // borrower's Dispose (which now discards un-sent rows via Clear, not a send) on the same
        // non-thread-safe inner sender. The gate parks the borrowed sender mid-return so Close is forced
        // to run during that exact window.
        using var gate = new ManualResetEventSlim(false);
        var pool = MakePool("sender_pool_min=2;sender_pool_max=2;", out var created);
        foreach (var s in created)
        {
            s.ClearGate = gate; // only the borrowed sender actually calls Clear and parks here
        }

        var borrowed = pool.Borrow(); // 1 in-use, 1 idle

        var ret = Task.Run(() => borrowed.Dispose()); // enters the return path, parks inside Clear
        Assert.That(SpinWait.SpinUntil(() => created.Any(s => s.ClearInProgress), TimeSpan.FromSeconds(5)),
            Is.True, "borrowed sender reached its return");
        var returning = created.Single(s => s.ClearInProgress);

        pool.Close(); // concurrent with the parked return

        Assert.Multiple(() =>
        {
            Assert.That(returning.Disposed, Is.False, "close must not dispose a sender mid-return");
            Assert.That(returning.DisposedDuringClear, Is.False, "no Dispose raced the in-flight return");
            Assert.That(created.Count(s => s.Disposed), Is.EqualTo(1), "only the idle sender was disposed");
        });

        gate.Set(); // let the return finish; borrower now tears its sender down (post-close return path)
        Assert.That(ret.Wait(TimeSpan.FromSeconds(5)), Is.True, "return completed");

        Assert.Multiple(() =>
        {
            Assert.That(returning.SendCount, Is.EqualTo(0), "return never sends");
            Assert.That(returning.ClearCount, Is.EqualTo(1));
            Assert.That(returning.DisposeCount, Is.EqualTo(1), "inner disposed exactly once, by its borrower");
            Assert.That(returning.DisposedDuringClear, Is.False);
        });
    }

    [Test]
    public async Task AsyncBorrowAndReturn()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=0;sender_pool_max=1;", out var created));
        var pool = g.Pool;

        var s = await pool.BorrowAsync();
        Assert.That(pool.AvailableSize, Is.EqualTo(0));
        await s.DisposeAsync();
        Assert.That(pool.AvailableSize, Is.EqualTo(1));
        Assert.That(created.Single().SendCount, Is.EqualTo(0), "async return never sends");
        Assert.That(created.Single().ClearCount, Is.EqualTo(1), "async return discarded un-sent rows");
    }

    [Test]
    public async Task AsyncBrokenSenderDiscarded()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=0;sender_pool_max=1;", out var created));
        var pool = g.Pool;

        var s = await pool.BorrowAsync();
        created.Single().ThrowOnClear = true;
        await s.DisposeAsync();

        Assert.That(pool.TotalSize, Is.EqualTo(0));
        Assert.That(created.Single().Disposed, Is.True);
    }

    [Test]
    public void ConcurrentBorrowsNeverExceedMax()
    {
        using var g = new PoolGuard(MakePool("sender_pool_min=0;sender_pool_max=4;acquire_timeout_ms=5000;", out var created));
        var pool = g.Pool;

        Parallel.For(0, 200, _ =>
        {
            var s = pool.Borrow();
            Thread.SpinWait(200);
            Assert.That(pool.TotalSize, Is.LessThanOrEqualTo(4));
            s.Dispose();
        });

        Assert.That(created, Has.Count.LessThanOrEqualTo(4), "never created more than max underlying senders");
        Assert.That(pool.TotalSize, Is.LessThanOrEqualTo(4));
    }

    [Test]
    public async Task HandleBorrowReturnsWorkingSender()
    {
        // End-to-end through the public QuestDBClient handle with an injected factory.
        var bag = new ConcurrentBag<FakeSender>();
        var options = new SenderOptions("http::addr=localhost:9000;sender_pool_min=1;sender_pool_max=2;");
        await using IQuestDBClient client = new QuestDBClientImpl(options, slot =>
        {
            var s = new FakeSender(slot);
            bag.Add(s);
            return s;
        });

        using (var sender = client.BorrowSender())
        {
            sender.Table("t").Column("x", 1).At(DateTime.UtcNow);
            sender.Send(); // explicit send — Dispose no longer flushes
        }

        Assert.That(client.AvailableSenderCount, Is.EqualTo(1));
        Assert.That(client.TotalSenderCount, Is.EqualTo(1));
        Assert.That(bag.Single().SendCount, Is.EqualTo(1), "rows were sent explicitly, not on dispose");
    }

    [Test]
    public void ConnectBuildsRealHttpSenders()
    {
        // Exercises the production factory path (CreateDefaultInner -> Sender.New -> HttpSender) and
        // real PooledSender delegation. No rows are written, so the return-clear is a no-op and no
        // server is contacted.
        using var client = QuestDBClient.Connect(
            "http::addr=localhost:9000;sender_pool_min=1;sender_pool_max=2;");

        Assert.That(client.TotalSenderCount, Is.EqualTo(1));

        using (var s = client.BorrowSender())
        {
            Assert.That(s.Options.protocol, Is.EqualTo(ProtocolType.http));
        }

        Assert.That(client.AvailableSenderCount, Is.EqualTo(1));
    }

    // Disposes the pool even when an assertion fails mid-test.
    private sealed class PoolGuard : IDisposable
    {
        public PoolGuard(SenderPool pool) => Pool = pool;
        public SenderPool Pool { get; }
        public void Dispose() => Pool.Close();
    }
}
