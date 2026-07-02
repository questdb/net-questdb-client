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
using QuestDB.Enums;
using QuestDB.Pooling;
using QuestDB.Senders;
using QuestDB.Utils;
#if NET7_0_OR_GREATER
using QuestDB.Qwp.Query;
#endif

namespace net_questdb_client_tests.Pooling;

public class PoolErrorSafetyTests
{
    private static SenderPool MakePool(string keys)
    {
        var options = new SenderOptions("http::addr=localhost:9000;" + keys);
        return new SenderPool(options, null, slot => new FakeSender(slot));
    }

    [Test]
    public void CloseWakesBlockedBorrowerPromptly()
    {
        var pool = MakePool("sender_pool_min=0;sender_pool_max=1;acquire_timeout_ms=10000;");
        var held = pool.Borrow();

        Exception? caught = null;
        var sw = new Stopwatch();
        var t = new Thread(() =>
        {
            try
            {
                pool.Borrow();
            }
            catch (Exception e)
            {
                caught = e;
            }
        });

        t.Start();
        Thread.Sleep(200); // let the borrower block on the exhausted pool
        sw.Start();
        pool.Close();
        t.Join(TimeSpan.FromSeconds(5));
        sw.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(caught, Is.InstanceOf<IngressError>());
            Assert.That(caught!.Message, Does.Contain("closed"));
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(4000), "woke on close, not on the 10s acquire timeout");
        });

        Assert.DoesNotThrow(() => held.Dispose());
    }

    [Test]
    public void DisposingReturnedSenderTwiceDoesNotDoubleReturn()
    {
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;");
        try
        {
            var s = pool.Borrow();
            s.Dispose();
            Assert.That(pool.AvailableSize, Is.EqualTo(1));
            s.Dispose(); // second dispose is a no-op
            Assert.That(pool.AvailableSize, Is.EqualTo(1), "idempotent: not pooled twice");
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void UsingABorrowedSenderAfterDisposeThrowsObjectDisposed()
    {
        // The use-after-return guard: once a borrowed handle is disposed (returned to the pool), every
        // ingest member is inert. A caller hanging on to the stale reference therefore cannot reach — and
        // corrupt — the entry the pool may have since lent to a different borrower.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;");
        try
        {
            var s = pool.Borrow();
            s.Table("t").Column("x", 1L).At(DateTime.UtcNow); // fine while borrowed
            s.Dispose();

            Assert.Multiple(() =>
            {
                Assert.Throws<ObjectDisposedException>(() => s.Table("t"));
                Assert.Throws<ObjectDisposedException>(() => s.Column("x", 1L));
                Assert.Throws<ObjectDisposedException>(() => s.At(DateTime.UtcNow));
                Assert.Throws<ObjectDisposedException>(() => s.Send());
                Assert.Throws<ObjectDisposedException>(() => _ = s.RowCount);
                Assert.Throws<ObjectDisposedException>(() => _ = s.Options);
            });

            // A second dispose is a tolerated no-op (no double-return, no throw).
            Assert.DoesNotThrow(() => s.Dispose());
            Assert.That(pool.AvailableSize, Is.EqualTo(1), "idempotent: returned exactly once");
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void DisposingBorrowedSenderAfterHandleCloseIsSafe()
    {
        var pool = MakePool("sender_pool_min=0;sender_pool_max=2;");
        var s = pool.Borrow();
        pool.Close();
        Assert.DoesNotThrow(() => s.Dispose());
        Assert.That(pool.AvailableSize, Is.EqualTo(0), "not re-pooled into a closed pool");
    }

    [Test]
    public void ConcurrentBorrowReturnAndCloseDoNotThrow()
    {
        var pool = MakePool("sender_pool_min=2;sender_pool_max=6;acquire_timeout_ms=2000;");
        var errors = new ConcurrentQueue<Exception>();
        var stop = false;

        var workers = Enumerable.Range(0, 6).Select(_ => new Thread(() =>
        {
            while (!Volatile.Read(ref stop))
            {
                try
                {
                    var s = pool.Borrow();
                    Thread.SpinWait(50);
                    s.Dispose();
                }
                catch (IngressError)
                {
                    // expected once the pool closes
                }
                catch (Exception e)
                {
                    errors.Enqueue(e);
                }
            }
        })).ToArray();

        foreach (var w in workers)
        {
            w.Start();
        }

        Thread.Sleep(300);
        pool.Close();
        Volatile.Write(ref stop, true);
        foreach (var w in workers)
        {
            w.Join(TimeSpan.FromSeconds(5));
        }

        Assert.That(errors, Is.Empty, "no unexpected exceptions under concurrent borrow/return/close");
    }

    [Test]
    public void PooledSenderFromNonWsPoolDoesNotMatchQwpCapabilityProbe()
    {
        // The documented probe — `if (sender is IQwpWebSocketSender ws)` — must answer per transport,
        // same as a standalone Sender.New: a borrowed handle from a non-WS pool must NOT match, so
        // callers never get a "capable" handle whose QWP members would only throw at runtime.
        var pool = MakePool("sender_pool_min=0;sender_pool_max=1;");
        try
        {
            ISender s = pool.Borrow();
            Assert.That(s, Is.Not.InstanceOf<IQwpWebSocketSender>(),
                "a non-WS pool must hand out a handle without the QWP surface");
            s.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void PooledSenderFromWsPoolExposesQwpSurfaceAndForwards()
    {
        var options = new SenderOptions("ws::addr=localhost:9000;sender_pool_min=0;sender_pool_max=1;");
        var pool = new SenderPool(options, null, slot => new FakeQwpSender(slot));
        try
        {
            ISender s = pool.Borrow();
            Assert.That(s, Is.InstanceOf<IQwpWebSocketSender>(),
                "a WS pool's handle matches the QWP capability probe");

            var qwp = (IQwpWebSocketSender)s;
            qwp.Ping();
            Assert.That(qwp.ColumnByte("b", 1), Is.SameAs(s), "fluent QWP calls chain on the handle");
            Assert.That(qwp.AckedFsn, Is.EqualTo(-1));

            s.Dispose();
            // Same use-after-return gate as the ISender members.
            Assert.Throws<ObjectDisposedException>(() => qwp.Ping());
            Assert.Throws<ObjectDisposedException>(() => _ = qwp.AckedFsn);
            Assert.Throws<ObjectDisposedException>(() => qwp.ColumnByte("b", 1));
        }
        finally
        {
            pool.Close();
        }
    }

#if NET7_0_OR_GREATER
    [Test]
    public void CtorDisposesSenderPoolWhenQueryPoolPrewarmThrows()
    {
        // Distinct-endpoint shape: ingest is healthy (sender pool warms fine) but the query endpoint is
        // down (query-pool prewarm throws). The half-built handle is never returned, so unless the ctor
        // tears down on failure the warm ingest senders (and SF flocks) leak — one per Connect retry.
        var options = new SenderOptions(
            "ws::addr=localhost:9000;sender_pool_min=2;sender_pool_max=4;query_pool_min=1;query_pool_max=4;");
        var senders = new ConcurrentBag<FakeSender>();

        var ex = Assert.Throws<IngressError>(() => _ = new QuestDBClientImpl(
            options,
            slot =>
            {
                var s = new FakeSender(slot);
                senders.Add(s);
                return s;
            },
            () => throw new IngressError(ErrorCode.SocketError, "query endpoint down")));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("query endpoint down"));
            Assert.That(senders, Is.Not.Empty, "sender pool pre-warmed before the query pool threw");
            Assert.That(senders, Has.All.Matches<FakeSender>(s => s.Disposed),
                "warmed senders disposed when query-pool construction throws (no leak)");
        });
    }
#endif

    [Test]
    public void FluentMethodsReturnTheWrapperNotTheDelegate()
    {
        var pool = MakePool("sender_pool_min=0;sender_pool_max=1;");
        try
        {
            ISender s = pool.Borrow();
            Assert.Multiple(() =>
            {
                Assert.That(s.Table("t"), Is.SameAs(s));
                Assert.That(s.Symbol("k", "v"), Is.SameAs(s));
                Assert.That(s.Column("c", 1L), Is.SameAs(s));
                Assert.That(s.Column("d", 1.5), Is.SameAs(s));
            });
            s.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }
}
