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
using QuestDB.Pooling;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

public class PooledQueryClientTests
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
    public void CleanDisposeReturnsWithoutDisposingInner()
    {
        var pool = MakePool("query_pool_min=1;query_pool_max=1;", out var created);
        try
        {
            var c = pool.Borrow();
            c.Dispose();

            Assert.Multiple(() =>
            {
                Assert.That(created.Single().Disposed, Is.False, "clean return does not dispose the real client");
                Assert.That(pool.AvailableSize, Is.EqualTo(1));
            });
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void DoubleDisposeIsNoOp()
    {
        var pool = MakePool("query_pool_min=1;query_pool_max=1;", out _);
        try
        {
            var c = pool.Borrow();
            c.Dispose();
            c.Dispose(); // second dispose must not re-return or double-count

            Assert.That(pool.AvailableSize, Is.EqualTo(1), "second dispose is a no-op");
        }
        finally
        {
            pool.Close();
        }
    }

    [Test]
    public void DelegationForwardsToInner()
    {
        var pool = MakePool("query_pool_min=1;query_pool_max=1;", out var created);
        try
        {
            var c = pool.Borrow();
            c.Execute("select 1", new NoopQueryHandler());
            c.Cancel();

            var inner = created.Single();
            Assert.Multiple(() =>
            {
                Assert.That(inner.ExecuteCount, Is.EqualTo(1));
                Assert.That(inner.LastSql, Is.EqualTo("select 1"));
                Assert.That(inner.CancelCount, Is.EqualTo(1));
            });
            c.Dispose();
        }
        finally
        {
            pool.Close();
        }
    }
}
#endif
