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
using QuestDB;
using QuestDB.Pooling;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

public class PoolAffinityTests
{
    private static QuestDBClientImpl MakeClient(string keys)
    {
        var options = new SenderOptions("http::addr=localhost:9000;" + keys);
        return new QuestDBClientImpl(options, slot => new FakeSender(slot));
    }

    [Test]
    public void SenderPinsToCallingContext()
    {
        using var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        var a = client.Sender();
        var b = client.Sender();
        Assert.That(b, Is.SameAs(a), "same flow gets the same pinned sender");
        Assert.That(client.TotalSenderCount, Is.EqualTo(1), "only one sender borrowed");
        client.ReleaseSender();
    }

    [Test]
    public async Task PinFlowsAcrossAwait()
    {
        // The AsyncLocal pin follows the execution context across awaits, even when the continuation
        // resumes on a different thread — the property a ThreadLocal pin could not guarantee.
        using var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        var before = client.Sender();
        await Task.Yield();
        await Task.Delay(1);
        var after = client.Sender();

        Assert.That(after, Is.SameAs(before), "pin survives the await hop");
        Assert.That(client.TotalSenderCount, Is.EqualTo(1), "no second sender borrowed");
        client.ReleaseSender();
        Assert.That(client.AvailableSenderCount, Is.EqualTo(1));
    }

    [Test]
    public void PinnedSenderIsUsable()
    {
        using var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        var s = client.Sender();
        Assert.DoesNotThrow(() => s.Table("t").Column("x", 1).At(DateTime.UtcNow));
        client.ReleaseSender();
    }

    [Test]
    public void ReleaseReturnsPinnedSenderToPool()
    {
        using var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        _ = client.Sender();
        Assert.That(client.AvailableSenderCount, Is.EqualTo(0));

        client.ReleaseSender();
        Assert.That(client.AvailableSenderCount, Is.EqualTo(1), "pinned sender returned to the pool");
        Assert.That(client.TotalSenderCount, Is.EqualTo(1));
    }

    [Test]
    public void ReleaseWithoutPinIsNoOp()
    {
        using var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        Assert.DoesNotThrow(() => client.ReleaseSender());
        Assert.That(client.TotalSenderCount, Is.EqualTo(0));
    }

    [Test]
    public void PinsAreScopedPerExecutionContext()
    {
        using var client = MakeClient("sender_pool_min=0;sender_pool_max=4;");

        // Two independent threads start with independent execution contexts, so each pins its own
        // sender — the AsyncLocal value set in one flow does not bleed into the other.
        ISender? fromA = null;
        ISender? fromB = null;
        var ta = new Thread(() =>
        {
            fromA = client.Sender();
        });
        var tb = new Thread(() =>
        {
            fromB = client.Sender();
        });
        ta.Start();
        tb.Start();
        ta.Join();
        tb.Join();

        Assert.That(fromA, Is.Not.Null);
        Assert.That(fromB, Is.Not.Null);
        Assert.That(fromA, Is.Not.SameAs(fromB), "independent flows get distinct pinned senders");
        Assert.That(client.TotalSenderCount, Is.EqualTo(2));
    }

    [Test]
    public void DisposingPinnedSenderClearsPin()
    {
        using var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        var s1 = client.Sender();
        s1.Dispose(); // user disposes the pinned wrapper directly -> pin must be cleared

        // A subsequent release must not double-return; total stays consistent.
        Assert.DoesNotThrow(() => client.ReleaseSender());
        Assert.That(client.AvailableSenderCount, Is.EqualTo(1));
        Assert.That(client.TotalSenderCount, Is.EqualTo(1));

        // Re-pinning works and yields an in-use sender.
        var s2 = client.Sender();
        Assert.That(client.AvailableSenderCount, Is.EqualTo(0));
        client.ReleaseSender();
        _ = s2;
    }

    [Test]
    public void ReleaseAfterCloseIsSafe()
    {
        var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        _ = client.Sender();
        client.Close();
        Assert.DoesNotThrow(() => client.ReleaseSender());
    }

    [Test]
    public void SenderAfterCloseThrows()
    {
        var client = MakeClient("sender_pool_min=0;sender_pool_max=2;");
        client.Close();
        Assert.Throws<IngressError>(() => client.Sender());
    }

    [Test]
    public void PinnedSenderDisposedWhenHandleClosed()
    {
        var created = new ConcurrentBag<FakeSender>();
        var options = new SenderOptions("http::addr=localhost:9000;sender_pool_min=0;sender_pool_max=2;");
        var client = new QuestDBClientImpl(options, slot =>
        {
            var s = new FakeSender(slot);
            created.Add(s);
            return s;
        });

        _ = client.Sender();
        client.Close();

        Assert.That(created.Single().Disposed, Is.True, "pinned sender's delegate closed on handle close");
    }
}
