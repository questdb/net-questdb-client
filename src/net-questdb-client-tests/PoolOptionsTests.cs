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

using NUnit.Framework;
using QuestDB.Utils;

namespace net_questdb_client_tests;

/// <summary>
///     Phase 0: connection-pool config keys on <see cref="SenderOptions" />.
/// </summary>
public class PoolOptionsTests
{
    [Test]
    public void Defaults()
    {
        var o = new SenderOptions("http::addr=localhost:9000;");
        Assert.Multiple(() =>
        {
            Assert.That(o.sender_pool_min, Is.EqualTo(1));
            Assert.That(o.sender_pool_max, Is.EqualTo(4));
            Assert.That(o.acquire_timeout_ms, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(o.idle_timeout_ms, Is.EqualTo(TimeSpan.FromSeconds(60)));
            Assert.That(o.max_lifetime_ms, Is.EqualTo(TimeSpan.FromMinutes(30)));
            Assert.That(o.housekeeper_interval_ms, Is.EqualTo(TimeSpan.FromSeconds(5)));
        });
    }

    [Test]
    public void ParseExplicitValues()
    {
        var o = new SenderOptions(
            "http::addr=localhost:9000;sender_pool_min=2;sender_pool_max=8;acquire_timeout_ms=1000;" +
            "idle_timeout_ms=30000;max_lifetime_ms=120000;housekeeper_interval_ms=250;");
        Assert.Multiple(() =>
        {
            Assert.That(o.sender_pool_min, Is.EqualTo(2));
            Assert.That(o.sender_pool_max, Is.EqualTo(8));
            Assert.That(o.acquire_timeout_ms, Is.EqualTo(TimeSpan.FromSeconds(1)));
            Assert.That(o.idle_timeout_ms, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(o.max_lifetime_ms, Is.EqualTo(TimeSpan.FromSeconds(120)));
            Assert.That(o.housekeeper_interval_ms, Is.EqualTo(TimeSpan.FromMilliseconds(250)));
        });
    }

    [TestCase("http::addr=localhost:9000;")]
    [TestCase("tcp::addr=localhost:9009;")]
    [TestCase("ws::addr=localhost:9000;")]
    public void AcceptedOnEveryScheme(string confStr)
    {
        // Protocol-agnostic: a plain Sender must accept (and ignore) the pool keys without tripping
        // the unknown-key check or the WS-only / ILP-only guards.
        var o = new SenderOptions(confStr + "sender_pool_max=8;acquire_timeout_ms=1000;");
        Assert.That(o.sender_pool_max, Is.EqualTo(8));
    }

    [Test]
    public void ToStringExcludesPoolKeysAndRoundTrips()
    {
        var o = new SenderOptions("http::addr=localhost:9000;sender_pool_max=8;acquire_timeout_ms=1000;");
        var s = o.ToString();
        Assert.That(s, Does.Not.Contain("sender_pool_max"));
        Assert.That(s, Does.Not.Contain("acquire_timeout_ms"));
        // The serialized sender stays byte-identical to one that never set pool keys.
        Assert.That(s, Is.EqualTo(new SenderOptions("http::addr=localhost:9000;").ToString()));
    }

    [TestCase("sender_pool_min=5;sender_pool_max=2;", "must be ≤")]
    [TestCase("sender_pool_max=0;", "sender_pool_max")]
    [TestCase("sender_pool_min=-1;sender_pool_max=4;", "sender_pool_min")]
    [TestCase("housekeeper_interval_ms=50;", "housekeeper_interval_ms")]
    [TestCase("idle_timeout_ms=0;", "idle_timeout_ms")]
    [TestCase("max_lifetime_ms=0;", "max_lifetime_ms")]
    public void InvalidCombinationsThrow(string keys, string expectedFragment)
    {
        var ex = Assert.Throws<IngressError>(() =>
            _ = new SenderOptions("http::addr=localhost:9000;" + keys));
        Assert.That(ex!.Message, Does.Contain(expectedFragment));
    }

    [Test]
    public void AcquireTimeoutZeroIsAllowed()
    {
        // Zero is the deliberate non-blocking-try opt-out.
        var o = new SenderOptions("http::addr=localhost:9000;acquire_timeout_ms=0;");
        Assert.That(o.acquire_timeout_ms, Is.EqualTo(TimeSpan.Zero));
    }
}
