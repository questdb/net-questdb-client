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
using QuestDB;
using QuestDB.Enums;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

/// <summary>
///     <c>lazy_connect=true</c>: tolerant startup for the <see cref="QuestDBClient" /> handle. The
///     ingest side connects asynchronously and the read pool defaults to <c>query_pool_min=0</c>, so the
///     handle builds even while the server is down. Blocking-startup knobs are rejected up front.
/// </summary>
public class LazyConnectTests
{
    // ---- connect-string parsing ----

    [Test]
    public void FlagParsesAndDefaultsOff()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new SenderOptions("ws::addr=localhost:9000;").lazy_connect, Is.False);
            Assert.That(new SenderOptions("ws::addr=localhost:9000;lazy_connect=true;").lazy_connect, Is.True);
            Assert.That(new SenderOptions("ws::addr=localhost:9000;lazy_connect=on;").lazy_connect, Is.True);
            Assert.That(new SenderOptions("ws::addr=localhost:9000;lazy_connect=off;").lazy_connect, Is.False);
        });
    }

    [TestCase("http::addr=localhost:9000;")]
    [TestCase("tcp::addr=localhost:9009;")]
    [TestCase("ws::addr=localhost:9000;")]
    public void AcceptedOnEveryScheme(string confStr)
    {
        // Pool key: a plain Sender parses and ignores it on every scheme.
        Assert.DoesNotThrow(() => _ = new SenderOptions(confStr + "lazy_connect=true;"));
    }

    [Test]
    public void PlainSenderIgnoresFlag()
    {
        Assert.DoesNotThrow(() =>
        {
            using var s = Sender.New("http::addr=localhost:9000;lazy_connect=true;");
        });
    }

    [Test]
    public void ExcludedFromToStringAndRoundTrips()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;lazy_connect=true;");
        var s = o.ToString();
        Assert.That(s, Does.Not.Contain("lazy_connect"));
        Assert.DoesNotThrow(() => _ = new SenderOptions(s));
    }

    // ---- resolution logic (QuestDBClientBuilder.ResolveLazyConnect), net-agnostic ----

    [Test]
    public void Resolve_Ws_ForcesAsyncAndDefaultsQueryPoolMinZero()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;");
        var forceAsync = QuestDBClientBuilder.ResolveLazyConnect(
            lazy: true, o, ingestIsWebSocket: true, queryPoolMinExplicit: false, queryPoolMinExplicitPositive: false);
        Assert.Multiple(() =>
        {
            Assert.That(forceAsync, Is.True);
            Assert.That(o.query_pool_min, Is.EqualTo(0));
        });
    }

    [Test]
    public void Resolve_Http_DoesNotForceAsyncButStillDefaultsQueryPoolMinZero()
    {
        var o = new SenderOptions("http::addr=localhost:9000;");
        var forceAsync = QuestDBClientBuilder.ResolveLazyConnect(
            lazy: true, o, ingestIsWebSocket: false, queryPoolMinExplicit: false, queryPoolMinExplicitPositive: false);
        Assert.Multiple(() =>
        {
            Assert.That(forceAsync, Is.False);
            Assert.That(o.query_pool_min, Is.EqualTo(0));
        });
    }

    [Test]
    public void Resolve_LazyFalse_IsNoOp()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;"); // query_pool_min default 1
        var forceAsync = QuestDBClientBuilder.ResolveLazyConnect(
            lazy: false, o, ingestIsWebSocket: true, queryPoolMinExplicit: false, queryPoolMinExplicitPositive: false);
        Assert.Multiple(() =>
        {
            Assert.That(forceAsync, Is.False);
            Assert.That(o.query_pool_min, Is.EqualTo(1));
        });
    }

    [Test]
    public void Resolve_ExplicitQueryPoolMinZero_Allowed()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;query_pool_min=0;");
        Assert.DoesNotThrow(() => QuestDBClientBuilder.ResolveLazyConnect(
            lazy: true, o, ingestIsWebSocket: true, queryPoolMinExplicit: true, queryPoolMinExplicitPositive: false));
        Assert.That(o.query_pool_min, Is.EqualTo(0));
    }

    [Test]
    public void Resolve_ExplicitInitialConnectAsync_Allowed()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;initial_connect_retry=async;");
        Assert.DoesNotThrow(() => QuestDBClientBuilder.ResolveLazyConnect(
            lazy: true, o, ingestIsWebSocket: true, queryPoolMinExplicit: false, queryPoolMinExplicitPositive: false));
        Assert.That(o.query_pool_min, Is.EqualTo(0));
    }

    [TestCase("off")]
    [TestCase("on")]
    [TestCase("sync")]
    public void Resolve_ExplicitBlockingInitialConnect_Throws(string mode)
    {
        var o = new SenderOptions($"ws::addr=localhost:9000;initial_connect_retry={mode};");
        var ex = Assert.Throws<IngressError>(() => QuestDBClientBuilder.ResolveLazyConnect(
            lazy: true, o, ingestIsWebSocket: true, queryPoolMinExplicit: false, queryPoolMinExplicitPositive: false));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(ex.Message, Does.Contain("initial_connect_retry"));
        });
    }

    [Test]
    public void Resolve_ExplicitQueryPoolMinPositive_Throws()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;query_pool_min=2;");
        var ex = Assert.Throws<IngressError>(() => QuestDBClientBuilder.ResolveLazyConnect(
            lazy: true, o, ingestIsWebSocket: true, queryPoolMinExplicit: true, queryPoolMinExplicitPositive: true));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(ex.Message, Does.Contain("query_pool_min"));
        });
    }

    // ---- facade build behaviour: conflicts are rejected before any pool is built (no server needed) ----

    [Test]
    public void Conflict_ExplicitQueryPoolMinInConfig_Throws()
    {
        var ex = Assert.Throws<IngressError>(() => QuestDBClient.Connect(
            "ws::addr=localhost:9000;lazy_connect=true;query_pool_min=2;sender_pool_min=0;"));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(ex.Message, Does.Contain("query_pool_min"));
        });
    }

    [Test]
    public void Conflict_ExplicitBlockingInitialConnectInConfig_Throws()
    {
        var ex = Assert.Throws<IngressError>(() => QuestDBClient.Connect(
            "ws::addr=localhost:9000;lazy_connect=true;initial_connect_retry=on;sender_pool_min=0;"));
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(ex.Message, Does.Contain("initial_connect_retry"));
        });
    }

#if NET7_0_OR_GREATER
    [Test]
    public void Conflict_QueryPoolMinViaBuilder_Throws()
    {
        var ex = Assert.Throws<IngressError>(() => QuestDBClient.Builder()
            .FromConfig("ws::addr=localhost:9000;lazy_connect=true;sender_pool_min=0;")
            .QueryPoolMin(2)
            .Build());
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(ex.Message, Does.Contain("query_pool_min"));
        });
    }

    [Test]
    public void Lazy_StartsWithReadPoolEnabledButNotPrewarmed()
    {
        // No server: without lazy_connect the query pool would prewarm one client and fail. lazy_connect
        // defaults query_pool_min to 0 so the read pool stays enabled but connects only on first borrow.
        using var h = QuestDBClient.Connect("ws::addr=localhost:9000;lazy_connect=true;sender_pool_min=0;");
        Assert.Multiple(() =>
        {
            Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
            Assert.DoesNotThrow(() => _ = h.NewQuery());
        });
    }

    [Test]
    public void Lazy_ViaBuilderMethod_StartsWithoutServer()
    {
        using var h = QuestDBClient.Builder()
            .FromConfig("ws::addr=localhost:9000;sender_pool_min=0;")
            .LazyConnect()
            .Build();
        Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
    }

    [Test]
    public void Lazy_WithExplicitAsyncInitialConnect_StartsWithoutServer()
    {
        using var h = QuestDBClient.Connect(
            "ws::addr=localhost:9000;lazy_connect=true;initial_connect_retry=async;sender_pool_min=0;");
        Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
    }

    [Test]
    public void Lazy_IngestBorrowBuffersWithoutServer()
    {
        // The pooled ws sender is created async, so borrowing and appending never blocks on the down
        // server (a non-async sender would block on first-connect and throw here).
        using var h = QuestDBClient.Connect("ws::addr=localhost:9000;lazy_connect=true;sender_pool_min=0;");
        Assert.DoesNotThrow(() =>
        {
            using var s = h.BorrowSender();
            s.Table("lazy_connect_test").Column("v", 1L).AtNow();
        });
    }

    [Test]
    public void Lazy_DefaultSenderPoolMin_PreWarmsAsyncWithoutServer()
    {
        // Headline behaviour (a): with the DEFAULT sender_pool_min=1, the sender pool pre-warms one ws
        // sender at construction. lazy_connect forces it async, so Build() returns promptly against a down
        // server; a non-async pre-warm would block on first-connect and throw. This exercises the
        // PreWarm -> CreateDefaultInner forced-async path the sender_pool_min=0 tests skip.
        using var h = QuestDBClient.Connect("ws::addr=localhost:9000;lazy_connect=true;");
        Assert.Multiple(() =>
        {
            Assert.That(h.TotalSenderCount, Is.EqualTo(1));
            Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void Lazy_BuilderFalseOverridesConfigTrue()
    {
        // `LazyConnect(false)` wins over a connect-string `lazy_connect=true` (the `??` precedence). With
        // lazy disabled the conflict guard is not evaluated, so the otherwise-conflicting
        // `initial_connect_retry=on` is accepted and Build() succeeds (both pools sized 0 — no server). If
        // the precedence regressed to OR, lazy would stay true and this would throw a ConfigError.
        Assert.DoesNotThrow(() => QuestDBClient.Builder()
            .FromConfig("ws::addr=localhost:9000;lazy_connect=true;initial_connect_retry=on;" +
                        "query_pool_min=0;sender_pool_min=0;")
            .LazyConnect(false)
            .Build()
            .Dispose());
    }

    [Test]
    public void Conflict_IngestQueryPoolMinWithSeparateQueryConfig_Throws()
    {
        // The ingest string's explicit query_pool_min>0 must still be rejected under lazy_connect even when
        // a separate query config (which wins for sizing) omits the key.
        var ex = Assert.Throws<IngressError>(() => QuestDBClient.Builder()
            .FromConfig("ws::addr=localhost:9000;query_pool_min=5;sender_pool_min=0;")
            .QueryConfig("ws::addr=localhost:9000;")
            .LazyConnect()
            .Build());
        Assert.Multiple(() =>
        {
            Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(ex.Message, Does.Contain("query_pool_min"));
        });
    }
#endif
}
