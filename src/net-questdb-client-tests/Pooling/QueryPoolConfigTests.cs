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
using QuestDB.Qwp.Query;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

public class QueryPoolConfigTests
{
    [Test]
    public void DefaultsAreOneAndFour()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;");
        Assert.Multiple(() =>
        {
            Assert.That(o.query_pool_min, Is.EqualTo(1));
            Assert.That(o.query_pool_max, Is.EqualTo(4));
        });
    }

    [Test]
    public void ParsesQueryPoolKeys()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;query_pool_min=2;query_pool_max=8;");
        Assert.Multiple(() =>
        {
            Assert.That(o.query_pool_min, Is.EqualTo(2));
            Assert.That(o.query_pool_max, Is.EqualTo(8));
        });
    }

    [Test]
    public void ValidateQueryPoolOptionsRejectsBadValues()
    {
        var min = new SenderOptions("ws::addr=localhost:9000;") { query_pool_min = -1 };
        var max = new SenderOptions("ws::addr=localhost:9000;") { query_pool_max = 0 };
        var minGtMax = new SenderOptions("ws::addr=localhost:9000;") { query_pool_min = 5, query_pool_max = 2 };

        Assert.Multiple(() =>
        {
            Assert.That(Assert.Throws<IngressError>(() => min.ValidateQueryPoolOptions())!.code,
                Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(Assert.Throws<IngressError>(() => max.ValidateQueryPoolOptions())!.code,
                Is.EqualTo(ErrorCode.ConfigError));
            Assert.That(Assert.Throws<IngressError>(() => minGtMax.ValidateQueryPoolOptions())!.code,
                Is.EqualTo(ErrorCode.ConfigError));
        });
    }

    [Test]
    public void QueryPoolKeysAreJsonIgnoredAndRoundTrip()
    {
        var o = new SenderOptions("ws::addr=localhost:9000;query_pool_max=8;");
        var s = o.ToString();

        Assert.That(s, Does.Not.Contain("query_pool_max"), "pool knobs are excluded from ToString");
        Assert.DoesNotThrow(() => _ = new SenderOptions(s), "the serialized string round-trips");
    }

    [Test]
    public void QueryOptionsAcceptsQueryPoolKeys()
    {
        Assert.DoesNotThrow(() => _ = new QueryOptions("ws::addr=localhost:9000;query_pool_max=8;"));
    }

#if NET7_0_OR_GREATER
    [Test]
    public void WsSingleStringHandleHasQueryPool()
    {
        using var h = QuestDBClient.Connect("ws::addr=localhost:9000;sender_pool_min=0;query_pool_min=0;");
        Assert.Multiple(() =>
        {
            Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
            Assert.DoesNotThrow(() => _ = h.NewQuery());
        });
    }

    [Test]
    public void HttpHandleWithoutQueryConfigRejectsQueries()
    {
        using var h = QuestDBClient.Connect("http::addr=localhost:9000;sender_pool_min=0;");
        var ex = Assert.Throws<IngressError>(() => h.NewQuery());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
    }

    [Test]
    public void DistinctIngestAndQueryConfigBuildsQueryPool()
    {
        using var h = QuestDBClient.Connect(
            "http::addr=localhost:9000;sender_pool_min=0;",
            "ws::addr=localhost:9000;query_pool_min=0;");
        Assert.Multiple(() =>
        {
            Assert.That(h.TotalQueryClientCount, Is.EqualTo(0));
            Assert.DoesNotThrow(() => _ = h.NewQuery());
        });
    }

    [Test]
    public void NonWsQueryConfigIsRejected()
    {
        var ex = Assert.Throws<IngressError>(() =>
            QuestDBClient.Builder().IngestConfig("http::addr=localhost:9000;").QueryConfig("http::addr=localhost:9000;"));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
    }

    // ---- sizing precedence with a separate query config (builder call > query config > ingest config
    // > default), asserted on the assembled config so no pool is pre-warmed ----

    [Test]
    public void SeparateQueryConfigWithoutSizingKeepsIngestSizing()
    {
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;query_pool_min=3;query_pool_max=8;")
            .QueryConfig("wss::addr=query:9000;")
            .BuildPoolConfig(out var queryConfStr, out _);
        Assert.Multiple(() =>
        {
            Assert.That(queryConfStr, Is.EqualTo("wss::addr=query:9000;"));
            Assert.That(cfg.query_pool_min, Is.EqualTo(3));
            Assert.That(cfg.query_pool_max, Is.EqualTo(8));
        });
    }

    [Test]
    public void SeparateQueryConfigExplicitSizingWinsOverIngest()
    {
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;query_pool_min=3;query_pool_max=8;")
            .QueryConfig("wss::addr=query:9000;query_pool_min=2;query_pool_max=6;")
            .BuildPoolConfig(out _, out _);
        Assert.Multiple(() =>
        {
            Assert.That(cfg.query_pool_min, Is.EqualTo(2));
            Assert.That(cfg.query_pool_max, Is.EqualTo(6));
        });
    }

    [Test]
    public void SeparateQueryConfigOverridesPerKeyNotWholesale()
    {
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;query_pool_min=3;query_pool_max=8;")
            .QueryConfig("wss::addr=query:9000;query_pool_max=6;")
            .BuildPoolConfig(out _, out _);
        Assert.Multiple(() =>
        {
            Assert.That(cfg.query_pool_min, Is.EqualTo(3), "min not carried by the query config survives from ingest");
            Assert.That(cfg.query_pool_max, Is.EqualTo(6), "max carried by the query config wins");
        });
    }

    [Test]
    public void SeparateQueryConfigExplicitDefaultEqualValuesStillWin()
    {
        // Explicitness is key presence, not value: 0 and the default 4 carried by the query config
        // must beat the ingest values, not be mistaken for "unset".
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;query_pool_min=3;query_pool_max=8;")
            .QueryConfig("wss::addr=query:9000;query_pool_min=0;query_pool_max=4;")
            .BuildPoolConfig(out _, out _);
        Assert.Multiple(() =>
        {
            Assert.That(cfg.query_pool_min, Is.EqualTo(0));
            Assert.That(cfg.query_pool_max, Is.EqualTo(4));
        });
    }

    [Test]
    public void BuilderSizingWinsOverBothConfigs()
    {
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;query_pool_min=3;query_pool_max=8;")
            .QueryConfig("wss::addr=query:9000;query_pool_min=5;query_pool_max=6;")
            .QueryPoolMin(2)
            .QueryPoolMax(9)
            .BuildPoolConfig(out _, out _);
        Assert.Multiple(() =>
        {
            Assert.That(cfg.query_pool_min, Is.EqualTo(2));
            Assert.That(cfg.query_pool_max, Is.EqualTo(9));
        });
    }

    [Test]
    public void MergedCrossConfigSizingIsValidated()
    {
        // min from the ingest string, max from the query config — the merged pair must still validate.
        var ex = Assert.Throws<IngressError>(() => QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;query_pool_min=6;")
            .QueryConfig("wss::addr=query:9000;query_pool_max=2;")
            .BuildPoolConfig(out _, out _));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
    }

    // ---- shared timing-knob precedence with a separate query config. acquire/idle/lifetime/housekeeper
    // drive BOTH pools through the single poolConfig, so a value carried only by the query config must
    // fold in (builder call > query config > ingest config > default), not be silently dropped ----

    [Test]
    public void SeparateQueryConfigTimingKnobsAreHonored()
    {
        // The finding's exact scenario: the timing knobs live only in the query config and must reach
        // the shared poolConfig instead of falling back to the ingest defaults (5000 / 60000 / ...).
        var cfg = QuestDBClient.Builder()
            .IngestConfig("http::addr=ingest:9000;")
            .QueryConfig("wss::addr=query:9000;acquire_timeout_ms=200;idle_timeout_ms=1000;" +
                         "max_lifetime_ms=1234;housekeeper_interval_ms=300;")
            .BuildPoolConfig(out _, out _);
        Assert.Multiple(() =>
        {
            Assert.That(cfg.acquire_timeout_ms, Is.EqualTo(TimeSpan.FromMilliseconds(200)));
            Assert.That(cfg.idle_timeout_ms, Is.EqualTo(TimeSpan.FromMilliseconds(1000)));
            Assert.That(cfg.max_lifetime_ms, Is.EqualTo(TimeSpan.FromMilliseconds(1234)));
            Assert.That(cfg.housekeeper_interval_ms, Is.EqualTo(TimeSpan.FromMilliseconds(300)));
        });
    }

    [Test]
    public void SeparateQueryConfigTimingWinsOverIngestPerKey()
    {
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;acquire_timeout_ms=5000;idle_timeout_ms=60000;")
            .QueryConfig("wss::addr=query:9000;acquire_timeout_ms=200;")
            .BuildPoolConfig(out _, out _);
        Assert.Multiple(() =>
        {
            Assert.That(cfg.acquire_timeout_ms, Is.EqualTo(TimeSpan.FromMilliseconds(200)),
                "carried by the query config wins");
            Assert.That(cfg.idle_timeout_ms, Is.EqualTo(TimeSpan.FromMilliseconds(60000)),
                "not carried by the query config survives from ingest");
        });
    }

    [Test]
    public void SeparateQueryConfigWithoutTimingKeepsIngestTiming()
    {
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;acquire_timeout_ms=250;")
            .QueryConfig("wss::addr=query:9000;")
            .BuildPoolConfig(out _, out _);
        Assert.That(cfg.acquire_timeout_ms, Is.EqualTo(TimeSpan.FromMilliseconds(250)));
    }

    [Test]
    public void BuilderTimingWinsOverQueryConfig()
    {
        var cfg = QuestDBClient.Builder()
            .IngestConfig("ws::addr=ingest:9000;")
            .QueryConfig("wss::addr=query:9000;acquire_timeout_ms=200;")
            .AcquireTimeout(TimeSpan.FromMilliseconds(777))
            .BuildPoolConfig(out _, out _);
        Assert.That(cfg.acquire_timeout_ms, Is.EqualTo(TimeSpan.FromMilliseconds(777)));
    }
#endif
}
