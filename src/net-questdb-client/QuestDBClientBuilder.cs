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

using QuestDB.Enums;
using QuestDB.Pooling;
using QuestDB.Utils;

namespace QuestDB;

/// <summary>
///     Fluent builder for <see cref="IQuestDBClient" />. A pool knob set here always wins over the
///     same key embedded in the connect string.
/// </summary>
public sealed class QuestDBClientBuilder
{
    private TimeSpan? _acquireTimeout;
    private string? _confStr;
    private TimeSpan? _housekeeperInterval;
    private TimeSpan? _idleTimeout;
    private bool? _lazyConnect;
    private TimeSpan? _maxLifetime;
    private int? _poolMax;
    private int? _poolMin;
#if NET7_0_OR_GREATER
    private string? _queryConfStr;
    private int? _queryPoolMax;
    private int? _queryPoolMin;
#endif

    /// <summary>
    ///     Sets the ingest connect string. Any ingest scheme; if it is <c>ws</c>/<c>wss</c> and no
    ///     separate <see cref="QueryConfig" /> is given, the same string also configures the query pool.
    /// </summary>
    public QuestDBClientBuilder FromConfig(string confStr)
    {
        _confStr = confStr;
        return this;
    }

    /// <summary>Alias of <see cref="FromConfig" />.</summary>
    public QuestDBClientBuilder IngestConfig(string confStr)
    {
        _confStr = confStr;
        return this;
    }

#if NET7_0_OR_GREATER
    /// <summary>
    ///     Sets the query (egress) connect string for the query-client pool. Must be <c>ws</c>/<c>wss</c>.
    ///     Use this when ingest and query endpoints differ (e.g. an <c>http</c>/<c>tcp</c> ingest handle
    ///     that also needs to query). net7.0+ only.
    /// </summary>
    public QuestDBClientBuilder QueryConfig(string confStr)
    {
        ArgumentNullException.ThrowIfNull(confStr);
        if (!IsWebSocketScheme(confStr))
        {
            throw new IngressError(ErrorCode.ConfigError,
                "query configuration must use the ws or wss scheme");
        }

        _queryConfStr = confStr;
        return this;
    }

    /// <summary>Minimum warm query clients.</summary>
    public QuestDBClientBuilder QueryPoolMin(int min)
    {
        _queryPoolMin = min;
        return this;
    }

    /// <summary>Maximum query clients.</summary>
    public QuestDBClientBuilder QueryPoolMax(int max)
    {
        _queryPoolMax = max;
        return this;
    }

    /// <summary>Fixes the query pool to exactly <paramref name="size" /> clients (min == max).</summary>
    public QuestDBClientBuilder QueryPoolSize(int size)
    {
        _queryPoolMin = size;
        _queryPoolMax = size;
        return this;
    }
#endif

    /// <summary>Minimum warm senders.</summary>
    public QuestDBClientBuilder SenderPoolMin(int min)
    {
        _poolMin = min;
        return this;
    }

    /// <summary>Maximum senders.</summary>
    public QuestDBClientBuilder SenderPoolMax(int max)
    {
        _poolMax = max;
        return this;
    }

    /// <summary>Fixes the pool to exactly <paramref name="size" /> senders (min == max).</summary>
    public QuestDBClientBuilder SenderPoolSize(int size)
    {
        _poolMin = size;
        _poolMax = size;
        return this;
    }

    /// <summary>How long a borrow blocks before throwing.</summary>
    public QuestDBClientBuilder AcquireTimeout(TimeSpan timeout)
    {
        _acquireTimeout = timeout;
        return this;
    }

    /// <summary>Idle duration before the housekeeper reaps a sender.</summary>
    public QuestDBClientBuilder IdleTimeout(TimeSpan timeout)
    {
        _idleTimeout = timeout;
        return this;
    }

    /// <summary>Maximum age before the housekeeper recycles a sender.</summary>
    public QuestDBClientBuilder MaxLifetime(TimeSpan lifetime)
    {
        _maxLifetime = lifetime;
        return this;
    }

    /// <summary>Housekeeper sweep interval.</summary>
    public QuestDBClientBuilder HousekeeperInterval(TimeSpan interval)
    {
        _housekeeperInterval = interval;
        return this;
    }

    /// <summary>
    ///     Tolerant startup: build the handle even while the server is down. The ingest side connects
    ///     asynchronously (buffering writes meanwhile) and the read pool defaults to
    ///     <see cref="QueryPoolMin" /> 0 so nothing connects eagerly — a query connects lazily on first
    ///     <see cref="IQuestDBClient.NewQuery" /> once the server is up. Set explicitly here wins over the
    ///     connect-string <c>lazy_connect</c> key. Rejects a conflicting blocking-startup knob (an explicit
    ///     <c>initial_connect_retry</c> other than <c>async</c>, or an explicit <c>query_pool_min</c> &gt; 0).
    /// </summary>
    public QuestDBClientBuilder LazyConnect(bool enabled = true)
    {
        _lazyConnect = enabled;
        return this;
    }

    /// <summary>Builds and pre-warms the pool.</summary>
    public IQuestDBClient Build()
    {
        if (_confStr is null)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "ingest configuration is required; call FromConfig() or IngestConfig()");
        }

        var poolConfig = new SenderOptions(_confStr);
        if (_poolMin.HasValue)
        {
            poolConfig.sender_pool_min = _poolMin.Value;
        }

        if (_poolMax.HasValue)
        {
            poolConfig.sender_pool_max = _poolMax.Value;
        }

        if (_acquireTimeout.HasValue)
        {
            poolConfig.acquire_timeout_ms = _acquireTimeout.Value;
        }

        if (_idleTimeout.HasValue)
        {
            poolConfig.idle_timeout_ms = _idleTimeout.Value;
        }

        if (_maxLifetime.HasValue)
        {
            poolConfig.max_lifetime_ms = _maxLifetime.Value;
        }

        if (_housekeeperInterval.HasValue)
        {
            poolConfig.housekeeper_interval_ms = _housekeeperInterval.Value;
        }

        poolConfig.ValidatePoolOptions();

        // lazy_connect may be carried by the ingest config or the (separate) query config, and an
        // explicit builder call wins over either. Two query_pool_min signals are tracked across all three
        // sources: `Explicit` (any source set it — drives the default-to-0 decision, against the effective
        // post-precedence value) and `ExplicitPositive` (any source set it > 0 — drives the conflict
        // rejection, so an ingest-string query_pool_min>0 isn't silently dropped when a separate query
        // config overrides sizing).
        var configLazy = poolConfig.lazy_connect;
        var queryPoolMinExplicit = poolConfig.IsQueryPoolMinExplicit;
        var queryPoolMinExplicitPositive = poolConfig.IsQueryPoolMinExplicit && poolConfig.query_pool_min > 0;
        var ingestIsWebSocket = IsWebSocketScheme(_confStr);

#if NET7_0_OR_GREATER
        // Resolve the query config: an explicit QueryConfig wins; otherwise a ws/wss ingest string
        // serves both pools (Java parity). An http/tcp ingest handle with no QueryConfig has no query
        // pool (NewQuery then throws a clear ConfigError).
        var queryConfStr = _queryConfStr;
        if (queryConfStr is null && ingestIsWebSocket)
        {
            queryConfStr = _confStr;
        }

        // Query-pool sizing precedence: explicit builder call > query-config value > ingest-config
        // value > default. poolConfig (parsed from the ingest string) already carries the ingest value.
        if (queryConfStr is not null && !ReferenceEquals(queryConfStr, _confStr))
        {
            var queryOpts = new SenderOptions(queryConfStr);
            poolConfig.query_pool_min = queryOpts.query_pool_min;
            poolConfig.query_pool_max = queryOpts.query_pool_max;
            configLazy = configLazy || queryOpts.lazy_connect;
            queryPoolMinExplicit = queryOpts.IsQueryPoolMinExplicit;
            queryPoolMinExplicitPositive = queryPoolMinExplicitPositive
                || (queryOpts.IsQueryPoolMinExplicit && queryOpts.query_pool_min > 0);
        }

        if (_queryPoolMin.HasValue)
        {
            poolConfig.query_pool_min = _queryPoolMin.Value;
            queryPoolMinExplicit = true;
            queryPoolMinExplicitPositive = queryPoolMinExplicitPositive || _queryPoolMin.Value > 0;
        }

        if (_queryPoolMax.HasValue)
        {
            poolConfig.query_pool_max = _queryPoolMax.Value;
        }
#endif

        var lazy = _lazyConnect ?? configLazy;
        var forceWsAsyncConnect = ResolveLazyConnect(
            lazy, poolConfig, ingestIsWebSocket, queryPoolMinExplicit, queryPoolMinExplicitPositive);

#if NET7_0_OR_GREATER
        if (queryConfStr is not null)
        {
            poolConfig.ValidateQueryPoolOptions();
        }

        return new QuestDBClientImpl(poolConfig, _confStr, queryConfStr, forceWsAsyncConnect);
#else
        return new QuestDBClientImpl(poolConfig, _confStr, null, forceWsAsyncConnect);
#endif
    }

    /// <summary>
    ///     Applies <c>lazy_connect</c> tolerant-startup semantics to the assembled pool config.
    ///     Returns whether pooled <c>ws</c>/<c>wss</c> senders must connect asynchronously so the
    ///     pre-warm does not fail-fast on a down server. When enabled it defaults
    ///     <see cref="SenderOptions.query_pool_min" /> to 0 (lazy first-borrow reads) unless the user set
    ///     it, and throws <see cref="IngressError" />(<see cref="ErrorCode.ConfigError" />) on a knob that
    ///     forces a blocking / fail-fast startup: an explicit <c>initial_connect_retry</c> other than
    ///     <c>async</c>, or an explicit <c>query_pool_min</c> &gt; 0.
    ///     <para />
    ///     <paramref name="queryPoolMinExplicit" /> is true when any source set <c>query_pool_min</c> at all
    ///     (drives the default-to-0 decision); <paramref name="queryPoolMinExplicitPositive" /> is true when
    ///     any source set it &gt; 0 (drives the conflict rejection).
    /// </summary>
    internal static bool ResolveLazyConnect(
        bool lazy, SenderOptions poolConfig, bool ingestIsWebSocket,
        bool queryPoolMinExplicit, bool queryPoolMinExplicitPositive)
    {
        if (!lazy)
        {
            return false;
        }

        if (poolConfig.IsInitialConnectModeExplicit
            && poolConfig.initial_connect_mode != InitialConnectMode.async)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`lazy_connect` requires a non-blocking startup, but `initial_connect_retry` is set to a " +
                "blocking mode; drop `initial_connect_retry` or set it to `async` to use `lazy_connect`");
        }

        if (queryPoolMinExplicitPositive)
        {
            throw new IngressError(ErrorCode.ConfigError,
                "`lazy_connect` defers the read connect, but `query_pool_min` is set > 0 which pre-warms " +
                "the read pool eagerly; drop `query_pool_min` or set it to 0 to use `lazy_connect`");
        }

        // Default the read pool to lazy first-borrow creation when the user left it unset.
        if (!queryPoolMinExplicit)
        {
            poolConfig.query_pool_min = 0;
        }

        // The ingest side goes async so Build() does not block on a down server; ws/wss only.
        return ingestIsWebSocket;
    }

    private static bool IsWebSocketScheme(string conf)
    {
        var idx = conf.IndexOf("::", StringComparison.Ordinal);
        var scheme = idx < 0 ? conf : conf.Substring(0, idx);
        return scheme.Equals("ws", StringComparison.OrdinalIgnoreCase)
               || scheme.Equals("wss", StringComparison.OrdinalIgnoreCase);
    }
}
