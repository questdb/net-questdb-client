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
    private TimeSpan? _maxLifetime;
    private int? _poolMax;
    private int? _poolMin;

    /// <summary>Sets the ingest connect string (any ingest scheme).</summary>
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
        return new QuestDBClientImpl(poolConfig, _confStr);
    }
}
