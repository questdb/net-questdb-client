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
using QuestDB.Senders;
using QuestDB.Utils;
#if NET7_0_OR_GREATER
using QwpColumnBatchHandler = QuestDB.Qwp.Query.QwpColumnBatchHandler;
#endif

namespace QuestDB.Pooling;

/// <summary>
///     Default <see cref="IQuestDBClient" />: owns a <see cref="SenderPool" /> and, on net7.0+ when a
///     query config is supplied, a <see cref="QueryClientPool" />. A single
///     <see cref="PoolHousekeeper" /> reaps idle / over-age instances from both pools.
/// </summary>
internal sealed class QuestDBClientImpl : IQuestDBClient
{
    private readonly PoolHousekeeper _housekeeper;
    private readonly SenderPool _pool;
#if NET7_0_OR_GREATER
    private readonly QueryClientPool? _queryPool;
#endif

    internal QuestDBClientImpl(SenderOptions poolConfig, string ingestConfStr, string? queryConfStr)
    {
        SenderPool? pool = null;
        PoolHousekeeper? housekeeper = null;
#if NET7_0_OR_GREATER
        QueryClientPool? queryPool = null;
#endif
        // The sender pool warms `sender_pool_min` live connections (and, in ws::+sf_dir mode, takes
        // slot flocks / mmaps) the moment it is constructed. If a later step throws — most realistically
        // the query pool's live /read/v1 prewarm against a down endpoint — the half-built handle is
        // never returned, so Close() never runs and everything already built leaks. Tear it down here.
        try
        {
            pool = new SenderPool(poolConfig, ingestConfStr);
#if NET7_0_OR_GREATER
            queryPool = queryConfStr is null ? null : new QueryClientPool(poolConfig, queryConfStr);
            housekeeper = new PoolHousekeeper(pool, queryPool, poolConfig.housekeeper_interval_ms);
#else
            housekeeper = new PoolHousekeeper(pool, poolConfig.housekeeper_interval_ms);
#endif
        }
        catch
        {
            // Same order as Close(): stop the sweeper, then query pool, then sender pool (owns SF/IO).
            SafeTeardownOnConstructionFailure(housekeeper,
#if NET7_0_OR_GREATER
                queryPool,
#endif
                pool);
            throw;
        }

        _pool = pool;
        _housekeeper = housekeeper;
#if NET7_0_OR_GREATER
        _queryPool = queryPool;
#endif
    }

    // Test seam: inject a sender factory (sender-only handle, no query pool).
    internal QuestDBClientImpl(SenderOptions poolConfig, Func<int, ISender> senderFactory)
    {
        SenderPool? pool = null;
        PoolHousekeeper? housekeeper = null;
        try
        {
            pool = new SenderPool(poolConfig, null, senderFactory);
            housekeeper = new PoolHousekeeper(pool, poolConfig.housekeeper_interval_ms);
        }
        catch
        {
            SafeTeardownOnConstructionFailure(housekeeper,
#if NET7_0_OR_GREATER
                null,
#endif
                pool);
            throw;
        }

        _pool = pool;
        _housekeeper = housekeeper;
    }

#if NET7_0_OR_GREATER
    // Test seam: inject both a sender factory and a query-client factory.
    internal QuestDBClientImpl(SenderOptions poolConfig, Func<int, ISender> senderFactory,
        Func<ValueTask<IQwpQueryClient>> queryFactory)
    {
        SenderPool? pool = null;
        QueryClientPool? queryPool = null;
        PoolHousekeeper? housekeeper = null;
        try
        {
            pool = new SenderPool(poolConfig, null, senderFactory);
            queryPool = new QueryClientPool(poolConfig, null, queryFactory);
            housekeeper = new PoolHousekeeper(pool, queryPool, poolConfig.housekeeper_interval_ms);
        }
        catch
        {
            SafeTeardownOnConstructionFailure(housekeeper, queryPool, pool);
            throw;
        }

        _pool = pool;
        _queryPool = queryPool;
        _housekeeper = housekeeper;
    }
#endif

    // Best-effort teardown of whatever was built before a constructor threw. Each step is independently
    // guarded so one failing teardown can't strand the resources owned by the others.
    private static void SafeTeardownOnConstructionFailure(
        PoolHousekeeper? housekeeper,
#if NET7_0_OR_GREATER
        QueryClientPool? queryPool,
#endif
        SenderPool? pool)
    {
        try
        {
            housekeeper?.Dispose();
        }
        catch
        {
            // best effort
        }

#if NET7_0_OR_GREATER
        try
        {
            queryPool?.Close();
        }
        catch
        {
            // best effort
        }
#endif

        try
        {
            pool?.Close();
        }
        catch
        {
            // best effort
        }
    }

    public int AvailableSenderCount => _pool.AvailableSize;
    public int TotalSenderCount => _pool.TotalSize;

    public ISender BorrowSender()
    {
        return _pool.Borrow();
    }

    public async ValueTask<ISender> BorrowSenderAsync(CancellationToken ct = default)
    {
        return await _pool.BorrowAsync(ct).ConfigureAwait(false);
    }

#if NET7_0_OR_GREATER
    public int AvailableQueryClientCount => RequireQueryPool().AvailableSize;
    public int TotalQueryClientCount => RequireQueryPool().TotalSize;

    public Query NewQuery()
    {
        return new Query(RequireQueryPool());
    }

    public Task ExecuteSqlAsync(string sql, QwpColumnBatchHandler handler, CancellationToken ct = default)
    {
        return NewQuery().Sql(sql).Handler(handler).ExecuteAsync(ct);
    }

    private QueryClientPool RequireQueryPool()
    {
        return _queryPool ?? throw new IngressError(ErrorCode.ConfigError,
            "no query configuration; pass a ws:: / wss:: query config via QueryConfig() or " +
            "QuestDBClient.Connect(ingest, query)");
    }
#endif

    public void Close()
    {
        // Stop the sweeper before tearing the pools down so it cannot reap mid-close.
        _housekeeper.Dispose();
#if NET7_0_OR_GREATER
        // Query pool first (no flock/mmap); sender pool last (owns SF/flock/IO resources).
        _queryPool?.Close();
#endif
        _pool.Close();
    }

    public void Dispose()
    {
        Close();
    }

    public async ValueTask DisposeAsync()
    {
        // Async join so we don't block the caller for up to the housekeeper's 2s budget.
        await _housekeeper.StopAsync().ConfigureAwait(false);
        _housekeeper.Dispose();
#if NET7_0_OR_GREATER
        if (_queryPool is not null)
        {
            await _queryPool.CloseAsync().ConfigureAwait(false);
        }
#endif
        await _pool.CloseAsync().ConfigureAwait(false);
    }
}
