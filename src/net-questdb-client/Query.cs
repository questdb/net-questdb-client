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
using QuestDB.Enums;
using QuestDB.Pooling;
using QuestDB.Utils;
// Alias the QWP types so this file does not `using QuestDB.Qwp.Query;` — that namespace shares its
// last segment with this `QuestDB.Query` type and the import would create a type/namespace ambiguity.
using QwpBindSetter = QuestDB.Qwp.Query.QwpBindSetter;
using QwpColumnBatchHandler = QuestDB.Qwp.Query.QwpColumnBatchHandler;

namespace QuestDB;

/// <summary>
///     Reusable builder for one egress query, obtained from <see cref="IQuestDBClient.NewQuery" />.
///     Configure with <see cref="Sql" />, optional <see cref="Binds" />, and <see cref="Handler" />,
///     then call <see cref="ExecuteAsync" /> (or <see cref="Execute" />). Each execution borrows a
///     query client from the handle's pool, runs the query, and returns the client.
///     <para />
///     One in-flight execution per <c>Query</c> instance (single-flight). For concurrent queries,
///     allocate a fresh <see cref="IQuestDBClient.NewQuery" /> per query.
///     <para />
///     Cancellation: <see cref="Cancel" /> is the cooperative path (posts a <c>CANCEL</c> frame, the
///     query ends normally and the client is re-pooled). Passing a <see cref="CancellationToken" /> to
///     <see cref="ExecuteAsync" /> and cancelling it is a hard cancel that tears the connection down —
///     the client is then discarded, not re-pooled.
/// </summary>
public sealed class Query
{
    private readonly QueryClientPool _pool;
    private string? _sql;
    private QwpBindSetter? _binds;
    private QwpColumnBatchHandler? _handler;

    // 0 idle, 1 while an ExecuteAsync is in flight (single-flight guard).
    private int _inFlight;

    // The client leased for the duration of the in-flight ExecuteAsync, so Cancel() can reach it.
    // Cleared before the client is returned to the pool so a late Cancel() can't hit a client now
    // serving another query.
    private volatile PooledQueryClient? _inFlightLease;

    internal Query(QueryClientPool pool)
    {
        _pool = pool;
    }

    /// <summary>Sets the SQL text. Returns this for chaining.</summary>
    public Query Sql(string sql)
    {
        _sql = sql;
        return this;
    }

    /// <summary>Sets the bind-value setter (optional). Returns this for chaining.</summary>
    public Query Binds(QwpBindSetter binds)
    {
        _binds = binds;
        return this;
    }

    /// <summary>Sets the result-batch handler. Returns this for chaining.</summary>
    public Query Handler(QwpColumnBatchHandler handler)
    {
        _handler = handler;
        return this;
    }

    /// <summary>
    ///     Borrows a pooled query client, runs the configured query, and returns the client to the
    ///     pool on clean completion (or discards it on any failure / hard cancellation).
    /// </summary>
    /// <exception cref="IngressError">
    ///     <see cref="ErrorCode.InvalidApiCall" /> for missing sql/handler, an overlapping execution,
    ///     or a closed handle; <see cref="ErrorCode.PoolExhausted" /> on acquire timeout.
    /// </exception>
    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        var sql = _sql;
        var handler = _handler;
        var binds = _binds;

        if (string.IsNullOrEmpty(sql))
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "sql is required; call Sql(...) before Execute");
        }

        if (handler is null)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "handler is required; call Handler(...) before Execute");
        }

        if (Interlocked.Exchange(ref _inFlight, 1) != 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "a previous Execute() is still in flight on this Query; use NewQuery() for concurrent queries");
        }

        try
        {
            var pooled = await _pool.BorrowAsync(ct).ConfigureAwait(false);
            _inFlightLease = pooled;
            try
            {
                if (binds is null)
                {
                    await pooled.ExecuteAsync(sql, handler, ct).ConfigureAwait(false);
                }
                else
                {
                    await pooled.ExecuteAsync(sql, binds, handler, ct).ConfigureAwait(false);
                }
            }
            catch
            {
                // Any throw (transport/protocol, or a hard CT cancel) leaves the client terminal.
                pooled.MarkBroken();
                throw;
            }
            finally
            {
                // Clear the lease BEFORE returning the client so a late Cancel() can't reach it.
                _inFlightLease = null;
                await pooled.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            Interlocked.Exchange(ref _inFlight, 0);
        }
    }

    /// <summary>Synchronous wrapper over <see cref="ExecuteAsync" />.</summary>
    public void Execute()
    {
        // Threadpool hop drops any captured SyncContext so sync-over-async can't deadlock.
        Task.Run(() => ExecuteAsync(CancellationToken.None)).GetAwaiter().GetResult();
    }

    /// <summary>
    ///     Cooperatively cancels the in-flight query (posts a <c>CANCEL</c> frame). The query ends with
    ///     a cancelled or normal terminator and the client is re-pooled. No-op when no query is in
    ///     flight.
    /// </summary>
    public void Cancel()
    {
        _inFlightLease?.Cancel();
    }
}
#endif
