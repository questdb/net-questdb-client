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
using IQwpQueryReader = QuestDB.Qwp.Query.IQwpQueryReader;
using QwpColumnBatch = QuestDB.Qwp.Query.QwpColumnBatch;
using QwpQueryException = QuestDB.Qwp.Query.QwpQueryException;
using QwpServerInfo = QuestDB.Qwp.Query.QwpServerInfo;

namespace QuestDB;

/// <summary>
///     Reusable builder for one egress query, obtained from <see cref="IQuestDBClient.NewQuery" />.
///     Configure with <see cref="Sql" /> and optional <see cref="Binds" />, then call
///     <see cref="ExecuteReaderAsync" /> and iterate the returned cursor. Each execution borrows a
///     query client from the handle's pool; disposing the reader returns it.
///     <para />
///     One in-flight execution per <c>Query</c> instance (single-flight). For concurrent queries,
///     allocate a fresh <see cref="IQuestDBClient.NewQuery" /> per query.
///     <para />
///     Cancellation: <see cref="Cancel" /> is the cooperative path (posts a <c>CANCEL</c> frame, the
///     query ends normally and the client is re-pooled). Passing a <see cref="CancellationToken" />
///     to <c>ReadBatchAsync</c> and cancelling it is a hard cancel that tears the connection down —
///     the client is then discarded, not re-pooled.
/// </summary>
public sealed class Query
{
    private readonly QueryClientPool _pool;
    private string? _sql;
    private QwpBindSetter? _binds;

    // 0 idle, 1 while an execution is in flight (single-flight guard).
    private int _inFlight;

    // The client leased for the duration of the in-flight execution, so Cancel() can reach it.
    // Cleared before the client is returned to the pool so a late Cancel() can't hit a client now
    // serving another query.
    private volatile PooledQueryClient? _inFlightLease;

    // Excludes Cancel()'s lease-read + request-id resolution against the lease-clear on the
    // return path: a lease seen non-null under this gate cannot have been returned to the pool
    // yet, so the request id resolved is this execution's own (or -1), never a successor
    // borrower's. Held only for those field reads — never across the cancel send.
    private readonly object _cancelGate = new();

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

    /// <summary>
    ///     Borrows a pooled query client, submits the configured query, and returns a pull cursor
    ///     over its result stream. The reader owns the borrowed
    ///     client for its whole lifetime: <b>dispose it</b> (<c>await using</c>) to end the query and
    ///     return the client to the pool. An open reader holds one of the pool's
    ///     <c>query_pool_max</c> permits, so leaving readers open exhausts the pool for other
    ///     borrowers.
    /// </summary>
    /// <exception cref="IngressError">
    ///     <see cref="ErrorCode.InvalidApiCall" /> for missing sql, an overlapping execution, or a
    ///     closed handle; <see cref="ErrorCode.PoolExhausted" /> on acquire timeout.
    /// </exception>
    public async ValueTask<IQwpQueryReader> ExecuteReaderAsync(CancellationToken ct = default)
    {
        var sql = _sql;
        var binds = _binds;

        if (string.IsNullOrEmpty(sql))
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "sql is required; call Sql(...) before ExecuteReader");
        }

        if (Interlocked.Exchange(ref _inFlight, 1) != 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "a previous ExecuteReader() is still in flight on this Query; use NewQuery() for concurrent queries");
        }

        PooledQueryClient? pooled = null;
        try
        {
            pooled = await _pool.BorrowAsync(ct).ConfigureAwait(false);
            _inFlightLease = pooled;
            var inner = binds is null
                ? await pooled.ExecuteReaderAsync(sql, ct).ConfigureAwait(false)
                : await pooled.ExecuteReaderAsync(sql, binds, ct).ConfigureAwait(false);
            return new PooledQueryReader(this, pooled, inner);
        }
        catch
        {
            // A failed submit leaves the client either healthy (pre-flight validation) or terminal
            // (send failure) — the return path's terminal check discards the latter.
            if (pooled is not null)
            {
                lock (_cancelGate)
                {
                    _inFlightLease = null;
                }

                await pooled.DisposeAsync().ConfigureAwait(false);
            }

            Interlocked.Exchange(ref _inFlight, 0);
            throw;
        }
    }

    /// <summary>
    ///     Cooperatively cancels the in-flight query (posts a <c>CANCEL</c> frame). The query ends with
    ///     a cancelled or normal terminator and the client is re-pooled. No-op when no query is in
    ///     flight.
    /// </summary>
    public void Cancel()
    {
        PooledQueryClient? lease;
        long requestId;
        lock (_cancelGate)
        {
            lease = _inFlightLease;
            if (lease is null) return;
            requestId = lease.CurrentRequestId;
        }

        if (requestId < 0) return;
        // Dispatched outside the gate: the CANCEL is addressed to this exact request id (ids are
        // unique per client), so even if the client is returned and re-borrowed before the frame is
        // sent, the client drops it rather than cancelling the new borrower's query.
        lease.CancelRequest(requestId);
    }

    /// <summary>
    ///     Extends the pool lease to the reader's lifetime: disposing the reader ends the query on
    ///     the inner reader (draining an abandoned stream), then returns the borrowed client to the
    ///     pool and re-arms this <see cref="Query" />'s single-flight slot.
    /// </summary>
    private sealed class PooledQueryReader : IQwpQueryReader
    {
        private readonly Query _owner;
        private readonly PooledQueryClient _pooled;
        private readonly IQwpQueryReader _inner;
        private int _disposed;

        internal PooledQueryReader(Query owner, PooledQueryClient pooled, IQwpQueryReader inner)
        {
            _owner = owner;
            _pooled = pooled;
            _inner = inner;
        }

        public QwpColumnBatch Current => _inner.Current;
        public long TotalRows => _inner.TotalRows;
        public long RowsAffected => _inner.RowsAffected;
        public QwpOpType OpType => _inner.OpType;
        public bool WasCancelled => _inner.WasCancelled;
        public bool FailoverReset => _inner.FailoverReset;
        public QwpServerInfo? ServerInfo => _inner.ServerInfo;

        public async ValueTask<bool> ReadBatchAsync(CancellationToken ct = default)
        {
            // Own-disposed guard BEFORE touching the pool entry: a stale post-dispose read must
            // not MarkBroken a client that may already be serving another borrower.
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            try
            {
                return await _inner.ReadBatchAsync(ct).ConfigureAwait(false);
            }
            catch (QwpQueryException)
            {
                // A query-level QUERY_ERROR ends at a clean frame boundary; the client is reusable.
                throw;
            }
            catch
            {
                // Transport/protocol damage or a hard CT cancel: discard on return.
                _pooled.MarkBroken();
                throw;
            }
        }

        public bool ReadBatch(CancellationToken ct = default)
        {
            // Threadpool hop drops any captured SyncContext so sync-over-async can't deadlock.
            return Task.Run(() => ReadBatchAsync(ct).AsTask()).GetAwaiter().GetResult();
        }

        public void Cancel()
        {
            // The inner reader's Cancel is rid-scoped and no-ops after dispose, so a stale handle
            // can never cancel a successor borrower's query; skip the call entirely once disposed.
            if (Volatile.Read(ref _disposed) != 0) return;
            _inner.Cancel();
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            try
            {
                await _inner.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                // Clear the lease BEFORE returning the client so a late Cancel() can't reach it;
                // re-arm single-flight only after the client is actually back in the pool.
                lock (_owner._cancelGate)
                {
                    _owner._inFlightLease = null;
                }

                try
                {
                    await _pooled.DisposeAsync().ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.Exchange(ref _owner._inFlight, 0);
                }
            }
        }

        public void Dispose()
        {
            Task.Run(() => DisposeAsync().AsTask()).GetAwaiter().GetResult();
        }
    }
}
#endif
