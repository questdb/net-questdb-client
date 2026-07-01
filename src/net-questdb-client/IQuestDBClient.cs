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

using QuestDB.Senders;
#if NET7_0_OR_GREATER
using QwpColumnBatchHandler = QuestDB.Qwp.Query.QwpColumnBatchHandler;
#endif

namespace QuestDB;

/// <summary>
///     High-level handle that owns a pool of <see cref="ISender" /> instances. Construct once with
///     <see cref="QuestDBClient.Connect(string)" /> or <see cref="QuestDBClient.Builder" />, then share across
///     threads — <see cref="BorrowSender" /> and <see cref="BorrowSenderAsync" /> may be called
///     concurrently from any thread.
/// </summary>
public interface IQuestDBClient : IDisposable, IAsyncDisposable
{
    /// <summary>Number of idle senders currently parked in the pool.</summary>
    int AvailableSenderCount { get; }

    /// <summary>Total senders alive in the pool (idle + in-use).</summary>
    int TotalSenderCount { get; }

    /// <summary>
    ///     Borrows a sender from the pool. The caller MUST dispose the returned instance (a
    ///     <c>using</c> block is the idiom) to release it back to the pool.
    ///     <para />
    ///     <b>Disposing does NOT send.</b> It discards any buffered-but-unsent rows and returns the sender
    ///     to the pool; the real connection is only closed when this handle is disposed. Call
    ///     <see cref="ISender.Send(CancellationToken)" /> (hand rows to the transport) — and, for delivery
    ///     confirmation, <see cref="ISender.Flush(TimeSpan, CancellationToken)" /> per sender or
    ///     <see cref="Flush(TimeSpan, CancellationToken)" /> across the whole pool — <b>before</b> disposing.
    ///     <para />
    ///     Blocks up to <c>acquire_timeout_ms</c> when every sender up to <c>sender_pool_max</c> is in
    ///     use, then throws.
    ///     <para />
    ///     <b>Do not use the sender after disposing it.</b> Dispose returns the instance to the pool,
    ///     where another thread may immediately re-borrow it. The returned handle is inert, not aliased:
    ///     every ingest member (<c>Table</c> / <c>Column</c> / <c>At</c> / <c>Send</c>, …) throws
    ///     <see cref="ObjectDisposedException" /> once disposed, so a stale reference can never reach — or
    ///     corrupt — the entry the pool has since lent to a different borrower (a second dispose is a
    ///     tolerated no-op). That guard makes accidental misuse fail loudly; it is not a licence to share a
    ///     <em>live</em> borrowed sender across threads — a single <see cref="ISender" /> is not
    ///     thread-safe. Borrow per unit of work, drop the reference when the <c>using</c> scope ends, and
    ///     never cache or share a borrowed sender across threads. There is no thread-affine / pinned-sender
    ///     API.
    /// </summary>
    /// <returns>A sender leased from the pool; release it with <see cref="IDisposable.Dispose" />.</returns>
    /// <exception cref="Utils.IngressError">If the pool is exhausted past the acquire timeout, or the handle is closed.</exception>
    ISender BorrowSender();

    /// <inheritdoc cref="BorrowSender" />
    /// <param name="ct">Cancels the wait for a free sender.</param>
    ValueTask<ISender> BorrowSenderAsync(CancellationToken ct = default);

    /// <summary>
    ///     Drains every sender in the pool: flushes each one's buffered rows and blocks until the server has
    ///     acknowledged them, or <paramref name="timeout" /> elapses (applied per sender). The pool-wide
    ///     delivery barrier — the analogue of Java's <c>drain()</c> at pool scope.
    ///     <para />
    ///     <b>Call this during quiescence</b> — after every borrowed sender has been returned. It does not
    ///     synchronise against a concurrent borrow (draining a sender another thread is writing to is a data
    ///     race). Typical use: producers finish and return their senders, then the coordinator calls
    ///     <c>Flush</c> and only marks the batch done when it returns <c>true</c>.
    /// </summary>
    /// <param name="timeout">Upper bound on the ACK wait, per sender.</param>
    /// <param name="ct">Cancels the flush and the wait.</param>
    /// <returns><c>true</c> if every sender drained fully; <c>false</c> if any timed out or latched an error.</returns>
    bool Flush(TimeSpan timeout, CancellationToken ct = default);

    /// <summary>Convenience overload using <c>close_flush_timeout_millis</c> as the per-sender drain timeout.</summary>
    bool Flush(CancellationToken ct = default);

    /// <inheritdoc cref="Flush(TimeSpan, CancellationToken)" />
    ValueTask<bool> FlushAsync(TimeSpan timeout, CancellationToken ct = default);

    /// <inheritdoc cref="Flush(CancellationToken)" />
    ValueTask<bool> FlushAsync(CancellationToken ct = default);

#if NET7_0_OR_GREATER
    /// <summary>Number of idle query clients currently parked in the pool.</summary>
    /// <exception cref="Utils.IngressError">If the handle has no query configuration.</exception>
    int AvailableQueryClientCount { get; }

    /// <summary>Total query clients alive in the pool (idle + in-use).</summary>
    /// <exception cref="Utils.IngressError">If the handle has no query configuration.</exception>
    int TotalQueryClientCount { get; }

    /// <summary>
    ///     Allocates a fresh <see cref="Query" /> bound to this handle's query-client pool. Configure it
    ///     with <c>Sql</c> / <c>Binds</c> / <c>Handler</c>, then <c>ExecuteAsync</c>. Each execution
    ///     borrows a pooled query client for the duration of one query and returns it automatically —
    ///     there is no explicit borrow/release for queries (unlike senders).
    ///     <para />
    ///     Allocate a fresh <c>NewQuery()</c> per query when running queries concurrently; a single
    ///     <see cref="Query" /> allows only one in-flight execution.
    ///     <para />
    ///     Requires a <c>ws</c>/<c>wss</c> query configuration (a single <c>ws</c> connect string, or an
    ///     explicit <c>QueryConfig</c> / <see cref="QuestDBClient.Connect(string, string)" />); throws
    ///     <see cref="Utils.IngressError" /> otherwise. net7.0+ only.
    /// </summary>
    Query NewQuery();

    /// <summary>
    ///     Convenience for a bind-less query: equivalent to
    ///     <c>NewQuery().Sql(sql).Handler(handler).ExecuteAsync(ct)</c>. Borrows a pooled query client,
    ///     runs the query, and returns the client (or discards it on a hard cancel / transport failure).
    /// </summary>
    Task ExecuteSqlAsync(string sql, QwpColumnBatchHandler handler, CancellationToken ct = default);
#endif

    /// <summary>
    ///     Shuts the pool down, closing every underlying sender. Idempotent. Threads blocked in
    ///     <see cref="BorrowSender" /> are released with an error. Equivalent to
    ///     <see cref="IDisposable.Dispose" />.
    /// </summary>
    void Close();
}
