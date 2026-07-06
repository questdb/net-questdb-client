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

namespace QuestDB.Qwp.Query;

/// <summary>
///     <c>DbDataReader</c>-style pull cursor over one query's result stream. Obtained from
///     <c>ExecuteReaderAsync</c> (on <see cref="QuestDB.Senders.IQwpQueryClient" />,
///     <see cref="QuestDB.Query" />, or <see cref="QuestDB.IQuestDBClient" />); the query is
///     already submitted when the reader is returned. Not thread-safe: one consumer drives one
///     reader.
///     <para />
///     <b>Always dispose the reader</b> (<c>await using</c> is the idiom). Disposal ends the query —
///     draining an abandoned stream to its terminator — and, for pooled readers, returns the
///     borrowed client to the pool; a reader that is never disposed wedges its client's
///     single-flight slot.
///     <para />
///     <see cref="Current" /> — and every span its accessors return — is only valid until the next
///     <see cref="ReadBatchAsync" /> call: the batch and its backing buffers are reused. Copy any
///     string / array data you need to keep.
/// </summary>
public interface IQwpQueryReader : IAsyncDisposable, IDisposable
{
    /// <summary>
    ///     Advances to the next result batch. Returns <c>true</c> when a batch is available in
    ///     <see cref="Current" />; <c>false</c> when the query finished (rows exhausted, DDL/DML
    ///     completion — see <see cref="RowsAffected" /> / <see cref="OpType" /> — or a cooperative
    ///     cancel, reported via <see cref="WasCancelled" />).
    /// </summary>
    /// <exception cref="QwpQueryException">
    ///     The server rejected or aborted the query (QUERY_ERROR). The underlying client is still
    ///     usable unless the error was connection-scoped.
    /// </exception>
    /// <exception cref="Utils.IngressError">Transport or protocol failure; the client is terminal.</exception>
    /// <exception cref="OperationCanceledException">
    ///     <paramref name="ct" /> fired — a hard cancel that tears the connection down and leaves
    ///     the client terminal. Use <see cref="Cancel" /> for cooperative cancellation.
    /// </exception>
    ValueTask<bool> ReadBatchAsync(CancellationToken ct = default);

    /// <summary>Synchronous wrapper over <see cref="ReadBatchAsync" />.</summary>
    bool ReadBatch(CancellationToken ct = default);

    /// <summary>
    ///     The batch produced by the last successful <see cref="ReadBatchAsync" />. Valid — including
    ///     every span returned from its accessors — only until the next <see cref="ReadBatchAsync" />
    ///     call. Throws before the first read and after a read returned <c>false</c>.
    /// </summary>
    QwpColumnBatch Current { get; }

    /// <summary>
    ///     Cumulative row count reported by RESULT_END; populated once reading has finished. This
    ///     and the other result stats stay readable after dispose (they freeze at this query's
    ///     final values).
    /// </summary>
    long TotalRows { get; }

    /// <summary>Rows affected reported by EXEC_DONE (DDL/DML); populated once reading has finished.</summary>
    long RowsAffected { get; }

    /// <summary>Operation type reported by EXEC_DONE; <see cref="QwpOpType.None" /> for row-returning queries.</summary>
    QwpOpType OpType { get; }

    /// <summary><c>true</c> when the query ended via cooperative <see cref="Cancel" /> (STATUS_CANCELLED).</summary>
    bool WasCancelled { get; }

    /// <summary>
    ///     <c>true</c> on a read that resumed after a transparent failover reconnect: all previously
    ///     returned rows are void and the stream is replaying from the top.
    /// </summary>
    bool FailoverReset { get; }

    /// <summary>Identity of the server currently streaming results (updated across failover).</summary>
    QwpServerInfo? ServerInfo { get; }

    /// <summary>
    ///     Posts a <c>CANCEL</c> frame for <b>this reader's query</b> (cooperative, thread-safe).
    ///     The stream ends with a normal read returning <c>false</c> and <see cref="WasCancelled" />
    ///     set — the client survives — unlike cancelling <see cref="ReadBatchAsync" />'s token,
    ///     which is terminal. Scoped to this query's request id and a no-op after dispose, so a
    ///     stale handle (e.g. a late timeout callback) can never cancel a later query on the same
    ///     — possibly re-pooled — client.
    /// </summary>
    void Cancel();
}

#endif
