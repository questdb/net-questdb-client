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

using QuestDB.Qwp.Query;

namespace QuestDB.Senders;

/// <summary>
///     Public surface of the QWP egress query client. One instance owns one WebSocket; one
///     in-flight query at a time per the Phase-1 server contract.
/// </summary>
public interface IQwpQueryClient : IDisposable, IAsyncDisposable
{
    /// <summary>Server identity / role observed during connect; null for v1 servers.</summary>
    QwpServerInfo? ServerInfo { get; }

    /// <summary>QWP version negotiated at the WebSocket upgrade; <c>0</c> until connect completes.</summary>
    int NegotiatedVersion { get; }

    /// <summary>Server-selected batch-body compression (e.g. <c>"zstd;level=3"</c>); <c>null</c> means raw.</summary>
    string? NegotiatedCompression { get; }

    /// <summary>
    ///     <c>true</c> when the most recent <c>Dispose</c>/<c>DisposeAsync</c> hit its 5-second
    ///     grace window before the in-flight Execute drained — the native zstd handle was released
    ///     best-effort but the I/O loop may still be running.
    /// </summary>
    bool WasLastCloseTimedOut { get; }

    /// <summary>
    ///     Submits the SQL query and synchronously drives the handler until the server emits a
    ///     terminator (RESULT_END, EXEC_DONE, or QUERY_ERROR). Throws on transport or protocol
    ///     failure; query-level errors surface via <see cref="QwpColumnBatchHandler.OnError" />.
    /// </summary>
    void Execute(string sql, QwpColumnBatchHandler handler);

    /// <inheritdoc cref="Execute(string, QwpColumnBatchHandler)" />
    void Execute(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler);

    /// <inheritdoc cref="Execute(string, QwpColumnBatchHandler)" />
    Task ExecuteAsync(string sql, QwpColumnBatchHandler handler, CancellationToken cancellationToken = default);

    /// <inheritdoc cref="Execute(string, QwpColumnBatchHandler)" />
    Task ExecuteAsync(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Posts a <c>CANCEL</c> frame for the in-flight query. Thread-safe. The query terminates
    ///     with a <c>QUERY_ERROR</c> (status <c>STATUS_CANCELLED</c>) or, if the server raced to
    ///     finish, a normal <c>RESULT_END</c>. No-op if no query is in flight.
    /// </summary>
    void Cancel();
}
