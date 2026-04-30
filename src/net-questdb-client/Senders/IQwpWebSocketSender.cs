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

namespace QuestDB.Senders;

/// <summary>
///     Extends <see cref="ISender" /> with WebSocket-only operations.
/// </summary>
/// <remarks>
///     <see cref="Sender.New(string)" /> returns an <see cref="ISender" /> for every transport. Users that
///     opted into <c>ws::</c> or <c>wss::</c> can cast to this interface to access ping and per-table
///     <c>seqTxn</c> watermarks. Durable watermarks require <c>request_durable_ack=on</c> on the
///     connect string; the committed watermark is always populated once the server has ACKed any
///     batch on a connection.
/// </remarks>
public interface IQwpWebSocketSender : ISender
{
    /// <summary>
    ///     Highest committed <c>seqTxn</c> the server has acknowledged for the given table on this
    ///     connection. Returns <c>-1</c> when the server has not yet sent a watermark for the table.
    /// </summary>
    /// <remarks>
    ///     Per-table watermarks arrive only when <c>request_durable_ack</c> is enabled. Without
    ///     opt-in the value stays at <c>-1</c>.
    /// </remarks>
    long GetHighestAckedSeqTxn(string tableName);

    /// <summary>
    ///     Highest <c>seqTxn</c> the server has reported as durably uploaded (object-store watermark).
    ///     Returns <c>-1</c> when no durable watermark has been observed.
    /// </summary>
    long GetHighestDurableSeqTxn(string tableName);

    /// <summary>
    ///     Drains the in-flight ACK window. After it returns successfully every batch sent so far has
    ///     been acknowledged by the server and per-table seqTxn watermarks reflect that. Bounded by
    ///     <c>ping_timeout</c>; on an idle connection with nothing in flight it returns immediately
    ///     and is NOT a wire-level liveness probe (ClientWebSocket exposes no PING API).
    /// </summary>
    void Ping(CancellationToken ct = default);

    /// <inheritdoc cref="Ping" />
    Task PingAsync(CancellationToken ct = default);
}
