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
 ******************************************************************************/

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Callback contract for consuming a streamed QWP egress query result. The .NET
///     counterpart of Java's <c>QwpColumnBatchHandler</c> on java-questdb-client main
///     64b7ee69. Invoked by <c>QwpQueryClient.Execute</c>: once per RESULT_BATCH frame
///     via <see cref="OnBatch"/>, then exactly once via either <see cref="OnEnd"/>,
///     <see cref="OnError"/>, or <see cref="OnExecDone"/> (non-SELECT statements).
/// </summary>
/// <remarks>
///     Experimental. The <see cref="QwpColumnBatch"/> handed to <see cref="OnBatch"/>
///     is a flyweight valid only for the duration of that call — copy any values you
///     need to retain.
///     <para/>
///     <strong>Exception contract</strong>: throwing from any callback aborts the
///     query and propagates the exception out of <c>Execute</c> on the caller's
///     thread; no further callbacks fire for that query. The connection remains
///     usable for subsequent queries.
/// </remarks>
internal interface IQwpColumnBatchHandler
{
    /// <summary>Invoked for every RESULT_BATCH received. Throwing aborts the query.</summary>
    void OnBatch(QwpColumnBatch batch);

    /// <summary>Invoked exactly once after the last batch on successful completion.</summary>
    void OnEnd(long totalRows);

    /// <summary>Invoked exactly once if the query fails at any point (instead of <see cref="OnEnd"/>).</summary>
    void OnError(byte status, string? message);

    /// <summary>
    ///     Invoked when <c>QwpQueryClient.Execute</c> has transparently reconnected
    ///     to another endpoint after a transport failure and is about to re-submit the
    ///     query. <paramref name="newNode"/> is the resolved server info, or null if
    ///     the new server negotiated v1 protocol (no SERVER_INFO frame). Default is a
    ///     no-op — handlers that accumulate state across batches should override and
    ///     discard the partial state from the prior attempt.
    /// </summary>
    void OnFailoverReset(QwpServerInfo? newNode) { /* default no-op */ }

    /// <summary>
    ///     Invoked in place of <see cref="OnBatch"/> + <see cref="OnEnd"/> when the
    ///     query was non-SELECT (DDL/INSERT/UPDATE). Default is a no-op for
    ///     SELECT-only handlers.
    /// </summary>
    void OnExecDone(short opType, long rowsAffected) { /* default no-op */ }
}

/// <summary>
///     Per-row callback consumed by <c>QwpColumnBatch.ForEachRow</c>. The
///     <see cref="RowView"/> handed to the delegate is a reusable flyweight pointing
///     at the current row; do not retain it past the call.
/// </summary>
internal delegate void RowCallback(RowView row);
