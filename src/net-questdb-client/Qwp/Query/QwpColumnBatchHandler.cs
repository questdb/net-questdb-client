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

namespace QuestDB.Qwp.Query;

/// <summary>
///     Receives result events from <c>QueryClient.Execute</c>. The batch passed to
///     <see cref="OnBatch" /> is valid only for the duration of the call; spans returned from
///     its accessors must not escape the handler invocation. Each query produces zero or more
///     <see cref="OnBatch" /> calls followed by exactly one terminator: <see cref="OnEnd" />,
///     <see cref="OnExecDone" />, or <see cref="OnError" />.
/// </summary>
public abstract class QwpColumnBatchHandler
{
    /// <summary>Invoked once per RESULT_BATCH. Spans from <paramref name="batch" /> must not escape the call.</summary>
    public virtual void OnBatch(QwpColumnBatch batch) { }

    /// <summary>Terminator for queries that returned rows; <paramref name="totalRows" /> is the cumulative row count.</summary>
    public virtual void OnEnd(long totalRows) { }

    /// <summary>Terminator for queries that failed; <paramref name="status" /> is the QWP status code.</summary>
    public virtual void OnError(byte status, string message) { }

    /// <summary>Terminator for non-row-returning ops (DDL/DML); <paramref name="opType" /> identifies the operation, <paramref name="rowsAffected" /> the row count when applicable.</summary>
    public virtual void OnExecDone(short opType, long rowsAffected) { }

    /// <summary>Fired when the connection failed over to a new node; the in-flight query is restarted from scratch.</summary>
    public virtual void OnFailoverReset(QwpServerInfo? newNode) { }
}
