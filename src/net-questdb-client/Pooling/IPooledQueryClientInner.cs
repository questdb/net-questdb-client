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

namespace QuestDB.Pooling;

/// <summary>
///     Implemented by the egress query client so the pool can tell whether a returned client is still
///     reusable. The client's terminal state is sticky and has no public reset; a clean
///     <c>ExecuteAsync</c> leaves it reusable, but the pool checks this seam as belt-and-braces before
///     re-pooling, so a future non-throwing terminal path can never re-pool a dead client.
///     <para />
///     Kept as a small seam (no net7-only types in the signature) so it can be faked in tests.
/// </summary>
internal interface IPooledQueryClientInner
{
    /// <summary>True when the client has transitioned to a non-recoverable (terminal) or disposed state.</summary>
    bool IsTerminalOrDisposed { get; }

    /// <summary>The request id of the in-flight query, or -1 when none. Ids are unique per client.</summary>
    long CurrentRequestId { get; }

    /// <summary>
    ///     Cooperatively cancels the query with exactly this request id; a no-op if that query is no
    ///     longer the in-flight one. Lets a caller that resolved the id while it provably owned the
    ///     client dispatch the cancel late without ever hitting a successor borrower's query.
    /// </summary>
    void CancelRequest(long requestId);
}
