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
///     Implemented by senders whose transactional mode can stage rows server-side ahead of an explicit
///     commit (the WS / QWP sender under <c>transaction=on</c>). On return, the pool reads
///     <see cref="HasUncommittedDeferredRows" /> to decide whether the entry is safe to re-pool: rows
///     already shipped under <c>FLAG_DEFER_COMMIT</c> cannot be rolled back client-side, so an entry
///     still owing a commit must be discarded (closing the connection drops the staged rows) — re-pooling
///     it would let the next borrower's first commit silently publish the previous borrow's abandoned rows.
///     <para />
///     Kept net-agnostic (no dependency on the net7-only WS type) so the pool compiles on net6.0, and
///     so it can be faked in tests.
/// </summary>
internal interface IPooledTransactionalSender
{
    /// <summary>True while rows shipped with <c>FLAG_DEFER_COMMIT</c> are still awaiting a commit frame.</summary>
    bool HasUncommittedDeferredRows { get; }
}
