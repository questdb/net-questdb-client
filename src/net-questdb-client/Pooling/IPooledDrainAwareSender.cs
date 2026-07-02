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
///     Implemented by senders that ship asynchronously and can hold un-acked in-flight data after a
///     borrow returns (the WS / QWP sender, whose cursor-engine ring keeps frames until the server
///     acks them). The pool reads <see cref="IsFullyDrained" /> during idle reaping and keeps the idle
///     clock from starting until it is true, so a pooled sender is never torn down — which in RAM mode
///     frees the ring, silently dropping those frames — while it still owes un-acked rows. Once drained,
///     the sender reaps normally; a permanently wedged one simply lives until the pool is closed.
///     <para />
///     Kept net-agnostic (no dependency on the net7-only WS type) so the pool compiles on net6.0, and
///     so it can be faked in tests. Senders that deliver synchronously (HTTP / TCP) don't implement it
///     and are treated as always drained.
/// </summary>
internal interface IPooledDrainAwareSender
{
    /// <summary>True when every appended frame has been acknowledged — the sender owes no un-acked data.</summary>
    bool IsFullyDrained { get; }
}
