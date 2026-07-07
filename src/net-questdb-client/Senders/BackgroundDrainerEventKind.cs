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
///     Categorisation of a <see cref="BackgroundDrainerEvent" /> emitted while a store-and-forward
///     sender adopts and drains a crashed sibling's slot directory (<c>drain_orphans=on</c>).
/// </summary>
public enum BackgroundDrainerEventKind
{
    /// <summary>An orphaned slot's lock was claimed and the slot was queued for a background drain.</summary>
    SlotAdopted,

    /// <summary>The slot drained cleanly: every un-acked frame was delivered and the segment files unlinked.</summary>
    DrainCompleted,

    /// <summary>
    ///     A <b>transient</b> failure (drain timeout, server outage, reconnect-budget exhaustion) left the
    ///     slot un-drained. No sentinel is dropped; the slot is left for re-adoption on a later sweep.
    /// </summary>
    DrainRetrying,

    /// <summary>
    ///     A <b>deterministic</b> terminal failure (auth reject, protocol / poison-frame violation, corrupt
    ///     segments). A <c>.failed</c> sentinel is written so later sweeps skip the slot for manual inspection.
    /// </summary>
    DrainQuarantined,

    /// <summary>The drain was cancelled (sender shutdown). The slot is left intact for a retry on next startup.</summary>
    DrainCancelled,
}
