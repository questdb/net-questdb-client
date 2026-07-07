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
///     User-supplied observability hook invoked when a store-and-forward sender adopts and drains a
///     crashed sibling's slot directory (<c>drain_orphans=on</c>). Registered via
///     <c>SenderOptions.DrainerListener</c>.
/// </summary>
/// <remarks>
///     <b>Threading.</b> Invoked on the background-drainer worker thread that owns the drain, never on
///     the I/O thread or a producer thread. Slow / throwing listeners cannot stall publishing or
///     reconnect: any exception is caught and traced, and the drain continues.
///     <para />
///     <b>Delivery.</b> Best-effort and rare relative to the data path — orphan adoption happens at
///     sender startup and each slot emits at most one <see cref="BackgroundDrainerEventKind.SlotAdopted" />
///     followed by one terminal outcome
///     (<see cref="BackgroundDrainerEventKind.DrainCompleted" /> /
///     <see cref="BackgroundDrainerEventKind.DrainRetrying" /> /
///     <see cref="BackgroundDrainerEventKind.DrainQuarantined" /> /
///     <see cref="BackgroundDrainerEventKind.DrainCancelled" />).
/// </remarks>
public interface IBackgroundDrainerListener
{
    /// <summary>Invoked once per observed background-drain transition.</summary>
    void OnEvent(BackgroundDrainerEvent evt);
}
