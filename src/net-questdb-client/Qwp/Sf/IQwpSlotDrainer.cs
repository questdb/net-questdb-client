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

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Drains all unacked envelopes from a single slot directory over a dedicated WebSocket
///     connection. Implemented by the cursor engine in drain mode.
/// </summary>
/// <remarks>
///     The drainer reads the slot's segment ring from disk, replays envelopes to the server,
///     advances the ack watermark, and trims segments as they're fully acked. Returns once
///     <c>OldestFsn == NextFsn</c> (slot fully drained) or throws on terminal failure.
///     <para />
///     The pool that invokes this is responsible for the slot lock — implementations must NOT
///     dispose the lock themselves.
/// </remarks>
internal interface IQwpSlotDrainer
{
    Task DrainAsync(string slotDirectory, CancellationToken cancellationToken);
}
