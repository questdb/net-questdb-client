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
///     Implemented by senders that hold a store-and-forward slot lock (the WS / QWP sender). After a
///     pooled sender is disposed, the pool reads <see cref="IsSlotLockReleased" /> to decide whether
///     the slot index is safe to reuse: a wedged I/O path could leave the OS lock held, in which case
///     the index must be retired rather than handed to a new sender.
///     <para />
///     Kept net-agnostic (no dependency on the net7-only WS type) so the pool compiles on net6.0, and
///     so it can be faked in tests.
/// </summary>
internal interface IPooledSlotSender
{
    /// <summary>True when this sender holds no slot lock, or its slot lock has been released.</summary>
    bool IsSlotLockReleased { get; }
}
