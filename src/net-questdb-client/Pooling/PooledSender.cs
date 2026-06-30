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

using QuestDB.Senders;

namespace QuestDB.Pooling;

/// <summary>
///     A pool entry: the long-lived box around one real <see cref="ISender" /> plus the bookkeeping the
///     <see cref="SenderPool" /> needs (slot index, creation / idle timestamps, lock-release probe). One
///     entry is allocated per pool slot and reused for every borrow, so steady-state reuse allocates
///     nothing on the inner sender.
///     <para />
///     The entry is never handed to callers directly — each borrow wraps it in a fresh
///     <see cref="BorrowedSender" /> handle, which owns the flush-and-return lifecycle and the
///     use-after-return guard. The entry only knows how to be torn down for real
///     (<see cref="DisposeInner" />), which exactly one party (close / discard / reap) ever does.
/// </summary>
internal sealed class PooledSender
{
    private readonly ISender _inner;

    // Guards the underlying sender's teardown so Close / DiscardBroken / ReapIdle racing each other can
    // never dispose the same delegate twice.
    private int _innerDisposed;

    internal PooledSender(ISender inner, int slotIndex)
    {
        _inner = inner;
        SlotIndex = slotIndex;
        CreatedAtUtc = DateTime.UtcNow;
        IdleSinceUtc = CreatedAtUtc;
    }

    /// <summary>The wrapped real sender, lent to a <see cref="BorrowedSender" /> for the duration of a borrow.</summary>
    internal ISender Inner => _inner;

    /// <summary>SF slot index this entry owns, or -1 when store-and-forward is disabled.</summary>
    internal int SlotIndex { get; }

    /// <summary>Wall-clock creation time; drives max-lifetime reaping.</summary>
    internal DateTime CreatedAtUtc { get; }

    /// <summary>Last time this entry was returned to the pool; drives idle reaping. Guarded by the pool lock.</summary>
    internal DateTime IdleSinceUtc { get; set; }

    /// <summary>
    ///     True if the inner sender holds no SF slot lock or has released it — i.e. the slot index is
    ///     safe to reuse. Always true for non-SF senders (HTTP/TCP/WS-RAM). Call only after the inner
    ///     sender has been disposed.
    /// </summary>
    internal bool IsInnerSlotLockReleased => _inner is not IPooledSlotSender s || s.IsSlotLockReleased;

    /// <summary>Disposes the underlying sender for real. Called only by the pool (close / discard / reap). Idempotent.</summary>
    internal void DisposeInner()
    {
        if (Interlocked.Exchange(ref _innerDisposed, 1) == 0)
        {
            _inner.Dispose();
        }
    }

    /// <summary>Asynchronously disposes the underlying sender for real. Called only by the pool. Idempotent.</summary>
    internal ValueTask DisposeInnerAsync()
    {
        return Interlocked.Exchange(ref _innerDisposed, 1) == 0
            ? _inner.DisposeAsync()
            : ValueTask.CompletedTask;
    }
}
