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

namespace QuestDB;

/// <summary>
///     High-level handle that owns a pool of <see cref="ISender" /> instances. Construct once with
///     <see cref="QuestDBClient.Connect" /> or <see cref="QuestDBClient.Builder" />, then share across
///     threads — <see cref="BorrowSender" /> and <see cref="BorrowSenderAsync" /> may be called
///     concurrently from any thread.
/// </summary>
public interface IQuestDBClient : IDisposable, IAsyncDisposable
{
    /// <summary>Number of idle senders currently parked in the pool.</summary>
    int AvailableSenderCount { get; }

    /// <summary>Total senders alive in the pool (idle + in-use).</summary>
    int TotalSenderCount { get; }

    /// <summary>
    ///     Borrows a sender from the pool. The caller MUST dispose the returned instance (a
    ///     <c>using</c> block is the idiom) to release it back to the pool: disposing a pooled sender
    ///     flushes pending rows and returns it — the real connection is only closed when this handle is
    ///     disposed.
    ///     <para />
    ///     Blocks up to <c>acquire_timeout_ms</c> when every sender up to <c>sender_pool_max</c> is in
    ///     use, then throws.
    /// </summary>
    /// <returns>A sender leased from the pool; release it with <see cref="IDisposable.Dispose" />.</returns>
    /// <exception cref="Utils.IngressError">If the pool is exhausted past the acquire timeout, or the handle is closed.</exception>
    ISender BorrowSender();

    /// <inheritdoc cref="BorrowSender" />
    /// <param name="ct">Cancels the wait for a free sender.</param>
    ValueTask<ISender> BorrowSenderAsync(CancellationToken ct = default);

    /// <summary>
    ///     Returns a sender pinned to the calling thread. The first call on a thread borrows one from
    ///     the pool and pins it; later calls on the same thread return the same instance until
    ///     <see cref="ReleaseSender" /> (or this handle is closed). Use this for dedicated, long-lived
    ///     producer threads where borrow / return overhead would dominate.
    ///     <para />
    ///     <b>Threading:</b> the pin is thread-affine. Do NOT hold a pinned sender across an
    ///     <c>await</c> — the continuation may resume on another thread. For async or short-lived
    ///     callers prefer <see cref="BorrowSender" />.
    /// </summary>
    ISender Sender();

    /// <summary>
    ///     Releases the calling thread's pinned <see cref="Sender" /> (if any) back to the pool. Call
    ///     this before a borrowed thread (e.g. a thread-pool / event-loop thread) is recycled so a
    ///     sender is not pinned for the lifetime of a thread that no longer needs it.
    /// </summary>
    void ReleaseSender();

    /// <summary>
    ///     Shuts the pool down, closing every underlying sender. Idempotent. Threads blocked in
    ///     <see cref="BorrowSender" /> are released with an error. Equivalent to
    ///     <see cref="IDisposable.Dispose" />.
    /// </summary>
    void Close();
}
