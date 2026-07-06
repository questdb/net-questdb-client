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

using QuestDB.Enums;
using QuestDB.Senders;
using QuestDB.Utils;

namespace QuestDB.Pooling;

/// <summary>
///     Elastic pool of <see cref="ISender" /> instances, each boxed in a reusable
///     <see cref="PooledSender" /> entry and lent out per borrow as a fresh
///     <see cref="BorrowedSender" /> handle. Keeps at least <c>min</c> senders warm, grows on
///     demand to <c>max</c>, and (via <see cref="ReapIdle" />, driven by the housekeeper) reaps
///     idle / over-age senders down to <c>min</c>.
///     <para />
///     Capacity is bounded by a <see cref="SemaphoreSlim" /> that counts in-use senders: a permit is
///     taken on borrow and released on return. A sender is only created when no idle one exists and a
///     permit is held, which keeps the total alive count ≤ <c>max</c>. Sender construction (TLS / DNS
///     / connect) happens OUTSIDE the lock so a slow connect cannot block other borrowers. The reaper
///     competes through the same gate (<see cref="TryWithholdPermit" />): it only removes an idle
///     sender after taking a permit for it, and a reaped or retired SF sender keeps that permit
///     physically withheld until its slot index is reusable, so the semaphore never advertises
///     capacity whose slot is still locked.
/// </summary>
internal sealed class SenderPool
{
    private readonly SemaphoreSlim _capacity;
    private readonly CancellationTokenSource _closeCts = new();
    private readonly string? _confStr;
    private readonly object _gate = new();
    private readonly TimeSpan _idleTimeout;
    private readonly TimeSpan _maxLifetime;
    private readonly int _max;
    private readonly int _min;
    private readonly Func<int, ISender> _senderFactory;
    private readonly int _acquireTimeoutMs;
    private readonly TimeSpan _defaultFlushTimeout;

    // lazy_connect: pooled ws senders connect asynchronously so a down server doesn't fail-fast the
    // pre-warm; writes buffer until the wire is up. No effect on non-ws senders.
    private readonly bool _forceWsAsyncConnect;

    // Idle senders as a deque sorted by idle time: index 0 is the coldest (longest idle), the tail the
    // hottest. Borrowers pop/push the hot end — LIFO reuse concentrates traffic on few senders so the
    // excess goes genuinely cold and shrinks toward min — which keeps the list IdleSinceUtc-ordered, so
    // the reaper only ever inspects the cold end and stops at the first entry inside its timeout:
    // O(reaped), not O(idle). max_lifetime is NOT covered by that order (age follows CreatedAtUtc, not
    // IdleSinceUtc), so it is handled by a separate walk gated on _idleOldestCreatedUtc: a conservative
    // lower bound over the parked entries' CreatedAtUtc, only lowered when an entry is parked and
    // retightened by the walk itself — the walk therefore runs only when some parked entry has actually
    // crossed max_lifetime, keeping the common sweep O(reaped).
    private readonly List<PooledSender> _idle = new();
    private DateTime _idleOldestCreatedUtc = DateTime.MaxValue;
    private readonly List<PooledSender> _all = new();
    private bool _closed;

    // Store-and-forward slot management. Each pooled WS+SF sender owns a distinct slot identity
    // (`<base>-<index>`) so siblings never collide on a slot directory / flock. `_freeSlots` holds the
    // indices owned by no live (or not-yet-fully-torn-down) sender; LIFO reuse keeps the on-disk
    // working set of slot directories compact. A retired index (lock not yet released after dispose) is
    // simply absent from the stack and counts against effective capacity, matching the Java pool's
    // `leakedSlots`. Unlike a permanent leak, the housekeeper re-tests retired slots
    // (`ReclaimRetiredSlots`) and frees the index + permit once the engine's deferred teardown finally
    // releases the lock, so retirement is a live shrink, not a monotonic counter.
    private readonly bool _storeAndForward;
    private readonly string _slotBaseId;
    private readonly Stack<int> _freeSlots = new();

    // Senders retired with their slot lock still held, awaiting reclaim. Every entry has exactly one
    // capacity permit physically withheld on its behalf: a discarded sender's own permit is simply not
    // released, and the reap path takes a free permit up front — skipping the reap entirely when none is
    // free (see ReapIdle). Reclaiming therefore always frees the index AND releases one permit; there is
    // no deferred "leak debt" to settle.
    private readonly List<PooledSender> _retired = new();

    /// <summary>Production constructor: pool sizes come from <paramref name="poolConfig" />, senders are
    ///     built from <paramref name="confStr" />. <paramref name="forceWsAsyncConnect" /> is set by the
    ///     <c>lazy_connect</c> facade path so pooled ws senders connect asynchronously.</summary>
    internal SenderPool(SenderOptions poolConfig, string confStr, bool forceWsAsyncConnect = false)
        : this(poolConfig, confStr, null, forceWsAsyncConnect)
    {
    }

    /// <summary>Test seam: inject a sender factory so unit tests need no live server. <paramref name="confStr" />
    ///     may be null when a factory is supplied.</summary>
    internal SenderPool(SenderOptions poolConfig, string? confStr, Func<int, ISender>? senderFactory,
        bool forceWsAsyncConnect = false)
    {
        // Re-validate: builder methods may have mutated min/max after the connect-string parse.
        poolConfig.ValidatePoolOptions();

        _min = poolConfig.sender_pool_min;
        _max = poolConfig.sender_pool_max;
        _acquireTimeoutMs = checked((int)poolConfig.acquire_timeout_ms.TotalMilliseconds);
        _defaultFlushTimeout = poolConfig.close_flush_timeout_millis;
        _idleTimeout = poolConfig.idle_timeout_ms;
        _maxLifetime = poolConfig.max_lifetime_ms;
        _confStr = confStr;
        _senderFactory = senderFactory ?? CreateDefaultInner;
        _forceWsAsyncConnect = forceWsAsyncConnect;
        _capacity = new SemaphoreSlim(_max, _max);

        _storeAndForward = poolConfig.IsWebSocket() && !string.IsNullOrEmpty(poolConfig.sf_dir);
        _slotBaseId = poolConfig.sender_id;
        if (_storeAndForward)
        {
            // Push high-to-low so the first pops hand out 0, 1, 2... — fresh pools fill from index 0.
            for (var i = _max - 1; i >= 0; i--)
            {
                _freeSlots.Push(i);
            }
        }

        try
        {
            PreWarm();
        }
        catch
        {
            // A failed pre-warm (e.g. a slow/refused warm connect) must not leak the primitives.
            _closeCts.Dispose();
            _capacity.Dispose();
            throw;
        }
    }

    /// <summary>Number of idle senders currently parked in the pool.</summary>
    internal int AvailableSize
    {
        get
        {
            lock (_gate)
            {
                return _idle.Count;
            }
        }
    }

    /// <summary>Total senders alive (idle + in-use).</summary>
    internal int TotalSize
    {
        get
        {
            lock (_gate)
            {
                return _all.Count;
            }
        }
    }

    internal bool IsClosed
    {
        get
        {
            lock (_gate)
            {
                return _closed;
            }
        }
    }

    /// <summary>Count of SF slot indices currently retired because a sender has not yet released its lock.
    ///     Reclaimed back toward zero by the housekeeper once the lock releases (see <see cref="ReclaimRetiredSlots" />).</summary>
    internal int LeakedSlotCount
    {
        get
        {
            lock (_gate)
            {
                return _retired.Count;
            }
        }
    }

    private void PreWarm()
    {
        var created = new List<PooledSender>(_min);
        try
        {
            for (var i = 0; i < _min; i++)
            {
                created.Add(CreateSender(AllocateSlotIndex()));
            }
        }
        catch
        {
            foreach (var ps in created)
            {
                try
                {
                    ps.DisposeInner();
                }
                catch
                {
                    // best-effort teardown of the partially-warmed pool
                }

                FreeSlotIndex(ps.SlotIndex);
            }

            throw;
        }

        lock (_gate)
        {
            foreach (var ps in created)
            {
                _all.Add(ps);
                _idle.Add(ps);
                if (ps.CreatedAtUtc < _idleOldestCreatedUtc)
                {
                    _idleOldestCreatedUtc = ps.CreatedAtUtc;
                }
            }
        }
    }

    /// <summary>Borrows a sender, blocking up to <c>acquire_timeout_ms</c>. The returned
    ///     <see cref="BorrowedSender" /> is a single-use handle: dispose it to return it to the pool
    ///     (dispose does not send — call Send()/Flush() first for delivery).</summary>
    internal BorrowedSender Borrow()
    {
        ThrowIfClosed();
        bool acquired;
        try
        {
            acquired = _capacity.Wait(_acquireTimeoutMs, _closeCts.Token);
        }
        catch (OperationCanceledException)
        {
            throw Closed();
        }
        catch (ObjectDisposedException)
        {
            throw Closed();
        }

        if (!acquired)
        {
            throw Exhausted();
        }

        return BorrowedSender.For(TakeOrCreate(), this);
    }

    /// <inheritdoc cref="Borrow" />
    internal async ValueTask<BorrowedSender> BorrowAsync(CancellationToken ct = default)
    {
        ThrowIfClosed();
        bool acquired;
        CancellationTokenSource? linked = null;
        try
        {
            // Reading _closeCts.Token must be inside the try: a concurrent Close that disposes the CTS
            // makes the getter throw, which we want surfaced as the friendly closed error.
            // Skip the linked-source allocation on the common ct=default path: WaitAsync takes a single
            // token, so pass _closeCts.Token straight through and only link when the caller can also cancel.
            CancellationToken waitToken;
            if (ct.CanBeCanceled)
            {
                linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _closeCts.Token);
                waitToken = linked.Token;
            }
            else
            {
                waitToken = _closeCts.Token;
            }

            acquired = await _capacity.WaitAsync(_acquireTimeoutMs, waitToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_closeCts.IsCancellationRequested)
        {
            throw Closed();
        }
        catch (ObjectDisposedException)
        {
            throw Closed();
        }
        finally
        {
            linked?.Dispose();
        }

        if (!acquired)
        {
            throw Exhausted();
        }

        return BorrowedSender.For(TakeOrCreate(), this);
    }

    /// <summary>
    ///     Drains every sender currently in the pool: flushes each one's buffered rows and blocks until the
    ///     server has acknowledged them, or <paramref name="timeout" /> elapses (per sender). This is the
    ///     pool-wide delivery barrier — the equivalent of calling <c>Flush</c> on each borrowed sender.
    ///     <para />
    ///     <b>Quiescence barrier:</b> intended to be called when no senders are borrowed (all returned). It
    ///     does not synchronise against a concurrent borrow — draining a sender another thread is actively
    ///     writing to is a data race. Returns <c>true</c> only if every sender drained fully within the
    ///     timeout; <c>false</c> if any timed out or latched an error.
    /// </summary>
    internal bool Flush(TimeSpan timeout, CancellationToken ct = default)
    {
        ThrowIfClosed();
        var snapshot = SnapshotAll();
        if (snapshot.Count == 0)
        {
            return true;
        }

        // Drain the senders concurrently on the thread pool — they are independent connections, so this
        // turns the barrier's wall-clock cost into the slowest single drain rather than their sum, mirroring
        // FlushAsync. DrainOneSync swallows a sender's terminal error (reports not-drained) and re-throws
        // OperationCanceledException, so the only fault WaitAll can surface is cancellation.
        var tasks = new Task<bool>[snapshot.Count];
        for (var i = 0; i < snapshot.Count; i++)
        {
            var ps = snapshot[i];
            tasks[i] = Task.Run(() => DrainOneSync(ps, timeout, ct));
        }

        try
        {
            Task.WaitAll(tasks);
        }
        catch (AggregateException ex)
        {
            foreach (var inner in ex.InnerExceptions)
            {
                if (inner is OperationCanceledException oce)
                {
                    throw oce;
                }
            }

            throw;
        }

        var ok = true;
        foreach (var t in tasks)
        {
            ok &= t.Result;
        }

        return ok;
    }

    /// <inheritdoc cref="Flush(TimeSpan, CancellationToken)" />
    internal bool Flush(CancellationToken ct = default) => Flush(_defaultFlushTimeout, ct);

    /// <inheritdoc cref="Flush(TimeSpan, CancellationToken)" />
    internal async ValueTask<bool> FlushAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        ThrowIfClosed();
        var snapshot = SnapshotAll();
        if (snapshot.Count == 0)
        {
            return true;
        }

        // Drain the senders concurrently — they are independent connections, so this is safe and turns the
        // barrier's wall-clock cost into the slowest single drain rather than their sum.
        var tasks = new List<Task<bool>>(snapshot.Count);
        foreach (var ps in snapshot)
        {
            tasks.Add(DrainOneAsync(ps, timeout, ct));
        }

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        var ok = true;
        foreach (var r in results)
        {
            ok &= r;
        }

        return ok;
    }

    /// <inheritdoc cref="Flush(TimeSpan, CancellationToken)" />
    internal ValueTask<bool> FlushAsync(CancellationToken ct = default) => FlushAsync(_defaultFlushTimeout, ct);

    private static async Task<bool> DrainOneAsync(PooledSender ps, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            return await ps.Inner.FlushAsync(timeout, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (ObjectDisposedException)
        {
            // The housekeeper reaped this sender between the snapshot and here. Reaping is drain-gated
            // (IsInnerFullyDrained), so a disposed inner provably owed no data — count it as drained
            // rather than letting the race flip the whole barrier to a spurious not-drained.
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool DrainOneSync(PooledSender ps, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            return ps.Inner.Flush(timeout, ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (ObjectDisposedException)
        {
            // See DrainOneAsync: reaped-during-drain ⇒ was fully drained ⇒ count as drained.
            return true;
        }
        catch
        {
            // One sender's terminal error must not abort draining the rest; report it as not-drained.
            return false;
        }
    }

    private List<PooledSender> SnapshotAll()
    {
        lock (_gate)
        {
            return new List<PooledSender>(_all);
        }
    }

    // Permit already held. Reuse an idle entry or create a fresh one outside the lock.
    private PooledSender TakeOrCreate()
    {
        int slotIndex;
        lock (_gate)
        {
            if (_closed)
            {
                ReleaseCapacity();
                throw Closed();
            }

            if (_idle.Count > 0)
            {
                var ps = _idle[^1];
                _idle.RemoveAt(_idle.Count - 1);
                return ps;
            }

            slotIndex = AllocateSlotIndex();
            if (_storeAndForward && slotIndex < 0)
            {
                // Defence in depth: every retire/reap withholds its permit physically (reap skips when it
                // can't — see TryWithholdPermit), so a held permit always implies an idle sender or a free
                // index. Surface the documented exhaustion error rather than a factory failure.
                ReleaseCapacity();
                throw Exhausted();
            }
        }

        PooledSender created;
        try
        {
            created = CreateSender(slotIndex);
        }
        catch
        {
            lock (_gate)
            {
                FreeSlotIndex(slotIndex);
            }

            ReleaseCapacity();
            throw;
        }

        bool closedRace;
        lock (_gate)
        {
            closedRace = _closed;
            if (closedRace)
            {
                FreeSlotIndex(slotIndex);
            }
            else
            {
                _all.Add(created);
            }
        }

        if (closedRace)
        {
            // Dispose outside _gate: DisposeInner can block up to the engine's ~5s pump-join budget,
            // and holding the pool lock across it stalls every other pool op. The pool is closed, so
            // the freed slot index is never reallocated. Matches DisposeAndSettle.
            try
            {
                created.DisposeInner();
            }
            catch
            {
                // best effort
            }

            ReleaseCapacity();
            throw Closed();
        }

        return created;
    }

    /// <summary>Returns a borrowed sender to the pool (after Dispose discarded its un-sent rows).</summary>
    internal void GiveBack(PooledSender ps)
    {
        if (GiveBackOrTakeOwnership(ps))
        {
            try
            {
                ps.DisposeInner();
            }
            catch
            {
                // best effort
            }
        }

        ReleaseCapacity();
    }

    /// <inheritdoc cref="GiveBack" />
    internal async ValueTask GiveBackAsync(PooledSender ps)
    {
        if (GiveBackOrTakeOwnership(ps))
        {
            try
            {
                await ps.DisposeInnerAsync().ConfigureAwait(false);
            }
            catch
            {
                // best effort
            }
        }

        ReleaseCapacity();
    }

    // Re-pool an idle-again sender, or — if the pool closed while it was on loan — hand teardown back to
    // the caller. Close() only disposes idle senders, never an in-use one (its borrower may be mid-clear in
    // Dispose on a non-thread-safe inner), so a sender returning post-close is disposed here by its
    // borrower, on the borrower's own stack after Dispose finished. Exactly one party ever disposes a given
    // inner. Returns true when the caller must dispose the inner.
    private bool GiveBackOrTakeOwnership(PooledSender ps)
    {
        lock (_gate)
        {
            if (_closed)
            {
                return true;
            }

            ps.IdleSinceUtc = DateTime.UtcNow;
            _idle.Add(ps);
            if (ps.CreatedAtUtc < _idleOldestCreatedUtc)
            {
                _idleOldestCreatedUtc = ps.CreatedAtUtc;
            }

            return false;
        }
    }

    /// <summary>Evicts a sender that can't be re-pooled (terminally failed, or holding un-rollback-able
    ///     transactional state): dispose it for real, then reclaim or retire its slot.</summary>
    internal void DiscardBroken(PooledSender ps)
    {
        lock (_gate)
        {
            _all.Remove(ps);
        }

        try
        {
            ps.DisposeInner();
        }
        catch
        {
            // best effort
        }

        FinishDiscard(ps);
    }

    /// <inheritdoc cref="DiscardBroken" />
    internal async ValueTask DiscardBrokenAsync(PooledSender ps)
    {
        lock (_gate)
        {
            _all.Remove(ps);
        }

        try
        {
            await ps.DisposeInnerAsync().ConfigureAwait(false);
        }
        catch
        {
            // best effort
        }

        FinishDiscard(ps);
    }

    // A discarded sender held a capacity permit. If its slot lock dropped, free the index and release
    // the permit. Otherwise retire the index and keep the permit withheld — simply by not releasing it —
    // so effective max shrinks by one. Mirrors the Java pool's leaked-slot accounting. The shrink is
    // reversible: the housekeeper re-tests retired slots and reclaims the index + permit once the
    // (possibly deferred) lock release lands.
    private void FinishDiscard(PooledSender ps)
    {
        if (SettleAfterDispose(ps))
        {
            ReleaseCapacity();
        }
    }

    /// <summary>Reaps idle / over-age senders down to <c>min</c>. Driven by the housekeeper. The idle
    ///     deque is sorted by idle time, so the idle sweep only inspects the cold end and stops at the
    ///     first entry inside its timeout — O(reaped), not O(idle). max_lifetime is age-ordered, not
    ///     idle-ordered, so it runs as a separate walk gated on <c>_idleOldestCreatedUtc</c>, which
    ///     fires only when some parked entry has actually crossed the lifetime.</summary>
    internal void ReapIdle()
    {
        var now = DateTime.UtcNow;
        ReapOverAge(now);

        // Bound the sweep to the entries present at its start: an un-drained cold entry is re-parked at
        // the hot end below, and without the bound a deque full of un-drained entries would be revisited
        // forever. Entries returned mid-sweep are fresh and land at the hot end — next sweep's problem.
        int budget;
        lock (_gate)
        {
            budget = _closed ? 0 : _idle.Count;
        }

        while (budget-- > 0)
        {
            PooledSender victim;
            lock (_gate)
            {
                if (_closed || _idle.Count == 0)
                {
                    return;
                }

                var ps = _idle[0];
                // The cold end is the longest-idle entry: if it is inside its timeout, everything
                // behind it is too. The min floor is global, so it ends the sweep as well.
                if (now - ps.IdleSinceUtc < _idleTimeout || _all.Count <= _min)
                {
                    return;
                }

                // Don't reap until every in-flight frame is acked. Reaping a WS sender whose ring still
                // holds un-acked data would, in RAM mode, free that data with no delivery and no error
                // (the pool-wide Flush can't cover an already-reaped sender). Re-park it at the hot end
                // with a fresh IdleSinceUtc so the idle clock effectively restarts (and the reap timer
                // starts at full-drain); a wedged sender that never drains simply lives until the pool
                // is closed (its data is retained, not silently dropped). No-op for HTTP/TCP, which
                // deliver synchronously.
                if (!ps.IsInnerFullyDrained)
                {
                    _idle.RemoveAt(0);
                    ps.IdleSinceUtc = now;
                    _idle.Add(ps);
                    continue;
                }

                // Withhold one capacity permit for the whole dispose window — atomically, BEFORE the
                // sender leaves the pool. An idle sender holds no permit (its unit of capacity sits
                // free in the semaphore), so taking it here guarantees the semaphore never advertises
                // capacity whose slot index is still locked mid-dispose — the spurious-PoolExhausted
                // race. If a mid-borrow thread already drained the last free permit, stop the sweep and
                // leave the deque untouched: that borrower is about to reuse this very sender, and with
                // zero free permits every other idle entry is equally spoken for. IdleSinceUtc is not
                // refreshed, so a skipped sender stays reap-eligible next sweep.
                if (!TryWithholdPermit())
                {
                    return;
                }

                _idle.RemoveAt(0);
                _all.Remove(ps);
                victim = ps;
            }

            DisposeAndSettle(victim);
        }
    }

    // max_lifetime pass. The deque is IdleSinceUtc-ordered, so an entry that crosses max_lifetime while
    // parked can hide behind younger-created but colder entries where the cold-end sweep never sees it.
    // Gated on the conservative _idleOldestCreatedUtc bound and retightens it while walking, so the
    // walk only runs when some parked entry has actually crossed — or while an over-age entry survives
    // it (un-drained, min floor, withhold loss) and must be re-checked next sweep.
    private void ReapOverAge(DateTime now)
    {
        List<PooledSender>? victims = null;
        lock (_gate)
        {
            if (_closed || now - _idleOldestCreatedUtc < _maxLifetime)
            {
                return;
            }

            var oldestKept = DateTime.MaxValue;
            var reapable = true;
            for (var i = 0; i < _idle.Count;)
            {
                var ps = _idle[i];
                if (reapable && now - ps.CreatedAtUtc >= _maxLifetime && ps.IsInnerFullyDrained)
                {
                    if (_all.Count <= _min || !TryWithholdPermit())
                    {
                        // Floor reached / a mid-borrow thread owns the free permits: stop reaping but
                        // keep walking to retighten the bound (survivors keep it low, so we re-check).
                        reapable = false;
                    }
                    else
                    {
                        _idle.RemoveAt(i);
                        _all.Remove(ps);
                        (victims ??= new List<PooledSender>()).Add(ps);
                        continue;
                    }
                }

                if (ps.CreatedAtUtc < oldestKept)
                {
                    oldestKept = ps.CreatedAtUtc;
                }

                i++;
            }

            _idleOldestCreatedUtc = oldestKept;
        }

        if (victims is null)
        {
            return;
        }

        foreach (var ps in victims)
        {
            DisposeAndSettle(ps);
        }
    }

    // Disposes a reaped sender outside the pool lock, then frees the slot and returns the withheld
    // permit — but only if the lock actually released; a new sender must never open a slot directory
    // whose flock is still held. If a deferred teardown is still holding it, SettleAfterDispose parks
    // the entry on _retired (permit still withheld) for ReclaimRetiredSlots to settle when the release
    // finally lands.
    private void DisposeAndSettle(PooledSender ps)
    {
        try
        {
            ps.DisposeInner();
        }
        catch
        {
            // best effort; reaping must never throw
        }

        if (SettleAfterDispose(ps))
        {
            ReleaseCapacity();
        }
    }

    /// <summary>
    ///     Re-tests slots retired by a deferred / wedged teardown and reclaims any whose slot lock has
    ///     since released — freeing the index and returning the withheld capacity permit. Driven by the
    ///     housekeeper. Turns the slot retirement of a transient teardown stall into a temporary capacity
    ///     dip rather than a permanent shrink toward <c>PoolExhausted</c>.
    /// </summary>
    internal void ReclaimRetiredSlots()
    {
        if (!_storeAndForward)
        {
            return;
        }

        var releaseCount = 0;
        lock (_gate)
        {
            if (_closed || _retired.Count == 0)
            {
                return;
            }

            for (var i = _retired.Count - 1; i >= 0; i--)
            {
                var ps = _retired[i];
                if (ps.IsInnerSlotLockReleased)
                {
                    _retired.RemoveAt(i);
                    FreeSlotIndex(ps.SlotIndex);
                    releaseCount++;
                }
            }
        }

        ReleaseReclaimedPermits(releaseCount);
    }

    /// <summary>Shuts the pool down, closing every idle underlying sender. Idempotent. Senders currently
    ///     borrowed are torn down by their borrower on return (never disposed here, to avoid racing a
    ///     non-thread-safe in-use sender) — return all borrowed senders before closing the handle.</summary>
    internal void Close()
    {
        var snapshot = BeginClose();
        if (snapshot is null)
        {
            return;
        }

        // Dispose concurrently: a wedged WS sender's DisposeInner can block up to the cursor engine's
        // ~5s pump-join budget, so a sequential loop would stall the caller for up to N×5s. Task.Run
        // bounds close latency to the slowest single dispose, mirroring Flush. DisposeInnerQuietly
        // swallows, so the tasks never fault and WaitAll never throws.
        var tasks = new Task[snapshot.Count];
        for (var i = 0; i < snapshot.Count; i++)
        {
            var ps = snapshot[i];
            tasks[i] = Task.Run(() => DisposeInnerQuietly(ps));
        }

        Task.WaitAll(tasks);

        DisposePrimitives();
    }

    /// <inheritdoc cref="Close" />
    internal async ValueTask CloseAsync()
    {
        var snapshot = BeginClose();
        if (snapshot is null)
        {
            return;
        }

        // Dispose concurrently — see Close for why sequential teardown can stall on a wedged WS sender.
        var tasks = new List<Task>(snapshot.Count);
        foreach (var ps in snapshot)
        {
            tasks.Add(DisposeInnerQuietlyAsync(ps));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        DisposePrimitives();
    }

    private static void DisposeInnerQuietly(PooledSender ps)
    {
        try
        {
            ps.DisposeInner();
        }
        catch
        {
            // best effort
        }
    }

    private static async Task DisposeInnerQuietlyAsync(PooledSender ps)
    {
        try
        {
            await ps.DisposeInnerAsync().ConfigureAwait(false);
        }
        catch
        {
            // best effort
        }
    }

    // ---- internals ----

    private List<PooledSender>? BeginClose()
    {
        List<PooledSender> idle;
        lock (_gate)
        {
            if (_closed)
            {
                return null;
            }

            _closed = true;

            // Tear down only the idle senders here. A borrowed (in-use) sender is non-thread-safe and may
            // be mid-clear inside its borrower's Dispose(); disposing it concurrently would corrupt its
            // buffer / transport. Its owning borrower disposes it instead when it returns post-close (see
            // GiveBackOrTakeOwnership / DiscardBroken), so each inner is torn down by exactly one party.
            // Callers wanting a hard "all I/O has stopped" guarantee must return every borrowed sender
            // before closing the handle.
            idle = new List<PooledSender>(_idle);
            _all.Clear();
            _idle.Clear();
            // Drop references to retired-but-unreclaimed senders; their OS locks release independently
            // (deferred continuation / process exit) and the pool no longer needs to track them.
            _retired.Clear();
        }

        try
        {
            _closeCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // already torn down
        }

        return idle;
    }

    private void DisposePrimitives()
    {
        _closeCts.Dispose();
        _capacity.Dispose();
    }

    private PooledSender CreateSender(int slotIndex)
    {
        var inner = _senderFactory(slotIndex);
        return new PooledSender(inner, slotIndex);
    }

    private ISender CreateDefaultInner(int slotIndex)
    {
        if (_confStr is null)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "SenderPool has no connect string and no sender factory");
        }

        // Each sender gets independent options parsed from the original connect string.
        var options = new SenderOptions(_confStr);
        // Pooled senders don't drain on Send() — the pooled connection survives the borrow's return and
        // ships asynchronously; delivery is confirmed via the pool-wide IQuestDBClient.Flush(). (Standalone
        // senders keep SendAwaitsAck=true so "Send() before Dispose" delivers.)
        options.SendAwaitsAck = false;
        // lazy_connect: the ingest side must not block the pre-warm on a down server. The builder has
        // already rejected an explicit non-async initial_connect_retry, so forcing async here is either a
        // no-op (already async) or the intended injection (unset / reconnect-promoted). ws only — the flag
        // is off for http/tcp handles.
        if (_forceWsAsyncConnect && options.IsWebSocket())
        {
            options.initial_connect_mode = InitialConnectMode.async;
        }
        if (_storeAndForward)
        {
            if (slotIndex < 0)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    "no free store-and-forward slot index (all slots leaked?)");
            }

            ApplySlotIdentity(options, _slotBaseId, slotIndex, _max);
        }

        return Sender.New(options);
    }

    /// <summary>
    ///     Stamps a per-slot identity onto <paramref name="options" /> so pooled WS+SF senders never
    ///     collide: a unique <c>sender_id</c> and the managed-slot family for the orphan scanner.
    /// </summary>
    internal static void ApplySlotIdentity(SenderOptions options, string baseId, int slotIndex, int managedCount)
    {
        options.sender_id = $"{baseId}-{slotIndex}";
        options.OrphanExcludeManagedBase = baseId;
        options.OrphanExcludeManagedCount = managedCount;
    }

    // Next free index in [0, max), LIFO: the most recently freed index is reused first, keeping the
    // on-disk working set of slot directories compact. Caller holds _gate (or is the single-threaded
    // pre-warm path).
    private int AllocateSlotIndex()
    {
        if (!_storeAndForward)
        {
            return -1;
        }

        return _freeSlots.Count > 0 ? _freeSlots.Pop() : -1;
    }

    private void FreeSlotIndex(int idx)
    {
        if (!_storeAndForward || idx < 0)
        {
            return;
        }

        _freeSlots.Push(idx);
    }

    // Non-blocking permit withhold used by the reap path: a reaped idle sender holds no permit, so the
    // reaper takes one out of the free pool before removing the sender, keeping the semaphore in
    // lock-step with allocatable slots for the whole dispose window. Returns false when no free permit
    // exists (a mid-borrow thread drained it) — the caller must then skip the reap. Caller holds _gate;
    // Wait(0) never blocks so holding the lock is safe.
    private bool TryWithholdPermit()
    {
        try
        {
            return _capacity.Wait(0);
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    // Post-dispose slot settlement shared by the discard and reap paths — both arrive holding exactly
    // one withheld capacity permit for ps. Returns true (caller releases the permit) when the slot lock
    // actually dropped and the index was freed; returns false after parking the entry on _retired with
    // the permit still withheld, for ReclaimRetiredSlots to settle once the deferred teardown finally
    // releases the lock. A new sender must never open a slot directory whose flock is still held.
    private bool SettleAfterDispose(PooledSender ps)
    {
        lock (_gate)
        {
            if (!_closed && ps.SlotIndex >= 0 && !ps.IsInnerSlotLockReleased)
            {
                _retired.Add(ps);
                // The index stays off _freeSlots: the directory / flock may still be held.
                return false;
            }

            FreeSlotIndex(ps.SlotIndex);
            return true;
        }
    }

    // Releases reclaimed capacity permits back to borrowers, outside the pool lock. Tolerates a concurrent
    // close (disposed / full semaphore) — the accounting is best-effort once the pool is tearing down.
    private void ReleaseReclaimedPermits(int count)
    {
        for (var i = 0; i < count; i++)
        {
            try
            {
                _capacity.Release();
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (SemaphoreFullException)
            {
                break;
            }
        }
    }

    private void ReleaseCapacity()
    {
        try
        {
            _capacity.Release();
        }
        catch (ObjectDisposedException)
        {
            // pool closed concurrently
        }
        catch (SemaphoreFullException)
        {
            // defensive: never expected, the borrow/return accounting is 1:1
        }
    }

    private void ThrowIfClosed()
    {
        if (IsClosed)
        {
            throw Closed();
        }
    }

    private static IngressError Closed() =>
        new(ErrorCode.InvalidApiCall, "QuestDBClient handle is closed");

    private IngressError Exhausted() =>
        new(ErrorCode.PoolExhausted,
            $"timed out waiting for a sender from the pool after {_acquireTimeoutMs}ms " +
            $"(sender_pool_max={_max})");
}
