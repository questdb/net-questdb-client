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
///     / connect) happens OUTSIDE the lock so a slow connect cannot block other borrowers.
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

    private readonly Stack<PooledSender> _available = new();
    private readonly List<PooledSender> _all = new();
    private bool _closed;

    // Store-and-forward slot management. Each pooled WS+SF sender owns a distinct slot identity
    // (`<base>-<index>`) so siblings never collide on a slot directory / flock. `_slotInUse[i]` tracks
    // index ownership; a retired index (lock not yet released after dispose) stays set and counts
    // against effective capacity, matching the Java pool's `leakedSlots`. Unlike a permanent leak, the
    // housekeeper re-tests retired slots (`ReclaimRetiredSlots`) and frees the index + permit once the
    // engine's deferred teardown finally releases the lock, so `_leakedSlots` is a live gauge of
    // currently-retired slots, not a monotonic counter.
    private readonly bool _storeAndForward;
    private readonly string _slotBaseId;
    private readonly bool[] _slotInUse;
    private int _leakedSlots;

    // Senders retired with their slot lock still held, awaiting reclaim. The housekeeper re-tests each
    // entry's IsInnerSlotLockReleased and, once true, frees the index and returns the withheld permit.
    private readonly List<PooledSender> _retired = new();

    // Permits owed to retired (leaked) slots. Retiring a slot removes one unit of capacity so the
    // semaphore never advertises more in-use slots than the bitmap can allocate. A discarded sender held
    // a permit (paid down when its Dispose/return path releases); a reaped sender held none, so the debt is
    // settled by draining an available permit now or at the next release. Reclaiming a slot reverses one
    // removal: it cancels a still-pending debt, or releases the permit back when the debt was already
    // settled physically.
    private int _leakDebt;

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
        _slotInUse = _storeAndForward ? new bool[_max] : Array.Empty<bool>();

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
                return _available.Count;
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
                return _leakedSlots;
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
                _available.Push(ps);
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

        return new BorrowedSender(TakeOrCreate(), this);
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

        return new BorrowedSender(TakeOrCreate(), this);
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
        var ok = true;
        foreach (var ps in SnapshotAll())
        {
            try
            {
                ok &= ps.Inner.Flush(timeout, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                // One sender's terminal error must not abort draining the rest; report it as not-drained.
                ok = false;
            }
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
        catch
        {
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

            if (_available.Count > 0)
            {
                return _available.Pop();
            }

            slotIndex = AllocateSlotIndex();
            if (_storeAndForward && slotIndex < 0)
            {
                // Defence in depth: with correct leak accounting a held permit always implies a free
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

        lock (_gate)
        {
            if (_closed)
            {
                FreeSlotIndex(slotIndex);
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

            _all.Add(created);
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
            _available.Push(ps);
            return false;
        }
    }

    /// <summary>Evicts a sender that can't be re-pooled (its buffer couldn't be cleared — terminally
    ///     failed): dispose it for real, then reclaim or retire its slot.</summary>
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

    // A discarded sender held a capacity permit. If its slot lock dropped, free the index. Otherwise
    // retire the index — RetireSlotIndex records the leak debt, and the ReleaseCapacity below pays it
    // down (the permit stays out of circulation) so effective max shrinks by one. Mirrors the Java
    // pool's leaked-slot accounting. The shrink is reversible: the housekeeper re-tests retired slots
    // and reclaims the index + permit once the (possibly deferred) lock release lands.
    private void FinishDiscard(PooledSender ps)
    {
        lock (_gate)
        {
            if (ps.SlotIndex < 0 || ps.IsInnerSlotLockReleased)
            {
                FreeSlotIndex(ps.SlotIndex);
            }
            else
            {
                RetireSlotIndex(ps);
            }
        }

        ReleaseCapacity();
    }

    /// <summary>Reaps idle / over-age senders down to <c>min</c>. Driven by the housekeeper.</summary>
    internal void ReapIdle()
    {
        if (IsClosed)
        {
            return;
        }

        var now = DateTime.UtcNow;
        List<PooledSender>? toDispose = null;

        lock (_gate)
        {
            if (_closed)
            {
                return;
            }

            var idle = _available.ToArray();
            _available.Clear();
            foreach (var ps in idle)
            {
                var overIdle = now - ps.IdleSinceUtc >= _idleTimeout;
                var overAge = now - ps.CreatedAtUtc >= _maxLifetime;
                if ((overIdle || overAge) && _all.Count > _min)
                {
                    _all.Remove(ps);
                    (toDispose ??= new List<PooledSender>()).Add(ps);
                }
                else
                {
                    _available.Push(ps);
                }
            }
        }

        if (toDispose is null)
        {
            return;
        }

        foreach (var ps in toDispose)
        {
            try
            {
                ps.DisposeInner();
            }
            catch
            {
                // best effort; reaping must never throw
            }

            // Free the slot index only after dispose, and only if the lock actually released — a new
            // sender must never open a slot directory whose flock is still held. Reaped senders were
            // idle and held no capacity permit, so nothing to release here.
            var reclaimable = ps.SlotIndex < 0 || ps.IsInnerSlotLockReleased;
            lock (_gate)
            {
                if (reclaimable)
                {
                    FreeSlotIndex(ps.SlotIndex);
                }
                else
                {
                    RetireSlotIndex(ps);
                }
            }
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
                if (!ps.IsInnerSlotLockReleased)
                {
                    continue;
                }

                _retired.RemoveAt(i);
                FreeSlotIndex(ps.SlotIndex);
                _leakedSlots--;

                // Reverse the one capacity unit this slot removed. A still-pending debt means the permit
                // was never physically taken, so just cancel the obligation; otherwise the permit was
                // drained and must be released back to borrowers (below, outside the lock).
                if (_leakDebt > 0)
                {
                    _leakDebt--;
                }
                else
                {
                    releaseCount++;
                }
            }
        }

        for (var i = 0; i < releaseCount; i++)
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

        foreach (var ps in snapshot)
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

        foreach (var ps in snapshot)
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

        DisposePrimitives();
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
            idle = new List<PooledSender>(_available);
            _all.Clear();
            _available.Clear();
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

    // Lowest-free index in [0, max). Caller holds _gate (or is the single-threaded pre-warm path).
    private int AllocateSlotIndex()
    {
        if (!_storeAndForward)
        {
            return -1;
        }

        for (var i = 0; i < _slotInUse.Length; i++)
        {
            if (!_slotInUse[i])
            {
                _slotInUse[i] = true;
                return i;
            }
        }

        return -1;
    }

    private void FreeSlotIndex(int idx)
    {
        if (!_storeAndForward || idx < 0)
        {
            return;
        }

        _slotInUse[idx] = false;
    }

    // The sender at this index disposed without releasing its slot lock: keep the bitmap bit set so the
    // index is not reused, remove one unit of capacity so the semaphore stays in lock-step with the
    // allocatable-index count, and remember the sender so the housekeeper can reclaim the index + permit
    // once its (possibly deferred) lock release lands. Caller holds _gate.
    private void RetireSlotIndex(PooledSender ps)
    {
        if (!_storeAndForward || ps.SlotIndex < 0)
        {
            return;
        }

        _leakedSlots++;
        _leakDebt++;
        SettleLeakDebtLocked();
        _retired.Add(ps);
        // _slotInUse[idx] stays true: the directory / flock may still be held.
    }

    // Drain available permits to pay outstanding leak debt. Non-blocking; whatever can't be paid now
    // (permits currently in use) is paid by ReleaseCapacity when those permits come back. Caller holds
    // _gate; Wait(0) never blocks so holding the lock is safe.
    private void SettleLeakDebtLocked()
    {
        while (_leakDebt > 0)
        {
            bool drained;
            try
            {
                drained = _capacity.Wait(0);
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            if (!drained)
            {
                break;
            }

            _leakDebt--;
        }
    }

    private void ReleaseCapacity()
    {
        // A returning permit first pays any outstanding leak debt — keeping it out of circulation so a
        // retired slot's lost capacity stays lost — before being released back to borrowers.
        lock (_gate)
        {
            if (_leakDebt > 0)
            {
                _leakDebt--;
                return;
            }
        }

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
