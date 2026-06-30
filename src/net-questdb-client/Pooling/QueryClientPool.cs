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

#if NET7_0_OR_GREATER
using QuestDB.Enums;
using QuestDB.Senders;
using QuestDB.Utils;

namespace QuestDB.Pooling;

/// <summary>
///     Elastic pool of <see cref="IQwpQueryClient" /> instances, each wrapped in a reusable
///     <see cref="PooledQueryClient" /> decorator. The read-side analog of <see cref="SenderPool" />,
///     deliberately stripped of all store-and-forward slot machinery (queries have no SF), the
///     leaked-slot / leak-debt accounting, and the AsyncLocal pin.
///     <para />
///     Capacity is bounded by a <see cref="SemaphoreSlim" /> counting in-use clients: a permit is
///     taken on borrow and released on return. A client is only created when no idle one exists and a
///     permit is held, so the total alive count ≤ <c>max</c>. The WebSocket upgrade happens OUTSIDE
///     the lock so a slow connect cannot block other borrowers.
/// </summary>
internal sealed class QueryClientPool
{
    private readonly SemaphoreSlim _capacity;
    private readonly CancellationTokenSource _closeCts = new();
    private readonly object _gate = new();
    private readonly TimeSpan _idleTimeout;
    private readonly TimeSpan _maxLifetime;
    private readonly int _max;
    private readonly int _min;
    private readonly int _acquireTimeoutMs;
    private readonly string? _queryConfStr;
    private readonly Func<ValueTask<IQwpQueryClient>>? _clientFactory;

    private readonly Stack<PooledQueryClient> _available = new();
    private readonly List<PooledQueryClient> _all = new();
    private bool _closed;

    /// <summary>Production constructor: pool sizes from <paramref name="poolConfig" />, clients built from
    ///     <paramref name="queryConfStr" /> (a <c>ws</c>/<c>wss</c> connect string).</summary>
    internal QueryClientPool(SenderOptions poolConfig, string queryConfStr)
        : this(poolConfig, queryConfStr, null)
    {
    }

    /// <summary>Test seam: inject a client factory so unit tests need no live server. <paramref name="queryConfStr" />
    ///     may be null when a factory is supplied.</summary>
    internal QueryClientPool(SenderOptions poolConfig, string? queryConfStr, Func<ValueTask<IQwpQueryClient>>? clientFactory)
    {
        // Re-validate: builder methods may have mutated min/max after the connect-string parse.
        poolConfig.ValidateQueryPoolOptions();

        _min = poolConfig.query_pool_min;
        _max = poolConfig.query_pool_max;
        _acquireTimeoutMs = checked((int)poolConfig.acquire_timeout_ms.TotalMilliseconds);
        _idleTimeout = poolConfig.idle_timeout_ms;
        _maxLifetime = poolConfig.max_lifetime_ms;
        _queryConfStr = queryConfStr;
        _clientFactory = clientFactory;
        _capacity = new SemaphoreSlim(_max, _max);

        try
        {
            PreWarm();
        }
        catch
        {
            _closeCts.Dispose();
            _capacity.Dispose();
            throw;
        }
    }

    /// <summary>Number of idle query clients currently parked in the pool.</summary>
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

    /// <summary>Total query clients alive (idle + in-use).</summary>
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

    private void PreWarm()
    {
        var created = new List<PooledQueryClient>(_min);
        try
        {
            for (var i = 0; i < _min; i++)
            {
                // Client creation is async (WS upgrade); prewarm eagerly, off any captured
                // SyncContext, matching QueryClient.New's sync-over-async idiom.
                created.Add(Task.Run(() => CreateClientAsync().AsTask()).GetAwaiter().GetResult());
            }
        }
        catch
        {
            foreach (var pc in created)
            {
                try
                {
                    pc.DisposeInner();
                }
                catch
                {
                    // best-effort teardown of the partially-warmed pool
                }
            }

            throw;
        }

        lock (_gate)
        {
            foreach (var pc in created)
            {
                _all.Add(pc);
                _available.Push(pc);
            }
        }
    }

    /// <summary>Borrows a query client, blocking up to <c>acquire_timeout_ms</c>.</summary>
    internal PooledQueryClient Borrow()
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

        return TakeOrCreate();
    }

    /// <inheritdoc cref="Borrow" />
    internal async ValueTask<PooledQueryClient> BorrowAsync(CancellationToken ct = default)
    {
        ThrowIfClosed();
        bool acquired;
        CancellationTokenSource? linked = null;
        try
        {
            // Skip the linked-source allocation on the common ct=default path: WaitAsync takes a single
            // token, so pass _closeCts.Token straight through and only link when the caller can also cancel.
            // Reading _closeCts.Token must be inside the try: a concurrent Close that disposes the CTS
            // makes the getter throw, which we want surfaced as the friendly closed error.
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

        return await TakeOrCreateAsync().ConfigureAwait(false);
    }

    // Permit already held. Reuse an idle client or create a fresh one outside the lock.
    private async ValueTask<PooledQueryClient> TakeOrCreateAsync()
    {
        lock (_gate)
        {
            if (_closed)
            {
                ReleaseCapacity();
                throw Closed();
            }

            if (_available.Count > 0)
            {
                var reused = _available.Pop();
                reused.MarkBorrowed();
                return reused;
            }
        }

        return await CreateOutsideLockAsync().ConfigureAwait(false);
    }

    private PooledQueryClient TakeOrCreate()
    {
        lock (_gate)
        {
            if (_closed)
            {
                ReleaseCapacity();
                throw Closed();
            }

            if (_available.Count > 0)
            {
                var reused = _available.Pop();
                reused.MarkBorrowed();
                return reused;
            }
        }

        // Off the captured SyncContext so sync-over-async creation can't deadlock.
        return Task.Run(() => CreateOutsideLockAsync().AsTask()).GetAwaiter().GetResult();
    }

    private async ValueTask<PooledQueryClient> CreateOutsideLockAsync()
    {
        PooledQueryClient created;
        try
        {
            created = await CreateClientAsync().ConfigureAwait(false);
        }
        catch
        {
            ReleaseCapacity();
            throw;
        }

        lock (_gate)
        {
            if (_closed)
            {
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

        created.MarkBorrowed();
        return created;
    }

    private async ValueTask<PooledQueryClient> CreateClientAsync()
    {
        var factory = _clientFactory ?? DefaultFactoryAsync;
        var inner = await factory().ConfigureAwait(false);
        return new PooledQueryClient(inner, this);
    }

    private ValueTask<IQwpQueryClient> DefaultFactoryAsync()
    {
        if (_queryConfStr is null)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "QueryClientPool has no query connect string and no client factory");
        }

        return new ValueTask<IQwpQueryClient>(QueryClient.NewAsync(_queryConfStr));
    }

    /// <summary>Returns a borrowed query client to the pool.</summary>
    internal void GiveBack(PooledQueryClient pc)
    {
        if (GiveBackOrTakeOwnership(pc))
        {
            try
            {
                pc.DisposeInner();
            }
            catch
            {
                // best effort
            }
        }

        ReleaseCapacity();
    }

    /// <inheritdoc cref="GiveBack" />
    internal async ValueTask GiveBackAsync(PooledQueryClient pc)
    {
        if (GiveBackOrTakeOwnership(pc))
        {
            try
            {
                await pc.DisposeInnerAsync().ConfigureAwait(false);
            }
            catch
            {
                // best effort
            }
        }

        ReleaseCapacity();
    }

    // Re-pool an idle-again client, or — if the pool closed while it was on loan — hand teardown back to
    // the caller. Close() only disposes idle clients and merely *cancels* in-flight ones; it never
    // disposes an in-use client (that would race the borrower's still-running query on the
    // non-thread-safe client). So a client returning post-close is disposed here by its borrower, on the
    // borrower's own stack after its query already unwound. Returns true when the caller must dispose.
    private bool GiveBackOrTakeOwnership(PooledQueryClient pc)
    {
        lock (_gate)
        {
            if (_closed)
            {
                return true;
            }

            pc.IdleSinceUtc = DateTime.UtcNow;
            _available.Push(pc);
            return false;
        }
    }

    /// <summary>Evicts a broken / terminal client: dispose it for real and release its permit. Unlike the
    ///     sender pool there is no slot to reclaim or retire, so effective <c>max</c> never shrinks.</summary>
    internal void DiscardBroken(PooledQueryClient pc)
    {
        lock (_gate)
        {
            _all.Remove(pc);
        }

        try
        {
            pc.DisposeInner();
        }
        catch
        {
            // best effort
        }

        ReleaseCapacity();
    }

    /// <inheritdoc cref="DiscardBroken" />
    internal async ValueTask DiscardBrokenAsync(PooledQueryClient pc)
    {
        lock (_gate)
        {
            _all.Remove(pc);
        }

        try
        {
            await pc.DisposeInnerAsync().ConfigureAwait(false);
        }
        catch
        {
            // best effort
        }

        ReleaseCapacity();
    }

    /// <summary>Reaps idle / over-age clients down to <c>min</c>. Driven by the housekeeper.</summary>
    internal void ReapIdle()
    {
        if (IsClosed)
        {
            return;
        }

        var now = DateTime.UtcNow;
        List<PooledQueryClient>? toDispose = null;

        lock (_gate)
        {
            if (_closed)
            {
                return;
            }

            var idle = _available.ToArray();
            _available.Clear();
            foreach (var pc in idle)
            {
                var overIdle = now - pc.IdleSinceUtc >= _idleTimeout;
                var overAge = now - pc.CreatedAtUtc >= _maxLifetime;
                if ((overIdle || overAge) && _all.Count > _min)
                {
                    _all.Remove(pc);
                    (toDispose ??= new List<PooledQueryClient>()).Add(pc);
                }
                else
                {
                    _available.Push(pc);
                }
            }
        }

        if (toDispose is null)
        {
            return;
        }

        foreach (var pc in toDispose)
        {
            try
            {
                pc.DisposeInner();
            }
            catch
            {
                // best effort; reaping must never throw
            }
        }
    }

    /// <summary>Shuts the pool down, closing every idle underlying client. Idempotent. Clients currently
    ///     borrowed are only *cancelled* here (so a blocked ExecuteAsync unwinds) and torn down by their
    ///     borrower on return — never disposed here, to avoid racing a non-thread-safe in-flight query.</summary>
    internal void Close()
    {
        var snapshot = BeginClose();
        if (snapshot is null)
        {
            return;
        }

        // Cancel in-flight queries first so blocked ExecuteAsync calls unwind and return their clients
        // (GiveBack/DiscardBroken dispose them). Never dispose an in-use client here.
        foreach (var pc in snapshot.Value.InUse)
        {
            CancelInner(pc);
        }

        foreach (var pc in snapshot.Value.Idle)
        {
            try
            {
                pc.DisposeInner();
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

        foreach (var pc in snapshot.Value.InUse)
        {
            CancelInner(pc);
        }

        foreach (var pc in snapshot.Value.Idle)
        {
            try
            {
                await pc.DisposeInnerAsync().ConfigureAwait(false);
            }
            catch
            {
                // best effort
            }
        }

        DisposePrimitives();
    }

    // ---- internals ----

    // Cooperatively cancel an in-flight query so a blocked ExecuteAsync returns before we dispose the
    // client (Java cancel-then-join). No-op on an idle client.
    private static void CancelInner(PooledQueryClient pc)
    {
        try
        {
            pc.Inner.Cancel();
        }
        catch
        {
            // best effort
        }
    }

    // Idle clients are disposed by Close(); in-use clients are only cancelled there and disposed by their
    // borrower on return. Splitting them keeps Close from disposing a client whose query is still running.
    private readonly struct CloseSet
    {
        internal CloseSet(List<PooledQueryClient> idle, List<PooledQueryClient> inUse)
        {
            Idle = idle;
            InUse = inUse;
        }

        internal List<PooledQueryClient> Idle { get; }
        internal List<PooledQueryClient> InUse { get; }
    }

    private CloseSet? BeginClose()
    {
        List<PooledQueryClient> idle;
        List<PooledQueryClient> inUse;
        lock (_gate)
        {
            if (_closed)
            {
                return null;
            }

            _closed = true;
            idle = new List<PooledQueryClient>(_available);
            var idleSet = new HashSet<PooledQueryClient>(_available, ReferenceEqualityComparer.Instance);
            inUse = new List<PooledQueryClient>();
            foreach (var pc in _all)
            {
                if (!idleSet.Contains(pc))
                {
                    inUse.Add(pc);
                }
            }

            _all.Clear();
            _available.Clear();
        }

        try
        {
            _closeCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // already torn down
        }

        return new CloseSet(idle, inUse);
    }

    private void DisposePrimitives()
    {
        _closeCts.Dispose();
        _capacity.Dispose();
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
            $"timed out waiting for a query client from the pool after {_acquireTimeoutMs}ms " +
            $"(query_pool_max={_max})");
}
#endif
