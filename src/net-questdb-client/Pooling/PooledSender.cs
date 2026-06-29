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
///     Decorator that lends a real <see cref="ISender" /> from a <see cref="SenderPool" />. One
///     decorator is allocated per pool slot and reused for every borrow, so steady-state borrow /
///     return is allocation-free.
///     <para />
///     Behaviour difference from a raw sender: <see cref="Dispose" /> (and
///     <see cref="DisposeAsync" />) flush pending rows and return this decorator to the pool — they
///     do NOT close the underlying sender. The real sender is only closed when the owning
///     <see cref="QuestDBClient" /> handle is disposed. A second <see cref="Dispose" /> after a
///     return is a no-op (idempotent).
///     <para />
///     Implements <see cref="IQwpWebSocketSender" /> so a borrowed <c>ws::</c> / <c>wss::</c> sender
///     can be cast to the full QWP surface (QWP-only column types, <c>Ping</c>, seqTxn watermarks);
///     those members throw a clear <see cref="IngressError" /> when the pool's transport is HTTP/TCP.
/// </summary>
internal sealed class PooledSender : IQwpWebSocketSender
{
    private readonly ISender _delegate;
    private readonly SenderPool _pool;

    // 1 while borrowed, 0 while idle in the pool. Toggled with Interlocked so a double Dispose is a
    // safe no-op and only the first one flushes + returns.
    private int _inUse;

    // Guards the underlying sender's teardown so Close / DiscardBroken / ReapIdle racing each other can
    // never dispose the same delegate twice.
    private int _innerDisposed;

    internal PooledSender(ISender inner, SenderPool pool, int slotIndex)
    {
        _delegate = inner;
        _pool = pool;
        SlotIndex = slotIndex;
        CreatedAtUtc = DateTime.UtcNow;
        IdleSinceUtc = CreatedAtUtc;
    }

    /// <summary>SF slot index this sender owns, or -1 when store-and-forward is disabled.</summary>
    internal int SlotIndex { get; }

    /// <summary>Wall-clock creation time; drives max-lifetime reaping.</summary>
    internal DateTime CreatedAtUtc { get; }

    /// <summary>Last time this sender was returned to the pool; drives idle reaping. Guarded by the pool lock.</summary>
    internal DateTime IdleSinceUtc { get; set; }

    /// <summary>
    ///     Set when the pool closes or evicts this wrapper so a thread-pinned reference can detect it
    ///     is stale. Read without a lock; written under the pool lock.
    /// </summary>
    internal volatile bool Invalidated;

    /// <summary>The wrapped sender, exposed to the pool for teardown.</summary>
    internal ISender Inner => _delegate;

    /// <summary>True while a borrower holds this wrapper.</summary>
    internal bool IsInUse => Volatile.Read(ref _inUse) == 1;

    /// <summary>
    ///     True if the inner sender holds no SF slot lock or has released it — i.e. the slot index is
    ///     safe to reuse. Always true for non-SF senders (HTTP/TCP/WS-RAM). Call only after the inner
    ///     sender has been disposed.
    /// </summary>
    internal bool IsInnerSlotLockReleased => _delegate is not IPooledSlotSender s || s.IsSlotLockReleased;

    /// <summary>Marks this wrapper as handed out. Called by the pool under its lock.</summary>
    internal void MarkBorrowed() => Volatile.Write(ref _inUse, 1);

    /// <summary>Disposes the underlying sender for real. Called only by the pool (close / discard / reap). Idempotent.</summary>
    internal void DisposeInner()
    {
        if (Interlocked.Exchange(ref _innerDisposed, 1) == 0)
        {
            _delegate.Dispose();
        }
    }

    /// <summary>Asynchronously disposes the underlying sender for real. Called only by the pool. Idempotent.</summary>
    internal ValueTask DisposeInnerAsync()
    {
        return Interlocked.Exchange(ref _innerDisposed, 1) == 0
            ? _delegate.DisposeAsync()
            : ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        // Only the borrow-side owner returns the wrapper; a second call (or a call on an idle wrapper)
        // is a no-op. The pool — not Dispose — is what tears the delegate down.
        if (Interlocked.Exchange(ref _inUse, 0) == 0)
        {
            return;
        }

        // Clear any thread pin BEFORE the wrapper becomes borrowable again, so another thread can't
        // grab it while this thread still holds a stale pin to it.
        _pool.ClearPinIfCurrent(this);

        try
        {
            _delegate.Send();
        }
        catch
        {
            _pool.DiscardBroken(this);
            return;
        }

        _pool.GiveBack(this);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _inUse, 0) == 0)
        {
            return;
        }

        _pool.ClearPinIfCurrent(this);

        try
        {
            await _delegate.SendAsync().ConfigureAwait(false);
        }
        catch
        {
            await _pool.DiscardBrokenAsync(this).ConfigureAwait(false);
            return;
        }

        _pool.GiveBack(this);
    }

    // ---- Pure delegation below. Fluent methods return `this` so chaining stays on the wrapper. ----

    public int Length => _delegate.Length;
    public int RowCount => _delegate.RowCount;
    public bool WithinTransaction => _delegate.WithinTransaction;
    public DateTime LastFlush => _delegate.LastFlush;
    public SenderOptions Options => _delegate.Options;

    public ISender Transaction(ReadOnlySpan<char> tableName)
    {
        _delegate.Transaction(tableName);
        return this;
    }

    public void Rollback() => _delegate.Rollback();
    public Task CommitAsync(CancellationToken ct = default) => _delegate.CommitAsync(ct);
    public void Commit(CancellationToken ct = default) => _delegate.Commit(ct);
    public Task SendAsync(CancellationToken ct = default) => _delegate.SendAsync(ct);
    public void Send(CancellationToken ct = default) => _delegate.Send(ct);

    public ISender Table(ReadOnlySpan<char> name)
    {
        _delegate.Table(name);
        return this;
    }

    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _delegate.Symbol(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, int value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        _delegate.ColumnNanos(name, timestampNanos);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, decimal value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender ColumnDecimal64(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        _delegate.ColumnDecimal64(name, value, scale);
        return this;
    }

    public ISender ColumnDecimal128(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        _delegate.ColumnDecimal128(name, value, scale);
        return this;
    }

    public ISender ColumnDecimal256(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        _delegate.ColumnDecimal256(name, value, scale);
        return this;
    }

    public ISender ColumnDecimal64(ReadOnlySpan<char> name, long unscaledValue, byte scale)
    {
        _delegate.ColumnDecimal64(name, unscaledValue, scale);
        return this;
    }

    public ISender ColumnDecimal128(ReadOnlySpan<char> name, long lo, long hi, byte scale)
    {
        _delegate.ColumnDecimal128(name, lo, hi, scale);
        return this;
    }

    public ISender ColumnDecimal256(ReadOnlySpan<char> name, long l0, long l1, long l2, long l3, byte scale)
    {
        _delegate.ColumnDecimal256(name, l0, l1, l2, l3, scale);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Guid value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, char value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct
    {
        _delegate.Column(name, value, shape);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Array value)
    {
        _delegate.Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        _delegate.Column(name, value);
        return this;
    }

    public ValueTask AtAsync(DateTime value, CancellationToken ct = default) => _delegate.AtAsync(value, ct);
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default) => _delegate.AtAsync(value, ct);
    public ValueTask AtAsync(long value, CancellationToken ct = default) => _delegate.AtAsync(value, ct);

    [Obsolete("Not compatible with deduplication. Please use `AtAsync(DateTime.UtcNow)` instead.")]
    public ValueTask AtNowAsync(CancellationToken ct = default)
    {
#pragma warning disable CS0618
        return _delegate.AtNowAsync(ct);
#pragma warning restore CS0618
    }

    public void At(DateTime value, CancellationToken ct = default) => _delegate.At(value, ct);
    public void At(DateTimeOffset value, CancellationToken ct = default) => _delegate.At(value, ct);
    public void At(long value, CancellationToken ct = default) => _delegate.At(value, ct);
    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default) => _delegate.AtNanosAsync(timestampNanos, ct);
    public void AtNanos(long timestampNanos, CancellationToken ct = default) => _delegate.AtNanos(timestampNanos, ct);

    [Obsolete("Not compatible with deduplication. Please use `At(DateTime.UtcNow)` instead.")]
    public void AtNow(CancellationToken ct = default)
    {
#pragma warning disable CS0618
        _delegate.AtNow(ct);
#pragma warning restore CS0618
    }

    public void Truncate() => _delegate.Truncate();
    public void CancelRow() => _delegate.CancelRow();
    public void Clear() => _delegate.Clear();

    // ---- QWP (ws::/wss::) superset. Forwarded to the inner WS sender; throws cleanly on HTTP/TCP. ----

    private IQwpWebSocketSender Qwp =>
        _delegate as IQwpWebSocketSender
        ?? throw new IngressError(ErrorCode.InvalidApiCall,
            "this pooled sender is not a ws:: / wss:: sender; QWP-only operations are unavailable");

    public long GetHighestAckedSeqTxn(string tableName) => Qwp.GetHighestAckedSeqTxn(tableName);
    public long GetHighestDurableSeqTxn(string tableName) => Qwp.GetHighestDurableSeqTxn(tableName);
    public void Ping(CancellationToken ct = default) => Qwp.Ping(ct);
    public ValueTask PingAsync(CancellationToken ct = default) => Qwp.PingAsync(ct);
    public long AckedFsn => Qwp.AckedFsn;
    public Task<long> FlushAndGetSequenceAsync(CancellationToken ct = default) => Qwp.FlushAndGetSequenceAsync(ct);

    public Task<bool> AwaitAckedFsnAsync(long targetFsn, TimeSpan timeout, CancellationToken ct = default) =>
        Qwp.AwaitAckedFsnAsync(targetFsn, timeout, ct);

    public IQwpWebSocketSender ColumnBinary(ReadOnlySpan<char> name, ReadOnlySpan<byte> value)
    {
        Qwp.ColumnBinary(name, value);
        return this;
    }

    public IQwpWebSocketSender ColumnIPv4(ReadOnlySpan<char> name, System.Net.IPAddress addr)
    {
        Qwp.ColumnIPv4(name, addr);
        return this;
    }

    public IQwpWebSocketSender ColumnByte(ReadOnlySpan<char> name, sbyte value)
    {
        Qwp.ColumnByte(name, value);
        return this;
    }

    public IQwpWebSocketSender ColumnShort(ReadOnlySpan<char> name, short value)
    {
        Qwp.ColumnShort(name, value);
        return this;
    }

    public IQwpWebSocketSender ColumnFloat(ReadOnlySpan<char> name, float value)
    {
        Qwp.ColumnFloat(name, value);
        return this;
    }

    public IQwpWebSocketSender ColumnDate(ReadOnlySpan<char> name, long millisSinceEpoch)
    {
        Qwp.ColumnDate(name, millisSinceEpoch);
        return this;
    }

    public IQwpWebSocketSender ColumnGeohash(ReadOnlySpan<char> name, ulong hash, int precisionBits)
    {
        Qwp.ColumnGeohash(name, hash, precisionBits);
        return this;
    }

    public IQwpWebSocketSender ColumnLong256(ReadOnlySpan<char> name, System.Numerics.BigInteger value)
    {
        Qwp.ColumnLong256(name, value);
        return this;
    }

    public long DroppedErrorNotifications => Qwp.DroppedErrorNotifications;
    public long DroppedConnectionNotifications => Qwp.DroppedConnectionNotifications;
    public long TotalErrorNotificationsDelivered => Qwp.TotalErrorNotificationsDelivered;
    public long TotalFramesSent => Qwp.TotalFramesSent;
    public long TotalAcks => Qwp.TotalAcks;
    public long TotalServerErrors => Qwp.TotalServerErrors;
    public long TotalReconnectAttempts => Qwp.TotalReconnectAttempts;
    public long TotalReconnectsSucceeded => Qwp.TotalReconnectsSucceeded;
}
