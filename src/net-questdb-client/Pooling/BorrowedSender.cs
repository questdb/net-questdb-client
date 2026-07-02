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
using QuestDB.Utils;

namespace QuestDB.Pooling;

/// <summary>
///     The single-borrow handle handed out by <see cref="SenderPool.Borrow" /> /
///     <see cref="SenderPool.BorrowAsync" />. A <b>fresh</b> handle is allocated per borrow and wraps a
///     reusable <see cref="PooledSender" /> pool entry; it is the only object the caller ever touches.
///     <para />
///     <see cref="Dispose" /> (and <see cref="DisposeAsync" />) are pure resource release: they discard any
///     buffered-but-unsent rows and return the underlying entry to the pool — they do NOT send, and do NOT
///     close the real sender (that happens only when the owning <see cref="QuestDBClient" /> handle is
///     closed). Call <see cref="Send" /> / <see cref="SendAsync" /> (and, on <c>ws</c>,
///     <see cref="Flush(TimeSpan, CancellationToken)" /> to await ACK) before disposing if the data must
///     land. Two invariants make a borrowed handle misuse-proof:
///     <list type="bullet">
///         <item><b>No use after return.</b> Once disposed, every ingest member throws
///         <see cref="ObjectDisposedException" />. Because each borrow gets its own handle, a caller
///         that hangs on to a returned handle can never reach (and corrupt) the entry the pool has
///         since lent to a different borrower — the stale handle is inert, not aliased.</item>
///         <item><b>Double dispose is safe.</b> The first <see cref="Dispose" /> returns the entry;
///         any later one is a no-op, so the entry is never returned (or flushed) twice.</item>
///     </list>
///     A single <see cref="ISender" /> is still not thread-safe: a borrowed handle must not be shared
///     across threads. Borrow one per unit of work and dispose it to return.
///     <para />
///     When the pool's transport is <c>ws::</c> / <c>wss::</c>, <see cref="For" /> hands out the
///     <see cref="BorrowedQwpSender" /> subtype instead, so the standard capability probe
///     (<c>sender is IQwpWebSocketSender</c>) answers truthfully per transport: it matches on a WS
///     pool and fails on HTTP/TCP, exactly as it does for a standalone <see cref="Sender.New(string)" />.
/// </summary>
internal class BorrowedSender : ISender
{
    private readonly ISender _inner;
    private readonly PooledSender _entry;
    private readonly SenderPool _pool;

    // 0 while live, 1 once returned. Interlocked.Exchange in Dispose makes the return-once / no-op-after
    // semantics atomic; every ingest member reads it first and throws if it has flipped.
    private int _disposed;

    private protected BorrowedSender(PooledSender entry, SenderPool pool)
    {
        _entry = entry;
        _pool = pool;
        _inner = entry.Inner;
    }

    /// <summary>Allocates the per-borrow handle, picking the QWP-capable subtype when (and only when)
    ///     the entry's real sender exposes the QWP surface — the inner's type is fixed at entry creation,
    ///     so the capability is known at borrow time.</summary>
    internal static BorrowedSender For(PooledSender entry, SenderPool pool)
    {
        return entry.Inner is IQwpWebSocketSender
            ? new BorrowedQwpSender(entry, pool)
            : new BorrowedSender(entry, pool);
    }

    /// <summary>SF slot index the backing entry owns, or -1 when store-and-forward is disabled. Pool-internal
    ///     metadata (stable for the entry's lifetime); exposed for tests, not gated on dispose.</summary>
    internal int SlotIndex => _entry.SlotIndex;

    /// <inheritdoc />
    public void Dispose()
    {
        // First dispose returns the entry; any later call is an idempotent no-op. Dispose is pure resource
        // release: it does NOT send — buffered-but-unsent rows are discarded so the reused entry starts
        // clean — and it never throws. Call Send()/Flush() before disposing for delivery. The pool — not
        // Dispose — tears the real sender down.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (TryDiscardUnsent())
        {
            _pool.GiveBack(_entry);
        }
        else
        {
            _pool.DiscardBroken(_entry);
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return default;
        }

        return TryDiscardUnsent()
            ? _pool.GiveBackAsync(_entry)
            : _pool.DiscardBrokenAsync(_entry);
    }

    // Discard this borrow's un-sent rows so the reused entry starts with a clean buffer. Returns false
    // (→ discard, don't re-pool) when the inner sender is terminally failed and cannot be cleared — so a
    // dead sender never aliases a later borrower — or when it still owes a transactional commit for rows
    // staged server-side. Never throws — Dispose must not throw.
    private bool TryDiscardUnsent()
    {
        try
        {
            _inner.Clear();

            // A transactional (`transaction=on`) ws sender whose auto-flush staged rows server-side under
            // FLAG_DEFER_COMMIT still owes a commit; Clear() only resets local buffers. QWP has no rollback,
            // so re-pooling the live connection would let the next borrower's first commit silently publish
            // this borrow's abandoned rows. Discarding closes the connection, which drops the staged rows.
            return _inner is not IPooledTransactionalSender t || !t.HasUncommittedDeferredRows;
        }
        catch
        {
            return false;
        }
    }

    // Gate + reach the delegate in one step: throws if this handle was already returned, otherwise hands
    // back the inner sender to forward the call to. Centralises the use-after-return check so every member
    // below is a one-liner that cannot forget it.
    private protected ISender Active()
    {
        if (_disposed != 0)
        {
            throw new ObjectDisposedException(nameof(ISender),
                "this pooled sender has been returned to the pool; borrow a fresh one per unit of work");
        }

        return _inner;
    }

    // ---- Pure delegation below. Fluent methods return `this` so chaining stays on the handle. ----

    public int Length => Active().Length;
    public int RowCount => Active().RowCount;
    public bool WithinTransaction => Active().WithinTransaction;
    public DateTime LastFlush => Active().LastFlush;
    public SenderOptions Options => Active().Options;

    public ISender Transaction(ReadOnlySpan<char> tableName)
    {
        Active().Transaction(tableName);
        return this;
    }

    public void Rollback() => Active().Rollback();
    public Task CommitAsync(CancellationToken ct = default) => Active().CommitAsync(ct);
    public void Commit(CancellationToken ct = default) => Active().Commit(ct);
    public Task SendAsync(CancellationToken ct = default) => Active().SendAsync(ct);
    public void Send(CancellationToken ct = default) => Active().Send(ct);
    public bool Flush(TimeSpan timeout, CancellationToken ct = default) => Active().Flush(timeout, ct);
    public Task<bool> FlushAsync(TimeSpan timeout, CancellationToken ct = default) => Active().FlushAsync(timeout, ct);
    public bool Flush(CancellationToken ct = default) => Active().Flush(ct);
    public Task<bool> FlushAsync(CancellationToken ct = default) => Active().FlushAsync(ct);

    public ISender Table(ReadOnlySpan<char> name)
    {
        Active().Table(name);
        return this;
    }

    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Active().Symbol(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, int value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        Active().ColumnNanos(name, timestampNanos);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, decimal value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender ColumnDecimal64(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        Active().ColumnDecimal64(name, value, scale);
        return this;
    }

    public ISender ColumnDecimal128(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        Active().ColumnDecimal128(name, value, scale);
        return this;
    }

    public ISender ColumnDecimal256(ReadOnlySpan<char> name, decimal value, byte scale)
    {
        Active().ColumnDecimal256(name, value, scale);
        return this;
    }

    public ISender ColumnDecimal128(ReadOnlySpan<char> name, long lo, long hi, byte scale)
    {
        Active().ColumnDecimal128(name, lo, hi, scale);
        return this;
    }

    public ISender ColumnDecimal256(ReadOnlySpan<char> name, long l0, long l1, long l2, long l3, byte scale)
    {
        Active().ColumnDecimal256(name, l0, l1, l2, l3, scale);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Guid value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, char value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct
    {
        Active().Column(name, value, shape);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Array value)
    {
        Active().Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        Active().Column(name, value);
        return this;
    }

    public ValueTask AtAsync(DateTime value, CancellationToken ct = default) => Active().AtAsync(value, ct);
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default) => Active().AtAsync(value, ct);
    public ValueTask AtAsync(long value, CancellationToken ct = default) => Active().AtAsync(value, ct);

    [Obsolete("Not compatible with deduplication. Please use `AtAsync(DateTime.UtcNow)` instead.")]
    public ValueTask AtNowAsync(CancellationToken ct = default)
    {
#pragma warning disable CS0618
        return Active().AtNowAsync(ct);
#pragma warning restore CS0618
    }

    public void At(DateTime value, CancellationToken ct = default) => Active().At(value, ct);
    public void At(DateTimeOffset value, CancellationToken ct = default) => Active().At(value, ct);
    public void At(long value, CancellationToken ct = default) => Active().At(value, ct);
    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default) => Active().AtNanosAsync(timestampNanos, ct);
    public void AtNanos(long timestampNanos, CancellationToken ct = default) => Active().AtNanos(timestampNanos, ct);

    [Obsolete("Not compatible with deduplication. Please use `At(DateTime.UtcNow)` instead.")]
    public void AtNow(CancellationToken ct = default)
    {
#pragma warning disable CS0618
        Active().AtNow(ct);
#pragma warning restore CS0618
    }

    public void Truncate() => Active().Truncate();
    public void CancelRow() => Active().CancelRow();
    public void Clear() => Active().Clear();
}
