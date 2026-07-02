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
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace QuestDB.Pooling;

/// <summary>
///     Decorator that lends a real <see cref="IQwpQueryClient" /> from a <see cref="QueryClientPool" />.
///     One decorator is allocated per pooled client and reused across borrows.
///     <para />
///     Unlike a borrowed ingest sender (<see cref="BorrowedSender" />) there is no flush on return
///     (queries are request/response), and — because the query client is never handed to the user (the
///     <see cref="Query" /> runner borrows and returns it internally per execute) — there is no
///     use-after-return hazard to guard, so the entry is reused directly rather than behind a per-borrow
///     handle. <see cref="Dispose" /> / <see cref="DisposeAsync" /> route the client back to the pool: a clean
///     borrow re-pools it (<see cref="QueryClientPool.GiveBack" />); a borrow that failed
///     (<see cref="MarkBroken" />) or left the client terminal/disposed is discarded
///     (<see cref="QueryClientPool.DiscardBroken" />), because the egress client's terminal state is
///     sticky with no public reset. A second dispose after a return is a no-op.
/// </summary>
internal sealed class PooledQueryClient : IQwpQueryClient
{
    private readonly IQwpQueryClient _delegate;
    private readonly QueryClientPool _pool;

    // 1 while borrowed, 0 while idle. Interlocked so a double dispose is a safe no-op and only the
    // first returns the client to the pool.
    private int _inUse;

    // Guards the underlying client's teardown so Close / DiscardBroken / ReapIdle racing can never
    // dispose the same delegate twice.
    private int _innerDisposed;

    // Set by the query runner when an Execute throws / cancels, so the dispose path discards instead
    // of re-pooling. Read on the return path.
    private volatile bool _broken;

    internal PooledQueryClient(IQwpQueryClient inner, QueryClientPool pool)
    {
        _delegate = inner;
        _pool = pool;
        CreatedAtUtc = DateTime.UtcNow;
        IdleSinceUtc = CreatedAtUtc;
    }

    /// <summary>Wall-clock creation time; drives max-lifetime reaping.</summary>
    internal DateTime CreatedAtUtc { get; }

    /// <summary>Last time this client was returned to the pool; drives idle reaping. Guarded by the pool lock.</summary>
    internal DateTime IdleSinceUtc { get; set; }

    /// <summary>The wrapped client, exposed to the pool for teardown.</summary>
    internal IQwpQueryClient Inner => _delegate;

    // Belt-and-braces: re-pool only when the inner client is still reusable. A clean Execute leaves it
    // reusable, but a future non-throwing terminal path must not silently re-pool a dead client.
    private bool IsInnerTerminalOrDisposed =>
        _delegate is IPooledQueryClientInner s && s.IsTerminalOrDisposed;

    /// <summary>The in-flight request id (-1 when none), so <see cref="Query.Cancel" /> can scope a cancel to it.</summary>
    internal long CurrentRequestId =>
        _delegate is IPooledQueryClientInner s ? s.CurrentRequestId : -1;

    /// <summary>Cancels only the query with this request id; a no-op once that query is no longer in flight.</summary>
    internal void CancelRequest(long requestId)
    {
        if (_delegate is IPooledQueryClientInner s)
        {
            s.CancelRequest(requestId);
        }
    }

    /// <summary>Marks this wrapper as handed out and clears the broken flag. Called by the pool under its lock.</summary>
    internal void MarkBorrowed()
    {
        _broken = false;
        Volatile.Write(ref _inUse, 1);
    }

    /// <summary>Flags the client as unusable so the return path discards rather than re-pools it.</summary>
    internal void MarkBroken() => _broken = true;

    /// <summary>Disposes the underlying client for real. Called only by the pool (close / discard / reap). Idempotent.</summary>
    internal void DisposeInner()
    {
        if (Interlocked.Exchange(ref _innerDisposed, 1) == 0)
        {
            _delegate.Dispose();
        }
    }

    /// <summary>Asynchronously disposes the underlying client for real. Called only by the pool. Idempotent.</summary>
    internal ValueTask DisposeInnerAsync()
    {
        return Interlocked.Exchange(ref _innerDisposed, 1) == 0
            ? _delegate.DisposeAsync()
            : ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _inUse, 0) == 0)
        {
            return;
        }

        if (_broken || IsInnerTerminalOrDisposed)
        {
            _pool.DiscardBroken(this);
        }
        else
        {
            _pool.GiveBack(this);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _inUse, 0) == 0)
        {
            return;
        }

        if (_broken || IsInnerTerminalOrDisposed)
        {
            await _pool.DiscardBrokenAsync(this).ConfigureAwait(false);
        }
        else
        {
            await _pool.GiveBackAsync(this).ConfigureAwait(false);
        }
    }

    // ---- Pure delegation below. ----

    public QwpServerInfo? ServerInfo => _delegate.ServerInfo;
    public int NegotiatedVersion => _delegate.NegotiatedVersion;
    public string? NegotiatedCompression => _delegate.NegotiatedCompression;
    public bool WasLastCloseTimedOut => _delegate.WasLastCloseTimedOut;

    public void Execute(string sql, QwpColumnBatchHandler handler) => _delegate.Execute(sql, handler);

    public void Execute(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler) =>
        _delegate.Execute(sql, binds, handler);

    public Task ExecuteAsync(string sql, QwpColumnBatchHandler handler, CancellationToken cancellationToken = default) =>
        _delegate.ExecuteAsync(sql, handler, cancellationToken);

    public Task ExecuteAsync(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler,
        CancellationToken cancellationToken = default) =>
        _delegate.ExecuteAsync(sql, binds, handler, cancellationToken);

    public void Cancel() => _delegate.Cancel();
}
#endif
