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
using QuestDB.Pooling;
using QuestDB.Qwp.Query;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

/// <summary>
///     A no-op <see cref="IQwpQueryClient" /> used to unit-test the query pool without a live server.
///     Tracks execution / dispose / cancel counts and can simulate a submit failure, hard
///     cancellation, a sticky terminal state, or a blocking in-flight read (via <see cref="Gate" />).
/// </summary>
internal sealed class FakeQueryClient : IQwpQueryClient, IPooledQueryClientInner
{
    public int ExecuteCount;
    public int DisposeCount;
    public int CancelCount;
    public string? LastSql;

    public bool ThrowOnExecute;
    public bool CancelOnExecute;
    public bool TerminalOrDisposed;

    // When set, an in-flight ReadBatchAsync parks on this until the test completes it.
    public TaskCompletionSource<bool>? Gate;

    // When set, CancelRequest signals CancelRequestEntered (with the rid it was called with) and
    // then parks on this before applying — lets a test hold a cancel dispatch across a
    // return-and-re-borrow to reproduce the stale-cancel race deterministically.
    public TaskCompletionSource<bool>? CancelGate;
    public readonly TaskCompletionSource<long> CancelRequestEntered =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Mimics the real client's per-execution request ids: unique, monotonic, -1 while idle.
    private long _nextRid;
    private long _currentRid = -1;

    public bool Disposed => Volatile.Read(ref DisposeCount) > 0;

    public QwpServerInfo? ServerInfo => null;
    public int NegotiatedVersion => 1;
    public string? NegotiatedCompression => null;
    public bool WasLastCloseTimedOut => false;

    public bool IsTerminalOrDisposed => TerminalOrDisposed || Disposed;

    public void Cancel() => Interlocked.Increment(ref CancelCount);

    public long CurrentRequestId => Interlocked.Read(ref _currentRid);

    public void CancelRequest(long requestId)
    {
        CancelRequestEntered.TrySetResult(requestId);
        CancelGate?.Task.GetAwaiter().GetResult();
        if (requestId >= 0 && requestId == Interlocked.Read(ref _currentRid))
        {
            Interlocked.Increment(ref CancelCount);
        }
    }

    public void Dispose() => Interlocked.Increment(ref DisposeCount);

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    public Task<IQwpQueryReader> ExecuteReaderAsync(string sql, CancellationToken cancellationToken = default) =>
        StartReaderAsync(sql, cancellationToken);

    public Task<IQwpQueryReader> ExecuteReaderAsync(string sql, QwpBindSetter binds,
        CancellationToken cancellationToken = default) =>
        StartReaderAsync(sql, cancellationToken);

    // Mirrors the real client: submit failures throw here; the query stays "in flight" (rid set)
    // until the reader is disposed. The fake stream is always empty (first read returns false).
    private Task<IQwpQueryReader> StartReaderAsync(string sql, CancellationToken ct)
    {
        Interlocked.Increment(ref ExecuteCount);
        LastSql = sql;
        Interlocked.Exchange(ref _currentRid, Interlocked.Increment(ref _nextRid));
        try
        {
            if (CancelOnExecute)
            {
                throw new OperationCanceledException();
            }

            if (ThrowOnExecute)
            {
                throw new IngressError(ErrorCode.SocketError, "fake execute failure");
            }
        }
        catch
        {
            Interlocked.Exchange(ref _currentRid, -1);
            throw;
        }

        return Task.FromResult<IQwpQueryReader>(new FakeQueryReader(this, ct));
    }

    private sealed class FakeQueryReader : IQwpQueryReader
    {
        private readonly FakeQueryClient _owner;
        private readonly CancellationToken _ct;

        internal FakeQueryReader(FakeQueryClient owner, CancellationToken ct)
        {
            _owner = owner;
            _ct = ct;
        }

        public QwpColumnBatch Current => throw new InvalidOperationException("fake reader has no batches");
        public long TotalRows => 0;
        public long RowsAffected => 0;
        public QwpOpType OpType => QwpOpType.None;
        public bool WasCancelled => false;
        public bool FailoverReset => false;
        public QwpServerInfo? ServerInfo => null;

        public async ValueTask<bool> ReadBatchAsync(CancellationToken ct = default)
        {
            if (_owner.Gate is not null)
            {
                await _owner.Gate.Task.WaitAsync(ct).ConfigureAwait(false);
            }

            _ct.ThrowIfCancellationRequested();
            ct.ThrowIfCancellationRequested();
            return false;
        }

        public bool ReadBatch(CancellationToken ct = default) =>
            Task.Run(() => ReadBatchAsync(ct).AsTask()).GetAwaiter().GetResult();

        public void Cancel() => _owner.Cancel();

        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref _owner._currentRid, -1);
            return ValueTask.CompletedTask;
        }

        public void Dispose() => DisposeAsync().GetAwaiter().GetResult();
    }
}

#endif
