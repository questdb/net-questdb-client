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
///     Tracks execute / dispose / cancel counts and can simulate failure, hard cancellation, a sticky
///     terminal state, or a blocking in-flight query (via <see cref="Gate" />).
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

    // When set, an in-flight ExecuteAsync parks on this until the test completes it.
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

    public void Execute(string sql, QwpColumnBatchHandler handler) => RunSync(sql);

    public void Execute(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler) => RunSync(sql);

    public Task ExecuteAsync(string sql, QwpColumnBatchHandler handler, CancellationToken cancellationToken = default) =>
        RunAsync(sql, cancellationToken);

    public Task ExecuteAsync(string sql, QwpBindSetter binds, QwpColumnBatchHandler handler,
        CancellationToken cancellationToken = default) =>
        RunAsync(sql, cancellationToken);

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

    private void RunSync(string sql)
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
        finally
        {
            Interlocked.Exchange(ref _currentRid, -1);
        }
    }

    private async Task RunAsync(string sql, CancellationToken ct)
    {
        Interlocked.Increment(ref ExecuteCount);
        LastSql = sql;
        Interlocked.Exchange(ref _currentRid, Interlocked.Increment(ref _nextRid));
        try
        {
            if (Gate is not null)
            {
                await Gate.Task.WaitAsync(ct).ConfigureAwait(false);
            }

            if (CancelOnExecute)
            {
                throw new OperationCanceledException();
            }

            if (ThrowOnExecute)
            {
                throw new IngressError(ErrorCode.SocketError, "fake execute failure");
            }
        }
        finally
        {
            Interlocked.Exchange(ref _currentRid, -1);
        }
    }
}

/// <summary>Minimal <see cref="QwpColumnBatchHandler" /> for tests; all callbacks are inherited no-ops.</summary>
internal sealed class NoopQueryHandler : QwpColumnBatchHandler
{
}
#endif
