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

namespace QuestDB.Qwp.Query;

/// <summary>
///     Thin <see cref="IQwpQueryReader" /> over <see cref="QwpQueryWebSocketClient" />'s pull
///     driver. The client's single-flight lock is held for this reader's whole lifetime;
///     <see cref="DisposeAsync" /> is what ends the query and releases it.
/// </summary>
internal sealed class QwpQueryReader : IQwpQueryReader
{
    private readonly QwpQueryWebSocketClient _client;
    // This reader's own request id, captured at creation. Cancel is scoped to it so a stale
    // handle can never cancel a successor query on the same (possibly re-pooled) client.
    private readonly long _requestId;
    private int _disposed;
    private bool _hasCurrent;
    private bool _done;

    // Final result stats, frozen at dispose: the client fields they mirror are reset and
    // rewritten by the next query on the same instance (pooled: another borrower's), so a
    // kept-around reader must keep answering for ITS query, never the successor's.
    private long _snapTotalRows;
    private long _snapRowsAffected;
    private QwpOpType _snapOpType;
    private bool _snapWasCancelled;
    private QwpServerInfo? _snapServerInfo;

    internal QwpQueryReader(QwpQueryWebSocketClient client)
    {
        _client = client;
        _requestId = client.ActiveRequestId;
    }

    public QwpColumnBatch Current
    {
        get
        {
            if (!_hasCurrent)
            {
                throw new InvalidOperationException(
                    "no current batch: call ReadBatchAsync and check that it returned true");
            }

            return _client.CurrentBatch;
        }
    }

    public bool FailoverReset { get; private set; }

    public long TotalRows =>
        Volatile.Read(ref _disposed) != 0 ? _snapTotalRows : _client.ReaderTotalRows;

    public long RowsAffected =>
        Volatile.Read(ref _disposed) != 0 ? _snapRowsAffected : _client.ReaderRowsAffected;

    public QwpOpType OpType =>
        Volatile.Read(ref _disposed) != 0 ? _snapOpType : _client.ReaderOpType;

    public bool WasCancelled =>
        Volatile.Read(ref _disposed) != 0 ? _snapWasCancelled : _client.ReaderWasCancelled;

    public QwpServerInfo? ServerInfo =>
        Volatile.Read(ref _disposed) != 0 ? _snapServerInfo : _client.ServerInfo;

    public async ValueTask<bool> ReadBatchAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_done) return false;
        _hasCurrent = false;
        var hasBatch = false;
        try
        {
            hasBatch = await _client.PullNextBatchAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            // Also consumed on a throw so a QUERY_ERROR that ends a post-reconnect replay still
            // reports that the previously returned rows were void.
            FailoverReset = _client.ConsumeFailoverReset();
        }
        _hasCurrent = hasBatch;
        _done = !hasBatch;
        return hasBatch;
    }

    public bool ReadBatch(CancellationToken ct = default)
    {
        // Threadpool hop drops any captured SyncContext so sync-over-async can't deadlock.
        return Task.Run(() => ReadBatchAsync(ct).AsTask()).GetAwaiter().GetResult();
    }

    public void Cancel()
    {
        if (Volatile.Read(ref _disposed) != 0) return;
        _client.CancelQuery(_requestId);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _hasCurrent = false;
        _snapTotalRows = _client.ReaderTotalRows;
        _snapRowsAffected = _client.ReaderRowsAffected;
        _snapOpType = _client.ReaderOpType;
        _snapWasCancelled = _client.ReaderWasCancelled;
        _snapServerInfo = _client.ServerInfo;
        await _client.EndReaderAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        Task.Run(() => DisposeAsync().AsTask()).GetAwaiter().GetResult();
    }
}

#endif
