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
using QuestDB.Pooling;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests.Pooling;

/// <summary>
///     A no-op <see cref="ISender" /> used to unit-test the pool without a live server. Tracks
///     dispose / flush counts and can be told to throw on flush to exercise the broken-sender path.
///     Implements <see cref="IPooledSlotSender" /> so SF slot reclaim / retire can be exercised.
/// </summary>
internal sealed class FakeSender : ISender, IPooledSlotSender
{
    private static readonly SenderOptions Opts = new("http::addr=localhost:9000;");

    public int SlotIndex { get; }
    public int DisposeCount;
    public int SendCount;
    public bool ThrowOnSend;
    public bool ThrowOnDispose;

    // Pretend this sender holds a slot lock that does (true) or does not (false) release on dispose.
    public bool SlotLockReleased = true;

    public FakeSender(int slotIndex)
    {
        SlotIndex = slotIndex;
    }

    public bool IsSlotLockReleased => SlotLockReleased;

    public bool Disposed => Volatile.Read(ref DisposeCount) > 0;

    public int Length => 0;
    public int RowCount => 0;
    public bool WithinTransaction => false;
    public DateTime LastFlush => DateTime.MinValue;
    public SenderOptions Options => Opts;

    public void Dispose()
    {
        Interlocked.Increment(ref DisposeCount);
        if (ThrowOnDispose)
        {
            throw new IngressError(ErrorCode.ServerFlushError, "fake dispose failure");
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    public void Send(CancellationToken ct = default)
    {
        Interlocked.Increment(ref SendCount);
        if (ThrowOnSend)
        {
            throw new IngressError(ErrorCode.ServerFlushError, "fake flush failure");
        }
    }

    public Task SendAsync(CancellationToken ct = default)
    {
        Send(ct);
        return Task.CompletedTask;
    }

    public ISender Transaction(ReadOnlySpan<char> tableName) => this;
    public void Rollback() { }
    public Task CommitAsync(CancellationToken ct = default) => Task.CompletedTask;
    public void Commit(CancellationToken ct = default) { }
    public ISender Table(ReadOnlySpan<char> name) => this;
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value) => this;
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value) => this;
    public ISender Column(ReadOnlySpan<char> name, long value) => this;
    public ISender Column(ReadOnlySpan<char> name, int value) => this;
    public ISender Column(ReadOnlySpan<char> name, bool value) => this;
    public ISender Column(ReadOnlySpan<char> name, double value) => this;
    public ISender Column(ReadOnlySpan<char> name, DateTime value) => this;
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value) => this;
    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos) => this;
    public ISender Column(ReadOnlySpan<char> name, decimal value) => this;
    public ISender ColumnDecimal64(ReadOnlySpan<char> name, decimal value, byte scale) => this;
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, decimal value, byte scale) => this;
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, decimal value, byte scale) => this;
    public ISender ColumnDecimal64(ReadOnlySpan<char> name, long unscaledValue, byte scale) => this;
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, long lo, long hi, byte scale) => this;
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, long l0, long l1, long l2, long l3, byte scale) => this;
    public ISender Column(ReadOnlySpan<char> name, Guid value) => this;
    public ISender Column(ReadOnlySpan<char> name, char value) => this;
    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct => this;
    public ISender Column(ReadOnlySpan<char> name, Array value) => this;
    public ISender Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct => this;

    public ValueTask AtAsync(DateTime value, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask AtAsync(long value, CancellationToken ct = default) => ValueTask.CompletedTask;

    [Obsolete("Not compatible with deduplication.")]
    public ValueTask AtNowAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

    public void At(DateTime value, CancellationToken ct = default) { }
    public void At(DateTimeOffset value, CancellationToken ct = default) { }
    public void At(long value, CancellationToken ct = default) { }
    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default) => ValueTask.CompletedTask;
    public void AtNanos(long timestampNanos, CancellationToken ct = default) { }

    [Obsolete("Not compatible with deduplication.")]
    public void AtNow(CancellationToken ct = default) { }

    public void Truncate() { }
    public void CancelRow() { }
    public void Clear() { }
}
