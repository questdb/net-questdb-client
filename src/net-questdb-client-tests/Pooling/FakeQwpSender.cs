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

namespace net_questdb_client_tests.Pooling;

/// <summary>
///     A <see cref="FakeSender" /> that also implements <see cref="IQwpWebSocketSender" />, standing in
///     for a real <c>ws::</c> sender so pool tests can assert that a borrowed handle exposes (and
///     forwards) the QWP surface exactly when the pooled inner does.
/// </summary>
internal sealed class FakeQwpSender : FakeSender, IQwpWebSocketSender
{
    public int PingCount;

    public FakeQwpSender(int slotIndex) : base(slotIndex)
    {
    }

    public long GetHighestAckedSeqTxn(string tableName) => -1;
    public long GetHighestDurableSeqTxn(string tableName) => -1;
    public void Ping(CancellationToken ct = default) => Interlocked.Increment(ref PingCount);

    public ValueTask PingAsync(CancellationToken ct = default)
    {
        Ping(ct);
        return ValueTask.CompletedTask;
    }

    public long AckedFsn => -1;
    public Task<long> FlushAndGetSequenceAsync(CancellationToken ct = default) => Task.FromResult(-1L);

    public Task<bool> AwaitAckedFsnAsync(long targetFsn, TimeSpan timeout, CancellationToken ct = default) =>
        Task.FromResult(true);

    public IQwpWebSocketSender ColumnBinary(ReadOnlySpan<char> name, ReadOnlySpan<byte> value) => this;
    public IQwpWebSocketSender ColumnIPv4(ReadOnlySpan<char> name, System.Net.IPAddress addr) => this;
    public IQwpWebSocketSender ColumnByte(ReadOnlySpan<char> name, sbyte value) => this;
    public IQwpWebSocketSender ColumnShort(ReadOnlySpan<char> name, short value) => this;
    public IQwpWebSocketSender ColumnFloat(ReadOnlySpan<char> name, float value) => this;
    public IQwpWebSocketSender ColumnDate(ReadOnlySpan<char> name, long millisSinceEpoch) => this;
    public IQwpWebSocketSender ColumnGeohash(ReadOnlySpan<char> name, ulong hash, int precisionBits) => this;
    public IQwpWebSocketSender ColumnLong256(ReadOnlySpan<char> name, System.Numerics.BigInteger value) => this;

    public long DroppedErrorNotifications => 0;
    public long DroppedConnectionNotifications => 0;
    public long TotalErrorNotificationsDelivered => 0;
    public long TotalFramesSent => 0;
    public long TotalAcks => 0;
    public long TotalServerErrors => 0;
    public long TotalReconnectAttempts => 0;
    public long TotalReconnectsSucceeded => 0;
}
