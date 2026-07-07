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

namespace QuestDB.Pooling;

/// <summary>
///     The borrow handle for a <c>ws::</c> / <c>wss::</c> pool. Extends <see cref="BorrowedSender" />
///     with the full QWP surface (QWP-only column types, <c>Ping</c>, seqTxn / FSN watermarks) so a
///     borrowed sender can be probed and cast exactly like a standalone one:
///     <c>sender is IQwpWebSocketSender ws</c>. <see cref="BorrowedSender.For" /> allocates this
///     subtype only when the pooled entry's real sender implements <see cref="IQwpWebSocketSender" />,
///     so a handle from an HTTP/TCP pool never matches the probe. Same lifecycle as the base: every
///     member (QWP ones included) throws <see cref="ObjectDisposedException" /> once the handle is
///     returned to the pool.
/// </summary>
internal sealed class BorrowedQwpSender : BorrowedSender, IQwpWebSocketSender
{
    private readonly IQwpWebSocketSender _qwpInner;

    internal BorrowedQwpSender(PooledSender entry, SenderPool pool)
        : base(entry, pool)
    {
        _qwpInner = (IQwpWebSocketSender)entry.Inner;
    }

    // Same use-after-return gate as the base's ISender members: Active() throws once the handle is
    // returned; _qwpInner is the same object it returns, pre-cast at construction.
    private IQwpWebSocketSender Qwp
    {
        get
        {
            Active();
            return _qwpInner;
        }
    }

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
