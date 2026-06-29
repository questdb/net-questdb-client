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
///     Default <see cref="IQuestDBClient" />: owns a <see cref="SenderPool" /> (and, from Phase 2, a
///     housekeeper that reaps idle / over-age senders).
/// </summary>
internal sealed class QuestDBClientImpl : IQuestDBClient
{
    private readonly PoolHousekeeper _housekeeper;
    private readonly SenderPool _pool;

    internal QuestDBClientImpl(SenderOptions poolConfig, string confStr)
        : this(new SenderPool(poolConfig, confStr), poolConfig)
    {
    }

    // Test seam: inject a sender factory.
    internal QuestDBClientImpl(SenderOptions poolConfig, Func<int, ISender> senderFactory)
        : this(new SenderPool(poolConfig, null, senderFactory), poolConfig)
    {
    }

    private QuestDBClientImpl(SenderPool pool, SenderOptions poolConfig)
    {
        _pool = pool;
        _housekeeper = new PoolHousekeeper(_pool, poolConfig.housekeeper_interval_ms);
    }

    public int AvailableSenderCount => _pool.AvailableSize;
    public int TotalSenderCount => _pool.TotalSize;

    public ISender BorrowSender()
    {
        return _pool.Borrow();
    }

    public async ValueTask<ISender> BorrowSenderAsync(CancellationToken ct = default)
    {
        return await _pool.BorrowAsync(ct).ConfigureAwait(false);
    }

    public ISender Sender()
    {
        return _pool.PinToCurrentContext();
    }

    public void ReleaseSender()
    {
        _pool.ReleaseCurrentContext();
    }

    public void Close()
    {
        // Stop the sweeper before tearing the pool down so it cannot reap mid-close.
        _housekeeper.Dispose();
        _pool.Close();
    }

    public void Dispose()
    {
        Close();
    }

    public async ValueTask DisposeAsync()
    {
        // Async join so we don't block the caller for up to the housekeeper's 2s budget.
        await _housekeeper.StopAsync().ConfigureAwait(false);
        _housekeeper.Dispose();
        await _pool.CloseAsync().ConfigureAwait(false);
    }
}
