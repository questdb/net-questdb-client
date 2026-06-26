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

using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpSenderErrorDispatcherTests
{
    [Test]
    public async Task Overflow_DropsOldest_KeepsNewest()
    {
        var seen = new List<long>();
        var gate = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();

        using var dispatcher = new QwpSenderErrorDispatcher(err =>
        {
            // Block the first delivery so the channel fills up before the loop drains it.
            if (seen.Count == 0)
            {
                gate.Set();
                release.Wait();
            }
            lock (seen) seen.Add(err.FromFsn);
        }, capacity: 3);

        // Prime the dispatcher loop with one item, then block its handler so subsequent Offers
        // pile up in the channel.
        dispatcher.Offer(MakeError(0));
        gate.Wait();

        // With the handler stuck and the reader paused, capacity is 3. Push 6 items; the
        // oldest 3 must be dropped, leaving FromFsn=3,4,5 in the channel.
        for (var i = 1; i <= 6; i++) dispatcher.Offer(MakeError(i));

        release.Set();

        // Wait until everything that could be delivered has been delivered.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            lock (seen)
            {
                if (seen.Count >= 4 && dispatcher.DroppedNotifications >= 3) break;
            }
            await Task.Delay(20);
        }

        lock (seen)
        {
            // First item was item 0 (it primed the handler). After it released, the channel held
            // the newest 3 (4, 5, 6); the oldest items (1, 2, 3) were evicted.
            Assert.That(seen, Is.EqualTo(new long[] { 0, 4, 5, 6 }));
        }
        Assert.That(dispatcher.DroppedNotifications, Is.EqualTo(3),
            "expected exactly 3 drops (items 1, 2, 3 evicted by DropOldest)");
        Assert.That(dispatcher.TotalDelivered, Is.EqualTo(4));
    }

    [Test]
    public void Offer_AfterDispose_ReturnsFalse_NoDrop()
    {
        using var dispatcher = new QwpSenderErrorDispatcher(_ => { }, capacity: 4);
        dispatcher.Dispose();

        Assert.That(dispatcher.Offer(MakeError(0)), Is.False);
        Assert.That(dispatcher.DroppedNotifications, Is.EqualTo(0));
    }

    [Test]
    public void Ctor_RejectsZeroCapacity()
    {
        Assert.Catch<ArgumentOutOfRangeException>(() => new QwpSenderErrorDispatcher(_ => { }, capacity: 0));
    }

    private static SenderError MakeError(long fsn) => new(
        category: SenderErrorCategory.ProtocolViolation,
        appliedPolicy: SenderErrorPolicy.DropAndContinue,
        serverStatusByte: SenderError.NoStatusByte,
        serverMessage: null,
        messageSequence: SenderError.NoMessageSequence,
        fromFsn: fsn,
        toFsn: fsn,
        tableName: null,
        detectedAtUtc: DateTime.UtcNow);
}
