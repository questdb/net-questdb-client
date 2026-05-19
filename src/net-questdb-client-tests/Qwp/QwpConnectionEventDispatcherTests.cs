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
using QuestDB.Qwp;
using QuestDB.Senders;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpConnectionEventDispatcherTests
{
    private sealed class CountingListener : ISenderConnectionListener
    {
        private long _count;
        public long Count => Interlocked.Read(ref _count);
        public void OnEvent(SenderConnectionEvent evt) => Interlocked.Increment(ref _count);
    }

    private static SenderConnectionEvent MakeEvent() => new(
        SenderConnectionEventKind.Connected, "localhost", 9000,
        SenderConnectionEvent.NoCounter, SenderConnectionEvent.NoCounter,
        null, DateTimeOffset.UtcNow);

    [Test]
    public void NeverStarted_DisposesCleanly()
    {
        var dispatcher = new QwpConnectionEventDispatcher(new CountingListener(), capacity: 8);
        Assert.DoesNotThrow(() => dispatcher.Dispose());
    }

    [Test]
    public void Offer_RacingFirstOfferAgainstDispose_NoUnobservedTaskAndCleanDisposal()
    {
        // The first Offer lazily starts the dispatch loop; racing it against Dispose used to let
        // Dispose skip the join and dispose the shutdown CTS out from under a starting loop.
        for (var i = 0; i < 200; i++)
        {
            var listener = new CountingListener();
            var dispatcher = new QwpConnectionEventDispatcher(listener, capacity: 8);
            var start = new ManualResetEventSlim();

            var offerThread = new Thread(() =>
            {
                start.Wait();
                dispatcher.Offer(MakeEvent());
            });
            var disposeThread = new Thread(() =>
            {
                start.Wait();
                dispatcher.Dispose();
            });

            offerThread.Start();
            disposeThread.Start();
            start.Set();

            Assert.That(offerThread.Join(TimeSpan.FromSeconds(5)), Is.True, "Offer thread wedged");
            Assert.That(disposeThread.Join(TimeSpan.FromSeconds(5)), Is.True, "Dispose thread wedged");

            // After Dispose returns, no dispatch-loop task may still be running unobserved: if the
            // loop started it delivered the queued event; if it never started, nothing was queued
            // for delivery. Either way the count is stable once both threads have joined.
            var settled = listener.Count;
            Thread.Sleep(20);
            Assert.That(listener.Count, Is.EqualTo(settled),
                "dispatch loop still running after Dispose returned");
            Assert.That(settled, Is.LessThanOrEqualTo(1));
        }
    }

    [Test]
    public void DeliversQueuedEvents_WhenNotRacingDispose()
    {
        var listener = new CountingListener();
        using (var dispatcher = new QwpConnectionEventDispatcher(listener, capacity: 8))
        {
            for (var i = 0; i < 4; i++) dispatcher.Offer(MakeEvent());

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (listener.Count < 4 && DateTime.UtcNow < deadline)
            {
                Thread.Sleep(5);
            }
        }

        Assert.That(listener.Count, Is.EqualTo(4));
    }
}
