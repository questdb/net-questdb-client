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
 ******************************************************************************/

using NUnit.Framework;
using QuestDB.Qwp.Egress;

namespace net_questdb_client_tests.Qwp.Egress;

[TestFixture]
public class QwpSpscQueueTests
{
    [Test]
    public void CapacityRoundsUpToPowerOfTwo()
    {
        Assert.That(new QwpSpscQueue<string>(1).Capacity, Is.EqualTo(1));
        Assert.That(new QwpSpscQueue<string>(2).Capacity, Is.EqualTo(2));
        Assert.That(new QwpSpscQueue<string>(3).Capacity, Is.EqualTo(4));
        Assert.That(new QwpSpscQueue<string>(7).Capacity, Is.EqualTo(8));
        Assert.That(new QwpSpscQueue<string>(17).Capacity, Is.EqualTo(32));
    }

    [Test]
    public void OfferAndPollFifo()
    {
        var q = new QwpSpscQueue<string>(4);
        Assert.That(q.Offer("a"), Is.True);
        Assert.That(q.Offer("b"), Is.True);
        Assert.That(q.Offer("c"), Is.True);

        Assert.That(q.TryPoll(out var v1), Is.True);
        Assert.That(v1, Is.EqualTo("a"));
        Assert.That(q.TryPoll(out var v2), Is.True);
        Assert.That(v2, Is.EqualTo("b"));
        Assert.That(q.TryPoll(out var v3), Is.True);
        Assert.That(v3, Is.EqualTo("c"));
        Assert.That(q.TryPoll(out _), Is.False);
    }

    [Test]
    public void OfferReturnsFalseWhenFull()
    {
        var q = new QwpSpscQueue<string>(2);
        Assert.That(q.Offer("a"), Is.True);
        Assert.That(q.Offer("b"), Is.True);
        Assert.That(q.Offer("c"), Is.False);
        Assert.That(q.TryPoll(out var v), Is.True);
        Assert.That(v, Is.EqualTo("a"));
        Assert.That(q.Offer("c"), Is.True);
    }

    [Test]
    public void TakeReturnsImmediatelyWhenAvailable()
    {
        var q = new QwpSpscQueue<string>(2);
        q.Offer("hello");
        Assert.That(q.Take(), Is.EqualTo("hello"));
    }

    [Test]
    public void TakeBlocksUntilOffer()
    {
        var q = new QwpSpscQueue<string>(2);
        string? received = null;
        var consumer = Task.Run(() => received = q.Take());
        // Give the consumer time to enter the spin/park.
        Thread.Sleep(50);
        Assert.That(consumer.IsCompleted, Is.False);
        Assert.That(q.Offer("late"), Is.True);
        Assert.That(consumer.Wait(TimeSpan.FromSeconds(5)), Is.True);
        Assert.That(received, Is.EqualTo("late"));
    }

    [Test]
    public void TakeWakesAfterParkPhase()
    {
        // Sleep long enough that the consumer falls past the spin loop into park.
        // Spin window is ~tens of microseconds; 200ms is multiple orders of
        // magnitude longer, so the consumer is definitively parked.
        var q = new QwpSpscQueue<string>(2);
        string? received = null;
        var consumer = Task.Run(() => received = q.Take());
        Thread.Sleep(200);
        q.Offer("after-park");
        Assert.That(consumer.Wait(TimeSpan.FromSeconds(5)), Is.True);
        Assert.That(received, Is.EqualTo("after-park"));
    }

    [Test]
    public void TakeThrowsOnCancellation()
    {
        var q = new QwpSpscQueue<string>(2);
        using var cts = new CancellationTokenSource();
        var consumer = Task.Run(() => q.Take(cts.Token));
        Thread.Sleep(50);
        cts.Cancel();
        Assert.That(() => consumer.Wait(TimeSpan.FromSeconds(5)),
            Throws.TypeOf<AggregateException>()
                .With.Property("InnerException").TypeOf<OperationCanceledException>());
    }

    [Test]
    public void ConcurrentOfferTakeRoundtripsAllValues()
    {
        const int N = 10_000;
        var q = new QwpSpscQueue<string>(64);
        var received = new List<string>(N);

        var consumer = Task.Run(() =>
        {
            for (var i = 0; i < N; i++) received.Add(q.Take());
        });

        var producer = Task.Run(() =>
        {
            for (var i = 0; i < N; i++)
            {
                while (!q.Offer("v" + i)) Thread.SpinWait(10);
            }
        });

        Assert.That(Task.WaitAll(new[] { consumer, producer }, TimeSpan.FromSeconds(15)), Is.True);
        Assert.That(received.Count, Is.EqualTo(N));
        for (var i = 0; i < N; i++)
        {
            Assert.That(received[i], Is.EqualTo("v" + i));
        }
    }

    [Test]
    public void RejectsCapacityBelowOne()
    {
        Assert.That(() => new QwpSpscQueue<string>(0), Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => new QwpSpscQueue<string>(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
