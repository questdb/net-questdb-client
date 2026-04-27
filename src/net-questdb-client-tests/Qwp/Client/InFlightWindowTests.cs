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

using System.Collections.Concurrent;
using System.Threading.Tasks;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Mirrors <c>InFlightWindowTest.java</c> on Java main 64b7ee69. Java's
///     LockSupport.park / VarHandle.getAndAdd pair becomes ManualResetEventSlim +
///     Interlocked in .NET; the lock-free single-producer-single-consumer protocol is
///     unchanged. Timeout exceptions become <see cref="IngressError"/> instead of
///     <c>LineSenderException</c>.
/// </summary>
[TestFixture]
public class InFlightWindowTests
{
    [Test]
    public void DefaultWindowSize()
    {
        using var w = new InFlightWindow();
        Assert.That(w.MaxWindowSize, Is.EqualTo(InFlightWindow.DEFAULT_WINDOW_SIZE));
    }

    [Test]
    public void GetMaxWindowSize()
    {
        using var w = new InFlightWindow(16, 5_000);
        Assert.That(w.MaxWindowSize, Is.EqualTo(16));
    }

    [Test]
    public void InvalidWindowSize()
    {
        Assert.That(() => new InFlightWindow(0, 5_000), Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => new InFlightWindow(-1, 5_000), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetHighestAckedSequenceInitiallyMinusOne()
    {
        using var w = new InFlightWindow();
        Assert.That(w.HighestAckedSequence, Is.EqualTo(-1L));
    }

    [Test]
    public void TryAddInFlight()
    {
        using var w = new InFlightWindow(2, 1_000);
        Assert.That(w.TryAddInFlight(0), Is.True);
        Assert.That(w.TryAddInFlight(1), Is.True);
        Assert.That(w.TryAddInFlight(2), Is.False);  // window full
        Assert.That(w.InFlightCount, Is.EqualTo(2));
    }

    [Test]
    public void HasWindowSpace()
    {
        using var w = new InFlightWindow(2, 1_000);
        Assert.That(w.HasWindowSpace, Is.True);
        w.TryAddInFlight(0);
        Assert.That(w.HasWindowSpace, Is.True);
        w.TryAddInFlight(1);
        Assert.That(w.HasWindowSpace, Is.False);
    }

    [Test]
    public void BasicAddAndAcknowledge()
    {
        using var w = new InFlightWindow(8, 1_000);
        w.AddInFlight(0);
        w.AddInFlight(1);
        w.AddInFlight(2);
        Assert.That(w.InFlightCount, Is.EqualTo(3));
        Assert.That(w.AcknowledgeUpTo(2), Is.EqualTo(3));
        Assert.That(w.IsEmpty, Is.True);
    }

    [Test]
    public void AcknowledgeUpToBasic()
    {
        using var w = new InFlightWindow(8, 1_000);
        w.AddInFlight(0); w.AddInFlight(1); w.AddInFlight(2);
        Assert.That(w.AcknowledgeUpTo(1), Is.EqualTo(2));
        Assert.That(w.InFlightCount, Is.EqualTo(1));
        Assert.That(w.HighestAckedSequence, Is.EqualTo(1L));
    }

    [Test]
    public void AcknowledgeUpToAllBatches()
    {
        using var w = new InFlightWindow(8, 1_000);
        for (var i = 0; i < 5; i++) w.AddInFlight(i);
        Assert.That(w.AcknowledgeUpTo(4), Is.EqualTo(5));
        Assert.That(w.InFlightCount, Is.EqualTo(0));
    }

    [Test]
    public void AcknowledgeUpToEmpty()
    {
        using var w = new InFlightWindow();
        Assert.That(w.AcknowledgeUpTo(5), Is.EqualTo(0));
        Assert.That(w.HighestAckedSequence, Is.EqualTo(-1L));
    }

    [Test]
    public void AcknowledgeUpToIdempotent()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0); w.AddInFlight(1);
        Assert.That(w.AcknowledgeUpTo(1), Is.EqualTo(2));
        Assert.That(w.AcknowledgeUpTo(1), Is.EqualTo(0));
        Assert.That(w.AcknowledgeUpTo(0), Is.EqualTo(0));
    }

    [Test]
    public void AcknowledgeAlreadyAcked()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0);
        Assert.That(w.Acknowledge(0), Is.True);
        Assert.That(w.Acknowledge(0), Is.True); // already acked, but still considered acked
    }

    [Test]
    public void GetHighestAckedSequenceAdvancesOnAcknowledge()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0); w.AddInFlight(1); w.AddInFlight(2);
        w.AcknowledgeUpTo(0);
        Assert.That(w.HighestAckedSequence, Is.EqualTo(0L));
        w.AcknowledgeUpTo(2);
        Assert.That(w.HighestAckedSequence, Is.EqualTo(2L));
    }

    [Test]
    public void GetHighestAckedSequenceDoesNotRegress()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0); w.AddInFlight(1); w.AddInFlight(2);
        w.AcknowledgeUpTo(2);
        w.AcknowledgeUpTo(0); // earlier ack should not regress
        Assert.That(w.HighestAckedSequence, Is.EqualTo(2L));
    }

    [Test]
    public void MultipleBatches()
    {
        using var w = new InFlightWindow(4, 1_000);
        for (var i = 0; i < 4; i++) w.AddInFlight(i);
        Assert.That(w.IsFull, Is.True);
        w.AcknowledgeUpTo(3);
        Assert.That(w.IsEmpty, Is.True);
    }

    [Test]
    public void WindowFull()
    {
        using var w = new InFlightWindow(2, 1_000);
        w.AddInFlight(0); w.AddInFlight(1);
        Assert.That(w.IsFull, Is.True);
        Assert.That(w.TryAddInFlight(2), Is.False);
    }

    [Test]
    public void WindowBlocksWhenFull()
    {
        using var w = new InFlightWindow(2, 5_000);
        w.AddInFlight(0); w.AddInFlight(1);

        // Acknowledge after 100ms from another task; the AddInFlight on this thread
        // should unblock once space opens.
        var ackTask = Task.Run(() =>
        {
            System.Threading.Thread.Sleep(100);
            w.AcknowledgeUpTo(0);
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        w.AddInFlight(2);
        sw.Stop();
        ackTask.Wait();

        Assert.That(sw.ElapsedMilliseconds, Is.GreaterThan(50));
        Assert.That(w.InFlightCount, Is.EqualTo(2));
    }

    [Test]
    public void WindowBlocksTimeout()
    {
        using var w = new InFlightWindow(1, 100);
        w.AddInFlight(0);
        Assert.That(() => w.AddInFlight(1),
                    Throws.TypeOf<IngressError>().With.Message.Contains("Timeout waiting for window space"));
    }

    [Test]
    public void AcknowledgeUpToWakesBlockedAdder()
    {
        using var w = new InFlightWindow(1, 5_000);
        w.AddInFlight(0);

        var addTask = Task.Run(() => w.AddInFlight(1));
        System.Threading.Thread.Sleep(50);
        Assert.That(addTask.IsCompleted, Is.False);

        w.AcknowledgeUpTo(0);
        Assert.That(addTask.Wait(TimeSpan.FromSeconds(1)), Is.True);
        Assert.That(w.InFlightCount, Is.EqualTo(1));
    }

    [Test]
    public void AwaitEmpty()
    {
        using var w = new InFlightWindow(8, 5_000);
        w.AddInFlight(0); w.AddInFlight(1); w.AddInFlight(2);

        var ackTask = Task.Run(() =>
        {
            System.Threading.Thread.Sleep(100);
            w.AcknowledgeUpTo(2);
        });

        w.AwaitEmpty();
        ackTask.Wait();
        Assert.That(w.IsEmpty, Is.True);
    }

    [Test]
    public void AwaitEmptyAlreadyEmpty()
    {
        using var w = new InFlightWindow();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        w.AwaitEmpty();
        sw.Stop();
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(100), "should return immediately");
    }

    [Test]
    public void AwaitEmptyTimeout()
    {
        using var w = new InFlightWindow(8, 100);
        w.AddInFlight(0);
        Assert.That(() => w.AwaitEmpty(),
                    Throws.TypeOf<IngressError>().With.Message.Contains("Timeout waiting for batch acknowledgments"));
    }

    [Test]
    public void AcknowledgeUpToWakesAwaitEmpty()
    {
        using var w = new InFlightWindow(8, 5_000);
        w.AddInFlight(0); w.AddInFlight(1);

        var awaitTask = Task.Run(() => w.AwaitEmpty());
        System.Threading.Thread.Sleep(50);
        Assert.That(awaitTask.IsCompleted, Is.False);

        w.AcknowledgeUpTo(1);
        Assert.That(awaitTask.Wait(TimeSpan.FromSeconds(1)), Is.True);
    }

    // ---- Error / fail paths ----

    [Test]
    public void FailBatch()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0); w.AddInFlight(1);
        var err = new InvalidOperationException("boom");
        w.Fail(1, err);
        Assert.That(w.LastError, Is.SameAs(err));
        Assert.That(w.TotalFailed, Is.EqualTo(1L));
    }

    [Test]
    public void FailPropagatesError()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0);
        w.Fail(0, new InvalidOperationException("boom"));
        Assert.That(() => w.AddInFlight(1),
                    Throws.TypeOf<IngressError>().With.Message.Contains("Batch 0 failed"));
    }

    [Test]
    public void FailAllPropagatesError()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0); w.AddInFlight(1); w.AddInFlight(2);
        w.FailAll(new InvalidOperationException("transport"));
        // FailAll advances highestAcked to highestSent so InFlightCount drops to 0.
        Assert.That(w.InFlightCount, Is.EqualTo(0));
        Assert.That(w.TotalFailed, Is.EqualTo(3L));
        Assert.That(() => w.AwaitEmpty(),
                    Throws.TypeOf<IngressError>().With.Message.Contains("transport"));
    }

    [Test]
    public void FailWakesBlockedAdder()
    {
        using var w = new InFlightWindow(1, 5_000);
        w.AddInFlight(0);

        var addTask = Task.Run(() =>
        {
            try { w.AddInFlight(1); return null; }
            catch (Exception ex) { return ex; }
        });
        System.Threading.Thread.Sleep(50);
        Assert.That(addTask.IsCompleted, Is.False);

        w.Fail(0, new InvalidOperationException("boom"));
        Assert.That(addTask.Wait(TimeSpan.FromSeconds(1)), Is.True);
        Assert.That(addTask.Result, Is.TypeOf<IngressError>());
    }

    [Test]
    public void FailWakesAwaitEmpty()
    {
        using var w = new InFlightWindow(8, 5_000);
        w.AddInFlight(0); w.AddInFlight(1);

        var awaitTask = Task.Run(() =>
        {
            try { w.AwaitEmpty(); return null; }
            catch (Exception ex) { return ex; }
        });
        System.Threading.Thread.Sleep(50);
        Assert.That(awaitTask.IsCompleted, Is.False);

        w.FailAll(new InvalidOperationException("boom"));
        Assert.That(awaitTask.Wait(TimeSpan.FromSeconds(1)), Is.True);
        Assert.That(awaitTask.Result, Is.TypeOf<IngressError>());
    }

    [Test]
    public void ClearError()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0);
        w.Fail(0, new InvalidOperationException("boom"));
        Assert.That(w.LastError, Is.Not.Null);
        w.ClearError();
        Assert.That(w.LastError, Is.Null);
    }

    [Test]
    public void FailThenClearThenAdd()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0);
        w.Fail(0, new InvalidOperationException("boom"));
        w.ClearError();
        // Acknowledge the failed batch + add a new one.
        w.AcknowledgeUpTo(0);
        w.AddInFlight(1);
        Assert.That(w.InFlightCount, Is.EqualTo(1));
    }

    [Test]
    public void Reset()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0); w.AddInFlight(1);
        w.Reset();
        Assert.That(w.InFlightCount, Is.EqualTo(0));
        Assert.That(w.HighestAckedSequence, Is.EqualTo(-1L));
        Assert.That(w.LastError, Is.Null);
    }

    [Test]
    public void MultipleResets()
    {
        using var w = new InFlightWindow();
        for (var i = 0; i < 5; i++)
        {
            w.AddInFlight(i);
            w.AcknowledgeUpTo(i);
            w.Reset();
            Assert.That(w.IsEmpty, Is.True);
        }
    }

    // ---- Edge cases ----

    [Test]
    public void ZeroBatchId()
    {
        using var w = new InFlightWindow();
        w.AddInFlight(0);
        Assert.That(w.InFlightCount, Is.EqualTo(1));
        Assert.That(w.AcknowledgeUpTo(0), Is.EqualTo(1));
    }

    [Test]
    public void SmallestPossibleWindow()
    {
        using var w = new InFlightWindow(1, 1_000);
        w.AddInFlight(0);
        Assert.That(w.IsFull, Is.True);
        w.AcknowledgeUpTo(0);
        w.AddInFlight(1);
        Assert.That(w.InFlightCount, Is.EqualTo(1));
    }

    [Test]
    public void VeryLargeWindow()
    {
        using var w = new InFlightWindow(10_000, 1_000);
        for (var i = 0; i < 10_000; i++) w.AddInFlight(i);
        Assert.That(w.IsFull, Is.True);
        Assert.That(w.AcknowledgeUpTo(9_999), Is.EqualTo(10_000));
    }

    [Test]
    public void FillAndDrainRepeatedly()
    {
        using var w = new InFlightWindow(4, 1_000);
        long batchId = 0;
        for (var round = 0; round < 10; round++)
        {
            for (var i = 0; i < 4; i++) w.AddInFlight(batchId++);
            Assert.That(w.IsFull, Is.True);
            w.AcknowledgeUpTo(batchId - 1);
            Assert.That(w.IsEmpty, Is.True);
        }
        Assert.That(batchId, Is.EqualTo(40L));
        Assert.That(w.TotalAcked, Is.EqualTo(40L));
    }

    [Test]
    public void RapidAddAndAck()
    {
        using var w = new InFlightWindow(8, 1_000);
        for (var i = 0; i < 1_000; i++)
        {
            w.AddInFlight(i);
            w.AcknowledgeUpTo(i);
        }
        Assert.That(w.IsEmpty, Is.True);
        Assert.That(w.TotalAcked, Is.EqualTo(1_000L));
    }

    // ---- Concurrency ----

    [Test]
    public void ConcurrentAddAndAck()
    {
        using var w = new InFlightWindow(16, 5_000);
        const int batches = 5_000;
        const long lastBatch = batches - 1;
        var ackTask = Task.Run(() =>
        {
            while (w.HighestAckedSequence < lastBatch)
            {
                var sent = w.HighestAckedSequence + w.InFlightCount;
                if (sent > w.HighestAckedSequence) w.AcknowledgeUpTo(sent);
                else System.Threading.Thread.Yield();
            }
        });

        for (var i = 0; i < batches; i++) w.AddInFlight(i);
        Assert.That(ackTask.Wait(TimeSpan.FromSeconds(10)), Is.True, "ack task should drain the window");
        Assert.That(w.HighestAckedSequence, Is.EqualTo(lastBatch));
    }

    [Test]
    public void ConcurrentAddAndCumulativeAck()
    {
        using var w = new InFlightWindow(8, 5_000);
        const int batches = 1_000;
        const long lastBatch = batches - 1;
        var ackTask = Task.Run(() =>
        {
            while (w.HighestAckedSequence < lastBatch)
            {
                var sent = w.HighestAckedSequence + w.InFlightCount;
                if (sent > w.HighestAckedSequence) w.AcknowledgeUpTo(sent);
                else System.Threading.Thread.Yield();
            }
        });

        for (var i = 0; i < batches; i++) w.AddInFlight(i);
        Assert.That(ackTask.Wait(TimeSpan.FromSeconds(10)), Is.True);
        Assert.That(w.HighestAckedSequence, Is.EqualTo(lastBatch));
    }

    [Test]
    public void HighConcurrencyStress()
    {
        using var w = new InFlightWindow(64, 30_000);
        const int batches = 10_000;
        const long lastBatch = batches - 1;
        var producer = Task.Run(() => { for (var i = 0; i < batches; i++) w.AddInFlight(i); });
        var acker = Task.Run(() =>
        {
            while (w.HighestAckedSequence < lastBatch)
            {
                var sent = w.HighestAckedSequence + w.InFlightCount;
                if (sent > w.HighestAckedSequence) w.AcknowledgeUpTo(sent);
                else System.Threading.Thread.Yield();
            }
        });
        Assert.That(Task.WaitAll(new[] { producer, acker }, TimeSpan.FromSeconds(30)), Is.True);
        Assert.That(w.HighestAckedSequence, Is.EqualTo(lastBatch));
        Assert.That(w.IsEmpty, Is.True);
    }
}
