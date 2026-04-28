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

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpInFlightWindowTests
{
    [Test]
    public void NewWindow_HasMinusOneSentinels()
    {
        var w = new QwpInFlightWindow();
        Assert.That(w.AckedSequence, Is.EqualTo(-1L));
        Assert.That(w.HighestSentSequence, Is.EqualTo(-1L));
        Assert.That(w.IsEmpty, Is.True, "empty by definition when nothing sent");
        Assert.That(w.InFlightCount, Is.Zero);
        Assert.That(w.HasFailure, Is.False);
    }

    [Test]
    public void Add_SequentialSequencesAdvancesHighest()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        w.Add(1);
        w.Add(2);

        Assert.That(w.HighestSentSequence, Is.EqualTo(2L));
        Assert.That(w.InFlightCount, Is.EqualTo(3));
        Assert.That(w.IsEmpty, Is.False);
    }

    [Test]
    public void Add_NonSequential_Throws()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        Assert.Throws<InvalidOperationException>(() => w.Add(2));
    }

    [Test]
    public void AcknowledgeUpTo_CumulativeAck_ReleasesAllSlots()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        w.Add(1);
        w.Add(2);

        w.AcknowledgeUpTo(2);

        Assert.That(w.AckedSequence, Is.EqualTo(2L));
        Assert.That(w.IsEmpty, Is.True);
    }

    [Test]
    public void AcknowledgeUpTo_AbsorbsDuplicates()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        w.Add(1);
        w.AcknowledgeUpTo(1);

        Assert.DoesNotThrow(() => w.AcknowledgeUpTo(0));
        Assert.That(w.AckedSequence, Is.EqualTo(1L));
    }

    [Test]
    public void AcknowledgeUpTo_BeyondHighestSent_Throws()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        Assert.Throws<InvalidOperationException>(() => w.AcknowledgeUpTo(5));
    }

    [Test]
    public void AwaitEmpty_AlreadyEmpty_ReturnsImmediately()
    {
        var w = new QwpInFlightWindow();
        w.AwaitEmpty(TimeSpan.FromMilliseconds(10));
    }

    [Test]
    public void AwaitEmpty_AfterAck_ReturnsImmediately()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        w.Add(1);
        w.AcknowledgeUpTo(1);

        w.AwaitEmpty(TimeSpan.FromMilliseconds(10));
    }

    [Test]
    public void AwaitEmpty_NotEmpty_TimesOut()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);

        Assert.Throws<TimeoutException>(() => w.AwaitEmpty(TimeSpan.FromMilliseconds(50)));
    }

    [Test]
    public void FailAll_PropagatesThroughAwaitEmpty()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        var ex = new InvalidOperationException("server error");
        w.FailAll(ex);

        var thrown = Assert.Throws<InvalidOperationException>(() => w.AwaitEmpty(TimeSpan.FromSeconds(1)));
        Assert.That(thrown, Is.SameAs(ex));
    }

    [Test]
    public void FailAll_RejectsSubsequentAdd()
    {
        var w = new QwpInFlightWindow();
        var ex = new InvalidOperationException("boom");
        w.FailAll(ex);

        Assert.Throws<InvalidOperationException>(() => w.Add(0));
    }

    [Test]
    public void FailAll_OnlyFirstWins()
    {
        var w = new QwpInFlightWindow();
        var first = new InvalidOperationException("first");
        var second = new InvalidOperationException("second");
        w.FailAll(first);
        w.FailAll(second);

        var thrown = Assert.Throws<InvalidOperationException>(() => w.AwaitEmpty(TimeSpan.FromSeconds(1)));
        Assert.That(thrown, Is.SameAs(first));
    }

    [Test]
    public void AwaitEmpty_DrainedConcurrently_ReturnsCleanly()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        w.Add(1);

        // Background ACK after a small delay.
        var t = Task.Run(() =>
        {
            Thread.Sleep(50);
            w.AcknowledgeUpTo(1);
        });

        w.AwaitEmpty(TimeSpan.FromSeconds(2));
        t.Wait();
        Assert.That(w.IsEmpty);
    }

    [Test]
    public void AwaitEmpty_Cancelled_ThrowsOperationCancelled()
    {
        var w = new QwpInFlightWindow();
        w.Add(0);
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(50);

        Assert.Throws<OperationCanceledException>(() => w.AwaitEmpty(TimeSpan.FromSeconds(10), cts.Token));
    }
}
