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
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpReconnectPolicyTests
{
    [Test]
    public void Constructor_NonPositiveInitialBackoff_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QwpReconnectPolicy(TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QwpReconnectPolicy(TimeSpan.FromTicks(-1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void Constructor_MaxBackoffLessThanInitial_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QwpReconnectPolicy(
                TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void Constructor_NegativeOutageDuration_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QwpReconnectPolicy(
                TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromTicks(-1)));
    }

    [Test]
    public void ComputeBackoff_AttemptZero_ReturnsInitial()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMinutes(1));

        Assert.That(policy.ComputeBackoff(0), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
    }

    [Test]
    public void ComputeBackoff_DoublesPerAttempt()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMinutes(5));

        Assert.That(policy.ComputeBackoff(0), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(policy.ComputeBackoff(1), Is.EqualTo(TimeSpan.FromMilliseconds(200)));
        Assert.That(policy.ComputeBackoff(2), Is.EqualTo(TimeSpan.FromMilliseconds(400)));
        Assert.That(policy.ComputeBackoff(3), Is.EqualTo(TimeSpan.FromMilliseconds(800)));
        Assert.That(policy.ComputeBackoff(4), Is.EqualTo(TimeSpan.FromMilliseconds(1600)));
    }

    [Test]
    public void ComputeBackoff_CapsAtMaxBackoff()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(500),
            TimeSpan.FromMinutes(5));

        Assert.That(policy.ComputeBackoff(0), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(policy.ComputeBackoff(1), Is.EqualTo(TimeSpan.FromMilliseconds(200)));
        Assert.That(policy.ComputeBackoff(2), Is.EqualTo(TimeSpan.FromMilliseconds(400)));
        // 800ms would exceed the 500ms cap → clamp.
        Assert.That(policy.ComputeBackoff(3), Is.EqualTo(TimeSpan.FromMilliseconds(500)));
        Assert.That(policy.ComputeBackoff(20), Is.EqualTo(TimeSpan.FromMilliseconds(500)));
    }

    [Test]
    public void ComputeBackoff_LargeAttemptIndex_DoesNotOverflow()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(30),
            TimeSpan.FromHours(1));

        Assert.That(policy.ComputeBackoff(1000), Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void ComputeBackoff_NegativeAttempt_Throws()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMinutes(1));

        Assert.Throws<ArgumentOutOfRangeException>(() => policy.ComputeBackoff(-1));
    }

    [Test]
    public void NextBackoffOrGiveUp_BudgetExhausted_ReturnsNull()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(30));

        Assert.That(policy.NextBackoffOrGiveUp(0, TimeSpan.FromSeconds(31)), Is.Null);
    }

    [Test]
    public void NextBackoffOrGiveUp_BeforeExhaustion_ReturnsBackoff()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(30));

        Assert.That(
            policy.NextBackoffOrGiveUp(0, TimeSpan.FromSeconds(5)),
            Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(
            policy.NextBackoffOrGiveUp(2, TimeSpan.FromSeconds(5)),
            Is.EqualTo(TimeSpan.FromMilliseconds(400)));
    }

    [Test]
    public void NextBackoffOrGiveUp_BackoffWouldExceedBudget_ClipsToRemaining()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(60),
            TimeSpan.FromSeconds(10));

        // Used 8s of the 10s budget → 2s remaining; computed backoff is 5s → clip to 2s.
        var next = policy.NextBackoffOrGiveUp(0, TimeSpan.FromSeconds(8));
        Assert.That(next, Is.EqualTo(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void NextBackoffOrGiveUp_NoBudgetLeft_ReturnsNull()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(10));

        // Exactly at the boundary — no remaining time after subtraction.
        Assert.That(policy.NextBackoffOrGiveUp(0, TimeSpan.FromSeconds(10)), Is.Null);
    }

    [Test]
    public void Jitter_IdentityByDefault_DeterministicForTests()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10));

        // No jitter passed → backoff is exactly the deterministic computation.
        Assert.That(policy.ComputeBackoff(0), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
        Assert.That(policy.ComputeBackoff(0), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
    }

    [Test]
    public void Jitter_Equal_SpreadsBackoffAcrossFullRange()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(10),
            jitter: QwpReconnectPolicy.EqualJitter);

        var samples = Enumerable.Range(0, 64).Select(_ => policy.ComputeBackoff(0)).ToArray();

        foreach (var s in samples)
        {
            Assert.That(s, Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100)));
            Assert.That(s, Is.LessThan(TimeSpan.FromMilliseconds(200)));
        }

        Assert.That(samples.Distinct().Count(), Is.GreaterThan(1),
            "uniform jitter must produce varied samples — otherwise it's not actually random");
    }

    [Test]
    public void Jitter_Equal_StillFiresWhenSaturated()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10),
            jitter: QwpReconnectPolicy.EqualJitter);

        var samples = Enumerable.Range(0, 64).Select(_ => policy.ComputeBackoff(10)).ToArray();

        foreach (var s in samples)
        {
            // At saturation (base==max==100ms) jitter spans [100ms, 200ms).
            Assert.That(s, Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100)));
            Assert.That(s, Is.LessThan(TimeSpan.FromMilliseconds(200)));
        }

        Assert.That(samples.Distinct().Count(), Is.GreaterThan(1),
            "jitter must still vary samples once exponential growth saturates at MaxBackoff");
    }

    [Test]
    public void Jitter_Full_SpreadsBackoffOverZeroToBase()
    {
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(10),
            jitter: QwpReconnectPolicy.FullJitter);

        var samples = Enumerable.Range(0, 64).Select(_ => policy.ComputeBackoff(0)).ToArray();

        foreach (var s in samples)
        {
            Assert.That(s, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
            Assert.That(s, Is.LessThanOrEqualTo(TimeSpan.FromMilliseconds(100)));
        }

        Assert.That(samples.Distinct().Count(), Is.GreaterThan(1));
    }
}
