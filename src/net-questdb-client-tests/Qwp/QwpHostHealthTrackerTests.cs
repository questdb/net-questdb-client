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

using System;
using NUnit.Framework;
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpHostHealthTrackerTests
{
    [Test]
    public void Constructor_RejectsEmptyHosts()
    {
        Assert.Throws<ArgumentException>(() => new QwpHostHealthTracker(Array.Empty<string>()));
    }

    [Test]
    public void Constructor_RejectsNullHosts()
    {
        Assert.Throws<ArgumentNullException>(() => new QwpHostHealthTracker(null!));
    }

    [Test]
    public void PickNext_ReturnsAddressOrderWhenAllUnknown()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b", "c" });
        Assert.That(t.PickNext(), Is.EqualTo(0));
        t.RecordTransportError(0);
        Assert.That(t.PickNext(), Is.EqualTo(1));
        t.RecordTransportError(1);
        Assert.That(t.PickNext(), Is.EqualTo(2));
        t.RecordTransportError(2);
        Assert.That(t.PickNext(), Is.EqualTo(-1));
    }

    [Test]
    public void PickNext_PrefersHealthyOverUnknown()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b", "c" });
        // host 1 was healthy in previous round; round flag cleared, classifications kept
        t.RecordSuccess(1);
        t.BeginRound(forgetClassifications: false);
        Assert.That(t.PickNext(), Is.EqualTo(1));
    }

    [Test]
    public void PickNext_PrefersUnknownOverTransientReject()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b", "c" });
        t.RecordRoleReject(0, transient: true);
        t.BeginRound(forgetClassifications: false);
        // host 0 is TransientReject; hosts 1 and 2 are Unknown — pick first Unknown
        Assert.That(t.PickNext(), Is.EqualTo(1));
    }

    [Test]
    public void PickNext_PrefersTransientRejectOverTransportError()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b" });
        t.RecordRoleReject(0, transient: true);
        t.RecordTransportError(1);
        t.BeginRound(forgetClassifications: false);
        // Both attempted-flags cleared, classifications kept; TransientReject (1)
        // beats TransportError (2) in priority.
        Assert.That(t.PickNext(), Is.EqualTo(0));
    }

    [Test]
    public void PickNext_PrefersTransportErrorOverTopologyReject()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b" });
        t.RecordRoleReject(0, transient: false); // TopologyReject
        t.RecordTransportError(1);
        t.BeginRound(forgetClassifications: false);
        Assert.That(t.PickNext(), Is.EqualTo(1));
    }

    [Test]
    public void BeginRound_ForgetClassifications_KeepsLastHealthyAsSticky()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b", "c" });
        t.RecordSuccess(0);
        t.RecordRoleReject(1, transient: false);
        t.RecordTransportError(2);
        t.BeginRound(forgetClassifications: true);
        Assert.That(t.GetState(0), Is.EqualTo(QwpHostState.Healthy));
        Assert.That(t.GetState(1), Is.EqualTo(QwpHostState.Unknown));
        Assert.That(t.GetState(2), Is.EqualTo(QwpHostState.Unknown));
        Assert.That(t.PickNext(), Is.EqualTo(0));
    }

    [Test]
    public void BeginRound_ForgetClassifications_NoHealthy_ResetsAllToUnknown()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b" });
        t.RecordRoleReject(0, transient: false);
        t.RecordTransportError(1);
        t.BeginRound(forgetClassifications: true);
        Assert.That(t.GetState(0), Is.EqualTo(QwpHostState.Unknown));
        Assert.That(t.GetState(1), Is.EqualTo(QwpHostState.Unknown));
    }

    [Test]
    public void BeginRound_ForgetClassifications_PrefersMostRecentHealthy()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b", "c" });
        t.RecordSuccess(0);
        t.RecordSuccess(2);
        t.BeginRound(forgetClassifications: true);
        Assert.That(t.GetState(0), Is.EqualTo(QwpHostState.Unknown));
        Assert.That(t.GetState(2), Is.EqualTo(QwpHostState.Healthy));
    }

    [Test]
    public void RecordSuccess_MarksAttemptedAndHealthy()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b" });
        t.RecordSuccess(0);
        Assert.That(t.GetState(0), Is.EqualTo(QwpHostState.Healthy));
        // attempted-this-round flag is set: should pick host 1 next
        Assert.That(t.PickNext(), Is.EqualTo(1));
    }

    [Test]
    public void RecordRoleReject_ClassifiesByTransientFlag()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b" });
        t.RecordRoleReject(0, transient: true);
        t.RecordRoleReject(1, transient: false);
        Assert.That(t.GetState(0), Is.EqualTo(QwpHostState.TransientReject));
        Assert.That(t.GetState(1), Is.EqualTo(QwpHostState.TopologyReject));
    }

    [Test]
    public void StickyHealthy_AcrossReconnectAfterDrop()
    {
        // Scenario B: connect succeeded on host 1, connection drops, reconnect.
        // BeginRound(false) keeps Healthy classification, host 1 picked first.
        var t = new QwpHostHealthTracker(new[] { "a", "b", "c" });
        t.RecordTransportError(0);
        t.RecordSuccess(1);
        // Connection drops; new reconnect round begins.
        t.BeginRound(forgetClassifications: false);
        Assert.That(t.PickNext(), Is.EqualTo(1));
    }

    [Test]
    public void FullRoundExhaustion_ReturnsMinusOne()
    {
        var t = new QwpHostHealthTracker(new[] { "a", "b" });
        Assert.That(t.PickNext(), Is.EqualTo(0));
        t.RecordTransportError(0);
        Assert.That(t.PickNext(), Is.EqualTo(1));
        t.RecordTransportError(1);
        Assert.That(t.PickNext(), Is.EqualTo(-1));
    }
}
