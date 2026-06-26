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

#if NET7_0_OR_GREATER

using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpTrackedCursorTransportTests
{
    [Test]
    public void RoleReject_WithZoneHeader_RecordsZoneBeforeRoleReject()
    {
        // Spec: zone is observed independently of state; once observed it persists for the host's
        // lifetime in this client. Without RecordZone on the 421 path, an ingress sender that later
        // sets a clientZone would tier hosts incorrectly.
        var hosts = new[] { "h1:9000", "h2:9000" };
        // targetIsPrimary=true folds every zone into Same (ingress-default behaviour); use
        // targetIsPrimary=false here so RecordZone actually distinguishes Same vs Other.
        var tracker = new QwpHostHealthTracker(hosts, clientZone: "us-east-1", targetIsPrimary: false);
        var stub = new RejectingStub("REPLICA", "us-east-1");
        var tracked = new QwpTrackedCursorTransport(stub, tracker, hostIndex: 0);

        Assert.ThrowsAsync<QwpIngressRoleRejectedException>(
            async () => await tracked.ConnectAsync(CancellationToken.None));

        Assert.That(tracker.GetZoneTier(0), Is.EqualTo(QwpZoneTier.Same),
            "host returning X-QuestDB-Zone matching clientZone must tier as Same");
        Assert.That(tracker.GetState(0), Is.EqualTo(QwpHostState.TopologyReject),
            "REPLICA when targetIsPrimary records a non-transient role reject");
    }

    [Test]
    public void RoleReject_WithoutZoneHeader_DoesNotTouchZoneTier()
    {
        var hosts = new[] { "h1:9000" };
        // targetIsPrimary=true folds every zone into Same (ingress-default behaviour); use
        // targetIsPrimary=false here so RecordZone actually distinguishes Same vs Other.
        var tracker = new QwpHostHealthTracker(hosts, clientZone: "us-east-1", targetIsPrimary: false);
        var stub = new RejectingStub("REPLICA", zone: null);
        var tracked = new QwpTrackedCursorTransport(stub, tracker, hostIndex: 0);

        Assert.ThrowsAsync<QwpIngressRoleRejectedException>(
            async () => await tracked.ConnectAsync(CancellationToken.None));

        Assert.That(tracker.GetZoneTier(0), Is.EqualTo(QwpZoneTier.Unknown));
    }

    [Test]
    public void RoleReject_CrossZoneRemoteHost_TiersAsOther()
    {
        var hosts = new[] { "h1:9000" };
        // targetIsPrimary=true folds every zone into Same (ingress-default behaviour); use
        // targetIsPrimary=false here so RecordZone actually distinguishes Same vs Other.
        var tracker = new QwpHostHealthTracker(hosts, clientZone: "us-east-1", targetIsPrimary: false);
        var stub = new RejectingStub("REPLICA", "eu-west-2");
        var tracked = new QwpTrackedCursorTransport(stub, tracker, hostIndex: 0);

        Assert.ThrowsAsync<QwpIngressRoleRejectedException>(
            async () => await tracked.ConnectAsync(CancellationToken.None));

        Assert.That(tracker.GetZoneTier(0), Is.EqualTo(QwpZoneTier.Other));
    }

    [Test]
    public void Success_WithZoneHeader_RecordsZoneOnConnect()
    {
        // The same-zone vs other-zone tiering must engage among *healthy* hosts, not just hosts
        // that role-reject. The success path therefore has to record the negotiated zone too.
        var hosts = new[] { "h1:9000", "h2:9000" };
        // targetIsPrimary=false so RecordZone actually distinguishes Same vs Other (ingress folds
        // every zone into Same when targetIsPrimary).
        var tracker = new QwpHostHealthTracker(hosts, clientZone: "us-east-1", targetIsPrimary: false);
        var stub = new ConnectingStub("us-east-1");
        var tracked = new QwpTrackedCursorTransport(stub, tracker, hostIndex: 0);

        Assert.DoesNotThrowAsync(async () => await tracked.ConnectAsync(CancellationToken.None));

        Assert.That(tracker.GetZoneTier(0), Is.EqualTo(QwpZoneTier.Same),
            "a healthy host whose X-QuestDB-Zone matches clientZone must tier as Same");
        Assert.That(tracker.GetState(0), Is.EqualTo(QwpHostState.Healthy));
    }

    [Test]
    public void Success_CrossZoneHost_TiersAsOther()
    {
        var hosts = new[] { "h1:9000" };
        var tracker = new QwpHostHealthTracker(hosts, clientZone: "us-east-1", targetIsPrimary: false);
        var stub = new ConnectingStub("eu-west-2");
        var tracked = new QwpTrackedCursorTransport(stub, tracker, hostIndex: 0);

        Assert.DoesNotThrowAsync(async () => await tracked.ConnectAsync(CancellationToken.None));

        Assert.That(tracker.GetZoneTier(0), Is.EqualTo(QwpZoneTier.Other));
        Assert.That(tracker.GetState(0), Is.EqualTo(QwpHostState.Healthy));
    }

    [Test]
    public void Success_WithoutZoneHeader_DoesNotTouchZoneTier()
    {
        var hosts = new[] { "h1:9000" };
        var tracker = new QwpHostHealthTracker(hosts, clientZone: "us-east-1", targetIsPrimary: false);
        var stub = new ConnectingStub(zone: null);
        var tracked = new QwpTrackedCursorTransport(stub, tracker, hostIndex: 0);

        Assert.DoesNotThrowAsync(async () => await tracked.ConnectAsync(CancellationToken.None));

        Assert.That(tracker.GetZoneTier(0), Is.EqualTo(QwpZoneTier.Unknown),
            "a server that advertises no zone must leave the tier untouched");
        Assert.That(tracker.GetState(0), Is.EqualTo(QwpHostState.Healthy));
    }

    private sealed class RejectingStub : IQwpCursorTransport
    {
        private readonly string _role;
        private readonly string? _zone;

        public RejectingStub(string role, string? zone)
        {
            _role = role;
            _zone = zone;
        }

        public (string Host, int Port)? Endpoint => ("h", 9000);

        public Task ConnectAsync(CancellationToken cancellationToken) =>
            throw new QwpIngressRoleRejectedException(_role, new Uri("ws://h:9000/write/v4"), _zone);

        public Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken) =>
            Task.FromResult(0);

        public Task CloseAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose() { }
    }

    private sealed class ConnectingStub : IQwpCursorTransport
    {
        public ConnectingStub(string? zone)
        {
            NegotiatedZone = zone;
        }

        public (string Host, int Port)? Endpoint => ("h", 9000);

        public string? NegotiatedZone { get; }

        public Task ConnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken) =>
            Task.FromResult(0);

        public Task CloseAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose() { }
    }
}

#endif
