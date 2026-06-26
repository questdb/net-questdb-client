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

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpCursorSendEngineMultiHostTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-mh-engine-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); }
            catch { /* mmap on Windows can hold the file briefly */ }
        }
    }

    [Test]
    public async Task RotatesPastReplicaToAccepting_FrameAcked()
    {
        var hosts = new[] { "replica.example:9000", "primary.example:9000" };
        var tracker = new QwpHostHealthTracker(hosts);
        var stubs = new List<MhStubTransport>();

        Func<IQwpCursorTransport> factory = () =>
        {
            var idx = tracker.PickNext();
            if (idx < 0)
            {
                tracker.BeginRound(forgetClassifications: true);
                idx = tracker.PickNext();
            }

            var stub = idx == 0
                ? new MhStubTransport(rejectWithRole: QwpConstants.RoleReplicaName, hosts[idx])
                : new MhStubTransport(rejectWithRole: null, hosts[idx]);
            stubs.Add(stub);
            return new QwpTrackedCursorTransport(stub, tracker, idx);
        };

        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromSeconds(5));
        using var engine = new QwpCursorSendEngine(
            slotLock, ring, factory, policy,
            appendDeadline: TimeSpan.FromSeconds(5),
            initialConnectMode: InitialConnectMode.async,
            skipBackoffPredicate: () => !tracker.IsRoundExhausted);

        engine.Start();
        engine.AppendBlocking(new byte[] { 7 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(engine.AckedFsn, Is.EqualTo(1L));
        Assert.That(tracker.GetState(0), Is.EqualTo(QwpHostState.TopologyReject));
        Assert.That(tracker.GetState(1), Is.EqualTo(QwpHostState.Healthy));

        // Replica stub never sent anything; primary stub received the frame.
        var primaryStubs = stubs.FindAll(s => s.HostId == hosts[1]);
        Assert.That(primaryStubs, Has.Count.GreaterThanOrEqualTo(1));
        var sentBytes = 0;
        foreach (var s in primaryStubs) sentBytes += s.Sent.Count;
        Assert.That(sentBytes, Is.EqualTo(1));
    }

    [Test]
    public async Task PrimaryCatchupReject_ClassifiedTransient_ContinuesToNext()
    {
        var hosts = new[] { "catchup.example:9000", "primary.example:9000" };
        var tracker = new QwpHostHealthTracker(hosts);

        Func<IQwpCursorTransport> factory = () =>
        {
            var idx = tracker.PickNext();
            if (idx < 0)
            {
                tracker.BeginRound(forgetClassifications: true);
                idx = tracker.PickNext();
            }

            var stub = idx == 0
                ? new MhStubTransport(rejectWithRole: QwpConstants.RolePrimaryCatchupName, hosts[idx])
                : new MhStubTransport(rejectWithRole: null, hosts[idx]);
            return new QwpTrackedCursorTransport(stub, tracker, idx);
        };

        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromSeconds(5));
        using var engine = new QwpCursorSendEngine(
            slotLock, ring, factory, policy,
            appendDeadline: TimeSpan.FromSeconds(5),
            initialConnectMode: InitialConnectMode.async,
            skipBackoffPredicate: () => !tracker.IsRoundExhausted);

        engine.Start();
        engine.AppendBlocking(new byte[] { 9 });

        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        Assert.That(engine.AckedFsn, Is.EqualTo(1L));
        Assert.That(tracker.GetState(0), Is.EqualTo(QwpHostState.TransientReject));
        Assert.That(tracker.GetState(1), Is.EqualTo(QwpHostState.Healthy));
    }

    [Test]
    public async Task FailedOver_FiresWhenSuccessHostDiffersFromPrevious()
    {
        var hosts = new[] { "h1:9000", "h2:9000" };
        var tracker = new QwpHostHealthTracker(hosts);
        var rejectFirst = true;
        var stubsByIdx = new MhStubTransport?[hosts.Length];

        Func<IQwpCursorTransport> factory = () =>
        {
            var idx = tracker.PickNext();
            if (idx < 0)
            {
                tracker.BeginRound(forgetClassifications: true);
                idx = tracker.PickNext();
            }
            var rejectThis = idx == 0 && rejectFirst;
            var stub = new MhStubTransport(rejectWithRole: rejectThis ? QwpConstants.RoleReplicaName : null, hosts[idx]);
            stubsByIdx[idx] = stub;
            return new QwpTrackedCursorTransport(stub, tracker, idx);
        };

        var events = new List<QuestDB.Senders.SenderConnectionEvent>();
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromSeconds(5));
        using var engine = new QwpCursorSendEngine(
            slotLock, ring, factory, policy,
            appendDeadline: TimeSpan.FromSeconds(5),
            initialConnectMode: InitialConnectMode.async,
            skipBackoffPredicate: () => !tracker.IsRoundExhausted,
            connectionEventSink: evt => { lock (events) events.Add(evt); });

        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });
        await engine.FlushAsync(TimeSpan.FromSeconds(5));

        QuestDB.Senders.SenderConnectionEvent[] snapshot;
        lock (events) snapshot = events.ToArray();
        Assert.That(snapshot, Has.Some.Matches<QuestDB.Senders.SenderConnectionEvent>(
            e => e.Kind == QuestDB.Senders.SenderConnectionEventKind.EndpointAttemptFailed && e.Host == "h1"),
            "must observe ENDPOINT_ATTEMPT_FAILED for the rejected first host");
        Assert.That(snapshot, Has.Some.Matches<QuestDB.Senders.SenderConnectionEvent>(
            e => e.Kind == QuestDB.Senders.SenderConnectionEventKind.Connected && e.Host == "h2"),
            "first success on a different host than the original pick is still CONNECTED (no prior live endpoint)");
    }

    [Test]
    public async Task AllEndpointsUnreachable_FiresPerSweepWhenAllHostsRejected()
    {
        var hosts = new[] { "r1:9000", "r2:9000" };
        var tracker = new QwpHostHealthTracker(hosts);

        Func<IQwpCursorTransport> factory = () =>
        {
            var idx = tracker.PickNext();
            if (idx < 0)
            {
                tracker.BeginRound(forgetClassifications: true);
                idx = tracker.PickNext();
            }
            var stub = new MhStubTransport(rejectWithRole: QwpConstants.RoleReplicaName, hosts[idx]);
            return new QwpTrackedCursorTransport(stub, tracker, idx);
        };

        var events = new List<QuestDB.Senders.SenderConnectionEvent>();
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(5),
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(400));
        using var engine = new QwpCursorSendEngine(
            slotLock, ring, factory, policy,
            appendDeadline: TimeSpan.FromSeconds(5),
            initialConnectMode: InitialConnectMode.async,
            skipBackoffPredicate: () => !tracker.IsRoundExhausted,
            connectionEventSink: evt => { lock (events) events.Add(evt); });

        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });
        Assert.ThrowsAsync<IngressError>(async () => await engine.FlushAsync(TimeSpan.FromSeconds(2)));

        QuestDB.Senders.SenderConnectionEvent[] snapshot;
        lock (events) snapshot = events.ToArray();
        var allUnreachableCount = snapshot.Count(e =>
            e.Kind == QuestDB.Senders.SenderConnectionEventKind.AllEndpointsUnreachable);
        Assert.That(allUnreachableCount, Is.GreaterThanOrEqualTo(1),
            "every failed sweep must fire ALL_ENDPOINTS_UNREACHABLE at least once");
        Assert.That(snapshot, Has.Some.Matches<QuestDB.Senders.SenderConnectionEvent>(
            e => e.Kind == QuestDB.Senders.SenderConnectionEventKind.ReconnectBudgetExhausted),
            "budget exhaustion must surface a terminal event");
    }

    [Test]
    public async Task AllHostsReplica_ExhaustsOutageBudgetThenTerminal()
    {
        var hosts = new[] { "r1:9000", "r2:9000" };
        var tracker = new QwpHostHealthTracker(hosts);
        var attempts = 0;

        Func<IQwpCursorTransport> factory = () =>
        {
            Interlocked.Increment(ref attempts);
            var idx = tracker.PickNext();
            if (idx < 0)
            {
                tracker.BeginRound(forgetClassifications: true);
                idx = tracker.PickNext();
            }
            var stub = new MhStubTransport(rejectWithRole: QwpConstants.RoleReplicaName, hosts[idx]);
            return new QwpTrackedCursorTransport(stub, tracker, idx);
        };

        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        // Tight budget; role-rejects must consume the wall-clock outage budget so a permanent
        // REPLICA topology eventually surfaces as terminal rather than blocking forever.
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(5),
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(400));
        using var engine = new QwpCursorSendEngine(
            slotLock, ring, factory, policy,
            appendDeadline: TimeSpan.FromSeconds(5),
            initialConnectMode: InitialConnectMode.async,
            skipBackoffPredicate: () => !tracker.IsRoundExhausted);

        engine.Start();
        engine.AppendBlocking(new byte[] { 1 });

        Assert.ThrowsAsync<IngressError>(async () =>
            await engine.FlushAsync(TimeSpan.FromSeconds(2)));

        Assert.That(attempts, Is.GreaterThanOrEqualTo(hosts.Length),
            "must rotate through every host at least once before giving up");
        Assert.That(engine.IsTerminallyFailed, Is.True,
            "role-rejects consume the outage budget; a permanent REPLICA topology must terminate");
    }

    private sealed class MhStubTransport : IQwpCursorTransport
    {
        public string HostId { get; }
        public List<byte[]> Sent { get; } = new();
        public (string Host, int Port)? Endpoint { get; }

        private readonly string? _rejectWithRole;
        private readonly Channel<byte[]> _acks = Channel.CreateUnbounded<byte[]>();
        private int _autoSeq;

        public MhStubTransport(string? rejectWithRole, string hostId)
        {
            _rejectWithRole = rejectWithRole;
            HostId = hostId;
            var colon = hostId.IndexOf(':');
            Endpoint = colon < 0
                ? (hostId, 9000)
                : (hostId.Substring(0, colon), int.Parse(hostId.Substring(colon + 1)));
        }

        public Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (_rejectWithRole is { } role)
            {
                throw new QwpIngressRoleRejectedException(role, new Uri($"ws://{HostId}/write/v4"));
            }
            return Task.CompletedTask;
        }

        public async Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            Sent.Add(data.ToArray());
            var ack = new byte[11];
            ack[0] = (byte)QwpStatusCode.Ok;
            BinaryPrimitives.WriteInt64LittleEndian(ack.AsSpan(1, 8), Interlocked.Increment(ref _autoSeq) - 1);
            BinaryPrimitives.WriteUInt16LittleEndian(ack.AsSpan(9, 2), 0);
            await _acks.Writer.WriteAsync(ack, cancellationToken).ConfigureAwait(false);
        }

        public async Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken)
        {
            var ack = await _acks.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            ack.CopyTo(destination.Span);
            return ack.Length;
        }

        public Task CloseAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose() => _acks.Writer.TryComplete();
    }
}

#endif
