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

using System.Buffers.Binary;
using System.Text;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Qwp.Egress;
using net_questdb_client_tests.Qwp.Client;

namespace net_questdb_client_tests.Qwp.Egress;

[TestFixture]
public class QwpQueryClientFailoverTests
{
    private static readonly QwpFailoverOptions FastFailoverOptions = new(
        Enabled: true,
        MaxAttempts: 4,
        InitialBackoffMs: 0,
        MaxBackoffMs: 0,
        TargetRole: QwpTargetRole.Any);

    [Test]
    public void ReconnectsToSecondEndpointOnTransportFailure()
    {
        var endpoints = new[]
        {
            new QwpEndpoint("primary-a", 9000),
            new QwpEndpoint("primary-b", 9000),
        };

        var fakeA = new FakeWebSocketChannel();
        fakeA.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_PRIMARY, "node-a"));
        fakeA.EnqueueInboundClose(); // First execute attempt sees server-close = transport error.

        var fakeB = new FakeWebSocketChannel();
        fakeB.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_PRIMARY, "node-b"));
        fakeB.EnqueueInboundBinary(BuildResultEnd(totalRows: 42L));

        var factory = MultiFactory(endpoints, new[] { fakeA, fakeB });

        using var client = new QwpQueryClient(endpoints, factory, FastFailoverOptions);
        var handler = new RecordingHandler();
        client.Execute("SELECT 1", handler);

        Assert.That(handler.EndTotalRows, Is.EqualTo(42L));
        Assert.That(handler.FailoverResetNodeIds, Is.EqualTo(new[] { "node-b" }));
        Assert.That(handler.ErrorStatus, Is.Null);
        Assert.That(client.CurrentEndpoint, Is.EqualTo((QwpEndpoint?)endpoints[1]));

        fakeA.Dispose();
        fakeB.Dispose();
    }

    [Test]
    public void AllEndpointsExhaustedSurfacesHandlerError()
    {
        var endpoints = new[]
        {
            new QwpEndpoint("a", 9000),
            new QwpEndpoint("b", 9000),
        };

        var fakeA = new FakeWebSocketChannel();
        fakeA.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_PRIMARY, "node-a"));
        fakeA.EnqueueInboundClose();

        var fakeB = new FakeWebSocketChannel();
        fakeB.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_PRIMARY, "node-b"));
        fakeB.EnqueueInboundClose();

        // Use a factory that on subsequent calls hands out fresh transport-broken channels.
        var spent = new HashSet<int>();
        Task<IWebSocketChannel> Factory(QwpEndpoint ep, CancellationToken ct)
        {
            var idx = Array.IndexOf(endpoints, ep);
            if (spent.Contains(idx))
            {
                throw new InvalidOperationException($"endpoint {ep} unreachable");
            }
            spent.Add(idx);
            return Task.FromResult<IWebSocketChannel>(idx == 0 ? fakeA : fakeB);
        }

        // MaxAttempts=2 → one initial connect (fakeA) then one failover (fakeB), then exhaustion.
        var opts = FastFailoverOptions with { MaxAttempts = 2 };
        using var client = new QwpQueryClient(endpoints, Factory, opts);
        var handler = new RecordingHandler();
        client.Execute("SELECT 1", handler);

        Assert.That(handler.ErrorStatus, Is.EqualTo(QwpConstants.STATUS_INTERNAL_ERROR));
        Assert.That(handler.ErrorMessage, Does.Contain("server closed"));
        fakeA.Dispose();
        fakeB.Dispose();
    }

    [Test]
    public void FailoverDisabledSurfacesTransportErrorImmediately()
    {
        var endpoints = new[] { new QwpEndpoint("a", 9000) };

        var fake = new FakeWebSocketChannel();
        fake.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_PRIMARY, "node-a"));
        fake.EnqueueInboundClose();

        var factory = MultiFactory(endpoints, new[] { fake });

        var opts = QwpFailoverOptions.Disabled;
        using var client = new QwpQueryClient(endpoints, factory, opts);
        var handler = new RecordingHandler();
        client.Execute("SELECT 1", handler);

        Assert.That(handler.ErrorStatus, Is.Not.Null);
        Assert.That(handler.ErrorMessage, Does.Contain("server closed"));
        Assert.That(handler.FailoverResetNodeIds, Is.Empty);
        fake.Dispose();
    }

    [Test]
    public void TargetRolePrimary_SkipsReplicaEndpoint()
    {
        var endpoints = new[]
        {
            new QwpEndpoint("replica", 9000),
            new QwpEndpoint("primary", 9000),
        };

        var fakeReplica = new FakeWebSocketChannel();
        fakeReplica.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_REPLICA, "replica-node"));

        var fakePrimary = new FakeWebSocketChannel();
        fakePrimary.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_PRIMARY, "primary-node"));
        fakePrimary.EnqueueInboundBinary(BuildResultEnd(totalRows: 1L));

        var factory = MultiFactory(endpoints, new[] { fakeReplica, fakePrimary });
        var opts = FastFailoverOptions with { TargetRole = QwpTargetRole.Primary };

        using var client = new QwpQueryClient(endpoints, factory, opts);
        Assert.That(client.CurrentEndpoint, Is.EqualTo((QwpEndpoint?)endpoints[1]));

        var handler = new RecordingHandler();
        client.Execute("SELECT 1", handler);
        Assert.That(handler.EndTotalRows, Is.EqualTo(1L));
        fakeReplica.Dispose();
        fakePrimary.Dispose();
    }

    [Test]
    public void TargetRolePrimary_ThrowsRoleMismatchWhenAllEndpointsReplica()
    {
        var endpoints = new[]
        {
            new QwpEndpoint("r1", 9000),
            new QwpEndpoint("r2", 9000),
        };

        var fake1 = new FakeWebSocketChannel();
        fake1.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_REPLICA, "r1"));
        var fake2 = new FakeWebSocketChannel();
        fake2.EnqueueInboundBinary(BuildServerInfo(QwpEgressMsgKind.ROLE_REPLICA, "r2"));

        var factory = MultiFactory(endpoints, new[] { fake1, fake2 });
        var opts = FastFailoverOptions with { TargetRole = QwpTargetRole.Primary, MaxAttempts = 1 };

        var ex = Assert.Throws<QwpRoleMismatchException>(() => _ = new QwpQueryClient(endpoints, factory, opts));
        Assert.That(ex!.LastObserved, Is.Not.Null);
        Assert.That(ex.Message, Does.Contain("Primary"));
        fake1.Dispose();
        fake2.Dispose();
    }

    private static Func<QwpEndpoint, CancellationToken, Task<IWebSocketChannel>> MultiFactory(
        IReadOnlyList<QwpEndpoint> endpoints, IReadOnlyList<FakeWebSocketChannel> channels)
    {
        return (ep, _) =>
        {
            var idx = endpoints.ToList().IndexOf(ep);
            if (idx < 0 || idx >= channels.Count)
            {
                throw new InvalidOperationException($"no fake channel for {ep}");
            }
            return Task.FromResult<IWebSocketChannel>(channels[idx]);
        };
    }

    private sealed class RecordingHandler : IQwpColumnBatchHandler
    {
        public long? EndTotalRows;
        public byte? ErrorStatus;
        public string? ErrorMessage;
        public List<string> FailoverResetNodeIds { get; } = new();

        public void OnBatch(QwpColumnBatch batch) { }

        public void OnEnd(long totalRows) => EndTotalRows = totalRows;

        public void OnError(byte status, string? message)
        {
            ErrorStatus = status;
            ErrorMessage = message;
        }

        public void OnFailoverReset(QwpServerInfo? newNode)
        {
            FailoverResetNodeIds.Add(newNode?.NodeId ?? "<v1>");
        }
    }

    private static byte[] BuildServerInfo(byte role, string nodeId)
    {
        var w = new TestPayloadWriter();
        WriteHeader(w);
        w.WriteByte(QwpEgressMsgKind.SERVER_INFO);
        w.WriteByte(role);
        w.WriteLongLE(0L);            // epoch
        w.WriteIntLE(0);              // capabilities
        w.WriteLongLE(0L);            // serverWallNs
        w.WriteByte(0); w.WriteByte(0); // clusterId len = 0
        var nodeBytes = Encoding.UTF8.GetBytes(nodeId);
        w.WriteByte((byte)nodeBytes.Length);
        w.WriteByte((byte)(nodeBytes.Length >> 8));
        w.WriteRaw(nodeBytes);
        return w.ToArray();
    }

    private static byte[] BuildResultEnd(long totalRows)
    {
        var w = new TestPayloadWriter();
        WriteHeader(w);
        w.WriteByte(QwpEgressMsgKind.RESULT_END);
        w.WriteLongLE(0L);
        w.WriteVarint(0L);
        w.WriteVarint(totalRows);
        return w.ToArray();
    }

    private static void WriteHeader(TestPayloadWriter w)
    {
        w.WriteIntLE(QwpConstants.MAGIC_MESSAGE);
        w.WriteByte(QwpConstants.VERSION_2);
        w.WriteByte(0);
        for (var i = 0; i < QwpConstants.HEADER_SIZE - 6; i++) w.WriteByte(0);
    }

    private sealed class TestPayloadWriter
    {
        private readonly List<byte> _bytes = new();
        public void WriteByte(byte v) => _bytes.Add(v);
        public void WriteRaw(byte[] data) => _bytes.AddRange(data);
        public void WriteIntLE(int v)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(b, v);
            for (var i = 0; i < 4; i++) _bytes.Add(b[i]);
        }
        public void WriteLongLE(long v)
        {
            Span<byte> b = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(b, v);
            for (var i = 0; i < 8; i++) _bytes.Add(b[i]);
        }
        public void WriteVarint(long value)
        {
            var v = (ulong)value;
            while (v > 0x7F) { _bytes.Add((byte)((v & 0x7F) | 0x80)); v >>= 7; }
            _bytes.Add((byte)v);
        }
        public byte[] ToArray() => _bytes.ToArray();
    }
}
