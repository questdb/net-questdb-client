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

namespace net_questdb_client_tests.Qwp.Egress;

[TestFixture]
public class QwpServerInfoDecoderTests
{
    [Test]
    public void DecodesValidFrame()
    {
        var bytes = BuildFrame(
            role: QwpEgressMsgKind.ROLE_PRIMARY,
            epoch: unchecked((long)0xDEAD_BEEF_CAFE_F00DUL),
            capabilities: 0x000F_00FF,
            serverWallNs: 1_700_000_000_000_000_000L,
            clusterId: "prod-eu-1",
            nodeId: "node-7");

        var info = QwpServerInfoDecoder.Decode(bytes);
        Assert.That(info.Role, Is.EqualTo(QwpEgressMsgKind.ROLE_PRIMARY));
        Assert.That(info.Epoch, Is.EqualTo(unchecked((long)0xDEAD_BEEF_CAFE_F00DUL)));
        Assert.That(info.Capabilities, Is.EqualTo(0x000F_00FF));
        Assert.That(info.ServerWallNs, Is.EqualTo(1_700_000_000_000_000_000L));
        Assert.That(info.ClusterId, Is.EqualTo("prod-eu-1"));
        Assert.That(info.NodeId, Is.EqualTo("node-7"));
    }

    [Test]
    public void DecodesEmptyStrings()
    {
        var bytes = BuildFrame(
            role: QwpEgressMsgKind.ROLE_STANDALONE,
            epoch: 0L,
            capabilities: 0,
            serverWallNs: 0L,
            clusterId: string.Empty,
            nodeId: string.Empty);

        var info = QwpServerInfoDecoder.Decode(bytes);
        Assert.That(info.ClusterId, Is.EqualTo(string.Empty));
        Assert.That(info.NodeId, Is.EqualTo(string.Empty));
    }

    [Test]
    public void DecodesUtf8MultiByte()
    {
        // Cyrillic + emoji to exercise the UTF-8 path.
        var bytes = BuildFrame(
            role: QwpEgressMsgKind.ROLE_REPLICA,
            epoch: 1L,
            capabilities: 0,
            serverWallNs: 0L,
            clusterId: "кластер",
            nodeId: "node-🚀");

        var info = QwpServerInfoDecoder.Decode(bytes);
        Assert.That(info.ClusterId, Is.EqualTo("кластер"));
        Assert.That(info.NodeId, Is.EqualTo("node-🚀"));
    }

    [Test]
    public void RejectsTruncatedFixedSection()
    {
        var bytes = new byte[QwpConstants.HEADER_SIZE + 4]; // not even enough for the fixed prefix
        Assert.That(() => QwpServerInfoDecoder.Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("truncated"));
    }

    [Test]
    public void RejectsWrongMsgKind()
    {
        var bytes = BuildFrame(
            role: QwpEgressMsgKind.ROLE_PRIMARY,
            epoch: 1L,
            capabilities: 0,
            serverWallNs: 0L,
            clusterId: "x",
            nodeId: "y");
        // Stomp the msg-kind byte just after the header.
        bytes[QwpConstants.HEADER_SIZE] = QwpEgressMsgKind.RESULT_BATCH;

        Assert.That(() => QwpServerInfoDecoder.Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("expected SERVER_INFO"));
    }

    [Test]
    public void RejectsClusterIdLengthOverrun()
    {
        var bytes = BuildFrame(
            role: QwpEgressMsgKind.ROLE_PRIMARY,
            epoch: 1L,
            capabilities: 0,
            serverWallNs: 0L,
            clusterId: "abc",
            nodeId: "y");
        // cluster_id length sits at HEADER + 1 + 1 + 8 + 4 + 8 = HEADER + 22.
        var clusterLenOffset = QwpConstants.HEADER_SIZE + 1 + 1 + 8 + 4 + 8;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(clusterLenOffset, 2), 999);

        Assert.That(() => QwpServerInfoDecoder.Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("cluster_id"));
    }

    [Test]
    public void RejectsTruncationBeforeNodeIdLength()
    {
        var bytes = BuildFrame(
            role: QwpEgressMsgKind.ROLE_PRIMARY,
            epoch: 1L,
            capabilities: 0,
            serverWallNs: 0L,
            clusterId: "abc",
            nodeId: "y");
        // Strip enough bytes that we're past the cluster_id but missing the node_id length.
        var truncated = bytes.AsSpan(0, bytes.Length - 1 - 1).ToArray(); // chop nodeId data + 1 byte of length
        Assert.That(() => QwpServerInfoDecoder.Decode(truncated),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("node_id"));
    }

    private static byte[] BuildFrame(
        byte role,
        long epoch,
        int capabilities,
        long serverWallNs,
        string clusterId,
        string nodeId)
    {
        var clusterBytes = Encoding.UTF8.GetBytes(clusterId);
        var nodeBytes = Encoding.UTF8.GetBytes(nodeId);
        var size = QwpConstants.HEADER_SIZE + 1 + 1 + 8 + 4 + 8 + 2 + clusterBytes.Length + 2 + nodeBytes.Length;
        var buf = new byte[size];
        // Header bytes are not inspected by the decoder; leave them zero.
        var p = QwpConstants.HEADER_SIZE;
        buf[p++] = QwpEgressMsgKind.SERVER_INFO;
        buf[p++] = role;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(p, 8), epoch); p += 8;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(p, 4), capabilities); p += 4;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(p, 8), serverWallNs); p += 8;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(p, 2), (ushort)clusterBytes.Length); p += 2;
        Array.Copy(clusterBytes, 0, buf, p, clusterBytes.Length); p += clusterBytes.Length;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(p, 2), (ushort)nodeBytes.Length); p += 2;
        Array.Copy(nodeBytes, 0, buf, p, nodeBytes.Length); p += nodeBytes.Length;
        return buf;
    }
}
