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
public class QwpQueryClientTests
{
    [Test]
    public void ExecuteDispatchesBatchEndToHandler()
    {
        using var fake = new FakeWebSocketChannel();
        // Pre-stage one RESULT_BATCH followed by RESULT_END.
        fake.EnqueueInboundBinary(BuildSingleLongBatch(value: 7L));
        fake.EnqueueInboundBinary(BuildResultEnd(totalRows: 1L));

        using var client = new QwpQueryClient(fake);
        var handler = new RecordingHandler();
        client.Execute("SELECT 7", handler);

        Assert.That(handler.BatchValues, Is.EqualTo(new[] { 7L }));
        Assert.That(handler.EndTotalRows, Is.EqualTo(1L));
        Assert.That(handler.ErrorStatus, Is.Null);
    }

    [Test]
    public void ExecuteWithBindsAppendsBindPayload()
    {
        using var fake = new FakeWebSocketChannel();
        fake.EnqueueInboundBinary(BuildResultEnd(totalRows: 0L));

        using var client = new QwpQueryClient(fake);
        var handler = new RecordingHandler();
        client.Execute("SELECT * FROM t WHERE x = ?",
            binds => binds.SetInt(0, 42),
            handler);

        Assert.That(handler.EndTotalRows, Is.EqualTo(0L));
        // The outbound frame should contain the bind count byte (1) followed by the
        // bind payload.
        Assert.That(fake.SentFrames, Has.Count.EqualTo(1));
        var sent = fake.SentFrames.Single();
        // QwpQueryClient builds: msg_kind(1) | request_id(8) | sql_len(varint=27) | sql(27B)
        // | initial_credit(varint=0) | bind_count(varint=1) | bind_bytes...
        // The "1" byte for bind_count is followed by TYPE_INT(0x04), 0x00 null flag, then 4-byte int LE.
        var bindStart = 1 + 8 + 1 + 27 + 1; // varint 27 is single-byte 0x1B
        Assert.That(sent[bindStart], Is.EqualTo((byte)1));   // bind_count varint
        Assert.That(sent[bindStart + 1], Is.EqualTo(QwpConstants.TYPE_INT));
        Assert.That(sent[bindStart + 2], Is.EqualTo((byte)0)); // non-null flag
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(sent.AsSpan(bindStart + 3, 4)), Is.EqualTo(42));
    }

    [Test]
    public void ExecuteForwardsExecDone()
    {
        using var fake = new FakeWebSocketChannel();
        fake.EnqueueInboundBinary(BuildExecDone(opType: 2, rowsAffected: 5L));

        using var client = new QwpQueryClient(fake);
        var handler = new RecordingHandler();
        client.Execute("CREATE TABLE t (x INT)", handler);

        Assert.That(handler.ExecOpType, Is.EqualTo((short)2));
        Assert.That(handler.ExecRowsAffected, Is.EqualTo(5L));
    }

    [Test]
    public void ExecuteForwardsQueryError()
    {
        using var fake = new FakeWebSocketChannel();
        fake.EnqueueInboundBinary(BuildQueryError(QwpConstants.STATUS_PARSE_ERROR, "syntax error"));

        using var client = new QwpQueryClient(fake);
        var handler = new RecordingHandler();
        client.Execute("THIS IS BROKEN", handler);

        Assert.That(handler.ErrorStatus, Is.EqualTo(QwpConstants.STATUS_PARSE_ERROR));
        Assert.That(handler.ErrorMessage, Is.EqualTo("syntax error"));
    }

    [Test]
    public void ExecuteSurfacesTransportErrorAsHandlerError()
    {
        using var fake = new FakeWebSocketChannel();
        fake.EnqueueInboundClose();

        using var client = new QwpQueryClient(fake);
        var handler = new RecordingHandler();
        client.Execute("SELECT 1", handler);

        Assert.That(handler.ErrorStatus, Is.Not.Null);
        Assert.That(handler.ErrorMessage, Does.Contain("server closed"));
    }

    [Test]
    public void RequestIdsAreMonotonic()
    {
        using var fake = new FakeWebSocketChannel();
        fake.EnqueueInboundBinary(BuildResultEnd(totalRows: 0L));
        fake.EnqueueInboundBinary(BuildResultEnd(totalRows: 0L));

        using var client = new QwpQueryClient(fake);
        client.Execute("SELECT 1", new RecordingHandler());
        client.Execute("SELECT 2", new RecordingHandler());
        Assert.That(client.QueriesSubmitted, Is.EqualTo(2L));

        // Inspect request_ids on the wire: bytes 1..8 of each sent frame.
        var sent = fake.SentFrames.ToArray();
        var id0 = BinaryPrimitives.ReadInt64LittleEndian(sent[0].AsSpan(1, 8));
        var id1 = BinaryPrimitives.ReadInt64LittleEndian(sent[1].AsSpan(1, 8));
        Assert.That(id1, Is.GreaterThan(id0));
    }

    [Test]
    public void DisposeShutsDownIoThread()
    {
        var fake = new FakeWebSocketChannel();
        var client = new QwpQueryClient(fake);
        client.Dispose();
        // Second dispose is a no-op.
        Assert.That(() => client.Dispose(), Throws.Nothing);
        fake.Dispose();
    }

    private sealed class RecordingHandler : IQwpColumnBatchHandler
    {
        public List<long> BatchValues { get; } = new();
        public long? EndTotalRows;
        public byte? ErrorStatus;
        public string? ErrorMessage;
        public short? ExecOpType;
        public long? ExecRowsAffected;

        public void OnBatch(QwpColumnBatch batch)
        {
            for (var r = 0; r < batch.RowCount; r++) BatchValues.Add(batch.GetLongValue(0, r));
        }

        public void OnEnd(long totalRows) { EndTotalRows = totalRows; }

        public void OnError(byte status, string? message)
        {
            ErrorStatus = status;
            ErrorMessage = message;
        }

        public void OnExecDone(short opType, long rowsAffected)
        {
            ExecOpType = opType;
            ExecRowsAffected = rowsAffected;
        }
    }

    private static byte[] BuildSingleLongBatch(long value)
    {
        var w = new TestPayloadWriter();
        WriteHeader(w);
        w.WriteByte(QwpEgressMsgKind.RESULT_BATCH);
        w.WriteLongLE(0L);
        w.WriteVarint(0);
        w.WriteVarint(0);
        w.WriteVarint(1);
        w.WriteVarint(1);
        w.WriteByte(QwpConstants.SCHEMA_MODE_FULL);
        w.WriteVarint(0);
        w.WriteVarint(1);
        w.WriteUtf8("v");
        w.WriteByte(QwpConstants.TYPE_LONG);
        w.WriteByte(0);
        w.WriteLongLE(value);
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

    private static byte[] BuildExecDone(byte opType, long rowsAffected)
    {
        var w = new TestPayloadWriter();
        WriteHeader(w);
        w.WriteByte(QwpEgressMsgKind.EXEC_DONE);
        w.WriteLongLE(0L);
        w.WriteByte(opType);
        w.WriteVarint(rowsAffected);
        return w.ToArray();
    }

    private static byte[] BuildQueryError(byte status, string message)
    {
        var w = new TestPayloadWriter();
        WriteHeader(w);
        w.WriteByte(QwpEgressMsgKind.QUERY_ERROR);
        w.WriteLongLE(0L);
        w.WriteByte(status);
        var msgBytes = Encoding.UTF8.GetBytes(message);
        w.WriteByte((byte)msgBytes.Length);
        w.WriteByte((byte)(msgBytes.Length >> 8));
        w.WriteRaw(msgBytes);
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
        public void WriteUtf8(string s) => _bytes.AddRange(Encoding.UTF8.GetBytes(s));
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
