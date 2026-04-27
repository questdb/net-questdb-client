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
public class QwpEgressIoThreadTests
{
    [Test]
    public async Task SendsQueryFrameAndReceivesBatch()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 2);
        io.Start();

        // Pre-bake an inbound RESULT_BATCH carrying a single LONG column with one row.
        var inbound = BuildSingleLongBatch(value: 42L);
        fake.EnqueueInboundBinary(inbound);

        // Submit a placeholder request frame; FakeWebSocketChannel just records what was sent.
        var request = new QueryRequest(requestId: 7, encodedFrame: new byte[] { 0xCA, 0xFE });
        await io.SubmitQueryAsync(request);

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_BATCH));
        Assert.That(ev.Buffer, Is.Not.Null);
        Assert.That(ev.Buffer!.Batch.GetLongValue(0, 0), Is.EqualTo(42L));

        // Hand the buffer back; the IO thread is free to reuse it.
        io.ReleaseBuffer(ev.Buffer);
        io.ReleaseEvent(ev);

        // The submit should have produced one outbound binary frame.
        Assert.That(fake.SentFrames, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task ResultEndEmitsKindEnd()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 1);
        io.Start();

        fake.EnqueueInboundBinary(BuildResultEnd(finalSeq: 0, totalRows: 1234L));
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_END));
        Assert.That(ev.TotalRows, Is.EqualTo(1234L));
    }

    [Test]
    public async Task ExecDoneEmitsKindExecDone()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 1);
        io.Start();

        fake.EnqueueInboundBinary(BuildExecDone(opType: 3, rowsAffected: 99L));
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_EXEC_DONE));
        Assert.That(ev.OpType, Is.EqualTo((short)3));
        Assert.That(ev.RowsAffected, Is.EqualTo(99L));
    }

    [Test]
    public async Task QueryErrorEmitsKindError()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 1);
        io.Start();

        fake.EnqueueInboundBinary(BuildQueryError(QwpConstants.STATUS_PARSE_ERROR, "syntax error"));
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_ERROR));
        Assert.That(ev.ErrorStatus, Is.EqualTo(QwpConstants.STATUS_PARSE_ERROR));
        Assert.That(ev.ErrorMessage, Is.EqualTo("syntax error"));
    }

    [Test]
    public async Task ServerCloseEmitsTransportError()
    {
        using var fake = new FakeWebSocketChannel();
        var notifications = new List<string>();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 1,
            terminalFailureListener: notifications.Add);
        io.Start();

        fake.EnqueueInboundClose();
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_TRANSPORT_ERROR));
        Assert.That(ev.ErrorMessage, Does.Contain("server closed"));
        Assert.That(notifications, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task DispatchesMultipleBatchesBackToBack()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 4);
        io.Start();

        for (var i = 0; i < 3; i++)
        {
            fake.EnqueueInboundBinary(BuildSingleLongBatch(value: 100L + i));
        }
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));

        var values = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            var ev = io.TakeEvent();
            Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_BATCH));
            values.Add(ev.Buffer!.Batch.GetLongValue(0, 0));
            io.ReleaseBuffer(ev.Buffer);
            io.ReleaseEvent(ev);
        }
        Assert.That(values, Is.EqualTo(new[] { 100L, 101L, 102L }));
    }

    [Test]
    public async Task TruncatedFrameEmitsTransportError()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 1);
        io.Start();

        // Frame just header bytes, no msg-kind — fails the early bounds check.
        var truncated = new byte[QwpConstants.HEADER_SIZE];
        BinaryPrimitives.WriteInt32LittleEndian(truncated, QwpConstants.MAGIC_MESSAGE);
        truncated[4] = QwpConstants.VERSION_2;
        fake.EnqueueInboundBinary(truncated);
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_TRANSPORT_ERROR));
        Assert.That(ev.ErrorMessage, Does.Contain("truncated"));
    }

    [Test]
    public async Task UnknownMsgKindEmitsTransportError()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 1);
        io.Start();

        var bytes = new byte[QwpConstants.HEADER_SIZE + 1];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, QwpConstants.MAGIC_MESSAGE);
        bytes[4] = QwpConstants.VERSION_2;
        bytes[QwpConstants.HEADER_SIZE] = 0xEE;
        fake.EnqueueInboundBinary(bytes);
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_TRANSPORT_ERROR));
        Assert.That(ev.ErrorMessage, Does.Contain("unknown msg_kind"));
    }

    [Test]
    public async Task ShutdownDuringInFlightQueryEmitsTransportError()
    {
        using var fake = new FakeWebSocketChannel();
        using var io = new QwpEgressIoThread(fake, bufferPoolSize: 1);
        io.Start();

        // Submit a query and shut down before any inbound frame arrives.
        await io.SubmitQueryAsync(new QueryRequest(1, new byte[] { 0x00 }));
        // Give the IO thread a moment to enter the receive loop.
        await Task.Delay(50);
        io.Shutdown();

        var ev = io.TakeEvent();
        Assert.That(ev.Kind, Is.EqualTo(QueryEvent.KIND_TRANSPORT_ERROR));
        Assert.That(ev.ErrorMessage, Does.Contain("shut down"));
    }

    private static byte[] BuildSingleLongBatch(long value)
    {
        // RESULT_BATCH with one column "v" (LONG), one row, no nulls, value = `value`.
        var w = new TestPayloadWriter();
        WriteHeader(w);
        w.WriteByte(QwpEgressMsgKind.RESULT_BATCH);
        w.WriteLongLE(0L);    // request_id
        w.WriteVarint(0);     // batch_seq
        w.WriteVarint(0);     // table name length (empty)
        w.WriteVarint(1);     // row_count
        w.WriteVarint(1);     // column_count
        w.WriteByte(QwpConstants.SCHEMA_MODE_FULL);
        w.WriteVarint(0);     // schema_id
        // Column def: name "v", wire type LONG.
        w.WriteVarint(1);
        w.WriteUtf8("v");
        w.WriteByte(QwpConstants.TYPE_LONG);
        // Per-column wire bytes: null flag = 0 (no nulls), then values.
        w.WriteByte(0);
        w.WriteLongLE(value);
        return w.ToArray();
    }

    private static byte[] BuildResultEnd(long finalSeq, long totalRows)
    {
        var w = new TestPayloadWriter();
        WriteHeader(w);
        w.WriteByte(QwpEgressMsgKind.RESULT_END);
        w.WriteLongLE(0L);
        w.WriteVarint(finalSeq);
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
        w.WriteByte(0); // flags
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
