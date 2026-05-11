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

using System.Buffers.Binary;
using System.Threading.Channels;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpCursorEngineDurableModeTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qdb-da-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch { }
    }

    [Test]
    public async Task Ok_InDurableMode_DoesNotAdvanceTrim()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 3);
        await transport.WaitSent(3);

        transport.Push(OkAck(seq: 0, ("trades", 10)));
        transport.Push(OkAck(seq: 1, ("trades", 20)));
        transport.Push(OkAck(seq: 2, ("trades", 30)));

        // No durable-ack issued ⇒ trim must stay put.
        await Task.Delay(80);
        Assert.That(engine.AckedFsn, Is.EqualTo(0L), "OK frames alone must not advance trim in durable mode");
    }

    [Test]
    public async Task DurableAck_CoversAllPending_AdvancesTrimToTail()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 3);
        await transport.WaitSent(3);

        transport.Push(OkAck(seq: 0, ("trades", 10)));
        transport.Push(OkAck(seq: 1, ("trades", 20)));
        transport.Push(OkAck(seq: 2, ("trades", 30)));
        transport.Push(DurableAck(("trades", 30)));

        await WaitFor(() => engine.AckedFsn == 3, "trim advanced to all 3 frames");
    }

    [Test]
    public async Task DurableAck_PartialWatermark_AdvancesOnlyCoveredPrefix()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 3);
        await transport.WaitSent(3);

        transport.Push(OkAck(seq: 0, ("trades", 10)));
        transport.Push(OkAck(seq: 1, ("trades", 20)));
        transport.Push(OkAck(seq: 2, ("trades", 30)));
        // Watermark only covers wireSeq 0 and 1, not 2.
        transport.Push(DurableAck(("trades", 20)));

        await WaitFor(() => engine.AckedFsn == 2, "trim advanced to covered prefix only");
        Assert.That(engine.AckedFsn, Is.EqualTo(2L));
    }

    [Test]
    public async Task DurableAck_CatchUpWatermark_DrainsRemainingPrefix()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 4);
        await transport.WaitSent(4);

        transport.Push(OkAck(seq: 0, ("trades", 10)));
        transport.Push(OkAck(seq: 1, ("trades", 20)));
        transport.Push(OkAck(seq: 2, ("trades", 30)));
        transport.Push(OkAck(seq: 3, ("trades", 40)));
        transport.Push(DurableAck(("trades", 20)));
        await WaitFor(() => engine.AckedFsn == 2, "covered prefix [0, 1]");

        transport.Push(DurableAck(("trades", 40)));
        await WaitFor(() => engine.AckedFsn == 4, "tail covered, full drain");
    }

    [Test]
    public async Task EmptyOk_TriviallyDurableAtHead_PopsImmediately()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 1);
        await transport.WaitSent(1);

        // tableCount = 0: no per-table commit to wait for; trims immediately on enqueue.
        transport.Push(OkAck(seq: 0));

        await WaitFor(() => engine.AckedFsn == 1, "trivially durable empty OK pops at head");
    }

    [Test]
    public async Task EmptyOk_BehindUncoveredEntry_WaitsThenPops()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 2);
        await transport.WaitSent(2);

        transport.Push(OkAck(seq: 0, ("trades", 10)));   // needs watermark
        transport.Push(OkAck(seq: 1));                    // trivially durable
        await Task.Delay(50);
        Assert.That(engine.AckedFsn, Is.EqualTo(0L), "head not yet covered, FIFO stalls behind it");

        transport.Push(DurableAck(("trades", 10)));
        await WaitFor(() => engine.AckedFsn == 2, "head covered ⇒ both entries pop in order");
    }

    [Test]
    public async Task MultiTable_AllTablesMustReachWatermark()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 1);
        await transport.WaitSent(1);

        transport.Push(OkAck(seq: 0, ("trades", 100), ("fills", 200)));
        transport.Push(DurableAck(("trades", 100)));
        await Task.Delay(50);
        Assert.That(engine.AckedFsn, Is.EqualTo(0L), "fills not yet covered");

        transport.Push(DurableAck(("fills", 200)));
        await WaitFor(() => engine.AckedFsn == 1, "both tables now covered");
    }

    [Test]
    public async Task DurableWatermark_MaxMerge_ToleratesReordering()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: true);
        engine.Start();

        AppendFrames(engine, 1);
        await transport.WaitSent(1);

        transport.Push(OkAck(seq: 0, ("trades", 100)));
        transport.Push(DurableAck(("trades", 100)));
        await WaitFor(() => engine.AckedFsn == 1);

        // A stale durable-ack for an older watermark MUST NOT regress state.
        transport.Push(DurableAck(("trades", 50)));
        await Task.Delay(50);
        Assert.That(engine.AckedFsn, Is.EqualTo(1L));
    }

    [Test]
    public async Task NonDurableMode_OkAdvancesTrim_LegacyPath()
    {
        var transport = new ScriptedTransport();
        using var engine = NewEngine(out _, transport, durableAckMode: false);
        engine.Start();

        AppendFrames(engine, 2);
        await transport.WaitSent(2);

        transport.Push(OkAck(seq: 0));
        transport.Push(OkAck(seq: 1));

        await WaitFor(() => engine.AckedFsn == 2, "non-durable mode trims on OK directly");
    }

    private QwpCursorSendEngine NewEngine(
        out string slotDir,
        ScriptedTransport transport,
        bool durableAckMode)
    {
        slotDir = Path.Combine(_root, "engine-" + Guid.NewGuid().ToString("N"));
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromSeconds(5));
        return new QwpCursorSendEngine(
            slotLock, ring, () => transport, policy,
            appendDeadline: TimeSpan.FromSeconds(5),
            initialConnectMode: InitialConnectMode.off,
            durableAckMode: durableAckMode);
    }

    private static void AppendFrames(QwpCursorSendEngine engine, int count)
    {
        Span<byte> payload = stackalloc byte[8];
        for (var i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload, i);
            engine.AppendBlocking(payload);
        }
    }

    private static byte[] OkAck(long seq, params (string Name, long SeqTxn)[] entries)
    {
        var size = 11;
        foreach (var e in entries) size += 2 + System.Text.Encoding.UTF8.GetByteCount(e.Name) + 8;

        var buf = new byte[size];
        buf[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), seq);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), (ushort)entries.Length);

        var pos = 11;
        foreach (var e in entries)
        {
            var name = System.Text.Encoding.UTF8.GetBytes(e.Name);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos, 2), (ushort)name.Length);
            pos += 2;
            name.CopyTo(buf, pos);
            pos += name.Length;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos, 8), e.SeqTxn);
            pos += 8;
        }
        return buf;
    }

    private static byte[] DurableAck(params (string Name, long SeqTxn)[] entries)
    {
        var size = 3;
        foreach (var e in entries) size += 2 + System.Text.Encoding.UTF8.GetByteCount(e.Name) + 8;

        var buf = new byte[size];
        buf[0] = (byte)QwpStatusCode.DurableAck;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(1, 2), (ushort)entries.Length);

        var pos = 3;
        foreach (var e in entries)
        {
            var name = System.Text.Encoding.UTF8.GetBytes(e.Name);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos, 2), (ushort)name.Length);
            pos += 2;
            name.CopyTo(buf, pos);
            pos += name.Length;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos, 8), e.SeqTxn);
            pos += 8;
        }
        return buf;
    }

    private static async Task WaitFor(Func<bool> condition, string? message = null, int timeoutMs = 2000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(5);
        }
        Assert.Fail(message ?? "condition never held");
    }

    // Transport that captures every frame the engine sends and lets the test push arbitrary
    // response bytes to the receive pump in any order (including durable-ack frames interleaved
    // between OKs).
    private sealed class ScriptedTransport : IQwpCursorTransport
    {
        private readonly Channel<byte[]> _sent = Channel.CreateUnbounded<byte[]>();
        private readonly Channel<byte[]> _responses = Channel.CreateUnbounded<byte[]>();

        public Task ConnectAsync(CancellationToken ct) => Task.CompletedTask;

        public async Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
        {
            await _sent.Writer.WriteAsync(data.ToArray(), ct).ConfigureAwait(false);
        }

        public async Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken ct)
        {
            var bytes = await _responses.Reader.ReadAsync(ct).ConfigureAwait(false);
            bytes.CopyTo(destination.Span);
            return bytes.Length;
        }

        public Task CloseAsync(CancellationToken ct) => Task.CompletedTask;

        public void Dispose() => _responses.Writer.TryComplete();

        public async Task WaitSent(int count, int timeoutMs = 2000)
        {
            using var cts = new CancellationTokenSource(timeoutMs);
            for (var i = 0; i < count; i++)
            {
                await _sent.Reader.ReadAsync(cts.Token).ConfigureAwait(false);
            }
        }

        public void Push(byte[] frame) => _responses.Writer.TryWrite(frame);
    }
}

#endif
