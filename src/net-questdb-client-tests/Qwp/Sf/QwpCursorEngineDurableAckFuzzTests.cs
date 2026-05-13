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

/// <summary>
///     Mirrors java-questdb-client's <c>CursorWebSocketSendLoopDurableAckFuzzTest</c>. Generates
///     randomised OK + DURABLE_ACK sequences over a small table set and verifies the spec
///     invariant from <c>sf-client.md</c> §10.2: the engine's trim watermark equals the longest
///     contiguous prefix of pending wireSeqs whose every <c>(table, seqTxn)</c> is covered by the
///     accumulated durable watermarks (with empty entries trivially covered).
/// </summary>
[TestFixture]
public class QwpCursorEngineDurableAckFuzzTests
{
    private const int Iterations = 200;
    private const int MaxFramesPerIteration = 48;
    private static readonly string[] TablePool = { "trades", "orders", "fills", "positions" };

    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qdb-da-fuzz-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch { }
    }

    [Test]
    public async Task DurableTrimInvariant_HoldsAcrossRandomSequences()
    {
        var seed = unchecked((int)(DateTime.UtcNow.Ticks ^ Environment.TickCount));
        var rnd = new Random(seed);
        try
        {
            for (var iter = 0; iter < Iterations; iter++)
            {
                await RunOneIteration(rnd, iter);
            }
        }
        catch (Exception ex)
        {
            throw new AssertionException($"fuzz failure with seed={seed}", ex);
        }
    }

    private async Task RunOneIteration(Random rnd, int iteration)
    {
        var frameCount = 1 + rnd.Next(MaxFramesPerIteration);
        var frames = new FrameSpec[frameCount];
        var tableCursors = new Dictionary<string, long>(StringComparer.Ordinal);

        for (var i = 0; i < frameCount; i++)
        {
            // Roughly 1-in-12 chance of an empty OK (matches the "materialized-view-only batch"
            // edge case from the spec — trivially durable).
            if (rnd.Next(12) == 0)
            {
                frames[i] = new FrameSpec(WireSeq: i, Array.Empty<(string, long)>());
                continue;
            }

            var nTables = 1 + rnd.Next(TablePool.Length);
            var picks = PickDistinct(rnd, TablePool, nTables);
            var entries = new (string, long)[picks.Length];
            for (var k = 0; k < picks.Length; k++)
            {
                if (!tableCursors.TryGetValue(picks[k], out var cur)) cur = 0;
                cur += 1 + rnd.Next(5);
                tableCursors[picks[k]] = cur;
                entries[k] = (picks[k], cur);
            }
            frames[i] = new FrameSpec(WireSeq: i, entries);
        }

        // Build the script: OKs in strict wireSeq order, with DURABLE_ACK frames inserted at
        // random positions. Each DURABLE_ACK carries a per-table watermark that lags the
        // already-emitted OK frames by a random amount in [0, 3].
        var events = new List<ResponseEvent>();
        var emittedPerTable = new Dictionary<string, long>(StringComparer.Ordinal);
        for (var i = 0; i < frameCount; i++)
        {
            events.Add(ResponseEvent.Ok(frames[i]));
            foreach (var (name, seqTxn) in frames[i].Entries)
            {
                if (!emittedPerTable.TryGetValue(name, out var prev) || seqTxn > prev)
                    emittedPerTable[name] = seqTxn;
            }

            // ~40% chance to inject a durable-ack at each tick.
            if (rnd.Next(10) < 4 && emittedPerTable.Count > 0)
            {
                var watermarks = SampleWatermarks(rnd, emittedPerTable);
                if (watermarks.Length > 0) events.Add(ResponseEvent.DurableAck(watermarks));
            }
        }

        // Final durable-ack guarantees the full set is eventually covered so the trim must reach
        // the tail; without it the oracle would correctly report a smaller AckedFsn.
        if (emittedPerTable.Count > 0)
        {
            var final = new (string, long)[emittedPerTable.Count];
            var k = 0;
            foreach (var kv in emittedPerTable) final[k++] = (kv.Key, kv.Value);
            events.Add(ResponseEvent.DurableAck(final));
        }

        // Run the script against the engine and check the oracle.
        var slotDir = Path.Combine(_root, $"iter-{iteration:D5}");
        var transport = new ScriptedTransport();
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromSeconds(5));
        using (var engine = new QwpCursorSendEngine(
                   slotLock, ring, () => transport, policy,
                   appendDeadline: TimeSpan.FromSeconds(5),
                   initialConnectMode: InitialConnectMode.off,
                   durableAckMode: true))
        {
            engine.Start();

            Span<byte> payload = stackalloc byte[8];
            for (var i = 0; i < frameCount; i++)
            {
                BinaryPrimitives.WriteInt64LittleEndian(payload, i);
                engine.AppendBlocking(payload);
            }

            await transport.WaitSent(frameCount);

            foreach (var ev in events)
            {
                transport.Push(ev.IsOk
                    ? OkAck(ev.Frame!.WireSeq, ev.Frame.Entries)
                    : DurableAck(ev.Watermarks!));
            }

            var expectedAcked = ComputeOracle(frames, events);
            await WaitFor(() => engine.AckedFsn == expectedAcked, 3000,
                $"iter={iteration} frames={frameCount} expected AckedFsn={expectedAcked} got {{actual}}");
        }
    }

    private static long ComputeOracle(FrameSpec[] frames, List<ResponseEvent> events)
    {
        var watermarks = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var ev in events)
        {
            if (ev.IsOk) continue;
            foreach (var (name, seqTxn) in ev.Watermarks!)
            {
                if (!watermarks.TryGetValue(name, out var prev) || seqTxn > prev)
                    watermarks[name] = seqTxn;
            }
        }

        var coveredPrefix = -1L;
        foreach (var f in frames)
        {
            if (!CoversFrame(watermarks, f)) break;
            coveredPrefix = f.WireSeq;
        }
        return coveredPrefix + 1;
    }

    private static bool CoversFrame(Dictionary<string, long> watermarks, FrameSpec f)
    {
        if (f.Entries.Length == 0) return true;
        foreach (var (name, seqTxn) in f.Entries)
        {
            if (!watermarks.TryGetValue(name, out var w) || w < seqTxn) return false;
        }
        return true;
    }

    private static string[] PickDistinct(Random rnd, string[] source, int count)
    {
        var copy = (string[])source.Clone();
        for (var i = copy.Length - 1; i > 0; i--)
        {
            var j = rnd.Next(i + 1);
            (copy[i], copy[j]) = (copy[j], copy[i]);
        }
        var result = new string[count];
        Array.Copy(copy, result, count);
        return result;
    }

    private static (string, long)[] SampleWatermarks(Random rnd, Dictionary<string, long> emittedPerTable)
    {
        // Report a random subset of tables, each at a random watermark in [1, emitted].
        var keys = new List<string>(emittedPerTable.Keys);
        var take = 1 + rnd.Next(keys.Count);
        for (var i = keys.Count - 1; i > 0; i--)
        {
            var j = rnd.Next(i + 1);
            (keys[i], keys[j]) = (keys[j], keys[i]);
        }
        var picks = new (string, long)[take];
        for (var k = 0; k < take; k++)
        {
            var emitted = emittedPerTable[keys[k]];
            // Pick a watermark anywhere from 1 up to emitted — durable-ack may lag.
            var w = emitted == 0 ? 0 : 1 + rnd.NextInt64(emitted);
            picks[k] = (keys[k], w);
        }
        return picks;
    }

    private static byte[] OkAck(long seq, (string, long)[] entries)
    {
        var size = 11;
        foreach (var (name, _) in entries) size += 2 + System.Text.Encoding.UTF8.GetByteCount(name) + 8;

        var buf = new byte[size];
        buf[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), seq);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), (ushort)entries.Length);
        var pos = 11;
        foreach (var (name, seqTxn) in entries)
        {
            var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos, 2), (ushort)nameBytes.Length);
            pos += 2;
            nameBytes.CopyTo(buf, pos);
            pos += nameBytes.Length;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos, 8), seqTxn);
            pos += 8;
        }
        return buf;
    }

    private static byte[] DurableAck((string, long)[] entries)
    {
        var size = 3;
        foreach (var (name, _) in entries) size += 2 + System.Text.Encoding.UTF8.GetByteCount(name) + 8;

        var buf = new byte[size];
        buf[0] = (byte)QwpStatusCode.DurableAck;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(1, 2), (ushort)entries.Length);
        var pos = 3;
        foreach (var (name, seqTxn) in entries)
        {
            var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(pos, 2), (ushort)nameBytes.Length);
            pos += 2;
            nameBytes.CopyTo(buf, pos);
            pos += nameBytes.Length;
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos, 8), seqTxn);
            pos += 8;
        }
        return buf;
    }

    private static async Task WaitFor(Func<bool> condition, int timeoutMs, string failureMessageTemplate)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(5);
        }
        Assert.Fail(failureMessageTemplate);
    }

    private sealed record FrameSpec(long WireSeq, (string Name, long SeqTxn)[] Entries);

    private sealed class ResponseEvent
    {
        public bool IsOk { get; }
        public FrameSpec? Frame { get; }
        public (string, long)[]? Watermarks { get; }

        private ResponseEvent(bool isOk, FrameSpec? frame, (string, long)[]? watermarks)
        {
            IsOk = isOk;
            Frame = frame;
            Watermarks = watermarks;
        }

        public static ResponseEvent Ok(FrameSpec frame) => new(true, frame, null);
        public static ResponseEvent DurableAck((string, long)[] watermarks) => new(false, null, watermarks);
    }

    private sealed class ScriptedTransport : IQwpCursorTransport
    {
        private readonly Channel<byte[]> _sent = Channel.CreateUnbounded<byte[]>();
        private readonly Channel<byte[]> _responses = Channel.CreateUnbounded<byte[]>();

        public (string Host, int Port)? Endpoint { get; } = ("stub", 0);

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

        public async Task WaitSent(int count, int timeoutMs = 5000)
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
