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

using System.Buffers.Binary;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public sealed class QwpSymbolDictionaryMirrorTests
{
    [Test]
    public async Task SendCatchUpAsync_SplitsAtAdvertisedBatchCapAndCoversEveryEntry()
    {
        var entries = Enumerable.Range(0, 5).Select(i => $"symbol-{i:00}").ToArray();
        var mirror = new QwpSymbolDictionaryMirror();
        mirror.Seed(entries);

        // Fits exactly two 10-byte encoded entries after the header and the two range varints,
        // forcing a 2+2+1 split.
        var cap = QwpConstants.HeaderSize + 2 + 2 * (1 + 9);
        using var transport = new CatchUpTransport { NegotiatedMaxBatchSize = cap };

        var framesSent = await mirror.SendCatchUpAsync(transport, CancellationToken.None);

        Assert.That(framesSent, Is.EqualTo(3));
        Assert.That(transport.Sent, Has.Count.EqualTo(3));
        var nextId = 0;
        var replayed = new List<string>();
        foreach (var frame in transport.Sent)
        {
            Assert.That(frame.Length, Is.LessThanOrEqualTo(cap));
            Assert.That(frame[QwpConstants.OffsetFlags], Is.EqualTo(
                (byte)(QwpConstants.FlagDeltaSymbolDict | QwpConstants.FlagGorilla | QwpConstants.FlagDeferCommit)));
            Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(
                frame.AsSpan(QwpConstants.OffsetTableCount, 2)), Is.Zero);

            var (start, decoded) = QwpFrameTestUtils.ReadSymbolDelta(frame);
            Assert.That(start, Is.EqualTo(nextId),
                "catch-up frames must partition the id space contiguously");
            nextId += decoded.Length;
            replayed.AddRange(decoded);
        }
        Assert.That(replayed, Is.EqualTo(entries));
    }

    [Test]
    public async Task Accumulate_OverlappingReplayDelta_AppendsOnlyTheUnseenTail()
    {
        var mirror = new QwpSymbolDictionaryMirror();
        mirror.Accumulate(BuildDeltaFrame(0, "alpha", "beta"));
        Assert.That(mirror.Count, Is.EqualTo(2));

        var overlapping = BuildDeltaFrame(1, "beta", "gamma", "delta");
        mirror.Accumulate(overlapping);
        Assert.That(mirror.Count, Is.EqualTo(4));

        mirror.Accumulate(overlapping);
        Assert.That(mirror.Count, Is.EqualTo(4), "a replayed delta must be idempotent");

        using var transport = new CatchUpTransport();
        var framesSent = await mirror.SendCatchUpAsync(transport, CancellationToken.None);

        Assert.That(framesSent, Is.EqualTo(1));
        var (start, replayed) = QwpFrameTestUtils.ReadSymbolDelta(transport.Sent.Single());
        Assert.That(start, Is.Zero);
        Assert.That(replayed, Is.EqualTo(new[] { "alpha", "beta", "gamma", "delta" }),
            "the mirrored tail must keep per-entry boundaries intact");
    }

    [Test]
    public void ValidateContinuity_RejectsDeltaStartingAboveMirroredCount()
    {
        var mirror = new QwpSymbolDictionaryMirror();
        mirror.Accumulate(BuildDeltaFrame(0, "alpha"));

        var gap = BuildDeltaFrame(2, "gamma");
        var error = Assert.Throws<InvalidDataException>(() => mirror.ValidateContinuity(gap));
        Assert.That(error!.Message, Does.Contain("gap"));
        Assert.That(error.Message, Does.Contain("mirror contains 1"));

        Assert.Throws<InvalidDataException>(() => mirror.Accumulate(gap));
        Assert.That(mirror.Count, Is.EqualTo(1), "a rejected gap delta must not mutate the mirror");

        // The boundary delta (start == Count) is a legal contiguous extension, not a gap.
        Assert.DoesNotThrow(() => mirror.ValidateContinuity(BuildDeltaFrame(1, "beta")));
    }

    [Test]
    public void ValidateContinuity_RejectsDictionaryRangePastProtocolCapBeforeSend()
    {
        var frame = BuildOversizedDeltaFrame();
        var mirror = new QwpSymbolDictionaryMirror();

        var error = Assert.Throws<InvalidDataException>(() => mirror.ValidateContinuity(frame));

        Assert.That(error!.Message, Does.Contain(QwpConstants.MaxSymbolDictionarySize.ToString()));
        Assert.That(error.Message, Does.Contain("entry limit"));
        Assert.That(mirror.Count, Is.Zero);
    }

    private static byte[] BuildOversizedDeltaFrame()
    {
        var builder = new QwpEncoder.FrameBuilder(QwpConstants.HeaderSize + 16);
        builder.Allocate(QwpConstants.HeaderSize);
        builder.WriteVarint(0);
        builder.WriteVarint((ulong)QwpConstants.MaxSymbolDictionarySize + 1);
        WriteDeltaFrameHeader(builder);
        return builder.ToArray();
    }

    private static byte[] BuildDeltaFrame(int start, params string[] entries)
    {
        var builder = new QwpEncoder.FrameBuilder(QwpConstants.HeaderSize + 64);
        builder.Allocate(QwpConstants.HeaderSize);
        builder.WriteVarint((ulong)start);
        builder.WriteVarint((ulong)entries.Length);
        foreach (var entry in entries)
        {
            var bytes = QwpStrictUtf8.Encoding.GetBytes(entry);
            builder.WriteVarint((ulong)bytes.Length);
            builder.WriteBytes(bytes);
        }
        WriteDeltaFrameHeader(builder);
        return builder.ToArray();
    }

    private static void WriteDeltaFrameHeader(QwpEncoder.FrameBuilder builder)
    {
        var header = builder.AsSpan(0, QwpConstants.HeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(
            header.Slice(QwpConstants.OffsetMagic, 4), QwpConstants.Magic);
        header[QwpConstants.OffsetVersion] = QwpConstants.SupportedVersion;
        header[QwpConstants.OffsetFlags] = QwpConstants.FlagDeltaSymbolDict;
        BinaryPrimitives.WriteUInt16LittleEndian(
            header.Slice(QwpConstants.OffsetTableCount, 2), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(
            header.Slice(QwpConstants.OffsetPayloadLength, 4),
            (uint)(builder.Length - QwpConstants.HeaderSize));
    }

    private sealed class CatchUpTransport : IQwpCursorTransport
    {
        public List<byte[]> Sent { get; } = new();
        public int NegotiatedMaxBatchSize { get; init; }
        public (string Host, int Port)? Endpoint => null;

        public Task ConnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            Sent.Add(data.ToArray());
            return Task.CompletedTask;
        }

        public Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task CloseAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose()
        {
        }
    }
}
