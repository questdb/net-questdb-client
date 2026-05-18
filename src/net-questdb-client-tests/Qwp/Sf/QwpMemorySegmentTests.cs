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

using System.Reflection;
using NUnit.Framework;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpMemorySegmentTests
{
    private const int EnvelopeHeaderSize = QwpMemorySegment.EnvelopeHeaderSize;

    [Test]
    public void Allocate_FreshSegment_HasZeroEnvelopes()
    {
        using var seg = QwpMemorySegment.Allocate(capacity: 4096, baseFsn: 0);

        Assert.That(seg.WritePosition, Is.Zero);
        Assert.That(seg.EnvelopeCount, Is.Zero);
        Assert.That(seg.NextFsn, Is.Zero);
        Assert.That(seg.IsSealed, Is.False);
        Assert.That(seg.Capacity, Is.EqualTo(4096));
    }

    [Test]
    public void Allocate_CapacityNotExceedingHeader_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => QwpMemorySegment.Allocate(capacity: EnvelopeHeaderSize, baseFsn: 0));
    }

    [Test]
    public void Append_OneFrame_RoundTrips()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 100);
        var frame = new byte[] { 1, 2, 3, 4, 5 };

        Assert.That(seg.TryAppend(frame), Is.True);
        Assert.That(seg.EnvelopeCount, Is.EqualTo(1));
        Assert.That(seg.WritePosition, Is.EqualTo(EnvelopeHeaderSize + 5));
        Assert.That(seg.NextFsn, Is.EqualTo(101));

        var dest = new byte[64];
        var read = seg.TryReadFrame(0, dest, out var fsn);
        Assert.That(read, Is.EqualTo(5));
        Assert.That(fsn, Is.EqualTo(100L));
        Assert.That(dest.AsSpan(0, read).ToArray(), Is.EqualTo(frame));
    }

    [Test]
    public void Append_MultipleFrames_AllReadableBackInOrder()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);

        var frames = Enumerable.Range(0, 10)
            .Select(i => Enumerable.Range(0, i + 1).Select(b => (byte)b).ToArray())
            .ToList();

        foreach (var f in frames)
        {
            Assert.That(seg.TryAppend(f), Is.True);
        }

        Assert.That(seg.EnvelopeCount, Is.EqualTo(frames.Count));

        var dest = new byte[64];
        for (var i = 0; i < frames.Count; i++)
        {
            var offset = seg.OffsetOfEnvelope(i);
            Assert.That(offset, Is.Not.Null);
            var len = seg.TryReadFrame(offset!.Value, dest, out var fsn);
            Assert.That(len, Is.EqualTo(frames[i].Length));
            Assert.That(fsn, Is.EqualTo((long)i));
            Assert.That(dest.AsSpan(0, len).ToArray(), Is.EqualTo(frames[i]));
        }
    }

    [Test]
    public void Append_BeyondCapacity_ReturnsFalse()
    {
        const int bodyRoom = 32;
        using var seg = QwpMemorySegment.Allocate(EnvelopeHeaderSize + bodyRoom, baseFsn: 0);

        Assert.That(seg.TryAppend(new byte[20]), Is.True);
        Assert.That(seg.TryAppend(new byte[20]), Is.False,
                    "second 20-byte frame needs 28 bytes; only 12 left");
        Assert.That(seg.EnvelopeCount, Is.EqualTo(1), "the failed append must not increase the envelope count");
    }

    [Test]
    public void Append_EmptyFrame_Throws()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);
        Assert.Throws<ArgumentException>(() => seg.TryAppend(ReadOnlySpan<byte>.Empty));
    }

    [Test]
    public void Append_FrameLargerThanMaxFrameLength_Throws()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0, maxFrameLength: 64);
        Assert.Throws<ArgumentException>(() => seg.TryAppend(new byte[65]));
    }

    [Test]
    public void TryReadFrame_OffsetPastEnd_ReturnsMinusOne()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);
        seg.TryAppend(new byte[] { 1, 2, 3 });

        var dest = new byte[64];
        Assert.That(seg.TryReadFrame(99999, dest, out _), Is.EqualTo(-1));
    }

    [Test]
    public void TryReadFrame_DestinationTooSmall_Throws()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);
        seg.TryAppend(new byte[] { 1, 2, 3, 4, 5 });

        var dest = new byte[2];
        Assert.Throws<ArgumentException>(() => seg.TryReadFrame(0, dest, out _));
    }

    [Test]
    public void TryReadFrame_AtMidEnvelopeOffset_IsRejected()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);
        seg.TryAppend(new byte[] { 1, 2, 3, 4, 5, 6 });

        // Reading one byte into the envelope re-interprets later bytes as the length field;
        // that decodes to an out-of-range length, so the read is rejected.
        var dest = new byte[64];
        Assert.Throws<ArgumentException>(() => seg.TryReadFrame(1, dest, out _));
    }

    [Test]
    public void Seal_PreventsFurtherAppends()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);
        seg.TryAppend(new byte[] { 1 });
        seg.Seal();

        Assert.That(seg.IsSealed, Is.True);
        Assert.Throws<InvalidOperationException>(() => seg.TryAppend(new byte[] { 2 }));
    }

    [Test]
    public void TryAppend_AfterDispose_Throws()
    {
        var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);
        seg.Dispose();

        Assert.Throws<ObjectDisposedException>(() => seg.TryAppend(new byte[] { 1 }));
    }

    [Test]
    public unsafe void TryReadFrame_VerifiesCrc_OnInMemoryCorruption()
    {
        using var seg = QwpMemorySegment.Allocate(4096, baseFsn: 0);
        seg.TryAppend(new byte[] { 1, 2, 3, 4 });

        // Flip the first body byte directly in the native buffer, leaving the stored CRC stale.
        var basePtrField = typeof(QwpMemorySegment).GetField(
            "_basePtr", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var basePtr = (byte*)Pointer.Unbox(basePtrField.GetValue(seg)!);
        basePtr[EnvelopeHeaderSize] ^= 0x55;

        var dest = new byte[64];
        Assert.Throws<InvalidDataException>(() => seg.TryReadFrame(0, dest, out _));
    }
}
