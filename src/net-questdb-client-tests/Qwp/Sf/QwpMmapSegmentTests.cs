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

using NUnit.Framework;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpMmapSegmentTests
{
    private string _tempDir = null!;

    [SetUp]
    public void SetUp()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "qwp-segment-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    [Test]
    public void Open_FreshFile_HasZeroEnvelopes()
    {
        using var seg = QwpMmapSegment.Open(SegmentPath(), capacity: 4096, baseFsn: 0);

        Assert.That(seg.WritePosition, Is.EqualTo(QwpMmapSegment.HeaderSize));
        Assert.That(seg.EnvelopeCount, Is.Zero);
        Assert.That(seg.NextFsn, Is.Zero);
        Assert.That(seg.IsSealed, Is.False);
    }

    [Test]
    public void Append_OneFrame_RoundTrips()
    {
        using var seg = QwpMmapSegment.Open(SegmentPath(), 4096, 100);
        var frame = new byte[] { 1, 2, 3, 4, 5 };

        Assert.That(seg.TryAppend(frame), Is.True);
        Assert.That(seg.EnvelopeCount, Is.EqualTo(1));
        Assert.That(seg.WritePosition, Is.EqualTo(QwpMmapSegment.HeaderSize + 8 + 5));
        Assert.That(seg.NextFsn, Is.EqualTo(101));

        var dest = new byte[64];
        var read = seg.TryReadFrame(QwpMmapSegment.HeaderSize, dest, out var fsn);
        Assert.That(read, Is.EqualTo(5));
        Assert.That(fsn, Is.EqualTo(100L));
        Assert.That(dest.AsSpan(0, read).ToArray(), Is.EqualTo(frame));
    }

    [Test]
    public void Append_MultipleFrames_AllReadableBackInOrder()
    {
        using var seg = QwpMmapSegment.Open(SegmentPath(), 4096, 0);

        var frames = Enumerable.Range(0, 10)
            .Select(i => Enumerable.Range(0, i + 1).Select(b => (byte)b).ToArray())
            .ToList();

        foreach (var f in frames)
        {
            Assert.That(seg.TryAppend(f), Is.True);
        }

        Assert.That(seg.EnvelopeCount, Is.EqualTo(frames.Count));

        long offset = QwpMmapSegment.HeaderSize;
        var dest = new byte[64];
        for (var i = 0; i < frames.Count; i++)
        {
            var len = seg.TryReadFrame(offset, dest, out _);
            Assert.That(len, Is.EqualTo(frames[i].Length));
            Assert.That(dest.AsSpan(0, len).ToArray(), Is.EqualTo(frames[i]));
            offset += 8 + len;
        }
    }

    [Test]
    public void Append_BeyondCapacity_ReturnsFalse()
    {
        // Capacity = HeaderSize + space for one envelope of header(8) + body up to 24 bytes.
        const int bodyRoom = 32;
        using var seg = QwpMmapSegment.Open(SegmentPath(), QwpMmapSegment.HeaderSize + bodyRoom, 0);

        Assert.That(seg.TryAppend(new byte[20]), Is.True);
        Assert.That(seg.TryAppend(new byte[20]), Is.False, "second 20-byte frame needs 28 bytes; only 4 left");
        Assert.That(seg.EnvelopeCount, Is.EqualTo(1), "the failed append must not increase the envelope count");
    }

    [Test]
    public void Append_EmptyFrame_Throws()
    {
        using var seg = QwpMmapSegment.Open(SegmentPath(), 4096, 0);
        Assert.Throws<ArgumentException>(() => seg.TryAppend(ReadOnlySpan<byte>.Empty));
    }

    [Test]
    public void Reopen_RecoversWritePositionAndEnvelopeCount()
    {
        var path = SegmentPath();

        using (var seg = QwpMmapSegment.Open(path, 4096, 100))
        {
            seg.TryAppend(new byte[] { 10 });
            seg.TryAppend(new byte[] { 20, 21 });
            seg.TryAppend(new byte[] { 30, 31, 32 });
        }

        using var reopened = QwpMmapSegment.Open(path, 4096, 100);
        Assert.That(reopened.EnvelopeCount, Is.EqualTo(3));
        // 3 envelopes × 8-byte header + (1 + 2 + 3) bytes payload = 30 envelope bytes after the file header.
        Assert.That(reopened.WritePosition, Is.EqualTo(QwpMmapSegment.HeaderSize + 30));
        Assert.That(reopened.NextFsn, Is.EqualTo(103));
    }

    [Test]
    public void Reopen_AfterCorruptedFrame_TruncatesToLastGood()
    {
        var path = SegmentPath();

        using (var seg = QwpMmapSegment.Open(path, 4096, 0))
        {
            seg.TryAppend(new byte[] { 1, 2, 3 });
            seg.TryAppend(new byte[] { 4, 5, 6 });
            seg.TryAppend(new byte[] { 7, 8, 9 });
        }

        var bytes = File.ReadAllBytes(path);
        var firstEnvSize = 8 + 3;
        bytes[QwpMmapSegment.HeaderSize + firstEnvSize] ^= 0xFF;
        File.WriteAllBytes(path, bytes);

        using var reopened = QwpMmapSegment.Open(path, 4096, 0);
        Assert.That(reopened.EnvelopeCount, Is.EqualTo(1), "replay must stop at the corruption");
        Assert.That(reopened.WritePosition, Is.EqualTo(QwpMmapSegment.HeaderSize + firstEnvSize));
    }

    [Test]
    public void Reopen_AfterTornTail_TruncatesToLastGood()
    {
        var path = SegmentPath();

        using (var seg = QwpMmapSegment.Open(path, 4096, 0))
        {
            seg.TryAppend(new byte[] { 1, 2, 3 });
        }

        var bytes = File.ReadAllBytes(path);
        var firstEnvSize = 8 + 3;
        BitConverter.TryWriteBytes(bytes.AsSpan(QwpMmapSegment.HeaderSize + firstEnvSize, 4), 0u);
        BitConverter.TryWriteBytes(bytes.AsSpan(QwpMmapSegment.HeaderSize + firstEnvSize + 4, 4), 999_999);
        File.WriteAllBytes(path, bytes);

        using var reopened = QwpMmapSegment.Open(path, 4096, 0);
        Assert.That(reopened.EnvelopeCount, Is.EqualTo(1));
        Assert.That(reopened.WritePosition, Is.EqualTo(QwpMmapSegment.HeaderSize + firstEnvSize));
    }

    [Test]
    public void AppendAfterReopenWithCorruption_OverwritesTornBytes()
    {
        var path = SegmentPath();

        using (var seg = QwpMmapSegment.Open(path, 4096, 0))
        {
            seg.TryAppend(new byte[] { 1, 2, 3 });
            seg.TryAppend(new byte[] { 4, 5, 6 });
        }

        var bytes = File.ReadAllBytes(path);
        bytes[QwpMmapSegment.HeaderSize + 8 + 3] ^= 0xFF;
        File.WriteAllBytes(path, bytes);

        using (var reopened = QwpMmapSegment.Open(path, 4096, 0))
        {
            Assert.That(reopened.EnvelopeCount, Is.EqualTo(1));
            Assert.That(reopened.TryAppend(new byte[] { 99, 99, 99 }), Is.True);
            Assert.That(reopened.EnvelopeCount, Is.EqualTo(2));
        }

        using var third = QwpMmapSegment.Open(path, 4096, 0);
        Assert.That(third.EnvelopeCount, Is.EqualTo(2));
    }

    [Test]
    public void Seal_PreventsFurtherAppends()
    {
        using var seg = QwpMmapSegment.Open(SegmentPath(), 4096, 0);
        seg.TryAppend(new byte[] { 1 });
        seg.Seal();

        Assert.Throws<InvalidOperationException>(() => seg.TryAppend(new byte[] { 2 }));
    }

    [Test]
    public void Seal_PersistsToDisk_RecoversAfterReopen()
    {
        var path = SegmentPath();
        using (var seg = QwpMmapSegment.Open(path, 4096, 0))
        {
            seg.TryAppend(new byte[] { 1, 2, 3 });
            seg.Seal();
            Assert.That(seg.IsSealed, Is.True);
        }

        using var reopened = QwpMmapSegment.Open(path, 4096, 0);
        Assert.That(reopened.IsSealed, Is.True,
            "the sealed flag must survive reopen so crash-after-Seal recovery treats the tail as sealed");
        Assert.Throws<InvalidOperationException>(() => reopened.TryAppend(new byte[] { 4 }));
    }

    [Test]
    public void Reopen_FreshSegment_IsNotSealed()
    {
        var path = SegmentPath();
        using (var seg = QwpMmapSegment.Open(path, 4096, 0))
        {
            seg.TryAppend(new byte[] { 1 });
        }

        using var reopened = QwpMmapSegment.Open(path, 4096, 0);
        Assert.That(reopened.IsSealed, Is.False);
    }

    [Test]
    public void TryReadFrame_OffsetPastEnd_ReturnsMinusOne()
    {
        using var seg = QwpMmapSegment.Open(SegmentPath(), 4096, 0);
        seg.TryAppend(new byte[] { 1, 2, 3 });

        var dest = new byte[64];
        Assert.That(seg.TryReadFrame(99999, dest, out _), Is.EqualTo(-1));
    }

    private string SegmentPath() => Path.Combine(_tempDir, "sf-0000000000000000.sfa");
}
