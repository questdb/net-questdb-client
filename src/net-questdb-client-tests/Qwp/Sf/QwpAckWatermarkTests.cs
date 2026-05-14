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
using QuestDB.Enums;
using QuestDB.Qwp.Sf;

namespace QuestDB.Tests.Qwp.Sf;

[TestFixture]
public class QwpAckWatermarkTests
{
    private string _tempDir = null!;

    [SetUp]
    public void SetUp()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "qwp-akw-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_tempDir, recursive: true); }
        catch { }
    }

    [Test]
    public void FreshFile_ReadsInvalid()
    {
        using var wm = QwpAckWatermark.Open(_tempDir)!;
        Assert.That(wm, Is.Not.Null);
        Assert.That(wm.Read(), Is.EqualTo(QwpAckWatermark.Invalid));
    }

    [Test]
    public void Write_RoundTripsAcrossReopen()
    {
        using (var wm = QwpAckWatermark.Open(_tempDir)!)
        {
            wm.Write(12345L);
            Assert.That(wm.Read(), Is.EqualTo(12345L));
        }

        using (var wm = QwpAckWatermark.Open(_tempDir)!)
        {
            Assert.That(wm.Read(), Is.EqualTo(12345L));
        }
    }

    [Test]
    public void Write_PersistsMonotonicAdvances()
    {
        using var wm = QwpAckWatermark.Open(_tempDir)!;
        wm.Write(1L);
        wm.Write(2L);
        wm.Write(100L);
        Assert.That(wm.Read(), Is.EqualTo(100L));
    }

    [Test]
    public void FileLayout_Is16Bytes_WithMagicAndFsn()
    {
        using (var wm = QwpAckWatermark.Open(_tempDir)!)
        {
            wm.Write(0x0102030405060708L);
        }

        var path = Path.Combine(_tempDir, QwpAckWatermark.FileName);
        var bytes = File.ReadAllBytes(path);
        Assert.That(bytes.Length, Is.EqualTo(16));
        // Magic "AKW1" little-endian at offset 0.
        Assert.That(bytes[0], Is.EqualTo(0x41));
        Assert.That(bytes[1], Is.EqualTo(0x4B));
        Assert.That(bytes[2], Is.EqualTo(0x57));
        Assert.That(bytes[3], Is.EqualTo(0x31));
        // Reserved bytes [4..7] are zero.
        Assert.That(bytes[4], Is.EqualTo(0));
        Assert.That(bytes[5], Is.EqualTo(0));
        Assert.That(bytes[6], Is.EqualTo(0));
        Assert.That(bytes[7], Is.EqualTo(0));
        // FSN little-endian at offset 8.
        Assert.That(bytes[8],  Is.EqualTo(0x08));
        Assert.That(bytes[9],  Is.EqualTo(0x07));
        Assert.That(bytes[10], Is.EqualTo(0x06));
        Assert.That(bytes[11], Is.EqualTo(0x05));
        Assert.That(bytes[12], Is.EqualTo(0x04));
        Assert.That(bytes[13], Is.EqualTo(0x03));
        Assert.That(bytes[14], Is.EqualTo(0x02));
        Assert.That(bytes[15], Is.EqualTo(0x01));
    }

    [Test]
    public void RemoveOrphan_DeletesFile()
    {
        using (var wm = QwpAckWatermark.Open(_tempDir)!)
        {
            wm.Write(99L);
        }
        var path = Path.Combine(_tempDir, QwpAckWatermark.FileName);
        Assert.That(File.Exists(path), Is.True);

        QwpAckWatermark.RemoveOrphan(_tempDir);
        Assert.That(File.Exists(path), Is.False);
    }

    [Test]
    public void RemoveOrphan_IsBestEffort_OnMissingFile()
    {
        QwpAckWatermark.RemoveOrphan(_tempDir);
        Assert.That(File.Exists(Path.Combine(_tempDir, QwpAckWatermark.FileName)), Is.False);
    }

    [Test]
    public void Open_PreservesWrongSizedFile_ReturnsNull()
    {
        var path = Path.Combine(_tempDir, QwpAckWatermark.FileName);
        var bogus = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF };
        File.WriteAllBytes(path, bogus);

        var wm = QwpAckWatermark.Open(_tempDir);
        Assert.That(wm, Is.Null);
        Assert.That(new FileInfo(path).Length, Is.EqualTo(bogus.Length));
        Assert.That(File.ReadAllBytes(path), Is.EqualTo(bogus));
    }

    [Test]
    public void Open_TreatsZeroFilledFile_AsInvalid()
    {
        var path = Path.Combine(_tempDir, QwpAckWatermark.FileName);
        File.WriteAllBytes(path, new byte[16]);

        using var wm = QwpAckWatermark.Open(_tempDir)!;
        Assert.That(wm.Read(), Is.EqualTo(QwpAckWatermark.Invalid));
    }

    [Test]
    public void Read_AfterDispose_ReturnsInvalid()
    {
        var wm = QwpAckWatermark.Open(_tempDir)!;
        wm.Write(42L);
        wm.Dispose();
        Assert.That(wm.Read(), Is.EqualTo(QwpAckWatermark.Invalid));
    }

    [Test]
    public void DoubleDispose_DoesNotThrow()
    {
        var wm = QwpAckWatermark.Open(_tempDir)!;
        wm.Dispose();
        Assert.That(() => wm.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Engine_RefinesAckedFsn_FromWatermark()
    {
        var slotDir = Path.Combine(_tempDir, "slot");
        Directory.CreateDirectory(slotDir);

        using (var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096))
        {
            Assert.That(ring.TryAppend(new byte[] { 0x00 }), Is.True);
            Assert.That(ring.TryAppend(new byte[] { 0x01 }), Is.True);
            Assert.That(ring.TryAppend(new byte[] { 0x02 }), Is.True);
        }

        using (var wm = QwpAckWatermark.Open(slotDir)!)
        {
            wm.Write(1L);
        }

        using var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring2 = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        Assert.That(ring2.OldestFsn, Is.EqualTo(0));
        Assert.That(ring2.NextFsn, Is.EqualTo(3));

        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(1));
        var wm2 = QwpAckWatermark.Open(slotDir);
        using var engine = new QwpCursorSendEngine(
            slotLock, ring2, () => null!, policy,
            TimeSpan.FromSeconds(5), InitialConnectMode.off,
            ackWatermark: wm2);

        Assert.That(engine.AckedFsn, Is.EqualTo(2));
    }

    [Test]
    public void Engine_FallsBackToSegmentSeed_WhenWatermarkExceedsPublished()
    {
        var slotDir = Path.Combine(_tempDir, "slot");
        Directory.CreateDirectory(slotDir);

        using (var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096))
        {
            Assert.That(ring.TryAppend(new byte[] { 0x00 }), Is.True);
            Assert.That(ring.TryAppend(new byte[] { 0x01 }), Is.True);
        }

        using (var wm = QwpAckWatermark.Open(slotDir)!)
        {
            wm.Write(99L);
        }

        using var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring2 = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);
        Assert.That(ring2.NextFsn, Is.EqualTo(2));

        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(1));
        var wm2 = QwpAckWatermark.Open(slotDir);
        using var engine = new QwpCursorSendEngine(
            slotLock, ring2, () => null!, policy,
            TimeSpan.FromSeconds(5), InitialConnectMode.off,
            ackWatermark: wm2);

        Assert.That(engine.AckedFsn, Is.EqualTo(ring2.OldestFsn));
    }

    [Test]
    public void Engine_UsesSegmentSeed_WhenNoWatermark()
    {
        var slotDir = Path.Combine(_tempDir, "slot");
        Directory.CreateDirectory(slotDir);

        using (var ring = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096))
        {
            Assert.That(ring.TryAppend(new byte[] { 0x00 }), Is.True);
        }

        using var slotLock = QwpSlotLock.Acquire(slotDir);
        var ring2 = QwpSegmentRing.Open(slotDir, segmentCapacity: 4096);

        var policy = new QwpReconnectPolicy(
            TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(1));
        using var engine = new QwpCursorSendEngine(
            slotLock, ring2, () => null!, policy,
            TimeSpan.FromSeconds(5), InitialConnectMode.off,
            ackWatermark: null);

        Assert.That(engine.AckedFsn, Is.EqualTo(ring2.OldestFsn));
    }
}
