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
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public sealed class QwpPersistedSymbolDictionaryTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-symbol-dict-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); } catch { }
        }
    }

    [Test]
    public void Append_WritesJavaCompatibleSyd1Chunk()
    {
        var slot = Slot("layout");
        using var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        using var persisted = QwpPersistedSymbolDictionary.OpenOrRecover(slot, ring);
        var dictionary = new QwpSymbolDictionary();
        dictionary.Add("a");
        dictionary.Add("é");

        persisted.AppendNewSymbols(dictionary);

        // Java PersistedSymbolDict format:
        // SYD1 + version/reserved + [count=2][entryBytes=5][1,'a'][2,C3,A9] + CRC32C.
        // CRC32C(02 05 01 61 02 C3 A9) = 0x0A8F49E8, stored little-endian.
        Assert.That(File.ReadAllBytes(Path.Combine(slot, QwpPersistedSymbolDictionary.FileName)),
            Is.EqualTo(new byte[]
            {
                0x53, 0x59, 0x44, 0x31, 0x01, 0x00, 0x00, 0x00,
                0x02, 0x05, 0x01, 0x61, 0x02, 0xC3, 0xA9,
                0xE8, 0x49, 0x8F, 0x0A
            }));
    }

    [Test]
    public void Reopen_LoadsAndValidatesPersistedEntriesAgainstRing()
    {
        var slot = Slot("roundtrip");
        var dictionary = new QwpSymbolDictionary();
        dictionary.Add("alpha");
        dictionary.Add("βeta");

        using (var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096))
        {
            using var persisted = QwpPersistedSymbolDictionary.OpenOrRecover(slot, ring);
            persisted.AppendNewSymbols(dictionary);
            Assert.That(ring.TryAppend(QwpEncoder.Encode(Array.Empty<QwpTableBuffer>(), dictionary)), Is.True);
        }

        using var recoveredRing = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        using var recovered = QwpPersistedSymbolDictionary.OpenOrRecover(slot, recoveredRing);
        Assert.That(recovered.SnapshotEntries(), Is.EqualTo(new[] { "alpha", "βeta" }));
    }

    [Test]
    public void MissingSideFile_LegacySelfSufficientFramesRebuildIt()
    {
        var slot = Slot("legacy-rebuild");
        var dictionary = new QwpSymbolDictionary();
        using (var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096))
        {
            dictionary.Add("alpha");
            Assert.That(ring.TryAppend(QwpEncoder.Encode(
                Array.Empty<QwpTableBuffer>(), dictionary, selfSufficient: true)), Is.True);
            dictionary.Commit();

            dictionary.Add("beta");
            Assert.That(ring.TryAppend(QwpEncoder.Encode(
                Array.Empty<QwpTableBuffer>(), dictionary, selfSufficient: true)), Is.True);
        }

        using var recoveredRing = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        using var recovered = QwpPersistedSymbolDictionary.OpenOrRecover(slot, recoveredRing);
        Assert.That(recovered.SnapshotEntries(), Is.EqualTo(new[] { "alpha", "beta" }));
        Assert.That(File.Exists(Path.Combine(slot, QwpPersistedSymbolDictionary.FileName)), Is.True);
    }

    [Test]
    public void MissingSideFile_TrueDeltaGapFailsClosed()
    {
        var slot = Slot("delta-gap");
        using (var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096))
        {
            var dictionary = new QwpSymbolDictionary();
            dictionary.Add("missing-prefix");
            dictionary.Commit();
            dictionary.Add("surviving-suffix");
            Assert.That(ring.TryAppend(QwpEncoder.Encode(
                Array.Empty<QwpTableBuffer>(), dictionary)), Is.True);
        }

        using var recoveredRing = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        var error = Assert.Throws<InvalidDataException>(() =>
            QwpPersistedSymbolDictionary.OpenOrRecover(slot, recoveredRing));
        Assert.That(error!.Message, Does.Contain("unreplayable symbol dictionary gap"));
        Assert.That(File.Exists(Path.Combine(slot, QwpPersistedSymbolDictionary.FileName)), Is.False,
            "a failed recovery must not fabricate a dictionary that would make the gap look valid next time");
    }

    [Test]
    public void TornTrailingChunk_IsTruncatedAndHealedFromSurvivingFrames()
    {
        var slot = Slot("torn-tail");
        var file = Path.Combine(slot, QwpPersistedSymbolDictionary.FileName);
        long completeLength;

        using (var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096))
        {
            using var persisted = QwpPersistedSymbolDictionary.OpenOrRecover(slot, ring);
            var dictionary = new QwpSymbolDictionary();

            dictionary.Add("alpha");
            persisted.AppendNewSymbols(dictionary);
            Assert.That(ring.TryAppend(QwpEncoder.Encode(Array.Empty<QwpTableBuffer>(), dictionary)), Is.True);
            dictionary.Commit();

            dictionary.Add("beta");
            persisted.AppendNewSymbols(dictionary);
            Assert.That(ring.TryAppend(QwpEncoder.Encode(Array.Empty<QwpTableBuffer>(), dictionary)), Is.True);
            completeLength = persisted.FileLength;
        }

        using (var stream = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.Read))
        {
            stream.SetLength(completeLength - 2); // tear the second chunk's CRC
        }

        using var recoveredRing = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        using var recovered = QwpPersistedSymbolDictionary.OpenOrRecover(slot, recoveredRing);
        Assert.That(recovered.SnapshotEntries(), Is.EqualTo(new[] { "alpha", "beta" }));
        Assert.That(recovered.FileLength, Is.EqualTo(completeLength),
            "the valid prefix should be retained and the missing suffix re-appended as one chunk");
    }

    [Test]
    public void PersistedAndFrameValueMismatch_FailsClosed()
    {
        var slot = Slot("mismatch");
        using (var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096))
        {
            using var persisted = QwpPersistedSymbolDictionary.OpenOrRecover(slot, ring);
            var persistedDictionary = new QwpSymbolDictionary();
            persistedDictionary.Add("persisted-value");
            persisted.AppendNewSymbols(persistedDictionary);

            var frameDictionary = new QwpSymbolDictionary();
            frameDictionary.Add("different-frame-value");
            Assert.That(ring.TryAppend(QwpEncoder.Encode(
                Array.Empty<QwpTableBuffer>(), frameDictionary)), Is.True);
        }

        using var recoveredRing = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        var error = Assert.Throws<InvalidDataException>(() =>
            QwpPersistedSymbolDictionary.OpenOrRecover(slot, recoveredRing));
        Assert.That(error!.Message, Does.Contain("persisted and frame values differ"));
    }

    private string Slot(string name)
    {
        var path = Path.Combine(_root, name);
        Directory.CreateDirectory(path);
        return path;
    }
}
