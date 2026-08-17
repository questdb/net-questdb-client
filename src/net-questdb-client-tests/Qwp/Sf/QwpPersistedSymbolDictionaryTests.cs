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

using System.Collections.Concurrent;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Utils;

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
        var dictionary = new QwpSymbolDictionary();
        dictionary.Add("a");
        dictionary.Add("é");

        using (var persisted = QwpPersistedSymbolDictionary.OpenOrRecover(slot, ring))
        {
            persisted.AppendNewSymbols(dictionary);
        }

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
    public void AppendNewSymbols_RetryAfterFailedPublish_DoesNotDuplicateEntries()
    {
        var slot = Slot("idempotent");
        using var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        var dictionary = new QwpSymbolDictionary();
        dictionary.Add("alpha");
        dictionary.Add("beta");

        using var persisted = QwpPersistedSymbolDictionary.OpenOrRecover(slot, ring);
        persisted.AppendNewSymbols(dictionary);
        var lengthAfterFirst = persisted.FileLength;

        // A failed ring publication retries the same flush: the retry must observe the advanced
        // persisted count and write nothing.
        persisted.AppendNewSymbols(dictionary);

        Assert.That(persisted.Count, Is.EqualTo(2));
        Assert.That(persisted.FileLength, Is.EqualTo(lengthAfterFirst));

        dictionary.Add("gamma");
        persisted.AppendNewSymbols(dictionary);

        Assert.That(persisted.SnapshotEntries(), Is.EqualTo(new[] { "alpha", "beta", "gamma" }));
        Assert.That(persisted.FileLength, Is.GreaterThan(lengthAfterFirst));
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
        var error = Assert.Throws<QwpUnreplayableSlotException>(() =>
            QwpPersistedSymbolDictionary.OpenOrRecover(slot, recoveredRing));
        Assert.That(error!.Message, Does.Contain("unreplayable symbol dictionary gap"));
        Assert.That(File.Exists(Path.Combine(slot, QwpPersistedSymbolDictionary.FileName)), Is.False,
            "a failed recovery must not fabricate a dictionary that would make the gap look valid next time");
    }

#if NET7_0_OR_GREATER
    [Test]
    public void SenderStartup_UnreplayableSlotIsQuarantinedAndSenderContinuesOnFreshSlot()
    {
        const string senderId = "sender-recovery-quarantine";
        var slot = SeedUnreplayableSlot(senderId);

        // The handler also sees background connect failures (the endpoint is unreachable), on the
        // dispatcher thread — collect thread-safely and assert on the DataLoss report alone.
        var reported = new ConcurrentQueue<SenderError>();
        var options = new SenderOptions(BuildConfString(senderId))
        {
            error_handler = reported.Enqueue,
        };

        using (var sender = Sender.New(options))
        {
            sender.Table("t").Symbol("s", "v").AtNow();
        }

        var quarantined = Path.Combine(_root, senderId + QwpOrphanScanner.QuarantineSlotInfix + "0");
        Assert.That(Directory.Exists(quarantined), Is.True);
        Assert.That(File.Exists(Path.Combine(quarantined, ".failed")), Is.True);
        Assert.That(Directory.EnumerateFiles(quarantined, "sf-*.sfa").Any(), Is.True,
            "the unreplayable slot's bytes must be preserved for inspection and resend");
        Assert.That(Directory.Exists(slot), Is.True);

        var dataLoss = reported.Where(e => e.Category == SenderErrorCategory.DataLoss).ToList();
        Assert.That(dataLoss, Has.Count.EqualTo(1));
        Assert.That(dataLoss[0].AppliedPolicy, Is.EqualTo(SenderErrorPolicy.Abandoned));
        Assert.That(dataLoss[0].QuarantinedPath, Is.EqualTo(quarantined));
        Assert.That(dataLoss[0].ServerMessage, Does.Contain("unreplayable"));
    }

    [Test]
    public void SenderStartup_QuarantineNamesDoNotCollideAcrossRepeatedFailures()
    {
        const string senderId = "sender-recovery-requarantine";
        SeedUnreplayableSlot(senderId);
        Directory.CreateDirectory(Path.Combine(_root, senderId + QwpOrphanScanner.QuarantineSlotInfix + "0"));

        using (Sender.New(BuildConfString(senderId)))
        {
        }

        Assert.That(Directory.Exists(
            Path.Combine(_root, senderId + QwpOrphanScanner.QuarantineSlotInfix + "1")), Is.True);
    }

    [Test]
    public void SenderStartup_TooManyQuarantinedSlotsFailsLoudly()
    {
        const string senderId = "sender-recovery-cap";
        var slot = SeedUnreplayableSlot(senderId);
        for (var i = 0; i < 64; i++)
        {
            Directory.CreateDirectory(Path.Combine(_root, senderId + QwpOrphanScanner.QuarantineSlotInfix + i));
        }

        var error = Assert.Throws<IngressError>(() => Sender.New(BuildConfString(senderId)));

        Assert.That(error!.code, Is.EqualTo(ErrorCode.ConfigError));
        Assert.That(error.Message, Does.Contain("too many quarantined slots"));
        Assert.That(Directory.EnumerateFiles(slot, "sf-*.sfa").Any(), Is.True,
            "a failed set-aside must never drop the slot's bytes");
    }

    private string SeedUnreplayableSlot(string senderId)
    {
        var slot = Slot(senderId);
        using var ring = QwpSegmentRing.Open(slot, segmentCapacity: 4096);
        var dictionary = new QwpSymbolDictionary();
        dictionary.Add("missing-prefix");
        dictionary.Commit();
        dictionary.Add("surviving-suffix");
        Assert.That(ring.TryAppend(QwpEncoder.Encode(
            Array.Empty<QwpTableBuffer>(), dictionary)), Is.True);
        return slot;
    }

    private string BuildConfString(string senderId)
    {
        return $"ws::addr=127.0.0.1:1;sf_dir={_root};sender_id={senderId};" +
               "sf_max_segment_bytes=4096;initial_connect_retry=async;" +
               "auto_flush=off;close_flush_timeout_millis=0;";
    }
#endif

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
        var error = Assert.Throws<QwpUnreplayableSlotException>(() =>
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
