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
public class QwpFilesTests
{
    private string _tempDir = null!;

    [SetUp]
    public void SetUp()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "qwp-files-" + Guid.NewGuid().ToString("N"));
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
    public void OpenExclusive_GrantsAccessToFirstOpener()
    {
        var path = Path.Combine(_tempDir, "lock");
        using var first = QwpFiles.OpenExclusive(path);
        Assert.That(first.CanWrite);
    }

    [Test]
    public void TryOpenExclusive_ReturnsNullOnLockCollision()
    {
        var path = Path.Combine(_tempDir, "lock");
        using var first = QwpFiles.OpenExclusive(path);

        var second = QwpFiles.TryOpenExclusive(path);
        Assert.That(second, Is.Null, "second open should fail because first holds the exclusive share");
    }

    [Test]
    public void TryOpenExclusive_MissingDirectory_PropagatesNotNullReturn()
    {
        var nestedMissing = Path.Combine(_tempDir, "no-such-dir", "lock");
        Assert.Throws<DirectoryNotFoundException>(() => QwpFiles.TryOpenExclusive(nestedMissing));
    }

    [Test]
    public void TryOpenExclusive_SucceedsAfterFirstReleases()
    {
        var path = Path.Combine(_tempDir, "lock");
        var first = QwpFiles.OpenExclusive(path);
        first.Dispose();

        using var second = QwpFiles.TryOpenExclusive(path);
        Assert.That(second, Is.Not.Null);
    }

    [Test]
    public void EnsureFileLength_ExtendsButDoesNotShrink()
    {
        var path = Path.Combine(_tempDir, "data");
        QwpFiles.EnsureFileLength(path, 1024);
        Assert.That(new FileInfo(path).Length, Is.EqualTo(1024));

        // Re-call with smaller size: must not shrink.
        QwpFiles.EnsureFileLength(path, 512);
        Assert.That(new FileInfo(path).Length, Is.EqualTo(1024));

        // Re-call with larger size: extends.
        QwpFiles.EnsureFileLength(path, 4096);
        Assert.That(new FileInfo(path).Length, Is.EqualTo(4096));
    }

    [Test]
    public void Truncate_ShrinksFile()
    {
        var path = Path.Combine(_tempDir, "data");
        QwpFiles.EnsureFileLength(path, 4096);
        QwpFiles.Truncate(path, 1024);
        Assert.That(new FileInfo(path).Length, Is.EqualTo(1024));
    }

    [Test]
    public void OpenMemoryMappedSegment_RoundTripsBytes()
    {
        var path = Path.Combine(_tempDir, "segment");
        const int capacity = 8192;

        {
            var (mmap, fs) = QwpFiles.OpenMemoryMappedSegment(path, capacity);
            using (mmap)
            using (fs)
            using (var view = mmap.CreateViewAccessor(0, capacity, System.IO.MemoryMappedFiles.MemoryMappedFileAccess.ReadWrite))
            {
                view.Write(0, 0xDEADBEEFu);
                view.Write(4, 0x12345678u);
                view.Flush();
            }
        }

        // Reopen and verify the writes survived.
        {
            var (mmap, fs) = QwpFiles.OpenMemoryMappedSegment(path, capacity);
            using (mmap)
            using (fs)
            using (var view = mmap.CreateViewAccessor(0, capacity, System.IO.MemoryMappedFiles.MemoryMappedFileAccess.Read))
            {
                Assert.That(view.ReadUInt32(0), Is.EqualTo(0xDEADBEEFu));
                Assert.That(view.ReadUInt32(4), Is.EqualTo(0x12345678u));
            }
        }
    }

    [Test]
    public void EnumerateSlotDirectories_ListsImmediateSubdirsOnly()
    {
        Directory.CreateDirectory(Path.Combine(_tempDir, "slot-a"));
        Directory.CreateDirectory(Path.Combine(_tempDir, "slot-b"));
        Directory.CreateDirectory(Path.Combine(_tempDir, "slot-b", "nested"));

        var slots = QwpFiles.EnumerateSlotDirectories(_tempDir).Select(Path.GetFileName).OrderBy(n => n).ToList();
        Assert.That(slots, Is.EqualTo(new[] { "slot-a", "slot-b" }));
    }

    [Test]
    public void EnumerateSlotDirectories_AbsentRoot_ReturnsEmpty()
    {
        Assert.That(QwpFiles.EnumerateSlotDirectories(Path.Combine(_tempDir, "nope")), Is.Empty);
    }

    [Test]
    public void Delete_AbsentFile_IsNoOp()
    {
        Assert.DoesNotThrow(() => QwpFiles.Delete(Path.Combine(_tempDir, "missing")));
    }

    [Test]
    public void PageSize_IsPositive()
    {
        Assert.That(QwpFiles.PageSize, Is.GreaterThan(0));
    }
}
