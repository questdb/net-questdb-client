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
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpSlotLockTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-slotlock-" + Guid.NewGuid().ToString("N"));
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Test]
    public void Acquire_FreshDirectory_Succeeds()
    {
        using var slotLock = QwpSlotLock.Acquire(_root);

        Assert.That(slotLock.SlotDirectory, Is.EqualTo(_root));
        Assert.That(slotLock.LockFilePath, Is.EqualTo(Path.Combine(_root, ".lock")));
        Assert.That(File.Exists(slotLock.LockFilePath), Is.True);
    }

    [Test]
    public void Acquire_CreatesSlotDirectoryIfMissing()
    {
        Assert.That(Directory.Exists(_root), Is.False);

        using var slotLock = QwpSlotLock.Acquire(_root);

        Assert.That(Directory.Exists(_root), Is.True);
    }

    [Test]
    public void Acquire_AlreadyHeldInProcess_ThrowsIngressError()
    {
        using var first = QwpSlotLock.Acquire(_root);

        var ex = Assert.Catch<IngressError>(() => QwpSlotLock.Acquire(_root));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
        Assert.That(ex.Message, Does.Contain(_root));
    }

    [Test]
    public void TryAcquire_FreshDirectory_ReturnsLock()
    {
        using var slotLock = QwpSlotLock.TryAcquire(_root);

        Assert.That(slotLock, Is.Not.Null);
        Assert.That(File.Exists(slotLock!.LockFilePath), Is.True);
    }

    [Test]
    public void TryAcquire_AlreadyHeld_ReturnsNull()
    {
        using var first = QwpSlotLock.Acquire(_root);

        var second = QwpSlotLock.TryAcquire(_root);
        Assert.That(second, Is.Null);
    }

    [Test]
    public void Dispose_ReleasesLock_AllowsReacquisition()
    {
        var first = QwpSlotLock.Acquire(_root);
        first.Dispose();

        // After disposal a second acquisition must succeed.
        using var second = QwpSlotLock.Acquire(_root);
        Assert.That(second.SlotDirectory, Is.EqualTo(_root));
    }

    [Test]
    public void Dispose_Idempotent()
    {
        var slotLock = QwpSlotLock.Acquire(_root);
        slotLock.Dispose();
        Assert.DoesNotThrow(() => slotLock.Dispose());
    }

    [Test]
    public void Acquire_DistinctSlots_AreIndependent()
    {
        var slotA = Path.Combine(_root, "sender-a");
        var slotB = Path.Combine(_root, "sender-b");

        using var lockA = QwpSlotLock.Acquire(slotA);
        using var lockB = QwpSlotLock.Acquire(slotB);

        Assert.That(lockA.SlotDirectory, Is.EqualTo(slotA));
        Assert.That(lockB.SlotDirectory, Is.EqualTo(slotB));
    }
}
