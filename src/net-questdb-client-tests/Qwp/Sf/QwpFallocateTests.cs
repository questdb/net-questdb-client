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

using NUnit.Framework;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpFallocateTests
{
    [Test]
    public void Reserve_ZeroLength_NoOp()
    {
        var path = Path.Combine(Path.GetTempPath(), $"qwp-fallocate-{Guid.NewGuid():N}");
        using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite))
        {
            QwpFallocate.Reserve(fs, 0);
            Assert.That(fs.Length, Is.EqualTo(0));
        }
        File.Delete(path);
    }

    [Test]
    public void Reserve_ExtendsFileToRequestedLength()
    {
        var path = Path.Combine(Path.GetTempPath(), $"qwp-fallocate-{Guid.NewGuid():N}");
        try
        {
            using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                QwpFallocate.Reserve(fs, 256 * 1024);
                Assert.That(fs.Length, Is.EqualTo(256 * 1024));
            }
            // Posix preallocate / page-walk fallback both leave the file with real bytes the OS can
            // serve without a SIGBUS on a subsequent mmap write; only the size is observable here.
            var info = new FileInfo(path);
            Assert.That(info.Length, Is.EqualTo(256 * 1024));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void Reserve_PreservesExistingBytes()
    {
        var path = Path.Combine(Path.GetTempPath(), $"qwp-fallocate-{Guid.NewGuid():N}");
        try
        {
            using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                Span<byte> probe = stackalloc byte[16];
                for (var i = 0; i < probe.Length; i++) probe[i] = (byte)(i + 1);
                fs.Write(probe);
                fs.Flush(true);

                QwpFallocate.Reserve(fs, 1024 * 1024);
                Assert.That(fs.Length, Is.EqualTo(1024 * 1024));

                fs.Position = 0;
                Span<byte> read = stackalloc byte[16];
                Assert.That(fs.Read(read), Is.EqualTo(16));
                for (var i = 0; i < read.Length; i++)
                {
                    Assert.That(read[i], Is.EqualTo((byte)(i + 1)),
                        "preallocation must not clobber bytes that were already written");
                }
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void Reserve_IdempotentWhenAlreadyAtLength()
    {
        var path = Path.Combine(Path.GetTempPath(), $"qwp-fallocate-{Guid.NewGuid():N}");
        try
        {
            using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite))
            {
                fs.SetLength(64 * 1024);
                QwpFallocate.Reserve(fs, 64 * 1024);
                Assert.That(fs.Length, Is.EqualTo(64 * 1024));
                QwpFallocate.Reserve(fs, 64 * 1024);
                Assert.That(fs.Length, Is.EqualTo(64 * 1024));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }
}

#endif
