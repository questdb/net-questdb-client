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
public class SfCleanupTests
{
    [Test]
    public void Run_PlantedIOException_Swallowed()
    {
        Assert.DoesNotThrow(() => SfCleanup.Run(() => throw new IOException("expected")));
    }

    [Test]
    public void Run_PlantedNullReferenceException_Escapes()
    {
        Assert.Throws<NullReferenceException>(
            () => SfCleanup.Run(() => throw new NullReferenceException("planted")));
    }

    [Test]
    public void Run_AggregateException_OfExpectedOnly_Swallowed()
    {
        Assert.DoesNotThrow(() => SfCleanup.Run(() =>
            throw new AggregateException(new IOException("a"), new ObjectDisposedException("b"))));
    }

    [Test]
    public void Run_AggregateException_WithUnexpectedInner_Escapes()
    {
        // The recursion guarantee: a wrapped real bug must not be silently swallowed.
        Assert.Throws<AggregateException>(() => SfCleanup.Run(() =>
            throw new AggregateException(new IOException("a"), new NullReferenceException("planted"))));
    }

    [Test]
    public void Run_NestedAggregateException_RecursesCorrectly()
    {
        Assert.Throws<AggregateException>(() => SfCleanup.Run(() =>
            throw new AggregateException(
                new AggregateException(new InvalidOperationException("planted")))));
    }

    [Test]
    public void Dispose_NullDisposable_NoOp()
    {
        Assert.DoesNotThrow(() => SfCleanup.Dispose(null));
    }

    [Test]
    public void Dispose_PlantedNullReference_Escapes()
    {
        var disposable = new ThrowingDisposable(new NullReferenceException("planted"));
        Assert.Throws<NullReferenceException>(() => SfCleanup.Dispose(disposable));
    }

    [Test]
    public void Dispose_PlantedIOException_Swallowed()
    {
        var disposable = new ThrowingDisposable(new IOException("expected"));
        Assert.DoesNotThrow(() => SfCleanup.Dispose(disposable));
    }

    [Test]
    public void DeleteFile_MissingPath_NoOp()
    {
        var path = Path.Combine(Path.GetTempPath(), "sf-cleanup-missing-" + Guid.NewGuid().ToString("N"));
        Assert.DoesNotThrow(() => SfCleanup.DeleteFile(path));
    }

    private sealed class ThrowingDisposable : IDisposable
    {
        private readonly Exception _ex;
        public ThrowingDisposable(Exception ex) { _ex = ex; }
        public void Dispose() => throw _ex;
    }
}
