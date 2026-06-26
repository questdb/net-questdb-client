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

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpErrorClassifierTests
{
    [TestCase(QwpStatusCode.SchemaMismatch, SenderErrorCategory.SchemaMismatch)]
    [TestCase(QwpStatusCode.ParseError, SenderErrorCategory.ParseError)]
    [TestCase(QwpStatusCode.InternalError, SenderErrorCategory.InternalError)]
    [TestCase(QwpStatusCode.SecurityError, SenderErrorCategory.SecurityError)]
    [TestCase(QwpStatusCode.WriteError, SenderErrorCategory.WriteError)]
    [TestCase((QwpStatusCode)0xFF, SenderErrorCategory.Unknown)]
    public void Classify_ReturnsExpectedCategory(QwpStatusCode status, SenderErrorCategory expected)
    {
        Assert.That(QwpErrorClassifier.Classify(status), Is.EqualTo(expected));
    }

    [TestCase(SenderErrorCategory.SchemaMismatch, SenderErrorPolicy.DropAndContinue)]
    [TestCase(SenderErrorCategory.WriteError, SenderErrorPolicy.DropAndContinue)]
    [TestCase(SenderErrorCategory.ParseError, SenderErrorPolicy.Halt)]
    [TestCase(SenderErrorCategory.InternalError, SenderErrorPolicy.Halt)]
    [TestCase(SenderErrorCategory.SecurityError, SenderErrorPolicy.Halt)]
    [TestCase(SenderErrorCategory.ProtocolViolation, SenderErrorPolicy.Halt)]
    [TestCase(SenderErrorCategory.Unknown, SenderErrorPolicy.Halt)]
    public void DefaultPolicy_MatchesSpec(SenderErrorCategory category, SenderErrorPolicy expected)
    {
        Assert.That(QwpErrorClassifier.DefaultPolicy(category), Is.EqualTo(expected));
    }

    [Test]
    public void ResolvePolicy_NullResolver_FallsBackToDefault()
    {
        Assert.That(
            QwpErrorClassifier.ResolvePolicy(SenderErrorCategory.SchemaMismatch, resolver: null),
            Is.EqualTo(SenderErrorPolicy.DropAndContinue));
    }

    [Test]
    public void ResolvePolicy_ResolverWins_ForOverridableCategories()
    {
        Assert.That(
            QwpErrorClassifier.ResolvePolicy(
                SenderErrorCategory.SchemaMismatch,
                _ => SenderErrorPolicy.Halt),
            Is.EqualTo(SenderErrorPolicy.Halt));
    }

    [TestCase(SenderErrorCategory.ProtocolViolation)]
    [TestCase(SenderErrorCategory.Unknown)]
    public void ResolvePolicy_AlwaysHalt_ForFatalCategories(SenderErrorCategory category)
    {
        Assert.That(
            QwpErrorClassifier.ResolvePolicy(category, _ => SenderErrorPolicy.DropAndContinue),
            Is.EqualTo(SenderErrorPolicy.Halt));
    }
}
