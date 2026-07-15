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

    // NACK policy v2: no silent drop. WRITE_ERROR / INTERNAL_ERROR / UNKNOWN retry; the
    // deterministic-under-replay categories latch terminal.
    [TestCase(SenderErrorCategory.WriteError, SenderErrorPolicy.Retriable)]
    [TestCase(SenderErrorCategory.InternalError, SenderErrorPolicy.Retriable)]
    [TestCase(SenderErrorCategory.Unknown, SenderErrorPolicy.Retriable)]
    [TestCase(SenderErrorCategory.SchemaMismatch, SenderErrorPolicy.Terminal)]
    [TestCase(SenderErrorCategory.ParseError, SenderErrorPolicy.Terminal)]
    [TestCase(SenderErrorCategory.SecurityError, SenderErrorPolicy.Terminal)]
    [TestCase(SenderErrorCategory.ProtocolViolation, SenderErrorPolicy.Terminal)]
    public void DefaultPolicy_MatchesSpec(SenderErrorCategory category, SenderErrorPolicy expected)
    {
        Assert.That(QwpErrorClassifier.DefaultPolicy(category), Is.EqualTo(expected));
    }

    [Test]
    public void ResolvePolicy_NullResolver_FallsBackToDefault()
    {
        Assert.That(
            QwpErrorClassifier.ResolvePolicy(SenderErrorCategory.WriteError, resolver: null),
            Is.EqualTo(SenderErrorPolicy.Retriable));
    }

    [Test]
    public void ResolvePolicy_ResolverCanUpgradeRetriableToTerminal()
    {
        Assert.That(
            QwpErrorClassifier.ResolvePolicy(
                SenderErrorCategory.WriteError,
                _ => SenderErrorPolicy.Terminal),
            Is.EqualTo(SenderErrorPolicy.Terminal));
    }

    // A user resolver must never downgrade a deterministic (terminal-default) rejection to retriable —
    // that would spin the reconnect machinery on a poison frame forever.
    [TestCase(SenderErrorCategory.SchemaMismatch)]
    [TestCase(SenderErrorCategory.ParseError)]
    [TestCase(SenderErrorCategory.SecurityError)]
    [TestCase(SenderErrorCategory.ProtocolViolation)]
    public void ResolvePolicy_AlwaysTerminal_ForDeterministicCategories(SenderErrorCategory category)
    {
        Assert.That(
            QwpErrorClassifier.ResolvePolicy(category, _ => SenderErrorPolicy.Retriable),
            Is.EqualTo(SenderErrorPolicy.Terminal));
    }
}
