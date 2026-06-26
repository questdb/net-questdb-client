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
using QuestDB;
using QuestDB.Enums;
using QuestDB.Utils;

namespace net_questdb_client_tests;

[TestFixture]
[NonParallelizable]
public class SenderFromEnvTests
{
    private const string EnvVar = "QDB_CLIENT_CONF";
    private string? _saved;

    [SetUp]
    public void SaveEnv() => _saved = Environment.GetEnvironmentVariable(EnvVar);

    [TearDown]
    public void RestoreEnv() => Environment.SetEnvironmentVariable(EnvVar, _saved);

    [Test]
    public void SenderFromEnv_BlankEnv_ThrowsConfigError()
    {
        Environment.SetEnvironmentVariable(EnvVar, null);
        var ex = Assert.Throws<IngressError>(() => Sender.FromEnv());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
        Assert.That(ex.Message, Does.Contain(EnvVar));
    }

    [Test]
    public void SenderFromEnv_WhitespaceEnv_ThrowsConfigError()
    {
        Environment.SetEnvironmentVariable(EnvVar, "   ");
        var ex = Assert.Throws<IngressError>(() => Sender.FromEnv());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
    }

    [Test]
    public void SenderFromEnv_ValidConfig_ProducesSender()
    {
        Environment.SetEnvironmentVariable(EnvVar, "http::addr=localhost:9000;");
        using var sender = Sender.FromEnv();
        Assert.That(sender, Is.Not.Null);
    }

    [Test]
    public void SenderFromEnv_BadConfig_PropagatesParserError()
    {
        Environment.SetEnvironmentVariable(EnvVar, "no_scheme_separator");
        Assert.Throws<IngressError>(() => Sender.FromEnv());
    }

    [Test]
    public void QueryClientFromEnv_BlankEnv_ThrowsConfigError()
    {
        Environment.SetEnvironmentVariable(EnvVar, null);
        var ex = Assert.Throws<IngressError>(() => QueryClient.FromEnv());
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
        Assert.That(ex.Message, Does.Contain(EnvVar));
    }

    [Test]
    public void QueryClientFromEnv_NonWsScheme_RejectsAtFactory()
    {
        Environment.SetEnvironmentVariable(EnvVar, "http::addr=localhost:9000;");
        Assert.Throws<IngressError>(() => QueryClient.FromEnv());
    }
}
