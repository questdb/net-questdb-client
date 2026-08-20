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
using dummy_http_server;

namespace net_questdb_client_zstd_broken_tests;

// This project's own build output is named net-questdb-client-zstd (see the .csproj) and carries a
// fake QuestDB.Zstd.ZstdDecompressor (FakeZstdDecompressor.cs) whose constructor always throws.
// QwpZstdCodec.TryResolveFactory's Assembly.Load("net-questdb-client-zstd") therefore finds THIS
// assembly, and GetType("QuestDB.Zstd.ZstdDecompressor") successfully resolves the fake type — so
// unlike net-questdb-client-zstd-absent-tests (where Assembly.Load itself fails), IsAvailable can
// only read false here because TryResolveFactory's CreateInstance().Dispose() probe actually runs
// the constructor and observes it throw. This is the "assembly present, its own dependency missing"
// state the probe (added in commit b482e00) defends against; reverting just that probe step would
// make IsAvailable read true here (GetType alone succeeds) and both tests below would fail.
[TestFixture]
public class ZstdPluginBrokenTests
{
    private const string ReadPath = "/read/v1";
    private const string HeaderAcceptEncoding = "X-QWP-Accept-Encoding";

    [Test]
    public void ExplicitZstd_PluginBroken_ThrowsConfigErrorBeforeConnecting()
    {
        // No server needed: EnsureZstdAvailableIfRequired throws before any address is touched.
        // Without the probe, IsAvailable would incorrectly read true, this would instead attempt to
        // connect to the unreachable address, and the assertions below would fail (a SocketError,
        // not a ConfigError, mentioning nothing about the zstd package).
        var ex = Assert.Throws<IngressError>(() =>
            QueryClient.New("ws::addr=127.0.0.1:1;target=any;compression=zstd;"));

        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
        StringAssert.Contains("net-questdb-client-zstd", ex.Message);
    }

    [Test]
    public async Task AutoCompression_PluginBroken_DegradesToRawWithoutThrowing()
    {
        await using var server = new DummyQwpServer(new DummyQwpServerOptions
        {
            Path = ReadPath,
            NegotiatedVersion = "1",
        });
        await server.StartAsync();

        using var client = QueryClient.New(
            $"ws::addr={server.Uri.Authority};path={ReadPath};target=any;compression=auto;");

        Assert.That(server.LastUpgradeHeaders, Is.Not.Null);
        var hasAcceptEncoding = server.LastUpgradeHeaders!.TryGetValue(HeaderAcceptEncoding, out var accept);
        Assert.That(hasAcceptEncoding, Is.False,
            $"expected no {HeaderAcceptEncoding} header when the zstd plugin is broken, got '{accept}'");
    }
}
