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

namespace net_questdb_client_zstd_absent_tests;

// This project references only net-questdb-client (no net-questdb-client-zstd, no ZstdSharp.Port),
// matching a consumer who never installed the optional zstd plugin. It proves QwpZstdCodec's
// Assembly.Load("net-questdb-client-zstd") genuinely fails to resolve in that shape — not just
// that the in-repo unit tests, which always carry the plugin via ProjectReference, happen to pass.
[TestFixture]
public class ZstdPluginAbsentTests
{
    // Mirrors QwpConstants.ReadPath / HeaderAcceptEncoding: this project can't reference those
    // (internal, and deliberately not using InternalsVisibleTo — a real external consumer wouldn't
    // have it either), so it observes the wire contract the same way an external consumer would.
    private const string ReadPath = "/read/v1";
    private const string HeaderAcceptEncoding = "X-QWP-Accept-Encoding";

    [Test]
    public void ExplicitZstd_PluginAbsent_ThrowsConfigErrorBeforeConnecting()
    {
        // No server needed: EnsureZstdAvailableIfRequired throws before any address is touched,
        // so even an unreachable/non-listening address must fail with ConfigError, not SocketError.
        var ex = Assert.Throws<IngressError>(() =>
            QueryClient.New("ws::addr=127.0.0.1:1;target=any;compression=zstd;"));

        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ConfigError));
        StringAssert.Contains("net-questdb-client-zstd", ex.Message);
    }

    [Test]
    public async Task AutoCompression_PluginAbsent_DegradesToRawWithoutThrowing()
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
            $"expected no {HeaderAcceptEncoding} header when the zstd plugin is absent, got '{accept}'");
    }

    [Test]
    public void RawCompression_PluginAbsent_WorksNormally()
    {
        Assert.DoesNotThrowAsync(async () =>
        {
            await using var server = new DummyQwpServer(new DummyQwpServerOptions
            {
                Path = ReadPath,
                NegotiatedVersion = "1",
            });
            await server.StartAsync();

            using var client = QueryClient.New(
                $"ws::addr={server.Uri.Authority};path={ReadPath};target=any;compression=raw;");
        });
    }
}
