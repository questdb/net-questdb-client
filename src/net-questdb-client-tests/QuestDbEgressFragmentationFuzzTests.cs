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

using System.Text.Json;
using NUnit.Framework;
using QuestDB;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_tests;

/// <summary>Live-server egress fragmentation fuzz; port of egress_live_server_fragmentation_fuzz.rs.</summary>
[TestFixture]
public class QuestDbEgressFragmentationFuzzTests
{
    private const ulong DefaultSeed = 0x5EED_1234_ABCD_0001UL;

    [Test]
    public Task FragmentedBackToBackQueries() => RunAsync(
        httpPort: 19300, ilpPort: 19309, testName: nameof(FragmentedBackToBackQueries),
        async (srv, http) =>
        {
            var chunk = PickChunk(SeedFor(nameof(FragmentedBackToBackQueries)));
            TestContext.Progress.WriteLine($"FragmentedBackToBackQueries chunk={chunk}");
            await ExecAsync(http, "drop table if exists btb");
            await ExecAsync(http, "create table btb(id LONG, v DOUBLE, ts TIMESTAMP) " +
                                  "timestamp(ts) partition by day wal");
            await ExecAsync(http, "insert into btb select x, CAST(x * 2.5 AS DOUBLE), x::TIMESTAMP " +
                                  "from long_sequence(8000)");
            await WaitForRowsAsync(http, "btb", 8000);

            using var client = QueryClient.New(EgressConn(srv));
            for (var q = 0; q < 5; q++)
            {
                var (rows, idSum) = SumId(client, "SELECT * FROM btb");
                Assert.That(rows, Is.EqualTo(8000), $"q={q} row_count drift");
                Assert.That(idSum, Is.EqualTo(ExpectedSum(8000)), $"q={q} id_sum drift");
            }
        });

    [Test]
    public Task FragmentedCreditFlow() => RunAsync(
        httpPort: 19310, ilpPort: 19319, testName: nameof(FragmentedCreditFlow),
        async (srv, http) =>
        {
            var chunk = PickChunk(SeedFor(nameof(FragmentedCreditFlow)));
            TestContext.Progress.WriteLine($"FragmentedCreditFlow chunk={chunk}");
            await ExecAsync(http, "drop table if exists cf");
            await ExecAsync(http, "create table cf as ( " +
                                  "select x as id, x::TIMESTAMP as ts from long_sequence(20000) " +
                                  ") timestamp(ts) partition by day wal");
            await WaitForRowsAsync(http, "cf", 20000);

            using var client = QueryClient.New(new QueryOptions(EgressConn(srv)) { initial_credit = 2048 });
            var (rows, idSum) = SumId(client, "SELECT * FROM cf");
            Assert.That(rows, Is.EqualTo(20000), "row_count");
            Assert.That(idSum, Is.EqualTo(ExpectedSum(20000)), "id_sum");
        });

    [Test]
    public Task FragmentedStreamingBigResult() => RunAsync(
        httpPort: 19320, ilpPort: 19329, testName: nameof(FragmentedStreamingBigResult),
        async (srv, http) =>
        {
            var chunk = PickChunk(SeedFor(nameof(FragmentedStreamingBigResult)));
            TestContext.Progress.WriteLine($"FragmentedStreamingBigResult chunk={chunk}");
            await ExecAsync(http, "drop table if exists bigt");
            await ExecAsync(http, "create table bigt as ( " +
                                  "select x as id, CAST(x * 1.5 AS DOUBLE) as v, " +
                                  "CAST('s_' || (x % 100) AS SYMBOL) as s, x::TIMESTAMP as ts " +
                                  "from long_sequence(50000) " +
                                  ") timestamp(ts) partition by day wal");
            await WaitForRowsAsync(http, "bigt", 50000);

            using var client = QueryClient.New(EgressConn(srv));
            var (rows, idSum) = SumId(client, "SELECT * FROM bigt");
            Assert.That(rows, Is.EqualTo(50000), "row_count");
            Assert.That(idSum, Is.EqualTo(ExpectedSum(50000)), "id_sum");
        });

    [Test]
    public Task HandshakeSurvivesMicroChunk() => RunAsync(
        httpPort: 19330, ilpPort: 19339, testName: nameof(HandshakeSurvivesMicroChunk),
        async (srv, http) =>
        {
            TestContext.Progress.WriteLine("HandshakeSurvivesMicroChunk chunk=5 (pinned)");
            await ExecAsync(http, "drop table if exists tiny");
            await ExecAsync(http, "create table tiny(id LONG, ts TIMESTAMP) timestamp(ts) partition by day wal");
            await ExecAsync(http, "insert into tiny select x, x::TIMESTAMP from long_sequence(3)");
            await WaitForRowsAsync(http, "tiny", 3);

            using var client = QueryClient.New(EgressConn(srv));
            var (rows, idSum) = SumId(client, "SELECT * FROM tiny");
            Assert.That(rows, Is.EqualTo(3), "row_count");
            Assert.That(idSum, Is.EqualTo(ExpectedSum(3)), "id_sum");
        }, fixedChunk: 5);

    private static async Task RunAsync(
        int httpPort, int ilpPort, string testName, Func<QuestDbManager, string, Task> body, int? fixedChunk = null)
    {
        var chunk = fixedChunk ?? PickChunk(SeedFor(testName));
        var chunkStr = chunk.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var server = new QuestDbManager(ilpPort, httpPort, new[]
        {
            $"debug.http.force.recv.fragmentation.chunk.size={chunkStr}",
            $"debug.http.force.send.fragmentation.chunk.size={chunkStr}",
        });

        try
        {
            await server.StartAsync();
        }
        catch (Exception ex) when (ex is InvalidOperationException or FileNotFoundException)
        {
            await server.DisposeAsync();
            Assert.Ignore($"QWP egress fragmentation fuzz needs a QuestDB master build: {ex.Message}");
            return;
        }

        try
        {
            await body(server, server.GetHttpEndpoint());
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    private static string EgressConn(QuestDbManager srv, string extra = "")
        => $"ws::addr={srv.GetWebSocketEndpoint()};path={QwpConstants.ReadPath};target=any;{extra}";

    private static (int RowCount, long IdSum) SumId(IQwpQueryClient client, string sql)
    {
        var handler = new SumIdHandler();
        client.Execute(sql, handler);
        return (handler.RowCount, handler.IdSum);
    }

    private static long ExpectedSum(int n) => (long)n * (n + 1) / 2;

    private static int PickChunk(ulong seed)
    {
        unchecked
        {
            var z = seed + 0x9E37_79B9_7F4A_7C15UL;
            z = (z ^ (z >> 30)) * 0xBF58_476D_1CE4_E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D0_49BB_1331_11EBUL;
            z ^= z >> 31;
            return 1 + (int)(z % 500);
        }
    }

    private static ulong SeedFor(string testName)
    {
        var raw = Environment.GetEnvironmentVariable("QWP_EGRESS_FUZZ_SEED");
        var baseSeed = DefaultSeed;
        if (!string.IsNullOrWhiteSpace(raw))
        {
            raw = raw.Trim();
            baseSeed = raw.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? Convert.ToUInt64(raw.Substring(2), 16)
                : ulong.Parse(raw);
        }

        unchecked
        {
            var hash = 0xCBF2_9CE4_8422_2325UL;
            foreach (var b in System.Text.Encoding.UTF8.GetBytes(testName))
            {
                hash ^= b;
                hash *= 0x100_0000_01B3UL;
            }
            var combined = baseSeed + hash;
            TestContext.Progress.WriteLine($"[qwp_egress_fuzz seed] {testName} seed=0x{combined:x16}");
            return combined;
        }
    }

    private static async Task ExecAsync(string httpEndpoint, string sql)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
        using var resp = await client.GetAsync($"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}");
        Assert.That((int)resp.StatusCode, Is.InRange(200, 399), $"exec failed ({(int)resp.StatusCode}): {sql}");
    }

    private static async Task WaitForRowsAsync(string httpEndpoint, string table, int expected)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        var deadline = DateTime.UtcNow.AddSeconds(90);
        long last = -1;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var resp = await client.GetAsync(
                    $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString($"select count() from \"{table}\"")}");
                if (resp.IsSuccessStatusCode)
                {
                    using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
                    if (json.RootElement.TryGetProperty("dataset", out var ds) && ds.GetArrayLength() > 0)
                    {
                        last = ds[0][0].GetInt64();
                        if (last >= expected)
                        {
                            return;
                        }
                    }
                }
            }
            catch
            {
            }

            await Task.Delay(80);
        }

        Assert.Fail($"{table} did not reach {expected} rows within 90s; last count={last}");
    }

    private sealed class SumIdHandler : QwpColumnBatchHandler
    {
        public int RowCount { get; private set; }
        public long IdSum { get; private set; }

        public override void OnBatch(QwpColumnBatch batch)
        {
            for (var r = 0; r < batch.RowCount; r++)
            {
                if (!batch.IsNull(0, r))
                {
                    IdSum = unchecked(IdSum + batch.GetLongValue(0, r));
                }
            }
            RowCount += batch.RowCount;
        }

        public override void OnError(byte status, string message)
            => Assert.Fail($"unexpected egress error: status={status}, msg={message}");
    }
}

#endif
