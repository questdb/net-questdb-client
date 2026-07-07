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

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using QuestDB;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_benchmarks;

/// <summary>
///     Single-query round-trip latency for the QWP egress client with an HTTP <c>/exec</c>
///     baseline — the read-side counterpart of <see cref="BenchLatencyWs" />. Each iteration is
///     one full query RTT, so IterationCount is the p50 / p95 / min / max sample size. Requires
///     <c>QDB_BENCH_ENDPOINT</c> pointing at a live QuestDB master — no dummy fallback because
///     hand-rolling <c>RESULT_BATCH</c> frames would measure the fixture, not the client.
///     <para>
///     The bind benchmark sends a parameterised query over QWP. HTTP <c>/exec</c> has no native
///     bind, so its baseline inlines the value into the SQL text — functionally equivalent work
///     for a latency comparison (server still parses + plans + executes the same query shape).
///     </para>
/// </summary>
[Config(typeof(LatencySamplingConfig))]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class BenchQueryLatencyWs
{
    private const string LatencyTable = "bench_query_latency";

    private string _endpoint = null!;
    private HttpClient _http = null!;
    private IQwpQueryClient _ws = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _endpoint = Environment.GetEnvironmentVariable("QDB_BENCH_ENDPOINT")
            ?? throw new InvalidOperationException(
                "BenchQueryLatencyWs requires QDB_BENCH_ENDPOINT (e.g. 127.0.0.1:9000) — egress bench runs against a live QuestDB master.");

        _http = new HttpClient { Timeout = TimeSpan.FromMinutes(1) };
        _ws = QueryClient.New($"ws::addr={_endpoint};");

        await ExecAsync($"DROP TABLE IF EXISTS {LatencyTable}");
        await ExecAsync($"CREATE TABLE {LatencyTable} (id LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        await ExecAsync($"INSERT INTO {LatencyTable} VALUES (1, now())");
        await WaitForSeedRowAsync();

        // Prime codec state + schema registry so the measurement window isn't paying first-query cost.
        await WsCountRowsAsync($"SELECT id FROM {LatencyTable}");
        await WsCountRowsAsync("SELECT x FROM long_sequence(10) WHERE x = $1", b => b.SetLong(0, 1));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _ws?.Dispose(); } catch { }
        try { await ExecAsync($"DROP TABLE IF EXISTS {LatencyTable}"); } catch { }
        _http?.Dispose();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("SingleRow")]
    public async Task<long> Http_SelectSingleRow() => await HttpExecAsync($"SELECT id FROM {LatencyTable}");

    [Benchmark, BenchmarkCategory("SingleRow")]
    public async Task<int> Ws_SelectSingleRow() => await WsCountRowsAsync($"SELECT id FROM {LatencyTable}");

    [Benchmark(Baseline = true), BenchmarkCategory("Bind")]
    public async Task<long> Http_SelectWhereBind()
    {
        var x = Random.Shared.NextInt64(1, 11);
        return await HttpExecAsync($"SELECT x FROM long_sequence(10) WHERE x = {x}");
    }

    [Benchmark, BenchmarkCategory("Bind")]
    public async Task<int> Ws_SelectWhereBind()
    {
        var x = Random.Shared.NextInt64(1, 11);
        return await WsCountRowsAsync("SELECT x FROM long_sequence(10) WHERE x = $1", b => b.SetLong(0, x));
    }

    private async Task<int> WsCountRowsAsync(string sql, QwpBindSetter? binds = null)
    {
        await using var reader = binds is null
            ? await _ws.ExecuteReaderAsync(sql)
            : await _ws.ExecuteReaderAsync(sql, binds);
        var rows = 0;
        while (await reader.ReadBatchAsync())
        {
            rows += reader.Current.RowCount;
        }
        return rows;
    }

    private async Task<long> HttpExecAsync(string sql)
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString(sql)}";
        using var resp = await _http.GetAsync(url);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync();
        return body.Length;
    }

    private async Task ExecAsync(string sql)
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString(sql)}";
        using var resp = await _http.GetAsync(url);
        resp.EnsureSuccessStatusCode();
    }

    private async Task WaitForSeedRowAsync()
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString($"SELECT count(*) FROM {LatencyTable}")}";
        for (var attempt = 0; attempt < 60; attempt++)
        {
            try
            {
                using var resp = await _http.GetAsync(url);
                if (resp.IsSuccessStatusCode)
                {
                    var body = await resp.Content.ReadAsStringAsync();
                    if (body.Contains("[[1]]", StringComparison.Ordinal)) return;
                }
            }
            catch { }
            await Task.Delay(250);
        }

        throw new TimeoutException($"table {LatencyTable} did not receive its seed row within the setup window");
    }
}

#endif
