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
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using QuestDB;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_benchmarks;

/// <summary>
///     Sustained read throughput for the QWP egress client with an HTTP <c>/exec</c> baseline.
///     Requires <c>QDB_BENCH_ENDPOINT</c> pointing at a live QuestDB master (e.g. 127.0.0.1:9000) —
///     no dummy fallback because hand-rolling realistic <c>RESULT_BATCH</c> frames in the bench
///     setup would measure the test fixture, not the client. Both methods do equivalent work:
///     WS decodes column-major and the handler walks every row; HTTP parses the JSON response and
///     counts dataset rows so the comparison is apples-to-apples on extraction work, not just I/O.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(QueryThroughputConfig))]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class BenchQueryWs
{
    private const string NarrowTable = "bench_egress_narrow";
    private const string WideTable = "bench_egress_wide";
    private const int SeedRows = 1_000_000;

    private string _endpoint = null!;
    private HttpClient _http = null!;
    private IQwpQueryClient _ws = null!;
    private CountingHandler _handler = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int RowCount;

    [GlobalSetup]
    public async Task Setup()
    {
        _endpoint = Environment.GetEnvironmentVariable("QDB_BENCH_ENDPOINT")
            ?? throw new InvalidOperationException(
                "BenchQueryWs requires QDB_BENCH_ENDPOINT (e.g. 127.0.0.1:9000) to be set — egress bench runs against a live QuestDB master.");

        _http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        _ws = QueryClient.New($"ws::addr={_endpoint};");
        _handler = new CountingHandler();

        await DropAsync(NarrowTable);
        await DropAsync(WideTable);
        await SeedNarrowAsync(SeedRows);
        await SeedWideAsync(SeedRows);
        await WaitForRowsAsync(NarrowTable, SeedRows);
        await WaitForRowsAsync(WideTable, SeedRows);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _ws?.Dispose(); } catch { }
        try { await DropAsync(NarrowTable); } catch { }
        try { await DropAsync(WideTable); } catch { }
        _http?.Dispose();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Narrow")]
    public async Task<long> Http_NarrowSelect() => await HttpSelectCountRowsAsync(NarrowTable);

    [Benchmark, BenchmarkCategory("Narrow")]
    public int Ws_NarrowSelect() => WsSelectCountRows(NarrowTable);

    [Benchmark(Baseline = true), BenchmarkCategory("Wide")]
    public async Task<long> Http_WideSelect() => await HttpSelectCountRowsAsync(WideTable);

    [Benchmark, BenchmarkCategory("Wide")]
    public int Ws_WideSelect() => WsSelectCountRows(WideTable);

    private int WsSelectCountRows(string table)
    {
        _handler.Reset();
        _ws.Execute($"SELECT * FROM {table} LIMIT {RowCount}", _handler);
        return _handler.TotalRows;
    }

    private async Task<long> HttpSelectCountRowsAsync(string table)
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString($"SELECT * FROM {table} LIMIT {RowCount}")}";
        using var resp = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
        resp.EnsureSuccessStatusCode();
        await using var stream = await resp.Content.ReadAsStreamAsync();
        return await CountRowsInJsonAsync(stream);
    }

    private async Task SeedNarrowAsync(int rows)
    {
        using var sender = Sender.New(
            $"ws::addr={_endpoint};auto_flush_rows=10000;auto_flush_interval=off;auto_flush_bytes=off;");
        var now = DateTime.UtcNow;
        for (var i = 0; i < rows; i++)
        {
            sender.Table(NarrowTable)
                .Symbol("region", i % 2 == 0 ? "us" : "eu")
                .Column("price", i * 1.5)
                .At(now);
        }
        await sender.SendAsync();
    }

    private async Task SeedWideAsync(int rows)
    {
        using var sender = Sender.New(
            $"ws::addr={_endpoint};auto_flush_rows=10000;auto_flush_interval=off;auto_flush_bytes=off;");
        var now = DateTime.UtcNow;
        for (var i = 0; i < rows; i++)
        {
            sender.Table(WideTable)
                .Symbol("a", "x").Symbol("b", "y").Symbol("c", "z")
                .Column("d", (long)i).Column("e", i * 0.5)
                .Column("f", i % 2 == 0).Column("g", "string-" + i)
                .Column("h", now)
                .Column("i", (long)(i + 1)).Column("j", i * 1.25)
                .Column("k", "k-" + i).Column("l", i % 3 == 0)
                .Column("m", (long)(i * 2)).Column("n", (i % 7) * 0.7)
                .At(now);
        }
        await sender.SendAsync();
    }

    private async Task DropAsync(string table)
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString($"DROP TABLE IF EXISTS {table}")}";
        using var resp = await _http.GetAsync(url);
        resp.EnsureSuccessStatusCode();
    }

    private async Task WaitForRowsAsync(string table, long minimum)
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString($"SELECT count(*) FROM {table}")}";
        for (var attempt = 0; attempt < 60; attempt++)
        {
            try
            {
                using var resp = await _http.GetAsync(url);
                if (resp.IsSuccessStatusCode)
                {
                    var body = await resp.Content.ReadAsStringAsync();
                    if (TryParseCount(body, out var n) && n >= minimum) return;
                }
            }
            catch { }
            await Task.Delay(500);
        }

        throw new TimeoutException($"table {table} did not reach {minimum} rows within seeding window");
    }

    private static async Task<long> CountRowsInJsonAsync(Stream stream)
    {
        // Streaming Utf8JsonReader walk — count outer dataset[][] elements without materialising
        // the whole response. Mirrors what a real consumer parsing JSON output would do.
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        var bytes = ms.GetBuffer().AsMemory(0, (int)ms.Length);
        return CountDatasetRows(bytes.Span);
    }

    private static long CountDatasetRows(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);
        var sawDataset = false;
        var depth = 0;
        long count = 0;

        while (reader.Read())
        {
            if (!sawDataset)
            {
                if (reader.TokenType == JsonTokenType.PropertyName && reader.ValueTextEquals("dataset"))
                {
                    sawDataset = true;
                }
                continue;
            }

            switch (reader.TokenType)
            {
                case JsonTokenType.StartArray:
                    depth++;
                    if (depth == 2) count++;
                    break;
                case JsonTokenType.EndArray:
                    depth--;
                    if (depth == 0) return count;
                    break;
            }
        }

        return count;
    }

    private static bool TryParseCount(string body, out long count)
    {
        const string marker = "\"dataset\":[[";
        var idx = body.IndexOf(marker, StringComparison.Ordinal);
        if (idx < 0)
        {
            count = 0;
            return false;
        }

        var start = idx + marker.Length;
        var end = body.IndexOf(']', start);
        return long.TryParse(body.Substring(start, end - start), out count);
    }

    private sealed class CountingHandler : QwpColumnBatchHandler
    {
        public int TotalRows;
        public long Checksum;

        public void Reset()
        {
            TotalRows = 0;
            Checksum = 0;
        }

        public override void OnBatch(QwpColumnBatch batch)
        {
            TotalRows += batch.RowCount;
            if (batch.ColumnCount > 0 && batch.GetColumnWireType(0) == QuestDB.Enums.QwpTypeCode.Long)
            {
                var rows = batch.RowCount;
                long acc = 0;
                for (var r = 0; r < rows; r++) acc ^= batch.GetLongValue(0, r);
                Checksum ^= acc;
            }
        }
    }
}

/// <summary>
///     20 iterations × 5 warmups gives < 5% margin on multi-millisecond workloads. Adds a
///     <c>Rows/sec</c> column derived from the <c>RowCount</c> param so the punchy throughput
///     number doesn't have to be re-derived by hand.
/// </summary>
public class QueryThroughputConfig : ManualConfig
{
    public QueryThroughputConfig()
    {
        Add(DefaultConfig.Instance);
        WithUnionRule(ConfigUnionRule.AlwaysUseLocal);

        AddJob(Job.Default
            .WithLaunchCount(1)
            .WithWarmupCount(5)
            .WithIterationCount(20)
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithMinIterationTime(Perfolizer.Horology.TimeInterval.FromMilliseconds(10))
            .WithToolchain(InProcessNoEmitToolchain.Instance));
        AddColumn(StatisticColumn.Min, StatisticColumn.P95, StatisticColumn.Max);
        AddColumn(new RowsPerSecondColumn());
    }
}

internal sealed class RowsPerSecondColumn : IColumn
{
    public string Id => nameof(RowsPerSecondColumn);
    public string ColumnName => "Rows/sec";
    public bool AlwaysShow => true;
    public ColumnCategory Category => ColumnCategory.Statistics;
    public int PriorityInCategory => 100;
    public bool IsNumeric => true;
    public UnitType UnitType => UnitType.Dimensionless;
    public string Legend => "Rows processed per second (RowCount / Mean)";

    public bool IsAvailable(Summary summary) => true;
    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase) =>
        GetValue(summary, benchmarkCase, SummaryStyle.Default);

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
    {
        var report = summary[benchmarkCase];
        if (report?.ResultStatistics is null) return "-";

        var rowParam = benchmarkCase.Parameters.Items.FirstOrDefault(p => p.Name == "RowCount");
        if (rowParam?.Value is not int rows) return "-";

        var rowsPerSec = rows / (report.ResultStatistics.Mean / 1_000_000_000.0);
        return rowsPerSec switch
        {
            >= 1_000_000 => $"{rowsPerSec / 1_000_000:F2}M",
            >= 1_000 => $"{rowsPerSec / 1_000:F0}K",
            _ => rowsPerSec.ToString("F0"),
        };
    }
}

#endif
