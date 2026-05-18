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
using BenchmarkDotNet.Diagnosers;
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
///     WS decodes column-major and the handler reads every cell through the typed accessors; HTTP
///     streams the JSON response and extracts every cell value (numbers + strings), so the
///     QWP-vs-HTTP ratio reflects decode cost on a matched workload, not just I/O.
/// </summary>
[Config(typeof(QueryThroughputConfig))]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class BenchQueryWs
{
    private const string NarrowTable = "bench_egress_narrow";
    private const string WideTable = "bench_egress_wide";
    private const string NarrowColumns = "ts, id, price, sym, note";
    private const string WideColumns = "ts, id, price, sym, note, d1, d2, d3, d4, d5, s1, s2, s3, s4, s5";
    private const int SeedRows = 10_000_000;

    private string _endpoint = null!;
    private HttpClient _http = null!;
    private IQwpQueryClient _wsNarrow = null!;
    private IQwpQueryClient _wsWide = null!;
    private CountingHandler _handler = null!;

    [Params(10_000, 100_000, 1_000_000, 10_000_000)]
    public int RowCount;

    [GlobalSetup]
    public async Task Setup()
    {
        _endpoint = Environment.GetEnvironmentVariable("QDB_BENCH_ENDPOINT")
            ?? throw new InvalidOperationException(
                "BenchQueryWs requires QDB_BENCH_ENDPOINT (e.g. 127.0.0.1:9000) to be set — egress bench runs against a live QuestDB master.");

        _http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        _wsNarrow = QueryClient.New($"ws::addr={_endpoint};");
        // The wide schema's high-cardinality symbol columns make a default-sized RESULT_BATCH
        // (16384 rows) overflow the server's egress send buffer and abort the query, so cap the
        // per-batch row count for wide. Narrow rows are small enough to stay uncapped.
        _wsWide = QueryClient.New($"ws::addr={_endpoint};max_batch_rows=4096;");
        _handler = new CountingHandler();

        await DropAsync(NarrowTable);
        await DropAsync(WideTable);
        await CreateNarrowTableAsync();
        await CreateWideTableAsync();
        await SeedNarrowAsync(SeedRows);
        await SeedWideAsync(SeedRows);
        await WaitForRowsAsync(NarrowTable, SeedRows);
        await WaitForRowsAsync(WideTable, SeedRows);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _wsNarrow?.Dispose(); } catch { }
        try { _wsWide?.Dispose(); } catch { }
        try { await DropAsync(NarrowTable); } catch { }
        try { await DropAsync(WideTable); } catch { }
        _http?.Dispose();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Narrow")]
    public async Task<long> Http_NarrowSelect() => await HttpSelectAsync(NarrowTable, NarrowColumns);

    [Benchmark, BenchmarkCategory("Narrow")]
    public long Ws_NarrowSelect() => WsSelect(_wsNarrow, NarrowTable, NarrowColumns);

    [Benchmark(Baseline = true), BenchmarkCategory("Wide")]
    public async Task<long> Http_WideSelect() => await HttpSelectAsync(WideTable, WideColumns);

    [Benchmark, BenchmarkCategory("Wide")]
    public long Ws_WideSelect() => WsSelect(_wsWide, WideTable, WideColumns);

    private long WsSelect(IQwpQueryClient ws, string table, string columns)
    {
        _handler.Reset();
        ws.Execute($"SELECT {columns} FROM {table} LIMIT {RowCount}", _handler);
        return _handler.TotalRows ^ _handler.Checksum;
    }

    private async Task<long> HttpSelectAsync(string table, string columns)
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString($"SELECT {columns} FROM {table} LIMIT {RowCount}")}";
        using var resp = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
        resp.EnsureSuccessStatusCode();
        await using var stream = await resp.Content.ReadAsStreamAsync();
        return await ParseRowsStreamingAsync(stream);
    }

    private Task CreateNarrowTableAsync() =>
        ExecAsync(
            $"CREATE TABLE {NarrowTable} (ts TIMESTAMP, id LONG, price DOUBLE, sym SYMBOL, note VARCHAR) " +
            "TIMESTAMP(ts) PARTITION BY HOUR WAL");

    private Task CreateWideTableAsync() =>
        ExecAsync(
            $"CREATE TABLE {WideTable} (ts TIMESTAMP, id LONG, price DOUBLE, sym SYMBOL, note VARCHAR, " +
            "d1 DOUBLE, d2 DOUBLE, d3 DOUBLE, d4 DOUBLE, d5 DOUBLE, " +
            "s1 SYMBOL capacity 200000, s2 SYMBOL capacity 200000, s3 SYMBOL capacity 200000, " +
            "s4 SYMBOL capacity 200000, s5 SYMBOL capacity 200000) " +
            "TIMESTAMP(ts) PARTITION BY HOUR WAL");

    private async Task SeedNarrowAsync(int rows)
    {
        using var sender = Sender.New(
            $"ws::addr={_endpoint};auto_flush_rows=10000;auto_flush_interval=off;auto_flush_bytes=off;");
        var now = DateTime.UtcNow;
        for (var i = 0; i < rows; i++)
        {
            sender.Table(NarrowTable)
                .Column("id", (long)i)
                .Column("price", i * 1.5)
                .Symbol("sym", i % 2 == 0 ? "us" : "eu")
                .Column("note", "note-" + i)
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
                .Column("id", (long)i)
                .Column("price", i * 1.5)
                .Symbol("sym", i % 2 == 0 ? "us" : "eu")
                .Column("note", "note-" + i)
                .Column("d1", i * 0.5).Column("d2", i * 1.25).Column("d3", (i % 7) * 0.7)
                .Column("d4", i * 2.0).Column("d5", i * 0.25)
                .Symbol("s1", "s1-" + (i % 100_000)).Symbol("s2", "s2-" + (i % 100_000))
                .Symbol("s3", "s3-" + (i % 100_000)).Symbol("s4", "s4-" + (i % 100_000))
                .Symbol("s5", "s5-" + (i % 100_000))
                .At(now);
        }
        await sender.SendAsync();
    }

    private Task DropAsync(string table) => ExecAsync($"DROP TABLE IF EXISTS {table}");

    private async Task ExecAsync(string sql)
    {
        var url = $"http://{_endpoint}/exec?query={Uri.EscapeDataString(sql)}";
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

    private static async Task<long> ParseRowsStreamingAsync(Stream stream)
    {
        // True streaming Utf8JsonReader walk — refill a buffer as the network feeds it. Avoids the
        // CopyToAsync(MemoryStream) penalty that would charge HTTP an unfair allocator tax. Every
        // cell value is extracted (not just counted) so the work matches the QWP typed accessors.
        var buffer = new byte[64 * 1024];
        var filled = 0;
        long count = 0;
        long checksum = 0;
        var sawDataset = false;
        var depth = 0;
        var state = new JsonReaderState();
        var done = false;

        while (!done)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(filled, buffer.Length - filled));
            var isFinal = read == 0;
            var consumed = ProcessChunk(buffer.AsSpan(0, filled + read), isFinal, ref state,
                ref sawDataset, ref depth, ref count, ref checksum, out var datasetEnded);

            if (datasetEnded || isFinal)
            {
                done = true;
                break;
            }

            var leftover = filled + read - consumed;
            if (leftover > 0)
            {
                if (consumed == 0)
                {
                    var grown = new byte[buffer.Length * 2];
                    Array.Copy(buffer, grown, filled + read);
                    buffer = grown;
                }
                else
                {
                    Array.Copy(buffer, consumed, buffer, 0, leftover);
                }
            }
            filled = leftover;
        }

        return count ^ checksum;
    }

    private static int ProcessChunk(
        ReadOnlySpan<byte> span, bool isFinal, ref JsonReaderState state,
        ref bool sawDataset, ref int depth, ref long count, ref long checksum, out bool datasetEnded)
    {
        datasetEnded = false;
        var reader = new Utf8JsonReader(span, isFinal, state);
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
                    if (depth == 0)
                    {
                        datasetEnded = true;
                        state = reader.CurrentState;
                        return (int)reader.BytesConsumed;
                    }
                    break;
                case JsonTokenType.Number:
                    // depth == 2 is a scalar cell inside a row array; extract it so HTTP does the
                    // same per-value decode work as the QWP typed accessors.
                    if (depth == 2)
                        checksum ^= reader.TryGetInt64(out var l)
                            ? l
                            : BitConverter.DoubleToInt64Bits(reader.GetDouble());
                    break;
                case JsonTokenType.String:
                    if (depth == 2) checksum ^= reader.ValueSpan.Length;
                    break;
                case JsonTokenType.True:
                    if (depth == 2) checksum ^= 1;
                    break;
            }
        }

        state = reader.CurrentState;
        return (int)reader.BytesConsumed;
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
            // Walk every column on every row through the typed accessors so the bench actually
            // exercises the primitive read path. Earlier shape checked only column 0; in the
            // narrow schema column 0 is a Symbol so the hot Long/Double accessor went uncalled.
            var cols = batch.ColumnCount;
            var rows = batch.RowCount;
            long acc = 0;
            for (var c = 0; c < cols; c++)
            {
                var t = batch.GetColumnWireType(c);
                switch (t)
                {
                    case QuestDB.Enums.QwpTypeCode.Long:
                    case QuestDB.Enums.QwpTypeCode.Date:
                    case QuestDB.Enums.QwpTypeCode.Timestamp:
                    case QuestDB.Enums.QwpTypeCode.TimestampNanos:
                        for (var r = 0; r < rows; r++) acc ^= batch.GetLongValue(c, r);
                        break;
                    case QuestDB.Enums.QwpTypeCode.Int:
                    case QuestDB.Enums.QwpTypeCode.IPv4:
                        for (var r = 0; r < rows; r++) acc ^= batch.GetIntValue(c, r);
                        break;
                    case QuestDB.Enums.QwpTypeCode.Double:
                        for (var r = 0; r < rows; r++)
                            acc ^= BitConverter.DoubleToInt64Bits(batch.GetDoubleValue(c, r));
                        break;
                    case QuestDB.Enums.QwpTypeCode.Float:
                        for (var r = 0; r < rows; r++)
                            acc ^= BitConverter.SingleToInt32Bits(batch.GetFloatValue(c, r));
                        break;
                    case QuestDB.Enums.QwpTypeCode.Boolean:
                        for (var r = 0; r < rows; r++) acc ^= batch.GetBoolValue(c, r) ? 1 : 0;
                        break;
                    case QuestDB.Enums.QwpTypeCode.Symbol:
                        for (var r = 0; r < rows; r++) acc ^= batch.GetSymbolId(c, r);
                        break;
                    case QuestDB.Enums.QwpTypeCode.Varchar:
                        for (var r = 0; r < rows; r++) acc ^= batch.GetStringSpan(c, r).Length;
                        break;
                }
            }
            Checksum ^= acc;
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
        // Added explicitly: AlwaysUseLocal drops the [MemoryDiagnoser] attribute's diagnoser,
        // so the Allocated column only appears if the local config carries it.
        AddDiagnoser(MemoryDiagnoser.Default);
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
