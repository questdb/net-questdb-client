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

using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using QuestDB;
using QuestDB.Senders;
using dummy_http_server;

namespace net_questdb_client_benchmarks;

/// <summary>
///     Sustained throughput for the WebSocket / QWP sender with HTTP baselines per row shape.
///     Senders live across iterations so handshake cost is amortised, not folded into the
///     per-iteration time. Each row shape (Narrow / Wide / MultiTable) has matched HTTP and WS
///     methods grouped by <see cref="BenchmarkCategoryAttribute" /> so BDN computes pairwise
///     ratios within the same workload.
/// </summary>
[Config(typeof(IngestThroughputConfig))]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class BenchInsertsWs
{
    private DummyQwpServer? _qwpServer;
    private DummyHttpServer? _httpServer;
    private string _httpEndpoint = null!;
    private string _wsEndpoint = null!;

    private ISender _httpSender = null!;
    private ISender _wsSender = null!;
    private string[] _wideStringG = null!;
    private string[] _wideStringK = null!;
    private long _rowSeq;

    [Params(100, 1000, 10000)]
    public int AutoFlushRows;

    [Params(10_000, 100_000)]
    public int Rows;

    [GlobalSetup]
    public async Task Setup()
    {
        var realEndpoint = Environment.GetEnvironmentVariable("QDB_BENCH_ENDPOINT");
        if (!string.IsNullOrEmpty(realEndpoint))
        {
            _httpEndpoint = realEndpoint;
            _wsEndpoint = realEndpoint;
        }
        else
        {
            long ackSeq = 0;
            _qwpServer = new DummyQwpServer(new DummyQwpServerOptions
            {
                FrameHandler = _ =>
                {
                    var seq = Interlocked.Increment(ref ackSeq) - 1;
                    var ack = new byte[9];
                    ack[0] = 0x00;
                    BinaryPrimitives.WriteInt64LittleEndian(ack.AsSpan(1, 8), seq);
                    return ack;
                },
            });
            await _qwpServer.StartAsync();

            var httpPort = GetFreeTcpPort();
            _httpServer = new DummyHttpServer();
            await _httpServer.StartAsync(httpPort);

            _httpEndpoint = $"localhost:{httpPort}";
            _wsEndpoint = $"127.0.0.1:{_qwpServer.Uri.Port}";
        }

        _httpSender = Sender.New(
            $"http::addr={_httpEndpoint};auto_flush_rows={AutoFlushRows};auto_flush_interval=off;auto_flush_bytes=off;");
        _wsSender = Sender.New(
            $"ws::addr={_wsEndpoint};" +
            $"auto_flush_rows={AutoFlushRows};auto_flush_interval=off;auto_flush_bytes=off;");

        _wideStringG = Enumerable.Range(0, Rows).Select(i => "string-" + i).ToArray();
        _wideStringK = Enumerable.Range(0, Rows).Select(i => "k-" + i).ToArray();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _httpSender?.Dispose(); } catch { }
        try { _wsSender?.Dispose(); } catch { }
        if (_qwpServer is not null) await _qwpServer.DisposeAsync();
        _httpServer?.Dispose();
    }

    // -- Narrow row (3 columns) ------------------------------------------------

    [Benchmark(Baseline = true), BenchmarkCategory("Narrow")]
    public Task Http_NarrowRow() => NarrowRowAsync(_httpSender);

    [Benchmark, BenchmarkCategory("Narrow")]
    public Task Ws_NarrowRow() => NarrowRowAsync(_wsSender);

    private async Task NarrowRowAsync(ISender sender)
    {
        for (var i = 0; i < Rows; i++)
        {
            sender.Table("bench")
                .Symbol("region", i % 2 == 0 ? "us" : "eu")
                .Column("price", i * 1.5)
                .At(DateTime.UtcNow);
        }

        await sender.SendAsync();
    }

    // -- Wide row (15 columns: 3 symbols + 10 typed + string + designated TS) -

    [Benchmark(Baseline = true), BenchmarkCategory("Wide")]
    public Task Http_WideRow() => WideRowAsync(_httpSender);

    [Benchmark, BenchmarkCategory("Wide")]
    public Task Ws_WideRow() => WideRowAsync(_wsSender);

    private async Task WideRowAsync(ISender sender)
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < Rows; i++)
        {
            sender.Table("wide")
                .Symbol("a", "x").Symbol("b", "y").Symbol("c", "z")
                .Column("d", (long)i).Column("e", i * 0.5)
                .Column("f", i % 2 == 0).Column("g", _wideStringG[i])
                .Column("h", now)
                .Column("i", (long)(i + 1)).Column("j", i * 1.25)
                .Column("k", _wideStringK[i]).Column("l", i % 3 == 0)
                .Column("m", (long)(i * 2)).Column("n", (i % 7) * 0.7)
                .At(now);
        }

        await sender.SendAsync();
    }

    // -- Multi-table (5 tables interleaved) -----------------------------------

    [Benchmark(Baseline = true), BenchmarkCategory("MultiTable")]
    public Task Http_MultiTable_5Way() => MultiTableAsync(_httpSender);

    [Benchmark, BenchmarkCategory("MultiTable")]
    public Task Ws_MultiTable_5Way() => MultiTableAsync(_wsSender);

    private async Task MultiTableAsync(ISender sender)
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < Rows; i++)
        {
            var tableIndex = i % 5;
            sender.Table($"t{tableIndex}")
                .Column("v", Interlocked.Increment(ref _rowSeq))
                .At(now);
        }

        await sender.SendAsync();
    }

    private static int GetFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>
///     Job for the ingest-throughput benches (<see cref="BenchInsertsWs" /> and
///     <see cref="BenchSfThroughput" />): 20 iterations × 5 warmup, one shot per invocation.
///     Replaces the 3-iteration fast job, whose error bars routinely exceeded the means.
/// </summary>
public class IngestThroughputConfig : ManualConfig
{
    public IngestThroughputConfig()
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
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}

#endif
