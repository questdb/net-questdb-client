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
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using QuestDB;
using QuestDB.Senders;
using dummy_http_server;

namespace net_questdb_client_benchmarks;

/// <summary>
///     Single-batch send latency, two categories. <b>RoundTrip</b>: <c>SendAsync</c> plus an
///     <see cref="IQwpWebSocketSender.PingAsync" /> ACK drain, so the measured time is a full
///     send + server-process + ACK round-trip — directly comparable to the blocking HTTP baseline.
///     <b>Handover</b>: bare <c>SendAsync</c>, which returns after the async enqueue, measuring
///     caller-observed send latency without waiting for the server; the RAM and SF variants expose
///     the mmap-staging cost that store-and-forward adds to that handover. IterationCount is the
///     p50/p95/min/max sample size; override with <c>--iterationCount N</c> on the CLI.
/// </summary>
[Config(typeof(LatencySamplingConfig))]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
public class BenchLatencyWs
{
    private DummyQwpServer? _qwpServer;
    private DummyHttpServer? _httpServer;
    private string _httpEndpoint = null!;
    private string _wsEndpoint = null!;
    private ISender _wsSender = null!;
    private ISender _wsSenderSf = null!;
    private ISender _httpSender = null!;
    private string _sfRoot = null!;
    private long _rowSeq;

    [Params(1, 100)]
    public int RowsPerBatch;

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

        _sfRoot = Path.Combine(Path.GetTempPath(), "qdb-sf-bench-lat-" + Guid.NewGuid().ToString("N"));
        _wsSender = Sender.New($"ws::addr={_wsEndpoint};auto_flush=off;");
        _wsSenderSf = Sender.New(
            $"ws::addr={_wsEndpoint};sf_dir={_sfRoot};sender_id=latbench;auto_flush=off;");
        _httpSender = Sender.New($"http::addr={_httpEndpoint};auto_flush=off;");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _wsSender?.Dispose(); } catch { }
        try { _wsSenderSf?.Dispose(); } catch { }
        try { _httpSender?.Dispose(); } catch { }
        if (_qwpServer is not null) await _qwpServer.DisposeAsync();
        _httpServer?.Dispose();
        if (_sfRoot is not null && Directory.Exists(_sfRoot))
        {
            try { Directory.Delete(_sfRoot, recursive: true); } catch { }
        }
    }

    [Benchmark(Baseline = true), BenchmarkCategory("RoundTrip")]
    public async Task Http_Roundtrip()
    {
        AppendRows(_httpSender);
        await _httpSender.SendAsync();
    }

    [Benchmark, BenchmarkCategory("RoundTrip")]
    public async Task Ws_SyncRoundtrip()
    {
        AppendRows(_wsSender);
        await _wsSender.SendAsync();
        // SendAsync returns after the async enqueue; PingAsync drains the in-flight ACK window so
        // the measured interval is a full server round-trip, not just caller-side handover.
        await ((IQwpWebSocketSender)_wsSender).PingAsync();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Handover")]
    public async Task Ws_HandoverRam()
    {
        AppendRows(_wsSender);
        // Bare SendAsync returns after the async enqueue into the RAM segment ring with no ACK
        // wait — caller-observed send latency, the Handover-category baseline.
        await _wsSender.SendAsync();
    }

    [Benchmark, BenchmarkCategory("Handover")]
    public async Task Ws_HandoverSf()
    {
        AppendRows(_wsSenderSf);
        // Same handover measurement against an sf_dir-backed sender; the delta over Ws_HandoverRam
        // is the mmap-staging cost store-and-forward adds to the caller's send path.
        await _wsSenderSf.SendAsync();
    }

    private void AppendRows(ISender sender)
    {
        for (var i = 0; i < RowsPerBatch; i++)
        {
            sender.Table("lat")
                .Column("v", Interlocked.Increment(ref _rowSeq))
                .At(DateTime.UtcNow);
        }
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
///     Sampling-oriented job for <see cref="BenchLatencyWs" />: every iteration is one RTT, so
///     IterationCount is the sample size feeding p50 / p95 / min / max. Defaults to 20k — solid
///     for p50–p95 and quick to run; pass <c>--iterationCount 100000</c> on the CLI when a stable
///     p99 / p99.9 tail is needed.
/// </summary>
public class LatencySamplingConfig : ManualConfig
{
    public LatencySamplingConfig()
    {
        Add(DefaultConfig.Instance);
        WithUnionRule(ConfigUnionRule.AlwaysUseLocal);

        AddJob(Job.Default
            .WithLaunchCount(1)
            .WithWarmupCount(5)
            .WithIterationCount(20_000)
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithToolchain(InProcessNoEmitToolchain.Instance));
        AddColumn(
            StatisticColumn.Min,
            StatisticColumn.P67,
            StatisticColumn.P80,
            StatisticColumn.P85,
            StatisticColumn.P90,
            StatisticColumn.P95,
            StatisticColumn.P100,
            StatisticColumn.Max);
        // Added explicitly: AlwaysUseLocal drops the [MemoryDiagnoser] attribute's diagnoser,
        // so the Allocated column only appears if the local config carries it.
        AddDiagnoser(MemoryDiagnoser.Default);
        
    }
}

#endif
