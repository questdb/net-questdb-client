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
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using QuestDB;
using QuestDB.Senders;
using dummy_http_server;

namespace net_questdb_client_benchmarks;

/// <summary>
///     Single-batch round-trip latency in sync mode (<c>in_flight_window=1</c>). Each iteration
///     is one full RTT (send + ack) — IterationCount = sample size for p50/p95/min/max.
///     Override with <c>--iterationCount N</c> on the CLI when you need fewer/more samples.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(LatencySamplingConfig))]
public class BenchLatencyWs
{
    private DummyQwpServer? _qwpServer;
    private DummyHttpServer? _httpServer;
    private string _httpEndpoint = null!;
    private string _wsEndpoint = null!;
    private ISender _wsSender = null!;
    private ISender _httpSender = null!;
    private long _rowSeq;

    [Params(1, 100, 10_000)]
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

        _wsSender = Sender.New($"ws::addr={_wsEndpoint};in_flight_window=1;auto_flush=off;");
        _httpSender = Sender.New($"http::addr={_httpEndpoint};auto_flush=off;");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _wsSender?.Dispose(); } catch { }
        try { _httpSender?.Dispose(); } catch { }
        if (_qwpServer is not null) await _qwpServer.DisposeAsync();
        _httpServer?.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task Http_Roundtrip()
    {
        for (var i = 0; i < RowsPerBatch; i++)
        {
            _httpSender.Table("lat")
                .Column("v", Interlocked.Increment(ref _rowSeq))
                .At(DateTime.UtcNow);
        }

        await _httpSender.SendAsync();
    }

    [Benchmark]
    public async Task Ws_SyncRoundtrip()
    {
        for (var i = 0; i < RowsPerBatch; i++)
        {
            _wsSender.Table("lat")
                .Column("v", Interlocked.Increment(ref _rowSeq))
                .At(DateTime.UtcNow);
        }

        await _wsSender.SendAsync();
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
///     IterationCount is the sample size feeding p50 / p95 / p99 / min / max. Defaults to 100k
///     for stable p99; override with <c>--iterationCount N</c> on the CLI for quick local runs.
/// </summary>
public class LatencySamplingConfig : ManualConfig
{
    public LatencySamplingConfig()
    {
        AddJob(Job.Default
            .WithLaunchCount(1)
            .WithWarmupCount(5)
            .WithIterationCount(100_000)
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
        // BDN 0.13 has no named P99 column. The CSV report contains every raw iteration time;
        // post-process the *-report.csv if a strict p99 figure is needed.
    }
}

#endif
