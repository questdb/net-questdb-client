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
using System.Globalization;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using QuestDB;
using QuestDB.Senders;
using dummy_http_server;

namespace net_questdb_client_benchmarks;

/// <summary>
///     Single-row <c>Table(...).Column(...).At(...) + Send()</c> latency for the QWP (WebSocket)
///     ingest sender — the .NET port of java-questdb-client's <c>QwpIngressLatencyBenchmark</c>.
///     Every iteration is one row, so the iteration count is the sample size feeding the
///     p50 / p90 / p99 / p99.9 tail (see <see cref="IngressLatencyConfig" />); ingest UX is gated by
///     the tail, not the mean.
///     <para>
///     Two user-facing contracts are measured, mirroring the Java bench's <c>-Dsf</c> modes:
///     </para>
///     <list type="bullet">
///       <item>
///         <b>No-SF round-trip</b> (<see cref="Ws_SingleRow_NoSf_Roundtrip" />, the baseline): default
///         <c>close_flush_timeout_millis</c>, so <c>Send()</c> drains — full encode → WS send → server
///         ACK. The number to quote when the contract is "the server has confirmed the row".
///       </item>
///       <item>
///         <b>SF durable handover</b> (<see cref="Ws_SingleRow_Sf_DurableHandover" />): the SF sender
///         runs with <c>close_flush_timeout_millis=0</c>, so <c>Send()</c> returns as soon as the row
///         is durable on the local mmap segment (CRC + write); the wire send and server ACK proceed
///         asynchronously and are NOT in the measurement window. The number to quote when the contract
///         is "recoverable if I crash now". Its ratio to the baseline is the handover speed-up.
///       </item>
///     </list>
///     Point at a live QuestDB with <c>QDB_BENCH_ENDPOINT=host:port</c> for meaningful wire numbers
///     (the Java bench always targets a live server); otherwise an in-process fast-ACK
///     <see cref="DummyQwpServer" /> stands in so the bench is runnable offline. Senders are opened
///     once in <see cref="Setup" /> and primed with a warm-up flush, so connection handshake / schema
///     registration / encoder JIT stay out of the measurement window.
/// </summary>
[Config(typeof(IngressLatencyConfig))]
public class BenchIngressLatencyWs
{
    private const string Table = "latency_bench_ingress";

    private DummyQwpServer? _qwpServer;
    private string _wsEndpoint = null!;
    private string _sfRoot = null!;

    private ISender _wsSenderNoSf = null!;
    private ISender _wsSenderSf = null!;

    private DateTime _baseTime;
    private long _rowSeq;

    [GlobalSetup]
    public async Task Setup()
    {
        var realEndpoint = Environment.GetEnvironmentVariable("QDB_BENCH_ENDPOINT");
        if (!string.IsNullOrEmpty(realEndpoint))
        {
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
                    // OK ACK = [status u8=0x00][seq i64][tableCount u16=0]; the trailing u16 is required
                    // (OffsetTableCountInOkAck=9, +2) — a bare 9-byte frame parses as a protocol violation.
                    var ack = new byte[11];
                    ack[0] = 0x00;
                    BinaryPrimitives.WriteInt64LittleEndian(ack.AsSpan(1, 8), seq);
                    BinaryPrimitives.WriteUInt16LittleEndian(ack.AsSpan(9, 2), 0);
                    return ack;
                },
            });
            await _qwpServer.StartAsync();
            _wsEndpoint = $"127.0.0.1:{_qwpServer.Uri.Port}";
        }

        _sfRoot = Path.Combine(Path.GetTempPath(), "qdb-sf-bench-inglat-" + Guid.NewGuid().ToString("N"));

        // Default close_flush_timeout_millis (60s) => Send() drains: full encode -> WS send -> ACK.
        _wsSenderNoSf = Sender.New($"ws::addr={_wsEndpoint};auto_flush=off;");
        // close_flush_timeout_millis=0 => Send() returns after the durable AppendBlocking into the
        // mmap segment ring, with no ACK wait: the SF "recoverable if I crash now" handover contract.
        _wsSenderSf = Sender.New(
            $"ws::addr={_wsEndpoint};sf_dir={_sfRoot};sender_id=inglatbench;" +
            "close_flush_timeout_millis=0;auto_flush=off;");

        _baseTime = DateTime.UtcNow;

        // Prime each sender: register the table schema, warm the WS encoder / async pipeline, and pay
        // the connection handshake once. Drain both so the connection is fully established before the
        // first measured row.
        IngestSingleRow(_wsSenderNoSf);
        IngestSingleRow(_wsSenderSf);
        _wsSenderNoSf.Flush(TimeSpan.FromSeconds(10));
        _wsSenderSf.Flush(TimeSpan.FromSeconds(10));

        _rowSeq = 0;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _wsSenderNoSf?.Dispose(); } catch { }
        try { _wsSenderSf?.Dispose(); } catch { }
        if (_qwpServer is not null) await _qwpServer.DisposeAsync();
        if (_sfRoot is not null && Directory.Exists(_sfRoot))
        {
            try { Directory.Delete(_sfRoot, recursive: true); } catch { }
        }
    }

    [Benchmark(Baseline = true)]
    public void Ws_SingleRow_NoSf_Roundtrip() => IngestSingleRow(_wsSenderNoSf);

    [Benchmark]
    public void Ws_SingleRow_Sf_DurableHandover() => IngestSingleRow(_wsSenderSf);

    private void IngestSingleRow(ISender sender)
    {
        // Monotonic id + ts so rows are unique and, against a live server, the WAL writer stays in
        // append-mostly mode (no out-of-order rewrites). 10 ticks == 1 microsecond.
        var n = ++_rowSeq;
        sender.Table(Table)
            .Column("id", n)
            .At(_baseTime.AddTicks(n * 10));
        sender.Send();
    }
}

/// <summary>
///     Sampling job for <see cref="BenchIngressLatencyWs" />: <c>InvocationCount=1</c>, so every
///     iteration is exactly one row and IterationCount is the per-op sample size behind the percentile
///     columns. Defaults to 200 samples — enough for p50 / p90 / p99, marginal for p99.9, and it
///     completes even when a single round-trip is slow. Raise it with <c>QDB_BENCH_SAMPLES=20000</c>
///     for a stable tail (BDN's <c>--iterationCount</c> flag does NOT override an explicitly-added
///     job, hence the env var).
///     <para>
///     Sizing note: the no-SF round-trip method waits for the server ACK, so its per-op time is one
///     localhost/network round-trip. On a machine whose loopback carries a security/EDR tax that can
///     be ~150ms+ per round-trip (independent of QuestDB — the same tax shows on the fast-ACK
///     <c>DummyQwpServer</c> and on plain HTTP), so at N=20000 that method alone would take hours and
///     BDN aborts ("takes too long"). Keep N modest unless you have measured a fast round-trip. The
///     SF durable-handover method never makes a round-trip on the hot path (it returns after the local
///     mmap write), so it is microseconds regardless.
///     </para>
///     Adds p50 / p90 / p99 / p99.9 (the last two via <see cref="LatencyPercentileColumn" />, since
///     BDN's built-in StatisticColumn tops out at P95 / P100) plus allocation tracking.
/// </summary>
public class IngressLatencyConfig : ManualConfig
{
    public IngressLatencyConfig()
    {
        Add(DefaultConfig.Instance);
        WithUnionRule(ConfigUnionRule.AlwaysUseLocal);

        var samples = int.TryParse(Environment.GetEnvironmentVariable("QDB_BENCH_SAMPLES"), out var n) && n > 0
            ? n
            : 200;

        AddJob(Job.Default
            .WithLaunchCount(1)
            .WithWarmupCount(3)
            .WithIterationCount(samples)
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithToolchain(InProcessNoEmitToolchain.Instance));

        AddColumn(StatisticColumn.Min);
        AddColumn(StatisticColumn.P50);
        AddColumn(StatisticColumn.P90);
        AddColumn(new LatencyPercentileColumn(99));
        AddColumn(new LatencyPercentileColumn(99.9));
        AddColumn(StatisticColumn.Max);
        // AlwaysUseLocal drops the [MemoryDiagnoser] attribute's diagnoser, so wire it explicitly for
        // the per-row Allocated column (the analogue of the Java bench's GCProfiler alloc rate).
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}

/// <summary>
///     A percentile column for arbitrary (fractional) percentiles — BDN's built-in
///     <see cref="StatisticColumn" /> only exposes up to P95 and P100, but the ingest tail we care
///     about is p99 / p99.9. Nearest-rank over the raw per-iteration samples (matching the Java bench's
///     <c>samples[(int)(n * p)]</c>), rendered in microseconds.
/// </summary>
public sealed class LatencyPercentileColumn : IColumn
{
    private readonly double _percentile;

    public LatencyPercentileColumn(double percentile)
    {
        _percentile = percentile;
    }

    public string Id => "Percentile" + _percentile.ToString(CultureInfo.InvariantCulture);
    public string ColumnName => "P" + _percentile.ToString(CultureInfo.InvariantCulture);
    public bool AlwaysShow => true;
    public ColumnCategory Category => ColumnCategory.Statistics;
    public int PriorityInCategory => 0;
    public bool IsNumeric => true;
    public UnitType UnitType => UnitType.Time;
    public string Legend => $"Percentile {_percentile} of measured latencies (nearest-rank)";

    public bool IsAvailable(Summary summary) => true;
    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        => GetValue(summary, benchmarkCase, summary.Style);

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
    {
        var stats = summary[benchmarkCase]?.ResultStatistics;
        var values = stats?.OriginalValues;
        if (values is null || values.Count == 0)
        {
            return "NA";
        }

        var sorted = values.ToArray();
        Array.Sort(sorted);
        var idx = Math.Min(sorted.Length - 1, (int)(sorted.Length * (_percentile / 100.0)));
        var micros = sorted[idx] / 1000.0;
        return micros.ToString("N3", CultureInfo.InvariantCulture) + " µs";
    }
}

#endif
