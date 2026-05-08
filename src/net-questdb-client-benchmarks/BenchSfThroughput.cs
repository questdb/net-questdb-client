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
using BenchmarkDotNet.Attributes;
using QuestDB;
using QuestDB.Senders;
using dummy_http_server;

namespace net_questdb_client_benchmarks;

/// <summary>
///     SF vs non-SF sustained throughput. Senders live across iterations so per-invocation cost
///     is row-encoding + frame I/O + ACK wait, not slot-lock acquire / mmap setup / engine spin-up.
///     Acceptance: SF overhead should stay within ~30% of non-SF.
/// </summary>
[MemoryDiagnoser]
public class BenchSfThroughput
{
    private DummyQwpServer? _qwpServer;
    private string _wsEndpoint = null!;
    private string _sfRoot = null!;
    private ISender _wsNoSf = null!;
    private ISender _wsWithSf = null!;

    [Params(10_000, 100_000)]
    public int Rows;

    [GlobalSetup]
    public async Task Setup()
    {
        _sfRoot = Path.Combine(Path.GetTempPath(), "qdb-sf-bench-thru-" + Guid.NewGuid().ToString("N"));

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
                    var ack = new byte[9];
                    ack[0] = 0x00;
                    BinaryPrimitives.WriteInt64LittleEndian(ack.AsSpan(1, 8), seq);
                    return ack;
                },
            });
            await _qwpServer.StartAsync();
            _wsEndpoint = $"127.0.0.1:{_qwpServer.Uri.Port}";
        }

        _wsNoSf = Sender.New(
            $"ws::addr={_wsEndpoint};" +
            $"auto_flush_rows=1000;auto_flush_interval=off;auto_flush_bytes=off;");

        _wsWithSf = Sender.New(
            $"ws::addr={_wsEndpoint};" +
            $"sf_dir={_sfRoot};sender_id=bench;" +
            $"auto_flush_rows=1000;auto_flush_interval=off;auto_flush_bytes=off;");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _wsNoSf?.Dispose(); } catch { }
        try { _wsWithSf?.Dispose(); } catch { }
        if (_qwpServer is not null) await _qwpServer.DisposeAsync();
        if (Directory.Exists(_sfRoot))
        {
            try { Directory.Delete(_sfRoot, recursive: true); } catch { }
        }
    }

    [Benchmark(Baseline = true)]
    public async Task Ws_NoSf()
    {
        for (var i = 0; i < Rows; i++)
        {
            _wsNoSf.Table("thru")
                .Symbol("region", i % 2 == 0 ? "us" : "eu")
                .Column("v", (long)i)
                .At(DateTime.UtcNow);
        }

        await _wsNoSf.SendAsync();
        // Block until every in-flight batch has been ACKed, so the comparison against Ws_WithSf
        // (which already pays this cost via its engine flush) is symmetric.
        ((IQwpWebSocketSender)_wsNoSf).Ping();
    }

    [Benchmark]
    public async Task Ws_WithSf()
    {
        for (var i = 0; i < Rows; i++)
        {
            _wsWithSf.Table("thru")
                .Symbol("region", i % 2 == 0 ? "us" : "eu")
                .Column("v", (long)i)
                .At(DateTime.UtcNow);
        }

        await _wsWithSf.SendAsync();
        ((IQwpWebSocketSender)_wsWithSf).Ping();
    }
}

#endif
