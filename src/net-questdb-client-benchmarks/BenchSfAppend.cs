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
///     Per-row append latency for the SF cursor engine wired through the public sender API.
///     Mirrors the Java <c>CursorEngineAppendLatencyBenchmark</c>: warm WebSocket connection +
///     fast-acking server, measure the time to publish a single row + flush.
/// </summary>
public class BenchSfAppend
{
    private DummyQwpServer? _server;
    private string _wsEndpoint = null!;
    private string _sfRoot = null!;
    private ISender _sfSender = null!;
    private ISender _wsSender = null!;
    private long _rowSeq;

    [Params(1, 100, 1000)]
    public int RowsPerSend;

    [GlobalSetup]
    public async Task Setup()
    {
        _sfRoot = Path.Combine(Path.GetTempPath(), "qdb-sf-bench-" + Guid.NewGuid().ToString("N"));

        var realEndpoint = Environment.GetEnvironmentVariable("QDB_BENCH_ENDPOINT");
        if (!string.IsNullOrEmpty(realEndpoint))
        {
            _wsEndpoint = realEndpoint;
        }
        else
        {
            long ackSeq = 0;
            _server = new DummyQwpServer(new DummyQwpServerOptions
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
            await _server.StartAsync();
            _wsEndpoint = $"127.0.0.1:{_server.Uri.Port}";
        }

        _sfSender = Sender.New(
            $"ws::addr={_wsEndpoint};auto_flush=off;sf_dir={_sfRoot};sender_id=bench;");
        _wsSender = Sender.New(
            $"ws::addr={_wsEndpoint};auto_flush=off;");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        try { _sfSender?.Dispose(); } catch { }
        try { _wsSender?.Dispose(); } catch { }
        if (_server is not null)
        {
            await _server.DisposeAsync();
        }

        if (Directory.Exists(_sfRoot))
        {
            try { Directory.Delete(_sfRoot, recursive: true); } catch { }
        }
    }

    [Benchmark(Baseline = true)]
    public void NonSf_AppendAndSend()
    {
        for (var i = 0; i < RowsPerSend; i++)
        {
            _wsSender.Table("bench")
                .Column("v", Interlocked.Increment(ref _rowSeq))
                .At(DateTime.UtcNow);
        }

        _wsSender.Send();
    }

    [Benchmark]
    public void Sf_AppendAndSend()
    {
        for (var i = 0; i < RowsPerSend; i++)
        {
            _sfSender.Table("bench")
                .Column("v", Interlocked.Increment(ref _rowSeq))
                .At(DateTime.UtcNow);
        }

        _sfSender.Send();
    }

    [Benchmark]
    public void Sf_AppendSendAndPing()
    {
        for (var i = 0; i < RowsPerSend; i++)
        {
            _sfSender.Table("bench")
                .Column("v", Interlocked.Increment(ref _rowSeq))
                .At(DateTime.UtcNow);
        }

        _sfSender.Send();
        ((IQwpWebSocketSender)_sfSender).Ping();
    }
}

#endif
