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
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using QuestDB;
using QuestDB.Senders;
using dummy_http_server;

namespace net_questdb_client_benchmarks;

/// <summary>
///     Allocation regression gate for the WS / QWP sender hot path. <see cref="MemoryDiagnoser" />
///     reports per-op allocations; CI gating logic should diff against a stored baseline. Three row
///     shapes cover the typical type mix; sender lives across iterations so handshake cost is
///     amortised and only the per-row + per-batch alloc shows up.
/// </summary>
[MemoryDiagnoser]
public class BenchAllocationsWs
{
    private DummyQwpServer? _qwpServer;
    private string _wsEndpoint = null!;
    private ISender _wsSender = null!;
    private long _rowSeq;

    [Params(100, 1000)]
    public int RowsPerOp;

    [GlobalSetup]
    public async Task Setup()
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

        _wsSender = Sender.New(
            $"ws::addr={_wsEndpoint};in_flight_window=128;auto_flush=off;");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _wsSender?.Dispose();
        if (_qwpServer is not null) await _qwpServer.DisposeAsync();
    }

    [Benchmark]
    public async Task NarrowRows()
    {
        for (var i = 0; i < RowsPerOp; i++)
        {
            _wsSender
                .Table("trades")
                .Symbol("symbol", "ETH-USD")
                .Column("price", 2615.54)
                .Column("amount", 0.00044);
            await _wsSender.AtAsync(DateTime.UtcNow);
        }
        await _wsSender.SendAsync();
    }

    [Benchmark]
    public async Task WideRows()
    {
        for (var i = 0; i < RowsPerOp; i++)
        {
            _wsSender
                .Table("wide")
                .Symbol("g", "GROUP_A")
                .Symbol("k", "KEY_X")
                .Column("counter", _rowSeq++ * 1.0)
                .Column("int_a", (long)i)
                .Column("int_b", (long)(i * 2))
                .Column("flag", i % 2 == 0)
                .Column("text", "hello");
            await _wsSender.AtAsync(DateTime.UtcNow);
        }
        await _wsSender.SendAsync();
    }

    [Benchmark]
    public async Task SymbolHeavy()
    {
        for (var i = 0; i < RowsPerOp; i++)
        {
            _wsSender
                .Table("sym_heavy")
                .Symbol("a", "VAL_" + (i % 16))
                .Symbol("b", "VAL_" + (i % 8))
                .Symbol("c", "VAL_" + (i % 4))
                .Column("value", (long)i);
            await _wsSender.AtAsync(DateTime.UtcNow);
        }
        await _wsSender.SendAsync();
    }
}

#endif
