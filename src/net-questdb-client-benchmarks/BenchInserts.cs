/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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


using BenchmarkDotNet.Attributes;
using dummy_http_server;
using QuestDB.Ingress;
using tcp_client_test;

namespace net_questdb_client_benchmarks;

[MarkdownExporterAttribute.GitHub]
public class BenchInserts
{
    private readonly DummyHttpServer _httpServer;
    private readonly DummyIlpServer _tcpServer;
    private readonly int _httpPort = 29473;
    private readonly int _httpsPort = 29474;
    private readonly int _tcpPort = 29472;

    [Params(1000, 10000, 25000, 75000, 100000)]
    public int BatchSize;

    [Params(10000, 100000, 1000000, 10000000)]
    public int RowsPerIteration;

    public BenchInserts()
    {
        _httpServer = new DummyHttpServer();
        _httpServer.StartAsync(_httpPort).Wait();

        _tcpServer = new DummyIlpServer(_tcpPort, false);
        _tcpServer.AcceptAsync();
    }

    [GlobalSetup]
    public async Task Setup()
    {
        if (_httpServer != null)
        {
            _httpServer.Clear();
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (_httpServer != null)
        {
            _httpServer.Dispose();
        }

        if (_tcpServer != null)
        {
            _tcpServer.Dispose();
        }
    }

    [Benchmark]
    public async Task BasicInsertsHttp()
    {
        var sender = Sender.New($"http::addr=localhost:{_httpPort};auto_flush=on;auto_flush_rows={BatchSize};");

        for (var i = 0; i < RowsPerIteration; i++)
        {
            await sender.Table("basic_inserts").Column("number", i).AtNow();
            if (i % BatchSize == 0)
            {
                _httpServer.Clear();
            }
        }

        await sender.SendAsync();
    }

    // [Benchmark]
    // public async Task BasicInsertsTcp()
    // {
    //     var sender = new Sender($"tcp::addr=localhost:{TcpPort};auto_flush=off;");
    //
    //     for (int i = 0; i < rows_per_iteration; i++)
    //     {
    //         sender.Table("basic_inserts").Column("number", i).AtNow();
    //     }
    //
    //     await sender.SendAsync();
    // }
    //
    // [Benchmark]
    // public async Task TinyInsertsTcp()
    // {
    //     var sender = new Sender($"tcp::addr=localhost:{TcpPort};auto_flush=on;auto_flush_rows={batch_size}");
    //
    //     for (int i = 0; i < rows_per_iteration; i++)
    //     {
    //         sender.Table("basic_inserts").Column("number", i).AtNow();
    //         if (i % batch_size == 0)
    //         {
    //             _httpServer.Clear();
    //         }
    //     }
    //
    //     await sender.SendAsync();
    // }
    //
    // [Benchmark]
    // public async Task BasicInsertsDeprecatedTcp()
    // {
    //     var sender = await LineTcpSender.ConnectAsync("localhost", TcpPort, tlsMode: TlsMode.Disable);
    //
    //     for (int i = 0; i < rows_per_iteration; i++)
    //     {
    //         sender.Table("basic_inserts").Column("number", i).AtNow();
    //     }
    //
    //     await sender.SendAsync();
    // }
}