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
using Org.BouncyCastle.Crypto.Agreement.Srp;
using QuestDB.Ingress;
using tcp_client_test;

namespace net_questdb_client_benchmarks;

public class BenchInserts
{
    private readonly DummyHttpServer _httpServer;
    private readonly DummyIlpServer _tcpServer;
    private readonly int HttpPort = 29473;

    private readonly int TcpPort = 29472;

    [Params(10000, 100000, 1000000)] public int n;

    [Params(1000, 10000, 100000)] public int r;

    public BenchInserts()
    {
        _httpServer = new DummyHttpServer();
        _httpServer.StartAsync(HttpPort).Wait();

        // _tcpServer = new DummyIlpServer(TcpPort, false);
        // _tcpServer.AcceptAsync();
    }

    [GlobalSetup]
    public async Task Setup()
    {
        if (_httpServer != null) _httpServer.Clear();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (_httpServer != null) _httpServer.Dispose();
    }

    // [Benchmark]
    // public async Task BasicInsertsHttp()
    // {
    //     var sender = new LineSender($"http::addr=localhost:{HttpPort};");
    //
    //     for (var i = 0; i < n; i++) sender.Table("basic_inserts").Column("number", i).AtNow();
    //
    //     await sender.SendAsync();
    // }


    [Benchmark]
    public async Task TinyInsertsHttp()
    {
        var sender = new LineSender($"http::addr=localhost:{HttpPort};auto_flush=off;");

        for (var i = 0; i < n; i++)
        {
            sender.Table("basic_inserts").Column("number", i).AtNow();

            if (i % r == 0)
            {
                await sender.SendAsync();
                _httpServer.Clear();
            }
        }
        
        
    }


    // [Benchmark]
    // public async Task BasicInsertsTcp()
    // {
    //     var sender = new LineSender($"tcp::addr=localhost:{TcpPort};");
    //
    //     for (int i = 0; i < n; i++)
    //     {
    //         sender.Table("basic_inserts").Column("number", i).AtNow();
    //     }
    //
    //     await sender.SendAsync();
    // }
    //
    //
    // [Benchmark]
    // public async Task BasicInsertsDeprecatedTcp()
    // {
    //     var sender = await LineTcpSender.ConnectAsync("localhost", TcpPort, tlsMode: TlsMode.Disable);
    //
    //     for (int i = 0; i < n; i++)
    //     {
    //         sender.Table("basic_inserts").Column("number", i).AtNow();
    //     }
    //
    //     await sender.SendAsync();
    // }
}