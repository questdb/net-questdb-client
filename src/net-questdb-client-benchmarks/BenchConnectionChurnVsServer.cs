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

[MarkdownExporterAttribute.GitHub()]
public class BenchConnectionChurnVsServer
{
    private readonly int HttpPort = 9000;
    private readonly int TcpPort = 9009;
    private static HttpClient _client;

    [Params(10000000)] public int rows_per_iteration;

    [Params(75000)] public int batch_size;

    [Params(3)] public int number_of_tables;

    [Params(1, 2, 4, 8, 16)] public int connection_limit;

    public BenchConnectionChurnVsServer()
    {
        _client = new HttpClient();
    }

    [GlobalSetup]
    public async Task Setup()
    {
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        for (int i = 0; i < number_of_tables; i++)
        {
            await _client.GetAsync($"http://localhost:{HttpPort}/exec?query=drop table \"random_table_{{i}}\"");
        }

        await Task.Delay(250);
    }
    
    [Benchmark]
    public async Task HttpRandomTableEveryRow()
    {
        var sender = new Sender($"http::addr=localhost:{HttpPort};auto_flush=on;auto_flush_rows={batch_size};auto_flush_interval=-1;pool_limit={connection_limit};");
        
        for (var i = 0; i < rows_per_iteration; i++)
        {
            sender.Table($"random_table_{Random.Shared.NextInt64(0, number_of_tables - 1)}").Column("number", i).AtNow();
        }        
        await sender.SendAsync();
    }
    
    [Benchmark]
    public async Task TcpRandomTableEveryRow()
    {
        var sender = new Sender($"tcp::addr=localhost:{TcpPort};auto_flush=on;auto_flush_rows={batch_size};auto_flush_interval=-1;");
        
        for (var i = 0; i < rows_per_iteration; i++)
        {
            sender.Table($"random_table_{Random.Shared.NextInt64(0, number_of_tables - 1)}").Column("number", i).AtNow();
        }        
        await sender.SendAsync();
    }
    
}