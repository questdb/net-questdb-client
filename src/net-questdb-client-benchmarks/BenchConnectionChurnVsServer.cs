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
using QuestDB;

namespace net_questdb_client_benchmarks;

[MarkdownExporterAttribute.GitHub]
public class BenchConnectionChurnVsServer
{
    private static HttpClient _client;
    private readonly int _httpPort = 9000;
    private readonly int _tcpPort = 9009;

    [Params(75000)] public int BatchSize;

    [Params(1, 2, 4, 8, 16)] public int ConnectionLimit;

    [Params(3)] public int NumberOfTables;

    [Params(10000000)] public int RowsPerIteration;

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
        for (var i = 0; i < NumberOfTables; i++)
        {
            await _client.GetAsync($"http://localhost:{_httpPort}/exec?query=drop table \"random_table_{{i}}\"");
        }

        await Task.Delay(250);
    }

    [Benchmark]
    public async Task HttpRandomTableEveryRow()
    {
        var sender =
            Sender.New(
                $"http::addr=localhost:{_httpPort};auto_flush=on;auto_flush_rows={BatchSize};auto_flush_interval=-1;pool_limit={ConnectionLimit};");

        for (var i = 0; i < RowsPerIteration; i++)
        {
            await sender.Table($"random_table_{Random.Shared.NextInt64(0, NumberOfTables - 1)}").Column("number", i)
                .AtNow();
        }

        await sender.SendAsync();
    }

    [Benchmark]
    public async Task TcpRandomTableEveryRow()
    {
        var sender =
            Sender.New(
                $"tcp::addr=localhost:{_tcpPort};auto_flush=on;auto_flush_rows={BatchSize};auto_flush_interval=-1;");

        for (var i = 0; i < RowsPerIteration; i++)
        {
            await sender.Table($"random_table_{Random.Shared.NextInt64(0, NumberOfTables - 1)}").Column("number", i)
                .AtNow();
        }

        await sender.SendAsync();
    }
}