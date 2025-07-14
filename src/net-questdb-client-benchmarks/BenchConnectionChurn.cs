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
using QuestDB;

#pragma warning disable CS0414 // Field is assigned but its value is never used

namespace net_questdb_client_benchmarks;

[MarkdownExporterAttribute.GitHub]
public class BenchConnectionChurn
{
    private readonly int _httpPort = 29473;
    private readonly DummyHttpServer _httpServer;
    private readonly int _httpsPort = 29474;

    [Params(1000, 10000, 100000)] public int BatchSize;

    [Params(1, 2, 4, 8, 16)] public int ConnectionLimit;

    [Params(1, 2, 4, 8, 16)] public int NumberOfTables;

    [Params(10000, 100000, 1000000)] public int RowsPerIteration;

    public BenchConnectionChurn()
    {
        _httpServer = new DummyHttpServer();
        _httpServer.StartAsync(_httpPort).Wait();
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
    }

    [Benchmark]
    public async Task RandomTableEveryRow()
    {
        var sender =
            Sender.New(
                $"http::addr=localhost:{_httpPort};auto_flush=on;auto_flush_rows={BatchSize};pool_limit={ConnectionLimit};");

        for (var i = 0; i < RowsPerIteration; i++)
        {
            await sender.Table($"random_table_{Random.Shared.NextInt64(0, NumberOfTables - 1)}").Column("number", i)
                        .AtNowAsync();
            if (i % BatchSize == 0)
            {
                _httpServer.Clear();
            }
        }

        await sender.SendAsync();
    }
}