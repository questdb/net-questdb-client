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


using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;

namespace net_questdb_client_benchmarks;

public class Program
{
    public static void Main(string[] args)
    {
        var fastJob = Job.Default
            .WithLaunchCount(1)
            .WithWarmupCount(2)
            .WithIterationCount(3)
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            // Stateful benches (open sender + send N rows) run as one shot per invocation; tell BDN
            // not to flag iterations < 100ms as "too small" — the workload is already meaningful.
            .WithMinIterationTime(Perfolizer.Horology.TimeInterval.FromMilliseconds(10))
            .WithToolchain(InProcessNoEmitToolchain.Instance);

        var config = DefaultConfig.Instance
            .AddJob(fastJob)
            .AddColumn(StatisticColumn.Min, StatisticColumn.Max, StatisticColumn.P95)
            .WithOptions(ConfigOptions.DisableOptimizationsValidator);

        if (args is { Length: > 0 })
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
            return;
        }

        RunConnectionChurnVsServerBench2(config);
    }

    public static void RunInsertBench(ManualConfig config)
    {
        BenchmarkRunner.Run<BenchInserts>(config);
    }

    public static void RunConnectionChurnBench(ManualConfig config)
    { 
        BenchmarkRunner.Run<BenchConnectionChurn>(config);
    }

    public static void RunConnectionChurnVsServerBench(ManualConfig config)
    { 
        BenchmarkRunner.Run<BenchConnectionChurnVsServer>(config);
    }
    
    public static void RunConnectionChurnVsServerBench2(ManualConfig config)
    {
        BenchmarkRunner.Run<BenchConnectionChurnVsServer2>(config);
    }

#if NET7_0_OR_GREATER
    public static void RunSfAppendBench(ManualConfig config)
    {
        BenchmarkRunner.Run<BenchSfAppend>(config);
    }

    public static void RunInsertsWsBench(ManualConfig config)
    {
        BenchmarkRunner.Run<BenchInsertsWs>(config);
    }

    public static void RunLatencyWsBench(ManualConfig config)
    {
        BenchmarkRunner.Run<BenchLatencyWs>(config);
    }

    public static void RunSfThroughputBench(ManualConfig config)
    {
        BenchmarkRunner.Run<BenchSfThroughput>(config);
    }
#endif
}