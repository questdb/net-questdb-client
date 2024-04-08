﻿/*******************************************************************************
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


using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;

namespace net_questdb_client_benchmarks;

public class Program
{
    public static void Main(string[] args)
    {
        var config =
            DefaultConfig.Instance.AddJob(Job.MediumRun.WithLaunchCount(1)
                    .WithToolchain(InProcessNoEmitToolchain.Instance))
                .WithOptions(ConfigOptions.DisableOptimizationsValidator);
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
}