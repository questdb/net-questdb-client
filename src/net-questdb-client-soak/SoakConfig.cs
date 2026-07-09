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

namespace QuestDB.Soak;

/// <summary>
///     All knobs are environment variables so the harness can run unattended under a profiler.
///     Every value has a default tuned for a local master build on the standard port.
/// </summary>
internal sealed class SoakConfig
{
    public string Addr { get; private init; } = "localhost:9000";
    public string? Username { get; private init; }
    public string? Password { get; private init; }
    public string SfDir { get; private init; } = Path.Combine(Path.GetTempPath(), "qdb-soak-sf");

    /// <summary>Long-run average wire throughput ceiling. Bounds disk growth on the server.</summary>
    public double GbPerHour { get; private init; } = 20.0;

    /// <summary>Token-bucket burst capacity. Big enough for a short ~1M row/s spike, then the average clamps.</summary>
    public long BurstBytes { get; private init; } = 1024L * 1024 * 1024;

    /// <summary>Hard cap on concurrent pooled borrowers (== sender_pool_max).</summary>
    public int MaxConnections { get; private init; } = 30;

    /// <summary>
    ///     sender_pool_min. Idle reaping and lifetime rotation only act on senders ABOVE min, so
    ///     setting this equal to <see cref="MaxConnections" /> pins the pool: every sender is created
    ///     once at startup and never reaped/recreated — removing the periodic buffer allocations that
    ///     come from the elastic pool growing and shrinking. Default 1 (fully elastic).
    /// </summary>
    public int MinConnections { get; private init; } = 1;

    /// <summary>Mean of the spiky active-borrower random walk.</summary>
    public int MeanConnections { get; private init; } = 5;

    public int ReportEverySec { get; private init; } = 10;

    /// <summary>0 disables egress verification.</summary>
    public int VerifyEverySec { get; private init; } = 60;

    /// <summary>0 == run forever (until Ctrl-C).</summary>
    public int DurationSec { get; private init; }

    /// <summary>Working-set growth over baseline that trips the leak guardrail warning.</summary>
    public long LeakWarnMib { get; private init; } = 512;

    public string TablePrefix { get; private init; } = "soak_";

    /// <summary>Requires enterprise primary replication; off by default so it works on OSS master.</summary>
    public bool DurableAck { get; private init; }

    /// <summary>The engine currently rejects long arrays over QWP, so this is off by default.</summary>
    public bool LongArrays { get; private init; }

    /// <summary>
    ///     Drop the soak_ tables at startup so verification counts this run only. Rows from prior runs
    ///     (and orphan-drained frames) otherwise make the server legitimately exceed this run's appended
    ///     count. On by default.
    /// </summary>
    public bool ResetTables { get; private init; } = true;

    public string IngestConfig(bool withSf, string senderId)
    {
        var scheme = "ws";
        var auth = Username is null ? "" : $"username={Username};password={Password};";
        // Each owner gets its own sf_dir subtree (per sender_id). drain_orphans scans the sf_dir root
        // for sibling slots, so a shared root would let the pooled churn client adopt the standalone
        // pinned senders' slots as "orphans" and lock them out from under the same process.
        var sf = withSf
            ? $"sf_dir={Path.Combine(SfDir, senderId)};sender_id={senderId};drain_orphans=on;"
            : "";
        var durable = DurableAck ? "request_durable_ack=on;" : "";
        return $"{scheme}::addr={Addr};{auth}{sf}" +
               "auto_flush_rows=10000;auto_flush_bytes=1048576;auto_flush_interval=200;" +
               durable;
    }

    /// <summary>Plain ws string used to build the query pool for verification (no SF on the read side).</summary>
    public string QueryConfig()
    {
        var auth = Username is null ? "" : $"username={Username};password={Password};";
        return $"ws::addr={Addr};{auth}query_pool_min=1;query_pool_max=2;";
    }

    public static SoakConfig FromEnvironment()
    {
        string Str(string k, string d) => Environment.GetEnvironmentVariable(k) is { Length: > 0 } v ? v : d;
        int Int(string k, int d) => int.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
        long Long(string k, long d) => long.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
        double Dbl(string k, double d) => double.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;

        var authOn = Str("QDB_SOAK_AUTH", "on") is "on" or "true";
        return new SoakConfig
        {
            Addr = Str("QDB_SOAK_ADDR", "localhost:9000"),
            Username = authOn ? Str("QDB_SOAK_USER", "admin") : null,
            Password = authOn ? Str("QDB_SOAK_PASS", "quest") : null,
            SfDir = Str("QDB_SOAK_SF_DIR", Path.Combine(Path.GetTempPath(), "qdb-soak-sf")),
            GbPerHour = Dbl("QDB_SOAK_GB_PER_HOUR", 20.0),
            BurstBytes = Long("QDB_SOAK_BURST_MIB", 1024) * 1024 * 1024,
            MaxConnections = Int("QDB_SOAK_MAX_CONN", 30),
            MinConnections = Int("QDB_SOAK_MIN_CONN", 1),
            MeanConnections = Int("QDB_SOAK_MEAN_CONN", 5),
            ReportEverySec = Int("QDB_SOAK_REPORT_SEC", 10),
            VerifyEverySec = Int("QDB_SOAK_VERIFY_SEC", 60),
            DurationSec = Int("QDB_SOAK_DURATION_SEC", 0),
            LeakWarnMib = Long("QDB_SOAK_LEAK_WARN_MIB", 512),
            TablePrefix = Str("QDB_SOAK_TABLE_PREFIX", "soak_"),
            DurableAck = Str("QDB_SOAK_DURABLE_ACK", "off") is "on" or "true",
            LongArrays = Str("QDB_SOAK_LONG_ARRAYS", "off") is "on" or "true",
            ResetTables = Str("QDB_SOAK_RESET_TABLES", "on") is "on" or "true"
        };
    }

    public double BytesPerSecond => GbPerHour * 1024 * 1024 * 1024 / 3600.0;

    public void Print()
    {
        Console.WriteLine("=== QuestDB soak harness config ===");
        Console.WriteLine($"  addr                {Addr}   auth={(Username is null ? "none" : "basic")}");
        Console.WriteLine($"  sf_dir              {SfDir}");
        Console.WriteLine($"  target throughput   {GbPerHour:0.##} GiB/h  (~{BytesPerSecond / (1024 * 1024):0.0} MiB/s avg)");
        Console.WriteLine($"  burst capacity      {BurstBytes / (1024.0 * 1024):0.0} MiB");
        Console.WriteLine($"  connections         mean {MeanConnections}, min {MinConnections}, max {MaxConnections}" +
                          (MinConnections >= MaxConnections ? "  (pinned — no sender create/reap churn)" : ""));
        Console.WriteLine($"  report / verify     {ReportEverySec}s / {(VerifyEverySec == 0 ? "off" : VerifyEverySec + "s")}");
        Console.WriteLine($"  duration            {(DurationSec == 0 ? "forever" : DurationSec + "s")}");
        Console.WriteLine($"  leak warn threshold +{LeakWarnMib} MiB working set over baseline");
        Console.WriteLine($"  durable_ack={(DurableAck ? "on" : "off")}  long_arrays={(LongArrays ? "on" : "off")}  reset_tables={(ResetTables ? "on" : "off")}");
        Console.WriteLine("====================================");
    }
}
