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

using QuestDB;
using QuestDB.Senders;
using QuestDB.Soak;

// Long-running load / profiling harness for the QWP WebSocket sender, pool and store-and-forward.
// It runs forever (or QDB_SOAK_DURATION_SEC) against a live QuestDB master build. See SoakConfig
// for every environment knob. Ctrl-C triggers a clean drain + final verify + report.

var cfg = SoakConfig.FromEnvironment();
cfg.Print();
Directory.CreateDirectory(cfg.SfDir);

RowFactory.Configure(cfg.LongArrays);
var specs    = TableSpec.BuildSet(cfg.TablePrefix);
var wide     = specs.First(s => s.Kind == TableKind.Wide);
var governor = new RateGovernor(cfg.BytesPerSecond, cfg.BurstBytes);
var metrics  = new Metrics();
var gate     = new PauseGate();

// One pooled client drives the spiky multi-connection churn (disk store-and-forward) and, via its
// query pool, the egress verification. Pinned senders are standalone (never pooled).
var client = QuestDBClient.Builder()
                          .IngestConfig(cfg.IngestConfig(true, "soak-churn"))
                          .QueryConfig(cfg.QueryConfig())
                          .SenderPoolMin(cfg.MinConnections)
                          .SenderPoolMax(cfg.MaxConnections)
                          .AcquireTimeout(TimeSpan.FromSeconds(10))
                          .Build();

var pinnedDisk = (IQwpWebSocketSender)Sender.New(cfg.IngestConfig(true, "pinned-disk"));
var pinnedRam  = (IQwpWebSocketSender)Sender.New(cfg.IngestConfig(false, "pinned-ram"));
var pinned = new[]
{
    pinnedDisk, pinnedRam
};
var pools = new[]
{
    client
};

// Start from empty tables so verification reconciles THIS run only. Rows left by previous runs (and
// frames flushed by orphan-drain) otherwise make the server legitimately exceed this run's appends.
if (cfg.ResetTables)
{
    Console.WriteLine("Resetting soak tables (drop if exists)...");
    foreach (var s in specs)
    {
        try
        {
            await using var r = await client.ExecuteReaderAsync($"drop table if exists {s.Name}", CancellationToken.None);
            while (await r.ReadBatchAsync())
            {
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  reset {s.Name}: {ex.GetType().Name}: {ex.Message}");
        }
    }
}

var churn     = new ChurnWorkload(cfg, client, governor, metrics, gate, specs);
var burstDisk = new PinnedWorkload("disk-sf", pinnedDisk, wide, cfg, governor, metrics, gate);
var burstRam  = new PinnedWorkload("ram-sf", pinnedRam, wide, cfg, governor, metrics, gate);
var reporter  = new Reporter(cfg, metrics, pools, pinned, specs);
var verifier  = cfg.VerifyEverySec > 0 ? new Verifier(cfg, client, pools, pinned, gate, specs) : null;

using var cts = new CancellationTokenSource();
if (cfg.DurationSec > 0) cts.CancelAfter(TimeSpan.FromSeconds(cfg.DurationSec));
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine("\nCtrl-C received — draining...");
    cts.Cancel();
};

Console.WriteLine("Running. Ctrl-C to stop.\n");

var running = new List<Task>
{
    churn.RunAsync(cts.Token), burstDisk.RunAsync(cts.Token), burstRam.RunAsync(cts.Token), reporter.RunAsync(cts.Token)
};
if (verifier is not null) running.Add(verifier.RunAsync(cts.Token));

try
{
    await Task.WhenAll(running);
}
catch (OperationCanceledException)
{
}
catch (Exception ex)
{
    Console.WriteLine($"Workload faulted: {ex}");
}

// --- Clean shutdown: drain everything, print a final report, run one last verification. ---
Console.WriteLine("\nFinal drain + verify...");
var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(120));
try
{
    await client.FlushAsync(TimeSpan.FromSeconds(60), shutdown.Token);
    foreach (var s in pinned)
        await s.FlushAsync(TimeSpan.FromSeconds(60), shutdown.Token);

    reporter.PrintOnce();

    if (verifier is not null)
        await verifier.VerifyOnceAsync(shutdown.Token);
}
catch (Exception ex)
{
    Console.WriteLine($"Shutdown drain/verify error: {ex.GetType().Name}: {ex.Message}");
}

Console.WriteLine("\n=== Final summary ===");
Console.WriteLine($"  rows sent : {metrics.Rows:N0}");
Console.WriteLine($"  wire bytes: {Reporter.FormatBytes(metrics.Bytes)}");
foreach (var s in specs)
    Console.WriteLine($"  {s.Name}: appended {s.Appended:N0}  generated(ids) {s.Generated:N0}  gap {s.Generated - s.Appended:N0}");
if (verifier is not null)
    Console.WriteLine($"  verification: {verifier.CountMismatches} count mismatch(es), " +
                      $"{verifier.ChecksumErrors} checksum error(s)");

await client.DisposeAsync();
foreach (var s in pinned)
    await s.DisposeAsync();

Console.WriteLine("Done.");