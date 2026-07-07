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

using System.Diagnostics;
using System.Text.Json;
using NUnit.Framework;
using QuestDB;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests;

/// <summary>
///     QWP (WebSocket) ingest fault-tolerance tests against a single-node QuestDB in Docker.
///     Opt-in exactly like <see cref="QuestDbDockerQueryTests" />:
///     <c>QDB_DOCKER_TESTS=on dotnet test --filter QuestDbDockerIngestFaultToleranceTests</c>
///     (or set <c>QDB_DOCKER_IMAGE</c>); otherwise the fixture self-skips so plain runs and the CI
///     legs stay green. The <c>/write/v4</c> ingest endpoint only exists on master builds, so the
///     image defaults to <c>questdb/questdb:nightly</c>. Outages are simulated with
///     <c>docker pause</c>/<c>unpause</c> (the socket black-holes without losing the port);
///     delivery is verified by polling <c>/exec select count()</c> over HTTP (WAL apply lag is
///     absorbed by the poll).
///
///     These are the OSS-single-node scenarios from the PR #60 (§6 Invariant-B / §7 NACK-v2) gap
///     analysis. Tests that exercise behaviour already shipped run live; tests that depend on the
///     not-yet-ported Invariant-B / NACK-v2 semantics call <see cref="Assert.Ignore(string)" /> at
///     the top of an otherwise-complete body — delete the Ignore when the feature lands. The three
///     cluster-role scenarios (mid-stream demotion, all-replica window, durable-ack gap) are NOT
///     here: they require the enterprise e2e harness, not single-node Docker.
/// </summary>
[TestFixture]
public class QuestDbDockerIngestFaultToleranceTests
{
    private const string DefaultImage = "questdb/questdb:nightly";
    private const string PendingInvariantB = "pending Invariant-B port (SF never terminates on a connection error)";
    private const string PendingNackV2 = "pending NACK-policy-v2 port (no silent drop / terminal reclassification)";

    private string? _containerName;
    private int _httpPort;
    private readonly List<string> _sfDirs = new();

    private string WsConn(string extra = "") => $"ws::addr=127.0.0.1:{_httpPort};{extra}";

    private string NewSfDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"qdbnet-sf-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        _sfDirs.Add(dir);
        return dir;
    }

    private static string NewTable() => $"ft_{Guid.NewGuid():N}";

    [OneTimeSetUp]
    public async Task StartContainer()
    {
        var image = Environment.GetEnvironmentVariable("QDB_DOCKER_IMAGE");
        var enabled = image is not null
                      || Environment.GetEnvironmentVariable("QDB_DOCKER_TESTS") is "1" or "on" or "true";
        if (!enabled)
        {
            Assert.Ignore("Docker ingest tests are opt-in: set QDB_DOCKER_TESTS=on (or QDB_DOCKER_IMAGE=...).");
        }

        image ??= DefaultImage;

        var (daemonOk, _, daemonErr) = await DockerAsync("version --format {{.Server.Version}}", TimeSpan.FromSeconds(15));
        if (!daemonOk)
        {
            throw new InvalidOperationException($"QDB_DOCKER_TESTS is set but the Docker daemon is unreachable: {daemonErr}");
        }

        var (pulled, _, pullErr) = await DockerAsync($"pull {image}", TimeSpan.FromMinutes(10));
        if (!pulled)
        {
            throw new InvalidOperationException($"docker pull {image} failed: {pullErr}");
        }

        _containerName = $"qdbnet-ingest-{Guid.NewGuid():N}";
        var (started, _, runErr) = await DockerAsync(
            $"run --rm -d --name {_containerName} -p 127.0.0.1:0:9000 {image}", TimeSpan.FromMinutes(2));
        if (!started)
        {
            throw new InvalidOperationException($"docker run {image} failed: {runErr}");
        }

        var (portOk, portOut, portErr) = await DockerAsync($"port {_containerName} 9000/tcp", TimeSpan.FromSeconds(15));
        if (!portOk)
        {
            throw new InvalidOperationException($"docker port lookup failed: {portErr}");
        }

        _httpPort = int.Parse(portOut.Trim().Split(':')[^1]);
        await WaitForHttpReadyAsync(TimeSpan.FromSeconds(120));
    }

    [TearDown]
    public async Task EnsureUnpaused()
    {
        // Defensive: a test that pauses the container must not leak the outage into the next one.
        if (_containerName is not null)
        {
            await DockerAsync($"unpause {_containerName}", TimeSpan.FromSeconds(10));
        }
    }

    [OneTimeTearDown]
    public async Task RemoveContainer()
    {
        if (_containerName is not null)
        {
            await DockerAsync($"rm -f {_containerName}", TimeSpan.FromSeconds(30));
        }

        foreach (var dir in _sfDirs)
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch
            {
                // best-effort scratch cleanup
            }
        }
    }

    // ------------------------------------------------------------------
    // Runnable today — behaviour already shipped
    // ------------------------------------------------------------------

    // Scenario: a transient mid-stream outage (shorter than the reconnect budget) reconnects and
    // replays the un-acked frames — every row lands exactly once, no duplication.
    [Test]
    public async Task TransientOutage_ReconnectReplays_NoDuplicateRows()
    {
        var table = NewTable();
        await using var sender = Sender.New(WsConn("sf_dir=" + NewSfDir() + ";reconnect_max_duration_millis=60000;"));

        await AppendRowsAsync(sender, table, 0, 200);
        await sender.SendAsync();

        await PauseAsync();
        await AppendRowsAsync(sender, table, 200, 200); // buffered into SF while the wire is down
        await sender.SendAsync();
        await Task.Delay(500);
        await UnpauseAsync();

        Assert.That(await sender.FlushAsync(TimeSpan.FromSeconds(30)), Is.True, "sender must drain after recovery");
        Assert.That(await WaitForCountAsync(table, 400, TimeSpan.FromSeconds(60)), Is.EqualTo(400),
            "all rows land exactly once — replay must not duplicate the acked prefix");
    }

    // Scenario: the client is created while the server is unreachable; with lazy_connect it buffers
    // writes non-blocking and delivers them once the server is reachable again.
    [Test]
    public async Task LazyConnect_ServerDownAtStart_BuffersThenDeliversOnRecovery()
    {
        var table = NewTable();
        await PauseAsync();

        await using (var sender = Sender.New(WsConn("lazy_connect=on;sf_dir=" + NewSfDir() + ";")))
        {
            await AppendRowsAsync(sender, table, 0, 100); // must not block or throw while the server is down
            await sender.SendAsync();
            await UnpauseAsync();
            Assert.That(await sender.FlushAsync(TimeSpan.FromSeconds(30)), Is.True);
        }

        Assert.That(await WaitForCountAsync(table, 100, TimeSpan.FromSeconds(60)), Is.EqualTo(100));
    }

    // Scenario: Dispose/Close while the wire is black-holed returns promptly instead of hanging —
    // Dispose is pure resource release (no drain).
    [Test]
    public async Task CloseDuringOutage_ReturnsPromptly()
    {
        var table = NewTable();
        var sender = Sender.New(WsConn("sf_dir=" + NewSfDir() + ";"));
        await AppendRowsAsync(sender, table, 0, 50);
        await sender.SendAsync();

        await PauseAsync();
        await AppendRowsAsync(sender, table, 50, 50);

        var sw = Stopwatch.StartNew();
        await sender.DisposeAsync();
        sw.Stop();

        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(10)),
            "close during an outage must not block on the unreachable wire");
    }

    // ------------------------------------------------------------------
    // Pending — depend on the Invariant-B / NACK-v2 port (bodies complete, un-Ignore when landed)
    // ------------------------------------------------------------------

    // Scenario: an outage LONGER than the old reconnect budget no longer terminalises the sender —
    // it keeps retrying and delivers every buffered row once the server returns.
    [Test]
    public async Task LongOutage_SenderRetriesForever_DeliversOnRecovery()
    {
        Assert.Ignore(PendingInvariantB);

        var table = NewTable();
        await using var sender = Sender.New(WsConn("sf_dir=" + NewSfDir() + ";reconnect_max_duration_millis=1000;"));

        await AppendRowsAsync(sender, table, 0, 100);
        await sender.SendAsync();

        await PauseAsync();
        await AppendRowsAsync(sender, table, 100, 100);
        await sender.SendAsync();
        await Task.Delay(3000); // well past reconnect_max_duration_millis — old behaviour would go terminal here
        await UnpauseAsync();

        Assert.That(await sender.FlushAsync(TimeSpan.FromSeconds(30)), Is.True,
            "an SF sender must never terminalise on a connection error, however long the outage");
        Assert.That(await WaitForCountAsync(table, 200, TimeSpan.FromSeconds(60)), Is.EqualTo(200));
    }

    // Scenario: a schema-conflicting write is NACKed as SCHEMA_MISMATCH and halts the sender loudly
    // (the next producer call throws) instead of silently dropping the batch (old DROP_AND_CONTINUE).
    [Test]
    public async Task SchemaMismatch_HaltsLoudly_NotSilentlyDropped()
    {
        Assert.Ignore(PendingNackV2);

        var table = NewTable();
        await using var sender = Sender.New(WsConn("sf_dir=" + NewSfDir() + ";"));

        // Establish the column type as LONG.
        await AppendRowsAsync(sender, table, 0, 1);
        await sender.SendAsync();
        Assert.That(await WaitForCountAsync(table, 1, TimeSpan.FromSeconds(30)), Is.EqualTo(1));

        // Now write the same column as a conflicting type -> server rejects with SCHEMA_MISMATCH.
        sender.Table(table).Column("v", "not-a-long").At(DateTime.UtcNow);
        Assert.ThrowsAsync<IngressError>(async () =>
        {
            await sender.SendAsync();
            await sender.FlushAsync(TimeSpan.FromSeconds(10));
        }, "SCHEMA_MISMATCH must surface as a loud terminal, not a silent drop");
    }

    // Scenario: SF store exhaustion (tiny cap, server down) surfaces to the producer as append
    // backpressure that eventually throws — never as a permanently terminal sender.
    [Test]
    public async Task SfExhaustion_SurfacesAsAppendBackpressure_NotTerminal()
    {
        Assert.Ignore(PendingInvariantB);

        var table = NewTable();
        await using var sender = Sender.New(WsConn(
            "sf_dir=" + NewSfDir() + ";sf_max_total_bytes=1048576;sf_append_deadline_millis=2000;"));

        await PauseAsync();
        // Keep appending with nowhere to drain until the tiny SF cap is hit and the deadline trips.
        Assert.ThrowsAsync<IngressError>(async () =>
        {
            for (var i = 0; i < 1_000_000; i++)
            {
                await AppendRowsAsync(sender, table, i, 1);
                await sender.SendAsync();
            }
        }, "a full SF store must throw append backpressure, not silently drop or die terminally");
    }

    // Scenario: a pooled SF sender whose slot is stranded by a down server is drained on recovery
    // by the background drainer instead of being quarantined.
    [Test]
    public async Task Drainer_DownServerSlot_RetriesAndDeliversOnRecovery()
    {
        Assert.Ignore(PendingInvariantB);

        var table = NewTable();
        var sfDir = NewSfDir();

        // First handle: buffer rows into SF, then drop it while the server is down so the slot is
        // left with pending segments (a stranded, adoptable orphan).
        await PauseAsync();
        await using (var db = QuestDBClient.Connect(WsConn($"sf_dir={sfDir};drain_orphans=on;sender_pool_min=1;")))
        {
            var s = db.BorrowSender();
            await AppendRowsAsync(s, table, 0, 100);
            await s.SendAsync();
            s.Dispose();
        }

        await UnpauseAsync();

        // A fresh handle over the same sf_dir must adopt and drain the stranded slot, not quarantine it.
        await using (var db = QuestDBClient.Connect(WsConn($"sf_dir={sfDir};drain_orphans=on;sender_pool_min=1;")))
        {
            Assert.That(await db.FlushAsync(TimeSpan.FromSeconds(30)), Is.True);
        }

        Assert.That(await WaitForCountAsync(table, 100, TimeSpan.FromSeconds(60)), Is.EqualTo(100),
            "a down-server slot must be drained on recovery, never quarantined");
    }

    // ------------------------------------------------------------------
    // fixture plumbing
    // ------------------------------------------------------------------

    private static async Task AppendRowsAsync(ISender sender, string table, int start, int count)
    {
        for (var i = 0; i < count; i++)
        {
            sender.Table(table).Column("v", (long)(start + i)).At(DateTime.UtcNow);
        }

        await Task.CompletedTask;
    }

    private Task PauseAsync() => RequireContainer("pause");
    private Task UnpauseAsync() => RequireContainer("unpause");

    private async Task RequireContainer(string verb)
    {
        var (ok, _, err) = await DockerAsync($"{verb} {_containerName}", TimeSpan.FromSeconds(15));
        if (!ok)
        {
            throw new InvalidOperationException($"docker {verb} {_containerName} failed: {err}");
        }
    }

    private async Task<long> WaitForCountAsync(string table, long expected, TimeSpan budget)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var url = $"http://127.0.0.1:{_httpPort}/exec?query={Uri.EscapeDataString($"select count() from '{table}'")}";
        var deadline = DateTime.UtcNow + budget;
        long last = -1;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var resp = await http.GetAsync(url);
                if (resp.IsSuccessStatusCode)
                {
                    var body = await resp.Content.ReadAsStringAsync();
                    last = ParseCount(body);
                    if (last >= expected) return last;
                }
            }
            catch
            {
                // table not created yet / server briefly unavailable — keep polling
            }

            await Task.Delay(500);
        }

        return last;
    }

    private static long ParseCount(string execJson)
    {
        // /exec returns {"...":..,"dataset":[[<count>]],..}; a query error omits "dataset".
        using var doc = JsonDocument.Parse(execJson);
        if (!doc.RootElement.TryGetProperty("dataset", out var dataset) || dataset.GetArrayLength() == 0)
        {
            return -1;
        }

        return dataset[0][0].GetInt64();
    }

    private async Task WaitForHttpReadyAsync(TimeSpan budget)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var url = $"http://127.0.0.1:{_httpPort}/exec?query={Uri.EscapeDataString("SELECT 1")}";
        var deadline = DateTime.UtcNow + budget;
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var resp = await http.GetAsync(url);
                if (resp.IsSuccessStatusCode) return;
            }
            catch (Exception ex)
            {
                last = ex;
            }

            await Task.Delay(500);
        }

        throw new TimeoutException(
            $"QuestDB container did not become ready on 127.0.0.1:{_httpPort} within {budget.TotalSeconds}s", last);
    }

    private static async Task<(bool Ok, string StdOut, string StdErr)> DockerAsync(string args, TimeSpan timeout)
    {
        try
        {
            var psi = new ProcessStartInfo("docker", args)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            var stdOut = proc.StandardOutput.ReadToEndAsync();
            var stdErr = proc.StandardError.ReadToEndAsync();
            using var cts = new CancellationTokenSource(timeout);
            await proc.WaitForExitAsync(cts.Token);
            return (proc.ExitCode == 0, await stdOut, await stdErr);
        }
        catch (Exception ex)
        {
            return (false, string.Empty, ex.Message);
        }
    }
}

#endif
