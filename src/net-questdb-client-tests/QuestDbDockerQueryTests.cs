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
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_tests;

/// <summary>
///     Egress query tests against QuestDB running in Docker. Opt-in:
///     <c>QDB_DOCKER_TESTS=on dotnet test --filter QuestDbDockerQueryTests</c> (or set
///     <c>QDB_DOCKER_IMAGE</c> directly); without either the fixture self-skips so plain runs and
///     the CI legs stay green. The image defaults to <c>questdb/questdb:nightly</c> — the
///     <c>/read/v1</c> egress endpoint only exists on master builds, so released tags won't work.
///     The container is started via the <c>docker</c> CLI on an ephemeral loopback port and removed
///     on teardown; queries use <c>long_sequence</c> so no table seeding or WAL apply waits are
///     needed.
/// </summary>
[TestFixture]
public class QuestDbDockerQueryTests
{
    private const string DefaultImage = "questdb/questdb:nightly";

    private string? _containerName;
    private int _port;

    private string WsConn(string extra = "") => $"ws::addr=127.0.0.1:{_port};{extra}";

    [OneTimeSetUp]
    public async Task StartContainer()
    {
        var image = Environment.GetEnvironmentVariable("QDB_DOCKER_IMAGE");
        var enabled = image is not null
                      || Environment.GetEnvironmentVariable("QDB_DOCKER_TESTS") is "1" or "on" or "true";
        if (!enabled)
        {
            Assert.Ignore("Docker egress tests are opt-in: set QDB_DOCKER_TESTS=on (or QDB_DOCKER_IMAGE=...).");
        }

        image ??= DefaultImage;

        var (daemonOk, _, daemonErr) = await DockerAsync("version --format {{.Server.Version}}", TimeSpan.FromSeconds(15));
        if (!daemonOk)
        {
            throw new InvalidOperationException($"QDB_DOCKER_TESTS is set but the Docker daemon is unreachable: {daemonErr}");
        }

        // Pull explicitly so the run step's readiness budget isn't spent on image download.
        var (pulled, _, pullErr) = await DockerAsync($"pull {image}", TimeSpan.FromMinutes(10));
        if (!pulled)
        {
            throw new InvalidOperationException($"docker pull {image} failed: {pullErr}");
        }

        _containerName = $"qdbnet-egress-{Guid.NewGuid():N}";
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

        _port = int.Parse(portOut.Trim().Split(':')[^1]);
        await WaitForHttpReadyAsync(TimeSpan.FromSeconds(120));
    }

    [OneTimeTearDown]
    public async Task RemoveContainer()
    {
        if (_containerName is not null)
        {
            await DockerAsync($"rm -f {_containerName}", TimeSpan.FromSeconds(30));
        }
    }

    [Test]
    public async Task SingleBatch_SingleColumn_AllRowsArriveInOneBatch()
    {
        await using var client = await QueryClient.NewAsync(WsConn());
        var cap = await RunQueryAsync(client, "SELECT x FROM long_sequence(10)");

        Assert.That(cap.BatchCount, Is.EqualTo(1), "10 rows fit comfortably in one RESULT_BATCH");
        Assert.That(cap.BatchSeqs, Is.EqualTo(new[] { 0L }));
        Assert.That(cap.TotalRows, Is.EqualTo(10L));
        Assert.That(cap.ColumnTypes[0], Is.EqualTo(QwpTypeCode.Long));
        Assert.That(cap.LongColumn(0), Is.EqualTo(Enumerable.Range(1, 10).Select(i => (long)i)));
    }

    [Test]
    public async Task MultiBatch_RowsSpanBatchesInOrder()
    {
        const int rows = 5000;
        const int maxBatchRows = 500;
        await using var client = await QueryClient.NewAsync(WsConn($"max_batch_rows={maxBatchRows};"));
        var cap = await RunQueryAsync(client, $"SELECT x FROM long_sequence({rows})");

        Assert.That(cap.BatchCount, Is.GreaterThanOrEqualTo(rows / maxBatchRows),
            "the server must honour the advertised per-batch row cap");
        Assert.That(cap.BatchRowCounts, Has.All.LessThanOrEqualTo(maxBatchRows));
        Assert.That(cap.BatchSeqs, Is.EqualTo(Enumerable.Range(0, cap.BatchCount).Select(i => (long)i)),
            "batch_seq must be contiguous from 0");
        Assert.That(cap.TotalRows, Is.EqualTo(rows));
        Assert.That(cap.Rows.Count, Is.EqualTo(rows));
        Assert.That(cap.LongColumn(0), Is.EqualTo(Enumerable.Range(1, rows).Select(i => (long)i)),
            "row order is preserved across batch boundaries");
    }

    [Test]
    public async Task MultiColumn_TypedValuesRoundTrip()
    {
        const int rows = 100;
        await using var client = await QueryClient.NewAsync(WsConn());
        var cap = await RunQueryAsync(client,
            "SELECT x, cast(x as int) i, x * 0.5 d, 'row_' || x s, x % 2 = 0 b, cast(x as timestamp) ts " +
            $"FROM long_sequence({rows})");

        Assert.That(cap.TotalRows, Is.EqualTo(rows));
        Assert.That(cap.ColumnCount, Is.EqualTo(6));
        Assert.That(cap.ColumnNames, Is.EqualTo(new[] { "x", "i", "d", "s", "b", "ts" }));
        Assert.Multiple(() =>
        {
            Assert.That(cap.ColumnTypes[0], Is.EqualTo(QwpTypeCode.Long));
            Assert.That(cap.ColumnTypes[1], Is.EqualTo(QwpTypeCode.Int));
            Assert.That(cap.ColumnTypes[2], Is.EqualTo(QwpTypeCode.Double));
            Assert.That(cap.ColumnTypes[3], Is.EqualTo(QwpTypeCode.Varchar));
            Assert.That(cap.ColumnTypes[4], Is.EqualTo(QwpTypeCode.Boolean));
            Assert.That(cap.ColumnTypes[5], Is.EqualTo(QwpTypeCode.Timestamp));
        });

        for (var r = 0; r < rows; r++)
        {
            var x = r + 1;
            Assert.That(cap.Rows[r][0], Is.EqualTo((long)x));
            Assert.That(cap.Rows[r][1], Is.EqualTo(x));
            Assert.That(cap.Rows[r][2], Is.EqualTo(x * 0.5));
            Assert.That(cap.Rows[r][3], Is.EqualTo($"row_{x}"));
            Assert.That(cap.Rows[r][4], Is.EqualTo(x % 2 == 0));
            Assert.That(cap.Rows[r][5], Is.EqualTo((long)x), "cast(long as timestamp) is the value in micros");
        }
    }

    [Test]
    public async Task MultiBatch_MultiColumn_ValuesSurviveBatchBoundaries()
    {
        const int rows = 1000;
        const int maxBatchRows = 64;
        await using var client = await QueryClient.NewAsync(WsConn($"max_batch_rows={maxBatchRows};"));
        var cap = await RunQueryAsync(client,
            $"SELECT x, x * 2 twice, 'v' || x tag FROM long_sequence({rows})");

        Assert.That(cap.BatchCount, Is.GreaterThanOrEqualTo(rows / maxBatchRows));
        Assert.That(cap.TotalRows, Is.EqualTo(rows));
        Assert.That(cap.ColumnCount, Is.EqualTo(3));
        for (var r = 0; r < rows; r++)
        {
            var x = r + 1;
            Assert.That(cap.Rows[r][0], Is.EqualTo((long)x));
            Assert.That(cap.Rows[r][1], Is.EqualTo((long)(x * 2)));
            Assert.That(cap.Rows[r][2], Is.EqualTo($"v{x}"));
        }
    }

    // ---- fixture plumbing ----

    private async Task WaitForHttpReadyAsync(TimeSpan budget)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var url = $"http://127.0.0.1:{_port}/exec?query={Uri.EscapeDataString("SELECT 1")}";
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
            $"QuestDB container did not become ready on 127.0.0.1:{_port} within {budget.TotalSeconds}s", last);
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

    private static async Task<Capture> RunQueryAsync(IQwpQueryClient client, string sql)
    {
        var cap = new Capture();
        await using var reader = await client.ExecuteReaderAsync(sql);
        while (await reader.ReadBatchAsync())
        {
            var b = reader.Current;
            cap.BatchCount++;
            cap.BatchSeqs.Add(b.BatchSeq);
            cap.BatchRowCounts.Add(b.RowCount);
            if (cap.BatchCount == 1)
            {
                cap.ColumnCount = b.ColumnCount;
                cap.ColumnNames = new string[b.ColumnCount];
                cap.ColumnTypes = new QwpTypeCode[b.ColumnCount];
                for (var c = 0; c < b.ColumnCount; c++)
                {
                    cap.ColumnNames[c] = b.GetColumnName(c);
                    cap.ColumnTypes[c] = b.GetColumnWireType(c);
                }
            }

            for (var r = 0; r < b.RowCount; r++)
            {
                var row = new object?[b.ColumnCount];
                for (var c = 0; c < b.ColumnCount; c++)
                {
                    row[c] = b.IsNull(c, r) ? null : ExtractCell(b, c, r);
                }

                cap.Rows.Add(row);
            }
        }

        cap.TotalRows = reader.TotalRows;
        return cap;
    }

    private static object ExtractCell(QwpColumnBatch b, int c, int r) => b.GetColumnWireType(c) switch
    {
        QwpTypeCode.Long or QwpTypeCode.Date or QwpTypeCode.Timestamp or QwpTypeCode.TimestampNanos
            => b.GetLongValue(c, r),
        QwpTypeCode.Int or QwpTypeCode.IPv4 => b.GetIntValue(c, r),
        QwpTypeCode.Double => b.GetDoubleValue(c, r),
        QwpTypeCode.Float => b.GetFloatValue(c, r),
        QwpTypeCode.Boolean => b.GetBoolValue(c, r),
        QwpTypeCode.Symbol => b.GetSymbol(c, r) ?? string.Empty,
        _ => b.GetString(c, r) ?? string.Empty,
    };

    private sealed class Capture
    {
        public int ColumnCount;
        public string[] ColumnNames = Array.Empty<string>();
        public QwpTypeCode[] ColumnTypes = Array.Empty<QwpTypeCode>();
        public int BatchCount;
        public long TotalRows;
        public readonly List<long> BatchSeqs = new();
        public readonly List<int> BatchRowCounts = new();
        public readonly List<object?[]> Rows = new();

        public IEnumerable<long> LongColumn(int col) => Rows.Select(r => Convert.ToInt64(r[col]));
    }
}

#endif
