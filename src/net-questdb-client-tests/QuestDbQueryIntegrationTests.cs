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

using System.Text.Json;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_tests;

/// <summary>
///     Integration tests against a QuestDB build that ships the <c>/read/v1</c> egress endpoint
///     (master only). Point <c>QUESTDB_REPO</c> at a built master repo (or <c>QUESTDB_JAR</c> at a
///     built jar), e.g. <c>QUESTDB_REPO=/path/to/questdb dotnet test --filter QuestDbQueryIntegrationTests</c>;
///     for a Docker-based run see <see cref="QuestDbDockerQueryTests" />.
/// </summary>
[TestFixture]
public class QuestDbQueryIntegrationTests
{
    private const int IlpPort = 19209;
    private const int HttpPort = 19200;
    private QuestDbManager? _questDb;

    [OneTimeSetUp]
    public async Task SetUpFixture()
    {
        _questDb = new QuestDbManager(IlpPort, HttpPort);
        await _questDb.StartAsync();
        await DropFixtureTablesAsync();
        await SeedFixtureTableAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownFixture()
    {
        if (_questDb is not null)
        {
            await DropFixtureTablesAsync();
            await _questDb.StopAsync();
            await _questDb.DisposeAsync();
        }
    }

    private async Task DropFixtureTablesAsync()
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        var endpoint = _questDb!.GetHttpEndpoint();
        foreach (var table in new[] { "qwp_egress_int_test", "qwp_egress_ddl_smoke" })
        {
            var url = $"http://{endpoint}/exec?query={Uri.EscapeDataString($"DROP TABLE IF EXISTS {table}")}";
            using var resp = await http.GetAsync(url);
            resp.EnsureSuccessStatusCode();
        }
    }

    [Test]
    public async Task SelectConstant_RoundTrips()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var cap = await RunQueryAsync(client, "SELECT 42 AS answer");

        Assert.That(cap.Ended, Is.True);
        Assert.That(cap.BatchCount, Is.GreaterThan(0));
        Assert.That(cap.TotalRowCount, Is.EqualTo(1));
        Assert.That(cap.ColumnCount, Is.EqualTo(1));
        Assert.That(cap.ColumnTypes[0], Is.EqualTo(QwpTypeCode.Int));
        Assert.That((int)cap.Rows[0][0]!, Is.EqualTo(42));
    }

    [Test]
    public async Task SelectFromSeededTable_ReturnsAllRows()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var cap = await RunQueryAsync(client, "SELECT id, value FROM qwp_egress_int_test ORDER BY id");

        Assert.That(cap.Ended, Is.True);
        Assert.That(cap.TotalRowCount, Is.EqualTo(5));
        Assert.That(cap.LongColumn(0), Is.EqualTo(new[] { 1L, 2L, 3L, 4L, 5L }));
    }

    [Test]
    public async Task Bind_ParameterFiltersTable()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var cap = await RunQueryAsync(
            client, "SELECT id FROM qwp_egress_int_test WHERE id = $1", b => b.SetLong(0, 3L));

        Assert.That(cap.TotalRowCount, Is.EqualTo(1));
        Assert.That(cap.LongColumn(0), Is.EqualTo(new[] { 3L }));
    }

    [Test]
    public async Task DdlStatement_TerminatesWithExecDone()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var cap = await RunQueryAsync(client, "CREATE TABLE qwp_egress_ddl_smoke (a LONG)");

        Assert.That(cap.ExecDoneObserved, Is.True,
            "DDL must terminate with EXEC_DONE rather than RESULT_END");
    }

    [Test]
    public void ServerInfo_PopulatedOnConnect()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        Assert.That(client.ServerInfo, Is.Not.Null,
            "Phase-1 server emits SERVER_INFO unconditionally; client must capture it");
    }

    [Test]
    public async Task BadSql_SurfacesQueryError()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var cap = await RunQueryAsync(client, "SELECT * FROM no_such_table_does_not_exist");

        Assert.That(cap.ErrorStatus, Is.GreaterThan((byte)0));
        Assert.That(cap.Ended, Is.False);
    }

    [Test]
    public async Task LargeResultSet_StreamsAcrossMultipleBatchesWithCreditRefills()
    {
        const string table = "qwp_egress_large_test";
        await SeedLargeFixtureAsync(table, rowCount: 25_000);

        using var client = QueryClient.New(new QueryOptions($"ws::addr={_questDb!.GetWebSocketEndpoint()};")
        {
            initial_credit = 4096,
        });
        var cap = await RunQueryAsync(client, $"SELECT id FROM {table} ORDER BY id");

        Assert.That(cap.Ended, Is.True);
        Assert.That(cap.TotalRowCount, Is.EqualTo(25_000));
        Assert.That(cap.BatchCount, Is.GreaterThan(1));

        var ids = cap.LongColumn(0);
        Assert.That(ids.Length, Is.EqualTo(25_000));
        Assert.That(ids[0], Is.EqualTo(1L));
        Assert.That(ids[^1], Is.EqualTo(25_000L));
    }

    private async Task SeedLargeFixtureAsync(string table, int rowCount)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        var endpoint = _questDb!.GetHttpEndpoint();
        using (var drop = await http.GetAsync(
            $"http://{endpoint}/exec?query={Uri.EscapeDataString($"DROP TABLE IF EXISTS {table}")}"))
        {
            drop.EnsureSuccessStatusCode();
        }

        using var sender = Sender.New($"http::addr={endpoint};auto_flush=off;");
        for (var i = 1; i <= rowCount; i++)
        {
            sender.Table(table).Column("id", (long)i).At(DateTime.UtcNow);
        }
        await sender.SendAsync();

        // Wait until the WAL drains the seeded rows.
        for (var attempt = 0; attempt < 60; attempt++)
        {
            using var resp = await http.GetAsync(
                $"http://{endpoint}/exec?query={Uri.EscapeDataString($"SELECT count(*) FROM {table}")}");
            if (resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync();
                using var json = JsonDocument.Parse(body);
                if (json.RootElement.TryGetProperty("dataset", out var ds)
                    && ds.GetArrayLength() > 0 && ds[0].GetArrayLength() > 0
                    && ds[0][0].GetInt64() >= rowCount)
                {
                    return;
                }
            }
            await Task.Delay(250);
        }
        Assert.Fail($"Seed of {rowCount} rows into {table} did not complete");
    }

    private async Task SeedFixtureTableAsync()
    {
        using var sender = Sender.New($"http::addr={_questDb!.GetHttpEndpoint()};auto_flush=off;");
        for (var i = 1; i <= 5; i++)
        {
            sender.Table("qwp_egress_int_test")
                .Column("id", (long)i)
                .Column("value", i * 10.0)
                .At(DateTime.UtcNow);
        }
        await sender.SendAsync();

        // ILP SendAsync returns once the rows are durably in the WAL, but the WAL applies to the
        // table asynchronously. Block setup until all five rows are queryable so no test can race
        // the apply — otherwise the first reader of this table (Bind_ParameterFiltersTable, which
        // runs before SelectFromSeededTable alphabetically) intermittently sees zero rows.
        await WaitForSeededRowsAsync("qwp_egress_int_test", expected: 5);
    }

    private async Task WaitForSeededRowsAsync(string table, long expected)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        var endpoint = _questDb!.GetHttpEndpoint();
        for (var attempt = 0; attempt < 120; attempt++)
        {
            using var resp = await http.GetAsync(
                $"http://{endpoint}/exec?query={Uri.EscapeDataString($"SELECT count(*) FROM {table}")}");
            if (resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync();
                using var json = JsonDocument.Parse(body);
                if (json.RootElement.TryGetProperty("dataset", out var ds)
                    && ds.GetArrayLength() > 0 && ds[0].GetArrayLength() > 0
                    && ds[0][0].GetInt64() >= expected)
                {
                    return;
                }
            }
            await Task.Delay(250);
        }
        Assert.Fail($"Seed of {expected} rows into {table} did not become queryable");
    }

    // Drives the pull cursor to completion, snapshotting each batch (spans are only valid until the
    // next read) so tests can assert against the captured shape after the stream ends.
    private static async Task<Capture> RunQueryAsync(
        IQwpQueryClient client, string sql, QwpBindSetter? binds = null)
    {
        var cap = new Capture();
        await using var reader = binds is null
            ? await client.ExecuteReaderAsync(sql)
            : await client.ExecuteReaderAsync(sql, binds);
        try
        {
            while (await reader.ReadBatchAsync())
            {
                var b = reader.Current;
                cap.BatchCount++;
                if (cap.BatchCount == 1)
                {
                    cap.ColumnCount = b.ColumnCount;
                    cap.ColumnTypes = new QwpTypeCode[b.ColumnCount];
                    for (var c = 0; c < b.ColumnCount; c++) cap.ColumnTypes[c] = b.GetColumnWireType(c);
                }

                cap.TotalRowCount += b.RowCount;
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

            cap.ExecDoneObserved = reader.OpType != QwpOpType.None;
            cap.Ended = !cap.ExecDoneObserved && !reader.WasCancelled;
        }
        catch (QwpQueryException ex)
        {
            cap.ErrorStatus = (byte)ex.Status;
        }

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
        public QwpTypeCode[] ColumnTypes = Array.Empty<QwpTypeCode>();
        public int TotalRowCount;
        public int BatchCount;
        public bool Ended;
        public bool ExecDoneObserved;
        public byte ErrorStatus;
        public readonly List<object?[]> Rows = new();

        public long[] LongColumn(int col) => Rows.Select(r => Convert.ToInt64(r[col])).ToArray();
    }
}

#endif
