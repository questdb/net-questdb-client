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

namespace net_questdb_client_tests;

/// <summary>
///     Integration tests against a QuestDB build that ships the <c>/read/v1</c> egress endpoint
///     (not in any released image yet). Point <c>QUESTDB_IMAGE</c> at any branch image that has it,
///     e.g. <c>QUESTDB_IMAGE=questdb/questdb:&lt;branch&gt; dotnet test --filter QuestDbQueryIntegrationTests</c>.
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
    public void SelectConstant_RoundTrips()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var handler = new RecordingHandler();
        client.Execute("SELECT 42 AS answer", handler);

        Assert.That(handler.Ended, Is.True);
        Assert.That(handler.LastBatch, Is.Not.Null);
        Assert.That(handler.LastBatch!.RowCount, Is.EqualTo(1));
        Assert.That(handler.LastBatch.ColumnCount, Is.EqualTo(1));
        Assert.That(handler.LastBatch.GetColumnWireType(0), Is.EqualTo(QwpTypeCode.Int));
        Assert.That(handler.LastBatch.GetIntValue(0, 0), Is.EqualTo(42));
    }

    [Test]
    public void SelectFromSeededTable_ReturnsAllRows()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var handler = new RecordingHandler();
        client.Execute("SELECT id, value FROM qwp_egress_int_test ORDER BY id", handler);

        Assert.That(handler.Ended, Is.True);
        Assert.That(handler.TotalRowCount, Is.EqualTo(5));
        var ids = handler.AllLongs(colIndex: 0);
        Assert.That(ids, Is.EqualTo(new[] { 1L, 2L, 3L, 4L, 5L }));
    }

    [Test]
    public void Bind_ParameterFiltersTable()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var handler = new RecordingHandler();
        client.Execute(
            "SELECT id FROM qwp_egress_int_test WHERE id = $1",
            b => b.SetLong(0, 3L),
            handler);

        Assert.That(handler.TotalRowCount, Is.EqualTo(1));
        Assert.That(handler.AllLongs(colIndex: 0), Is.EqualTo(new[] { 3L }));
    }

    [Test]
    public void DdlStatement_TerminatesViaOnExecDone()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var handler = new RecordingHandler();
        client.Execute("CREATE TABLE qwp_egress_ddl_smoke (a LONG)", handler);

        Assert.That(handler.ExecDoneObserved, Is.True,
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
    public void BadSql_SurfacesQueryErrorViaHandler()
    {
        using var client = QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};");
        var handler = new RecordingHandler();
        client.Execute("SELECT * FROM no_such_table_does_not_exist", handler);

        Assert.That(handler.LastErrorStatus, Is.GreaterThan((byte)0));
        Assert.That(handler.Ended, Is.False);
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
        var handler = new RecordingHandler();
        client.Execute($"SELECT id FROM {table} ORDER BY id", handler);

        Assert.That(handler.Ended, Is.True);
        Assert.That(handler.TotalRowCount, Is.EqualTo(25_000));
        Assert.That(handler.BatchCount, Is.GreaterThan(1));

        var ids = handler.AllLongs(colIndex: 0);
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
    }

    private sealed class RecordingHandler : QwpColumnBatchHandler
    {
        private readonly List<long[]> _columnLongs = new();
        public QwpColumnBatch? LastBatch { get; private set; }
        public int TotalRowCount { get; private set; }
        public int BatchCount { get; private set; }
        public bool Ended { get; private set; }
        public bool ExecDoneObserved { get; private set; }
        public byte LastErrorStatus { get; private set; }

        public override void OnBatch(QwpColumnBatch batch)
        {
            LastBatch = batch;
            BatchCount++;
            TotalRowCount += batch.RowCount;
            if (batch.ColumnCount > 0 && batch.GetColumnWireType(0) is QwpTypeCode.Long)
            {
                var longs = new long[batch.RowCount];
                for (var r = 0; r < batch.RowCount; r++) longs[r] = batch.GetLongValue(0, r);
                _columnLongs.Add(longs);
            }
        }

        public override void OnEnd(long totalRows) => Ended = true;
        public override void OnExecDone(short opType, long rowsAffected) => ExecDoneObserved = true;
        public override void OnError(byte status, string message) => LastErrorStatus = status;

        public long[] AllLongs(int colIndex)
        {
            return _columnLongs.SelectMany(x => x).ToArray();
        }
    }
}

#endif
