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

using System.Globalization;
using System.Text.Json;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp.Query;
using QuestDB.Senders;
using QuestDB.Utils;

namespace net_questdb_client_tests;

/// <summary>
///     Integration tests against a QuestDB build that ships <c>/write/v4</c> (currently master,
///     not yet released). Run with <c>QUESTDB_IMAGE=questdb/questdb:master dotnet test --filter
///     QuestDbWebSocketIntegrationTests</c> once the snapshot is available.
/// </summary>
[TestFixture]
public class QuestDbWebSocketIntegrationTests
{
    private const int IlpPort = 19109;
    private const int HttpPort = 19100;
    private QuestDbManager? _questDb;

    [OneTimeSetUp]
    public async Task SetUpFixture()
    {
        _questDb = new QuestDbManager(IlpPort, HttpPort);
        await _questDb.StartAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownFixture()
    {
        if (_questDb != null)
        {
            await _questDb.StopAsync();
            await _questDb.DisposeAsync();
        }
    }

    [Test]
    public async Task CanSendDataOverWebSocket()
    {
        var endpoint = _questDb!.GetWebSocketEndpoint();
        using var sender = Sender.New($"ws::addr={endpoint};auto_flush=off;");

        sender.Table("test_ws_basic")
            .Symbol("ticker", "ETH-USD")
            .Column("price", 2615.54)
            .Column("volume", 1234L)
            .At(DateTime.UtcNow);
        await sender.SendAsync();

        await VerifyTableHasDataAsync("test_ws_basic");
    }

    [Test]
    public async Task CanSendBatchOverWebSocket()
    {
        var endpoint = _questDb!.GetWebSocketEndpoint();
        using var sender = Sender.New($"ws::addr={endpoint};auto_flush=off;");

        for (var i = 0; i < 100; i++)
        {
            sender.Table("test_ws_batch")
                .Symbol("region", i % 2 == 0 ? "us" : "eu")
                .Column("seq", (long)i)
                .Column("score", i * 1.5)
                .At(DateTime.UtcNow);
        }

        await sender.SendAsync();

        await VerifyTableRowCountAsync("test_ws_batch", expected: 100);
    }

    [Test]
    public async Task CanSendOverStoreAndForward()
    {
        var endpoint = _questDb!.GetWebSocketEndpoint();
        var sfRoot = Path.Combine(Path.GetTempPath(), "qdb-sf-int-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var sender = Sender.New(
                       $"ws::addr={endpoint};auto_flush=off;sf_dir={sfRoot};sender_id=int-test;"))
            {
                for (var i = 0; i < 10; i++)
                {
                    sender.Table("test_ws_sf")
                        .Column("seq", (long)i)
                        .At(DateTime.UtcNow);
                }

                await sender.SendAsync();
                ((QuestDB.Senders.IQwpWebSocketSender)sender).Ping();
            }

            await VerifyTableRowCountAsync("test_ws_sf", expected: 10);
        }
        finally
        {
            if (Directory.Exists(sfRoot))
            {
                try { Directory.Delete(sfRoot, recursive: true); } catch { }
            }
        }
    }

    [Test]
    public async Task DurableAck_OnRequestDurableAck_PopulatesSeqTxn()
    {
        var endpoint = _questDb!.GetWebSocketEndpoint();
        ISender sender;
        try
        {
            sender = Sender.New($"ws::addr={endpoint};auto_flush=off;request_durable_ack=on;");
        }
        catch (IngressError ex) when (ex.code == ErrorCode.DurableAckNotSupported)
        {
            Assert.Inconclusive(
                "server did not echo `X-QWP-Durable-Ack: enabled` — primary replication not "
                + "configured on this engine; skipping durable-ack assertion");
            return;
        }

        using (sender)
        {
            sender.Table("test_ws_durable").Column("v", 42L).At(DateTime.UtcNow);
            await sender.SendAsync();

            var ws = (IQwpWebSocketSender)sender;
            // Ping triggers server-side flush of pending durable-acks; the recv pump processes
            // the resulting frame asynchronously, so poll instead of racing the first Ping.
            long durableSeqTxn = -1;
            for (var i = 0; i < 50 && durableSeqTxn < 0; i++)
            {
                ws.Ping();
                durableSeqTxn = ws.GetHighestDurableSeqTxn("test_ws_durable");
                if (durableSeqTxn < 0) await Task.Delay(100);
            }
            Assert.That(durableSeqTxn, Is.GreaterThanOrEqualTo(0L));
        }
    }

    [Test]
    public async Task RoundTrip_AllSupportedTypes_PreservesValuesViaHttpExec()
    {
        await DropTableAsync("test_ws_types");
        var endpoint = _questDb!.GetWebSocketEndpoint();
        var ts = new DateTime(2026, 5, 11, 12, 0, 0, DateTimeKind.Utc);
        var guid = new Guid("11223344-5566-7788-99aa-bbccddeeff00");

        using (var sender = Sender.New($"ws::addr={endpoint};auto_flush=off;"))
        {
            sender.Table("test_ws_types")
                .Symbol("sym", "alpha")
                .Column("s", "hello 世界")
                .Column("i", 42)
                .Column("l", 9_876_543_210L)
                .Column("d", 3.14159)
                .Column("b", true)
                .Column("c", 'Q')
                .Column("g", guid)
                .Column("dec", 12345.67m)
                .ColumnNanos("tsn", 1_700_000_000_123_456_789L)
                .At(ts);
            await sender.SendAsync();
        }

        await VerifyTableRowCountAsync("test_ws_types", expected: 1);

        var row = await QueryFirstRowAsync(
            "select sym, s, i, l, d, b, c, g, dec, tsn from test_ws_types");
        Assert.That(row[0].GetString(), Is.EqualTo("alpha"));
        Assert.That(row[1].GetString(), Is.EqualTo("hello 世界"));
        Assert.That(row[2].GetInt32(), Is.EqualTo(42));
        Assert.That(row[3].GetInt64(), Is.EqualTo(9_876_543_210L));
        Assert.That(row[4].GetDouble(), Is.EqualTo(3.14159).Within(1e-9));
        Assert.That(row[5].GetBoolean(), Is.True);
        Assert.That(row[6].GetString(), Is.EqualTo("Q"));
        Assert.That(row[7].GetString(), Is.EqualTo(guid.ToString()));
        Assert.That(decimal.Parse(row[8].GetString()!, CultureInfo.InvariantCulture),
            Is.EqualTo(12345.67m));
        Assert.That(row[9].GetString(), Is.EqualTo("2023-11-14T22:13:20.123456789Z"));
    }

    [Test]
    public async Task RoundTrip_AllSupportedTypes_PreservesValuesViaEgress()
    {
        await DropTableAsync("test_ws_egress_rt");
        var endpoint = _questDb!.GetWebSocketEndpoint();
        var ts = new DateTime(2026, 5, 11, 13, 0, 0, DateTimeKind.Utc);

        using (var sender = Sender.New($"ws::addr={endpoint};auto_flush=off;"))
        {
            sender.Table("test_ws_egress_rt")
                .Symbol("sym", "beta")
                .Column("s", "round-trip")
                .Column("i", -7)
                .Column("l", -123_456_789L)
                .Column("d", -2.5)
                .Column("b", false)
                .At(ts);
            await sender.SendAsync();
        }

        await VerifyTableRowCountAsync("test_ws_egress_rt", expected: 1);

        using var client = QueryClient.New($"ws::addr={endpoint};");
        var handler = new SingleRowRecordingHandler();
        client.Execute("select sym, s, i, l, d, b from test_ws_egress_rt", handler);

        Assert.That(handler.Ended, Is.True);
        var batch = handler.Batch ?? throw new InvalidOperationException("batch missing");
        Assert.That(batch.RowCount, Is.EqualTo(1));
        Assert.That(batch.GetSymbol(0, 0), Is.EqualTo("beta"));
        Assert.That(batch.GetString(1, 0), Is.EqualTo("round-trip"));
        Assert.That(batch.GetIntValue(2, 0), Is.EqualTo(-7));
        Assert.That(batch.GetLongValue(3, 0), Is.EqualTo(-123_456_789L));
        Assert.That(batch.GetDoubleValue(4, 0), Is.EqualTo(-2.5).Within(1e-12));
        Assert.That(batch.GetBoolValue(5, 0), Is.False);
    }

    [Test]
    public async Task Reconnect_DuringDbRestart_SfReplaysAllRows()
    {
        await DropTableAsync("test_ws_restart");
        var endpoint = _questDb!.GetWebSocketEndpoint();
        var sfRoot = Path.Combine(Path.GetTempPath(), "qdb-int-restart-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var sender = Sender.New(
                       $"ws::addr={endpoint};auto_flush=off;sf_dir={sfRoot};"
                       + "sender_id=restart-test;reconnect_max_duration_millis=60000;"
                       + "reconnect_initial_backoff_millis=50;reconnect_max_backoff_millis=500;"))
            {
                await _questDb.StopAsync();
                await Task.Delay(500);

                for (var i = 0; i < 30; i++)
                {
                    sender.Table("test_ws_restart").Column("v", (long)i).At(DateTime.UtcNow);
                }
                await sender.SendAsync();

                await _questDb.StartAsync();

                var qwp = (IQwpWebSocketSender)sender;
                for (var i = 0; i < 200; i++)
                {
                    try { await qwp.PingAsync(); break; }
                    catch { await Task.Delay(100); }
                }
            }

            await VerifyTableRowCountAsync("test_ws_restart", expected: 30, maxAttempts: 150);
        }
        finally
        {
            if (Directory.Exists(sfRoot))
            {
                try { Directory.Delete(sfRoot, recursive: true); } catch { }
            }
        }
    }

    [Test]
    public async Task Reconnect_WsCloseMidSession_SfReplaysQueuedRows()
    {
        await DropTableAsync("test_ws_close_sf");
        var realEndpoint = _questDb!.GetWebSocketEndpoint();
        using var proxy = new TcpProxy(realEndpoint);
        await proxy.StartAsync();

        var sfRoot = Path.Combine(Path.GetTempPath(), "qdb-int-wsclose-sf-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var sender = Sender.New(
                       $"ws::addr={proxy.LocalEndpoint};auto_flush=off;sf_dir={sfRoot};"
                       + "sender_id=wsclose-sf-test;reconnect_max_duration_millis=60000;"
                       + "reconnect_initial_backoff_millis=50;reconnect_max_backoff_millis=500;"))
            {
                var qwp = (IQwpWebSocketSender)sender;

                for (var i = 0; i < 10; i++)
                {
                    sender.Table("test_ws_close_sf").Column("v", (long)i).At(DateTime.UtcNow);
                }
                await sender.SendAsync();
                await qwp.PingAsync();

                proxy.KillAllConnections();

                for (var i = 10; i < 30; i++)
                {
                    sender.Table("test_ws_close_sf").Column("v", (long)i).At(DateTime.UtcNow);
                }
                await sender.SendAsync();

                for (var i = 0; i < 200; i++)
                {
                    try { await qwp.PingAsync(); break; }
                    catch { await Task.Delay(100); }
                }
            }

            await VerifyTableRowCountAsync("test_ws_close_sf", expected: 30, maxAttempts: 150);
        }
        finally
        {
            if (Directory.Exists(sfRoot))
            {
                try { Directory.Delete(sfRoot, recursive: true); } catch { }
            }
        }
    }

    [Test]
    public async Task Reconnect_WsCloseMidSession_NoSfDir_ReplaysFromRamRing()
    {
        await DropTableAsync("test_ws_close_ram");
        var realEndpoint = _questDb!.GetWebSocketEndpoint();
        using var proxy = new TcpProxy(realEndpoint);
        await proxy.StartAsync();

        using (var sender = Sender.New(
                   $"ws::addr={proxy.LocalEndpoint};auto_flush=off;"
                   + "reconnect_max_duration_millis=60000;"
                   + "reconnect_initial_backoff_millis=50;reconnect_max_backoff_millis=500;"))
        {
            var qwp = (IQwpWebSocketSender)sender;

            for (var i = 0; i < 10; i++)
            {
                sender.Table("test_ws_close_ram").Column("v", (long)i).At(DateTime.UtcNow);
            }
            await sender.SendAsync();
            await qwp.PingAsync();

            proxy.KillAllConnections();

            for (var i = 10; i < 30; i++)
            {
                sender.Table("test_ws_close_ram").Column("v", (long)i).At(DateTime.UtcNow);
            }
            await sender.SendAsync();

            for (var i = 0; i < 200; i++)
            {
                try { await qwp.PingAsync(); break; }
                catch { await Task.Delay(100); }
            }
        }

        await VerifyTableRowCountAsync("test_ws_close_ram", expected: 30, maxAttempts: 150);
    }

    [Test]
    public async Task SchemaCacheReuse_TwoFlushesSameTable_BothLand()
    {
        await DropTableAsync("test_ws_schema_reuse");
        var endpoint = _questDb!.GetWebSocketEndpoint();
        using (var sender = Sender.New($"ws::addr={endpoint};auto_flush=off;"))
        {
            for (var i = 0; i < 3; i++)
            {
                sender.Table("test_ws_schema_reuse").Symbol("s", "x").Column("v", (long)i).At(DateTime.UtcNow);
            }
            await sender.SendAsync();

            for (var i = 3; i < 6; i++)
            {
                sender.Table("test_ws_schema_reuse").Symbol("s", "y").Column("v", (long)i).At(DateTime.UtcNow);
            }
            await sender.SendAsync();
        }

        await VerifyTableRowCountAsync("test_ws_schema_reuse", expected: 6);
    }

    [Test]
    public async Task SymbolDeltaDict_SecondFlushAddsNewSymbol_BothPersist()
    {
        await DropTableAsync("test_ws_sym_delta");
        var endpoint = _questDb!.GetWebSocketEndpoint();
        using (var sender = Sender.New($"ws::addr={endpoint};auto_flush=off;"))
        {
            sender.Table("test_ws_sym_delta").Symbol("sym", "first").Column("v", 1L).At(DateTime.UtcNow);
            sender.Table("test_ws_sym_delta").Symbol("sym", "second").Column("v", 2L).At(DateTime.UtcNow);
            await sender.SendAsync();

            sender.Table("test_ws_sym_delta").Symbol("sym", "third").Column("v", 3L).At(DateTime.UtcNow);
            await sender.SendAsync();
        }

        await VerifyTableRowCountAsync("test_ws_sym_delta", expected: 3);

        var distinct = await QueryAllRowsAsync(
            "select distinct sym from test_ws_sym_delta order by sym");
        var symbols = distinct.Select(r => r[0].GetString()).OrderBy(s => s).ToArray();
        Assert.That(symbols, Is.EqualTo(new[] { "first", "second", "third" }));
    }

    [Test]
    public async Task ServerErrorFrame_IncompatibleSchema_SurfacesViaErrorHandler()
    {
        await DropTableAsync("test_ws_err_frame");
        var endpoint = _questDb!.GetWebSocketEndpoint();

        using (var setup = Sender.New($"ws::addr={endpoint};auto_flush=off;"))
        {
            setup.Table("test_ws_err_frame").Column("v", 1L).At(DateTime.UtcNow);
            await setup.SendAsync();
        }
        await VerifyTableRowCountAsync("test_ws_err_frame", expected: 1);

        SenderError? observed = null;
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Fresh sender's per-column buffer doesn't carry the Long type lock from the first
        // sender, so the conflicting Varchar bytes actually reach the wire and the server
        // returns SCHEMA_MISMATCH.
        using (var sender = Sender.New(new SenderOptions(
                   $"ws::addr={endpoint};auto_flush=off;on_server_error=halt;")
               {
                   error_handler = err => { observed = err; gate.TrySetResult(true); },
               }))
        {
            sender.Table("test_ws_err_frame").Column("v", "not a long").At(DateTime.UtcNow);
            try { await sender.SendAsync(); } catch (IngressError) { }
        }

        var fired = await Task.WhenAny(gate.Task, Task.Delay(TimeSpan.FromSeconds(5))) == gate.Task;
        Assert.That(fired, Is.True, "error handler never fired");
        Assert.That(observed, Is.Not.Null);
        Assert.That(observed!.Category, Is.AnyOf(
            SenderErrorCategory.SchemaMismatch,
            SenderErrorCategory.WriteError));
    }

    private async Task DropTableAsync(string tableName)
    {
        var endpoint = _questDb!.GetHttpEndpoint();
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        var url = $"http://{endpoint}/exec?query={Uri.EscapeDataString($"DROP TABLE IF EXISTS {tableName}")}";
        using var resp = await client.GetAsync(url);
        resp.EnsureSuccessStatusCode();
    }

    private async Task<JsonElement[]> QueryFirstRowAsync(string sql)
    {
        var rows = await QueryAllRowsAsync(sql);
        if (rows.Count == 0) Assert.Fail($"query returned no rows: {sql}");
        return rows[0];
    }

    private async Task<IReadOnlyList<JsonElement[]>> QueryAllRowsAsync(string sql)
    {
        var endpoint = _questDb!.GetHttpEndpoint();
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        var response = await client.GetAsync(
            $"http://{endpoint}/exec?query={Uri.EscapeDataString(sql)}");
        response.EnsureSuccessStatusCode();
        var content = await response.Content.ReadAsStringAsync();
        using var json = JsonDocument.Parse(content);
        if (!json.RootElement.TryGetProperty("dataset", out var dataset)
            || dataset.ValueKind != JsonValueKind.Array)
        {
            return Array.Empty<JsonElement[]>();
        }
        var result = new List<JsonElement[]>(dataset.GetArrayLength());
        foreach (var row in dataset.EnumerateArray())
        {
            var cols = new JsonElement[row.GetArrayLength()];
            var i = 0;
            foreach (var cell in row.EnumerateArray()) cols[i++] = cell.Clone();
            result.Add(cols);
        }
        return result;
    }

    private sealed class SingleRowRecordingHandler : QwpColumnBatchHandler
    {
        public QwpColumnBatch? Batch { get; private set; }
        public bool Ended { get; private set; }

        public override void OnBatch(QwpColumnBatch batch) => Batch = batch;
        public override void OnEnd(long totalRows) => Ended = true;
        public override void OnError(QwpStatusCode status, string message) =>
            Assert.Fail($"unexpected egress error: status={status}, msg={message}");
    }

    private async Task VerifyTableHasDataAsync(string tableName)
    {
        var value = await GetTableRowCountAsync(tableName, minimum: 1);
        Assert.That(value, Is.GreaterThan(0));
    }

    private async Task VerifyTableRowCountAsync(string tableName, long expected, int maxAttempts = 30)
    {
        var value = await GetTableRowCountAsync(tableName, minimum: expected, maxAttempts: maxAttempts);
        Assert.That(value, Is.GreaterThanOrEqualTo(expected));
    }

    private async Task<long> GetTableRowCountAsync(string tableName, long minimum, int maxAttempts = 30)
    {
        var endpoint = _questDb!.GetHttpEndpoint();
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        var attempts = 0;

        while (attempts < maxAttempts)
        {
            try
            {
                var response = await client.GetAsync(
                    $"http://{endpoint}/exec?query=select count(*) from {tableName}");
                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    using var json = JsonDocument.Parse(content);
                    if (json.RootElement.TryGetProperty("dataset", out var dataset)
                        && dataset.ValueKind == JsonValueKind.Array
                        && dataset.GetArrayLength() > 0)
                    {
                        var row = dataset[0];
                        if (row.ValueKind == JsonValueKind.Array && row.GetArrayLength() > 0)
                        {
                            var rowCount = row[0].GetInt64();
                            if (rowCount >= minimum)
                            {
                                return rowCount;
                            }
                        }
                    }
                }
            }
            catch
            {
            }

            await Task.Delay(200);
            attempts++;
        }

        Assert.Fail($"Table {tableName} did not reach {minimum} rows after {maxAttempts} attempts");
        return 0;
    }
}

#endif
