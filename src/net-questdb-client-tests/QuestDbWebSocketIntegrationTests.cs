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

namespace net_questdb_client_tests;

/// <summary>
///     Integration tests against a QuestDB build that ships <c>/write/v4</c> (currently master,
///     not yet released). Run with <c>QUESTDB_IMAGE=questdb/questdb:master dotnet test --filter
///     QuestDbWebSocketIntegrationTests</c> once the snapshot is available.
/// </summary>
[TestFixture]
[Explicit("Requires QuestDB master image (questdb/questdb:master) — sets QUESTDB_IMAGE env var")]
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
        using var sender = Sender.New(
            $"ws::addr={endpoint};auto_flush=off;request_durable_ack=on;");

        sender.Table("test_ws_durable")
            .Column("v", 42L)
            .At(DateTime.UtcNow);
        await sender.SendAsync();

        var ws = (QuestDB.Senders.IQwpWebSocketSender)sender;
        ws.Ping();

        var seqTxn = ws.GetHighestAckedSeqTxn("test_ws_durable");
        Assert.That(seqTxn, Is.GreaterThanOrEqualTo(0L));
    }

    private async Task VerifyTableHasDataAsync(string tableName)
    {
        var value = await GetTableRowCountAsync(tableName, minimum: 1);
        Assert.That(value, Is.GreaterThan(0));
    }

    private async Task VerifyTableRowCountAsync(string tableName, long expected)
    {
        var value = await GetTableRowCountAsync(tableName, minimum: expected);
        Assert.That(value, Is.GreaterThanOrEqualTo(expected));
    }

    private async Task<long> GetTableRowCountAsync(string tableName, long minimum)
    {
        var endpoint = _questDb!.GetHttpEndpoint();
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        var attempts = 0;
        const int maxAttempts = 30;

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
