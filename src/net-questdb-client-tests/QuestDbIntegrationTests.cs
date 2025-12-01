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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using NUnit.Framework;
using QuestDB;

namespace QuestDB.Client.Tests;

/// <summary>
/// Integration tests against a real QuestDB instance.
/// Requires QuestDB to be downloaded and running.
/// </summary>
[TestFixture]
public class QuestDbIntegrationTests
{
    private QuestDbManager? _questDb;
    private const int IlpPort = 19009;
    private const int HttpPort = 19000;

    [OneTimeSetUp]
    public async Task SetUpFixture()
    {
        _questDb = new QuestDbManager(IlpPort, HttpPort);
        await _questDb.EnsureDownloadedAsync();
        await _questDb.StartAsync();
        await Task.Delay(1000); // Give QuestDB a moment to fully initialize
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
    public async Task CanSendDataOverHttp()
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        using var sender = Sender.New($"http::addr={httpEndpoint};auto_flush=off;");

        // Send test data
        await sender
            .Table("test_http")
            .Symbol("tag", "test")
            .Column("value", 42L)
            .AtAsync(DateTime.UtcNow);
        await sender.SendAsync();

        // Verify data was written
        await VerifyTableHasDataAsync("test_http");
    }

    [Test]
    public async Task CanSendDataOverIlp()
    {
        var ilpEndpoint = _questDb!.GetIlpEndpoint();
        using var sender = Sender.New($"tcp::addr={ilpEndpoint};auto_flush=off;");

        // Send test data
        await sender
            .Table("test_ilp")
            .Symbol("tag", "test")
            .Column("value", 123L)
            .AtAsync(DateTime.UtcNow);
        await sender.SendAsync();

        // Verify data was written
        await VerifyTableHasDataAsync("test_ilp");
    }

    [Test]
    public async Task CanSendMultipleRows()
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        using var sender = Sender.New($"http::addr={httpEndpoint};auto_flush=off;");

        // Send multiple rows
        for (int i = 0; i < 10; i++)
        {
            await sender
                .Table("test_multiple_rows")
                .Symbol("tag", $"test_{i}")
                .Column("value", (long)(i * 10))
                .AtAsync(DateTime.UtcNow);
        }

        await sender.SendAsync();

        // Verify all rows were written
        var rowCount = await GetTableRowCountAsync("test_multiple_rows");
        Assert.That(rowCount, Is.GreaterThanOrEqualTo(10), "Expected at least 10 rows");
    }

    [Test]
    public async Task CanSendDifferentDataTypes()
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        using var sender = Sender.New($"http::addr={httpEndpoint};auto_flush=off;");

        var now = DateTime.UtcNow;

        // Send different data types
        await sender
            .Table("test_data_types")
            .Symbol("symbol_col", "test")
            .Column("long_col", 42L)
            .Column("double_col", 3.14)
            .Column("string_col", "hello world")
            .Column("bool_col", true)
            .AtAsync(now);

        await sender.SendAsync();

        // Verify the row exists
        var rowCount = await GetTableRowCountAsync("test_data_types");
        Assert.That(rowCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task MultiUrlFallback()
    {
        // Test that the client properly handles multiple URLs with fallback
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        var badEndpoint = "http://localhost:19001"; // Non-existent endpoint

        // The client should try the bad endpoint first, then fallback to the good one
        using var sender = Sender.New(
            $"http::addr={badEndpoint},{httpEndpoint};auto_flush=off;");

        await sender
            .Table("test_multi_url")
            .Symbol("tag", "fallback")
            .Column("value", 999L)
            .AtAsync(DateTime.UtcNow);

        await sender.SendAsync();

        // Verify data was written despite the bad endpoint
        await VerifyTableHasDataAsync("test_multi_url");
    }

    [Test]
    public async Task CanAutoFlush()
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        using var sender = Sender.New(
            $"http::addr={httpEndpoint};auto_flush=on;auto_flush_rows=1;");

        // Send data - should auto-flush due to auto_flush_rows=1
        await sender
            .Table("test_auto_flush")
            .Symbol("tag", "test")
            .Column("value", 777L)
            .AtAsync(DateTime.UtcNow);

        // Give it a moment to flush
        await Task.Delay(100);

        // Verify data was written
        await VerifyTableHasDataAsync("test_auto_flush");
    }

    [Test]
    public async Task HealthCheckEndpoint()
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();

        using var client = new HttpClient();
        var response = await client.GetAsync($"{httpEndpoint}/api/v1/health");

        Assert.That(response.IsSuccessStatusCode, "Health check should succeed");

        var content = await response.Content.ReadAsStringAsync();
        var json = JsonDocument.Parse(content);
        var status = json.RootElement.GetProperty("status").GetString();

        Assert.That(status, Is.EqualTo("ok"), "Status should be 'ok'");
    }

    [Test]
    public async Task TablesEndpoint()
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();

        using var client = new HttpClient();
        var response = await client.GetAsync($"{httpEndpoint}/api/v1/tables");

        Assert.That(response.IsSuccessStatusCode, "Tables endpoint should succeed");

        var content = await response.Content.ReadAsStringAsync();
        var json = JsonDocument.Parse(content);
        Assert.That(json.RootElement.TryGetProperty("tables", out _), "Response should contain tables property");
    }

    private async Task VerifyTableHasDataAsync(string tableName)
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        // Retry a few times to allow for write latency
        var attempts = 0;
        const int maxAttempts = 10;

        while (attempts < maxAttempts)
        {
            try
            {
                var response = await client.GetAsync(
                    $"{httpEndpoint}/api/v1/query?query=select count(*) as cnt from {tableName}");

                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var json = JsonDocument.Parse(content);
                    if (json.RootElement.TryGetProperty("dataset", out var dataset) &&
                        dataset.TryGetProperty("count", out var count))
                    {
                        var rowCount = count.GetInt64();
                        if (rowCount > 0)
                        {
                            return;
                        }
                    }
                }
            }
            catch
            {
                // Retry
            }

            await Task.Delay(100);
            attempts++;
        }

        Assert.Fail($"Table {tableName} has no data after {maxAttempts} attempts");
    }

    private async Task<long> GetTableRowCountAsync(string tableName)
    {
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        var response = await client.GetAsync(
            $"{httpEndpoint}/api/v1/query?query=select count(*) as cnt from {tableName}");

        if (!response.IsSuccessStatusCode)
        {
            return 0;
        }

        var content = await response.Content.ReadAsStringAsync();
        var json = JsonDocument.Parse(content);
        if (json.RootElement.TryGetProperty("dataset", out var dataset) &&
            dataset.TryGetProperty("count", out var count))
        {
            return count.GetInt64();
        }

        return 0;
    }
}
