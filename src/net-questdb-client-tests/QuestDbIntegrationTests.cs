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

using System.Text.Json;
using NUnit.Framework;

namespace QuestDB.Client.Tests;

/// <summary>
///     Integration tests against a real QuestDB instance running in Docker.
///     Requires Docker to be installed and running.
/// </summary>
[TestFixture]
public class QuestDbIntegrationTests
{
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

    private QuestDbManager? _questDb;
    private const int IlpPort = 19009;
    private const int HttpPort = 19000;

    [Test]
    public async Task CanSendDataOverHttp()
    {
        var       httpEndpoint = _questDb!.GetHttpEndpoint();
        using var sender       = Sender.New($"http::addr={httpEndpoint};auto_flush=off;");

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
        var       ilpEndpoint = _questDb!.GetIlpEndpoint();
        using var sender      = Sender.New($"tcp::addr={ilpEndpoint};auto_flush=off;");

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
        var       httpEndpoint = _questDb!.GetHttpEndpoint();
        using var sender       = Sender.New($"http::addr={httpEndpoint};auto_flush=off;");

        // Send multiple rows
        for (var i = 0; i < 10; i++)
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
        var       httpEndpoint = _questDb!.GetHttpEndpoint();
        using var sender       = Sender.New($"http::addr={httpEndpoint};auto_flush=off;");

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

        long rowCount = 0;
        var  retries  = 10;
        for (var i = 0; i < retries; i++)
        {
            rowCount = await GetTableRowCountAsync("test_data_types");
            if (rowCount != 0)
            {
                break;
            }

            await Task.Delay(500);
        }

        Assert.That(rowCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task MultiUrlFallback()
    {
        // Test that the client properly handles multiple URLs with fallback
        var httpEndpoint = _questDb!.GetHttpEndpoint();
        var badEndpoint  = "http://localhost:19001"; // Non-existent endpoint

        // The client should try the bad endpoint first, then fallback to the good one
        using var sender = Sender.New(
            $"http::addr={badEndpoint};addr={httpEndpoint};auto_flush=off;");

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
    public async Task SendRowsWhileRestartingDatabase()
    {
        const int rowsPerBatch = 10;
        const int numBatches = 5;
        const int expectedTotalRows = rowsPerBatch * numBatches;

        // Create a persistent Docker volume for the test database
        var volumeName = $"questdb-test-vol-{Guid.NewGuid().ToString().Substring(0, 8)}";

        // Use a separate QuestDB instance for this chaos test to avoid conflicts
        var testDb = new QuestDbManager(port: 29009, httpPort: 29000);
        testDb.SetVolume(volumeName);
        try
        {
            await testDb.StartAsync();

            var httpEndpoint = testDb.GetHttpEndpoint();
            using var sender = Sender.New(
                $"http::addr={httpEndpoint};auto_flush=off;retry_timeout=60000;");

            var batchesSent = 0;
            var sendLock = new object();

            // Task that restarts the database
            var restartTask = Task.Run(async () =>
            {
                // Allow first batch to be sent while database is up
                await Task.Delay(600);

                // Perform restart cycles
                for (var i = 0; i < 2; i++)
                {
                    TestContext.WriteLine($"Stopping test database (cycle {i + 1})");
                    await testDb.StopAsync();

                    // Database is down - sender will retry
                    await Task.Delay(1200);

                    TestContext.WriteLine($"Starting test database (cycle {i + 1})");
                    await testDb.StartAsync();

                    // Wait for client to detect database is back up
                    await Task.Delay(800);
                }

                TestContext.WriteLine("Test database restart cycles complete");
            });

            // Task that sends rows continuously
            var sendTask = Task.Run(async () =>
            {
                for (var batch = 0; batch < numBatches; batch++)
                {
                    try
                    {
                        // Build batch of rows
                        for (var i = 0; i < rowsPerBatch; i++)
                        {
                            var rowId = batch * rowsPerBatch + i;
                            await sender
                                  .Table("test_chaos")
                                  .Symbol("batch", $"batch_{batch}")
                                  .Column("row_id", (long)rowId)
                                  .Column("value", (double)(rowId * 100))
                                  .AtAsync(DateTime.UtcNow);
                        }

                        // Send the batch
                        TestContext.WriteLine($"Sending batch {batch}");
                        await sender.SendAsync();

                        lock (sendLock)
                        {
                            batchesSent++;
                        }
                        TestContext.WriteLine($"Batch {batch} sent successfully");

                        // Wait before next batch
                        await Task.Delay(500);
                    }
                    catch (Exception ex)
                    {
                        TestContext.WriteLine($"Error sending batch {batch}: {ex.GetType().Name} - {ex.Message}");
                        throw;
                    }
                }

                TestContext.WriteLine($"All batches sent. Total: {batchesSent}");
            });

            // Wait for both tasks to complete
            await Task.WhenAll(sendTask, restartTask);

            // Wait for final data to be written
            await Task.Delay(2000);

            // Query the row count, with retries
            long actualRowCount = 0;
            var maxAttempts = 20;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                try
                {
                    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
                    var response = await client.GetAsync($"{httpEndpoint}/exec?query=test_chaos");

                    if (response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        var json = JsonDocument.Parse(content);
                        if (json.RootElement.TryGetProperty("count", out var countProp))
                        {
                            actualRowCount = countProp.GetInt64();
                            TestContext.WriteLine($"Attempt {attempt + 1}: Found {actualRowCount} rows");
                            if (actualRowCount >= expectedTotalRows)
                                break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    TestContext.WriteLine($"Attempt {attempt + 1}: Query failed - {ex.Message}");
                }

                await Task.Delay(500);
            }

            // Assert that all rows made it
            Assert.That(
                actualRowCount,
                Is.GreaterThanOrEqualTo(expectedTotalRows),
                $"Expected {expectedTotalRows} rows but found {actualRowCount}. " +
                $"Successfully sent {batchesSent} batches of {rowsPerBatch} rows each");
        }
        finally
        {
            // Cleanup
            await testDb.StopAsync();
            await testDb.DisposeAsync();
        }
    }
    
    private async Task VerifyTableHasDataAsync(string tableName)
    {
        var value = await GetTableRowCountAsync(tableName);
        Assert.That(value, Is.GreaterThan(0));
    }

    private async Task<long> GetTableRowCountAsync(string tableName)
    {
        var       httpEndpoint = _questDb!.GetHttpEndpoint();
        using var client       = new HttpClient { Timeout = TimeSpan.FromSeconds(10), };

        // Retry a few times to allow for write latency
        var       attempts    = 0;
        const int maxAttempts = 10;

        while (attempts < maxAttempts)
        {
            try
            {
                var response = await client.GetAsync(
                                   $"{httpEndpoint}/exec?query={tableName}");

                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var json    = JsonDocument.Parse(content);
                    if (
                        json.RootElement.TryGetProperty("count", out var count))
                    {
                        var rowCount = count.GetInt64();
                        if (rowCount > 0)
                        {
                            return rowCount;
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
        return 0;
    }
}