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

using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using NUnit.Framework;
using QuestDB;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_tests;

/// <summary>
///     Live two-server failover suite for the QWP egress reader; port of
///     test_egress_failover.py. Two managed QuestDB instances, seeded identically; servers
///     are SIGKILLed mid-query (<see cref="QuestDbManager.StopAsync" /> is a hard kill) to
///     exercise the reader's failover path.
/// </summary>
[TestFixture]
public class QuestDbEgressFailoverTests
{
    // Large enough that the SELECT-after-failover spans many RESULT_BATCH frames.
    private const int RowCount = 1_000_000;
    private const string Table = "failover_test";

    private QuestDbManager? _server1;
    private QuestDbManager? _server2;

    [OneTimeSetUp]
    public async Task SetUpFixture()
    {
        _server1 = new QuestDbManager(19311, 19301);
        _server2 = new QuestDbManager(19312, 19302);

        if (_server1.UseLiveServer)
        {
            await DisposeServersAsync();
            Assert.Ignore("egress failover tests need two managed QuestDB instances");
        }

        try
        {
            await _server1.StartAsync();
            await _server2.StartAsync();
        }
        catch (Exception ex) when (ex is InvalidOperationException or FileNotFoundException)
        {
            await DisposeServersAsync();
            Assert.Ignore($"egress failover needs a QuestDB master build: {ex.Message}");
        }

        await SeedAsync(_server1);
        await SeedAsync(_server2);
    }

    [OneTimeTearDown]
    public async Task TearDownFixture()
    {
        await DisposeServersAsync();
    }

    // Mirrors the Python setUp: a previous test may have killed a server on purpose.
    [SetUp]
    public async Task EnsureServersUp()
    {
        foreach (var srv in new[] { _server1!, _server2! })
        {
            if (!srv.IsRunning)
            {
                await srv.StartAsync();
                await SeedAsync(srv);
            }
        }
    }

    /// <summary>
    ///     Connect string lists an unreachable address first and a healthy one second.
    ///     <see cref="QueryClient.New(string)" /> must walk past the refused connect and run the
    ///     query against the healthy endpoint; the connect-time walk does not count as a failover.
    /// </summary>
    [Test]
    public void InitialConnectWalksPastUnreachable()
    {
        var deadPort = ReserveClosedPort();
        var conf = $"ws::addr=127.0.0.1:{deadPort},127.0.0.1:{Server2HttpPort};";

        var handler = new FailoverHandler();
        using var client = QueryClient.New(conf);
        client.Execute("select 1", handler);

        Assert.That(handler.FailoverResets, Is.EqualTo(0),
            "connect-time endpoint walk must not increment the failover counter");
        Assert.That(handler.TotalRows, Is.EqualTo(1));
    }

    /// <summary>
    ///     The cursor opens against server #1, consumes one batch, then server #1 is SIGKILLed.
    ///     The reader fails over to server #2, replays the query, and re-streams every row.
    /// </summary>
    [Test]
    public void MidQueryFailover()
    {
        Assert.That(_server1!.IsRunning && _server2!.IsRunning, Is.True);

        var conf = $"ws::addr=127.0.0.1:{Server1HttpPort},127.0.0.1:{Server2HttpPort};";
        var handler = new FailoverHandler(onFirstBatch: () => Kill(_server1));

        using var client = QueryClient.New(conf);
        client.Execute($"SELECT * FROM {Table} ORDER BY val", handler);

        Assert.That(handler.FailoverResets, Is.GreaterThanOrEqualTo(1),
            "no failover happened despite the mid-stream kill");
        Assert.That(handler.TotalRows, Is.EqualTo(RowCount),
            "the replayed query against server #2 must deliver every row exactly once");
    }

    /// <summary>
    ///     With both endpoints dead and a tight failover budget, <c>Execute</c> must surface a
    ///     clean error rather than hang or crash — and the now-poisoned client must keep
    ///     surfacing clean errors on subsequent calls.
    /// </summary>
    [Test]
    public void ReaderPoisonedAfterFailoverExhaustion()
    {
        Assert.That(_server1!.IsRunning && _server2!.IsRunning, Is.True);

        var conf = $"ws::addr=127.0.0.1:{Server1HttpPort},127.0.0.1:{Server2HttpPort};" +
                   "failover_max_attempts=1;failover_backoff_initial_ms=1;failover_backoff_max_ms=2;";
        var handler = new FailoverHandler(onFirstBatch: () =>
        {
            Kill(_server1);
            Kill(_server2);
        });

        using var client = QueryClient.New(conf);
        Assert.Catch(() => client.Execute($"SELECT * FROM {Table} ORDER BY val", handler),
            "failover exhaustion must throw, not hang or crash");
        Assert.Catch(() => client.Execute("select 1", new FailoverHandler()),
            "a poisoned reader must keep surfacing a clean error");
    }

    /// <summary>
    ///     A single-address connect list: the failover rotation collapses to the same endpoint.
    ///     Once it dies, the budget must exhaust into a clean error rather than retrying forever.
    /// </summary>
    [Test]
    public void SingleEndpointFailoverExhaustsBudget()
    {
        Assert.That(_server1!.IsRunning, Is.True);

        var conf = $"ws::addr=127.0.0.1:{Server1HttpPort};" +
                   "failover_max_attempts=2;failover_backoff_initial_ms=1;failover_backoff_max_ms=2;";
        var handler = new FailoverHandler(onFirstBatch: () => Kill(_server1));

        using var client = QueryClient.New(conf);
        Assert.Catch(() => client.Execute($"SELECT * FROM {Table} ORDER BY val", handler),
            "single-endpoint exhaustion must throw, not retry indefinitely");
        Assert.Catch(() => client.Execute("select 1", new FailoverHandler()),
            "a poisoned reader must keep surfacing a clean error");
    }

    private int Server1HttpPort => int.Parse(_server1!.GetHttpEndpoint().Split(':')[^1]);
    private int Server2HttpPort => int.Parse(_server2!.GetHttpEndpoint().Split(':')[^1]);

    // QuestDbManager.StopAsync is a hard process kill — exactly the crash (no graceful WS Close)
    // the failover path must engage on.
    private static void Kill(QuestDbManager server) => server.StopAsync().GetAwaiter().GetResult();

    private async Task DisposeServersAsync()
    {
        if (_server1 is not null)
        {
            await _server1.DisposeAsync();
            _server1 = null;
        }

        if (_server2 is not null)
        {
            await _server2.DisposeAsync();
            _server2 = null;
        }
    }

    private static async Task SeedAsync(QuestDbManager server)
    {
        var endpoint = server.GetHttpEndpoint();
        await ExecAsync(endpoint, $"DROP TABLE IF EXISTS {Table}");
        // BYPASS WAL keeps the CTAS synchronous so the row count is correct on first read.
        await ExecAsync(endpoint,
            $"CREATE TABLE {Table} AS (" +
            $"SELECT cast(x*1000 AS timestamp) ts, x val FROM long_sequence({RowCount})" +
            ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

        var deadline = DateTime.UtcNow.AddSeconds(60);
        long last = -1;
        while (DateTime.UtcNow < deadline)
        {
            last = await CountAsync(endpoint);
            if (last == RowCount)
            {
                return;
            }

            await Task.Delay(200);
        }

        Assert.Fail($"seed timed out for {Table} on {endpoint}; last count {last}");
    }

    private static async Task ExecAsync(string httpEndpoint, string sql)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
        using var resp = await client.GetAsync(
            $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}");
        resp.EnsureSuccessStatusCode();
        using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        if (json.RootElement.TryGetProperty("error", out var err))
        {
            throw new InvalidOperationException($"exec failed: {err.GetString()}");
        }
    }

    private static async Task<long> CountAsync(string httpEndpoint)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        using var resp = await client.GetAsync(
            $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString($"SELECT count(*) FROM {Table}")}");
        if (!resp.IsSuccessStatusCode)
        {
            return -1;
        }

        using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        return json.RootElement.TryGetProperty("dataset", out var ds) && ds.GetArrayLength() > 0
            ? ds[0][0].GetInt64()
            : -1;
    }

    private static int ReserveClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class FailoverHandler : QwpColumnBatchHandler
    {
        private readonly Action? _onFirstBatch;
        private bool _firstBatchSeen;

        public FailoverHandler(Action? onFirstBatch = null) => _onFirstBatch = onFirstBatch;

        public long TotalRows { get; private set; }

        public int FailoverResets { get; private set; }

        public override void OnBatch(QwpColumnBatch batch)
        {
            if (!_firstBatchSeen)
            {
                _firstBatchSeen = true;
                _onFirstBatch?.Invoke();
            }

            TotalRows += batch.RowCount;
        }

        public override void OnFailoverReset(QwpServerInfo? newNode)
        {
            FailoverResets++;
            // The query replays from batch 0 on the new endpoint; drop rows counted pre-failover.
            TotalRows = 0;
        }
    }
}

#endif
