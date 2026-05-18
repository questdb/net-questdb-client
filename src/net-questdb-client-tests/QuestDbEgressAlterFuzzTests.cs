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

/// <summary>Live-server egress ALTER fuzz; port of egress_live_server_alter_fuzz.rs.</summary>
[TestFixture]
public class QuestDbEgressAlterFuzzTests
{
    private const int IlpPort = 19509;
    private const int HttpPort = 19500;
    private const ulong DefaultSeed = 0x5EED_1234_ABCD_0001UL;

    private static readonly string[] BaseCols = { "id", "v", "cat", "ts" };

    [Test]
    public async Task SelectAlterSequence()
    {
        var rng = new SplitMix64(SeedFor(nameof(SelectAlterSequence)));

        var rowCount = 50 + (int)rng.GenRange(951);
        var spacing = PickSpacingMicros(rng);
        var opCount = 15 + (int)rng.GenRange(26);
        var structuralProbPermil = 150 + (int)rng.GenRange(251);
        var maxLiveAdded = 2 + (int)rng.GenRange(5);
        var compression = PickCompression(rng);
        TestContext.Progress.WriteLine(
            $"[select_alter_sequence] rows={rowCount} spacing={spacing} ops={opCount} " +
            $"structuralPermil={structuralProbPermil} maxLiveAdded={maxLiveAdded} compression={compression}");

        var server = new QuestDbManager(IlpPort, HttpPort);
        try
        {
            await server.StartAsync();
        }
        catch (Exception ex) when (ex is InvalidOperationException or FileNotFoundException)
        {
            await server.DisposeAsync();
            Assert.Ignore($"QWP egress ALTER fuzz needs a QuestDB master build: {ex.Message}");
            return;
        }

        try
        {
            var http = server.GetHttpEndpoint();
            const string table = "fz_seq";

            await ExecAsync(http, $"drop table if exists \"{table}\"");
            await ExecAsync(http,
                $"create table \"{table}\" (id LONG, v DOUBLE, cat SYMBOL, ts TIMESTAMP) " +
                "timestamp(ts) partition by day wal");
            await ExecAsync(http,
                $"insert into \"{table}\" select x, x * 1.5, " +
                "case when x % 4 = 0 then 'a' when x % 4 = 1 then 'b' " +
                "when x % 4 = 2 then 'c' else 'd' end, " +
                $"CAST((x - 1) * {spacing}L AS TIMESTAMP) from long_sequence({rowCount})");
            await WaitForRowCountAsync(http, table, rowCount);

            var conn = $"ws::addr={server.GetWebSocketEndpoint()};path={QwpConstants.ReadPath};target=any;" +
                       (compression.Length > 0 ? compression + ";" : string.Empty);
            using var client = QueryClient.New(conn);

            RunSelectShape(client, rng, 0, table, rowCount, spacing, Array.Empty<string>());

            var liveAdded = new List<string>();
            var nextColumnId = 0;

            for (var op = 0; op < opCount; op++)
            {
                var wantStructural = rng.GenRange(1000) < (ulong)structuralProbPermil;
                var canAdd = liveAdded.Count < maxLiveAdded;
                var canDrop = liveAdded.Count > 0;
                var didStructural = false;

                if (wantStructural)
                {
                    var doAdd = (canAdd, canDrop) switch
                    {
                        (true, false) => true,
                        (false, true) => false,
                        (true, true) => rng.GenRange(10) < 6,
                        _ => false,
                    };

                    if (canAdd && doAdd)
                    {
                        var name = $"extra_{nextColumnId++}";
                        await ExecAsync(http, $"alter table \"{table}\" add column \"{name}\" VARCHAR");
                        liveAdded.Add(name);
                        await WaitForColumnCountAsync(http, table, BaseCols.Length + liveAdded.Count);
                        didStructural = true;
                    }
                    else if (canDrop && !doAdd)
                    {
                        var victim = liveAdded[(int)rng.GenRange((ulong)liveAdded.Count)];
                        liveAdded.Remove(victim);
                        await ExecAsync(http, $"alter table \"{table}\" drop column \"{victim}\"");
                        await WaitForColumnCountAsync(http, table, BaseCols.Length + liveAdded.Count);
                        didStructural = true;
                    }
                }

                if (!didStructural)
                {
                    var shape = (int)rng.GenRange(6);
                    RunSelectShape(client, rng, shape, table, rowCount, spacing, liveAdded.ToArray());
                }
            }
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    private static void RunSelectShape(
        IQwpQueryClient client, SplitMix64 rng, int shape, string table,
        int totalRows, long spacing, string[] liveAdded)
    {
        var label = $"shape={shape}";
        switch (shape)
        {
            case 0:
            {
                var rows = Query(client, $"select id from \"{table}\"");
                Assert.That(rows.Count, Is.EqualTo(totalRows), $"{label}: row_count drift");
                for (var r = 0; r < rows.Count; r++)
                {
                    Assert.That(rows[r][0], Is.EqualTo((long)(r + 1)), $"{label}: row {r} id-mismatch");
                }
                break;
            }
            case 1:
            {
                var threshold = 1 + (long)rng.GenRange((ulong)Math.Max(1, totalRows - 1));
                var rows = Query(client, $"select id, v from \"{table}\" where id > {threshold}");
                Assert.That(rows.Count, Is.EqualTo(totalRows - threshold), $"{label}: row_count drift");
                for (var r = 0; r < rows.Count; r++)
                {
                    var id = threshold + r + 1;
                    Assert.That(rows[r][0], Is.EqualTo(id), $"{label}: id-mismatch");
                    Assert.That(rows[r][1], Is.EqualTo(ExpectedV(id)), $"{label}: v-mismatch id={id}");
                }
                break;
            }
            case 2:
            {
                var rows = Query(client, $"select cat, count(*) as c from \"{table}\"");
                var counts = new Dictionary<string, long>(StringComparer.Ordinal);
                foreach (var row in rows)
                {
                    counts[(string)row[0]!] = (long)row[1]!;
                }
                Assert.That(counts.Count, Is.EqualTo(4), $"{label}: 4 distinct cats expected");
                foreach (var (kMod, name) in new[] { (0L, "a"), (1L, "b"), (2L, "c"), (3L, "d") })
                {
                    Assert.That(counts.TryGetValue(name, out var got), Is.True, $"{label}: cat={name} missing");
                    Assert.That(got, Is.EqualTo(CatCount(totalRows, kMod)), $"{label}: cat={name} count-mismatch");
                }
                break;
            }
            case 3:
            {
                var loRow = 1 + (long)rng.GenRange((ulong)Math.Max(1, totalRows - 2));
                var span = 1 + (long)rng.GenRange((ulong)Math.Max(1, totalRows - loRow));
                var tsLo = (loRow - 1) * spacing;
                var tsHi = (loRow + span - 1) * spacing;
                var rows = Query(client,
                    $"select id from \"{table}\" where ts >= CAST({tsLo}L AS TIMESTAMP) " +
                    $"and ts < CAST({tsHi}L AS TIMESTAMP)");
                Assert.That(rows.Count, Is.EqualTo(span), $"{label}: row_count drift");
                for (var r = 0; r < rows.Count; r++)
                {
                    Assert.That(rows[r][0], Is.EqualTo(loRow + r), $"{label}: id-mismatch");
                }
                break;
            }
            case 4:
            {
                var pickCount = 1 + (int)rng.GenRange((ulong)BaseCols.Length);
                var shuffled = Enumerable.Range(0, BaseCols.Length).ToList();
                for (var i = shuffled.Count - 1; i > 0; i--)
                {
                    var j = (int)rng.GenRange((ulong)(i + 1));
                    (shuffled[i], shuffled[j]) = (shuffled[j], shuffled[i]);
                }
                var cols = shuffled.Take(pickCount).Select(i => BaseCols[i]).ToArray();
                var rows = Query(client, $"select {string.Join(",", cols)} from \"{table}\" order by id");
                Assert.That(rows.Count, Is.EqualTo(totalRows), $"{label}: row_count drift");
                for (var r = 0; r < rows.Count; r++)
                {
                    for (var c = 0; c < cols.Length; c++)
                    {
                        VerifyBaseCell(rows[r][c], cols[c], r + 1, spacing, $"shape=4 col={cols[c]}");
                    }
                }
                break;
            }
            default:
            {
                var rows = Query(client, $"select * from \"{table}\"", out var columnCount);
                Assert.That(columnCount, Is.EqualTo(BaseCols.Length + liveAdded.Length),
                    "shape=5: column count drift");
                Assert.That(rows.Count, Is.EqualTo(totalRows), "shape=5: row_count drift");
                for (var r = 0; r < rows.Count; r++)
                {
                    for (var c = 0; c < BaseCols.Length; c++)
                    {
                        VerifyBaseCell(rows[r][c], BaseCols[c], r + 1, spacing, "shape=5 base");
                    }

                    for (var i = 0; i < liveAdded.Length; i++)
                    {
                        Assert.That(rows[r][BaseCols.Length + i], Is.Null,
                            $"shape=5: extra column {liveAdded[i]} row {r} expected NULL");
                    }
                }
                break;
            }
        }
    }

    private static void VerifyBaseCell(object? cell, string inCol, long id, long spacing, string label)
    {
        switch (inCol)
        {
            case "id":
                Assert.That(cell, Is.EqualTo(id), $"{label}: id-mismatch");
                break;
            case "v":
                Assert.That(cell, Is.EqualTo(ExpectedV(id)), $"{label}: v-mismatch id={id}");
                break;
            case "cat":
                Assert.That(cell, Is.EqualTo(CatFor(id)), $"{label}: cat-mismatch id={id}");
                break;
            case "ts":
                Assert.That(cell, Is.EqualTo((id - 1) * spacing), $"{label}: ts-mismatch id={id}");
                break;
            default:
                Assert.Fail($"{label}: unexpected base column {inCol}");
                break;
        }
    }

    private static double ExpectedV(long id) => id * 1.5;

    private static string CatFor(long id) => (id % 4) switch { 0 => "a", 1 => "b", 2 => "c", _ => "d" };

    private static long CatCount(long totalRows, long kMod)
        => kMod == 0 ? totalRows / 4 : (totalRows + 4 - kMod) / 4;

    private static long PickSpacingMicros(SplitMix64 rng)
    {
        long[] choices = { 300_000_000, 864_000_000, 3_600_000_000, 21_600_000_000 };
        return choices[(int)rng.GenRange((ulong)choices.Length)];
    }

    private static string PickCompression(SplitMix64 rng) => rng.GenRange(5) switch
    {
        0 => string.Empty,
        1 => "compression=raw",
        2 => "compression=auto",
        3 => "compression=zstd",
        _ => $"compression=zstd;compression_level={1 + rng.GenRange(9)}",
    };

    private static List<object?[]> Query(IQwpQueryClient client, string sql)
        => Query(client, sql, out _);

    private static List<object?[]> Query(IQwpQueryClient client, string sql, out int columnCount)
    {
        var handler = new CollectingHandler();
        client.Execute(sql, handler);
        columnCount = handler.ColumnCount;
        return handler.Rows;
    }

    private sealed class CollectingHandler : QwpColumnBatchHandler
    {
        public List<object?[]> Rows { get; } = new();
        public int ColumnCount { get; private set; }

        public override void OnBatch(QwpColumnBatch batch)
        {
            ColumnCount = batch.ColumnCount;
            for (var r = 0; r < batch.RowCount; r++)
            {
                var row = new object?[batch.ColumnCount];
                for (var c = 0; c < batch.ColumnCount; c++)
                {
                    row[c] = batch.IsNull(c, r) ? null : ExtractCell(batch, c, r);
                }
                Rows.Add(row);
            }
        }

        public override void OnError(byte status, string message)
            => Assert.Fail($"unexpected egress error: status={status}, msg={message}");

        private static object ExtractCell(QwpColumnBatch batch, int c, int r) => batch.GetColumnWireType(c) switch
        {
            QwpTypeCode.Long => batch.GetLongValue(c, r),
            QwpTypeCode.Int => batch.GetIntValue(c, r),
            QwpTypeCode.Double => batch.GetDoubleValue(c, r),
            QwpTypeCode.Timestamp => batch.GetTimestampValue(c, r),
            QwpTypeCode.TimestampNanos => batch.GetTimestampValue(c, r),
            QwpTypeCode.Symbol => batch.GetSymbol(c, r) ?? string.Empty,
            QwpTypeCode.Boolean => batch.GetBoolValue(c, r),
            _ => batch.GetString(c, r) ?? string.Empty,
        };
    }

    private static async Task ExecAsync(string httpEndpoint, string sql)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
        using var resp = await client.GetAsync($"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}");
        Assert.That((int)resp.StatusCode, Is.InRange(200, 399), $"exec failed: {sql}");
    }

    private static async Task WaitForRowCountAsync(string httpEndpoint, string table, int expected)
        => await PollAsync(httpEndpoint, $"select count() from \"{table}\"",
            ds => ds.GetArrayLength() > 0 && ds[0][0].GetInt64() >= expected,
            $"{table} did not reach {expected} rows");

    private static async Task WaitForColumnCountAsync(string httpEndpoint, string table, int expected)
        => await PollAsync(httpEndpoint, $"show columns from \"{table}\"",
            ds => ds.GetArrayLength() == expected,
            $"{table} did not reach {expected} columns");

    private static async Task PollAsync(string httpEndpoint, string sql, Func<JsonElement, bool> done, string failMsg)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var resp = await client.GetAsync($"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}");
                if (resp.IsSuccessStatusCode)
                {
                    using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
                    if (json.RootElement.TryGetProperty("dataset", out var ds) && done(ds))
                    {
                        return;
                    }
                }
            }
            catch
            {
            }

            await Task.Delay(80);
        }

        Assert.Fail($"{failMsg} within 60s");
    }

    private static ulong SeedFor(string testName)
    {
        var raw = Environment.GetEnvironmentVariable("QWP_EGRESS_FUZZ_SEED");
        var baseSeed = DefaultSeed;
        if (!string.IsNullOrWhiteSpace(raw))
        {
            raw = raw.Trim();
            baseSeed = raw.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? Convert.ToUInt64(raw.Substring(2), 16)
                : ulong.Parse(raw);
        }

        unchecked
        {
            var hash = 0xCBF2_9CE4_8422_2325UL;
            foreach (var b in System.Text.Encoding.UTF8.GetBytes(testName))
            {
                hash ^= b;
                hash *= 0x100_0000_01B3UL;
            }
            var combined = baseSeed + hash;
            TestContext.Progress.WriteLine($"[qwp_egress_fuzz seed] {testName} seed=0x{combined:x16}");
            return combined;
        }
    }

    private sealed class SplitMix64
    {
        private ulong _state;

        public SplitMix64(ulong seed) => _state = seed | 0x9E37_79B9_7F4A_7C15UL;

        public ulong NextU64()
        {
            unchecked
            {
                _state += 0x9E37_79B9_7F4A_7C15UL;
                var z = _state;
                z = (z ^ (z >> 30)) * 0xBF58_476D_1CE4_E5B9UL;
                z = (z ^ (z >> 27)) * 0x94D0_49BB_1331_11EBUL;
                return z ^ (z >> 31);
            }
        }

        public ulong GenRange(ulong bound) => NextU64() % bound;
    }
}

#endif
