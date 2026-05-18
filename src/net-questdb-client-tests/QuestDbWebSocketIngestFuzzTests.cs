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

using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using NUnit.Framework;
using QuestDB;
using QuestDB.Senders;

namespace net_questdb_client_tests;

/// <summary>QWP ingress schema-fuzz suite; port of qwp_ws_fuzz.py + TestQwpWsFuzz.</summary>
[TestFixture]
public class QuestDbWebSocketIngestFuzzTests
{
    private const int IlpPort = 19209;
    private const int HttpPort = 19200;
    private const int BatchSize = 10;
    private const int NewColumnRandomizeFactor = 2;
    private const int MaxSkippedColumns = 2;

    private QuestDbManager? _questDb;

    [OneTimeSetUp]
    public async Task SetUpFixture()
    {
        _questDb = new QuestDbManager(IlpPort, HttpPort);
        try
        {
            await _questDb.StartAsync();
        }
        catch (Exception ex) when (ex is InvalidOperationException or FileNotFoundException)
        {
            await _questDb.DisposeAsync();
            _questDb = null;
            Assert.Ignore($"QWP ingress fuzz needs a QuestDB master build: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public async Task TearDownFixture()
    {
        if (_questDb is not null)
        {
            await _questDb.DisposeAsync();
        }
    }

    private static readonly string[][] ColNameBases =
    {
        new[] { "terület", "TERÜLet", "tERülET", "TERÜLET" },
        new[] { "temperature", "TEMPERATURE", "Temperature", "TempeRaTuRe" },
        new[] { "humidity", "HUMIdity", "HumiditY", "HUMIDITY" },
        new[] { "notes", "NOTES", "NotEs", "noTeS" },
        new[] { "hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET" },
        new[] { "ветер", "Ветер", "ВЕТЕР", "вЕТЕр" },
        new[] { "flag", "FLAG", "Flag", "flAG" },
        new[] { "count", "COUNT", "CounT", "Count" },
        new[] { "rank_i", "RANK_I", "rank_I", "Rank_i" },
        new[] { "trace_id", "TRACE_ID", "trace_Id", "TraceID" },
    };

    private enum ColType { String, Double, Boolean, Long, Int, Uuid }

    private static readonly ColType[] ColTypes =
    {
        ColType.String, ColType.Double, ColType.Double, ColType.String, ColType.Double,
        ColType.String, ColType.Boolean, ColType.Long, ColType.Int, ColType.Uuid,
    };

    private static readonly string[] ColValueBases =
        { "europe", "8", "2", "note", "6", "wind", "", "4", "9", "" };

    private static readonly string[][] SymbolNameBases =
    {
        new[] { "location", "Location", "LOCATION", "loCATion" },
        new[] { "city", "ciTY", "CITY", "City" },
    };

    private static readonly string[] SymbolValueBases = { "us-midwest", "London" };

    private static readonly char[] NonAsciiChars = { 'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 'む' };

    [Test]
    public Task AddColumns() => RunFuzz(
        new LoadParams(NumLines: 40, NumIterations: 3, NumThreads: 3, NumTables: 3, WaitMs: 25),
        new FuzzParams { ColumnSkipFactor = 1, NewColumnFactor = 2, NonAsciiValueFactor = 6,
            ExerciseSymbols = true, ColumnConvertProb = 0.1 });

    [Test]
    public Task AddColumnsNoSymbols() => RunFuzz(
        new LoadParams(15, 2, 2, 5, 25),
        new FuzzParams { NewColumnFactor = 4, NonAsciiValueFactor = 3, DiffCasesInColNames = true,
            ExerciseSymbols = false, ColumnConvertProb = 0.15 });

    [Test]
    public Task AddConvertColumns() => RunFuzz(
        new LoadParams(15, 2, 2, 5, 25),
        new FuzzParams { NewColumnFactor = 4, ExerciseSymbols = true, ColumnConvertProb = 0.2 });

    [Test]
    public Task AllMixed() => RunFuzz(
        new LoadParams(40, 3, 4, 5, 20),
        new FuzzParams { DuplicatesFactor = 3, ColumnReorderingFactor = 4, ColumnSkipFactor = 5,
            NewColumnFactor = 10, NonAsciiValueFactor = 5, DiffCasesInColNames = true,
            ExerciseSymbols = true, ColumnConvertProb = 0.05 });

    [Test]
    public Task AllMixedNoSymbols() => RunFuzz(
        new LoadParams(40, 3, 4, 5, 20),
        new FuzzParams { DuplicatesFactor = 3, ColumnReorderingFactor = 4, ColumnSkipFactor = 5,
            NewColumnFactor = 10, NonAsciiValueFactor = 5, DiffCasesInColNames = true,
            ExerciseSymbols = false, ColumnConvertProb = 0.05 });

    [Test]
    public Task AllMixedSingleTable() => RunFuzz(
        new LoadParams(40, 3, 4, 1, 20),
        new FuzzParams { DuplicatesFactor = 3, ColumnReorderingFactor = 4, ColumnSkipFactor = 5,
            NewColumnFactor = 10, NonAsciiValueFactor = 5, DiffCasesInColNames = true,
            ExerciseSymbols = true, ColumnConvertProb = 0.05 });

    [Test]
    public Task CaseVariationReorderingColumns() => RunFuzz(
        new LoadParams(60, 3, 4, 5, 20),
        new FuzzParams { ColumnReorderingFactor = 4, NewColumnFactor = 2, DiffCasesInColNames = true,
            ExerciseSymbols = true });

    [Test]
    public Task DuplicatesReorderingColumns() => RunFuzz(
        new LoadParams(60, 3, 4, 5, 20),
        new FuzzParams { DuplicatesFactor = 4, ColumnReorderingFactor = 4, DiffCasesInColNames = true,
            ExerciseSymbols = true, ColumnConvertProb = 0.05 });

    [Test]
    public Task Load() => RunFuzz(
        new LoadParams(60, 3, 5, 8, 20),
        new FuzzParams());

    [Test]
    public Task LoadNoSymbols() => RunFuzz(
        new LoadParams(60, 3, 5, 8, 20),
        new FuzzParams { NonAsciiValueFactor = 5, DiffCasesInColNames = true, ExerciseSymbols = false,
            ColumnConvertProb = 0.05 });

    [Test]
    public Task NonAsciiValues() => RunFuzz(
        new LoadParams(60, 3, 4, 5, 20),
        new FuzzParams { NewColumnFactor = 3, NonAsciiValueFactor = 1, ExerciseSymbols = true });

    private async Task RunFuzz(LoadParams load, FuzzParams fuzz)
    {
        var seed = DeriveMasterSeed();
        TestContext.Progress.WriteLine($"{TestContext.CurrentContext.Test.Name} seed=0x{seed:x16}");
        var master = new Rng(seed);

        var endpoint = _questDb!.GetWebSocketEndpoint();
        var httpEndpoint = _questDb.GetHttpEndpoint();

        var tables = new Dictionary<string, TableData>(StringComparer.Ordinal);
        for (var i = 0; i < load.NumTables; i++)
        {
            var name = $"weather{i}";
            tables[name] = new TableData(name);
            await ExecAsync(httpEndpoint, $"DROP TABLE IF EXISTS '{name}'");
        }

        var timestampTicks = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        long tsCounter = 0;
        DateTime NextTs() => new(timestampTicks + Interlocked.Increment(ref tsCounter) * 10, DateTimeKind.Utc);

        var failures = new ConcurrentQueue<string>();
        var producersDone = new CancellationTokenSource();

        var producers = new List<Task>();
        for (var t = 0; t < load.NumThreads; t++)
        {
            var threadRng = master.Child();
            producers.Add(Task.Run(() =>
                ProducerLoop(endpoint, load, fuzz, threadRng, tables, NextTs, failures)));
        }

        Task? alterTask = null;
        if (fuzz.ColumnConvertProb > 0)
        {
            var budget = Math.Max(1, (int)(load.NumLines * load.NumTables * fuzz.ColumnConvertProb));
            var alterRng = master.Child();
            alterTask = Task.Run(() => AlterLoop(httpEndpoint, tables.Keys.ToArray(), budget, alterRng,
                producersDone.Token, failures));
        }

        await Task.WhenAll(producers);
        producersDone.Cancel();
        if (alterTask is not null)
        {
            await alterTask;
        }

        Assert.That(failures, Is.Empty, $"producer/alter failures: {string.Join(" | ", failures)}");

        foreach (var table in tables.Values)
        {
            var expected = table.RowCount;
            if (expected == 0)
            {
                continue;
            }

            await WaitForRowCountAsync(httpEndpoint, table.Name, expected);
            await CompareTableAsync(httpEndpoint, table, seed);
        }
    }

    private void ProducerLoop(
        string endpoint, LoadParams load, FuzzParams fuzz, Rng rng,
        Dictionary<string, TableData> tables, Func<DateTime> nextTs, ConcurrentQueue<string> failures)
    {
        try
        {
            using var sender = Sender.New($"ws::addr={endpoint};auto_flush=off;");
            var points = 0;
            for (var iter = 0; iter < load.NumIterations; iter++)
            {
                for (var line = 0; line < load.NumLines; line++)
                {
                    var tableName = PickTableName(load.NumTables, rng);
                    var table = tables[tableName];
                    var ts = nextTs();
                    var row = GenerateLine(sender, tableName, fuzz, rng, table, ts);
                    sender.At(ts);
                    table.AddRow(row);
                    if (++points % BatchSize == 0)
                    {
                        sender.SendAsync().GetAwaiter().GetResult();
                    }
                }

                sender.SendAsync().GetAwaiter().GetResult();
                if (load.WaitMs > 0)
                {
                    Thread.Sleep(load.WaitMs);
                }
            }

            sender.SendAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            failures.Enqueue($"producer failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private LineData GenerateLine(
        ISender sender, string tableName, FuzzParams fuzz, Rng rng, TableData table, DateTime ts)
    {
        var row = new LineData(ts);
        sender.Table(tableName);

        if (fuzz.ExerciseSymbols)
        {
            var symIndexes = SkipColumns(GenerateOrdering(SymbolNameBases.Length, fuzz.ColumnReorderingFactor, rng),
                fuzz.ColumnSkipFactor, rng);
            foreach (var symIndex in symIndexes)
            {
                var name = GenerateName(SymbolNameBases[symIndex], false, fuzz.DiffCasesInColNames, rng);
                var value = SymbolValueBases[symIndex] + RandomSuffix(fuzz, rng);
                sender.Symbol(name, value);
                row.Add(name, value);
                AddNewSymbol(sender, row, fuzz, rng);
            }
        }

        var colIndexes = SkipColumns(GenerateOrdering(ColNameBases.Length, fuzz.ColumnReorderingFactor, rng),
            fuzz.ColumnSkipFactor, rng);
        foreach (var colIndex in colIndexes)
        {
            var name = GenerateName(ColNameBases[colIndex], false, fuzz.DiffCasesInColNames, rng);
            table.RecordInitialType(name, ColTypes[colIndex]);
            var value = AppendColumnValue(sender, name, ColTypes[colIndex], ColValueBases[colIndex], fuzz, rng);
            row.Add(name, value);

            if (ShouldFuzz(fuzz.DuplicatesFactor, rng))
            {
                AppendColumnValue(sender, name, ColTypes[colIndex], ColValueBases[colIndex], fuzz, rng);
            }

            AddNewColumn(sender, row, table, fuzz, rng);
        }

        return row;
    }

    private void AddNewColumn(ISender sender, LineData row, TableData table, FuzzParams fuzz, Rng rng)
    {
        if (!ShouldFuzz(fuzz.NewColumnFactor, rng))
        {
            return;
        }

        var idx = rng.NextInt(ColNameBases.Length);
        var name = GenerateName(ColNameBases[idx], true, fuzz.DiffCasesInColNames, rng);
        table.RecordInitialType(name, ColTypes[idx]);
        var value = AppendColumnValue(sender, name, ColTypes[idx], ColValueBases[idx], fuzz, rng);
        row.Add(name, value);
    }

    private void AddNewSymbol(ISender sender, LineData row, FuzzParams fuzz, Rng rng)
    {
        if (!ShouldFuzz(fuzz.NewColumnFactor, rng))
        {
            return;
        }

        var idx = rng.NextInt(SymbolNameBases.Length);
        var name = GenerateName(SymbolNameBases[idx], true, fuzz.DiffCasesInColNames, rng);
        var value = SymbolValueBases[idx] + RandomSuffix(fuzz, rng);
        sender.Symbol(name, value);
        row.Add(name, value);
    }

    private object AppendColumnValue(
        ISender sender, string name, ColType type, string valueBase, FuzzParams fuzz, Rng rng)
    {
        switch (type)
        {
            case ColType.String:
            {
                var v = valueBase + RandomSuffix(fuzz, rng);
                sender.Column(name, v);
                return v;
            }
            case ColType.Double:
            {
                var v = (double)(int.Parse(valueBase, CultureInfo.InvariantCulture) * 10 + rng.NextInt(9));
                sender.Column(name, v);
                return v;
            }
            case ColType.Boolean:
            {
                var v = rng.NextBoolean();
                sender.Column(name, v);
                return v;
            }
            case ColType.Long:
            {
                var v = (long)int.Parse(valueBase, CultureInfo.InvariantCulture) * 10 + rng.NextInt(9);
                sender.Column(name, v);
                return v;
            }
            case ColType.Int:
            {
                var v = int.Parse(valueBase, CultureInfo.InvariantCulture) * 1_000_000 + rng.NextInt(1_000_000);
                sender.Column(name, v);
                return v;
            }
            case ColType.Uuid:
            {
                var v = new Guid(rng.NextBytes(16));
                sender.Column(name, v);
                return v;
            }
            default:
                throw new InvalidOperationException($"unhandled column type {type}");
        }
    }

    private string RandomSuffix(FuzzParams fuzz, Rng rng)
        => ShouldFuzz(fuzz.NonAsciiValueFactor, rng)
            ? NonAsciiChars[rng.NextInt(NonAsciiChars.Length)].ToString()
            : ((char)('A' + rng.NextInt(26))).ToString();

    private async Task AlterLoop(
        string httpEndpoint, string[] tableNames, int budget, Rng rng,
        CancellationToken producersDone, ConcurrentQueue<string> failures)
    {
        var applied = 0;
        while (applied < budget && !producersDone.IsCancellationRequested)
        {
            try
            {
                var table = tableNames[rng.NextInt(tableNames.Length)];
                var columns = await ListColumnsAsync(httpEndpoint, table);
                var candidate = columns.FirstOrDefault(c =>
                    c.Type is "STRING" or "SYMBOL" or "VARCHAR");
                if (candidate.Name is not null)
                {
                    var target = candidate.Type switch
                    {
                        "STRING" => rng.NextBoolean() ? "SYMBOL" : "VARCHAR",
                        "SYMBOL" => rng.NextBoolean() ? "STRING" : "VARCHAR",
                        _ => rng.NextBoolean() ? "STRING" : "SYMBOL",
                    };
                    await ExecAsync(httpEndpoint,
                        $"ALTER TABLE '{table}' ALTER COLUMN \"{candidate.Name}\" TYPE {target}");
                    applied++;
                }
            }
            catch
            {
            }

            try
            {
                await Task.Delay(20 + rng.NextInt(60), producersDone);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task CompareTableAsync(string httpEndpoint, TableData table, ulong seed)
    {
        var (columns, rows) = await QuerySortedAsync(httpEndpoint, table.Name);
        var expectedRows = table.RowsSortedByTimestamp();

        Assert.That(rows.Count, Is.EqualTo(expectedRows.Count),
            $"[seed=0x{seed:x16}] table {table.Name}: row count mismatch");

        var tsIndex = Array.FindIndex(columns, c => c.Name.Equals("timestamp", StringComparison.OrdinalIgnoreCase));
        Assert.That(tsIndex, Is.GreaterThanOrEqualTo(0),
            $"[seed=0x{seed:x16}] table {table.Name}: no designated timestamp column");

        string ExpectedAt(int row, int col) => expectedRows[row].TryGet(columns[col].Name, out var raw)
            ? CanonicalExpected(raw!, columns[col].Type)
            : MissingDefault(columns[col].Type);
        string ActualAt(int row, int col) => CanonicalActual(rows[row][col], columns[col].Type);

        var mismatches = new List<(int Row, int Col)>();
        for (var r = 0; r < expectedRows.Count; r++)
        {
            for (var c = 0; c < columns.Length; c++)
            {
                if (c != tsIndex && ExpectedAt(r, c) != ActualAt(r, c))
                {
                    mismatches.Add((r, c));
                }
            }
        }

        if (mismatches.Count == 0)
        {
            return;
        }

        var msg = new System.Text.StringBuilder();
        var badCols = mismatches.Select(m => m.Col).Distinct().ToArray();
        var badRows = mismatches.Select(m => m.Row).Distinct().Count();
        msg.AppendLine($"[seed=0x{seed:x16}] table {table.Name}: {mismatches.Count} cell mismatch(es) " +
                       $"over {badRows} row(s), columns: {string.Join(",", badCols.Select(c => columns[c].Name))}");
        foreach (var (r, c) in mismatches.Take(5))
        {
            msg.AppendLine($"  row {r} tsTicks={expectedRows[r].Timestamp.Ticks} " +
                           $"'{columns[c].Name}' ({columns[c].Type}): " +
                           $"expected '{ExpectedAt(r, c)}' got '{ActualAt(r, c)}'");
        }

        // Multiset compare per mismatching column: equal => row mis-pairing only;
        // unequal => a value was genuinely changed/lost in transit.
        foreach (var c in badCols)
        {
            var exp = new Dictionary<string, int>();
            var act = new Dictionary<string, int>();
            for (var r = 0; r < expectedRows.Count; r++)
            {
                exp[ExpectedAt(r, c)] = exp.GetValueOrDefault(ExpectedAt(r, c)) + 1;
                act[ActualAt(r, c)] = act.GetValueOrDefault(ActualAt(r, c)) + 1;
            }

            var equal = exp.Count == act.Count && exp.All(kv => act.GetValueOrDefault(kv.Key) == kv.Value);
            msg.AppendLine($"  column '{columns[c].Name}' multiset expected==server: {equal}");
            if (!equal)
            {
                foreach (var key in exp.Keys.Union(act.Keys).OrderBy(k => k))
                {
                    var e = exp.GetValueOrDefault(key);
                    var a = act.GetValueOrDefault(key);
                    if (e != a)
                    {
                        msg.AppendLine($"    '{key}': expected x{e}, server x{a}");
                    }
                }
            }
        }

        Assert.Fail(msg.ToString());
    }

    private static string MissingDefault(string serverType) => serverType switch
    {
        "BOOLEAN" => "false",
        _ => "<null>",
    };

    private static string CanonicalExpected(object value, string serverType)
    {
        switch (value)
        {
            case bool b:
                return b ? "true" : "false";
            case double d:
                return d.ToString("R", CultureInfo.InvariantCulture);
            case long l:
                return l.ToString(CultureInfo.InvariantCulture);
            case int i:
                return i.ToString(CultureInfo.InvariantCulture);
            case Guid g:
                return g.ToString("D").ToLowerInvariant();
            default:
                return value.ToString() ?? "<null>";
        }
    }

    private static string CanonicalActual(JsonElement cell, string serverType)
    {
        if (cell.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return serverType == "BOOLEAN" ? "false" : "<null>";
        }

        switch (serverType)
        {
            case "BOOLEAN":
                return cell.ValueKind == JsonValueKind.True ? "true"
                    : cell.ValueKind == JsonValueKind.False ? "false"
                    : cell.GetString()!.ToLowerInvariant();
            case "DOUBLE":
            case "FLOAT":
                return (cell.ValueKind == JsonValueKind.Number ? cell.GetDouble() : double.Parse(cell.GetString()!, CultureInfo.InvariantCulture))
                    .ToString("R", CultureInfo.InvariantCulture);
            case "LONG":
            case "INT":
                return cell.ValueKind == JsonValueKind.Number
                    ? cell.GetInt64().ToString(CultureInfo.InvariantCulture)
                    : cell.GetString()!;
            case "UUID":
                return cell.GetString()!.ToLowerInvariant();
            default:
                return cell.GetString() ?? "<null>";
        }
    }

    private static async Task ExecAsync(string httpEndpoint, string sql)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        using var resp = await client.GetAsync($"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}");
        resp.EnsureSuccessStatusCode();
    }

    private static async Task<(ColumnInfo[] Columns, IReadOnlyList<JsonElement[]> Rows)> QuerySortedAsync(
        string httpEndpoint, string table)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        var resp = await client.GetAsync(
            $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString($"SELECT * FROM '{table}' ORDER BY timestamp")}");
        resp.EnsureSuccessStatusCode();
        using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        var root = json.RootElement;

        var columns = root.GetProperty("columns").EnumerateArray()
            .Select(c => new ColumnInfo(c.GetProperty("name").GetString()!, c.GetProperty("type").GetString()!))
            .ToArray();

        var rows = new List<JsonElement[]>();
        if (root.TryGetProperty("dataset", out var dataset) && dataset.ValueKind == JsonValueKind.Array)
        {
            foreach (var row in dataset.EnumerateArray())
            {
                rows.Add(row.EnumerateArray().Select(e => e.Clone()).ToArray());
            }
        }

        return (columns, rows);
    }

    private static async Task<List<ColumnInfo>> ListColumnsAsync(string httpEndpoint, string table)
    {
        var (columns, _) = await QuerySortedAsync(httpEndpoint, table);
        return columns.ToList();
    }

    private static async Task WaitForRowCountAsync(string httpEndpoint, string table, int expected)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        var deadline = DateTime.UtcNow.AddSeconds(120);
        long last = -1;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var resp = await client.GetAsync(
                    $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString($"SELECT count() FROM '{table}'")}");
                if (resp.IsSuccessStatusCode)
                {
                    using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
                    if (json.RootElement.TryGetProperty("dataset", out var ds) && ds.GetArrayLength() > 0)
                    {
                        last = ds[0][0].GetInt64();
                        if (last >= expected)
                        {
                            return;
                        }
                    }
                }
            }
            catch
            {
            }

            await Task.Delay(100);
        }

        Assert.Fail($"timed out waiting for {expected} rows in {table}; last count={last}");
    }

    private static bool ShouldFuzz(int factor, Rng rng) => factor > 0 && rng.NextInt(factor) == 0;

    private static string GenerateName(string[] bases, bool randomize, bool diffCases, Rng rng)
    {
        var caseIndex = diffCases ? rng.NextInt(bases.Length) : 0;
        var suffix = randomize ? rng.NextInt(NewColumnRandomizeFactor).ToString() : string.Empty;
        return bases[caseIndex] + suffix;
    }

    private static List<int> GenerateOrdering(int count, int reorderFactor, Rng rng)
    {
        var indexes = Enumerable.Range(0, count).ToList();
        if (ShouldFuzz(reorderFactor, rng))
        {
            rng.Shuffle(indexes);
        }
        return indexes;
    }

    private static List<int> SkipColumns(List<int> indexes, int skipFactor, Rng rng)
    {
        if (!ShouldFuzz(skipFactor, rng))
        {
            return indexes;
        }

        var result = new List<int>(indexes);
        var toSkip = Math.Min(1 + rng.NextInt(MaxSkippedColumns), Math.Max(0, result.Count - 1));
        for (var i = 0; i < toSkip && result.Count > 0; i++)
        {
            result.RemoveAt(rng.NextInt(result.Count));
        }
        return result;
    }

    private static string PickTableName(int numTables, Rng rng) => $"weather{rng.NextInt(numTables)}";

    private static ulong DeriveMasterSeed()
    {
        var raw = Environment.GetEnvironmentVariable("QWP_WS_FUZZ_SEED");
        if (!string.IsNullOrWhiteSpace(raw))
        {
            raw = raw.Trim();
            return raw.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? Convert.ToUInt64(raw.Substring(2), 16)
                : ulong.Parse(raw);
        }
        return unchecked((ulong)(DateTime.UtcNow.Ticks ^ ((long)Environment.TickCount << 32)));
    }

    private sealed class Rng
    {
        private readonly Random _impl;

        public Rng(ulong seed) => _impl = new Random(unchecked((int)(seed ^ (seed >> 32))));

        public int NextInt(int bound) => _impl.Next(bound);

        public bool NextBoolean() => _impl.Next(2) == 1;

        public byte[] NextBytes(int n)
        {
            var b = new byte[n];
            _impl.NextBytes(b);
            return b;
        }

        public void Shuffle<T>(IList<T> list)
        {
            for (var i = list.Count - 1; i > 0; i--)
            {
                var j = _impl.Next(i + 1);
                (list[i], list[j]) = (list[j], list[i]);
            }
        }

        public Rng Child() => new(unchecked((ulong)_impl.NextInt64()));
    }

    private sealed class LineData
    {
        private readonly Dictionary<string, object> _values = new(StringComparer.OrdinalIgnoreCase);

        public LineData(DateTime timestamp) => Timestamp = timestamp;

        public DateTime Timestamp { get; }

        public void Add(string name, object value) => _values.TryAdd(name, value);

        public bool TryGet(string name, out object? value)
        {
            var found = _values.TryGetValue(name, out var v);
            value = v;
            return found;
        }
    }

    private sealed class TableData
    {
        private readonly object _lock = new();
        private readonly List<LineData> _rows = new();
        private readonly Dictionary<string, ColType> _initialTypes = new(StringComparer.OrdinalIgnoreCase);

        public TableData(string name) => Name = name;

        public string Name { get; }

        public int RowCount
        {
            get { lock (_lock) { return _rows.Count; } }
        }

        public void AddRow(LineData row)
        {
            lock (_lock) { _rows.Add(row); }
        }

        public void RecordInitialType(string column, ColType type)
        {
            lock (_lock) { _initialTypes.TryAdd(column, type); }
        }

        public List<LineData> RowsSortedByTimestamp()
        {
            lock (_lock)
            {
                return _rows.OrderBy(r => r.Timestamp).ToList();
            }
        }
    }

    private readonly record struct ColumnInfo(string Name, string Type);

    private sealed class FuzzParams
    {
        public int DuplicatesFactor { get; init; } = -1;
        public int ColumnReorderingFactor { get; init; } = -1;
        public int ColumnSkipFactor { get; init; } = -1;
        public int NewColumnFactor { get; init; } = -1;
        public int NonAsciiValueFactor { get; init; } = -1;
        public bool DiffCasesInColNames { get; init; }
        public bool ExerciseSymbols { get; init; } = true;
        public double ColumnConvertProb { get; init; }
    }

    private sealed record LoadParams(int NumLines, int NumIterations, int NumThreads, int NumTables, int WaitMs);
}

#endif
