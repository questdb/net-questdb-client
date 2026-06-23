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
using System.Numerics;
using System.Text;
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
    private const int MaxSkippedColumns = 2;
    private const int NewColumnRandomizeFactor = 2;
    private const int SendSymbolsWithSpaceRandomizeFactor = 2;
    private const int UppercaseTableRandomizeFactor = 2;

    // Designated timestamp seed (microseconds since epoch). Matches BASE_TIMESTAMP_US.
    private const long BaseTimestampUs = 1_465_839_830_102_300;

    // Non-designated timestamp regions: default near now, pre-epoch ~1922, far future ~2096.
    private const long DefaultTsUsBase = 1_700_000_000_000_000;
    private const long PreEpochTsUsBase = -1_500_000_000_000_000;
    private const long FarFutureTsUsBase = 4_000_000_000_000_000;
    private const long TsUsJitter = 86_400_000_000;

    private const long DefaultTsNsBase = 1_700_000_000_000_000_000;
    private const long PreEpochTsNsBase = -1_500_000_000_000_000_000;
    private const long FarFutureTsNsBase = 4_000_000_000_000_000_000;
    private const long TsNsJitter = 86_400_000_000_000;

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
        new[] { "humidity", "HUMIdity", "HumiditY", "HUmiDIty", "HUMIDITY", "Humidity" },
        new[] { "hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET" },
        new[] { "notes", "NOTES", "NotEs", "noTeS" },
        new[] { "ветер", "Ветер", "ВЕТЕР", "вЕТЕр", "ВетЕР" },
        new[] { "flag", "FLAG", "Flag", "flAG" },
        new[] { "count", "COUNT", "CounT", "Count" },
        new[] { "price", "PRICE", "pRICE", "Price" },
        new[] { "series_1d", "SERIES_1D", "sERIES_1d", "Series_1d" },
        new[] { "series_2d", "SERIES_2D", "sERIES_2d", "Series_2d" },
        new[] { "series_3d", "SERIES_3D", "sERIES_3d", "Series_3d" },
        new[] { "event_us", "EVENT_US", "event_Us", "Event_US" },
        new[] { "event_ns", "EVENT_NS", "event_Ns", "Event_NS" },
        new[] { "age", "AGE", "Age", "aGE" },
        new[] { "depth", "DEPTH", "DePth", "depTH" },
        new[] { "rank_i", "RANK_I", "rank_I", "Rank_i" },
        new[] { "amount64", "AMOUNT64", "amount_64", "Amount64" },
        new[] { "amount128", "AMOUNT128", "amount_128", "Amount128" },
        new[] { "trace_id", "TRACE_ID", "trace_Id", "TraceID" },
        new[] { "hash256", "HASH256", "hash_256", "Hash256" },
        new[] { "event_ms", "EVENT_MS", "event_Ms", "Event_MS" },
        new[] { "marker_c", "MARKER_C", "marker_C", "Marker_C" },
        new[] { "region", "REGION", "Region", "reGION" },
        new[] { "frame_pos", "FRAME_POS", "frame_Pos", "Frame_PoS" },
    };

    private enum ColType
    {
        String, Double, Boolean, Long, Decimal256,
        DoubleArray1D, DoubleArray2D, DoubleArray3D,
        TimestampMicros, TimestampNanos,
        Byte, Short, Int,
        Decimal64, Decimal128,
        Uuid, Long256,
        Date, Char,
        Geohash,
        Float,
    }

    private static readonly ColType[] ColTypes =
    {
        ColType.String, ColType.Double, ColType.Double, ColType.Double, ColType.String, ColType.Double,
        ColType.Boolean, ColType.Long, ColType.Decimal256,
        ColType.DoubleArray1D, ColType.DoubleArray2D, ColType.DoubleArray3D,
        ColType.TimestampMicros, ColType.TimestampNanos,
        ColType.Byte, ColType.Short, ColType.Int,
        ColType.Decimal64, ColType.Decimal128,
        ColType.Uuid, ColType.Long256,
        ColType.Date, ColType.Char,
        ColType.Geohash,
        ColType.Float,
    };

    private static readonly string[] ColValueBases =
    {
        "europe", "8", "2", "1", "note", "6",
        "", "4", "7",
        "", "", "",
        "", "",
        "3", "5", "9",
        "11", "13",
        "", "",
        "", "",
        "",
        "",
    };

    private static readonly string[][] SymbolNameBases =
    {
        new[] { "location", "Location", "LOCATION", "loCATion", "LocATioN" },
        new[] { "city", "ciTY", "CITY" },
    };

    private static readonly string[] SymbolValueBases = { "us-midwest", "London" };

    // EXTREME_STRINGS — copied verbatim. Soft hyphen entry contains U+00AD.
    private static readonly string[] ExtremeStrings =
    {
        "",
        "x",
        new string('X', 256),
        "🎉",
        "🎉🎉🎉🎉🎉",
        "שלום",
        "مرحبا",
        "naïve café",
        "A­B",
    };

    // EXTREME_SYMBOLS — the EXTREME_STRINGS subset without an embedded space.
    private static readonly string[] ExtremeSymbols =
        ExtremeStrings.Where(s => !s.Contains(' ')).ToArray();

    // NON_ASCII_CHARS — 10 entries incl. fullwidth space U+3000 and U+3A55.
    private static readonly string[] NonAsciiChars =
        { "ó", "í", "Á", "ч", "Ъ", "Ж", "ю", "　", "む", "㩕" };

    private static readonly string[] IntegerColumnTypes = { "BYTE", "SHORT", "INT", "LONG" };

    private static readonly string[] AlterToleratedPatterns =
    {
        "type is already",
        "designated timestamp",
        "cannot change type of column",
        "column type is fixed",
        "table does not exist",
        "no such column",
        "column does not exist",
        "invalid column",
        "unsupported conversion",
    };


    [Test]
    public Task AddColumns() => RunFuzz(r =>
    {
        var load = new LoadParams(15 + r.NextInt(100), 5 + r.NextInt(5),
            2 + r.NextInt(20), 1 + r.NextInt(4), r.NextInt(75));
        var fuzz = new FuzzParams
        {
            ColumnSkipFactor = 1, NewColumnFactor = 1 + r.NextInt(3), NonAsciiValueFactor = 6,
            ExerciseSymbols = true, ColumnConvertProb = 0.1,
        };
        return (load, fuzz);
    });

    [Test]
    public Task AddColumnsNoSymbols() => RunFuzz(_ => (
        new LoadParams(15, 2, 2, 5, 75),
        new FuzzParams
        {
            NewColumnFactor = 4, NonAsciiValueFactor = 3, DiffCasesInColNames = true,
            ExerciseSymbols = false, ColumnConvertProb = 0.15,
        }));

    [Test]
    public Task AddConvertColumns() => RunFuzz(_ => (
        new LoadParams(15, 2, 2, 5, 75),
        new FuzzParams
        {
            NewColumnFactor = 4, ExerciseSymbols = true, SendSymbolsWithSpace = true,
            ColumnConvertProb = 0.2,
        }));

    [Test]
    public Task AllMixed() => RunFuzz(_ => (
        new LoadParams(50, 5, 5, 5, 50),
        new FuzzParams
        {
            DuplicatesFactor = 3, ColumnReorderingFactor = 4, ColumnSkipFactor = 5,
            NewColumnFactor = 10, NonAsciiValueFactor = 5, DiffCasesInColNames = true,
            ExerciseSymbols = true, SendSymbolsWithSpace = true, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task AllMixedNoSymbols() => RunFuzz(_ => (
        new LoadParams(50, 5, 5, 5, 50),
        new FuzzParams
        {
            DuplicatesFactor = 3, ColumnReorderingFactor = 4, ColumnSkipFactor = 5,
            NewColumnFactor = 10, NonAsciiValueFactor = 5, DiffCasesInColNames = true,
            ExerciseSymbols = false, SendSymbolsWithSpace = true, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task AllMixedSingleTable() => RunFuzz(_ => (
        new LoadParams(50, 5, 5, 1, 50),
        new FuzzParams
        {
            DuplicatesFactor = 3, ColumnReorderingFactor = 4, ColumnSkipFactor = 5,
            NewColumnFactor = 10, NonAsciiValueFactor = 5, DiffCasesInColNames = true,
            ExerciseSymbols = true, SendSymbolsWithSpace = true, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task AllMixedSplitPart() => RunFuzz(_ => (
        new LoadParams(50, 5, 5, 1, 50),
        new FuzzParams
        {
            NonAsciiValueFactor = 10, ExerciseSymbols = true, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task CaseVariationReorderingColumns() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, NewColumnFactor = 2, DiffCasesInColNames = true,
            ExerciseSymbols = true,
        }));

    [Test]
    public Task CaseVariationReorderingColumnsNoSymbols() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, DiffCasesInColNames = true, ExerciseSymbols = false,
        }));

    [Test]
    public Task CaseVariationReorderingColumnsSendSymbolsWithSpace() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, NewColumnFactor = 3, DiffCasesInColNames = true,
            ExerciseSymbols = true, SendSymbolsWithSpace = true,
        }));

    [Test]
    public Task DuplicatesReorderingColumns() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            DuplicatesFactor = 4, ColumnReorderingFactor = 4, DiffCasesInColNames = true,
            ExerciseSymbols = true, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task DuplicatesReorderingColumnsNoSymbols() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            DuplicatesFactor = 4, ColumnReorderingFactor = 4, DiffCasesInColNames = true,
            ExerciseSymbols = false, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task DuplicatesReorderingColumnsSendSymbolsWithSpace() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            DuplicatesFactor = 4, ColumnReorderingFactor = 4, DiffCasesInColNames = true,
            ExerciseSymbols = true, SendSymbolsWithSpace = true, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task Load() => RunFuzz(_ => (
        new LoadParams(100, 5, 7, 12, 20),
        new FuzzParams()));

    [Test]
    public Task LoadLargePayload() => RunFuzz(_ => (
        new LoadParams(500, 5, 5, 5, 10),
        new FuzzParams()));

    [Test]
    public Task LoadNoSymbols() => RunFuzz(_ => (
        new LoadParams(100, 5, 7, 12, 20),
        new FuzzParams
        {
            NonAsciiValueFactor = 5, DiffCasesInColNames = true, ExerciseSymbols = false,
            ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task LoadSendSymbolsWithSpace() => RunFuzz(_ => (
        new LoadParams(100, 5, 4, 8, 20),
        new FuzzParams
        {
            NewColumnFactor = 2, ExerciseSymbols = true, SendSymbolsWithSpace = true,
        }));

    [Test]
    public Task LoadSmallBuffer() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 20),
        new FuzzParams()));

    [Test]
    public Task NonAsciiValues() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            NewColumnFactor = 3, NonAsciiValueFactor = 4, ExerciseSymbols = true,
        }));

    [Test]
    public Task NonAsciiValuesNoSymbols() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            NonAsciiValueFactor = 4, ExerciseSymbols = false,
        }));

    [Test]
    public Task ReorderingColumns() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, NonAsciiValueFactor = 8, ExerciseSymbols = true,
            SendSymbolsWithSpace = true, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task ReorderingColumnsNoSymbols() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, DiffCasesInColNames = true, ExerciseSymbols = false,
            ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task ReorderingManyThreads() => RunFuzz(r =>
    {
        var load = new LoadParams(15 + r.NextInt(100), 5 + r.NextInt(5),
            2 + r.NextInt(20), 1 + r.NextInt(4), r.NextInt(75));
        var fuzz = new FuzzParams
        {
            ColumnReorderingFactor = 3, NewColumnFactor = 1 + r.NextInt(3),
            ExerciseSymbols = true,
        };
        return (load, fuzz);
    });

    [Test]
    public Task ReorderingNonAscii() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, NewColumnFactor = 2, NonAsciiValueFactor = 4,
            ExerciseSymbols = true,
        }));

    [Test]
    public Task ReorderingSkipColumnsWithNonAscii() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, ColumnSkipFactor = 4, NewColumnFactor = 2,
            NonAsciiValueFactor = 4, DiffCasesInColNames = true, ExerciseSymbols = true,
        }));

    [Test]
    public Task ReorderingSkipColumnsWithNonAsciiNoSymbols() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, ColumnSkipFactor = 4, NonAsciiValueFactor = 4,
            DiffCasesInColNames = true, ExerciseSymbols = false,
        }));

    [Test]
    public Task ReorderingSkipDuplicateColumnsWithNonAscii() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            DuplicatesFactor = 4, ColumnReorderingFactor = 4, ColumnSkipFactor = 4,
            NonAsciiValueFactor = 4, DiffCasesInColNames = true, ExerciseSymbols = true,
            ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task ReorderingSkipDuplicateColumnsWithNonAsciiNoSymbols() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 50),
        new FuzzParams
        {
            DuplicatesFactor = 4, ColumnReorderingFactor = 4, ColumnSkipFactor = 4,
            NonAsciiValueFactor = 4, DiffCasesInColNames = true, ExerciseSymbols = false,
            ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task LoadWithBounce() => RunFuzz(_ => (
        new LoadParams(100, 5, 5, 5, 400),
        new FuzzParams { MaxBounces = 3, MinBounceIntervalMs = 300, MaxBounceIntervalMs = 1500 }));

    [Test]
    public Task AllMixedWithBounce() => RunFuzz(_ => (
        new LoadParams(50, 5, 5, 5, 400),
        new FuzzParams
        {
            DuplicatesFactor = 3, ColumnReorderingFactor = 4, ColumnSkipFactor = 5,
            NewColumnFactor = 10, NonAsciiValueFactor = 5, DiffCasesInColNames = true,
            ExerciseSymbols = true, SendSymbolsWithSpace = true, ColumnConvertProb = 0.05,
            MaxBounces = 3, MinBounceIntervalMs = 300, MaxBounceIntervalMs = 1500,
        }));

    [Test]
    public Task ExtremeStringsTest() => RunFuzz(_ => (
        new LoadParams(80, 5, 4, 3, 30),
        new FuzzParams
        {
            ColumnReorderingFactor = 3, NewColumnFactor = 4, NonAsciiValueFactor = 3,
            DiffCasesInColNames = true, ExtremeStringFactor = 2,
        }));

    [Test]
    public Task ExtremeNumerics() => RunFuzz(_ => (
        new LoadParams(80, 5, 4, 3, 30),
        new FuzzParams
        {
            ColumnReorderingFactor = 3, NewColumnFactor = 4, DiffCasesInColNames = true,
            ExtremeNumericFactor = 2, NegativeZeroFactor = 4,
        }));

    [Test]
    public Task ExtremeTimestamps() => RunFuzz(_ => (
        new LoadParams(80, 5, 4, 3, 30),
        new FuzzParams
        {
            ColumnReorderingFactor = 3, ExtremeTimestampFactor = 2,
        }));

    [Test]
    public Task ExtremeEverything() => RunFuzz(_ => (
        new LoadParams(80, 5, 4, 3, 30),
        new FuzzParams
        {
            DuplicatesFactor = 4, ColumnReorderingFactor = 3, ColumnSkipFactor = 5,
            NewColumnFactor = 4, NonAsciiValueFactor = 3, DiffCasesInColNames = true,
            ExerciseSymbols = true, ExtremeStringFactor = 3, ExtremeNumericFactor = 3,
            ExtremeTimestampFactor = 3, NegativeZeroFactor = 4, ColumnConvertProb = 0.05,
        }));

    [Test]
    public Task NBounceSweep() => RunFuzz(_ => (
        new LoadParams(100, 10, 5, 5, 400),
        new FuzzParams
        {
            ColumnReorderingFactor = 4, NewColumnFactor = 5, NonAsciiValueFactor = 4,
            DiffCasesInColNames = true, ExerciseSymbols = true,
            MaxBounces = 10, MinBounceIntervalMs = 300, MaxBounceIntervalMs = 1200,
        }));


    private async Task RunFuzz(Func<Rng, (LoadParams, FuzzParams)> builder)
    {
        var seed = DeriveMasterSeed();
        TestContext.Progress.WriteLine($"{TestContext.CurrentContext.Test.Name} seed=0x{seed:x16}");
        var master = new Rng(seed);
        var (load, fuzz) = builder(master);

        var httpEndpoint = _questDb!.GetHttpEndpoint();
        var endpoint = _questDb.GetWebSocketEndpoint();

        var tables = new Dictionary<string, TableData>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < load.NumTables; i++)
        {
            var name = $"weather{i}";
            tables[name] = new TableData(name);
            await ExecAsync(httpEndpoint, $"DROP TABLE IF EXISTS '{name}'");
        }

        long tsCounter = BaseTimestampUs;
        long NextTsUs() => Interlocked.Increment(ref tsCounter);

        var failures = new ConcurrentQueue<string>();
        var producersDone = new CancellationTokenSource();

        if (fuzz.MaxBounces > 0 && _questDb.UseLiveServer)
        {
            Assert.Ignore("bounce fuzz tests require a managed QuestDB fixture");
        }

        // All producers run in SF mode so they survive a server bounce and replay queued frames.
        var sfRoot = Path.Combine(Path.GetTempPath(), "qwp-ws-fuzz-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(sfRoot);
        var runId = Guid.NewGuid().ToString("N")[..8];

        try
        {
            var producers = new List<Task>();
            for (var t = 0; t < load.NumThreads; t++)
            {
                var threadRng = master.Child();
                var sfDir = Path.Combine(sfRoot, $"producer-{t}");
                Directory.CreateDirectory(sfDir);
                var senderId = $"fuzz-{runId}-t{t}";
                producers.Add(Task.Run(() =>
                    ProducerLoop(endpoint, sfDir, senderId, load, fuzz, threadRng, tables, NextTsUs, failures)));
            }

            Task? alterTask = null;
            if (fuzz.ColumnConvertProb > 0)
            {
                var budget = Math.Max(1, (int)(load.NumLines * load.NumTables * fuzz.ColumnConvertProb));
                var alterRng = master.Child();
                alterTask = Task.Run(() => AlterLoop(httpEndpoint, tables.Keys.ToArray(), budget, alterRng,
                    producersDone.Token, failures));
            }

            Task? bounceTask = null;
            if (fuzz.MaxBounces > 0)
            {
                var bounceRng = master.Child();
                bounceTask = Task.Run(() => BounceLoop(fuzz, bounceRng, producersDone.Token, failures));
            }

            await Task.WhenAll(producers);
            producersDone.Cancel();

            if (alterTask is not null)
            {
                await alterTask;
            }

            if (bounceTask is not null)
            {
                await bounceTask;
            }

            Assert.That(failures, Is.Empty, $"producer/alter/bounce failures: {string.Join(" | ", failures)}");

            foreach (var table in tables.Values)
            {
                var expected = table.RowCount;
                if (expected == 0)
                {
                    continue;
                }

                await WaitForRowCountAsync(httpEndpoint, table.Name, expected);
            }

            foreach (var table in tables.Values)
            {
                if (table.RowCount == 0)
                {
                    continue;
                }

                await CompareTableAsync(httpEndpoint, table, seed);
            }
        }
        finally
        {
            TryDeleteDirectory(sfRoot);
            foreach (var name in tables.Keys)
            {
                try
                {
                    await ExecAsync(httpEndpoint, $"DROP TABLE IF EXISTS '{name}'");
                }
                catch
                {
                }
            }
        }
    }

    private void ProducerLoop(
        string endpoint, string sfDir, string senderId, LoadParams load, FuzzParams fuzz, Rng rng,
        Dictionary<string, TableData> tables, Func<long> nextTsUs, ConcurrentQueue<string> failures)
    {
        try
        {
            using var sender = (IQwpWebSocketSender)Sender.New(BuildProducerConnString(endpoint, sfDir, senderId));
            var points = 0;
            for (var iter = 0; iter < load.NumIterations; iter++)
            {
                for (var line = 0; line < load.NumLines; line++)
                {
                    var tableName = PickTableName(load.NumTables, rng);
                    var table = tables[tableName];
                    var tsUs = nextTsUs();
                    var row = GenerateLine(sender, tableName, fuzz, rng, table, tsUs);
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
            failures.Enqueue($"producer {senderId} failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private LineData GenerateLine(
        IQwpWebSocketSender sender, string tableName, FuzzParams fuzz, Rng rng, TableData table, long tsUs)
    {
        var row = new LineData(tsUs);
        sender.Table(tableName);

        if (fuzz.ExerciseSymbols)
        {
            var symIndexes = SkipColumns(
                GenerateOrdering(SymbolNameBases.Length, fuzz.ColumnReorderingFactor, rng),
                fuzz.ColumnSkipFactor, rng);
            foreach (var symIndex in symIndexes)
            {
                var symName = AddSymbol(sender, row, symIndex, fuzz, rng);
                AddDuplicateSymbol(sender, row, symIndex, symName, fuzz, rng);
                AddNewSymbol(sender, row, fuzz, rng);
            }
        }

        var colIndexes = SkipColumns(
            GenerateOrdering(ColNameBases.Length, fuzz.ColumnReorderingFactor, rng),
            fuzz.ColumnSkipFactor, rng);
        foreach (var colIndex in colIndexes)
        {
            var colName = AddColumn(sender, row, colIndex, fuzz, rng, table);
            AddDuplicateColumn(sender, row, colIndex, colName, fuzz, rng, table);
            AddNewColumn(sender, row, fuzz, rng, table);
        }

        sender.At(tsUs);
        return row;
    }

    private string AddColumn(IQwpWebSocketSender sender, LineData row, int colIndex,
        FuzzParams fuzz, Rng rng, TableData table)
    {
        var name = GenerateName(ColNameBases[colIndex], false, fuzz.DiffCasesInColNames, rng);
        table.RecordInitialType(name, ColTypes[colIndex]);
        var value = AppendColumnValue(sender, ColTypes[colIndex], ColValueBases[colIndex], name, fuzz, rng);
        row.Add(name, value);
        return name;
    }

    private void AddDuplicateColumn(IQwpWebSocketSender sender, LineData row, int colIndex,
        string colName, FuzzParams fuzz, Rng rng, TableData table)
    {
        if (!ShouldFuzz(fuzz.DuplicatesFactor, rng))
        {
            return;
        }

        table.RecordInitialType(colName, ColTypes[colIndex]);
        var value = AppendColumnValue(sender, ColTypes[colIndex], ColValueBases[colIndex], colName, fuzz, rng);
        row.Add(colName, value);
    }

    private void AddNewColumn(IQwpWebSocketSender sender, LineData row, FuzzParams fuzz, Rng rng, TableData table)
    {
        if (!ShouldFuzz(fuzz.NewColumnFactor, rng))
        {
            return;
        }

        var idx = rng.NextInt(ColNameBases.Length);
        var name = GenerateName(ColNameBases[idx], true, fuzz.DiffCasesInColNames, rng);
        table.RecordInitialType(name, ColTypes[idx]);
        var value = AppendColumnValue(sender, ColTypes[idx], ColValueBases[idx], name, fuzz, rng);
        row.Add(name, value);
    }

    private string AddSymbol(IQwpWebSocketSender sender, LineData row, int symIndex, FuzzParams fuzz, Rng rng)
    {
        var name = GenerateName(SymbolNameBases[symIndex], false, fuzz.DiffCasesInColNames, rng);
        var value = AppendSymbolValue(sender, SymbolValueBases[symIndex], name, fuzz, rng);
        row.Add(name, value);
        return name;
    }

    private void AddDuplicateSymbol(IQwpWebSocketSender sender, LineData row, int symIndex,
        string symName, FuzzParams fuzz, Rng rng)
    {
        if (!ShouldFuzz(fuzz.DuplicatesFactor, rng))
        {
            return;
        }

        var value = AppendSymbolValue(sender, SymbolValueBases[symIndex], symName, fuzz, rng);
        row.Add(symName, value);
    }

    private void AddNewSymbol(IQwpWebSocketSender sender, LineData row, FuzzParams fuzz, Rng rng)
    {
        if (!ShouldFuzz(fuzz.NewColumnFactor, rng))
        {
            return;
        }

        var idx = rng.NextInt(SymbolNameBases.Length);
        var name = GenerateName(SymbolNameBases[idx], true, fuzz.DiffCasesInColNames, rng);
        var value = AppendSymbolValue(sender, SymbolValueBases[idx], name, fuzz, rng);
        row.Add(name, value);
    }


    private string AppendSymbolValue(IQwpWebSocketSender sender, string valueBase, string colName,
        FuzzParams fuzz, Rng rng)
    {
        string symVal;
        if (ShouldFuzz(fuzz.ExtremeStringFactor, rng))
        {
            symVal = ExtremeSymbols[rng.NextInt(ExtremeSymbols.Length)];
        }
        else
        {
            var postfix = ShouldFuzz(fuzz.NonAsciiValueFactor, rng)
                ? NonAsciiChars[rng.NextInt(NonAsciiChars.Length)]
                : ((char)('A' + rng.NextInt(26))).ToString();
            var baseStr = valueBase;
            if (fuzz.SendSymbolsWithSpace && rng.NextInt(SendSymbolsWithSpaceRandomizeFactor) == 0
                && baseStr.Length > 1)
            {
                var spaceIndex = rng.NextInt(baseStr.Length - 1);
                baseStr = baseStr[..spaceIndex] + "  " + baseStr[spaceIndex..];
            }

            symVal = baseStr + postfix;
        }

        sender.Symbol(colName, symVal);
        return symVal;
    }

    private object AppendColumnValue(IQwpWebSocketSender sender, ColType type, string valueBase,
        string colName, FuzzParams fuzz, Rng rng)
    {
        switch (type)
        {
            case ColType.Double:
            {
                var d = rng.NextInt(9);
                var fVal = (double)(long.Parse(valueBase, CultureInfo.InvariantCulture) * 10 + d);
                if (ShouldFuzz(fuzz.NegativeZeroFactor, rng))
                {
                    fVal = -0.0;
                }
                else if (ShouldFuzz(fuzz.ExtremeNumericFactor, rng) && rng.NextBoolean())
                {
                    fVal = -fVal;
                }

                sender.Column(colName, fVal);
                return FormatFloat(fVal);
            }
            case ColType.String:
            {
                string strVal;
                if (ShouldFuzz(fuzz.ExtremeStringFactor, rng))
                {
                    strVal = ExtremeStrings[rng.NextInt(ExtremeStrings.Length)];
                }
                else
                {
                    var postfix = ShouldFuzz(fuzz.NonAsciiValueFactor, rng)
                        ? NonAsciiChars[rng.NextInt(NonAsciiChars.Length)]
                        : ((char)('A' + rng.NextInt(26))).ToString();
                    strVal = valueBase + postfix;
                }

                sender.Column(colName, strVal);
                return strVal;
            }
            case ColType.Boolean:
            {
                var bVal = rng.NextBoolean();
                sender.Column(colName, bVal);
                return bVal;
            }
            case ColType.Long:
            {
                var d = rng.NextInt(9);
                var iVal = long.Parse(valueBase, CultureInfo.InvariantCulture) * 10 + d;
                if (ShouldFuzz(fuzz.ExtremeNumericFactor, rng))
                {
                    long[] picks =
                    {
                        0, -1, 1, -1L << 30, (1L << 30) - 1, -1L << 60, (1L << 60) - 1,
                    };
                    iVal = picks[rng.NextInt(picks.Length)];
                }

                sender.Column(colName, iVal);
                return iVal;
            }
            case ColType.Byte:
            {
                var iVal = (int)((long.Parse(valueBase, CultureInfo.InvariantCulture) * 10 + rng.NextInt(9)) % 100);
                sender.ColumnByte(colName, (sbyte)iVal);
                return iVal;
            }
            case ColType.Short:
            {
                var iVal = (int)(long.Parse(valueBase, CultureInfo.InvariantCulture) * 1000 + rng.NextInt(900));
                sender.ColumnShort(colName, (short)iVal);
                return iVal;
            }
            case ColType.Int:
            {
                var iVal = (int)(long.Parse(valueBase, CultureInfo.InvariantCulture) * 1_000_000 + rng.NextInt(1_000_000));
                sender.Column(colName, iVal);
                return iVal;
            }
            case ColType.Decimal64:
            {
                var whole = long.Parse(valueBase, CultureInfo.InvariantCulture) * 10 + rng.NextInt(9);
                var frac = rng.NextInt(100);
                var decStr = $"{whole}.{frac:D2}";
                sender.ColumnDecimal64(colName, decimal.Parse(decStr, CultureInfo.InvariantCulture), 2);
                return decStr;
            }
            case ColType.Decimal128:
            {
                var whole = long.Parse(valueBase, CultureInfo.InvariantCulture) * 1000 + rng.NextInt(1000);
                var frac = rng.NextInt(100);
                var decStr = $"{whole}.{frac:D2}";
                sender.Column(colName, decimal.Parse(decStr, CultureInfo.InvariantCulture));
                return decStr;
            }
            case ColType.Decimal256:
            {
                var whole = BigInteger.Parse(valueBase, CultureInfo.InvariantCulture) * 10 + rng.NextInt(9);
                var frac = rng.NextInt(100);
                var negative = false;
                if (ShouldFuzz(fuzz.ExtremeNumericFactor, rng))
                {
                    var shape = rng.NextInt(5);
                    if (shape == 0)
                    {
                        whole = 0;
                        frac = 0;
                    }
                    else if (shape == 1)
                    {
                        negative = true;
                    }
                    else if (shape == 2)
                    {
                        whole = BigInteger.Pow(10, 20 + rng.NextInt(40)) + whole;
                    }
                    else if (shape == 3)
                    {
                        negative = true;
                        whole = BigInteger.Pow(10, 20 + rng.NextInt(40)) + whole;
                    }
                }

                var decStr = $"{(negative ? "-" : "")}{whole}.{frac:D2}";
                AppendDecimal256(sender, colName, whole, frac, negative);
                return decStr;
            }
            case ColType.Uuid:
            {
                var lo = rng.NextLong();
                var hi = rng.NextLong();
                sender.Column(colName, UuidFromLoHi(lo, hi));
                return FormatUuid(lo, hi);
            }
            case ColType.Long256:
            {
                var raw = new byte[32];
                for (var i = 0; i < raw.Length; i++)
                {
                    raw[i] = (byte)rng.NextInt(256);
                }

                // Non-negative BigInteger from the 32 random bytes (little-endian, unsigned).
                var leUnsigned = new byte[33];
                Array.Copy(raw, leUnsigned, 32);
                var big = new BigInteger(leUnsigned);
                sender.ColumnLong256(colName, big);
                return FormatLong256(raw);
            }
            case ColType.Date:
            {
                var ms = 1_700_000_000_000L + rng.NextInt(86_400_000);
                sender.ColumnDate(colName, ms);
                return ms;
            }
            case ColType.Char:
            {
                var v = rng.NextInt(0x110000);
                if (v < 0x20 || (v >= 0x7F && v <= 0x9F) || (v >= 0xD800 && v <= 0xDFFF) || v >= 0x10000)
                {
                    v = 'A' + rng.NextInt(26);
                }

                sender.Column(colName, (char)v);
                return char.ConvertFromUtf32(v);
            }
            case ColType.Geohash:
            {
                const int precisionBits = 25;
                var mask = (1UL << precisionBits) - 1;
                var bits = (ulong)rng.NextLong() & mask;
                sender.ColumnGeohash(colName, bits, precisionBits);
                return $"{bits:x8}/{precisionBits}";
            }
            case ColType.Float:
            {
                var whole = rng.NextInt(1000);
                var frac = rng.NextInt(1000);
                var fVal = whole + frac / 1000.0f;
                sender.ColumnFloat(colName, fVal);
                return FormatFloat((float)fVal);
            }
            case ColType.TimestampMicros:
            {
                var tsUs = PickTimestampUs(fuzz, rng);
                sender.Column(colName, DateTime.SpecifyKind(DateTime.UnixEpoch.AddTicks(tsUs * 10), DateTimeKind.Utc));
                return tsUs * 1000L;
            }
            case ColType.TimestampNanos:
            {
                var tsNs = PickTimestampNs(fuzz, rng);
                sender.ColumnNanos(colName, tsNs);
                return tsNs;
            }
            case ColType.DoubleArray1D:
            {
                var length = 2 + rng.NextInt(8);
                var arr = new double[length];
                for (var i = 0; i < length; i++)
                {
                    var v = (double)rng.NextInt(100);
                    if (ShouldFuzz(fuzz.NegativeZeroFactor, rng))
                    {
                        v = -0.0;
                    }
                    else if (ShouldFuzz(fuzz.ExtremeNumericFactor, rng) && rng.NextBoolean())
                    {
                        v = -v;
                    }

                    arr[i] = v;
                }

                sender.Column(colName, arr);
                return arr;
            }
            case ColType.DoubleArray2D:
            {
                var rows = 2 + rng.NextInt(2);
                var cols = 2 + rng.NextInt(2);
                var arr = new double[rows, cols];
                for (var i = 0; i < rows; i++)
                {
                    for (var j = 0; j < cols; j++)
                    {
                        arr[i, j] = rng.NextInt(100);
                    }
                }

                sender.Column(colName, arr);
                return arr;
            }
            case ColType.DoubleArray3D:
            {
                var d1 = 2 + rng.NextInt(2);
                var d2 = 2 + rng.NextInt(2);
                var d3 = 2 + rng.NextInt(2);
                var arr = new double[d1, d2, d3];
                for (var i = 0; i < d1; i++)
                {
                    for (var j = 0; j < d2; j++)
                    {
                        for (var k = 0; k < d3; k++)
                        {
                            arr[i, j, k] = rng.NextInt(100);
                        }
                    }
                }

                sender.Column(colName, arr);
                return arr;
            }
            default:
                sender.Column(colName, valueBase);
                return valueBase;
        }
    }

    // Always the four-limb path: it pins scale=2 explicitly so every row of the column agrees.
    // The decimal overload derives scale from the value and yields scale 0/1 on trailing zeros.
    private static void AppendDecimal256(IQwpWebSocketSender sender, string colName,
        BigInteger whole, int frac, bool negative)
    {
        var unscaled = whole * 100 + frac;
        if (negative)
        {
            unscaled = -unscaled;
        }

        var twos = unscaled.Sign < 0 ? unscaled + (BigInteger.One << 256) : unscaled;
        const ulong limbMask = ulong.MaxValue;
        var l0 = (long)(ulong)(twos & limbMask);
        var l1 = (long)(ulong)((twos >> 64) & limbMask);
        var l2 = (long)(ulong)((twos >> 128) & limbMask);
        var l3 = (long)(ulong)((twos >> 192) & limbMask);
        sender.ColumnDecimal256(colName, l0, l1, l2, l3, 2);
    }


    private async Task AlterLoop(
        string httpEndpoint, string[] tableNames, int budget, Rng rng,
        CancellationToken producersDone, ConcurrentQueue<string> failures)
    {
        var remaining = budget;
        while (remaining > 0 && !producersDone.IsCancellationRequested && failures.IsEmpty)
        {
            var table = tableNames[rng.NextInt(tableNames.Length)];
            if (await TryOneAlterAsync(httpEndpoint, table, rng, failures))
            {
                remaining--;
            }

            try
            {
                await Task.Delay(10 + rng.NextInt(100), producersDone);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task<bool> TryOneAlterAsync(
        string httpEndpoint, string table, Rng rng, ConcurrentQueue<string> failures)
    {
        List<ColumnInfo> cols;
        try
        {
            cols = await ListColumnsAsync(httpEndpoint, table);
        }
        catch (Exception e)
        {
            if (IsTransientNetworkError(e))
            {
                return false;
            }

            return false;
        }

        if (cols.Count == 0)
        {
            return false;
        }

        var start = rng.NextInt(cols.Count);
        for (var offset = 0; offset < cols.Count; offset++)
        {
            var col = cols[(start + offset) % cols.Count];
            if (col.Type == "TIMESTAMP")
            {
                continue;
            }

            var target = ChangeColumnTypeTo(col.Type, rng);
            if (target == col.Type)
            {
                continue;
            }

            try
            {
                await ExecAsync(httpEndpoint,
                    $"ALTER TABLE \"{table}\" ALTER COLUMN \"{col.Name}\" TYPE {target}");
                return true;
            }
            catch (Exception e)
            {
                var classification = ClassifyAlterError(e);
                if (classification == AlterErrorClass.Transient)
                {
                    return false;
                }

                if (classification == AlterErrorClass.Tolerated)
                {
                    continue;
                }

                failures.Enqueue($"fuzz alter: unexpected failure on {table}.{col.Name} -> {target}: " +
                                 $"{e.GetType().Name}: {e.Message}");
                return false;
            }
        }

        return false;
    }

    private static string ChangeColumnTypeTo(string colType, Rng rng)
    {
        switch (colType)
        {
            case "STRING":
                return rng.NextBoolean() ? "SYMBOL" : "VARCHAR";
            case "SYMBOL":
                return rng.NextBoolean() ? "STRING" : "VARCHAR";
            case "VARCHAR":
                return rng.NextBoolean() ? "STRING" : "SYMBOL";
            case "FLOAT":
                return "DOUBLE";
            case "DOUBLE":
                return "FLOAT";
        }

        var rank = Array.IndexOf(IntegerColumnTypes, colType);
        if (rank >= 0)
        {
            var wider = IntegerColumnTypes.Where((_, i) => i > rank).ToArray();
            if (wider.Length == 0)
            {
                return colType;
            }

            return wider[rng.NextInt(wider.Length)];
        }

        return colType;
    }

    internal enum AlterErrorClass
    {
        Transient,
        Tolerated,
        Fatal,
    }

    internal static AlterErrorClass ClassifyAlterError(Exception e)
    {
        if (IsTransientNetworkError(e))
        {
            return AlterErrorClass.Transient;
        }

        var message = e.Message.ToLowerInvariant();
        return AlterToleratedPatterns.Any(p => message.Contains(p))
            ? AlterErrorClass.Tolerated
            : AlterErrorClass.Fatal;
    }

    internal static bool IsTransientNetworkError(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException!)
        {
            if (e is HttpRequestException or TaskCanceledException or System.Net.Sockets.SocketException
                or TimeoutException or System.IO.IOException)
            {
                return true;
            }

            var message = e.Message.ToLowerInvariant();
            if (message.Contains("connection reset") || message.Contains("connection refused")
                || message.Contains("connection aborted") || message.Contains("broken pipe")
                || message.Contains("timed out") || message.Contains("timeout"))
            {
                return true;
            }

            if (e.InnerException is null)
            {
                break;
            }
        }

        return false;
    }


    private async Task BounceLoop(
        FuzzParams fuzz, Rng rng, CancellationToken producersDone, ConcurrentQueue<string> failures)
    {
        var performed = 0;
        while (performed < fuzz.MaxBounces && !producersDone.IsCancellationRequested && failures.IsEmpty)
        {
            var span = Math.Max(1, fuzz.MaxBounceIntervalMs - fuzz.MinBounceIntervalMs);
            var interval = fuzz.MinBounceIntervalMs + rng.NextInt(span);
            try
            {
                await Task.Delay(interval, producersDone);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                await _questDb!.StopGracefulAsync();
                await Task.Delay(20 + rng.NextInt(200));
                await _questDb.StartAsync();
                performed++;
            }
            catch (Exception ex)
            {
                failures.Enqueue($"bounce #{performed + 1} failed: {ex.GetType().Name}: {ex.Message}");
                try
                {
                    await _questDb!.StartAsync();
                }
                catch
                {
                }

                return;
            }
        }
    }

    // SF mode + long reconnect/close-drain budgets let producers survive a server bounce.
    private static string BuildProducerConnString(string endpoint, string sfDir, string senderId)
        => $"ws::addr={endpoint};auto_flush=off;sf_dir={sfDir};sender_id={senderId};" +
           "reconnect_max_duration_millis=120000;close_flush_timeout_millis=120000;";

    private static void TryDeleteDirectory(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
        catch
        {
        }
    }


    private async Task CompareTableAsync(string httpEndpoint, TableData table, ulong seed)
    {
        var seedLabel = $"seed=0x{seed:x16}";
        var (columns, rows) = await QuerySortedAsync(httpEndpoint, table.Name);
        var expectedRows = table.RowsSortedByTimestamp();

        Assert.That(rows.Count, Is.EqualTo(expectedRows.Count),
            $"[{seedLabel}] table {table.Name}: row count mismatch — " +
            $"expected {expectedRows.Count}, server returned {rows.Count}");

        var hasDesignatedTs = columns.Any(c =>
            c.Name.Equals("timestamp", StringComparison.OrdinalIgnoreCase) && IsTimestampType(c.Type));
        Assert.That(hasDesignatedTs, Is.True,
            $"[{seedLabel}] table {table.Name}: no designated TIMESTAMP column found");

        for (var rowIndex = 0; rowIndex < expectedRows.Count; rowIndex++)
        {
            var expectedLine = expectedRows[rowIndex];
            var serverRow = rows[rowIndex];
            for (var colIndex = 0; colIndex < columns.Length; colIndex++)
            {
                var col = columns[colIndex];
                if (col.Name.Equals("timestamp", StringComparison.OrdinalIgnoreCase) && IsTimestampType(col.Type))
                {
                    continue;
                }

                var hasExpected = expectedLine.TryGet(col.Name, out var expectedRaw);
                var actualStr = FormatActualCell(serverRow[colIndex], col.Type);
                string expectedStr;
                if (!hasExpected)
                {
                    var initialType = table.InitialColType(col.Name) ?? ServerTypeToColType(col.Type);
                    expectedStr = MissingDefault(initialType);
                    if (initialType is ColType.Byte or ColType.Short
                        && ServerTypeToColType(col.Type) != initialType
                        && actualStr == MissingDefault(ServerTypeToColType(col.Type)))
                    {
                        continue;
                    }
                }
                else
                {
                    expectedStr = FormatExpectedCell(expectedRaw!, col.Type);
                }

                if (expectedStr != actualStr)
                {
                    Assert.Fail($"[{seedLabel}] table {table.Name} row {rowIndex} " +
                                $"column {col.Name} ({col.Type}): " +
                                $"expected '{expectedStr}', got '{actualStr}' " +
                                $"(timestamp_us={expectedLine.TimestampUs})");
                }
            }
        }
    }


    private static bool IsTimestampType(string serverType)
        => serverType == "TIMESTAMP" || serverType.StartsWith("TIMESTAMP_");

    private static bool IsArrayType(string serverType)
        => serverType.Contains('[') || serverType.StartsWith("ARRAY", StringComparison.Ordinal);

    private static bool IsDecimalType(string serverType) => serverType.StartsWith("DECIMAL");

    private static bool IsGeohashType(string serverType)
        => serverType.StartsWith("GEOHASH(") && serverType.EndsWith(")");

    private static readonly string[] NumericFloatTypes = { "FLOAT", "DOUBLE" };
    private static readonly string[] NumericIntTypes = { "BYTE", "SHORT", "INT", "LONG" };

    private static string MissingDefault(string serverType)
    {
        if (NumericFloatTypes.Contains(serverType) || IsArrayType(serverType))
        {
            return "null";
        }

        if (serverType is "BYTE" or "SHORT")
        {
            return "0";
        }

        if (serverType == "BOOLEAN")
        {
            return "false";
        }

        return "";
    }

    private static string MissingDefault(ColType colType) => colType switch
    {
        ColType.Float or ColType.Double => "null",
        ColType.DoubleArray1D or ColType.DoubleArray2D or ColType.DoubleArray3D => "null",
        ColType.Byte or ColType.Short => "0",
        ColType.Boolean => "false",
        _ => "",
    };

    private static ColType ServerTypeToColType(string serverType)
    {
        if (IsArrayType(serverType))
        {
            return ColType.DoubleArray1D;
        }

        return serverType switch
        {
            "BYTE" => ColType.Byte,
            "SHORT" => ColType.Short,
            "INT" => ColType.Int,
            "LONG" => ColType.Long,
            "FLOAT" => ColType.Float,
            "DOUBLE" => ColType.Double,
            "BOOLEAN" => ColType.Boolean,
            "CHAR" => ColType.Char,
            "UUID" => ColType.Uuid,
            "LONG256" => ColType.Long256,
            "DATE" => ColType.Date,
            _ => ColType.String,
        };
    }

    private static string FormatExpectedCell(object value, string serverType)
    {
        if (serverType == "FLOAT")
        {
            var f = value switch
            {
                float fv => fv,
                double dv => (float)dv,
                _ => float.Parse(value.ToString() ?? "0", CultureInfo.InvariantCulture),
            };
            return FormatFloat32(f);
        }

        if (NumericFloatTypes.Contains(serverType))
        {
            if (value is double d)
            {
                return FormatFloat(d);
            }

            if (value is float ff)
            {
                return FormatFloat((double)ff);
            }

            return value.ToString() ?? "";
        }

        if (NumericIntTypes.Contains(serverType))
        {
            return value switch
            {
                bool b => b ? "1" : "0",
                long l => l.ToString(CultureInfo.InvariantCulture),
                int i => i.ToString(CultureInfo.InvariantCulture),
                _ => value.ToString() ?? "",
            };
        }

        if (serverType == "BOOLEAN")
        {
            if (value is string s)
            {
                return s.ToLowerInvariant();
            }

            return value is true ? "true" : "false";
        }

        if (IsTimestampType(serverType))
        {
            return value switch
            {
                long l => l.ToString(CultureInfo.InvariantCulture),
                string s => IsoToNanos(s).ToString(CultureInfo.InvariantCulture),
                _ => value.ToString() ?? "",
            };
        }

        if (serverType == "DATE")
        {
            return value switch
            {
                long l => l.ToString(CultureInfo.InvariantCulture),
                string s => (IsoToNanos(s) / 1_000_000).ToString(CultureInfo.InvariantCulture),
                _ => value.ToString() ?? "",
            };
        }

        if (IsGeohashType(serverType))
        {
            return FormatGeohashExpected(value, serverType);
        }

        if (IsDecimalType(serverType))
        {
            return NormalizeDecimal(value);
        }

        if (IsArrayType(serverType))
        {
            return FormatArray(value);
        }

        return value.ToString() ?? "";
    }

    private static string FormatActualCell(JsonElement value, string serverType)
    {
        if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return MissingDefault(serverType);
        }

        if (serverType == "FLOAT")
        {
            var fv = value.ValueKind == JsonValueKind.Number
                ? value.GetDouble()
                : double.Parse(value.GetString()!, CultureInfo.InvariantCulture);
            return FormatFloat32(fv);
        }

        if (NumericFloatTypes.Contains(serverType))
        {
            var d = value.ValueKind == JsonValueKind.Number
                ? value.GetDouble()
                : double.Parse(value.GetString()!, CultureInfo.InvariantCulture);
            return FormatFloat(d);
        }

        if (NumericIntTypes.Contains(serverType))
        {
            return value.ValueKind == JsonValueKind.Number
                ? value.GetInt64().ToString(CultureInfo.InvariantCulture)
                : value.GetString()!;
        }

        if (serverType == "BOOLEAN")
        {
            return value.ValueKind switch
            {
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => value.GetString()!.ToLowerInvariant(),
            };
        }

        if (IsTimestampType(serverType))
        {
            return value.ValueKind == JsonValueKind.Number
                ? value.GetInt64().ToString(CultureInfo.InvariantCulture)
                : IsoToNanos(value.GetString()!).ToString(CultureInfo.InvariantCulture);
        }

        if (serverType == "DATE")
        {
            return value.ValueKind == JsonValueKind.Number
                ? value.GetInt64().ToString(CultureInfo.InvariantCulture)
                : (IsoToNanos(value.GetString()!) / 1_000_000).ToString(CultureInfo.InvariantCulture);
        }

        if (IsGeohashType(serverType))
        {
            return value.ValueKind == JsonValueKind.String ? value.GetString()! : value.ToString();
        }

        if (IsDecimalType(serverType))
        {
            return NormalizeDecimal(JsonScalarToString(value));
        }

        if (IsArrayType(serverType))
        {
            return FormatJsonArray(value);
        }

        return value.ValueKind == JsonValueKind.String ? value.GetString()! : value.ToString();
    }

    private static string JsonScalarToString(JsonElement value)
        => value.ValueKind == JsonValueKind.String ? value.GetString()! : value.GetRawText();

    // Match QuestDB /exec JSON: integer-valued doubles render as e.g. "80.0".
    private static string FormatFloat(double f)
    {
        if (f == 0.0)
        {
            return "0.0";
        }

        return PythonRepr(f);
    }

    // QuestDB renders FLOAT at 32-bit precision; narrow both sides to float before comparing.
    private static string FormatFloat32(double value)
    {
        var f = (float)value;
        if (f == 0.0f)
        {
            return "0.0";
        }

        var s = f.ToString("R", CultureInfo.InvariantCulture);
        if (!s.Contains('.') && !s.Contains('e') && !s.Contains('E'))
        {
            s += ".0";
        }

        return s;
    }

    private static string PythonRepr(double f)
    {
        if (double.IsNaN(f))
        {
            return "nan";
        }

        if (double.IsPositiveInfinity(f))
        {
            return "inf";
        }

        if (double.IsNegativeInfinity(f))
        {
            return "-inf";
        }

        var s = f.ToString("R", CultureInfo.InvariantCulture);
        if (s.Contains('E') || s.Contains('e'))
        {
            // Normalise exponent form to Python's e+NN / e-NN with a mantissa decimal point.
            var idx = s.IndexOfAny(new[] { 'E', 'e' });
            var mantissa = s[..idx];
            var exp = s[(idx + 1)..];
            if (!mantissa.Contains('.'))
            {
                mantissa += ".0";
            }

            var expSign = exp.StartsWith("-") ? "-" : "+";
            var expDigits = exp.TrimStart('+', '-').TrimStart('0');
            if (expDigits.Length == 0)
            {
                expDigits = "0";
            }

            if (expDigits.Length < 2)
            {
                expDigits = "0" + expDigits;
            }

            return $"{mantissa}e{expSign}{expDigits}";
        }

        if (!s.Contains('.'))
        {
            s += ".0";
        }

        return s;
    }

    private static string FormatUuid(ulong lo, ulong hi)
    {
        var hex = hi.ToString("x16") + lo.ToString("x16");
        return $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..32]}";
    }

    // Build a Guid whose canonical "D" string equals FormatUuid(lo, hi).
    private static Guid UuidFromLoHi(ulong lo, ulong hi)
    {
        var hex = hi.ToString("x16") + lo.ToString("x16");
        return Guid.ParseExact(
            $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..32]}", "D");
    }

    private static string FormatLong256(byte[] raw)
    {
        var rev = new byte[raw.Length];
        for (var i = 0; i < raw.Length; i++)
        {
            rev[i] = raw[raw.Length - 1 - i];
        }

        var hex = Convert.ToHexString(rev).ToLowerInvariant();
        while (hex.Length > 2 && hex.StartsWith("00"))
        {
            hex = hex[2..];
        }

        return "0x" + hex;
    }

    private const string GeohashBase32 = "0123456789bcdefghjkmnpqrstuvwxyz";

    private static (char Suffix, int N) GeohashDimension(string serverType)
    {
        var inner = serverType[("GEOHASH(".Length)..^1];
        if (inner.Length == 0)
        {
            return ('\0', -1);
        }

        var suffix = inner[^1];
        if (!int.TryParse(inner[..^1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
        {
            return ('\0', -1);
        }

        return (suffix, n);
    }

    private static string FormatGeohashExpected(object value, string serverType)
    {
        if (value is not string s || !s.Contains('/'))
        {
            return value.ToString() ?? "";
        }

        var hexPart = s[..s.IndexOf('/')];
        if (!ulong.TryParse(hexPart, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var bits))
        {
            return s;
        }

        var (suffix, n) = GeohashDimension(serverType);
        if (suffix == 'c' && n >= 0)
        {
            var sb = new StringBuilder();
            for (var i = n - 1; i >= 0; i--)
            {
                sb.Append(GeohashBase32[(int)((bits >> (i * 5)) & 0x1F)]);
            }

            return sb.ToString();
        }

        if (suffix == 'b' && n >= 0)
        {
            var sb = new StringBuilder();
            for (var i = n - 1; i >= 0; i--)
            {
                sb.Append(((bits >> i) & 1) != 0 ? '1' : '0');
            }

            return sb.ToString();
        }

        return s;
    }

    private static string FormatArray(object value)
    {
        switch (value)
        {
            case double[] arr1:
                return "[" + string.Join(",", arr1.Select(v => FormatFloat(v))) + "]";
            case double[,] arr2:
            {
                var rows = new List<string>();
                for (var i = 0; i < arr2.GetLength(0); i++)
                {
                    var cells = new List<string>();
                    for (var j = 0; j < arr2.GetLength(1); j++)
                    {
                        cells.Add(FormatFloat(arr2[i, j]));
                    }

                    rows.Add("[" + string.Join(",", cells) + "]");
                }

                return "[" + string.Join(",", rows) + "]";
            }
            case double[,,] arr3:
            {
                var planes = new List<string>();
                for (var i = 0; i < arr3.GetLength(0); i++)
                {
                    var rows = new List<string>();
                    for (var j = 0; j < arr3.GetLength(1); j++)
                    {
                        var cells = new List<string>();
                        for (var k = 0; k < arr3.GetLength(2); k++)
                        {
                            cells.Add(FormatFloat(arr3[i, j, k]));
                        }

                        rows.Add("[" + string.Join(",", cells) + "]");
                    }

                    planes.Add("[" + string.Join(",", rows) + "]");
                }

                return "[" + string.Join(",", planes) + "]";
            }
            default:
                return value.ToString() ?? "";
        }
    }

    private static string FormatJsonArray(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Array)
        {
            return "[" + string.Join(",", value.EnumerateArray().Select(FormatJsonArray)) + "]";
        }

        if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return "null";
        }

        if (value.ValueKind == JsonValueKind.Number)
        {
            return FormatFloat(value.GetDouble());
        }

        return value.ToString();
    }

    private static string NormalizeDecimal(object? value)
    {
        if (value is null)
        {
            return "";
        }

        return (value.ToString() ?? "").ToLowerInvariant();
    }

    private static readonly DateTime IsoEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static long IsoToNanos(string s)
    {
        var text = s;
        if (text.EndsWith("Z"))
        {
            text = text[..^1];
        }

        string datePart;
        string frac9;
        var dot = text.IndexOf('.');
        if (dot >= 0)
        {
            datePart = text[..dot];
            var frac = text[(dot + 1)..];
            frac9 = (frac + "000000000")[..9];
        }
        else
        {
            datePart = text;
            frac9 = "000000000";
        }

        var dt = DateTime.ParseExact(datePart, "yyyy-MM-ddTHH:mm:ss",
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        var deltaTicks = dt.Ticks - IsoEpoch.Ticks;
        // Total whole seconds (can be negative for pre-epoch); ticks are 100ns each.
        var secs = deltaTicks / TimeSpan.TicksPerSecond;
        return secs * 1_000_000_000L + long.Parse(frac9, CultureInfo.InvariantCulture);
    }

    private static long PickTimestampUs(FuzzParams fuzz, Rng rng)
    {
        long baseUs;
        if (ShouldFuzz(fuzz.ExtremeTimestampFactor, rng))
        {
            baseUs = rng.NextInt(2) == 0 ? PreEpochTsUsBase : FarFutureTsUsBase;
        }
        else
        {
            baseUs = DefaultTsUsBase;
        }

        return baseUs + rng.NextLongBounded(TsUsJitter);
    }

    private static long PickTimestampNs(FuzzParams fuzz, Rng rng)
    {
        long baseNs;
        if (ShouldFuzz(fuzz.ExtremeTimestampFactor, rng))
        {
            baseNs = rng.NextInt(2) == 0 ? PreEpochTsNsBase : FarFutureTsNsBase;
        }
        else
        {
            baseNs = DefaultTsNsBase;
        }

        return baseNs + rng.NextLongBounded(TsNsJitter);
    }


    private static async Task ExecAsync(string httpEndpoint, string sql)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        using var resp = await client.GetAsync($"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}");
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync();
        using var json = JsonDocument.Parse(body);
        if (json.RootElement.TryGetProperty("error", out var err))
        {
            throw new InvalidOperationException(err.GetString());
        }
    }

    private static async Task<(ColumnInfo[] Columns, IReadOnlyList<JsonElement[]> Rows)> QuerySortedAsync(
        string httpEndpoint, string table)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
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
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        var resp = await client.GetAsync(
            $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString($"SHOW COLUMNS FROM '{table}'")}");
        resp.EnsureSuccessStatusCode();
        using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        var root = json.RootElement;
        if (root.TryGetProperty("error", out _))
        {
            return new List<ColumnInfo>();
        }

        if (!root.TryGetProperty("columns", out var cols) || !root.TryGetProperty("dataset", out var dataset))
        {
            return new List<ColumnInfo>();
        }

        var nameIdx = -1;
        var typeIdx = -1;
        var ci = 0;
        foreach (var c in cols.EnumerateArray())
        {
            var n = c.GetProperty("name").GetString();
            if (n == "column")
            {
                nameIdx = ci;
            }
            else if (n == "type")
            {
                typeIdx = ci;
            }

            ci++;
        }

        if (nameIdx < 0 || typeIdx < 0)
        {
            return new List<ColumnInfo>();
        }

        var result = new List<ColumnInfo>();
        foreach (var row in dataset.EnumerateArray())
        {
            var arr = row.EnumerateArray().ToArray();
            result.Add(new ColumnInfo(arr[nameIdx].GetString()!, arr[typeIdx].GetString()!));
        }

        return result;
    }

    private async Task WaitForRowCountAsync(string httpEndpoint, string table, int expected)
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

            await Task.Delay(50);
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

    private static string PickTableName(int numTables, Rng rng)
    {
        var baseName = rng.NextInt(UppercaseTableRandomizeFactor) == 0 ? "WEATHER" : "weather";
        return baseName + rng.NextInt(numTables);
    }

    private static ulong DeriveMasterSeed()
    {
        var raw = Environment.GetEnvironmentVariable("QWP_WS_FUZZ_SEED");
        if (!string.IsNullOrWhiteSpace(raw))
        {
            raw = raw.Trim();
            return raw.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? Convert.ToUInt64(raw[2..], 16)
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

        public ulong NextLong() => unchecked((ulong)_impl.NextInt64());

        public long NextLongBounded(long bound) => _impl.NextInt64(bound);

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

        public LineData(long timestampUs) => TimestampUs = timestampUs;

        public long TimestampUs { get; }

        // First write of a column name within a line wins (Java putIfAbsent semantics).
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
            get
            {
                lock (_lock)
                {
                    return _rows.Count;
                }
            }
        }

        public void AddRow(LineData row)
        {
            lock (_lock)
            {
                _rows.Add(row);
            }
        }

        public void RecordInitialType(string column, ColType type)
        {
            lock (_lock)
            {
                _initialTypes.TryAdd(column, type);
            }
        }

        public ColType? InitialColType(string column)
        {
            lock (_lock)
            {
                return _initialTypes.TryGetValue(column, out var t) ? t : null;
            }
        }

        public List<LineData> RowsSortedByTimestamp()
        {
            lock (_lock)
            {
                return _rows.OrderBy(r => r.TimestampUs).ToList();
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
        public bool SendSymbolsWithSpace { get; init; }
        public double ColumnConvertProb { get; init; }
        public int MaxBounces { get; init; }
        public int MinBounceIntervalMs { get; init; } = 500;
        public int MaxBounceIntervalMs { get; init; } = 3000;
        public int ExtremeStringFactor { get; init; } = -1;
        public int ExtremeNumericFactor { get; init; } = -1;
        public int ExtremeTimestampFactor { get; init; } = -1;
        public int NegativeZeroFactor { get; init; } = -1;
    }

    private sealed record LoadParams(int NumLines, int NumIterations, int NumThreads, int NumTables, int WaitMs);
}

#endif
