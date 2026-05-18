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
using System.Text;
using System.Text.Json;
using NUnit.Framework;
using QuestDB;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_tests;

/// <summary>
///     Live-server egress random-schema fuzz with a per-cell hash oracle; port of
///     egress_live_server_fuzz.rs.
/// </summary>
[TestFixture]
public class QuestDbEgressFuzzTests
{
    private const ulong DefaultSeed = 0xB39C_4F7E_2A85_91D2UL;

    [Test]
    public Task RandomSchemaRoundtrip() => RunAsync(
        httpPort: 19600, ilpPort: 19609, testName: nameof(RandomSchemaRoundtrip),
        (server, rng, compression) =>
        {
            var generators = BuildGenerators();
            for (var iter = 0; iter < 15; iter++)
            {
                var colCount = 1 + (int)rng.GenRange(6);
                using var client = QueryClient.New(EgressConn(server, compression));
                RunOneCase(server, client, rng, generators, "fuzz_iter", iter, colCount);
            }
            return Task.CompletedTask;
        });

    [Test]
    public Task BackToBackQueriesSameConnection() => RunAsync(
        httpPort: 19610, ilpPort: 19619, testName: nameof(BackToBackQueriesSameConnection),
        (server, rng, compression) =>
        {
            var generators = BuildGenerators();
            using var client = QueryClient.New(EgressConn(server, compression));
            for (var iter = 0; iter < 12; iter++)
            {
                var colCount = 1 + (int)rng.GenRange(4);
                RunOneCase(server, client, rng, generators, "fuzz_back", iter, colCount);
            }
            return Task.CompletedTask;
        });

    [Test]
    public Task WideTables() => RunAsync(
        httpPort: 19620, ilpPort: 19629, testName: nameof(WideTables),
        (server, rng, compression) =>
        {
            var generators = BuildGenerators();
            using var client = QueryClient.New(EgressConn(server, compression));
            var colCount = 10 + (int)rng.GenRange(7);
            RunOneCase(server, client, rng, generators, "fuzz_wide", 0, colCount);
            return Task.CompletedTask;
        });

    private static async Task RunAsync(
        int httpPort, int ilpPort, string testName, Func<QuestDbManager, SplitMix64, string, Task> body)
    {
        var rng = new SplitMix64(SeedFor(testName));
        var chunk = 1 + (int)rng.GenRange(500);
        var compression = PickCompression(rng);
        TestContext.Progress.WriteLine($"{testName} chunk={chunk} compression={compression}");

        var chunkStr = chunk.ToString(CultureInfo.InvariantCulture);
        var server = new QuestDbManager(ilpPort, httpPort, new[]
        {
            $"debug.http.force.recv.fragmentation.chunk.size={chunkStr}",
            $"debug.http.force.send.fragmentation.chunk.size={chunkStr}",
        });

        try
        {
            await server.StartAsync();
        }
        catch (Exception ex) when (ex is InvalidOperationException or FileNotFoundException)
        {
            await server.DisposeAsync();
            Assert.Ignore($"QWP egress fuzz needs a QuestDB master build: {ex.Message}");
            return;
        }

        try
        {
            await body(server, rng, compression);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    private static string EgressConn(QuestDbManager srv, string compression)
        => $"ws::addr={srv.GetWebSocketEndpoint()};path={QwpConstants.ReadPath};target=any;" +
           (compression.Length > 0 ? compression + ";" : string.Empty);

    private static void RunOneCase(
        QuestDbManager server, IQwpQueryClient client, SplitMix64 rng,
        ColumnGenerator[] generators, string tableStem, int iter, int colCount)
    {
        var http = server.GetHttpEndpoint();

        var picked = new ColumnGenerator[colCount];
        for (var i = 0; i < colCount; i++)
        {
            picked[i] = generators[(int)rng.GenRange((ulong)generators.Length)];
        }

        var rowCount = PickRowCount(rng);
        var table = $"{tableStem}_{iter}";

        Exec(http, $"drop table if exists \"{table}\"");
        var create = new StringBuilder($"create table \"{table}\" (id LONG, ts TIMESTAMP");
        for (var i = 0; i < colCount; i++)
        {
            create.Append($", c{i} {picked[i].SqlType}");
        }
        create.Append(") timestamp(ts) partition by day wal");
        Exec(http, create.ToString());

        var expectedHash = new long[rowCount][];
        var expectedNull = new bool[rowCount][];
        var valuesClauses = new List<string>(rowCount);
        for (var r = 0; r < rowCount; r++)
        {
            expectedHash[r] = new long[colCount];
            expectedNull[r] = new bool[colCount];
            var id = r + 1L;
            var ts = id * 1000;
            var lits = new List<string>(colCount + 2) { $"{id}L", $"CAST({ts} AS TIMESTAMP)" };
            for (var c = 0; c < colCount; c++)
            {
                var gen = picked[c];
                var forceNull = gen.SupportsNull && rng.GenRange(5) == 0;
                if (forceNull)
                {
                    expectedNull[r][c] = true;
                    lits.Add($"CAST(NULL AS {gen.SqlType})");
                }
                else
                {
                    var cell = gen.RandomValue(rng);
                    expectedHash[r][c] = cell.Hash;
                    lits.Add(cell.Literal);
                }
            }
            valuesClauses.Add($"({string.Join(",", lits)})");
        }

        Exec(http, $"insert into \"{table}\" values {string.Join(",", valuesClauses)}");
        WaitForRows(http, table, rowCount);

        var plan = PlanQuery(rng, table, colCount, rowCount, iter);
        var expectedRows = plan.LastRowId - plan.FirstRowId + 1;

        var handler = new HashCollectingHandler(picked, plan.ProjMap);
        client.Execute(plan.Sql, handler);

        Assert.That(handler.Rows.Count, Is.EqualTo(expectedRows),
            $"iter={iter} shape={plan.Shape} row_count drift");

        for (var outRow = 0; outRow < handler.Rows.Count; outRow++)
        {
            var id = plan.Descending ? plan.LastRowId - outRow : plan.FirstRowId + outRow;
            var inputRow = id - 1;
            var cells = handler.Rows[outRow];
            for (var outC = 0; outC < plan.ProjMap.Length; outC++)
            {
                var inC = plan.ProjMap[outC];
                Assert.That(cells[outC].IsNull, Is.EqualTo(expectedNull[inputRow][inC]),
                    $"iter={iter} shape={plan.Shape} row={inputRow} outC={outC} inC={inC} null-mismatch");
                if (!cells[outC].IsNull)
                {
                    Assert.That(cells[outC].Hash, Is.EqualTo(expectedHash[inputRow][inC]),
                        $"iter={iter} shape={plan.Shape} row={inputRow} outC={outC} inC={inC} " +
                        $"type={picked[inC].SqlType} hash-mismatch");
                }
            }
        }

        Exec(http, $"drop table \"{table}\"");
    }

    private sealed class Plan
    {
        public string Sql = string.Empty;
        public int Shape;
        public bool Descending;
        public int FirstRowId;
        public int LastRowId;
        public int[] ProjMap = Array.Empty<int>();
    }

    private static Plan PlanQuery(SplitMix64 rng, string table, int colCount, int rowCount, int iter)
    {
        var shape = rowCount < 4 ? 0 : iter % 4;
        var identity = Enumerable.Range(0, colCount).ToArray();
        string Proj(int[] cols) => string.Join(",", cols.Select(c => $"c{c}"));

        switch (shape)
        {
            case 0:
                return new Plan
                {
                    Sql = $"select {Proj(identity)} from \"{table}\" order by id",
                    Shape = 0, Descending = false, FirstRowId = 1, LastRowId = rowCount, ProjMap = identity,
                };
            case 1:
            {
                var pickCount = 1 + (int)rng.GenRange((ulong)colCount);
                var shuffled = (int[])identity.Clone();
                for (var i = colCount - 1; i > 0; i--)
                {
                    var j = (int)rng.GenRange((ulong)(i + 1));
                    (shuffled[i], shuffled[j]) = (shuffled[j], shuffled[i]);
                }
                var subset = shuffled.Take(pickCount).ToArray();
                return new Plan
                {
                    Sql = $"select {Proj(subset)} from \"{table}\" order by id",
                    Shape = 1, Descending = false, FirstRowId = 1, LastRowId = rowCount, ProjMap = subset,
                };
            }
            case 2:
            {
                var lo = 1 + (int)rng.GenRange((ulong)(rowCount - 1));
                var maxSpan = rowCount - lo;
                var span = maxSpan == 0 ? 0 : (int)rng.GenRange((ulong)(maxSpan + 1));
                var hi = lo + span;
                return new Plan
                {
                    Sql = $"select {Proj(identity)} from \"{table}\" where id >= {lo} and id <= {hi} order by id",
                    Shape = 2, Descending = false, FirstRowId = lo, LastRowId = hi, ProjMap = identity,
                };
            }
            default:
            {
                var limit = 1 + (int)rng.GenRange((ulong)rowCount);
                return new Plan
                {
                    Sql = $"select {Proj(identity)} from \"{table}\" order by id desc limit {limit}",
                    Shape = 3, Descending = true,
                    FirstRowId = rowCount - limit + 1, LastRowId = rowCount, ProjMap = identity,
                };
            }
        }
    }

    private readonly record struct CellGen(string Literal, long Hash);

    private sealed class ColumnGenerator
    {
        public required string SqlType { get; init; }
        public bool SupportsNull { get; init; } = true;
        public required Func<SplitMix64, CellGen> RandomValue { get; init; }
        public required Func<QwpColumnBatch, int, int, long> ObservedHash { get; init; }
    }

    private static ColumnGenerator[] BuildGenerators()
    {
        var loPool = Enumerable.Range(0, 8).Select(i => $"s_lo_{i}").ToArray();
        var hiPool = Enumerable.Range(0, 1000).Select(i => $"s_hi_{i}").ToArray();

        return new[]
        {
            new ColumnGenerator
            {
                SqlType = "LONG",
                RandomValue = rng =>
                {
                    long v;
                    do { v = rng.NextI64(); } while (v == long.MinValue);
                    return new CellGen($"{v}L", v);
                },
                ObservedHash = (b, c, r) => b.GetLongValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "INT",
                RandomValue = rng =>
                {
                    int v;
                    do { v = rng.NextI32(); } while (v == int.MinValue);
                    return new CellGen(v.ToString(CultureInfo.InvariantCulture), v);
                },
                ObservedHash = (b, c, r) => b.GetIntValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "SHORT", SupportsNull = false,
                RandomValue = rng =>
                {
                    var v = (short)((int)rng.GenRange(65535) - 32767);
                    return new CellGen($"CAST({v} AS SHORT)", v);
                },
                ObservedHash = (b, c, r) => b.GetShortValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "BYTE", SupportsNull = false,
                RandomValue = rng =>
                {
                    var v = (sbyte)((int)rng.GenRange(255) - 127);
                    return new CellGen($"CAST({v} AS BYTE)", v);
                },
                ObservedHash = (b, c, r) => b.GetSByteValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "BOOLEAN", SupportsNull = false,
                RandomValue = rng =>
                {
                    var v = rng.NextBool();
                    return new CellGen(v ? "true" : "false", v ? 1 : 0);
                },
                ObservedHash = (b, c, r) => b.GetBoolValue(c, r) ? 1 : 0,
            },
            new ColumnGenerator
            {
                SqlType = "DOUBLE",
                RandomValue = rng =>
                {
                    double v;
                    do { v = rng.NextF64(); } while (!double.IsFinite(v));
                    return new CellGen(FormatDoubleLiteral(v), BitConverter.DoubleToInt64Bits(v));
                },
                ObservedHash = (b, c, r) => BitConverter.DoubleToInt64Bits(b.GetDoubleValue(c, r)),
            },
            new ColumnGenerator
            {
                SqlType = "FLOAT",
                RandomValue = rng =>
                {
                    float v;
                    do { v = rng.NextF32(); } while (!float.IsFinite(v));
                    return new CellGen($"CAST({FormatFloatLiteral(v)} AS FLOAT)",
                        (uint)BitConverter.SingleToInt32Bits(v));
                },
                ObservedHash = (b, c, r) => (uint)BitConverter.SingleToInt32Bits(b.GetFloatValue(c, r)),
            },
            new ColumnGenerator
            {
                SqlType = "CHAR", SupportsNull = false,
                RandomValue = rng =>
                {
                    var ch = (char)('A' + (int)rng.GenRange(26));
                    return new CellGen($"'{ch}'", ch);
                },
                ObservedHash = (b, c, r) => b.GetCharValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "TIMESTAMP",
                RandomValue = rng =>
                {
                    var v = rng.NextI64() & 0x0FFF_FFFF_FFFF_FFFFL;
                    return new CellGen($"CAST({v} AS TIMESTAMP)", v);
                },
                ObservedHash = (b, c, r) => b.GetTimestampValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "TIMESTAMP_NS",
                RandomValue = rng =>
                {
                    var v = rng.NextI64() & 0x0FFF_FFFF_FFFF_FFFFL;
                    return new CellGen($"CAST({v} AS TIMESTAMP_NS)", v);
                },
                ObservedHash = (b, c, r) => b.GetTimestampValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "DATE",
                RandomValue = rng =>
                {
                    var v = rng.NextI64() & 0x0000_FFFF_FFFF_FFFFL;
                    return new CellGen($"CAST({v} AS DATE)", v);
                },
                ObservedHash = (b, c, r) => b.GetDateValue(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "VARCHAR",
                RandomValue = rng =>
                {
                    var s = RandomAsciiString(rng, (int)rng.GenRange(30));
                    return new CellGen($"CAST('{s.Replace("'", "''")}' AS VARCHAR)", HashBytes(s));
                },
                ObservedHash = (b, c, r) => HashBytes(b.GetString(c, r) ?? string.Empty),
            },
            SymbolGenerator(loPool),
            SymbolGenerator(hiPool),
            new ColumnGenerator
            {
                SqlType = "UUID",
                RandomValue = rng =>
                {
                    var lo = rng.NextU64();
                    var hi = rng.NextU64();
                    if (lo == unchecked((ulong)long.MinValue) && hi == unchecked((ulong)long.MinValue))
                    {
                        lo = 0;
                    }
                    var uuid = $"{hi:x16}".Insert(12, "-").Insert(8, "-")
                               + "-" + $"{lo:x16}".Insert(4, "-");
                    return new CellGen($"CAST('{uuid}' AS UUID)", unchecked((long)(hi ^ lo)));
                },
                ObservedHash = (b, c, r) => b.GetUuidHi(c, r) ^ b.GetUuidLo(c, r),
            },
            new ColumnGenerator
            {
                SqlType = "LONG256",
                RandomValue = rng =>
                {
                    var w0 = rng.NextU64();
                    var w1 = rng.NextU64();
                    var w2 = rng.NextU64();
                    var w3 = rng.NextU64();
                    var hex = $"0x{w3:x16}{w2:x16}{w1:x16}{w0:x16}";
                    return new CellGen(hex, unchecked((long)(w0 ^ w1 ^ w2 ^ w3)));
                },
                ObservedHash = (b, c, r) =>
                {
                    b.GetLong256(c, r, out var w0, out var w1, out var w2, out var w3);
                    return w0 ^ w1 ^ w2 ^ w3;
                },
            },
            new ColumnGenerator
            {
                SqlType = "IPV4",
                RandomValue = rng =>
                {
                    var a = 1 + (uint)rng.GenRange(254);
                    var b = 1 + (uint)rng.GenRange(254);
                    var c = 1 + (uint)rng.GenRange(254);
                    var d = 1 + (uint)rng.GenRange(254);
                    var packed = (a << 24) | (b << 16) | (c << 8) | d;
                    return new CellGen($"CAST('{a}.{b}.{c}.{d}' AS IPV4)", packed);
                },
                ObservedHash = (b, c, r) => (uint)b.GetIPv4Value(c, r),
            },
        };
    }

    private static ColumnGenerator SymbolGenerator(string[] pool) => new()
    {
        SqlType = "SYMBOL",
        RandomValue = rng =>
        {
            var s = pool[(int)rng.GenRange((ulong)pool.Length)];
            return new CellGen($"CAST('{s}' AS SYMBOL)", HashBytes(s));
        },
        ObservedHash = (b, c, r) => HashBytes(b.GetSymbol(c, r) ?? string.Empty),
    };

    private static long HashBytes(string s) => HashBytes(Encoding.UTF8.GetBytes(s));

    private static long HashBytes(byte[] bytes)
    {
        var h = 1_125_899_906_842_597UL;
        foreach (var b in bytes)
        {
            h = unchecked(h * 31 + b);
        }
        return unchecked((long)(h ^ (ulong)bytes.Length));
    }

    private static string RandomAsciiString(SplitMix64 rng, int len)
    {
        var chars = new char[len];
        for (var i = 0; i < len; i++)
        {
            var c = (byte)(0x20 + rng.GenRange(0x7E - 0x20));
            if (c == 0x27) c = 0x20;
            chars[i] = (char)c;
        }
        return new string(chars);
    }

    private static string FormatDoubleLiteral(double v)
    {
        var s = v.ToString("R", CultureInfo.InvariantCulture);
        return s.IndexOfAny(new[] { '.', 'e', 'E' }) < 0 ? s + ".0" : s;
    }

    private static string FormatFloatLiteral(float v)
    {
        var s = v.ToString("R", CultureInfo.InvariantCulture);
        return s.IndexOfAny(new[] { '.', 'e', 'E' }) < 0 ? s + ".0" : s;
    }

    private static int PickRowCount(SplitMix64 rng)
    {
        int[] choices = { 1, 2, 7, 64, 257, 499, 500 };
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

    private static void Exec(string httpEndpoint, string sql)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
        using var resp = client.GetAsync($"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}")
            .GetAwaiter().GetResult();
        Assert.That((int)resp.StatusCode, Is.InRange(200, 399),
            $"exec failed ({(int)resp.StatusCode}): {(sql.Length > 200 ? sql.Substring(0, 200) + "…" : sql)}");
    }

    private static void WaitForRows(string httpEndpoint, string table, int expected)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var resp = client.GetAsync(
                        $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString($"select count() from \"{table}\"")}")
                    .GetAwaiter().GetResult();
                if (resp.IsSuccessStatusCode)
                {
                    using var json = JsonDocument.Parse(resp.Content.ReadAsStringAsync().GetAwaiter().GetResult());
                    if (json.RootElement.TryGetProperty("dataset", out var ds) && ds.GetArrayLength() > 0
                        && ds[0][0].GetInt64() >= expected)
                    {
                        return;
                    }
                }
            }
            catch
            {
            }

            Thread.Sleep(80);
        }

        Assert.Fail($"{table} did not reach {expected} rows within 60s");
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
            foreach (var b in Encoding.UTF8.GetBytes(testName))
            {
                hash ^= b;
                hash *= 0x100_0000_01B3UL;
            }
            var combined = baseSeed + hash;
            TestContext.Progress.WriteLine($"[qwp_egress_fuzz seed] {testName} seed=0x{combined:x16}");
            return combined;
        }
    }

    private readonly record struct CellResult(bool IsNull, long Hash);

    private sealed class HashCollectingHandler : QwpColumnBatchHandler
    {
        private readonly ColumnGenerator[] _picked;
        private readonly int[] _projMap;

        public HashCollectingHandler(ColumnGenerator[] picked, int[] projMap)
        {
            _picked = picked;
            _projMap = projMap;
        }

        public List<CellResult[]> Rows { get; } = new();

        public override void OnBatch(QwpColumnBatch batch)
        {
            for (var r = 0; r < batch.RowCount; r++)
            {
                var cells = new CellResult[_projMap.Length];
                for (var outC = 0; outC < _projMap.Length; outC++)
                {
                    var isNull = batch.IsNull(outC, r);
                    var hash = isNull ? 0L : _picked[_projMap[outC]].ObservedHash(batch, outC, r);
                    cells[outC] = new CellResult(isNull, hash);
                }
                Rows.Add(cells);
            }
        }

        public override void OnError(byte status, string message)
            => Assert.Fail($"unexpected egress error: status={status}, msg={message}");
    }

    public sealed class SplitMix64
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

        public long NextI64() => unchecked((long)NextU64());

        public int NextI32() => unchecked((int)NextU64());

        public bool NextBool() => (NextU64() & 1) == 0;

        public double NextF64()
        {
            var raw = (NextU64() >> 11) / (double)(1UL << 53);
            return (raw - 0.5) * 1e9;
        }

        public float NextF32()
        {
            var raw = (NextU64() >> 40) / (float)(1U << 24);
            return (raw - 0.5f) * 1e5f;
        }

        public ulong GenRange(ulong bound) => NextU64() % bound;
    }
}

#endif
