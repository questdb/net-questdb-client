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
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Senders;

namespace net_questdb_client_tests;

/// <summary>Live-server egress bind-parameter fuzz; port of egress_live_server_bind_fuzz.rs.</summary>
[TestFixture]
public class QuestDbEgressBindFuzzTests
{
    private const int IlpPort = 19409;
    private const int HttpPort = 19400;
    private const int IterationsPerTest = 25;
    private const ulong DefaultSeed = 0x5EED_1234_ABCD_0001UL;

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
            Assert.Ignore($"QWP egress bind fuzz needs a QuestDB master build: {ex.Message}");
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

    private IQwpQueryClient NewClient()
        => QueryClient.New($"ws::addr={_questDb!.GetWebSocketEndpoint()};path={QwpConstants.ReadPath};target=any;");

    [Test]
    public void FuzzDoubleBinds()
    {
        var rng = new SplitMix64(SeedFor(nameof(FuzzDoubleBinds)));
        using var client = NewClient();
        for (var iter = 0; iter < IterationsPerTest; iter++)
        {
            var v = PickSpecialOrRandomDouble(rng);
            var handler = new SingleBatchHandler();
            client.Execute("SELECT $1::DOUBLE AS d FROM long_sequence(1)",
                b => b.SetDouble(0, v), handler);

            if (double.IsNaN(v))
            {
                var nanOrNull = handler.Batch!.IsNull(0, 0) || double.IsNaN(handler.Batch.GetDoubleValue(0, 0));
                Assert.That(nanOrNull, Is.True,
                    $"iter {iter}: NaN bind must round-trip as null or NaN (QuestDB DOUBLE null is NaN)");
            }
            else
            {
                var got = handler.Batch!.GetDoubleValue(0, 0);
                Assert.That(BitConverter.DoubleToInt64Bits(got),
                    Is.EqualTo(BitConverter.DoubleToInt64Bits(v)), $"iter {iter}: double bit-mismatch");
            }
        }
    }

    [Test]
    public void FuzzIntegralBindsProjection()
    {
        var rng = new SplitMix64(SeedFor(nameof(FuzzIntegralBindsProjection)));
        using var client = NewClient();
        for (var iter = 0; iter < IterationsPerTest; iter++)
        {
            var longVal = PickNonNullLong(rng);
            var intVal = PickNonNullInt(rng);
            var shortVal = unchecked((short)rng.NextI32());
            var byteVal = unchecked((byte)rng.NextI32());
            var boolVal = rng.NextBool();

            var handler = new SingleBatchHandler();
            client.Execute(
                "SELECT $1::LONG AS l, $2::INT AS i, $3::SHORT AS s, " +
                "$4::BYTE AS b, $5::BOOLEAN AS x FROM long_sequence(1)",
                binds => binds
                    .SetLong(0, longVal)
                    .SetInt(1, intVal)
                    .SetShort(2, shortVal)
                    .SetByte(3, byteVal)
                    .SetBoolean(4, boolVal),
                handler);

            var batch = handler.Batch!;
            Assert.That(batch.GetLongValue(0, 0), Is.EqualTo(longVal), $"iter {iter}: long");
            Assert.That(batch.GetIntValue(1, 0), Is.EqualTo(intVal), $"iter {iter}: int");
            Assert.That(batch.GetShortValue(2, 0), Is.EqualTo(shortVal), $"iter {iter}: short");
            Assert.That(batch.GetByteValue(3, 0), Is.EqualTo(byteVal), $"iter {iter}: byte");
            Assert.That(batch.GetBoolValue(4, 0), Is.EqualTo(boolVal), $"iter {iter}: bool");
        }
    }

    [Test]
    public async Task FuzzSameSqlDifferentBindsCacheReuse()
    {
        var table = $"egress_bind_fuzz_cache_{Guid.NewGuid():N}";
        var http = _questDb!.GetHttpEndpoint();
        await ExecAsync(http,
            $"create table \"{table}\" (id LONG, v LONG, part_ts TIMESTAMP) " +
            "timestamp(part_ts) partition by day wal");

        var insert = new System.Text.StringBuilder($"insert into \"{table}\" values ");
        for (var r = 0; r < 100; r++)
        {
            if (r > 0) insert.Append(',');
            insert.Append($"({r}, {r * 7}, CAST({r + 1} AS TIMESTAMP))");
        }
        await ExecAsync(http, insert.ToString());
        await WaitForRowsAsync(http, table, 100);

        var rng = new SplitMix64(SeedFor(nameof(FuzzSameSqlDifferentBindsCacheReuse)));
        using var client = NewClient();
        var sql = $"SELECT v FROM \"{table}\" WHERE id = $1";
        for (var iter = 0; iter < 50; iter++)
        {
            var target = (int)rng.GenRangeU32(100);
            var handler = new SingleBatchHandler();
            client.Execute(sql, b => b.SetInt(0, target), handler);

            Assert.That(handler.Batch!.RowCount, Is.EqualTo(1), $"iter {iter}: row_count");
            Assert.That(handler.Batch.GetLongValue(0, 0), Is.EqualTo((long)target * 7),
                $"iter {iter}: target={target}");
        }

        await ExecAsync(http, $"drop table \"{table}\"");
    }

    [Test]
    public void FuzzUuidBinds()
    {
        var rng = new SplitMix64(SeedFor(nameof(FuzzUuidBinds)));
        using var client = NewClient();
        for (var iter = 0; iter < IterationsPerTest; iter++)
        {
            var lo = unchecked((long)rng.NextU64());
            var hi = unchecked((long)rng.NextU64());
            var handler = new SingleBatchHandler();
            client.Execute("SELECT $1::UUID AS u FROM long_sequence(1)",
                b => b.SetUuid(0, lo, hi), handler);

            Assert.That(handler.Batch!.GetUuidLo(0, 0), Is.EqualTo(lo), $"iter {iter}: uuid lo");
            Assert.That(handler.Batch.GetUuidHi(0, 0), Is.EqualTo(hi), $"iter {iter}: uuid hi");
        }
    }

    private static long PickNonNullLong(SplitMix64 rng)
    {
        while (true)
        {
            var v = rng.NextI64();
            if (v != long.MinValue) return v;
        }
    }

    private static int PickNonNullInt(SplitMix64 rng)
    {
        while (true)
        {
            var v = rng.NextI32();
            if (v != int.MinValue) return v;
        }
    }

    private static double PickSpecialOrRandomDouble(SplitMix64 rng)
    {
        switch (rng.GenRangeU32(4))
        {
            case 0:
                return double.NaN;
            case 1:
                return 0.0;
            default:
                while (true)
                {
                    var v = BitConverter.Int64BitsToDouble(unchecked((long)rng.NextU64()));
                    if (!double.IsInfinity(v)) return v;
                }
        }
    }

    private static async Task ExecAsync(string httpEndpoint, string sql)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        using var resp = await client.GetAsync($"http://{httpEndpoint}/exec?query={Uri.EscapeDataString(sql)}");
        Assert.That((int)resp.StatusCode, Is.InRange(200, 399), $"exec failed: {sql}");
    }

    private static async Task WaitForRowsAsync(string httpEndpoint, string table, int expected)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var resp = await client.GetAsync(
                    $"http://{httpEndpoint}/exec?query={Uri.EscapeDataString($"select count() from \"{table}\"")}");
                if (resp.IsSuccessStatusCode)
                {
                    using var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
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

            await Task.Delay(80);
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

    private sealed class SingleBatchHandler : QwpColumnBatchHandler
    {
        public QwpColumnBatch? Batch { get; private set; }

        public override void OnBatch(QwpColumnBatch batch) => Batch ??= batch;

        public override void OnError(byte status, string message)
            => Assert.Fail($"unexpected egress error: status={status}, msg={message}");
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

        public long NextI64() => unchecked((long)NextU64());

        public int NextI32() => unchecked((int)NextU64());

        public bool NextBool() => (NextU64() & 1) == 0;

        public uint GenRangeU32(uint bound) => (uint)(NextU64() % bound);
    }
}

#endif
