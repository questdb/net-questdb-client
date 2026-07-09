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

using System.Net;
using System.Numerics;
using QuestDB.Senders;

namespace QuestDB.Soak;

internal enum TableKind
{
    Wide,
    Trades,
    Sensors,
    Events
}

/// <summary>
///     One target table. Every row is derived deterministically from a monotonic per-table
///     <c>row_id</c> so the verifier can recompute the expected <c>chk</c> / <c>dval</c> for any row
///     it reads back, and so <c>count()</c> can be reconciled against <see cref="Appended" />.
/// </summary>
internal sealed class TableSpec
{
    private long _rowId = -1;
    private long _appended;

    public TableSpec(string name, TableKind kind, int approxRowBytes)
    {
        Name = name;
        Kind = kind;
        ApproxRowBytes = approxRowBytes;
    }

    public string Name { get; }
    public TableKind Kind { get; }
    public int ApproxRowBytes { get; }

    /// <summary>Total rows handed to a sender buffer for this table (across all producers).</summary>
    public long Appended => Interlocked.Read(ref _appended);

    /// <summary>Total row ids allocated (== rows the client intended to build). Diagnostic vs Appended.</summary>
    public long Generated => Interlocked.Read(ref _rowId) + 1;

    public long NextRowId() => Interlocked.Increment(ref _rowId);
    public void CountAppended() => Interlocked.Increment(ref _appended);

    public static IReadOnlyList<TableSpec> BuildSet(string prefix) => new[]
    {
        new TableSpec(prefix + "wide", TableKind.Wide, 220),
        new TableSpec(prefix + "trades", TableKind.Trades, 64),
        new TableSpec(prefix + "sensors", TableKind.Sensors, 56),
        new TableSpec(prefix + "events", TableKind.Events, 80)
    };
}

/// <summary>
///     Deterministic row generation. All values are pure functions of the row's <c>id</c> so the
///     round-trip verifier can recompute them. The wide table exercises every QWP column type.
/// </summary>
internal static class RowFactory
{
    // The current QuestDB engine accepts only double arrays on the QWP wire ("long arrays are not
    // supported, only double arrays"), so the long-array column is opt-in and defaults off.
    private static bool _includeLongArrays;

    public static void Configure(bool includeLongArrays) => _includeLongArrays = includeLongArrays;

    private static readonly string[] Symbols =
    {
        "AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "AMD",
        "us-east", "us-west", "eu-central", "ap-south", "sensor-a", "sensor-b", "node-1", "node-2"
    };

    private static readonly string[] Strings =
    {
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
        "a moderately long descriptive string value used to exercise varchar encoding"
    };

    // Alloc-free IPv4 source: cast into a small ring rather than allocating per row.
    private static readonly IPAddress[] Ips =
    {
        IPAddress.Parse("10.0.0.1"), IPAddress.Parse("10.0.0.2"), IPAddress.Parse("192.168.1.5"),
        IPAddress.Parse("172.16.0.9"), IPAddress.Parse("127.0.0.1"), IPAddress.Parse("8.8.8.8")
    };

    // BigInteger allocates a backing uint[] for values above int range, so long256 values are
    // pre-built once into a pool and reused. long256 is not checksum-verified, so a fixed set of
    // varied-bit-length values still exercises the encoder with zero per-row allocation.
    private static readonly System.Numerics.BigInteger[] Long256Pool = BuildLong256Pool();

    private static System.Numerics.BigInteger[] BuildLong256Pool()
    {
        var pool = new System.Numerics.BigInteger[1024];
        for (var i = 0; i < pool.Length; i++)
        {
            // Spread bit lengths across the 256-bit range; always non-negative (long256 is unsigned).
            var hi = System.Numerics.BigInteger.One << i % 250;
            var lo = new System.Numerics.BigInteger((ulong)((uint)i * 2654435761u + 1u));
            pool[i] = hi | lo;
        }

        return pool;
    }

    // Base epoch offsets (micros / nanos / millis) kept off the hot path.
    private static readonly long BaseMicros =
        (long)(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) - DateTime.UnixEpoch).TotalMilliseconds * 1000L;

    /// <summary>FNV-1a over the 8 bytes of <paramref name="id" />; the round-trip integrity witness.</summary>
    public static long Checksum(long id)
    {
        unchecked
        {
            var h = 14695981039346656037UL;
            var v = (ulong)id;
            for (var i = 0; i < 8; i++)
            {
                h ^= v & 0xFF;
                h *= 1099511628211UL;
                v >>= 8;
            }

            return (long)h;
        }
    }

    public static double Dval(long id) => id * 1.5;

    /// <summary>Designated timestamp (micros). Ascending in assignment order to keep server-side sorting cheap.</summary>
    public static long TimestampMicros(long id) => BaseMicros + id;

    public static void BuildRow(IQwpWebSocketSender ws, TableSpec spec, long id)
    {
        var sym = Symbols[(int)((ulong)id % (ulong)Symbols.Length)];
        var tsMicros = TimestampMicros(id);

        switch (spec.Kind)
        {
            case TableKind.Wide:
                BuildWide(ws, spec.Name, id, sym, tsMicros);
                break;
            case TableKind.Trades:
                ws.Table(spec.Name);
                ws.Symbol("sym", sym);
                ws.Column("price", 100.0 + id % 5000 * 0.01);
                ws.Column("amount", (id % 1000 + 1) * 0.001);
                ws.Column("dval", Dval(id));
                ws.Column("row_id", id);
                ws.Column("chk", Checksum(id));
                ws.At(tsMicros);
                break;
            case TableKind.Sensors:
                ws.Table(spec.Name);
                ws.Symbol("sym", sym);
                ws.Column("temp", -40.0 + id % 1200 * 0.1);
                ws.Column("dval", Dval(id));
                ws.Column("row_id", id);
                ws.Column("chk", Checksum(id));
                ws.At(tsMicros);
                break;
            case TableKind.Events:
                ws.Table(spec.Name);
                ws.Symbol("sym", sym);
                ws.Column("msg", Strings[(int)((ulong)id % (ulong)Strings.Length)]);
                ws.Column("level", (int)(id % 5));
                ws.Column("dval", Dval(id));
                ws.Column("row_id", id);
                ws.Column("chk", Checksum(id));
                ws.At(tsMicros);
                break;
        }

        spec.CountAppended();
    }

    private static void BuildWide(IQwpWebSocketSender ws, string name, long id, string sym, long tsMicros)
    {
        Span<byte> bin = stackalloc byte[8];
        BitConverter.TryWriteBytes(bin, id);

        Span<double> darr = stackalloc double[3] { id, id * 2.0, id * 0.5 };

        var uid = MakeGuid(id);
        var l256 = Long256Pool[(int)((ulong)id % (ulong)Long256Pool.Length)];
        var geoMask = (1UL << 30) - 1;

        ws.Table(name);
        ws.Symbol("sym", sym);
        ws.Column("str", Strings[(int)((ulong)id % (ulong)Strings.Length)]);
        ws.Column("f_bool", (id & 1) == 0);
        ws.ColumnByte("i8", (sbyte)(id % 127));
        ws.ColumnShort("i16", (short)(id % 30000));
        ws.Column("i32", (int)(id % 1_000_000));
        ws.Column("i64", id);
        ws.ColumnFloat("f32", id % 1000 * 0.5f);
        ws.Column("f64", id * 3.25);
        ws.Column("ch", (char)('A' + (int)(id % 26)));
        ws.ColumnNanos("ts_ns", (BaseMicros + id) * 1000L);
        ws.ColumnDate("dt", BaseMicros / 1000L + id % 86_400_000);
        ws.Column("uid", uid);
        ws.ColumnIPv4("ip", Ips[(int)((ulong)id % (ulong)Ips.Length)]);
        ws.ColumnGeohash("gh", (ulong)id & geoMask, 30);
        ws.ColumnLong256("l256", l256);
        ws.ColumnDecimal64("dec64", (decimal)(id % 100_000), 4);
        ws.ColumnDecimal128("dec128", (decimal)id, 6);
        ws.ColumnDecimal256("dec256", (decimal)(id % 1_000_000), 8);
        ws.ColumnBinary("bin", bin);
        ws.Column<double>("darr", darr);
        if (_includeLongArrays)
        {
            Span<long> larr = stackalloc long[2] { id, -id };
            ws.Column<long>("larr", larr);
        }

        ws.Column("dval", Dval(id));
        ws.Column("row_id", id);
        ws.Column("chk", Checksum(id));
        ws.At(tsMicros);
    }

    private static Guid MakeGuid(long id)
    {
        Span<byte> g = stackalloc byte[16];
        BitConverter.TryWriteBytes(g, id);
        BitConverter.TryWriteBytes(g.Slice(8), ~id);
        return new Guid(g);
    }
}
