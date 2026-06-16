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

using System.Buffers.Binary;
using System.Text;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Query;

/// <summary>Bounds-check fuzz for the QWP egress RESULT_BATCH decoder; port of qwp_egress_bounds_fuzz.rs.</summary>
[TestFixture]
public class QwpEgressBoundsFuzzTests
{
    private const int Iterations = 50;
    private const int CorruptionsPerMessage = 30;

    private static readonly QwpTypeCode[] FuzzableKinds =
    {
        QwpTypeCode.Boolean, QwpTypeCode.Byte, QwpTypeCode.Short, QwpTypeCode.Char,
        QwpTypeCode.Int, QwpTypeCode.Long, QwpTypeCode.Float, QwpTypeCode.Double,
        QwpTypeCode.Date, QwpTypeCode.Uuid, QwpTypeCode.Long256, QwpTypeCode.IPv4,
        QwpTypeCode.Timestamp, QwpTypeCode.TimestampNanos,
        QwpTypeCode.Varchar, QwpTypeCode.Binary, QwpTypeCode.Symbol, QwpTypeCode.Geohash,
        QwpTypeCode.Decimal64, QwpTypeCode.Decimal128, QwpTypeCode.Decimal256,
        QwpTypeCode.DoubleArray,
    };

    [Test]
    public void TruncationAtEveryOffset()
    {
        var masterSeed = DeriveMasterSeed();
        TestContext.Progress.WriteLine($"QwpEgressBoundsFuzz.TruncationAtEveryOffset seed=0x{masterSeed:x16}");
        var master = new SplitMix64(masterSeed);
        for (var iter = 0; iter < Iterations; iter++)
        {
            var seed = master.NextU64();
            try
            {
                var (payload, headerFlags) = GenerateValidMessage(seed);
                SanityCheckDecode(payload, headerFlags);
                for (var truncLen = 0; truncLen < payload.Length; truncLen++)
                {
                    AttemptDecodeNoPanic(payload.AsSpan(0, truncLen), headerFlags);
                }
            }
            catch (Exception ex)
            {
                throw new AssertionException(
                    $"truncation fuzz failure: masterSeed=0x{masterSeed:x16} iteration={iter} iterSeed=0x{seed:x16}", ex);
            }
        }
    }

    [Test]
    public void ByteCorruption()
    {
        var masterSeed = DeriveMasterSeed();
        TestContext.Progress.WriteLine($"QwpEgressBoundsFuzz.ByteCorruption seed=0x{masterSeed:x16}");
        var master = new SplitMix64(masterSeed);
        for (var iter = 0; iter < Iterations; iter++)
        {
            var seed = master.NextU64();
            try
            {
                var (payload, headerFlags) = GenerateValidMessage(seed);
                SanityCheckDecode(payload, headerFlags);

                var rng = new SplitMix64(seed ^ 0xDEAD_BEEF_DEAD_BEEFUL);
                for (var c = 0; c < CorruptionsPerMessage; c++)
                {
                    var corrupted = (byte[])payload.Clone();
                    var nCorrupt = 1 + rng.GenRange(3);
                    for (var i = 0; i < nCorrupt; i++)
                    {
                        corrupted[rng.GenRange(corrupted.Length)] = rng.NextU8();
                    }
                    AttemptDecodeNoPanic(corrupted, headerFlags);
                }
            }
            catch (Exception ex)
            {
                throw new AssertionException(
                    $"corruption fuzz failure: masterSeed=0x{masterSeed:x16} iteration={iter} iterSeed=0x{seed:x16}", ex);
            }
        }
    }

    private static void SanityCheckDecode(byte[] payload, byte headerFlags)
    {
        try
        {
            new QwpResultBatchDecoder(new QwpEgressConnState())
                .Decode(payload, headerFlags, new QwpColumnBatch());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"generated message must decode cleanly (len={payload.Length})", ex);
        }
    }

    /// <summary>
    ///     The decoder must reject malformed input with a controlled exception; any other type
    ///     (a slice overrun, overflow, OOM) signals a missing bounds check and propagates.
    /// </summary>
    private static void AttemptDecodeNoPanic(ReadOnlySpan<byte> bytes, byte headerFlags)
    {
        try
        {
            new QwpResultBatchDecoder(new QwpEgressConnState())
                .Decode(bytes, headerFlags, new QwpColumnBatch());
        }
        catch (QwpDecodeException)
        {
        }
        catch (IngressError)
        {
        }
        catch (DecoderFallbackException)
        {
        }
    }

    private static (byte[] Payload, byte HeaderFlags) GenerateValidMessage(ulong seed)
    {
        var rng = new SplitMix64(seed);
        var rowCount = rng.GenRange(20);
        var colCount = 1 + rng.GenRange(6);

        var kinds = new QwpTypeCode[colCount];
        for (var i = 0; i < colCount; i++)
        {
            kinds[i] = FuzzableKinds[rng.GenRange(FuzzableKinds.Length)];
        }

        DeltaSymbolDict? dict = null;
        var dictSize = 0;
        if (Array.IndexOf(kinds, QwpTypeCode.Symbol) >= 0)
        {
            dictSize = 1 + rng.GenRange(8);
            var entries = new List<string>(dictSize);
            for (var d = 0; d < dictSize; d++)
            {
                var entryLen = 1 + rng.GenRange(8);
                var chars = new char[entryLen];
                for (var k = 0; k < entryLen; k++)
                {
                    chars[k] = (char)('a' + rng.NextU8() % 26);
                }
                entries.Add(new string(chars));
            }
            dict = new DeltaSymbolDict { DeltaStart = 0, Entries = entries };
        }

        var schema = new ResultSchema();
        for (var i = 0; i < colCount; i++)
        {
            schema.Columns.Add(new SchemaColumn($"c{i}", kinds[i]));
        }

        var data = new ResultBatchData { RowCount = rowCount };
        for (var i = 0; i < colCount; i++)
        {
            data.Columns.Add(BuildColumnData(kinds[i], rowCount, dictSize, rng));
        }

        var frame = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data, dict, gorillaEnabled: false);
        var headerFlags = frame[QwpConstants.OffsetFlags];
        var payloadLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(
            frame.AsSpan(QwpConstants.OffsetPayloadLength, 4));
        var payload = frame.AsSpan(QwpConstants.HeaderSize, payloadLen).ToArray();
        return (payload, headerFlags);
    }

    private static TestColumnData BuildColumnData(QwpTypeCode kind, int rowCount, int dictSize, SplitMix64 rng)
    {
        switch (kind)
        {
            case QwpTypeCode.Boolean:
                return new FixedColumnData { DenseBytes = RandomBytes(rng, (rowCount + 7) >> 3) };
            case QwpTypeCode.Byte:
                return new FixedColumnData { DenseBytes = RandomBytes(rng, rowCount) };
            case QwpTypeCode.Short:
            case QwpTypeCode.Char:
                return new FixedColumnData { DenseBytes = RandomBytes(rng, rowCount * 2) };
        }

        var (bitmap, nonNull) = Validity(rowCount, rng);
        switch (kind)
        {
            case QwpTypeCode.Int:
            case QwpTypeCode.Float:
            case QwpTypeCode.IPv4:
                return new FixedColumnData { NullBitmap = bitmap, DenseBytes = RandomBytes(rng, nonNull * 4) };
            case QwpTypeCode.Long:
            case QwpTypeCode.Double:
                return new FixedColumnData { NullBitmap = bitmap, DenseBytes = RandomBytes(rng, nonNull * 8) };
            case QwpTypeCode.Uuid:
                return new FixedColumnData { NullBitmap = bitmap, DenseBytes = RandomBytes(rng, nonNull * 16) };
            case QwpTypeCode.Long256:
                return new FixedColumnData { NullBitmap = bitmap, DenseBytes = RandomBytes(rng, nonNull * 32) };
            case QwpTypeCode.Date:
            case QwpTypeCode.Timestamp:
            case QwpTypeCode.TimestampNanos:
            {
                var values = new long[nonNull];
                for (var i = 0; i < nonNull; i++) values[i] = unchecked((long)rng.NextU64());
                return new TimestampColumnData { NullBitmap = bitmap, DenseValues = values };
            }
            case QwpTypeCode.Varchar:
            {
                var values = new string[nonNull];
                for (var i = 0; i < nonNull; i++) values[i] = RandomAscii(rng, rng.GenRange(20));
                return new VarcharColumnData { NullBitmap = bitmap, DenseValues = values };
            }
            case QwpTypeCode.Binary:
            {
                var values = new byte[nonNull][];
                for (var i = 0; i < nonNull; i++) values[i] = RandomBytes(rng, rng.GenRange(20));
                return new BinaryColumnData { NullBitmap = bitmap, DenseValues = values };
            }
            case QwpTypeCode.Symbol:
            {
                var ids = new int[nonNull];
                for (var i = 0; i < nonNull; i++) ids[i] = rng.GenRange(dictSize);
                return new SymbolColumnData { NullBitmap = bitmap, DenseDictIds = ids };
            }
            case QwpTypeCode.Geohash:
            {
                var precision = 1 + rng.GenRange(QwpConstants.MaxGeohashPrecisionBits);
                var stride = (precision + 7) >> 3;
                return new GeohashColumnData
                {
                    NullBitmap = bitmap,
                    PrecisionBits = precision,
                    DenseBytes = RandomBytes(rng, nonNull * stride),
                };
            }
            case QwpTypeCode.Decimal64:
                return new DecimalColumnData
                {
                    NullBitmap = bitmap, Scale = (byte)rng.GenRange(20),
                    DenseBytes = RandomBytes(rng, nonNull * QwpConstants.Decimal64SizeBytes),
                };
            case QwpTypeCode.Decimal128:
                return new DecimalColumnData
                {
                    NullBitmap = bitmap, Scale = (byte)rng.GenRange(20),
                    DenseBytes = RandomBytes(rng, nonNull * QwpConstants.Decimal128SizeBytes),
                };
            case QwpTypeCode.Decimal256:
                return new DecimalColumnData
                {
                    NullBitmap = bitmap, Scale = (byte)rng.GenRange(20),
                    DenseBytes = RandomBytes(rng, nonNull * QwpConstants.Decimal256SizeBytes),
                };
            case QwpTypeCode.DoubleArray:
            {
                var arrays = new (int[] Shape, double[] Values)[nonNull];
                for (var i = 0; i < nonNull; i++)
                {
                    var dim = 1 + rng.GenRange(3);
                    var values = new double[dim];
                    for (var k = 0; k < dim; k++)
                    {
                        values[k] = BitConverter.Int64BitsToDouble(unchecked((long)rng.NextU64()));
                    }
                    arrays[i] = (new[] { dim }, values);
                }
                return new DoubleArrayColumnData { NullBitmap = bitmap, DenseArrays = arrays };
            }
            default:
                throw new InvalidOperationException($"FuzzableKinds contains an unhandled kind {kind}");
        }
    }

    private static (byte[]? Bitmap, int NonNull) Validity(int rowCount, SplitMix64 rng)
    {
        if (rowCount == 0 || !rng.GenBool())
        {
            return (null, rowCount);
        }

        var nullCount = rng.GenRange(rowCount + 1);
        var bitmap = new byte[(rowCount + 7) >> 3];
        var remaining = nullCount;
        while (remaining > 0)
        {
            var pos = rng.GenRange(rowCount);
            var bit = (byte)(1 << (pos & 7));
            if ((bitmap[pos >> 3] & bit) == 0)
            {
                bitmap[pos >> 3] |= bit;
                remaining--;
            }
        }
        return (bitmap, rowCount - nullCount);
    }

    private static byte[] RandomBytes(SplitMix64 rng, int n)
    {
        var bytes = new byte[n];
        for (var i = 0; i < n; i++) bytes[i] = rng.NextU8();
        return bytes;
    }

    private static string RandomAscii(SplitMix64 rng, int n)
    {
        var chars = new char[n];
        for (var i = 0; i < n; i++) chars[i] = (char)(0x20 + rng.NextU8() % 95);
        return new string(chars);
    }

    private static ulong DeriveMasterSeed()
    {
        var raw = Environment.GetEnvironmentVariable("QWP_FUZZ_SEED");
        if (!string.IsNullOrWhiteSpace(raw))
        {
            raw = raw.Trim();
            return raw.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? Convert.ToUInt64(raw.Substring(2), 16)
                : ulong.Parse(raw);
        }
        return unchecked((ulong)(DateTime.UtcNow.Ticks ^ ((long)Environment.TickCount << 32)));
    }

    private sealed class SplitMix64
    {
        private ulong _state;

        public SplitMix64(ulong seed)
        {
            _state = seed | 0x9E37_79B9_7F4A_7C15UL;
        }

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

        public byte NextU8() => (byte)NextU64();

        public int GenRange(int bound) => (int)(NextU64() % (ulong)bound);

        public bool GenBool() => (NextU64() & 1) == 0;
    }
}
