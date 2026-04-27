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
 ******************************************************************************/

using System.Buffers.Binary;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Protocol;

/// <summary>
///     Mirrors <c>QwpGorillaEncoderTest.java</c> on Java main 64b7ee69. Java tests reach
///     into native memory; .NET tests use long arrays and span overloads. The wire format
///     and bucket boundaries are identical.
/// </summary>
[TestFixture]
public class QwpGorillaEncoderTests
{
    [Test]
    public void ToIntCheckedAtBoundary()
    {
        Assert.That(QwpGorillaEncoder.ToIntChecked(int.MaxValue), Is.EqualTo(int.MaxValue));
    }

    [Test]
    public void ToIntCheckedJustAboveBoundary()
    {
        Assert.That(() => QwpGorillaEncoder.ToIntChecked((long)int.MaxValue + 1),
                    Throws.TypeOf<OverflowException>().With.Message.Contains("exceeds int range"));
    }

    [Test]
    public void ToIntCheckedLargeValue()
    {
        Assert.That(() => QwpGorillaEncoder.ToIntChecked(long.MaxValue),
                    Throws.TypeOf<OverflowException>().With.Message.Contains("exceeds int range"));
    }

    [Test]
    public void BucketBoundariesExact()
    {
        Assert.That(QwpGorillaEncoder.GetBucket(0), Is.EqualTo(0));
        Assert.That(QwpGorillaEncoder.GetBucket(-64), Is.EqualTo(1));
        Assert.That(QwpGorillaEncoder.GetBucket(63), Is.EqualTo(1));
        Assert.That(QwpGorillaEncoder.GetBucket(-1), Is.EqualTo(1));
        Assert.That(QwpGorillaEncoder.GetBucket(1), Is.EqualTo(1));
        Assert.That(QwpGorillaEncoder.GetBucket(-65), Is.EqualTo(2));
        Assert.That(QwpGorillaEncoder.GetBucket(64), Is.EqualTo(2));
        Assert.That(QwpGorillaEncoder.GetBucket(-256), Is.EqualTo(2));
        Assert.That(QwpGorillaEncoder.GetBucket(255), Is.EqualTo(2));
        Assert.That(QwpGorillaEncoder.GetBucket(-257), Is.EqualTo(3));
        Assert.That(QwpGorillaEncoder.GetBucket(256), Is.EqualTo(3));
        Assert.That(QwpGorillaEncoder.GetBucket(-2048), Is.EqualTo(3));
        Assert.That(QwpGorillaEncoder.GetBucket(2047), Is.EqualTo(3));
        Assert.That(QwpGorillaEncoder.GetBucket(-2049), Is.EqualTo(4));
        Assert.That(QwpGorillaEncoder.GetBucket(2048), Is.EqualTo(4));
        Assert.That(QwpGorillaEncoder.GetBucket(int.MinValue), Is.EqualTo(4));
        Assert.That(QwpGorillaEncoder.GetBucket(int.MaxValue), Is.EqualTo(4));
    }

    [Test]
    public void BitsRequiredPerBucket()
    {
        Assert.That(QwpGorillaEncoder.GetBitsRequired(0), Is.EqualTo(1));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(1), Is.EqualTo(9));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(-64), Is.EqualTo(9));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(63), Is.EqualTo(9));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(-65), Is.EqualTo(12));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(255), Is.EqualTo(12));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(-257), Is.EqualTo(16));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(2047), Is.EqualTo(16));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(-2049), Is.EqualTo(36));
        Assert.That(QwpGorillaEncoder.GetBitsRequired(100_000), Is.EqualTo(36));
    }

    [Test]
    public void CanUseGorillaZeroTimestamps()
    {
        Assert.That(QwpGorillaEncoder.CanUseGorilla(ReadOnlySpan<long>.Empty), Is.True);
    }

    [Test]
    public void CanUseGorillaOneTimestamp()
    {
        Assert.That(QwpGorillaEncoder.CanUseGorilla(new long[] { 1_000_000L }), Is.True);
    }

    [Test]
    public void CanUseGorillaTwoTimestamps()
    {
        Assert.That(QwpGorillaEncoder.CanUseGorilla(new long[] { 1_000_000L, 2_000_000L }), Is.True);
    }

    [Test]
    public void CanUseGorillaReturnsFalseWhenDodExceedsIntRange()
    {
        // delta0 = 0, delta1 = long.MaxValue ⇒ DoD = long.MaxValue
        Assert.That(QwpGorillaEncoder.CanUseGorilla(new long[] { 0, 0, long.MaxValue }), Is.False);
    }

    [Test]
    public void CanUseGorillaReturnsFalseForNegativeOverflow()
    {
        // ts: 0, MAX, MAX ⇒ delta0 = MAX, delta1 = 0, DoD = -MAX (still in long but negative,
        // and -long.MaxValue is just below -int.MaxValue ⇒ outside int range)
        Assert.That(QwpGorillaEncoder.CanUseGorilla(new long[] { 0, long.MaxValue, long.MaxValue }),
                    Is.False);
    }

    [Test]
    public void CanUseGorillaReturnsTrueAtIntBoundary()
    {
        // delta0 = 0, delta1 = int.MaxValue ⇒ DoD exactly int.MaxValue (within range)
        Assert.That(QwpGorillaEncoder.CanUseGorilla(new long[] { 0, 0, int.MaxValue }), Is.True);
    }

    [Test]
    public void CalculateEncodedSizeZeroTimestamps()
    {
        Assert.That(QwpGorillaEncoder.CalculateEncodedSize(ReadOnlySpan<long>.Empty), Is.EqualTo(0));
    }

    [Test]
    public void CalculateEncodedSizeOneTimestamp()
    {
        Assert.That(QwpGorillaEncoder.CalculateEncodedSize(new long[] { 12345L }), Is.EqualTo(8));
    }

    [Test]
    public void CalculateEncodedSizeTwoTimestamps()
    {
        Assert.That(QwpGorillaEncoder.CalculateEncodedSize(new long[] { 1000L, 2000L }),
                    Is.EqualTo(16));
    }

    [Test]
    public void CalculateEncodedSizeConstantDelta()
    {
        const int count = 100;
        var ts = new long[count];
        for (var i = 0; i < count; i++) ts[i] = i * 1000L;
        // 16 bytes for first two ts + ceil(98 * 1 / 8) = 16 + 13 = 29
        Assert.That(QwpGorillaEncoder.CalculateEncodedSize(ts), Is.EqualTo(16 + (98 + 7) / 8));
    }

    [Test]
    public void CalculateEncodedSizeDoesNotOverflowWithLargeCount()
    {
        // Java needed 60M timestamps to trigger an int totalBits overflow. The .NET impl
        // uses long for the bit accumulator, so this is a "large input doesn't crash"
        // smoke test. 1M timestamps ≈ 8 MB allocation.
        const int count = 1_000_000;
        var ts = new long[count];
        long t = 0;
        long delta = 1;
        for (var i = 0; i < count; i++)
        {
            ts[i] = t;
            t += delta;
            delta = (i % 2 == 0) ? 10_001 : 1;
        }

        Assert.That(QwpGorillaEncoder.CanUseGorilla(ts), Is.True);
        var size = QwpGorillaEncoder.CalculateEncodedSize(ts);
        Assert.That(size, Is.GreaterThan(0));
        var minBits = 36L * (count - 2);
        var minSize = 16 + (minBits + 7) / 8;
        Assert.That(size, Is.GreaterThanOrEqualTo(minSize));
    }

    [Test]
    public void CalculateEncodedSizeAllBuckets()
    {
        // 7 timestamps: 2 uncompressed + 5 DoD values, one per bucket
        long[] ts = { 0, 1000, 2000, 3010, 4120, 6230, 16_230 };
        // DoDs: 0 (1b), 10 (9b), 100 (12b), 1000 (16b), 7890 (36b) = 74 bits
        const int expectedDodBytes = (1 + 9 + 12 + 16 + 36 + 7) / 8; // 9 (+ remainder absorbed)
        Assert.That(QwpGorillaEncoder.CalculateEncodedSize(ts), Is.EqualTo(16 + expectedDodBytes));
    }

    [Test]
    public void EncodeZeroTimestamps()
    {
        var encoder = new QwpGorillaEncoder();
        Assert.That(encoder.EncodeTimestamps(Array.Empty<byte>(), 0, ReadOnlySpan<long>.Empty),
                    Is.EqualTo(0));
    }

    [Test]
    public void EncodeOneTimestamp()
    {
        var encoder = new QwpGorillaEncoder();
        var dst = new byte[64];
        var written = encoder.EncodeTimestamps(dst, 0, new long[] { 42_000_000L });
        Assert.That(written, Is.EqualTo(8));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(dst), Is.EqualTo(42_000_000L));
    }

    [Test]
    public void EncodeTwoTimestamps()
    {
        var encoder = new QwpGorillaEncoder();
        var dst = new byte[64];
        var written = encoder.EncodeTimestamps(dst, 0, new long[] { 1_000_000L, 2_000_000L });
        Assert.That(written, Is.EqualTo(16));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(dst), Is.EqualTo(1_000_000L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(dst.AsSpan(8)), Is.EqualTo(2_000_000L));
    }

    [Test]
    public void EncodeSizeMatchesActualEncodedSize()
    {
        var cases = new[]
        {
            new long[] { 100, 200, 300 },
            new long[] { 0, 0, 0, 0 },
            new long[] { 0, 100, 250, 500, 1000 },
            new long[] { 0, 1000, 1_000_000, 1_000_001, 2_000_000 },
        };

        var encoder = new QwpGorillaEncoder();
        foreach (var ts in cases)
        {
            var predicted = QwpGorillaEncoder.CalculateEncodedSize(ts);
            var dst = new byte[ts.Length * 16 + 64];
            var actual = encoder.EncodeTimestamps(dst, 0, ts);
            Assert.That(actual, Is.EqualTo(predicted),
                        $"size mismatch for [{string.Join(",", ts)}]");
        }
    }

    [Test]
    public void EncodeThrowsWhenBufferTooSmallForFirstTimestamp()
    {
        var encoder = new QwpGorillaEncoder();
        var dst = new byte[4]; // < 8
        Assert.That(() => encoder.EncodeTimestamps(dst, 0, new long[] { 1000L }),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void EncodeThrowsWhenBufferTooSmallForSecondTimestamp()
    {
        var encoder = new QwpGorillaEncoder();
        var dst = new byte[12]; // < 16
        Assert.That(() => encoder.EncodeTimestamps(dst, 0, new long[] { 1000L, 2000L }),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void EncoderIsReusableAcrossMultipleCalls()
    {
        var encoder = new QwpGorillaEncoder();
        var dst = new byte[256];

        var first = new long[] { 0, 1000, 2000, 3000, 4000 };
        var size1 = encoder.EncodeTimestamps(dst, 0, first);
        Assert.That(size1, Is.GreaterThan(0));

        var second = new long[] { 0, 9999, 19_998, 29_997, 39_996 };
        var size2 = encoder.EncodeTimestamps(dst, 0, second);
        Assert.That(size2, Is.GreaterThan(0));
        // Round-trip the second encode.
        AssertDecoded(dst, size2, second);
    }

    [Test]
    public void RoundTripConstantDelta()
    {
        var ts = new long[50];
        for (var i = 0; i < ts.Length; i++) ts[i] = 1_000_000L + i * 1000L;
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripIdenticalTimestamps()
    {
        var ts = new long[20];
        Array.Fill(ts, 42_000_000L);
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripThreeTimestampsZeroDod()
    {
        AssertRoundTrip(new long[] { 100, 200, 300 });
    }

    [Test]
    public void RoundTripExactlyThreeTimestamps()
    {
        AssertRoundTrip(new long[] { 0, 500, 1500 });
        AssertRoundTrip(new long[] { 0, 500, 1000 });
        AssertRoundTrip(new long[] { 0, 500, 510 });
        AssertRoundTrip(new long[] { 0, 500, 1_000_500 });
    }

    [Test]
    public void RoundTripBucket1SmallPositiveDod()
    {
        AssertRoundTrip(new long[] { 0, 100, 201, 303, 406, 510 });
    }

    [Test]
    public void RoundTripBucket1SmallNegativeDod()
    {
        AssertRoundTrip(new long[] { 0, 1000, 1999, 2997, 3994, 4990 });
    }

    [Test]
    public void RoundTripBucket1AtBoundaries()
    {
        // DoD exactly -64 and 63
        AssertRoundTrip(new long[] { 0, 100, 263, 362 });
    }

    [Test]
    public void RoundTripBucket2MediumDod()
    {
        AssertRoundTrip(new long[] { 0, 100, 300, 300 });
    }

    [Test]
    public void RoundTripBucket2AtBoundaries()
    {
        // DoD exactly -256 and 255
        AssertRoundTrip(new long[] { 0, 1000, 2255, 3254 });
    }

    [Test]
    public void RoundTripBucket3LargeDod()
    {
        AssertRoundTrip(new long[] { 0, 1000, 2500, 2500 });
    }

    [Test]
    public void RoundTripBucket3AtBoundaries()
    {
        // DoD exactly -2048 and 2047
        AssertRoundTrip(new long[] { 0, 10_000, 22_047, 32_046 });
    }

    [Test]
    public void RoundTripBucket4VeryLargeDod()
    {
        AssertRoundTrip(new long[] { 0, 1000, 1_000_000, 1_000_000 });
    }

    [Test]
    public void RoundTripBucket4AtIntMaxBoundary()
    {
        AssertRoundTrip(new long[] { 0, 0, int.MaxValue });
    }

    [Test]
    public void RoundTripBucket4AtIntMinBoundary()
    {
        AssertRoundTrip(new long[] { 0, 0, int.MinValue });
    }

    [Test]
    public void RoundTripAllBucketsInOneSequence()
    {
        var ts = new long[]
        {
            0, 1000,                       // delta=1000
            2000,                          // DoD=0 (bucket 0)
            3010,                          // DoD=10 (bucket 1)
            4120,                          // DoD=100 (bucket 2)
            6230,                          // DoD=1000 (bucket 3)
            16_230,                        // DoD=7890 (bucket 4)
            26_230,                        // DoD=0 (bucket 0)
            36_200,                        // DoD=-30 (bucket 1)
            45_970,                        // DoD=-200 (bucket 2)
            54_740,                        // DoD=-1000 (bucket 3)
            54_740,                        // DoD=-8770 (bucket 4)
        };
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripNegativeTimestamps()
    {
        AssertRoundTrip(new long[] { -3000, -2000, -1000, 0, 1000, 2000 });
    }

    [Test]
    public void RoundTripLargeTimestampsRealisticMicros()
    {
        const long baseMicros = 1_704_067_200_000_000L; // 2024-01-01
        var ts = new long[100];
        for (var i = 0; i < ts.Length; i++) ts[i] = baseMicros + i * 1_000_000L;
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripRealisticNanosWithJitter()
    {
        const long baseNanos = 1_704_067_200_000_000_000L;
        var ts = new long[200];
        const long interval = 1_000_000L; // 1 ms in ns
        for (var i = 0; i < ts.Length; i++)
        {
            var jitter = (i % 7) * 100 - 300;
            ts[i] = baseNanos + i * interval + jitter;
        }
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripMonotonicDecreasing()
    {
        var ts = new long[30];
        for (var i = 0; i < ts.Length; i++) ts[i] = 1_000_000L - i * 100L;
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripAlternatingDelta()
    {
        var ts = new long[40];
        ts[0] = 0;
        for (var i = 1; i < ts.Length; i++) ts[i] = ts[i - 1] + (i % 2 == 0 ? 100 : 200);
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripZeroValues()
    {
        AssertRoundTrip(new long[] { 0, 0, 0, 0, 0 });
    }

    [Test]
    public void RoundTripLongMaxValues()
    {
        var b = long.MaxValue - 10_000;
        AssertRoundTrip(new long[] { b, b + 1000, b + 2000, b + 3000 });
    }

    [Test]
    public void RoundTripSingleLargeSpike()
    {
        var ts = new long[10];
        for (var i = 0; i < 8; i++) ts[i] = i * 1000L;
        ts[8] = 7000 + 1_000_000;
        ts[9] = 7000 + 1_000_000 + 1000;
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripLargeCountAllBucket0()
    {
        const int count = 10_000;
        var ts = new long[count];
        for (var i = 0; i < count; i++) ts[i] = i * 1_000_000L;
        AssertRoundTrip(ts);
    }

    [Test]
    public void RoundTripLargeCountMixedBuckets()
    {
        const int count = 5000;
        var ts = new long[count];
        ts[0] = 0;
        ts[1] = 1000;
        for (var i = 2; i < count; i++)
        {
            var prevDelta = ts[i - 1] - ts[i - 2];
            int dod = (i % 5) switch
            {
                0 => 0,
                1 => 30,
                2 => -100,
                3 => 1500,
                _ => 5000,
            };
            ts[i] = ts[i - 1] + prevDelta + dod;
        }
        AssertRoundTrip(ts);
    }

    private static void AssertRoundTrip(ReadOnlySpan<long> timestamps)
    {
        Assert.That(timestamps.Length < 3 || QwpGorillaEncoder.CanUseGorilla(timestamps), Is.True,
                    "test sequence must satisfy CanUseGorilla precondition");

        var dst = new byte[16 + timestamps.Length * 5 + 64];
        var encoder = new QwpGorillaEncoder();
        var written = encoder.EncodeTimestamps(dst, 0, timestamps);
        Assert.That(written, Is.GreaterThan(0));
        AssertDecoded(dst, written, timestamps);
    }

    /// <summary>
    ///     Decodes a Gorilla-encoded payload via <see cref="QwpBitReader"/> + a hand-rolled
    ///     bucket walk, verifying every value matches the input. Mirrors the helper at the
    ///     bottom of <c>QwpGorillaEncoderTest.java</c>.
    /// </summary>
    private static void AssertDecoded(byte[] dst, int encodedSize, ReadOnlySpan<long> expected)
    {
        var count = expected.Length;
        if (count == 0) return;

        var ts0 = BinaryPrimitives.ReadInt64LittleEndian(dst);
        Assert.That(ts0, Is.EqualTo(expected[0]), "timestamp[0]");
        if (count == 1) return;

        var ts1 = BinaryPrimitives.ReadInt64LittleEndian(dst.AsSpan(8));
        Assert.That(ts1, Is.EqualTo(expected[1]), "timestamp[1]");
        if (count == 2) return;

        var reader = new QwpBitReader();
        reader.Reset(dst.AsMemory(16, encodedSize - 16));

        var prevTs = ts1;
        var prevDelta = ts1 - ts0;
        for (var i = 2; i < count; i++)
        {
            var dod = ReadDoD(reader);
            var delta = prevDelta + dod;
            var ts = prevTs + delta;
            Assert.That(ts, Is.EqualTo(expected[i]), $"timestamp[{i}]");
            prevDelta = delta;
            prevTs = ts;
        }
    }

    private static long ReadDoD(QwpBitReader r)
    {
        var bit = r.ReadBit();
        if (bit == 0) return 0;
        bit = r.ReadBit();
        if (bit == 0) return r.ReadSigned(7);
        bit = r.ReadBit();
        if (bit == 0) return r.ReadSigned(9);
        bit = r.ReadBit();
        if (bit == 0) return r.ReadSigned(12);
        return r.ReadSigned(32);
    }
}
