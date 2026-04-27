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

using NUnit.Framework;
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp.Protocol;

/// <summary>
///     Mirrors <c>QwpGorillaDecoderTest.java</c> on Java main 64b7ee69. Round-trips
///     timestamp sequences through <see cref="QwpGorillaEncoder"/> (the known-good
///     wire-format generator covered by <see cref="QwpGorillaEncoderTests"/>) and verifies
///     the decoder reproduces the input across every DoD bucket and the past-end /
///     sign-extension paths.
/// </summary>
[TestFixture]
public class QwpGorillaDecoderTests
{
    [Test]
    public void ZeroDodBucketRoundTrips()
    {
        // Constant stride ⇒ every DoD is 0 ⇒ 1-bit prefix.
        RoundTrip(new long[] { 1000L, 2000L, 3000L, 4000L, 5000L, 6000L });
    }

    [Test]
    public void SevenBitBucketRoundTrips()
    {
        // DoDs in [-64, 63]: small jitter on a stable stride.
        RoundTrip(new long[] { 1000L, 2000L, 3010L, 4030L, 5040L, 6050L });
    }

    [Test]
    public void NineBitBucketRoundTrips()
    {
        // DoDs in [-256, 255] but >7-bit.
        RoundTrip(new long[] { 1000L, 2000L, 3200L, 4600L, 6200L, 7900L });
    }

    [Test]
    public void TwelveBitBucketRoundTrips()
    {
        // DoDs in (255, 2047].
        RoundTrip(new long[] { 1000L, 2000L, 4000L, 8000L, 13800L, 19500L, 25500L });
    }

    [Test]
    public void ThirtyTwoBitFallbackRoundTrips()
    {
        // DoDs beyond 12-bit bucket ⇒ 36-bit fallback.
        RoundTrip(new long[] { 1_000_000L, 2_000_000L, 3_500_000L, 7_000_000L, 12_000_000L });
    }

    [Test]
    public void NegativeDodSignExtensionRoundTrips()
    {
        // Decreasing stride ⇒ negative DoDs ⇒ exercises sign extension in ReadSigned.
        RoundTrip(new long[] { 0L, 10_000L, 19_900L, 29_700L, 39_400L, 48_950L });
    }

    [Test]
    public void MixedBucketsRoundTrip()
    {
        // Heterogeneous: walks every prefix path within one column.
        RoundTrip(new long[] { 0, 1000L, 2000L, 3010L, 4220L, 9430L, 14_638L });
    }

    [Test]
    public void RandomLongSequenceRoundTrips()
    {
        // Stress test: 1000 timestamps with a noisy walk across multiple bucket transitions
        // and bit-reader refills.
        var rng = new Random(0xC0FFEE);
        var ts = new long[1000];
        var t = 1_700_000_000_000_000L;
        long stride = 100;
        for (var i = 0; i < ts.Length; i++)
        {
            ts[i] = t;
            stride += rng.Next(2001) - 1000;
            t += Math.Max(1, stride);
        }
        RoundTrip(ts);
    }

    [Test]
    public void GetBitPositionPropagatesFromBitReader()
    {
        long[] timestamps = { 0L, 100L, 200L, 300L };
        var dst = new byte[256];
        var encoder = new QwpGorillaEncoder();
        var written = encoder.EncodeTimestamps(dst, 0, timestamps);

        var decoder = new QwpGorillaDecoder();
        decoder.Reset(timestamps[0], timestamps[1], dst.AsMemory(16, written - 16));
        Assert.That(decoder.BitPosition, Is.EqualTo(0L));

        decoder.DecodeNext(); // third timestamp
        var posAfterFirst = decoder.BitPosition;
        Assert.That(posAfterFirst, Is.GreaterThan(0L));

        decoder.DecodeNext();
        Assert.That(decoder.BitPosition, Is.GreaterThan(posAfterFirst));
    }

    [Test]
    public void DecodePastEndOfEmptyBitstreamThrows()
    {
        var decoder = new QwpGorillaDecoder();
        decoder.Reset(0L, 100L, ReadOnlyMemory<byte>.Empty);
        Assert.That(() => decoder.DecodeNext(),
                    Throws.TypeOf<QwpDecodeException>().With.Message.Contains("read past end"));
    }

    [Test]
    public void DecodePastEndOfLargeBucketBitstreamThrows()
    {
        // 36-bit fallback bucket per DoD ⇒ predictable byte consumption. Decode the encoded
        // values, then keep asking for more until the past-end check fires. Loop bound caps
        // overrun at < 64 attempts.
        long[] timestamps = { 1_000_000L, 2_000_000L, 3_500_000L, 7_000_000L };
        var dst = new byte[256];
        var encoder = new QwpGorillaEncoder();
        var written = encoder.EncodeTimestamps(dst, 0, timestamps);

        var decoder = new QwpGorillaDecoder();
        decoder.Reset(timestamps[0], timestamps[1], dst.AsMemory(16, written - 16));
        for (var i = 2; i < timestamps.Length; i++)
        {
            Assert.That(decoder.DecodeNext(), Is.EqualTo(timestamps[i]));
        }

        var threw = false;
        for (var i = 0; i < 64; i++)
        {
            try
            {
                decoder.DecodeNext();
            }
            catch (QwpDecodeException ex)
            {
                Assert.That(ex.Message, Does.Contain("read past end"));
                threw = true;
                break;
            }
        }
        Assert.That(threw, Is.True, "decoder must eventually throw past end of bitstream");
    }

    [Test]
    public void ResetBetweenColumnsForgetsPreviousState()
    {
        long[] colA = { 1L, 2L, 3L, 4L };
        long[] colB = { 10_000L, 20_000L, 30_000L, 40_000L };

        var encoderA = new QwpGorillaEncoder();
        var dstA = new byte[64];
        var writtenA = encoderA.EncodeTimestamps(dstA, 0, colA);

        var encoderB = new QwpGorillaEncoder();
        var dstB = new byte[64];
        var writtenB = encoderB.EncodeTimestamps(dstB, 0, colB);

        var decoder = new QwpGorillaDecoder();
        decoder.Reset(colA[0], colA[1], dstA.AsMemory(16, writtenA - 16));
        for (var i = 2; i < colA.Length; i++)
        {
            Assert.That(decoder.DecodeNext(), Is.EqualTo(colA[i]));
        }

        // Re-bind to column B with its own seed timestamps; previous prevDelta /
        // prevTimestamp must NOT leak.
        decoder.Reset(colB[0], colB[1], dstB.AsMemory(16, writtenB - 16));
        for (var i = 2; i < colB.Length; i++)
        {
            Assert.That(decoder.DecodeNext(), Is.EqualTo(colB[i]), $"col B index {i}");
        }
    }

    private static void RoundTrip(ReadOnlySpan<long> timestamps)
    {
        Assert.That(QwpGorillaEncoder.CanUseGorilla(timestamps), Is.True,
                    "test sequence must satisfy CanUseGorilla precondition");

        var dst = new byte[16 + timestamps.Length * 5 + 64];
        var encoder = new QwpGorillaEncoder();
        var written = encoder.EncodeTimestamps(dst, 0, timestamps);

        var decoder = new QwpGorillaDecoder();
        decoder.Reset(timestamps[0], timestamps[1], dst.AsMemory(16, written - 16));
        for (var i = 2; i < timestamps.Length; i++)
        {
            Assert.That(decoder.DecodeNext(), Is.EqualTo(timestamps[i]),
                        $"timestamp index {i}");
        }
    }
}
