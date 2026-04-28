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

using NUnit.Framework;
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpGorillaTests
{
    [Test]
    public void Encode_EmptyArray_WritesEncodingFlagOnly()
    {
        var dest = new byte[QwpGorilla.MaxEncodedSize(0)];
        var written = QwpGorilla.Encode(dest, ReadOnlySpan<long>.Empty);

        Assert.That(written, Is.EqualTo(1));
        Assert.That(dest[0], Is.EqualTo(QwpGorilla.EncodingUncompressed));
    }

    [Test]
    public void Encode_SingleValue_FallsBackToUncompressed()
    {
        var input = new long[] { 1_700_000_000L };
        var dest = new byte[QwpGorilla.MaxEncodedSize(1)];
        var written = QwpGorilla.Encode(dest, input);

        Assert.That(written, Is.EqualTo(QwpGorilla.UncompressedSize(1)));
        Assert.That(dest[0], Is.EqualTo(QwpGorilla.EncodingUncompressed));

        AssertRoundTrip(input);
    }

    [Test]
    public void Encode_TwoValues_FallsBackToUncompressed()
    {
        // Gorilla mode allows N=2 (no DoDs), but our encoder skips Gorilla under N=3 since the
        // overhead of the 17-byte Gorilla header exceeds the uncompressed cost at small N.
        var input = new long[] { 1L, 2L };
        var dest = new byte[QwpGorilla.MaxEncodedSize(input.Length)];
        var written = QwpGorilla.Encode(dest, input);

        Assert.That(dest[0], Is.EqualTo(QwpGorilla.EncodingUncompressed));
        Assert.That(written, Is.EqualTo(QwpGorilla.UncompressedSize(2)));
    }

    [Test]
    public void Encode_ConstantDelta_CompressesToOneBitPerDoD()
    {
        // Steady-tick timestamps: every DoD is 0 → 1 bit per DoD after the first two.
        var input = new long[100];
        for (var i = 0; i < input.Length; i++)
        {
            input[i] = 1000L + i;
        }

        var dest = new byte[QwpGorilla.MaxEncodedSize(input.Length)];
        var written = QwpGorilla.Encode(dest, input);

        Assert.That(dest[0], Is.EqualTo(QwpGorilla.EncodingGorilla));
        // 1 (flag) + 16 (first + second) + ceil(98 × 1 / 8) = 1 + 16 + 13 = 30 bytes.
        Assert.That(written, Is.EqualTo(1 + 16 + (98 + 7) / 8));

        AssertRoundTrip(input);
    }

    [Test]
    public void Encode_AllBucketBoundaries_RoundTrip()
    {
        // Build a sequence whose DoD pattern hits every bucket: 0, 7-bit, 9-bit, 12-bit, 32-bit.
        var deltas = new long[] { 100, 100, 165 /* DoD = 65, 9-bit bucket */, 165, 165, 422 /* DoD=257, 12-bit */, 422, 5000 /* DoD ~4578, 12-bit */, 5000, 1_000_000 /* DoD~995000, 32-bit */ };
        var ts = new long[deltas.Length + 1];
        ts[0] = 1_000_000L;
        for (var i = 0; i < deltas.Length; i++)
        {
            ts[i + 1] = ts[i] + deltas[i];
        }

        AssertRoundTrip(ts);
    }

    [Test]
    public void Encode_DoDOverflowsInt32_FallsBackToUncompressed()
    {
        // First two values are zero, then a step that creates a DoD beyond int32 range.
        var ts = new long[] { 0L, 0L, 0L, long.MaxValue / 2 };
        var dest = new byte[QwpGorilla.MaxEncodedSize(ts.Length)];
        var written = QwpGorilla.Encode(dest, ts);

        Assert.That(dest[0], Is.EqualTo(QwpGorilla.EncodingUncompressed));
        Assert.That(written, Is.EqualTo(QwpGorilla.UncompressedSize(ts.Length)));

        AssertRoundTrip(ts);
    }

    [Test]
    public void Encode_NegativeDoDs_RoundTrip()
    {
        var ts = new long[] { 100L, 200L, 250L /* delta 50, dod -50 */, 280L /* delta 30, dod -20 */, 290L };
        AssertRoundTrip(ts);
    }

    [Test]
    public void Encode_RandomFuzz_RoundTrip()
    {
        var rnd = new Random(0xCAFE);
        for (var trial = 0; trial < 100; trial++)
        {
            var n = rnd.Next(3, 200);
            var ts = new long[n];
            ts[0] = rnd.NextInt64(0L, 1_000_000_000L);
            for (var i = 1; i < n; i++)
            {
                // Random delta, occasionally large enough to land in higher buckets.
                var delta = rnd.Next(0, 10_000);
                ts[i] = ts[i - 1] + delta;
            }

            AssertRoundTrip(ts);
        }
    }

    [Test]
    public void Decode_UnknownEncodingFlag_Throws()
    {
        var frame = new byte[] { 0xFF, 0, 0, 0, 0, 0, 0, 0, 0 };
        Assert.Throws<QuestDB.Utils.IngressError>(() =>
            QwpGorilla.Decode(frame, new long[1].AsSpan(), 1));
    }

    [Test]
    public void Decode_TruncatedUncompressed_Throws()
    {
        // Encoding-flag says uncompressed, claims 3 values, but body only has 2 × 8 = 16 bytes.
        var frame = new byte[1 + 16];
        frame[0] = QwpGorilla.EncodingUncompressed;
        Assert.Throws<QuestDB.Utils.IngressError>(() =>
            QwpGorilla.Decode(frame, new long[3].AsSpan(), 3));
    }

    private static void AssertRoundTrip(long[] input)
    {
        var dest = new byte[QwpGorilla.MaxEncodedSize(input.Length)];
        var written = QwpGorilla.Encode(dest, input);

        var roundTripped = new long[input.Length];
        QwpGorilla.Decode(dest.AsSpan(0, written), roundTripped.AsSpan(), input.Length);

        Assert.That(roundTripped, Is.EqualTo(input),
            $"round-trip failed; encoded {written} bytes for {input.Length} values");
    }
}
