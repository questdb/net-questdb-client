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
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpVarintTests
{
    private static readonly (ulong Value, byte[] Encoded)[] SpecVectors =
    {
        (0,     new byte[] { 0x00 }),
        (1,     new byte[] { 0x01 }),
        (127,   new byte[] { 0x7F }),
        (128,   new byte[] { 0x80, 0x01 }),
        (255,   new byte[] { 0xFF, 0x01 }),
        (300,   new byte[] { 0xAC, 0x02 }),
        (16384, new byte[] { 0x80, 0x80, 0x01 }),
    };

    [Test]
    public void Write_SpecVectors_ProducesExactBytes()
    {
        Span<byte> buffer = stackalloc byte[QwpVarint.MaxBytes];
        foreach (var (value, encoded) in SpecVectors)
        {
            var written = QwpVarint.Write(buffer, value);

            Assert.That(written, Is.EqualTo(encoded.Length), $"byte count for value {value}");
            Assert.That(buffer[..written].ToArray(), Is.EqualTo(encoded), $"bytes for value {value}");
        }
    }

    [Test]
    public void Read_SpecVectors_DecodesExactValues()
    {
        foreach (var (value, encoded) in SpecVectors)
        {
            var decoded = QwpVarint.Read(encoded, out var bytesRead);

            Assert.That(decoded, Is.EqualTo(value), $"value for bytes {BitConverter.ToString(encoded)}");
            Assert.That(bytesRead, Is.EqualTo(encoded.Length), $"bytes read for value {value}");
        }
    }

    [Test]
    public void GetByteCount_MatchesWrite()
    {
        foreach (var (value, encoded) in SpecVectors)
        {
            Assert.That(QwpVarint.GetByteCount(value), Is.EqualTo(encoded.Length), $"value {value}");
        }
    }

    [Test]
    public void RoundTrip_PowerOfTwoBoundaries_PreservesValue()
    {
        // Boundary values around the 7-bit byte breaks (1<<0, 1<<7, 1<<14, ..., 1<<63).
        for (var bit = 0; bit < 64; bit++)
        {
            var value = bit == 63 ? 1ul << 63 : 1ul << bit;
            AssertRoundTrip(value);
            if (value > 0) AssertRoundTrip(value - 1);
        }
    }

    [Test]
    public void RoundTrip_MaxUlong_Uses10Bytes()
    {
        Span<byte> buffer = stackalloc byte[QwpVarint.MaxBytes];
        var written = QwpVarint.Write(buffer, ulong.MaxValue);

        Assert.That(written, Is.EqualTo(QwpVarint.MaxBytes));
        Assert.That(QwpVarint.GetByteCount(ulong.MaxValue), Is.EqualTo(QwpVarint.MaxBytes));

        var decoded = QwpVarint.Read(buffer[..written], out var read);
        Assert.That(decoded, Is.EqualTo(ulong.MaxValue));
        Assert.That(read, Is.EqualTo(written));
    }

    [Test]
    public void Read_TruncatedInput_Throws()
    {
        // 0x80 has the continuation bit set but no follow-up byte.
        var truncated = new byte[] { 0x80 };
        Assert.Throws<IngressError>(() => QwpVarint.Read(truncated, out _));
    }

    [Test]
    public void Read_OverlongInput_Throws()
    {
        // 11 bytes of continuation bytes — exceeds the 10-byte limit.
        var overlong = new byte[QwpVarint.MaxBytes + 1];
        Array.Fill(overlong, (byte)0x80);
        Assert.Throws<IngressError>(() => QwpVarint.Read(overlong, out _));
    }

    [Test]
    public void Write_DestinationTooSmall_Throws()
    {
        var buffer = new byte[1];
        Assert.Throws<ArgumentException>(() => QwpVarint.Write(buffer, 128));
    }

    [Test]
    public void Read_TrailingBytes_AreNotConsumed()
    {
        // 300 encodes to 0xAC 0x02 (2 bytes); 0xFF after must remain untouched.
        var input = new byte[] { 0xAC, 0x02, 0xFF };
        var decoded = QwpVarint.Read(input, out var bytesRead);

        Assert.That(decoded, Is.EqualTo(300u));
        Assert.That(bytesRead, Is.EqualTo(2));
    }

    [Test]
    public void RoundTrip_RandomFuzz_PreservesValue()
    {
        var rnd = new Random(0xCAFE);
        Span<byte> buffer = stackalloc byte[QwpVarint.MaxBytes];

        for (var i = 0; i < 10_000; i++)
        {
            var hi = (ulong)rnd.NextInt64();
            var lo = (uint)rnd.Next();
            var value = (hi << 32) | lo;

            var written = QwpVarint.Write(buffer, value);
            var decoded = QwpVarint.Read(buffer[..written], out var read);

            Assert.That(decoded, Is.EqualTo(value));
            Assert.That(read, Is.EqualTo(written));
        }
    }

    private static void AssertRoundTrip(ulong value)
    {
        Span<byte> buffer = stackalloc byte[QwpVarint.MaxBytes];
        var written = QwpVarint.Write(buffer, value);
        var decoded = QwpVarint.Read(buffer[..written], out var read);

        Assert.That(decoded, Is.EqualTo(value), $"value {value}");
        Assert.That(read, Is.EqualTo(written), $"bytes match for value {value}");
        Assert.That(written, Is.EqualTo(QwpVarint.GetByteCount(value)), $"GetByteCount for value {value}");
    }
}
