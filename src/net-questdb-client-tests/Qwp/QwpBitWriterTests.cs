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
public class QwpBitWriterTests
{
    [Test]
    public void RoundTrip_ZeroBits_NoState()
    {
        Span<byte> buf = stackalloc byte[8];
        var w = new QwpBitWriter(buf, 0);
        w.WriteBits(0, 0);
        Assert.That(w.FinishToByteBoundary(), Is.Zero);
    }

    [Test]
    public void RoundTrip_SingleBit()
    {
        Span<byte> buf = stackalloc byte[1];
        var w = new QwpBitWriter(buf, 0);
        w.WriteBits(1, 1);
        Assert.That(buf[0], Is.EqualTo((byte)1));

        var r = new QwpBitReader(buf, 0);
        Assert.That(r.ReadBits(1), Is.EqualTo(1UL));
    }

    [Test]
    public void RoundTrip_PartialThenWholeBytesThenTail_64Bits()
    {
        Span<byte> buf = stackalloc byte[12];
        var w = new QwpBitWriter(buf, 0);
        w.WriteBits(0b101UL, 3);
        w.WriteBits(0xFEDCBA9876543210UL, 64);
        w.WriteBits(0b1011, 4);
        var written = w.FinishToByteBoundary();

        var r = new QwpBitReader(buf, 0);
        Assert.That(r.ReadBits(3), Is.EqualTo(0b101UL));
        Assert.That(r.ReadBits(64), Is.EqualTo(0xFEDCBA9876543210UL));
        Assert.That(r.ReadBits(4), Is.EqualTo(0b1011UL));
        Assert.That(written, Is.EqualTo((3 + 64 + 4 + 7) / 8));
    }

    [Test]
    public void WriteBits_HigherBitsTruncated()
    {
        Span<byte> buf = stackalloc byte[1];
        var w = new QwpBitWriter(buf, 0);
        w.WriteBits(0xFFUL, 4);

        var r = new QwpBitReader(buf, 0);
        Assert.That(r.ReadBits(4), Is.EqualTo(0x0FUL));
    }

    [Test]
    public void WriteBits_ExhaustedBuffer_Throws()
    {
        var buf = new byte[1];
        var w = new QwpBitWriter(buf, 0);
        w.WriteBits(0, 8);

        InvalidOperationException? thrown = null;
        try { w.WriteBits(0, 1); }
        catch (InvalidOperationException ex) { thrown = ex; }
        Assert.That(thrown, Is.Not.Null);
    }

    [Test]
    public void RoundTrip_FuzzedRandomWidths()
    {
        var rnd = new Random(0xBEEF);
        for (var trial = 0; trial < 200; trial++)
        {
            var widths = new int[rnd.Next(1, 64)];
            var values = new ulong[widths.Length];
            var totalBits = 0;
            for (var i = 0; i < widths.Length; i++)
            {
                var w = rnd.Next(1, 65);
                widths[i] = w;
                var v = ((ulong)rnd.NextInt64() << 32) ^ (uint)rnd.Next();
                values[i] = w == 64 ? v : v & ((1UL << w) - 1UL);
                totalBits += w;
            }

            var buf = new byte[(totalBits + 7) / 8 + 1];
            var writer = new QwpBitWriter(buf, 0);
            for (var i = 0; i < widths.Length; i++)
            {
                writer.WriteBits(values[i], widths[i]);
            }

            var reader = new QwpBitReader(buf, 0);
            for (var i = 0; i < widths.Length; i++)
            {
                Assert.That(reader.ReadBits(widths[i]), Is.EqualTo(values[i]),
                    $"trial={trial} idx={i} width={widths[i]}");
            }
        }
    }

    [Test]
    public void WriteBits_NonZeroStartOffset_PreservesPreceding()
    {
        Span<byte> buf = stackalloc byte[4];
        buf[0] = 0xAA;
        var w = new QwpBitWriter(buf, 1);
        w.WriteBits(0xBEEFUL, 16);

        Assert.That(buf[0], Is.EqualTo((byte)0xAA), "offset preceding the writer must be untouched");

        var r = new QwpBitReader(buf, 1);
        Assert.That(r.ReadBits(16), Is.EqualTo(0xBEEFUL));
    }
}
