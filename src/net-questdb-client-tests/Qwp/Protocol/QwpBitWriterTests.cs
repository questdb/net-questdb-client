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
///     Mirrors <c>QwpBitWriterTest.java</c> on Java main 64b7ee69. The .NET writer accepts
///     a managed <see cref="byte"/> array slice rather than a raw native pointer; the
///     overflow / round-trip semantics are identical.
/// </summary>
[TestFixture]
public class QwpBitWriterTests
{
    [Test]
    public void FlushThrowsOnOverflow()
    {
        var buf = new byte[1];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteBits(0xFF, 8);
        writer.WriteBits(0x3, 4); // bits sit in the bit buffer
        Assert.That(() => writer.Flush(),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void WriteBitsThrowsOnOverflow()
    {
        var buf = new byte[4];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteBits(unchecked((int)0xFFFF_FFFF), 32); // fills the buffer
        Assert.That(() => writer.WriteBits(1, 8),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void WriteBitsWithinCapacitySucceeds()
    {
        var buf = new byte[8];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        const long expected = unchecked((long)0xDEAD_BEEF_CAFE_BABEUL);
        writer.WriteBits(expected, 64);
        writer.Flush();
        Assert.That(writer.BytesWritten, Is.EqualTo(8));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(buf), Is.EqualTo(expected));
    }

    [Test]
    public void WriteByteThrowsOnOverflow()
    {
        var buf = new byte[1];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteByte(0x42);
        Assert.That(() => writer.WriteByte(0x43),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void WriteIntThrowsOnOverflow()
    {
        var buf = new byte[4];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteInt(42);
        Assert.That(() => writer.WriteInt(99),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void WriteLongThrowsOnOverflow()
    {
        var buf = new byte[8];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteLong(42L);
        Assert.That(() => writer.WriteLong(99L),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void FinishReturnsBytesWritten()
    {
        var buf = new byte[16];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteByte(0x01);
        writer.WriteInt(0x02030405);
        writer.WriteLong(0x06070809_0A0B0C0DL);
        Assert.That(writer.Finish(), Is.EqualTo(13));
    }

    [Test]
    public void AlignToByteIsNoopWhenAlreadyAligned()
    {
        var buf = new byte[4];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteByte(0xAA);
        writer.AlignToByte();
        Assert.That(writer.BytesWritten, Is.EqualTo(1));
    }

    [Test]
    public void AlignToByteFlushesPartialBits()
    {
        var buf = new byte[4];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteBits(0b101, 3);
        writer.AlignToByte();
        Assert.That(writer.BytesWritten, Is.EqualTo(1));
        Assert.That(buf[0], Is.EqualTo((byte)0b101));
    }

    [Test]
    public void WriteBitArbitraryNumBitsRoundTripsLsbFirst()
    {
        // Verifies LSB-first packing: writing 4 bits 0b1101 then 4 bits 0b0010 yields
        // a single byte 0x2D = 0b0010_1101.
        var buf = new byte[2];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        writer.WriteBits(0b1101, 4);
        writer.WriteBits(0b0010, 4);
        writer.Flush();
        Assert.That(buf[0], Is.EqualTo((byte)0x2D));
    }

    [Test]
    public void WriteBitsRejectsZeroAndNegative()
    {
        var buf = new byte[8];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 0, buf.Length);
        Assert.That(() => writer.WriteBits(0, 0),
                    Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => writer.WriteBits(0, 65),
                    Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ResetWithOffsetWritesIntoSlice()
    {
        // The writer should respect the offset + capacity — bytes outside the slice are not touched.
        var buf = new byte[10];
        var writer = new QwpBitWriter();
        writer.Reset(buf, 2, 4);
        writer.WriteInt(unchecked((int)0xCAFE_BABE));
        Assert.That(writer.BytesWritten, Is.EqualTo(4));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(buf.AsSpan(2, 4)),
                    Is.EqualTo(unchecked((int)0xCAFE_BABE)));
        // Bytes outside the slice remain zero.
        Assert.That(buf[0], Is.EqualTo((byte)0));
        Assert.That(buf[1], Is.EqualTo((byte)0));
        Assert.That(buf[6], Is.EqualTo((byte)0));
    }

    [Test]
    public void ResetRejectsOutOfRangeOffsetOrCapacity()
    {
        var buf = new byte[4];
        var writer = new QwpBitWriter();
        Assert.That(() => writer.Reset(buf, 5, 0),
                    Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => writer.Reset(buf, 2, 5),
                    Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => writer.Reset(null!, 0, 0),
                    Throws.TypeOf<ArgumentNullException>());
    }

    // ---- Cross-component coverage: bit writer used by QwpGorillaEncoder ----

    [Test]
    public void GorillaEncoderThrowsOnInsufficientCapacityForFirstTimestamp()
    {
        var encoder = new QwpGorillaEncoder();
        var dst = new byte[4]; // < 8 bytes for the first timestamp
        Assert.That(() => encoder.EncodeTimestamps(dst, 0, new long[] { 1000L }),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }

    [Test]
    public void GorillaEncoderThrowsOnInsufficientCapacityForSecondTimestamp()
    {
        var encoder = new QwpGorillaEncoder();
        var dst = new byte[12]; // < 16 for both seed timestamps
        Assert.That(() => encoder.EncodeTimestamps(dst, 0, new long[] { 1000L, 2000L }),
                    Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
    }
}
