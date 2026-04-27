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

using System.Runtime.InteropServices;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Protocol;

/// <summary>
///     Mirrors <c>QwpBitWriterTest.java</c> on Java main 64b7ee69. .NET allocates the test buffer
///     via <see cref="Marshal"/> rather than <c>Unsafe.malloc</c>; semantically equivalent.
/// </summary>
[TestFixture]
public class QwpBitWriterTests
{
    private static nint AllocBuffer(long bytes) => Marshal.AllocHGlobal((nint)bytes);
    private static void FreeBuffer(nint addr) => Marshal.FreeHGlobal(addr);

    [Test]
    public void FlushThrowsOnOverflow()
    {
        var ptr = AllocBuffer(1);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 1);
            writer.WriteBits(0xFF, 8);
            writer.WriteBits(0x3, 4); // bits sit in the bit buffer
            Assert.That(() => writer.Flush(),
                        Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void WriteBitsThrowsOnOverflow()
    {
        var ptr = AllocBuffer(4);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 4);
            writer.WriteBits(unchecked((int)0xFFFF_FFFF), 32); // fills the buffer
            Assert.That(() => writer.WriteBits(1, 8),
                        Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void WriteBitsWithinCapacitySucceeds()
    {
        var ptr = AllocBuffer(8);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 8);
            const long expected = unchecked((long)0xDEAD_BEEF_CAFE_BABEUL);
            writer.WriteBits(expected, 64);
            writer.Flush();
            Assert.That((long)(writer.Position - ptr), Is.EqualTo(8));
            Assert.That(Marshal.ReadInt64(ptr), Is.EqualTo(expected));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void WriteByteThrowsOnOverflow()
    {
        var ptr = AllocBuffer(1);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 1);
            writer.WriteByte(0x42);
            Assert.That(() => writer.WriteByte(0x43),
                        Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void WriteIntThrowsOnOverflow()
    {
        var ptr = AllocBuffer(4);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 4);
            writer.WriteInt(42);
            Assert.That(() => writer.WriteInt(99),
                        Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void WriteLongThrowsOnOverflow()
    {
        var ptr = AllocBuffer(8);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 8);
            writer.WriteLong(42L);
            Assert.That(() => writer.WriteLong(99L),
                        Throws.TypeOf<IngressError>().With.Message.Contains("buffer overflow"));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void FinishReturnsBytesWritten()
    {
        var ptr = AllocBuffer(16);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 16);
            writer.WriteByte(0x01);
            writer.WriteInt(0x02030405);
            writer.WriteLong(0x06070809_0A0B0C0DL);
            Assert.That(writer.Finish(), Is.EqualTo(13));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void AlignToByteIsNoopWhenAlreadyAligned()
    {
        var ptr = AllocBuffer(4);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 4);
            writer.WriteByte(0xAA);
            writer.AlignToByte(); // already aligned, must not advance
            Assert.That((long)(writer.Position - ptr), Is.EqualTo(1));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void AlignToByteFlushesPartialBits()
    {
        var ptr = AllocBuffer(4);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 4);
            writer.WriteBits(0b101, 3);
            writer.AlignToByte();
            Assert.That((long)(writer.Position - ptr), Is.EqualTo(1));
            Assert.That(Marshal.ReadByte(ptr), Is.EqualTo((byte)0b101));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void WriteBitArbitraryNumBitsRoundTripsLsbFirst()
    {
        // Verifies LSB-first packing: writing 4 bits 0b1101 (=13) emits the byte 0x0D
        // when followed by a flush.
        var ptr = AllocBuffer(2);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 2);
            writer.WriteBits(0b1101, 4);
            writer.WriteBits(0b0010, 4);
            writer.Flush();
            // Bits are packed LSB-first: first 4 bits → low nibble; next 4 bits → high nibble.
            // 0b1101 (low) | 0b0010 << 4 (high) = 0x2D
            Assert.That(Marshal.ReadByte(ptr), Is.EqualTo((byte)0x2D));
        }
        finally { FreeBuffer(ptr); }
    }

    [Test]
    public void WriteBitsRejectsZeroAndNegative()
    {
        var ptr = AllocBuffer(8);
        try
        {
            var writer = new QwpBitWriter();
            writer.Reset(ptr, 8);
            Assert.That(() => writer.WriteBits(0, 0),
                        Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => writer.WriteBits(0, 65),
                        Throws.TypeOf<ArgumentOutOfRangeException>());
        }
        finally { FreeBuffer(ptr); }
    }

    // ---- Pending: needs QwpGorillaEncoder which lands in PR 2 ----

    [Test]
    public void GorillaEncoderThrowsOnInsufficientCapacityForFirstTimestamp()
    {
        Assert.Inconclusive("Awaiting PR 2: QwpGorillaEncoder.");
    }

    [Test]
    public void GorillaEncoderThrowsOnInsufficientCapacityForSecondTimestamp()
    {
        Assert.Inconclusive("Awaiting PR 2: QwpGorillaEncoder.");
    }
}
