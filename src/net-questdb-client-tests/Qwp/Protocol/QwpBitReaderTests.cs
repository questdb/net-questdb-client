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
///     Mirrors <c>QwpBitReaderTest.java</c> on Java main 64b7ee69. The .NET reader accepts
///     a <see cref="System.ReadOnlyMemory{T}"/> over <see cref="byte"/> rather than a raw
///     native pointer; the LSB-first / sign-extension / past-end semantics are identical.
///     <see cref="QwpBitReader.ReadBits(int)"/> with numBits &gt; 64 raises an
///     <see cref="System.ArgumentOutOfRangeException"/> instead of Java's <c>AssertionError</c>
///     — the .NET-idiomatic spelling for "caller passed a guaranteed-bad arg".
/// </summary>
[TestFixture]
public class QwpBitReaderTests
{
    [Test]
    public void ReadBitPastEndThrows()
    {
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0xFF });
        for (var i = 0; i < 8; i++) reader.ReadBit();
        Assert.That(() => reader.ReadBit(),
                    Throws.TypeOf<QwpDecodeException>().With.Message.Contains("read past end"));
    }

    [Test]
    public void ReadBitYieldsLsbFirstAcrossMultipleBytes()
    {
        // byte 0b1010_0001 -> 1,0,0,0,0,1,0,1
        // byte 0b0000_0010 -> 0,1,0,0,0,0,0,0
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0b10100001, 0b00000010 });
        int[] expected = { 1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0 };
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.That(reader.ReadBit(), Is.EqualTo(expected[i]), $"bit {i}");
            Assert.That(reader.BitPosition, Is.EqualTo((long)i + 1));
        }
    }

    [Test]
    public void ReadBits64ReadsFullWord()
    {
        const long value = 0x0123456789ABCDEFL;
        var bytes = new byte[8];
        for (var i = 0; i < 8; i++) bytes[i] = (byte)((value >> (8 * i)) & 0xFF);

        var reader = new QwpBitReader();
        reader.Reset(bytes);
        Assert.That(reader.ReadBits(64), Is.EqualTo(value));
        Assert.That(reader.BitPosition, Is.EqualTo(64L));
    }

    [Test]
    public void ReadBits64TwiceDoesNotLeakStaleBuffer()
    {
        // Regression: `ulong >>= 64` is a no-op in C# (shift count masked to 6 bits ⇒ 0).
        // After the first all-ones word the bit buffer must be cleared explicitly,
        // otherwise the second 64-bit read OR-fills onto stale all-ones and reports -1
        // instead of 0.
        var bytes = new byte[16];
        for (var i = 0; i < 8; i++) bytes[i] = 0xFF;
        for (var i = 8; i < 16; i++) bytes[i] = 0x00;

        var reader = new QwpBitReader();
        reader.Reset(bytes);

        Assert.That(reader.ReadBits(64), Is.EqualTo(-1L));
        Assert.That(reader.BitPosition, Is.EqualTo(64L));
        Assert.That(reader.ReadBits(64), Is.EqualTo(0L),
                    "second readBits(64) must reflect the bytes that follow, not stale buffer");
        Assert.That(reader.BitPosition, Is.EqualTo(128L));
    }

    [Test]
    public void ReadBitsAcrossLargeRefill()
    {
        // 16 bytes; arbitrary widths summing to 128 bits; verify position tracking and
        // that the refill loop terminates cleanly. After exhaustion the next ReadBit throws.
        var bytes = new byte[16];
        for (var i = 0; i < 16; i++) bytes[i] = (byte)(i & 0xFF);
        int[] widths = { 1, 7, 13, 19, 23, 33, 32 }; // sums to 128

        var reader = new QwpBitReader();
        reader.Reset(bytes);
        long total = 0;
        foreach (var w in widths)
        {
            reader.ReadBits(w);
            total += w;
            Assert.That(reader.BitPosition, Is.EqualTo(total));
        }
        Assert.That(() => reader.ReadBit(), Throws.TypeOf<QwpDecodeException>());
    }

    [Test]
    public void ReadBitsArbitraryWidths()
    {
        // 0xFF 0x55 0xAA 0x00 -- mixed pattern, 32 bits total.
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0xFF, 0x55, 0xAA, 0x00 });

        Assert.That(reader.ReadBits(5), Is.EqualTo(0b11111L));
        Assert.That(reader.ReadBits(3), Is.EqualTo(0b111L));
        Assert.That(reader.BitPosition, Is.EqualTo(8L));
        Assert.That(reader.ReadBits(8), Is.EqualTo(0x55L));
        Assert.That(reader.BitPosition, Is.EqualTo(16L));
        // LSB-first: byte 0xAA is bits 0-7 of result, byte 0x00 is bits 8-15.
        Assert.That(reader.ReadBits(16), Is.EqualTo(0x00AAL));
        Assert.That(reader.BitPosition, Is.EqualTo(32L));
    }

    [Test]
    public void ReadBitsMoreThan64Throws()
    {
        var reader = new QwpBitReader();
        reader.Reset(new byte[16]);
        Assert.That(() => reader.ReadBits(65),
                    Throws.TypeOf<ArgumentOutOfRangeException>().With.Message.Contains("64"));
    }

    [Test]
    public void ReadBitsPastEndThrows()
    {
        // 1 byte = 8 bits available. Asking for 9 must throw before any state change.
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0xFF });
        Assert.That(() => reader.ReadBits(9),
                    Throws.TypeOf<QwpDecodeException>().With.Message.Contains("read past end"));
        Assert.That(reader.BitPosition, Is.EqualTo(0L));
    }

    [Test]
    public void ReadBitsSpansBufferRefills()
    {
        // 24-bit read forces the inner refill loop to walk multiple boundaries.
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0x01, 0x02, 0x03, 0x00 });
        // LSB-first: 0x01 | (0x02 << 8) | (0x03 << 16) = 0x030201
        Assert.That(reader.ReadBits(24), Is.EqualTo(0x030201L));
        Assert.That(reader.BitPosition, Is.EqualTo(24L));
    }

    [Test]
    public void ReadBitsZeroBitsReturnsZeroWithoutAdvancing()
    {
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0xFF });
        Assert.That(reader.ReadBits(0), Is.EqualTo(0L));
        Assert.That(reader.BitPosition, Is.EqualTo(0L));
        // Subsequent read still sees the byte intact.
        Assert.That(reader.ReadBit(), Is.EqualTo(1));
    }

    [Test]
    public void ReadSigned64BitsBehavesLikeReadBits()
    {
        // numBits == 64 skips the sign-extend branch (value already fills the long).
        const ulong u = 0xFFEEDDCCBBAA9988UL;
        const long value = unchecked((long)u);
        var bytes = new byte[8];
        for (var i = 0; i < 8; i++) bytes[i] = (byte)(u >> (8 * i));

        var reader = new QwpBitReader();
        reader.Reset(bytes);
        Assert.That(reader.ReadSigned(64), Is.EqualTo(value));
    }

    [Test]
    public void ReadSignedDoesNotExtendWhenMsbClear()
    {
        // +5 in 5 bits (0b00101). MSB clear -> no sign extension.
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0b00000101 });
        Assert.That(reader.ReadSigned(5), Is.EqualTo(5L));
    }

    [Test]
    public void ReadSignedExtendsWhenMsbSet()
    {
        // -1 in 5 bits (0b11111). Sign-extend yields -1L.
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0b00011111 });
        Assert.That(reader.ReadSigned(5), Is.EqualTo(-1L));
    }

    [Test]
    public void ResetClearsAllState()
    {
        var reader = new QwpBitReader();
        reader.Reset(new byte[] { 0xAB, 0xCD });
        reader.ReadBits(10);
        Assert.That(reader.BitPosition, Is.EqualTo(10L));

        // Reset to a fresh buffer; position drops back to 0 and the first read must
        // come from the new buffer, not the leftover bit buffer.
        reader.Reset(new byte[] { 0x12, 0x34 });
        Assert.That(reader.BitPosition, Is.EqualTo(0L));
        Assert.That(reader.ReadBits(8), Is.EqualTo(0x12L));
        Assert.That(reader.BitPosition, Is.EqualTo(8L));
    }
}
