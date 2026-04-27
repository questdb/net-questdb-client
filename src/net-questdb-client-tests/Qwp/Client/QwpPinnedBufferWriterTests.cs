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

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Mirrors <c>NativeBufferWriterTest.java</c> on Java main 64b7ee69. The Java
///     implementation is renamed to <c>QwpPinnedBufferWriter</c> in .NET — backed by a
///     POH-pinned managed array rather than off-heap native memory. The wire-format /
///     varint / UTF-8 / patch / skip semantics are identical.
/// </summary>
[TestFixture]
public class QwpPinnedBufferWriterTests
{
    [Test]
    public void EnsureCapacityGrowsBuffer()
    {
        var writer = new QwpPinnedBufferWriter(16);
        Assert.That(writer.Capacity, Is.EqualTo(16));
        writer.EnsureCapacity(32);
        Assert.That(writer.Capacity, Is.GreaterThanOrEqualTo(32));
    }

    [Test]
    public void GetWritableSpanReflectsPositionAndShrinksWithWrites()
    {
        var writer = new QwpPinnedBufferWriter(64);
        Assert.That(writer.GetWritableSpan().Length, Is.EqualTo(64));
        Assert.That(writer.WritableBytes, Is.EqualTo(64));

        writer.PutInt(42);
        writer.PutLong(123L);
        Assert.That(writer.Position, Is.EqualTo(12));
        Assert.That(writer.WritableBytes, Is.EqualTo(writer.Capacity - 12));

        writer.EnsureCapacity(100);
        Assert.That(writer.WritableBytes, Is.EqualTo(writer.Capacity - 12));

        // Direct write via GetWritableSpan + Skip — caller must advance the position
        // manually after writing into the returned span.
        BinaryPrimitives.WriteInt64LittleEndian(writer.GetWritableSpan(), 999L);
        writer.Skip(8);
        Assert.That(writer.Position, Is.EqualTo(20));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(writer.AsReadOnlySpan().Slice(12, 8)),
                    Is.EqualTo(999L));
    }

    [Test]
    public void GrowBuffer()
    {
        var writer = new QwpPinnedBufferWriter(16);
        for (var i = 0; i < 100; i++) writer.PutLong(i);
        Assert.That(writer.Position, Is.EqualTo(800));
        var span = writer.AsReadOnlySpan();
        for (var i = 0; i < 100; i++)
        {
            Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(i * 8, 8)),
                        Is.EqualTo((long)i));
        }
    }

    [Test]
    public void MultipleWritesAssembleQwp1Header()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutByte((byte)'Q');
        writer.PutByte((byte)'W');
        writer.PutByte((byte)'P');
        writer.PutByte((byte)'1');
        writer.PutByte(1);          // Version
        writer.PutByte(0);          // Flags
        writer.PutShort(1);         // Table count
        writer.PutInt(0);           // Payload length placeholder

        Assert.That(writer.Position, Is.EqualTo(12));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)'Q'));
        Assert.That(span[1], Is.EqualTo((byte)'W'));
        Assert.That(span[2], Is.EqualTo((byte)'P'));
        Assert.That(span[3], Is.EqualTo((byte)'1'));
    }

    [Test]
    public void Utf8LengthHandlesInvalidSurrogatePair()
    {
        // High surrogate followed by non-low-surrogate: '?' (1) + 'X' (1) = 2
        Assert.That(QwpPinnedBufferWriter.Utf8Length("\uD800X"), Is.EqualTo(2));
        // Lone high surrogate at end -> '?' (1)
        Assert.That(QwpPinnedBufferWriter.Utf8Length("\uD800"), Is.EqualTo(1));
        // Lone low surrogate -> '?' (1)
        Assert.That(QwpPinnedBufferWriter.Utf8Length("\uDC00"), Is.EqualTo(1));
        // Valid pair -> 4 bytes
        Assert.That(QwpPinnedBufferWriter.Utf8Length("😀"), Is.EqualTo(4));
    }

    [Test]
    public void PatchInt()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutInt(0);   // placeholder at offset 0
        writer.PutInt(100); // at offset 4
        writer.PatchInt(0, 42);
        var span = writer.AsReadOnlySpan();
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(0, 4)), Is.EqualTo(42));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4, 4)), Is.EqualTo(100));
    }

    [Test]
    public void PatchIntAtLastValidOffset()
    {
        var writer = new QwpPinnedBufferWriter(16);
        writer.PutLong(0L); // 8 bytes, position = 8
        // offset 4 covers bytes [4..7], boundary case — must be allowed
        writer.PatchInt(4, 0x1234);
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(writer.AsReadOnlySpan().Slice(4, 4)),
                    Is.EqualTo(0x1234));
    }

    [Test]
    public void PatchIntAtValidOffset()
    {
        var writer = new QwpPinnedBufferWriter(16);
        writer.PutInt(0);          // placeholder at offset 0
        writer.PutInt(unchecked((int)0xBEEF));
        writer.PatchInt(0, unchecked((int)0xCAFE));
        var span = writer.AsReadOnlySpan();
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(0, 4)),
                    Is.EqualTo(unchecked((int)0xCAFE)));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4, 4)),
                    Is.EqualTo(unchecked((int)0xBEEF)));
    }

    [Test]
    public void PatchIntRejectsOutOfRangeOffset()
    {
        var writer = new QwpPinnedBufferWriter(16);
        writer.PutLong(0L); // position = 8

        Assert.That(() => writer.PatchInt(-1, 0),
                    Throws.TypeOf<ArgumentOutOfRangeException>());
        // Offset that would write past the written region (5..8 vs position 8 ⇒ 5+4=9 > 8).
        Assert.That(() => writer.PatchInt(5, 0),
                    Throws.TypeOf<ArgumentOutOfRangeException>());
        // Offset at the last valid 4-byte slot is allowed.
        Assert.That(() => writer.PatchInt(4, 0x1234), Throws.Nothing);
    }

    [Test]
    public void PutBlockOfBytesCopiesIntoBuffer()
    {
        var writer = new QwpPinnedBufferWriter();
        var source = new byte[] { 1, 2, 3, 4 };
        writer.PutBlockOfBytes(source);
        Assert.That(writer.Position, Is.EqualTo(4));
        Assert.That(writer.AsReadOnlySpan().SequenceEqual(source), Is.True);
    }

    [Test]
    public void PutBlockOfBytesEmptyIsNoOp()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutBlockOfBytes(ReadOnlyMemory<byte>.Empty);
        Assert.That(writer.Position, Is.EqualTo(0));
    }

    [Test]
    public void PutUtf8InvalidSurrogatePair()
    {
        var writer = new QwpPinnedBufferWriter(64);
        writer.PutUtf8("\uD800X"); // lone high surrogate -> '?', then 'X'
        Assert.That(writer.Position, Is.EqualTo(2));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)'?'));
        Assert.That(span[1], Is.EqualTo((byte)'X'));
    }

    [Test]
    public void PutUtf8LoneHighSurrogateAtEnd()
    {
        var writer = new QwpPinnedBufferWriter(64);
        writer.PutUtf8("\uD800");
        Assert.That(writer.Position, Is.EqualTo(1));
        Assert.That(writer.AsReadOnlySpan()[0], Is.EqualTo((byte)'?'));
    }

    [Test]
    public void PutUtf8LoneLowSurrogate()
    {
        var writer = new QwpPinnedBufferWriter(64);
        writer.PutUtf8("\uDC00");
        Assert.That(writer.Position, Is.EqualTo(1));
        Assert.That(writer.AsReadOnlySpan()[0], Is.EqualTo((byte)'?'));
    }

    [Test]
    public void PutUtf8LoneSurrogateMatchesUtf8Length()
    {
        var writer = new QwpPinnedBufferWriter(64);
        var cases = new[] { "\uD800", "\uDBFF", "\uDC00", "\uDFFF", "\uD800X", "A\uDC00B" };
        foreach (var s in cases)
        {
            writer.Reset();
            writer.PutUtf8(s);
            Assert.That(writer.Position, Is.EqualTo(QwpPinnedBufferWriter.Utf8Length(s)),
                        $"length mismatch for {s.Length} char input");
        }
    }

    [Test]
    public void Reset()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutInt(12345);
        Assert.That(writer.Position, Is.EqualTo(4));
        writer.Reset();
        Assert.That(writer.Position, Is.EqualTo(0));
        writer.PutByte(0xFF);
        Assert.That(writer.Position, Is.EqualTo(1));
    }

    [Test]
    public void SkipAdvancesPosition()
    {
        var writer = new QwpPinnedBufferWriter(16);
        writer.Skip(4);
        Assert.That(writer.Position, Is.EqualTo(4));
        writer.Skip(8);
        Assert.That(writer.Position, Is.EqualTo(12));
    }

    [Test]
    public void SkipBeyondCapacityGrowsBuffer()
    {
        var writer = new QwpPinnedBufferWriter(16);
        writer.Skip(32); // skip past initial 16-byte capacity — must grow
        Assert.That(writer.Position, Is.EqualTo(32));
        Assert.That(writer.Capacity, Is.GreaterThanOrEqualTo(32));
        writer.PutInt(unchecked((int)0xCAFE));
        Assert.That(writer.Position, Is.EqualTo(36));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(writer.AsReadOnlySpan().Slice(32, 4)),
                    Is.EqualTo(unchecked((int)0xCAFE)));
    }

    [Test]
    public void SkipThenPatchInt()
    {
        var writer = new QwpPinnedBufferWriter(8);
        var patchOffset = writer.Position;
        writer.Skip(4);              // reserve space for a length field
        writer.PutInt(unchecked((int)0xDEAD));
        writer.PatchInt(patchOffset, 4);
        var span = writer.AsReadOnlySpan();
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(patchOffset, 4)),
                    Is.EqualTo(4));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4, 4)),
                    Is.EqualTo(unchecked((int)0xDEAD)));
    }

    [Test]
    public void Utf8LengthHandlesAsciiAndMultiByte()
    {
        Assert.That(QwpPinnedBufferWriter.Utf8Length(null), Is.EqualTo(0));
        Assert.That(QwpPinnedBufferWriter.Utf8Length(""), Is.EqualTo(0));
        Assert.That(QwpPinnedBufferWriter.Utf8Length("hello"), Is.EqualTo(5));
        Assert.That(QwpPinnedBufferWriter.Utf8Length("ñ"), Is.EqualTo(2));
        Assert.That(QwpPinnedBufferWriter.Utf8Length("€"), Is.EqualTo(3));
    }

    [Test]
    public void VarintSizeMatchesEncodedLength()
    {
        var writer = new QwpPinnedBufferWriter();
        long[] values = { 0, 1, 127, 128, 16383, 16384, 2_097_151, 2_097_152, long.MaxValue };
        foreach (var value in values)
        {
            writer.Reset();
            writer.PutVarint(value);
            Assert.That(writer.Position, Is.EqualTo(QwpPinnedBufferWriter.VarintSize(value)),
                        $"value={value}");
        }
    }

    [Test]
    public void WriteByte()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutByte(0x42);
        Assert.That(writer.Position, Is.EqualTo(1));
        Assert.That(writer.AsReadOnlySpan()[0], Is.EqualTo((byte)0x42));
    }

    [Test]
    public void WriteDouble()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutDouble(3.14159265359);
        Assert.That(writer.Position, Is.EqualTo(8));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(writer.AsReadOnlySpan()),
                    Is.EqualTo(3.14159265359).Within(1e-10));
    }

    [Test]
    public void WriteEmptyString()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutString("");
        Assert.That(writer.Position, Is.EqualTo(1));
        Assert.That(writer.AsReadOnlySpan()[0], Is.EqualTo((byte)0));
    }

    [Test]
    public void WriteFloat()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutFloat(3.14f);
        Assert.That(writer.Position, Is.EqualTo(4));
        Assert.That(BinaryPrimitives.ReadSingleLittleEndian(writer.AsReadOnlySpan()),
                    Is.EqualTo(3.14f).Within(0.0001f));
    }

    [Test]
    public void WriteInt()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutInt(0x12345678);
        Assert.That(writer.Position, Is.EqualTo(4));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(writer.AsReadOnlySpan()),
                    Is.EqualTo(0x12345678));
    }

    [Test]
    public void WriteLong()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutLong(0x123456789ABCDEF0L);
        Assert.That(writer.Position, Is.EqualTo(8));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(writer.AsReadOnlySpan()),
                    Is.EqualTo(0x123456789ABCDEF0L));
    }

    [Test]
    public void WriteNullString()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutString(null);
        Assert.That(writer.Position, Is.EqualTo(1));
        Assert.That(writer.AsReadOnlySpan()[0], Is.EqualTo((byte)0));
    }

    [Test]
    public void WriteShort()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutShort(0x1234);
        Assert.That(writer.Position, Is.EqualTo(2));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)0x34));
        Assert.That(span[1], Is.EqualTo((byte)0x12));
    }

    [Test]
    public void WriteString()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutString("hello");
        Assert.That(writer.Position, Is.EqualTo(6)); // 1-byte varint + 5 chars
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)5));
        Assert.That(span[1], Is.EqualTo((byte)'h'));
        Assert.That(span[2], Is.EqualTo((byte)'e'));
        Assert.That(span[3], Is.EqualTo((byte)'l'));
        Assert.That(span[4], Is.EqualTo((byte)'l'));
        Assert.That(span[5], Is.EqualTo((byte)'o'));
    }

    [Test]
    public void WriteStringMixedAsciiAndNonAscii()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutString("hi€");
        const int utf8Len = 5; // 'h'(1) + 'i'(1) + '€'(3)
        Assert.That(writer.Position, Is.EqualTo(1 + utf8Len));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)utf8Len));
        Assert.That(span[1], Is.EqualTo((byte)'h'));
        Assert.That(span[2], Is.EqualTo((byte)'i'));
        Assert.That(span[3], Is.EqualTo((byte)0xE2));
        Assert.That(span[4], Is.EqualTo((byte)0x82));
        Assert.That(span[5], Is.EqualTo((byte)0xAC));
    }

    [Test]
    public void WriteStringNonAscii()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutString("€ñ"); // 3 + 2 = 5 UTF-8 bytes
        Assert.That(writer.Position, Is.EqualTo(1 + 5));
        Assert.That(writer.AsReadOnlySpan()[0], Is.EqualTo((byte)5));
    }

    [Test]
    public void WriteUtf8Ascii()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutUtf8("ABC");
        Assert.That(writer.Position, Is.EqualTo(3));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)'A'));
        Assert.That(span[1], Is.EqualTo((byte)'B'));
        Assert.That(span[2], Is.EqualTo((byte)'C'));
    }

    [Test]
    public void WriteUtf8MixedAsciiAndNonAscii()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutUtf8("abc€def"); // 9 UTF-8 bytes
        Assert.That(writer.Position, Is.EqualTo(9));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)'a'));
        Assert.That(span[3], Is.EqualTo((byte)0xE2));
        Assert.That(span[4], Is.EqualTo((byte)0x82));
        Assert.That(span[5], Is.EqualTo((byte)0xAC));
        Assert.That(span[6], Is.EqualTo((byte)'d'));
    }

    [Test]
    public void WriteUtf8MixedAsciiAndNonAsciiAfterGrow()
    {
        var writer = new QwpPinnedBufferWriter(8); // tiny initial capacity to force grow
        const string value = "abcdefghijklmnop世界世界世界世界世界世界世界世界世界世界";
        writer.PutUtf8(value);
        var utf8Len = QwpPinnedBufferWriter.Utf8Length(value);
        Assert.That(writer.Position, Is.EqualTo(utf8Len));
        var roundTripped = System.Text.Encoding.UTF8.GetString(writer.AsReadOnlySpan());
        Assert.That(roundTripped, Is.EqualTo(value));
    }

    [Test]
    public void WriteUtf8ThreeByte()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutUtf8("€"); // 0xE2 0x82 0xAC
        Assert.That(writer.Position, Is.EqualTo(3));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)0xE2));
        Assert.That(span[1], Is.EqualTo((byte)0x82));
        Assert.That(span[2], Is.EqualTo((byte)0xAC));
    }

    [Test]
    public void WriteUtf8TwoByte()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutUtf8("ñ"); // 0xC3 0xB1
        Assert.That(writer.Position, Is.EqualTo(2));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)0xC3));
        Assert.That(span[1], Is.EqualTo((byte)0xB1));
    }

    [Test]
    public void WriteVarintLarge()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutVarint(16384);
        Assert.That(writer.Position, Is.EqualTo(3));
        // LEB128: 16384 = 0x80 0x80 0x01
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)0x80));
        Assert.That(span[1], Is.EqualTo((byte)0x80));
        Assert.That(span[2], Is.EqualTo((byte)0x01));
    }

    [Test]
    public void WriteVarintMedium()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutVarint(128);
        Assert.That(writer.Position, Is.EqualTo(2));
        var span = writer.AsReadOnlySpan();
        Assert.That(span[0], Is.EqualTo((byte)0x80));
        Assert.That(span[1], Is.EqualTo((byte)0x01));
    }

    [Test]
    public void WriteVarintSmall()
    {
        var writer = new QwpPinnedBufferWriter();
        writer.PutVarint(127);
        Assert.That(writer.Position, Is.EqualTo(1));
        Assert.That(writer.AsReadOnlySpan()[0], Is.EqualTo((byte)127));
    }
}
