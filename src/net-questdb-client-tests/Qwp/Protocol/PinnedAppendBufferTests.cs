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

namespace net_questdb_client_tests.Qwp.Protocol;

/// <summary>
///     Mirrors <c>OffHeapAppendMemoryTest.java</c> on Java main 64b7ee69. .NET uses
///     pinned managed arrays (POH) instead of native memory, so the Java leak-accounting
///     tests are replaced with span-based round-trip checks.
/// </summary>
[TestFixture]
public class PinnedAppendBufferTests
{
    [Test]
    public void Growth()
    {
        var mem = new PinnedAppendBuffer(8);
        for (var i = 0; i < 100; i++)
        {
            mem.PutLong(i);
        }

        Assert.That(mem.Length, Is.EqualTo(800));
        var span = mem.AsReadOnlySpan(0, 800);
        for (var i = 0; i < 100; i++)
        {
            Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(i * 8, 8)),
                        Is.EqualTo((long)i));
        }
    }

    [Test]
    public void JumpTo()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutLong(100);
        mem.PutLong(200);
        mem.PutLong(300);
        Assert.That(mem.Length, Is.EqualTo(24));

        mem.JumpTo(8);
        Assert.That(mem.Length, Is.EqualTo(8));

        mem.PutLong(999);
        Assert.That(mem.Length, Is.EqualTo(16));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(mem.AsReadOnlySpan(0, 8)), Is.EqualTo(100L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(mem.AsReadOnlySpan(8, 8)), Is.EqualTo(999L));
    }

    [Test]
    public void JumpToRejectsOutOfRange()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutLong(42);
        Assert.That(() => mem.JumpTo(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => mem.JumpTo(9), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void LargeGrowth()
    {
        var mem = new PinnedAppendBuffer(8);
        for (var i = 0; i < 10_000; i++)
        {
            mem.PutDouble(i * 1.1);
        }
        Assert.That(mem.Length, Is.EqualTo(80_000));
        var span = mem.AsReadOnlySpan(0, 80_000);
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(0, 8)), Is.EqualTo(0.0));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(79_992, 8)),
                    Is.EqualTo(9999 * 1.1).Within(0.001));
    }

    [Test]
    public void MixedTypes()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutByte(1);
        mem.PutShort(2);
        mem.PutInt(3);
        mem.PutLong(4L);
        mem.PutFloat(5.0f);
        mem.PutDouble(6.0);

        Assert.That(mem.Length, Is.EqualTo(27));
        var span = mem.AsReadOnlySpan(0, 27);
        Assert.That(span[0], Is.EqualTo((byte)1));
        Assert.That(BinaryPrimitives.ReadInt16LittleEndian(span.Slice(1, 2)), Is.EqualTo((short)2));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(3, 4)), Is.EqualTo(3));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(7, 8)), Is.EqualTo(4L));
        Assert.That(BinaryPrimitives.ReadSingleLittleEndian(span.Slice(15, 4)), Is.EqualTo(5.0f));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(19, 8)), Is.EqualTo(6.0));
    }

    [Test]
    public void PutAndReadByte()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutByte(42);
        mem.PutByte(unchecked((byte)-1));
        mem.PutByte(0);

        Assert.That(mem.Length, Is.EqualTo(3));
        var span = mem.AsReadOnlySpan(0, 3);
        Assert.That(span[0], Is.EqualTo((byte)42));
        Assert.That(span[1], Is.EqualTo((byte)0xFF));
        Assert.That(span[2], Is.EqualTo((byte)0));
    }

    [Test]
    public void PutAndReadDouble()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutDouble(2.718281828);
        mem.PutDouble(double.NaN);

        Assert.That(mem.Length, Is.EqualTo(16));
        var span = mem.AsReadOnlySpan(0, 16);
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(0, 8)), Is.EqualTo(2.718281828));
        Assert.That(double.IsNaN(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(8, 8))), Is.True);
    }

    [Test]
    public void PutAndReadFloat()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutFloat(3.14f);
        mem.PutFloat(float.NaN);

        Assert.That(mem.Length, Is.EqualTo(8));
        var span = mem.AsReadOnlySpan(0, 8);
        Assert.That(BinaryPrimitives.ReadSingleLittleEndian(span.Slice(0, 4)), Is.EqualTo(3.14f));
        Assert.That(float.IsNaN(BinaryPrimitives.ReadSingleLittleEndian(span.Slice(4, 4))), Is.True);
    }

    [Test]
    public void PutAndReadInt()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutInt(100_000);
        mem.PutInt(int.MinValue);

        Assert.That(mem.Length, Is.EqualTo(8));
        var span = mem.AsReadOnlySpan(0, 8);
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(0, 4)), Is.EqualTo(100_000));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4, 4)), Is.EqualTo(int.MinValue));
    }

    [Test]
    public void PutAndReadLong()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutLong(1_000_000_000_000L);
        mem.PutLong(long.MinValue);

        Assert.That(mem.Length, Is.EqualTo(16));
        var span = mem.AsReadOnlySpan(0, 16);
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(0, 8)), Is.EqualTo(1_000_000_000_000L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(8, 8)), Is.EqualTo(long.MinValue));
    }

    [Test]
    public void PutAndReadShort()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutShort(12_345);
        mem.PutShort(short.MinValue);
        mem.PutShort(short.MaxValue);

        Assert.That(mem.Length, Is.EqualTo(6));
        var span = mem.AsReadOnlySpan(0, 6);
        Assert.That(BinaryPrimitives.ReadInt16LittleEndian(span.Slice(0, 2)), Is.EqualTo((short)12_345));
        Assert.That(BinaryPrimitives.ReadInt16LittleEndian(span.Slice(2, 2)), Is.EqualTo(short.MinValue));
        Assert.That(BinaryPrimitives.ReadInt16LittleEndian(span.Slice(4, 2)), Is.EqualTo(short.MaxValue));
    }

    [Test]
    public void PutBoolean()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutBoolean(true);
        mem.PutBoolean(false);
        mem.PutBoolean(true);

        Assert.That(mem.Length, Is.EqualTo(3));
        var span = mem.AsReadOnlySpan(0, 3);
        Assert.That(span[0], Is.EqualTo((byte)1));
        Assert.That(span[1], Is.EqualTo((byte)0));
        Assert.That(span[2], Is.EqualTo((byte)1));
    }

    [Test]
    public void PutUtf8Ascii()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutUtf8("hello");
        Assert.That(mem.Length, Is.EqualTo(5));
        var span = mem.AsReadOnlySpan(0, 5);
        Assert.That(span[0], Is.EqualTo((byte)'h'));
        Assert.That(span[1], Is.EqualTo((byte)'e'));
        Assert.That(span[2], Is.EqualTo((byte)'l'));
        Assert.That(span[3], Is.EqualTo((byte)'l'));
        Assert.That(span[4], Is.EqualTo((byte)'o'));
    }

    [Test]
    public void PutUtf8Empty()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutUtf8("");
        Assert.That(mem.Length, Is.EqualTo(0));
    }

    [Test]
    public void PutUtf8Null()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutUtf8(null);
        Assert.That(mem.Length, Is.EqualTo(0));
    }

    [Test]
    public void PutUtf8MultiByte()
    {
        var mem = new PinnedAppendBuffer();
        // U+00E9 (e-acute) = C3 A9
        mem.PutUtf8("é");
        Assert.That(mem.Length, Is.EqualTo(2));
        var span = mem.AsReadOnlySpan(0, 2);
        Assert.That(span[0], Is.EqualTo((byte)0xC3));
        Assert.That(span[1], Is.EqualTo((byte)0xA9));
    }

    [Test]
    public void PutUtf8ThreeByte()
    {
        var mem = new PinnedAppendBuffer();
        // U+4E16 = E4 B8 96
        mem.PutUtf8("世");
        Assert.That(mem.Length, Is.EqualTo(3));
        var span = mem.AsReadOnlySpan(0, 3);
        Assert.That(span[0], Is.EqualTo((byte)0xE4));
        Assert.That(span[1], Is.EqualTo((byte)0xB8));
        Assert.That(span[2], Is.EqualTo((byte)0x96));
    }

    [Test]
    public void PutUtf8SurrogatePairs()
    {
        var mem = new PinnedAppendBuffer();
        // U+1F600 (grinning face) = F0 9F 98 80
        mem.PutUtf8("😀");
        Assert.That(mem.Length, Is.EqualTo(4));
        var span = mem.AsReadOnlySpan(0, 4);
        Assert.That(span[0], Is.EqualTo((byte)0xF0));
        Assert.That(span[1], Is.EqualTo((byte)0x9F));
        Assert.That(span[2], Is.EqualTo((byte)0x98));
        Assert.That(span[3], Is.EqualTo((byte)0x80));
    }

    [Test]
    public void PutUtf8InvalidSurrogatePair()
    {
        var mem = new PinnedAppendBuffer();
        // High surrogate U+D800 followed by 'X' (not a low surrogate). The lone high
        // surrogate is replaced with '?', then 'X' is written normally.
        mem.PutUtf8("\uD800X");
        Assert.That(mem.Length, Is.EqualTo(2));
        var span = mem.AsReadOnlySpan(0, 2);
        Assert.That(span[0], Is.EqualTo((byte)'?'));
        Assert.That(span[1], Is.EqualTo((byte)'X'));
    }

    [Test]
    public void PutUtf8Mixed()
    {
        var mem = new PinnedAppendBuffer();
        // ASCII (1B) + e-acute (2B) + CJK (3B) + emoji (4B) = 10 bytes
        mem.PutUtf8("Aé世😀");
        Assert.That(mem.Length, Is.EqualTo(10));
    }

    [Test]
    public void PutBlockOfBytesCopiesSource()
    {
        var mem = new PinnedAppendBuffer();
        var source = new byte[] { 1, 2, 3, 4, 5 };
        mem.PutBlockOfBytes(source);
        Assert.That(mem.Length, Is.EqualTo(5));
        Assert.That(mem.AsReadOnlySpan(0, 5).SequenceEqual(source), Is.True);
    }

    [Test]
    public void PutBlockOfBytesEmptyIsNoOp()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutBlockOfBytes(ReadOnlySpan<byte>.Empty);
        Assert.That(mem.Length, Is.EqualTo(0));
    }

    [Test]
    public void Skip()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutInt(1);
        mem.Skip(8);
        mem.PutInt(2);

        Assert.That(mem.Length, Is.EqualTo(16));
        var span = mem.AsReadOnlySpan(0, 16);
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(0, 4)), Is.EqualTo(1));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(12, 4)), Is.EqualTo(2));
    }

    [Test]
    public void Truncate()
    {
        var mem = new PinnedAppendBuffer();
        mem.PutInt(1);
        mem.PutInt(2);
        mem.PutInt(3);
        Assert.That(mem.Length, Is.EqualTo(12));

        mem.Truncate();
        Assert.That(mem.Length, Is.EqualTo(0));

        mem.PutInt(42);
        Assert.That(mem.Length, Is.EqualTo(4));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(mem.AsReadOnlySpan(0, 4)), Is.EqualTo(42));
    }

    [Test]
    public void AsReadOnlyMemoryIsStableAndUsableForSendAsync()
    {
        // Memory backed by a pinned array survives across awaits — the typical use case
        // for Socket.SendAsync(ReadOnlyMemory<byte>) and ClientWebSocket.SendAsync.
        var mem = new PinnedAppendBuffer();
        mem.PutInt(0x12345678);
        var memory = mem.AsReadOnlyMemory(0, 4);
        Assert.That(memory.Length, Is.EqualTo(4));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(memory.Span), Is.EqualTo(0x12345678));
    }

    [Test]
    public void CapacityGrowsToHoldLargeBlock()
    {
        var mem = new PinnedAppendBuffer(8);
        Assert.That(mem.Capacity, Is.EqualTo(8));
        var big = new byte[10_000];
        for (var i = 0; i < big.Length; i++) big[i] = (byte)(i & 0xFF);
        mem.PutBlockOfBytes(big);
        Assert.That(mem.Length, Is.EqualTo(10_000));
        Assert.That(mem.Capacity, Is.GreaterThanOrEqualTo(10_000));
        Assert.That(mem.AsReadOnlySpan(0, 10_000).SequenceEqual(big), Is.True);
    }
}
