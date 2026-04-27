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

namespace net_questdb_client_tests.Qwp.Protocol;

/// <summary>
///     Mirrors <c>OffHeapAppendMemoryTest.java</c> on Java main 64b7ee69. Read-back via
///     <see cref="Marshal"/> (no unsafe blocks needed in tests). Java-side memory-leak
///     accounting is replaced with a post-<see cref="OffHeapAppendMemory.Dispose"/>
///     <c>PageAddress == IntPtr.Zero</c> check.
/// </summary>
[TestFixture]
public class OffHeapAppendMemoryTests
{
    private static byte ReadByte(nint addr) => Marshal.ReadByte(addr);
    private static short ReadShort(nint addr) => Marshal.ReadInt16(addr);
    private static int ReadInt(nint addr) => Marshal.ReadInt32(addr);
    private static long ReadLong(nint addr) => Marshal.ReadInt64(addr);
    private static float ReadFloat(nint addr) => BitConverter.Int32BitsToSingle(Marshal.ReadInt32(addr));
    private static double ReadDouble(nint addr) => BitConverter.Int64BitsToDouble(Marshal.ReadInt64(addr));

    [Test]
    public void CloseFreesMemory()
    {
        var mem = new OffHeapAppendMemory(1024);
        Assert.That(mem.PageAddress, Is.Not.EqualTo(IntPtr.Zero));
        mem.Dispose();
        Assert.That(mem.PageAddress, Is.EqualTo(IntPtr.Zero));
    }

    [Test]
    public void DoubleCloseIsSafe()
    {
        var mem = new OffHeapAppendMemory();
        mem.PutInt(42);
        mem.Dispose();
        Assert.DoesNotThrow(() => mem.Dispose());
    }

    [Test]
    public void Growth()
    {
        using var mem = new OffHeapAppendMemory(8);
        for (var i = 0; i < 100; i++)
        {
            mem.PutLong(i);
        }

        Assert.That(mem.AppendOffset, Is.EqualTo(800));
        for (var i = 0; i < 100; i++)
        {
            Assert.That(ReadLong(mem.AddressOf(i * 8L)), Is.EqualTo(i));
        }
    }

    [Test]
    public void JumpTo()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutLong(100);
        mem.PutLong(200);
        mem.PutLong(300);
        Assert.That(mem.AppendOffset, Is.EqualTo(24));

        mem.JumpTo(8);
        Assert.That(mem.AppendOffset, Is.EqualTo(8));

        mem.PutLong(999);
        Assert.That(mem.AppendOffset, Is.EqualTo(16));
        Assert.That(ReadLong(mem.AddressOf(0)), Is.EqualTo(100));
        Assert.That(ReadLong(mem.AddressOf(8)), Is.EqualTo(999));
    }

    [Test]
    public void LargeGrowth()
    {
        using var mem = new OffHeapAppendMemory(8);
        for (var i = 0; i < 10_000; i++)
        {
            mem.PutDouble(i * 1.1);
        }
        Assert.That(mem.AppendOffset, Is.EqualTo(80_000));
        Assert.That(ReadDouble(mem.AddressOf(0)), Is.EqualTo(0.0));
        Assert.That(ReadDouble(mem.AddressOf(79_992)), Is.EqualTo(9999 * 1.1).Within(0.001));
    }

    [Test]
    public void MixedTypes()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutByte(1);
        mem.PutShort(2);
        mem.PutInt(3);
        mem.PutLong(4L);
        mem.PutFloat(5.0f);
        mem.PutDouble(6.0);

        var addr = mem.PageAddress;
        Assert.That(ReadByte(addr), Is.EqualTo(1));
        Assert.That(ReadShort(addr + 1), Is.EqualTo(2));
        Assert.That(ReadInt(addr + 3), Is.EqualTo(3));
        Assert.That(ReadLong(addr + 7), Is.EqualTo(4L));
        Assert.That(ReadFloat(addr + 15), Is.EqualTo(5.0f));
        Assert.That(ReadDouble(addr + 19), Is.EqualTo(6.0));
        Assert.That(mem.AppendOffset, Is.EqualTo(27));
    }

    [Test]
    public void PageAddress_NonZero_AndAdvancesByOffset()
    {
        using var mem = new OffHeapAppendMemory();
        Assert.That(mem.PageAddress, Is.Not.EqualTo(IntPtr.Zero));
        Assert.That(mem.PageAddress, Is.EqualTo(mem.AddressOf(0)));
        mem.PutLong(42);
        Assert.That(mem.AddressOf(8), Is.EqualTo(mem.PageAddress + 8));
    }

    [Test]
    public void PutAndReadByte()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutByte(42);
        mem.PutByte(unchecked((byte)-1));
        mem.PutByte(0);

        Assert.That(mem.AppendOffset, Is.EqualTo(3));
        Assert.That(ReadByte(mem.AddressOf(0)), Is.EqualTo((byte)42));
        Assert.That(ReadByte(mem.AddressOf(1)), Is.EqualTo((byte)0xFF));
        Assert.That(ReadByte(mem.AddressOf(2)), Is.EqualTo((byte)0));
    }

    [Test]
    public void PutAndReadDouble()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutDouble(2.718281828);
        mem.PutDouble(double.NaN);

        Assert.That(mem.AppendOffset, Is.EqualTo(16));
        Assert.That(ReadDouble(mem.AddressOf(0)), Is.EqualTo(2.718281828));
        Assert.That(double.IsNaN(ReadDouble(mem.AddressOf(8))), Is.True);
    }

    [Test]
    public void PutAndReadFloat()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutFloat(3.14f);
        mem.PutFloat(float.NaN);

        Assert.That(mem.AppendOffset, Is.EqualTo(8));
        Assert.That(ReadFloat(mem.AddressOf(0)), Is.EqualTo(3.14f));
        Assert.That(float.IsNaN(ReadFloat(mem.AddressOf(4))), Is.True);
    }

    [Test]
    public void PutAndReadInt()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutInt(100_000);
        mem.PutInt(int.MinValue);

        Assert.That(mem.AppendOffset, Is.EqualTo(8));
        Assert.That(ReadInt(mem.AddressOf(0)), Is.EqualTo(100_000));
        Assert.That(ReadInt(mem.AddressOf(4)), Is.EqualTo(int.MinValue));
    }

    [Test]
    public void PutAndReadLong()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutLong(1_000_000_000_000L);
        mem.PutLong(long.MinValue);

        Assert.That(mem.AppendOffset, Is.EqualTo(16));
        Assert.That(ReadLong(mem.AddressOf(0)), Is.EqualTo(1_000_000_000_000L));
        Assert.That(ReadLong(mem.AddressOf(8)), Is.EqualTo(long.MinValue));
    }

    [Test]
    public void PutAndReadShort()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutShort(12_345);
        mem.PutShort(short.MinValue);
        mem.PutShort(short.MaxValue);

        Assert.That(mem.AppendOffset, Is.EqualTo(6));
        Assert.That(ReadShort(mem.AddressOf(0)), Is.EqualTo((short)12_345));
        Assert.That(ReadShort(mem.AddressOf(2)), Is.EqualTo(short.MinValue));
        Assert.That(ReadShort(mem.AddressOf(4)), Is.EqualTo(short.MaxValue));
    }

    [Test]
    public void PutBoolean()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutBoolean(true);
        mem.PutBoolean(false);
        mem.PutBoolean(true);

        Assert.That(mem.AppendOffset, Is.EqualTo(3));
        Assert.That(ReadByte(mem.AddressOf(0)), Is.EqualTo((byte)1));
        Assert.That(ReadByte(mem.AddressOf(1)), Is.EqualTo((byte)0));
        Assert.That(ReadByte(mem.AddressOf(2)), Is.EqualTo((byte)1));
    }

    [Test]
    public void PutUtf8Ascii()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutUtf8("hello");
        Assert.That(mem.AppendOffset, Is.EqualTo(5));
        Assert.That(ReadByte(mem.AddressOf(0)), Is.EqualTo((byte)'h'));
        Assert.That(ReadByte(mem.AddressOf(1)), Is.EqualTo((byte)'e'));
        Assert.That(ReadByte(mem.AddressOf(2)), Is.EqualTo((byte)'l'));
        Assert.That(ReadByte(mem.AddressOf(3)), Is.EqualTo((byte)'l'));
        Assert.That(ReadByte(mem.AddressOf(4)), Is.EqualTo((byte)'o'));
    }

    [Test]
    public void PutUtf8Empty()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutUtf8("");
        Assert.That(mem.AppendOffset, Is.EqualTo(0));
    }

    [Test]
    public void PutUtf8Null()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutUtf8(null);
        Assert.That(mem.AppendOffset, Is.EqualTo(0));
    }

    [Test]
    public void PutUtf8MultiByte()
    {
        using var mem = new OffHeapAppendMemory();
        // U+00E9 (e-acute) = C3 A9
        mem.PutUtf8("é");
        Assert.That(mem.AppendOffset, Is.EqualTo(2));
        Assert.That(ReadByte(mem.AddressOf(0)), Is.EqualTo((byte)0xC3));
        Assert.That(ReadByte(mem.AddressOf(1)), Is.EqualTo((byte)0xA9));
    }

    [Test]
    public void PutUtf8ThreeByte()
    {
        using var mem = new OffHeapAppendMemory();
        // U+4E16 = E4 B8 96
        mem.PutUtf8("世");
        Assert.That(mem.AppendOffset, Is.EqualTo(3));
        Assert.That(ReadByte(mem.AddressOf(0)), Is.EqualTo((byte)0xE4));
        Assert.That(ReadByte(mem.AddressOf(1)), Is.EqualTo((byte)0xB8));
        Assert.That(ReadByte(mem.AddressOf(2)), Is.EqualTo((byte)0x96));
    }

    [Test]
    public void PutUtf8SurrogatePairs()
    {
        using var mem = new OffHeapAppendMemory();
        // U+1F600 (grinning face) = F0 9F 98 80
        mem.PutUtf8("😀");
        Assert.That(mem.AppendOffset, Is.EqualTo(4));
        Assert.That(ReadByte(mem.AddressOf(0)), Is.EqualTo((byte)0xF0));
        Assert.That(ReadByte(mem.AddressOf(1)), Is.EqualTo((byte)0x9F));
        Assert.That(ReadByte(mem.AddressOf(2)), Is.EqualTo((byte)0x98));
        Assert.That(ReadByte(mem.AddressOf(3)), Is.EqualTo((byte)0x80));
    }

    [Test]
    public void PutUtf8InvalidSurrogatePair()
    {
        using var mem = new OffHeapAppendMemory();
        // High surrogate U+D800 followed by 'X' (not a low surrogate). The lone high
        // surrogate is replaced with '?', then 'X' is written normally.
        mem.PutUtf8("\uD800X");
        Assert.That(mem.AppendOffset, Is.EqualTo(2));
        Assert.That(ReadByte(mem.AddressOf(0)), Is.EqualTo((byte)'?'));
        Assert.That(ReadByte(mem.AddressOf(1)), Is.EqualTo((byte)'X'));
    }

    [Test]
    public void PutUtf8Mixed()
    {
        using var mem = new OffHeapAppendMemory();
        // ASCII (1B) + e-acute (2B) + CJK (3B) + emoji (4B) = 10 bytes
        mem.PutUtf8("Aé世😀");
        Assert.That(mem.AppendOffset, Is.EqualTo(10));
    }

    [Test]
    public void Skip()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutInt(1);
        mem.Skip(8);
        mem.PutInt(2);

        Assert.That(mem.AppendOffset, Is.EqualTo(16));
        Assert.That(ReadInt(mem.AddressOf(0)), Is.EqualTo(1));
        Assert.That(ReadInt(mem.AddressOf(12)), Is.EqualTo(2));
    }

    [Test]
    public void Truncate()
    {
        using var mem = new OffHeapAppendMemory();
        mem.PutInt(1);
        mem.PutInt(2);
        mem.PutInt(3);
        Assert.That(mem.AppendOffset, Is.EqualTo(12));

        mem.Truncate();
        Assert.That(mem.AppendOffset, Is.EqualTo(0));

        mem.PutInt(42);
        Assert.That(mem.AppendOffset, Is.EqualTo(4));
        Assert.That(ReadInt(mem.AddressOf(0)), Is.EqualTo(42));
    }
}
