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

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace QuestDB.Qwp;

/// <summary>
///     Lightweight append-only off-heap buffer for columnar QWP data.
/// </summary>
/// <remarks>
///     Experimental. Mirrors <c>OffHeapAppendMemory.java</c> on Java main 64b7ee69. Uses
///     <see cref="NativeMemory"/> instead of GC-managed arrays. Capacity doubles on resize.
///     Default initial capacity is 128 bytes; minimum is 8.
/// </remarks>
internal sealed unsafe class OffHeapAppendMemory : IDisposable
{
    private const int DEFAULT_INITIAL_CAPACITY = 128;
    private byte* _pageAddress;
    private byte* _appendAddress;
    private nuint _capacity;

    public OffHeapAppendMemory() : this(DEFAULT_INITIAL_CAPACITY) { }

    public OffHeapAppendMemory(long initialCapacity)
    {
        _capacity = (nuint)Math.Max(initialCapacity, 8);
        _pageAddress = (byte*)NativeMemory.Alloc(_capacity);
        _appendAddress = _pageAddress;
    }

    /// <summary>Base address of the buffer (zero after <see cref="Dispose"/>).</summary>
    public nint PageAddress => (nint)_pageAddress;

    /// <summary>Number of bytes currently written.</summary>
    public long AppendOffset => _appendAddress - _pageAddress;

    /// <summary>Returns the address at the given byte offset from the start.</summary>
    public nint AddressOf(long offset) => (nint)(_pageAddress + offset);

    /// <summary>Sets the append position to <paramref name="offset"/> bytes from the start.</summary>
    public void JumpTo(long offset)
    {
        if (offset < 0 || offset > AppendOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                "JumpTo offset must be within the range [0, " + AppendOffset + "]");
        }
        _appendAddress = _pageAddress + offset;
    }

    /// <summary>Resets the append position to 0 without freeing memory.</summary>
    public void Truncate()
    {
        _appendAddress = _pageAddress;
    }

    /// <summary>Advances the append position by <paramref name="bytes"/> without writing.</summary>
    public void Skip(long bytes)
    {
        EnsureCapacity((nuint)bytes);
        _appendAddress += bytes;
    }

    public void PutBoolean(bool value) => PutByte(value ? (byte)1 : (byte)0);

    public void PutByte(byte value)
    {
        EnsureCapacity(1);
        *_appendAddress = value;
        _appendAddress++;
    }

    public void PutShort(short value)
    {
        EnsureCapacity(2);
        Unsafe.WriteUnaligned(_appendAddress, value);
        _appendAddress += 2;
    }

    public void PutInt(int value)
    {
        EnsureCapacity(4);
        Unsafe.WriteUnaligned(_appendAddress, value);
        _appendAddress += 4;
    }

    public void PutLong(long value)
    {
        EnsureCapacity(8);
        Unsafe.WriteUnaligned(_appendAddress, value);
        _appendAddress += 8;
    }

    public void PutFloat(float value)
    {
        EnsureCapacity(4);
        Unsafe.WriteUnaligned(_appendAddress, value);
        _appendAddress += 4;
    }

    public void PutDouble(double value)
    {
        EnsureCapacity(8);
        Unsafe.WriteUnaligned(_appendAddress, value);
        _appendAddress += 8;
    }

    public void PutBlockOfBytes(nint from, long len)
    {
        if (len <= 0) return;
        EnsureCapacity((nuint)len);
        Buffer.MemoryCopy((void*)from, _appendAddress, (long)(_capacity - (nuint)AppendOffset), len);
        _appendAddress += len;
    }

    /// <summary>
    ///     Encodes a string to UTF-8 directly into the buffer. Lone surrogates are written as <c>'?'</c>,
    ///     matching the Java reference. <c>null</c> or empty input is a no-op.
    /// </summary>
    public void PutUtf8(string? value)
    {
        if (string.IsNullOrEmpty(value)) return;

        var len = value.Length;
        EnsureCapacity((nuint)len * 4); // worst case: each char becomes 4 bytes
        for (var i = 0; i < len; i++)
        {
            var c = value[i];
            if (c < 0x80)
            {
                *_appendAddress++ = (byte)c;
            }
            else if (c < 0x800)
            {
                *_appendAddress++ = (byte)(0xC0 | (c >> 6));
                *_appendAddress++ = (byte)(0x80 | (c & 0x3F));
            }
            else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < len)
            {
                var c2 = value[++i];
                if (char.IsLowSurrogate(c2))
                {
                    var codePoint = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
                    *_appendAddress++ = (byte)(0xF0 | (codePoint >> 18));
                    *_appendAddress++ = (byte)(0x80 | ((codePoint >> 12) & 0x3F));
                    *_appendAddress++ = (byte)(0x80 | ((codePoint >> 6) & 0x3F));
                    *_appendAddress++ = (byte)(0x80 | (codePoint & 0x3F));
                }
                else
                {
                    *_appendAddress++ = (byte)'?';
                    i--;
                }
            }
            else if (char.IsSurrogate(c))
            {
                *_appendAddress++ = (byte)'?';
            }
            else
            {
                *_appendAddress++ = (byte)(0xE0 | (c >> 12));
                *_appendAddress++ = (byte)(0x80 | ((c >> 6) & 0x3F));
                *_appendAddress++ = (byte)(0x80 | (c & 0x3F));
            }
        }
    }

    public void Dispose()
    {
        if (_pageAddress != null)
        {
            NativeMemory.Free(_pageAddress);
            _pageAddress = null;
            _appendAddress = null;
            _capacity = 0;
        }
    }

    private void EnsureCapacity(nuint needed)
    {
        var used = (nuint)AppendOffset;
        if (used + needed > _capacity)
        {
            var newCapacity = Math.Max(_capacity * 2, used + needed);
            _pageAddress = (byte*)NativeMemory.Realloc(_pageAddress, newCapacity);
            _capacity = newCapacity;
            _appendAddress = _pageAddress + used;
        }
    }
}
