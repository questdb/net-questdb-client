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
using System.Numerics;
using System.Text;

namespace QuestDB.Qwp;

/// <summary>
///     Single-chunk QWP buffer writer backed by a POH-pinned managed array. The .NET
///     equivalent of Java's <c>NativeBufferWriter</c> on java-questdb-client main 64b7ee69,
///     reimplemented over <see cref="GC.AllocateUninitializedArray{T}(int, bool)"/> with
///     <c>pinned: true</c> instead of <c>Unsafe.malloc</c>.
/// </summary>
/// <remarks>
///     Experimental. Default initial capacity is 8192 bytes; capacity doubles on resize.
///     The backing array's address is stable for its lifetime, so
///     <see cref="AsReadOnlyMemory"/> can feed
///     <see cref="System.Net.Sockets.Socket.SendAsync(ReadOnlyMemory{byte}, System.Net.Sockets.SocketFlags, System.Threading.CancellationToken)"/>
///     directly.
/// </remarks>
internal sealed class QwpPinnedBufferWriter : IQwpBufferWriter
{
    private const int DEFAULT_CAPACITY = 8192;

    private byte[] _array;
    private int _position;

    public QwpPinnedBufferWriter() : this(DEFAULT_CAPACITY) { }

    public QwpPinnedBufferWriter(int initialCapacity)
    {
        if (initialCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialCapacity),
                "initial capacity must be positive");
        }
        _array = GC.AllocateUninitializedArray<byte>(initialCapacity, pinned: true);
        _position = 0;
    }

    public int Position => _position;

    public int Capacity => _array.Length;

    public int WritableBytes => _array.Length - _position;

    public Span<byte> GetWritableSpan() => _array.AsSpan(_position);

    /// <summary>
    ///     Read-only view of the bytes written so far. The view is valid until the next
    ///     resize-triggering write — typical pattern is "write everything, then call this
    ///     once and hand to <c>SendAsync</c>".
    /// </summary>
    public ReadOnlyMemory<byte> AsReadOnlyMemory() => _array.AsMemory(0, _position);

    /// <summary>Read-only span over the bytes written so far.</summary>
    public ReadOnlySpan<byte> AsReadOnlySpan() => _array.AsSpan(0, _position);

    /// <summary>
    ///     Underlying pinned <see cref="byte"/> array. For native-style encoders that need
    ///     a <c>byte[]</c> + offset (like <see cref="QwpGorillaEncoder"/>) — combine with
    ///     <see cref="Position"/> for the destination offset.
    /// </summary>
    internal byte[] UnderlyingArray => _array;

    public void EnsureCapacity(int additionalBytes)
    {
        var required = (long)_position + additionalBytes;
        if (required > _array.Length)
        {
            var newCapacity = Math.Max((long)_array.Length * 2, required);
            if (newCapacity > int.MaxValue)
            {
                throw new OutOfMemoryException(
                    "QwpPinnedBufferWriter capacity would exceed Int32.MaxValue");
            }
            var bigger = GC.AllocateUninitializedArray<byte>((int)newCapacity, pinned: true);
            _array.AsSpan(0, _position).CopyTo(bigger);
            _array = bigger;
        }
    }

    public void PutByte(byte value)
    {
        EnsureCapacity(1);
        _array[_position++] = value;
    }

    public void PutShort(short value)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteInt16LittleEndian(_array.AsSpan(_position), value);
        _position += 2;
    }

    public void PutInt(int value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteInt32LittleEndian(_array.AsSpan(_position), value);
        _position += 4;
    }

    public void PutLong(long value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteInt64LittleEndian(_array.AsSpan(_position), value);
        _position += 8;
    }

    public void PutFloat(float value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteSingleLittleEndian(_array.AsSpan(_position), value);
        _position += 4;
    }

    public void PutDouble(double value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteDoubleLittleEndian(_array.AsSpan(_position), value);
        _position += 8;
    }

    public void PutVarint(long value)
    {
        EnsureCapacity(10); // max LEB128 size
        var v = (ulong)value;
        while (v > 0x7F)
        {
            _array[_position++] = (byte)((v & 0x7F) | 0x80);
            v >>= 7;
        }
        _array[_position++] = (byte)v;
    }

    public void PutString(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            PutVarint(0);
            return;
        }
        var utf8Len = Utf8Length(value);
        PutVarint(utf8Len);
        EnsureCapacity(utf8Len);
        var written = Utf8WithReplacement.GetBytes(value.AsSpan(), _array.AsSpan(_position, utf8Len));
        _position += written;
    }

    public void PutUtf8(string? value)
    {
        if (string.IsNullOrEmpty(value)) return;
        var utf8Len = Utf8Length(value);
        EnsureCapacity(utf8Len);
        var written = Utf8WithReplacement.GetBytes(value.AsSpan(), _array.AsSpan(_position, utf8Len));
        _position += written;
    }

    public void PutBlockOfBytes(ReadOnlyMemory<byte> source)
    {
        if (source.IsEmpty) return;
        EnsureCapacity(source.Length);
        source.Span.CopyTo(_array.AsSpan(_position));
        _position += source.Length;
    }

    public void PatchInt(int offset, int value)
    {
        if (offset < 0 || offset > _position - 4)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                "patch offset must lie within already-written data");
        }
        BinaryPrimitives.WriteInt32LittleEndian(_array.AsSpan(offset, 4), value);
    }

    public void Skip(int bytes)
    {
        if (bytes < 0) throw new ArgumentOutOfRangeException(nameof(bytes));
        EnsureCapacity(bytes);
        _position += bytes;
    }

    public void Reset() => _position = 0;

    /// <summary>
    ///     Returns the number of UTF-8 bytes <paramref name="value"/> would encode to.
    ///     Lone surrogates are counted as one byte each (replaced with <c>'?'</c> to mirror
    ///     Java's <c>Utf8s.utf8Bytes</c>).
    /// </summary>
    public static int Utf8Length(string? value)
    {
        if (string.IsNullOrEmpty(value)) return 0;
        return Utf8WithReplacement.GetByteCount(value);
    }

    /// <summary>
    ///     Returns the number of bytes <paramref name="value"/> would consume as an unsigned
    ///     LEB128 varint.
    /// </summary>
    public static int VarintSize(long value)
    {
        if (value == 0) return 1;
        return (64 - BitOperations.LeadingZeroCount((ulong)value) + 6) / 7;
    }

    private static readonly Encoding Utf8WithReplacement =
        Encoding.GetEncoding("utf-8",
                             new EncoderReplacementFallback("?"),
                             new DecoderReplacementFallback("?"));
}
