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
using System.Text;

namespace QuestDB.Qwp;

/// <summary>
///     Append-only growable byte buffer backed by a managed array on the .NET Pinned
///     Object Heap (POH). The .NET equivalent of Java's <c>OffHeapAppendMemory</c>.
/// </summary>
/// <remarks>
///     Experimental. Allocated via <see cref="GC.AllocateUninitializedArray{T}(int, bool)"/>
///     with <c>pinned: true</c>, so the array's address never moves and feeds
///     <see cref="System.Net.Sockets.Socket.SendAsync(System.ReadOnlyMemory{byte}, System.Net.Sockets.SocketFlags, System.Threading.CancellationToken)"/>
///     and <see cref="System.Net.WebSockets.ClientWebSocket"/> directly without bridging
///     code. Default initial capacity is 128 bytes (minimum 8); capacity doubles on resize.
///     <para/>
///     Pattern matches Kestrel's <c>PinnedBlockMemoryPool</c>: pinned managed array +
///     <see cref="Span{T}"/>/<see cref="Memory{T}"/> APIs at boundaries.
/// </remarks>
internal sealed class PinnedAppendBuffer
{
    private const int DEFAULT_INITIAL_CAPACITY = 128;
    private byte[] _array;
    private int _position;

    public PinnedAppendBuffer() : this(DEFAULT_INITIAL_CAPACITY) { }

    public PinnedAppendBuffer(int initialCapacity)
    {
        var size = Math.Max(initialCapacity, 8);
        _array = GC.AllocateUninitializedArray<byte>(size, pinned: true);
        _position = 0;
    }

    /// <summary>Number of bytes currently written.</summary>
    public int Length => _position;

    /// <summary>Total capacity (may be larger than <see cref="Length"/>).</summary>
    public int Capacity => _array.Length;

    /// <summary>
    ///     Returns a writable view of <paramref name="length"/> bytes starting at
    ///     <paramref name="offset"/>. The returned span is valid until the next
    ///     resize-triggering append.
    /// </summary>
    public Span<byte> AsSpan(int offset, int length) => _array.AsSpan(offset, length);

    /// <summary>
    ///     Returns a read-only view of <paramref name="length"/> bytes starting at
    ///     <paramref name="offset"/>. The returned span is valid until the next
    ///     resize-triggering append.
    /// </summary>
    public ReadOnlySpan<byte> AsReadOnlySpan(int offset, int length) => _array.AsSpan(offset, length);

    /// <summary>
    ///     Returns a read-only memory view suitable for handing to
    ///     <see cref="System.Net.Sockets.Socket.SendAsync(System.ReadOnlyMemory{byte}, System.Net.Sockets.SocketFlags, System.Threading.CancellationToken)"/>
    ///     or <see cref="System.Net.WebSockets.ClientWebSocket.SendAsync(System.ReadOnlyMemory{byte}, System.Net.WebSockets.WebSocketMessageType, bool, System.Threading.CancellationToken)"/>.
    /// </summary>
    /// <remarks>
    ///     The view's address is stable for the underlying POH array's lifetime — but a
    ///     resize-triggering append (<see cref="PutByte(byte)"/>, <see cref="PutBlockOfBytes"/>, etc.)
    ///     swaps in a fresh array, leaving any previously-returned <see cref="ReadOnlyMemory{T}"/>
    ///     pointing at the now-stale backing. Treat the returned memory as valid only until
    ///     the next mutation. Typical pattern: write everything, call <c>AsReadOnlyMemory</c>
    ///     once, hand to <c>SendAsync</c>.
    /// </remarks>
    public ReadOnlyMemory<byte> AsReadOnlyMemory(int offset, int length) => _array.AsMemory(offset, length);

    /// <summary>
    ///     Sets the append position to the given offset. Must be in <c>[0, Length]</c>.
    ///     Used for truncate-to operations on column buffers.
    /// </summary>
    public void JumpTo(int offset)
    {
        if ((uint)offset > (uint)_position)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                "JumpTo offset must be within [0, Length]");
        }
        _position = offset;
    }

    /// <summary>Resets the append position to 0 without freeing memory.</summary>
    public void Truncate() => _position = 0;

    /// <summary>Advances the append position by <paramref name="bytes"/> without writing.</summary>
    public void Skip(int bytes)
    {
        EnsureCapacity(bytes);
        _position += bytes;
    }

    /// <summary>
    ///     Public facade for the internal grow helper. Useful when an external encoder
    ///     wants to stage the destination capacity before writing in place.
    /// </summary>
    public void EnsureCapacityFor(int additionalBytes) => EnsureCapacity(additionalBytes);

    public void PutBoolean(bool value) => PutByte(value ? (byte)1 : (byte)0);

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

    /// <summary>Copies <paramref name="source"/> into the buffer at the current position.</summary>
    public void PutBlockOfBytes(ReadOnlySpan<byte> source)
    {
        if (source.IsEmpty) return;
        EnsureCapacity(source.Length);
        source.CopyTo(_array.AsSpan(_position));
        _position += source.Length;
    }

    /// <summary>
    ///     Writes <paramref name="value"/> as UTF-8. Lone surrogates are replaced with
    ///     <c>'?'</c> to mirror Java's <c>OffHeapAppendMemory.putUtf8</c>. <c>null</c>
    ///     or empty input is a no-op.
    /// </summary>
    public void PutUtf8(string? value)
    {
        if (string.IsNullOrEmpty(value)) return;
        // Reserve worst-case (4 bytes per char) up-front, then encode.
        EnsureCapacity(value.Length * 4);
        var written = Utf8Encoding.GetBytes(value.AsSpan(), _array.AsSpan(_position));
        _position += written;
    }

    private void EnsureCapacity(int needed)
    {
        var used = _position;
        var required = (long)used + needed;
        if (required > _array.Length)
        {
            var newCapacity = Math.Max((long)_array.Length * 2, required);
            if (newCapacity > int.MaxValue)
            {
                throw new OutOfMemoryException(
                    "PinnedAppendBuffer capacity would exceed Int32.MaxValue");
            }
            var bigger = GC.AllocateUninitializedArray<byte>((int)newCapacity, pinned: true);
            _array.AsSpan(0, used).CopyTo(bigger);
            _array = bigger;
        }
    }

    /// <summary>
    ///     UTF-8 encoder configured to substitute <c>'?'</c> for invalid surrogate pairs,
    ///     matching the Java reference implementation.
    /// </summary>
    private static readonly Encoding Utf8Encoding =
        Encoding.GetEncoding("utf-8",
                             new EncoderReplacementFallback("?"),
                             new DecoderReplacementFallback("?"));
}
