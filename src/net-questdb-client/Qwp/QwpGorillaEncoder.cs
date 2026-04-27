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
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Gorilla delta-of-delta encoder for QWP v1 timestamp columns. The .NET counterpart of
///     Java's <c>QwpGorillaEncoder</c> on java-questdb-client main 64b7ee69, reimplemented
///     over <see cref="ReadOnlySpan{T}"/> instead of raw native pointers.
/// </summary>
/// <remarks>
///     Experimental. Format:
///     <list type="bullet">
///         <item>First two timestamps: int64 little-endian (16 bytes total).</item>
///         <item>Remaining: bit-packed delta-of-delta with five buckets:
///             <c>'0'</c> ⇒ DoD == 0 (1 bit);
///             <c>'10'</c>+7-bit signed ⇒ [-64, 63] (9 bits);
///             <c>'110'</c>+9-bit signed ⇒ [-256, 255] (12 bits);
///             <c>'1110'</c>+12-bit signed ⇒ [-2048, 2047] (16 bits);
///             <c>'1111'</c>+32-bit signed ⇒ otherwise (36 bits).</item>
///     </list>
///     The encoding flag byte is the caller's responsibility (this method writes only the
///     timestamp payload).
/// </remarks>
internal sealed class QwpGorillaEncoder
{
    private const int BUCKET_7BIT_MIN = -64;
    private const int BUCKET_7BIT_MAX = 63;
    private const int BUCKET_9BIT_MIN = -256;
    private const int BUCKET_9BIT_MAX = 255;
    private const int BUCKET_12BIT_MIN = -2048;
    private const int BUCKET_12BIT_MAX = 2047;

    private readonly QwpBitWriter _bitWriter = new();

    /// <summary>
    ///     Bytes required to encode <paramref name="timestamps"/>, or <c>-1</c> if any
    ///     delta-of-delta would exceed <see cref="int.MinValue"/>..<see cref="int.MaxValue"/>
    ///     (the largest bucket caps at 32 bits signed). Single-pass: the canUseGorilla check
    ///     and the size computation share one walk over the input.
    /// </summary>
    public static int CalculateEncodedSizeIfSupported(ReadOnlySpan<long> timestamps)
    {
        var count = timestamps.Length;
        if (count == 0) return 0;
        long size = 8; // first ts
        if (count == 1) return (int)size;
        size += 8;     // second ts
        if (count == 2) return (int)size;

        var prevTs = timestamps[1];
        var prevDelta = unchecked(prevTs - timestamps[0]);
        long totalBits = 0;

        for (var i = 2; i < count; i++)
        {
            var ts = timestamps[i];
            var delta = unchecked(ts - prevTs);
            var dod = unchecked(delta - prevDelta);
            if (dod < int.MinValue || dod > int.MaxValue) return -1;
            totalBits += GetBitsRequired(dod);
            prevDelta = delta;
            prevTs = ts;
        }

        size += (totalBits + 7) / 8;
        return ToIntChecked(size);
    }

    /// <summary>
    ///     Bytes required to encode <paramref name="timestamps"/>. Caller must verify
    ///     <see cref="CanUseGorilla"/> is <c>true</c> first; otherwise this throws.
    /// </summary>
    public static int CalculateEncodedSize(ReadOnlySpan<long> timestamps)
    {
        var size = CalculateEncodedSizeIfSupported(timestamps);
        if (size < 0)
        {
            throw new InvalidOperationException(
                "caller must verify CanUseGorilla before CalculateEncodedSize");
        }
        return size;
    }

    /// <summary>
    ///     Whether Gorilla encoding can encode <paramref name="timestamps"/>. Returns
    ///     <c>false</c> when any delta-of-delta exceeds the 32-bit signed int range.
    /// </summary>
    public static bool CanUseGorilla(ReadOnlySpan<long> timestamps) =>
        CalculateEncodedSizeIfSupported(timestamps) >= 0;

    /// <summary>
    ///     Narrows a long encoded size to int, throwing if it exceeds <see cref="int.MaxValue"/>.
    /// </summary>
    public static int ToIntChecked(long size)
    {
        if (size > int.MaxValue)
        {
            throw new OverflowException("Gorilla encoded size exceeds int range");
        }
        return (int)size;
    }

    /// <summary>Returns bucket 0..4 for a delta-of-delta value.</summary>
    public static int GetBucket(long deltaOfDelta)
    {
        if (deltaOfDelta == 0) return 0;
        if (deltaOfDelta >= BUCKET_7BIT_MIN && deltaOfDelta <= BUCKET_7BIT_MAX) return 1;
        if (deltaOfDelta >= BUCKET_9BIT_MIN && deltaOfDelta <= BUCKET_9BIT_MAX) return 2;
        if (deltaOfDelta >= BUCKET_12BIT_MIN && deltaOfDelta <= BUCKET_12BIT_MAX) return 3;
        return 4;
    }

    /// <summary>Returns the bit count required to encode a delta-of-delta value.</summary>
    public static int GetBitsRequired(long deltaOfDelta) => GetBucket(deltaOfDelta) switch
    {
        0 => 1,
        1 => 9,
        2 => 12,
        3 => 16,
        _ => 36,
    };

    /// <summary>Writes a single delta-of-delta value via the encoder's bit writer.</summary>
    public void EncodeDoD(long deltaOfDelta)
    {
        switch (GetBucket(deltaOfDelta))
        {
            case 0:
                _bitWriter.WriteBit(0);
                break;
            case 1:
                _bitWriter.WriteBits(0b01, 2);
                _bitWriter.WriteSigned(deltaOfDelta, 7);
                break;
            case 2:
                _bitWriter.WriteBits(0b011, 3);
                _bitWriter.WriteSigned(deltaOfDelta, 9);
                break;
            case 3:
                _bitWriter.WriteBits(0b0111, 4);
                _bitWriter.WriteSigned(deltaOfDelta, 12);
                break;
            default:
                _bitWriter.WriteBits(0b1111, 4);
                _bitWriter.WriteSigned(deltaOfDelta, 32);
                break;
        }
    }

    /// <summary>
    ///     Encodes <paramref name="timestamps"/> into <paramref name="destination"/> starting
    ///     at <paramref name="destOffset"/>. Returns the number of bytes written.
    /// </summary>
    /// <exception cref="IngressError">Destination is too small.</exception>
    public int EncodeTimestamps(byte[] destination, int destOffset, ReadOnlySpan<long> timestamps)
    {
        if (destination is null) throw new ArgumentNullException(nameof(destination));
        if ((uint)destOffset > (uint)destination.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(destOffset));
        }

        var count = timestamps.Length;
        if (count == 0) return 0;

        var capacity = destination.Length - destOffset;

        if (capacity < 8)
        {
            throw new IngressError(ErrorCode.BufferOverflow, "Gorilla encoder buffer overflow");
        }
        var ts0 = timestamps[0];
        BinaryPrimitives.WriteInt64LittleEndian(destination.AsSpan(destOffset, 8), ts0);
        var pos = 8;

        if (count == 1) return pos;

        if (capacity < pos + 8)
        {
            throw new IngressError(ErrorCode.BufferOverflow, "Gorilla encoder buffer overflow");
        }
        var ts1 = timestamps[1];
        BinaryPrimitives.WriteInt64LittleEndian(destination.AsSpan(destOffset + pos, 8), ts1);
        pos = 16;

        if (count == 2) return pos;

        _bitWriter.Reset(destination, destOffset + pos, capacity - pos);

        var prevTs = ts1;
        var prevDelta = unchecked(ts1 - ts0);

        for (var i = 2; i < count; i++)
        {
            var ts = timestamps[i];
            var delta = unchecked(ts - prevTs);
            var dod = unchecked(delta - prevDelta);
            EncodeDoD(dod);
            prevDelta = delta;
            prevTs = ts;
        }

        return pos + _bitWriter.Finish();
    }
}
