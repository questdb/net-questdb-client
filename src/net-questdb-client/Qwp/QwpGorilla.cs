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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

using System.Buffers.Binary;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Gorilla delta-of-delta timestamp compression (specification §12).
/// </summary>
/// <remarks>
///     Output layout when called with <c>FLAG_GORILLA</c> on:
///     <list type="bullet">
///         <item><c>encoding_flag (uint8)</c>: <c>0x00</c> uncompressed, <c>0x01</c> Gorilla.</item>
///         <item>Uncompressed: <c>value_count × int64 LE</c>.</item>
///         <item>
///             Gorilla: <c>first_timestamp (int64 LE) + second_timestamp (int64 LE) +
///             bit-packed delta-of-deltas for timestamps 3..N</c>.
///         </item>
///     </list>
///     <para />
///     The encoder falls back to uncompressed mode when:
///     <list type="bullet">
///         <item>fewer than 3 values are present (no DoDs to compress);</item>
///         <item>any DoD overflows the 32-bit signed integer range.</item>
///     </list>
///     <para />
///     Bucket layout — bits LSB-first, prefix then signed value:
///     <list type="table">
///         <listheader><term>DoD range</term><description>prefix bits + value bits</description></listheader>
///         <item><term><c>== 0</c></term><description><c>0</c> (1 bit total)</description></item>
///         <item><term><c>[-64, 63]</c></term><description><c>10</c> + 7-bit signed (9 bits total)</description></item>
///         <item><term><c>[-256, 255]</c></term><description><c>110</c> + 9-bit signed (12 bits total)</description></item>
///         <item><term><c>[-2048, 2047]</c></term><description><c>1110</c> + 12-bit signed (16 bits total)</description></item>
///         <item><term>otherwise</term><description><c>1111</c> + 32-bit signed (36 bits total)</description></item>
///     </list>
/// </remarks>
internal static class QwpGorilla
{
    /// <summary>Encoding-flag byte signalling uncompressed values follow.</summary>
    public const byte EncodingUncompressed = 0x00;

    /// <summary>Encoding-flag byte signalling a Gorilla bit stream follows.</summary>
    public const byte EncodingGorilla = 0x01;

    /// <summary>
    ///     Worst-case byte count for a Gorilla-encoded column with <paramref name="valueCount" /> values.
    /// </summary>
    public static int MaxEncodedSize(int valueCount)
    {
        if (valueCount <= 0)
        {
            return 1;
        }

        if (valueCount == 1)
        {
            return UncompressedSize(valueCount);
        }

        // 1 (encoding flag) + 16 (first + second) + ceil((N - 2) × 36 / 8) for the worst-case fallback bucket.
        var dodBytes = ((valueCount - 2) * 36 + 7) / 8;
        var gorilla = 1 + 16 + dodBytes;
        return Math.Max(gorilla, UncompressedSize(valueCount));
    }

    /// <summary>Size of the uncompressed encoding for <paramref name="valueCount" /> values.</summary>
    public static int UncompressedSize(int valueCount)
    {
        return 1 + 8 * valueCount;
    }

    /// <summary>
    ///     Encodes <paramref name="timestamps" /> into <paramref name="dest" />. Picks Gorilla or
    ///     uncompressed automatically; falls back to uncompressed on int32 DoD overflow or when
    ///     fewer than 3 values are present.
    /// </summary>
    /// <returns>Number of bytes written into <paramref name="dest" />.</returns>
    public static int Encode(Span<byte> dest, ReadOnlySpan<long> timestamps)
    {
        if (timestamps.Length < 3)
        {
            return EncodeUncompressed(dest, timestamps);
        }

        var gorillaSize = TryEncodeGorilla(dest, timestamps);
        if (gorillaSize >= 0)
        {
            return gorillaSize;
        }

        return EncodeUncompressed(dest, timestamps);
    }

    /// <summary>
    ///     Decodes a Gorilla / uncompressed timestamp column. The caller supplies the expected
    ///     <paramref name="valueCount" /> from the row count − null count.
    /// </summary>
    /// <returns>Number of source bytes consumed.</returns>
    public static int Decode(ReadOnlySpan<byte> source, Span<long> dest, int valueCount)
    {
        if (valueCount <= 0)
        {
            return 0;
        }

        if (source.Length < 1)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "Gorilla source truncated: missing encoding flag");
        }

        var flag = source[0];
        if (flag == EncodingUncompressed)
        {
            return DecodeUncompressed(source, dest, valueCount);
        }

        if (flag != EncodingGorilla)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"Gorilla source: unknown encoding flag 0x{flag:X2}");
        }

        return DecodeGorilla(source, dest, valueCount);
    }

    /// <summary>
    ///     Decodes into a little-endian <see cref="byte" /> destination — endianness-stable on big-endian
    ///     platforms unlike the <see cref="Span{T}" /> overload which writes through native-endian
    ///     <c>long</c> storage. Use this when the caller wants to interpret the result through
    ///     <c>BinaryPrimitives.ReadInt64LittleEndian</c>.
    /// </summary>
    public static int DecodeToBytes(ReadOnlySpan<byte> source, Span<byte> dest, int valueCount)
    {
        if (valueCount <= 0)
        {
            return 0;
        }

        if (source.Length < 1)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "Gorilla source truncated: missing encoding flag");
        }

        var flag = source[0];
        if (flag == EncodingUncompressed)
        {
            return DecodeUncompressedToBytes(source, dest, valueCount);
        }

        if (flag != EncodingGorilla)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"Gorilla source: unknown encoding flag 0x{flag:X2}");
        }

        return DecodeGorillaToBytes(source, dest, valueCount);
    }

    private static int DecodeUncompressedToBytes(ReadOnlySpan<byte> source, Span<byte> dest, int valueCount)
    {
        var expected = 1 + valueCount * 8;
        if (source.Length < expected)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"uncompressed timestamp column truncated: need {expected} bytes, have {source.Length}");
        }
        if (dest.Length < valueCount * 8)
        {
            throw new ArgumentException("dest too small", nameof(dest));
        }
        source.Slice(1, valueCount * 8).CopyTo(dest);
        return expected;
    }

    private static int DecodeGorillaToBytes(ReadOnlySpan<byte> source, Span<byte> dest, int valueCount)
    {
        if (valueCount < 2)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                "Gorilla-encoded column requires at least two timestamps");
        }

        if (source.Length < 17)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                "Gorilla source truncated: missing first/second timestamps");
        }
        if (dest.Length < valueCount * 8)
        {
            throw new ArgumentException("dest too small", nameof(dest));
        }

        var t0 = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(1, 8));
        var t1 = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(9, 8));
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(0, 8), t0);
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(8, 8), t1);

        var reader = new QwpBitReader(source, 17);
        var prevDelta = t1 - t0;
        var prev = t1;

        for (var i = 2; i < valueCount; i++)
        {
            var dod = DecodeDoD(ref reader);
            var delta = prevDelta + dod;
            var val = prev + delta;
            BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(i * 8, 8), val);
            prevDelta = delta;
            prev = val;
        }

        return reader.BytePosition;
    }

    private static int EncodeUncompressed(Span<byte> dest, ReadOnlySpan<long> timestamps)
    {
        dest[0] = EncodingUncompressed;
        for (var i = 0; i < timestamps.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(1 + i * 8, 8), timestamps[i]);
        }

        return 1 + timestamps.Length * 8;
    }

    /// <summary>
    ///     Tries Gorilla encoding. Returns the byte count, or <c>-1</c> if any DoD overflows int32.
    /// </summary>
    private static int TryEncodeGorilla(Span<byte> dest, ReadOnlySpan<long> timestamps)
    {
        dest[0] = EncodingGorilla;
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(1, 8), timestamps[0]);
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(9, 8), timestamps[1]);

        var writer = new QwpBitWriter(dest, 17);
        var prevDelta = timestamps[1] - timestamps[0];

        for (var i = 2; i < timestamps.Length; i++)
        {
            var delta = timestamps[i] - timestamps[i - 1];
            var dod = delta - prevDelta;
            if (dod < int.MinValue || dod > int.MaxValue)
            {
                return -1;
            }

            EncodeDoD(ref writer, (int)dod);
            prevDelta = delta;
        }

        return 17 + writer.FinishToByteBoundary();
    }

    private static void EncodeDoD(ref QwpBitWriter writer, int dod)
    {
        if (dod == 0)
        {
            writer.WriteBits(0, 1);
            return;
        }

        if (dod >= -64 && dod <= 63)
        {
            // prefix '10' LSB-first → bits 1, 0 → ulong value 0b01 = 1
            writer.WriteBits(0b01, 2);
            writer.WriteBits((ulong)dod & 0x7FUL, 7);
            return;
        }

        if (dod >= -256 && dod <= 255)
        {
            // prefix '110' LSB-first → bits 1, 1, 0 → ulong value 0b011 = 3
            writer.WriteBits(0b011, 3);
            writer.WriteBits((ulong)dod & 0x1FFUL, 9);
            return;
        }

        if (dod >= -2048 && dod <= 2047)
        {
            // prefix '1110' LSB-first → bits 1, 1, 1, 0 → ulong value 0b0111 = 7
            writer.WriteBits(0b0111, 4);
            writer.WriteBits((ulong)dod & 0xFFFUL, 12);
            return;
        }

        // prefix '1111' → ulong value 0b1111 = 15
        writer.WriteBits(0b1111, 4);
        writer.WriteBits((uint)dod, 32);
    }

    private static int DecodeUncompressed(ReadOnlySpan<byte> source, Span<long> dest, int valueCount)
    {
        var expected = 1 + valueCount * 8;
        if (source.Length < expected)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"uncompressed timestamp column truncated: need {expected} bytes, have {source.Length}");
        }

        for (var i = 0; i < valueCount; i++)
        {
            dest[i] = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(1 + i * 8, 8));
        }

        return expected;
    }

    private static int DecodeGorilla(ReadOnlySpan<byte> source, Span<long> dest, int valueCount)
    {
        if (valueCount < 2)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                "Gorilla-encoded column requires at least two timestamps");
        }

        if (source.Length < 17)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                "Gorilla source truncated: missing first/second timestamps");
        }

        dest[0] = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(1, 8));
        dest[1] = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(9, 8));

        var reader = new QwpBitReader(source, 17);
        var prevDelta = dest[1] - dest[0];

        for (var i = 2; i < valueCount; i++)
        {
            var dod = DecodeDoD(ref reader);
            var delta = prevDelta + dod;
            dest[i] = dest[i - 1] + delta;
            prevDelta = delta;
        }

        return reader.BytePosition;
    }

    private static long DecodeDoD(ref QwpBitReader reader)
    {
        if (reader.ReadBits(1) == 0)
        {
            return 0;
        }

        if (reader.ReadBits(1) == 0)
        {
            // prefix '10' → 7-bit signed
            return SignExtend(reader.ReadBits(7), 7);
        }

        if (reader.ReadBits(1) == 0)
        {
            // prefix '110' → 9-bit signed
            return SignExtend(reader.ReadBits(9), 9);
        }

        if (reader.ReadBits(1) == 0)
        {
            // prefix '1110' → 12-bit signed
            return SignExtend(reader.ReadBits(12), 12);
        }

        // prefix '1111' → 32-bit signed
        return (int)reader.ReadBits(32);
    }

    private static long SignExtend(ulong raw, int bitCount)
    {
        var sign = 1UL << (bitCount - 1);
        if ((raw & sign) != 0)
        {
            return (long)raw - (long)(1UL << bitCount);
        }

        return (long)raw;
    }
}
