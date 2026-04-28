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

using System.Runtime.CompilerServices;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Unsigned LEB128 varint codec used throughout QWP for variable-length integers.
/// </summary>
/// <remarks>
///     Wire layout: 7 data bits per byte, LSB first; the high bit on every byte except the last
///     signals "more bytes follow". A 64-bit value occupies at most <see cref="MaxBytes" /> bytes.
///     <para />
///     The QWP spec uses unsigned LEB128 even when representing logically signed values
///     (schema ids, delta-start counters, etc.) — every actual use is non-negative.
/// </remarks>
internal static class QwpVarint
{
    /// <summary>Maximum encoded length of a 64-bit value: <c>ceil(64/7) = 10</c> bytes.</summary>
    public const int MaxBytes = 10;

    /// <summary>
    ///     Writes <paramref name="value" /> into <paramref name="dest" /> in LEB128 form.
    /// </summary>
    /// <returns>Number of bytes written.</returns>
    /// <exception cref="ArgumentException">If <paramref name="dest" /> is too small.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Write(Span<byte> dest, ulong value)
    {
        var i = 0;
        while ((value & ~0x7Ful) != 0)
        {
            if ((uint)i >= (uint)dest.Length)
            {
                throw new ArgumentException("destination span too small for varint", nameof(dest));
            }

            dest[i++] = (byte)((value & 0x7F) | 0x80);
            value >>= 7;
        }

        if ((uint)i >= (uint)dest.Length)
        {
            throw new ArgumentException("destination span too small for varint", nameof(dest));
        }

        dest[i++] = (byte)value;
        return i;
    }

    /// <summary>
    ///     Reads an LEB128-encoded value from <paramref name="src" />.
    /// </summary>
    /// <param name="src">Buffer to read from. The caller is expected to have ensured at least one byte is available.</param>
    /// <param name="bytesRead">Number of bytes consumed.</param>
    /// <returns>The decoded value.</returns>
    /// <exception cref="IngressError">
    ///     If the encoding runs past <see cref="MaxBytes" /> bytes, or the input ends mid-value.
    /// </exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Read(ReadOnlySpan<byte> src, out int bytesRead)
    {
        ulong result = 0;
        var shift = 0;

        for (var i = 0; i < MaxBytes; i++)
        {
            if (i >= src.Length)
            {
                throw new IngressError(ErrorCode.ProtocolVersionError, "varint truncated");
            }

            var b = src[i];
            result |= (ulong)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
            {
                bytesRead = i + 1;
                return result;
            }

            shift += 7;
        }

        throw new IngressError(ErrorCode.ProtocolVersionError, "varint exceeds 10 bytes");
    }

    /// <summary>
    ///     Returns the number of bytes <see cref="Write" /> would emit for <paramref name="value" />.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetByteCount(ulong value)
    {
        var n = 1;
        while ((value & ~0x7Ful) != 0)
        {
            value >>= 7;
            n++;
        }

        return n;
    }
}
