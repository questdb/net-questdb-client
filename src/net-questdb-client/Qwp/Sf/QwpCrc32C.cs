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
using System.Runtime.CompilerServices;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     CRC32C (Castagnoli) checksum, software slice-by-8 implementation.
/// </summary>
/// <remarks>
///     Polynomial <c>0x1EDC6F41</c> (reflected: <c>0x82F63B78</c>); seed and final XOR are
///     <c>0xFFFFFFFF</c>; input and output are reflected.
///     <para />
///     Used by the store-and-forward segment envelope (<c>[u32 crc | u32 frame_len | frame bytes]</c>).
///     The implementation deliberately avoids <c>System.IO.Hashing.Crc32C</c> and the SSE 4.2 / arm64
///     intrinsics so behaviour is identical across runtime versions and CPUs.
///     <para />
///     <b>Validation</b>: matches the standard test vector <c>CRC32C("123456789") == 0xE3069283</c>
///     and Java's <c>Crc32c.java</c> output byte-for-byte.
/// </remarks>
internal static class QwpCrc32C
{
    private const uint Polynomial = 0x82F63B78u;

    private static readonly uint[][] Tables = BuildSliceBy8Tables();

    /// <summary>Computes the CRC32C of <paramref name="data" />.</summary>
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        return Compute(data, 0u);
    }

    /// <summary>
    ///     Computes the CRC32C of <paramref name="data" /> chained from a previous result.
    /// </summary>
    /// <param name="data">Bytes to checksum.</param>
    /// <param name="seed">Previous CRC32C output, or <c>0</c> for a fresh checksum.</param>
    public static uint Compute(ReadOnlySpan<byte> data, uint seed)
    {
        // CRC32 convention: invert the running register before processing and again afterwards so
        // that chaining (Compute(b, Compute(a))) gives Compute(a + b).
        var crc = ~seed;

        var i = 0;
        var len = data.Length;

        // Slice-by-8: process 8 bytes per iteration. Reads are little-endian to match the way the
        // tables were derived.
        while (i + 8 <= len)
        {
            var word0 = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i, 4)) ^ crc;
            var word1 = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i + 4, 4));
            crc = Tables[7][word0 & 0xFF]
                ^ Tables[6][(word0 >> 8) & 0xFF]
                ^ Tables[5][(word0 >> 16) & 0xFF]
                ^ Tables[4][(word0 >> 24) & 0xFF]
                ^ Tables[3][word1 & 0xFF]
                ^ Tables[2][(word1 >> 8) & 0xFF]
                ^ Tables[1][(word1 >> 16) & 0xFF]
                ^ Tables[0][(word1 >> 24) & 0xFF];
            i += 8;
        }

        // Tail bytes processed one at a time.
        var t0 = Tables[0];
        for (; i < len; i++)
        {
            crc = t0[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        }

        return ~crc;
    }

    /// <summary>
    ///     Step a single byte. Used by the segment-replay code where we want to verify checksums
    ///     incrementally without re-reading the entire frame.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint UpdateByte(uint runningCrc, byte value)
    {
        // runningCrc here is the *raw* register state (not the user-visible Compute output).
        // Callers chaining via Compute should `~Compute(prev)` to get the register, step bytes,
        // and `~register` to get the final value.
        return Tables[0][(runningCrc ^ value) & 0xFF] ^ (runningCrc >> 8);
    }

    private static uint[][] BuildSliceBy8Tables()
    {
        var tables = new uint[8][];
        for (var i = 0; i < 8; i++)
        {
            tables[i] = new uint[256];
        }

        // Table 0: standard CRC32C byte-by-byte table (reflected polynomial).
        for (uint i = 0; i < 256; i++)
        {
            var c = i;
            for (var j = 0; j < 8; j++)
            {
                c = (c & 1) != 0 ? (c >> 1) ^ Polynomial : c >> 1;
            }

            tables[0][i] = c;
        }

        // Tables 1..7: T_n[b] = T_(n-1)[T_(n-1)[b] & 0xFF] ^ ... no — derive via the standard
        // recurrence for slice-by-N: T_n[b] = T_(n-1)[b] right-shifted one byte, XOR'd with the
        // standard table indexed by the low byte that fell off.
        for (uint i = 0; i < 256; i++)
        {
            var c = tables[0][i];
            for (var t = 1; t < 8; t++)
            {
                c = tables[0][c & 0xFF] ^ (c >> 8);
                tables[t][i] = c;
            }
        }

        return tables;
    }
}
