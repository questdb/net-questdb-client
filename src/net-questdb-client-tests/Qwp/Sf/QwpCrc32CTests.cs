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

using System.Text;
using NUnit.Framework;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpCrc32CTests
{
    [Test]
    public void Compute_StandardVector_Matches()
    {
        // Well-known test vector from RFC 3720 / iSCSI, also used by Intel's CRC32C SSE 4.2 docs.
        var input = Encoding.ASCII.GetBytes("123456789");
        Assert.That(QwpCrc32C.Compute(input), Is.EqualTo(0xE3069283u));
    }

    [Test]
    public void Compute_EmptyInput_IsZero()
    {
        // CRC32C of empty input: standard produces 0 (after final XOR).
        Assert.That(QwpCrc32C.Compute(ReadOnlySpan<byte>.Empty), Is.EqualTo(0u));
    }

    [Test]
    public void Compute_SingleByte_KnownVector()
    {
        // CRC32C of a single 'a' (0x61) byte.
        // Cross-checked against reference impls: 0xC1D04330.
        var input = new byte[] { (byte)'a' };
        Assert.That(QwpCrc32C.Compute(input), Is.EqualTo(0xC1D04330u));
    }

    [Test]
    public void Compute_AllZeros_KnownLengths()
    {
        // For all-zero inputs, CRC32C also produces well-known values.
        // 32 zero bytes: 0x8A9136AA per Botan / Crypto++ test vectors.
        var input = new byte[32];
        Assert.That(QwpCrc32C.Compute(input), Is.EqualTo(0x8A9136AAu));
    }

    [Test]
    public void Compute_AllOnes_32Bytes()
    {
        // 32 0xFF bytes: 0x62A8AB43.
        var input = new byte[32];
        Array.Fill(input, (byte)0xFF);
        Assert.That(QwpCrc32C.Compute(input), Is.EqualTo(0x62A8AB43u));
    }

    [Test]
    public void Compute_Chaining_EquivalentToSingleCall()
    {
        var rnd = new Random(0xCAFE);
        var data = new byte[1000];
        rnd.NextBytes(data);

        var oneShot = QwpCrc32C.Compute(data);

        for (var split = 0; split <= data.Length; split += 17)
        {
            var first = QwpCrc32C.Compute(data.AsSpan(0, split));
            var chained = QwpCrc32C.Compute(data.AsSpan(split), first);
            Assert.That(chained, Is.EqualTo(oneShot), $"split at {split}");
        }
    }

    [Test]
    public void Compute_SingleBitFlip_ChangesChecksum()
    {
        var rnd = new Random(0xFEED);
        var data = new byte[256];
        rnd.NextBytes(data);

        var baseline = QwpCrc32C.Compute(data);

        for (var bytePos = 0; bytePos < data.Length; bytePos++)
        {
            for (var bit = 0; bit < 8; bit++)
            {
                data[bytePos] ^= (byte)(1 << bit);
                Assert.That(QwpCrc32C.Compute(data), Is.Not.EqualTo(baseline),
                    $"flipping bit {bit} of byte {bytePos} did not change the checksum");
                data[bytePos] ^= (byte)(1 << bit); // restore
            }
        }
    }

    [Test]
    public void Compute_VaryingLengths_NoBoundaryBugs()
    {
        // Specifically exercises the slice-by-8 loop boundary at lengths around 8, 16, 24, etc.
        var rnd = new Random(0xBEEF);
        for (var n = 0; n < 40; n++)
        {
            var data = new byte[n];
            rnd.NextBytes(data);

            // Compute with the slice-by-8 path and with byte-by-byte chaining; must match.
            var fast = QwpCrc32C.Compute(data);
            var bytewise = 0u;
            for (var i = 0; i < n; i++)
            {
                bytewise = QwpCrc32C.Compute(data.AsSpan(i, 1), bytewise);
            }

            Assert.That(fast, Is.EqualTo(bytewise), $"length {n}");
        }
    }
}
