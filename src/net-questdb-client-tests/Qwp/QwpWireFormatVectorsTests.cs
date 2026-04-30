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

using System.Globalization;
using System.Numerics;
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp;

/// <summary>
///     Pinned byte-exact wire-format vectors. Any change to a column-level encoder must
///     keep these stable or it will break interop with other clients on the same connection.
/// </summary>
[TestFixture]
public class QwpWireFormatVectorsTests
{
    [TestCase("0", (byte)0,
        new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
    [TestCase("123.456", (byte)3,
        new byte[] { 0x40, 0xE2, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
    [TestCase("-1", (byte)0,
        new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
    [TestCase("-79228162514264337593543950335", (byte)0,
        new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF })]
    [TestCase("79228162514264337593543950335", (byte)0,
        new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00 })]
    public void Decimal128_Vector_PinnedBytes(string decimalText, byte expectedScale, byte[] expectedBytes)
    {
        var col = new QwpColumn("d", 0);
        col.AppendDecimal128(decimal.Parse(decimalText, CultureInfo.InvariantCulture));

        Assert.That(col.DecimalScale, Is.EqualTo(expectedScale));
        Assert.That(col.FixedLen, Is.EqualTo(16));
        Assert.That(col.FixedData!.AsSpan(0, 16).ToArray(), Is.EqualTo(expectedBytes));
    }

    [TestCase("00112233-4455-6677-8899-aabbccddeeff",
        new byte[] { 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00 })]
    [TestCase("00000000-0000-0000-0000-000000000000",
        new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
    [TestCase("ffffffff-ffff-ffff-ffff-ffffffffffff",
        new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
    [TestCase("01020304-0506-0708-090a-0b0c0d0e0f10",
        new byte[] { 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 })]
    public void Uuid_Vector_PinnedBytes(string guidText, byte[] expectedBytes)
    {
        var col = new QwpColumn("u", 0);
        col.AppendUuid(Guid.Parse(guidText));

        Assert.That(col.FixedLen, Is.EqualTo(16));
        Assert.That(col.FixedData!.AsSpan(0, 16).ToArray(), Is.EqualTo(expectedBytes));
    }

    [TestCase("0",
        new byte[] {
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
    [TestCase("1",
        new byte[] {
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
    [TestCase("00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDDEEFF",
        new byte[] {
            0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00,
            0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00 })]
    [TestCase("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        new byte[] {
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
    public void Long256_Vector_PinnedBytes(string hexOrDecimal, byte[] expectedBytes)
    {
        // Test cases above are either short decimals ("0", "1") or full 64-char hex.
        var value = hexOrDecimal.Length > 4
            ? BigInteger.Parse("0" + hexOrDecimal, NumberStyles.HexNumber, CultureInfo.InvariantCulture)
            : BigInteger.Parse(hexOrDecimal, CultureInfo.InvariantCulture);

        var col = new QwpColumn("l", 0);
        col.AppendLong256(value);

        Assert.That(col.FixedLen, Is.EqualTo(32));
        Assert.That(col.FixedData!.AsSpan(0, 32).ToArray(), Is.EqualTo(expectedBytes));
    }

    [TestCase(new byte[] { }, 0x00000000u)]
    [TestCase(new byte[] { 0x00 }, 0x527D5351u)]
    [TestCase(new byte[] { (byte)'a' }, 0xC1D04330u)]
    [TestCase(new byte[] { 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39 }, 0xE3069283u)]
    [TestCase(new byte[] { 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64 }, 0xC99465AAu)]
    public void Crc32C_Vector_MatchesStandard(byte[] input, uint expected)
    {
        Assert.That(QwpCrc32C.Compute(input), Is.EqualTo(expected));
    }
}
