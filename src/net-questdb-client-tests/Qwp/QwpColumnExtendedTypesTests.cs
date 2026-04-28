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
using System.Numerics;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpColumnExtendedTypesTests
{
    // -- DECIMAL128 --------------------------------------------------------------

    [Test]
    public void AppendDecimal128_PositiveValue_WritesUnscaledLittleEndian()
    {
        // 12.34m has unscaled = 1234, scale = 2.
        var col = new QwpColumn("price", 0);
        col.AppendDecimal128(12.34m);

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.Decimal128));
        Assert.That(col.DecimalScale, Is.EqualTo((byte)2));
        Assert.That(col.DecimalScaleSet, Is.True);
        Assert.That(col.FixedLen, Is.EqualTo(16));

        var unscaled = ReadInt128(col.FixedData!.AsSpan(0, 16));
        Assert.That(unscaled, Is.EqualTo(new BigInteger(1234)));
    }

    [Test]
    public void AppendDecimal128_NegativeValue_IsTwosComplement()
    {
        var col = new QwpColumn("delta", 0);
        col.AppendDecimal128(-1.5m);

        Assert.That(col.DecimalScale, Is.EqualTo((byte)1));

        var unscaled = ReadInt128(col.FixedData!.AsSpan(0, 16));
        Assert.That(unscaled, Is.EqualTo(new BigInteger(-15)));
    }

    [Test]
    public void AppendDecimal128_Zero_WritesAllZeroes()
    {
        var col = new QwpColumn("z", 0);
        col.AppendDecimal128(0m);

        Assert.That(col.DecimalScale, Is.EqualTo((byte)0));
        var bytes = col.FixedData!.AsSpan(0, 16).ToArray();
        Assert.That(bytes, Is.EqualTo(new byte[16]));
    }

    [Test]
    public void AppendDecimal128_ScaleMismatch_Throws()
    {
        var col = new QwpColumn("p", 0);
        col.AppendDecimal128(1.5m); // scale 1
        Assert.Throws<IngressError>(() => col.AppendDecimal128(2.05m)); // scale 2
    }

    [Test]
    public void AppendDecimal128_LargePositiveValue_RoundTrips()
    {
        // .NET decimal max ≈ 7.9e28, scale 0.
        var col = new QwpColumn("big", 0);
        col.AppendDecimal128(decimal.MaxValue);

        var unscaled = ReadInt128(col.FixedData!.AsSpan(0, 16));
        // decimal.MaxValue = 79228162514264337593543950335 = 2^96 - 1.
        Assert.That(unscaled, Is.EqualTo((BigInteger.One << 96) - 1));
    }

    [Test]
    public void AppendDecimal128_LargeNegativeValue_RoundTrips()
    {
        var col = new QwpColumn("big", 0);
        col.AppendDecimal128(decimal.MinValue);

        var unscaled = ReadInt128(col.FixedData!.AsSpan(0, 16));
        Assert.That(unscaled, Is.EqualTo(-((BigInteger.One << 96) - 1)));
    }

    // -- LONG256 -----------------------------------------------------------------

    [Test]
    public void AppendLong256_SmallValue_PadsTo32Bytes()
    {
        var col = new QwpColumn("hash", 0);
        col.AppendLong256(new BigInteger(0x123456789ABCDEF0));

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.Long256));
        Assert.That(col.FixedLen, Is.EqualTo(32));

        var span = col.FixedData!.AsSpan(0, 32);
        // Low 8 bytes: LE encoding of 0x123456789ABCDEF0 = F0 DE BC 9A 78 56 34 12.
        Assert.That(BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(0, 8)),
            Is.EqualTo(0x123456789ABCDEF0UL));
        // Upper 24 bytes are zero.
        for (var i = 8; i < 32; i++)
        {
            Assert.That(span[i], Is.Zero);
        }
    }

    [Test]
    public void AppendLong256_MaxValue_FillsAll32Bytes()
    {
        var col = new QwpColumn("hash", 0);
        var max = (BigInteger.One << 256) - 1;
        col.AppendLong256(max);

        var span = col.FixedData!.AsSpan(0, 32);
        for (var i = 0; i < 32; i++)
        {
            Assert.That(span[i], Is.EqualTo((byte)0xFF), $"byte {i}");
        }
    }

    [Test]
    public void AppendLong256_Negative_Throws()
    {
        var col = new QwpColumn("hash", 0);
        Assert.Throws<IngressError>(() => col.AppendLong256(new BigInteger(-1)));
    }

    [Test]
    public void AppendLong256_TooLarge_Throws()
    {
        var col = new QwpColumn("hash", 0);
        var tooBig = BigInteger.One << 256;
        Assert.Throws<IngressError>(() => col.AppendLong256(tooBig));
    }

    [Test]
    public void AppendLong256_StaleBytesAreCleared()
    {
        // Pre-fill FixedData with garbage to ensure padding actually clears.
        var col = new QwpColumn("hash", 0);
        col.AppendLong256((BigInteger.One << 256) - 1);
        col.Clear();

        col.AppendLong256(new BigInteger(0x42));
        Assert.That(col.FixedData!.AsSpan(0, 32).ToArray()[1..], Is.All.Zero);
        Assert.That(col.FixedData![0], Is.EqualTo((byte)0x42));
    }

    // -- GEOHASH -----------------------------------------------------------------

    [Test]
    public void AppendGeohash_LocksPrecision()
    {
        var col = new QwpColumn("loc", 0);
        col.AppendGeohash(0xABCDEF, 24);

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.Geohash));
        Assert.That(col.GeohashPrecisionBits, Is.EqualTo(24));
        Assert.That(col.GeohashPrecisionSet, Is.True);
        Assert.That(col.FixedLen, Is.EqualTo(3), "ceil(24/8) = 3 bytes");

        var span = col.FixedData!.AsSpan(0, 3);
        Assert.That(span[0], Is.EqualTo(0xEF));
        Assert.That(span[1], Is.EqualTo(0xCD));
        Assert.That(span[2], Is.EqualTo(0xAB));
    }

    [Test]
    public void AppendGeohash_PrecisionMismatch_Throws()
    {
        var col = new QwpColumn("loc", 0);
        col.AppendGeohash(1, 24);
        Assert.Throws<IngressError>(() => col.AppendGeohash(2, 32));
    }

    [Test]
    public void AppendGeohash_OutOfRangePrecision_Throws()
    {
        var col = new QwpColumn("loc", 0);
        Assert.Throws<IngressError>(() => col.AppendGeohash(0, 0));

        var col2 = new QwpColumn("loc", 0);
        Assert.Throws<IngressError>(() => col2.AppendGeohash(0, 61));
    }

    [Test]
    public void AppendGeohash_OddPrecisionBits_StillWorks()
    {
        // 7 bits → ceil(7/8) = 1 byte, low 7 bits of value preserved.
        var col = new QwpColumn("loc", 0);
        col.AppendGeohash(0b1010101, 7);

        Assert.That(col.FixedLen, Is.EqualTo(1));
        Assert.That(col.FixedData![0], Is.EqualTo(0b1010101));
    }

    [Test]
    public void AppendGeohash_60Bits_Uses8Bytes()
    {
        var col = new QwpColumn("loc", 0);
        col.AppendGeohash(0x0FEDCBA987654321UL, 60);

        Assert.That(col.FixedLen, Is.EqualTo(8));
        var hash = BinaryPrimitives.ReadUInt64LittleEndian(col.FixedData!.AsSpan(0, 8));
        Assert.That(hash, Is.EqualTo(0x0FEDCBA987654321UL));
    }

    // -- DOUBLE_ARRAY ------------------------------------------------------------

    [Test]
    public void AppendDoubleArray_1D_WritesNDimsShapeAndValues()
    {
        var col = new QwpColumn("vec", 0);
        col.AppendDoubleArray(new double[] { 1.0, 2.0, 3.0 }, new[] { 3 });

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.DoubleArray));
        Assert.That(col.FixedLen, Is.EqualTo(1 + 4 + 3 * 8));

        var span = col.FixedData!.AsSpan(0, col.FixedLen);
        Assert.That(span[0], Is.EqualTo((byte)1), "n_dims = 1");
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1, 4)), Is.EqualTo(3));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(5, 8)), Is.EqualTo(1.0));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(13, 8)), Is.EqualTo(2.0));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(21, 8)), Is.EqualTo(3.0));
    }

    [Test]
    public void AppendDoubleArray_2D_RoundTripsShape()
    {
        var col = new QwpColumn("mat", 0);
        // 2x3 matrix
        col.AppendDoubleArray(new double[] { 1, 2, 3, 4, 5, 6 }, new[] { 2, 3 });

        var span = col.FixedData!.AsSpan(0, col.FixedLen);
        Assert.That(span[0], Is.EqualTo((byte)2));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(1, 4)), Is.EqualTo(2));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(5, 4)), Is.EqualTo(3));
        // 6 values × 8 bytes = 48 bytes after 9-byte header.
        Assert.That(col.FixedLen, Is.EqualTo(9 + 48));
    }

    [Test]
    public void AppendDoubleArray_ShapeMismatch_Throws()
    {
        var col = new QwpColumn("v", 0);
        Assert.Throws<IngressError>(() =>
            col.AppendDoubleArray(new double[] { 1, 2, 3 }, new[] { 2 })); // shape product=2 but 3 values
    }

    [Test]
    public void AppendDoubleArray_TooManyDimensions_Throws()
    {
        var col = new QwpColumn("v", 0);
        var shape = new int[QwpConstants.MaxArrayDimensions + 1];
        Array.Fill(shape, 1);
        Assert.Throws<IngressError>(() => col.AppendDoubleArray(new double[1], shape));
    }

    [Test]
    public void AppendDoubleArray_EmptyShape_Throws()
    {
        var col = new QwpColumn("v", 0);
        Assert.Throws<IngressError>(() =>
            col.AppendDoubleArray(ReadOnlySpan<double>.Empty, ReadOnlySpan<int>.Empty));
    }

    [Test]
    public void AppendDoubleArray_ZeroLengthDimension_IsAllowed()
    {
        var col = new QwpColumn("v", 0);
        col.AppendDoubleArray(ReadOnlySpan<double>.Empty, new[] { 0 });

        Assert.That(col.NonNullCount, Is.EqualTo(1));
        // 1 byte n_dims + 4 bytes dim_len(0) + 0 bytes values = 5 bytes.
        Assert.That(col.FixedLen, Is.EqualTo(5));
    }

    [Test]
    public void AppendDoubleArray_MultipleRows_PackBackToBack()
    {
        var col = new QwpColumn("v", 0);
        col.AppendDoubleArray(new double[] { 1.0 }, new[] { 1 });
        col.AppendDoubleArray(new double[] { 2.0, 3.0 }, new[] { 2 });

        // First row: 5 + 8 = 13 bytes; second row: 5 + 16 = 21 bytes; total 34.
        Assert.That(col.FixedLen, Is.EqualTo(13 + 21));
        Assert.That(col.NonNullCount, Is.EqualTo(2));
    }

    [Test]
    public void AppendLongArray_1D_WritesValues()
    {
        var col = new QwpColumn("v", 0);
        col.AppendLongArray(new long[] { 100, -200, 300 }, new[] { 3 });

        Assert.That(col.TypeCode, Is.EqualTo(QwpTypeCode.LongArray));
        var span = col.FixedData!.AsSpan(0, col.FixedLen);
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(5, 8)), Is.EqualTo(100L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(13, 8)), Is.EqualTo(-200L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(21, 8)), Is.EqualTo(300L));
    }

    [Test]
    public void Clear_ResetsDecimalScaleAndGeohashPrecision()
    {
        var col = new QwpColumn("c", 0);
        col.AppendDecimal128(1.5m);
        Assert.That(col.DecimalScaleSet, Is.True);
        col.Clear();
        Assert.That(col.DecimalScaleSet, Is.False);

        // After Clear, a value with a different scale is accepted.
        col.AppendDecimal128(2.55m);
        Assert.That(col.DecimalScale, Is.EqualTo((byte)2));
    }

    // -- Helpers ----------------------------------------------------------------

    /// <summary>Reads a 16-byte little-endian two's-complement integer.</summary>
    private static BigInteger ReadInt128(ReadOnlySpan<byte> bytes)
    {
        // BigInteger ctor takes signed two's-complement when isUnsigned=false,
        // and is little-endian when isBigEndian=false (matches the wire format).
        return new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
    }
}
