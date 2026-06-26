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
using QuestDB.Enums;
using QuestDB.Qwp.Query;

namespace net_questdb_client_tests.Qwp.Query;

/// <summary>
///     Pinned byte-exact bind-payload vectors. Hand-rolled little-endian expected bytes per
///     wire type; any drift here is a wire-format regression that breaks egress interop.
/// </summary>
[TestFixture]
public class QwpBindValuesVectorsTests
{
    private const byte NonNull = 0x00;
    private const byte NullFlag = 0x01;
    private const byte NullBitmap = 0x01;

    private const byte TypeBoolean = 0x01;
    private const byte TypeByte = 0x02;
    private const byte TypeShort = 0x03;
    private const byte TypeInt = 0x04;
    private const byte TypeLong = 0x05;
    private const byte TypeFloat = 0x06;
    private const byte TypeDouble = 0x07;
    private const byte TypeTimestamp = 0x0A;
    private const byte TypeDate = 0x0B;
    private const byte TypeUuid = 0x0C;
    private const byte TypeLong256 = 0x0D;
    private const byte TypeGeohash = 0x0E;
    private const byte TypeVarchar = 0x0F;
    private const byte TypeTimestampNanos = 0x10;
    private const byte TypeDecimal64 = 0x13;
    private const byte TypeDecimal128 = 0x14;
    private const byte TypeDecimal256 = 0x15;
    private const byte TypeChar = 0x16;

    [Test]
    public void EncodeBoolean_True_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetBoolean(0, true);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeBoolean);
            w.Add(NonNull);
            w.Add(0x01);
        }));
    }

    [Test]
    public void EncodeBoolean_False_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetBoolean(0, false);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeBoolean);
            w.Add(NonNull);
            w.Add(0x00);
        }));
    }

    [Test]
    public void EncodeByte_BoundaryValues_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetByte(0, unchecked((byte)-128));
        b.SetByte(1, 0);
        b.SetByte(2, 127);
        AssertBytes(b, 3, BuildExpected(w =>
        {
            w.Add(TypeByte); w.Add(NonNull); w.Add(0x80);
            w.Add(TypeByte); w.Add(NonNull); w.Add(0x00);
            w.Add(TypeByte); w.Add(NonNull); w.Add(0x7F);
        }));
    }

    [Test]
    public void EncodeShort_BoundaryValues_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetShort(0, short.MinValue);
        b.SetShort(1, 0);
        b.SetShort(2, short.MaxValue);
        AssertBytes(b, 3, BuildExpected(w =>
        {
            w.Add(TypeShort); w.Add(NonNull); w.AddI16Le(short.MinValue);
            w.Add(TypeShort); w.Add(NonNull); w.AddI16Le(0);
            w.Add(TypeShort); w.Add(NonNull); w.AddI16Le(short.MaxValue);
        }));
    }

    [Test]
    public void EncodeChar_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetChar(0, 'Z');
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeChar);
            w.Add(NonNull);
            w.AddU16Le('Z');
        }));
    }

    [Test]
    public void EncodeInt_BoundaryValues_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetInt(0, int.MinValue);
        b.SetInt(1, 0);
        b.SetInt(2, int.MaxValue);
        AssertBytes(b, 3, BuildExpected(w =>
        {
            w.Add(TypeInt); w.Add(NonNull); w.AddI32Le(int.MinValue);
            w.Add(TypeInt); w.Add(NonNull); w.AddI32Le(0);
            w.Add(TypeInt); w.Add(NonNull); w.AddI32Le(int.MaxValue);
        }));
    }

    [Test]
    public void EncodeLong_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetLong(0, 42L);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeLong);
            w.Add(NonNull);
            w.AddI64Le(42L);
        }));
    }

    [Test]
    public void EncodeFloat_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetFloat(0, 3.14f);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeFloat);
            w.Add(NonNull);
            w.AddI32Le(BitConverter.SingleToInt32Bits(3.14f));
        }));
    }

    [Test]
    public void EncodeDouble_FiniteAndNaN_PinnedBytes()
    {
        var b1 = new QwpBindValues();
        b1.SetDouble(0, 2.718281828);
        AssertBytes(b1, 1, BuildExpected(w =>
        {
            w.Add(TypeDouble);
            w.Add(NonNull);
            w.AddI64Le(BitConverter.DoubleToInt64Bits(2.718281828));
        }));

        var b2 = new QwpBindValues();
        b2.SetDouble(0, double.NaN);
        AssertBytes(b2, 1, BuildExpected(w =>
        {
            w.Add(TypeDouble);
            w.Add(NonNull);
            w.AddI64Le(BitConverter.DoubleToInt64Bits(double.NaN));
        }));
    }

    [Test]
    public void EncodeDate_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetDate(0, 1_700_000_000_000L);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeDate);
            w.Add(NonNull);
            w.AddI64Le(1_700_000_000_000L);
        }));
    }

    [Test]
    public void EncodeTimestampMicros_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetTimestampMicros(0, 1_700_000_000_000_000L);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeTimestamp);
            w.Add(NonNull);
            w.AddI64Le(1_700_000_000_000_000L);
        }));
    }

    [Test]
    public void EncodeTimestampNanos_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetTimestampNanos(0, 1_700_000_000_000_000_000L);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeTimestampNanos);
            w.Add(NonNull);
            w.AddI64Le(1_700_000_000_000_000_000L);
        }));
    }

    [Test]
    public void EncodeUuid_ExplicitLimbs_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetUuid(0, unchecked((long)0xFEEDFACECAFEBEEFUL), unchecked((long)0x0BADF00DDEADBEEFUL));
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeUuid);
            w.Add(NonNull);
            w.AddI64Le(unchecked((long)0xFEEDFACECAFEBEEFUL));
            w.AddI64Le(unchecked((long)0x0BADF00DDEADBEEFUL));
        }));
    }

    [Test]
    public void EncodeUuid_FromGuid_PinnedBytes()
    {
        var guid = Guid.Parse("123e4567-e89b-12d3-a456-426614174000");
        var b = new QwpBindValues();
        b.SetUuid(0, guid);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeUuid);
            w.Add(NonNull);
            w.AddI64Le(unchecked((long)0xA456426614174000UL));
            w.AddI64Le(unchecked((long)0x123E4567E89B12D3UL));
        }));
    }

    [Test]
    public void EncodeUuid_FromGuid_AscendingNibbles_PinnedBytes()
    {
        var guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var b = new QwpBindValues();
        b.SetUuid(0, guid);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeUuid);
            w.Add(NonNull);
            w.AddI64Le(unchecked((long)0x8899AABBCCDDEEFFUL));
            w.AddI64Le(unchecked((long)0x0011223344556677UL));
        }));
    }

    [Test]
    public void EncodeLong256_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetLong256(0,
            unchecked((long)0x1111111111111111UL),
            unchecked((long)0x2222222222222222UL),
            unchecked((long)0x3333333333333333UL),
            unchecked((long)0x4444444444444444UL));
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeLong256);
            w.Add(NonNull);
            w.AddI64Le(unchecked((long)0x1111111111111111UL));
            w.AddI64Le(unchecked((long)0x2222222222222222UL));
            w.AddI64Le(unchecked((long)0x3333333333333333UL));
            w.AddI64Le(unchecked((long)0x4444444444444444UL));
        }));
    }

    [Test]
    public void EncodeGeohash_MaxPrecision_PinnedBytes()
    {
        const long value = 0x0FFF_FFFF_FFFF_FFFFL;
        var b = new QwpBindValues();
        b.SetGeohash(0, 60, value);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeGeohash);
            w.Add(NonNull);
            w.AddVarint(60);
            for (var i = 0; i < 8; i++) w.Add((byte)(value >>> (i * 8)));
        }));
    }

    [Test]
    public void EncodeGeohash_MinPrecision_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetGeohash(0, 1, 1L);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeGeohash);
            w.Add(NonNull);
            w.AddVarint(1);
            w.Add(0x01);
        }));
    }

    [Test]
    public void EncodeGeohash_MasksHighBitsForSubByteprecision_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetGeohash(0, 5, 0xFFL);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeGeohash);
            w.Add(NonNull);
            w.AddVarint(5);
            w.Add(0x1F);
        }));
    }

    [Test]
    public void EncodeGeohash_MasksHighBitsAtMaxPrecision_PinnedBytes()
    {
        const long expectedValue = (1L << 60) - 1L;
        var b = new QwpBindValues();
        b.SetGeohash(0, 60, -1L);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeGeohash);
            w.Add(NonNull);
            w.AddVarint(60);
            for (var i = 0; i < 8; i++) w.Add((byte)(expectedValue >>> (i * 8)));
        }));
    }

    [Test]
    public void EncodeDecimal64_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetDecimal64(0, 2, 12345L);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeDecimal64);
            w.Add(NonNull);
            w.Add(0x02);
            w.AddI64Le(12345L);
        }));
    }

    [Test]
    public void EncodeDecimal128_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetDecimal128(0, 6,
            unchecked((long)0x0123456789ABCDEFUL),
            unchecked((long)0x7766554433221100UL));
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeDecimal128);
            w.Add(NonNull);
            w.Add(0x06);
            w.AddI64Le(unchecked((long)0x0123456789ABCDEFUL));
            w.AddI64Le(unchecked((long)0x7766554433221100UL));
        }));
    }

    [Test]
    public void EncodeDecimal256_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetDecimal256(0, 10,
            unchecked((long)0x1111111111111111UL),
            unchecked((long)0x2222222222222222UL),
            unchecked((long)0x3333333333333333UL),
            unchecked((long)0x4444444444444444UL));
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeDecimal256);
            w.Add(NonNull);
            w.Add(0x0A);
            w.AddI64Le(unchecked((long)0x1111111111111111UL));
            w.AddI64Le(unchecked((long)0x2222222222222222UL));
            w.AddI64Le(unchecked((long)0x3333333333333333UL));
            w.AddI64Le(unchecked((long)0x4444444444444444UL));
        }));
    }

    [Test]
    public void EncodeVarchar_Ascii_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetVarchar(0, "hello");
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeVarchar);
            w.Add(NonNull);
            w.AddI32Le(0);
            w.AddI32Le(5);
            foreach (var x in Encoding.UTF8.GetBytes("hello")) w.Add(x);
        }));
    }

    [Test]
    public void EncodeVarchar_Empty_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetVarchar(0, "");
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeVarchar);
            w.Add(NonNull);
            w.AddI32Le(0);
            w.AddI32Le(0);
        }));
    }

    [Test]
    public void EncodeVarchar_Null_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetVarchar(0, null);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeVarchar);
            w.Add(NullFlag);
            w.Add(NullBitmap);
        }));
    }

    [Test]
    public void EncodeVarchar_Unicode_PinnedBytes()
    {
        const string value = "café";
        var utf8 = Encoding.UTF8.GetBytes(value);
        var b = new QwpBindValues();
        b.SetVarchar(0, value);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeVarchar);
            w.Add(NonNull);
            w.AddI32Le(0);
            w.AddI32Le(utf8.Length);
            foreach (var x in utf8) w.Add(x);
        }));
    }

    [Test]
    public void EncodeNull_Scalar_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetNull(0, QwpTypeCode.Long);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeLong);
            w.Add(NullFlag);
            w.Add(NullBitmap);
        }));
    }

    [Test]
    public void EncodeNullDecimal64_WithExplicitScale_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetNullDecimal64(0, 3);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeDecimal64);
            w.Add(NullFlag);
            w.Add(NullBitmap);
            w.Add(0x03);
        }));
    }

    [Test]
    public void EncodeNullDecimal128_WithExplicitScale_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetNullDecimal128(0, 12);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeDecimal128);
            w.Add(NullFlag);
            w.Add(NullBitmap);
            w.Add(0x0C);
        }));
    }

    [Test]
    public void EncodeNullDecimal256_WithExplicitScale_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetNullDecimal256(0, 50);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeDecimal256);
            w.Add(NullFlag);
            w.Add(NullBitmap);
            w.Add(0x32);
        }));
    }

    [Test]
    public void EncodeNullGeohash_WithExplicitPrecision_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetNullGeohash(0, 40);
        AssertBytes(b, 1, BuildExpected(w =>
        {
            w.Add(TypeGeohash);
            w.Add(NullFlag);
            w.Add(NullBitmap);
            w.AddVarint(40);
        }));
    }

    [Test]
    public void EncodeNullTypes_Exhaustive_PinnedBytes()
    {
        var types = new[]
        {
            QwpTypeCode.Boolean, QwpTypeCode.Byte, QwpTypeCode.Short, QwpTypeCode.Char,
            QwpTypeCode.Int, QwpTypeCode.Long, QwpTypeCode.Float, QwpTypeCode.Double,
            QwpTypeCode.Date, QwpTypeCode.Timestamp, QwpTypeCode.TimestampNanos,
            QwpTypeCode.Uuid, QwpTypeCode.Long256, QwpTypeCode.Geohash, QwpTypeCode.Varchar,
            QwpTypeCode.Decimal64, QwpTypeCode.Decimal128, QwpTypeCode.Decimal256,
        };

        var b = new QwpBindValues();
        for (var i = 0; i < types.Length; i++) b.SetNull(i, types[i]);

        var expected = BuildExpected(w =>
        {
            foreach (var t in types)
            {
                w.Add((byte)t);
                w.Add(NullFlag);
                w.Add(NullBitmap);
                if (t is QwpTypeCode.Decimal64 or QwpTypeCode.Decimal128 or QwpTypeCode.Decimal256)
                    w.Add(0x00);
                else if (t is QwpTypeCode.Geohash)
                    w.AddVarint(1);
            }
        });
        AssertBytes(b, types.Length, expected);
    }

    [Test]
    public void EncodeMultiBind_MixedTypes_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetLong(0, 1234567890L);
        b.SetVarchar(1, "hello");
        b.SetBoolean(2, true);
        b.SetDouble(3, 1.5);

        AssertBytes(b, 4, BuildExpected(w =>
        {
            w.Add(TypeLong); w.Add(NonNull); w.AddI64Le(1234567890L);

            w.Add(TypeVarchar); w.Add(NonNull); w.AddI32Le(0); w.AddI32Le(5);
            foreach (var x in Encoding.UTF8.GetBytes("hello")) w.Add(x);

            w.Add(TypeBoolean); w.Add(NonNull); w.Add(0x01);

            w.Add(TypeDouble); w.Add(NonNull);
            w.AddI64Le(BitConverter.DoubleToInt64Bits(1.5));
        }));
    }

    [Test]
    public void Reset_ProducesIdenticalBytes()
    {
        var b = new QwpBindValues();
        b.SetLong(0, 42L);
        b.SetInt(1, 7);
        var first = b.AsMemory().ToArray();

        b.Reset();
        Assert.That(b.Count, Is.EqualTo(0));
        Assert.That(b.AsMemory().Length, Is.EqualTo(0));

        b.SetLong(0, 42L);
        b.SetInt(1, 7);
        Assert.That(b.AsMemory().ToArray(), Is.EqualTo(first));
    }

    private static void AssertBytes(QwpBindValues binds, int expectedCount, byte[] expected)
    {
        Assert.That(binds.Count, Is.EqualTo(expectedCount));
        Assert.That(binds.AsMemory().ToArray(), Is.EqualTo(expected));
    }

    private static byte[] BuildExpected(Action<ByteList> body)
    {
        var w = new ByteList();
        body(w);
        return w.ToArray();
    }

    private sealed class ByteList
    {
        private readonly List<byte> _bytes = new();

        public void Add(byte b) => _bytes.Add(b);

        public void AddI16Le(short v)
        {
            _bytes.Add((byte)(v & 0xFF));
            _bytes.Add((byte)((v >> 8) & 0xFF));
        }

        public void AddU16Le(ushort v)
        {
            _bytes.Add((byte)(v & 0xFF));
            _bytes.Add((byte)((v >> 8) & 0xFF));
        }

        public void AddI32Le(int v)
        {
            for (var i = 0; i < 4; i++) _bytes.Add((byte)((v >> (i * 8)) & 0xFF));
        }

        public void AddI64Le(long v)
        {
            for (var i = 0; i < 8; i++) _bytes.Add((byte)((v >> (i * 8)) & 0xFF));
        }

        public void AddVarint(ulong v)
        {
            while (v > 0x7F)
            {
                _bytes.Add((byte)((v & 0x7F) | 0x80));
                v >>= 7;
            }
            _bytes.Add((byte)(v & 0x7F));
        }

        public byte[] ToArray() => _bytes.ToArray();
    }
}
