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
using NUnit.Framework;
using QuestDB.Qwp;
using QuestDB.Qwp.Egress;

namespace net_questdb_client_tests.Qwp.Egress;

[TestFixture]
public class QwpBindValuesTests
{
    [Test]
    public void EmptyAfterConstruction()
    {
        var b = new QwpBindValues();
        Assert.That(b.Count, Is.EqualTo(0));
        Assert.That(b.BufferLength, Is.EqualTo(0));
    }

    [Test]
    public void SetBooleanEmitsTypeFlagValue()
    {
        var b = new QwpBindValues().SetBoolean(0, true);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(b.Count, Is.EqualTo(1));
        Assert.That(bytes, Is.EqualTo(new byte[] { QwpConstants.TYPE_BOOLEAN, 0x00, 0x01 }));
    }

    [Test]
    public void SetByteEmitsValue()
    {
        var b = new QwpBindValues().SetByte(0, 0x42);
        Assert.That(b.BufferSpan.ToArray(),
            Is.EqualTo(new byte[] { QwpConstants.TYPE_BYTE, 0x00, 0x42 }));
    }

    [Test]
    public void SetShortIsLittleEndian()
    {
        var b = new QwpBindValues().SetShort(0, 0x1234);
        Assert.That(b.BufferSpan.ToArray(),
            Is.EqualTo(new byte[] { QwpConstants.TYPE_SHORT, 0x00, 0x34, 0x12 }));
    }

    [Test]
    public void SetCharEncodesAsUtf16Short()
    {
        var b = new QwpBindValues().SetChar(0, 'Z');
        Assert.That(b.BufferSpan.ToArray(),
            Is.EqualTo(new byte[] { QwpConstants.TYPE_CHAR, 0x00, (byte)'Z', 0x00 }));
    }

    [Test]
    public void SetIntIsLittleEndian()
    {
        var b = new QwpBindValues().SetInt(0, unchecked((int)0xCAFEBABE));
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_INT));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(2)), Is.EqualTo(unchecked((int)0xCAFEBABE)));
    }

    [Test]
    public void SetLongIsLittleEndian()
    {
        var b = new QwpBindValues().SetLong(0, 0x0102_0304_0506_0708L);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_LONG));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(2)), Is.EqualTo(0x0102_0304_0506_0708L));
    }

    [Test]
    public void SetFloatPreservesBits()
    {
        var b = new QwpBindValues().SetFloat(0, 1.5f);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_FLOAT));
        Assert.That(BinaryPrimitives.ReadSingleLittleEndian(bytes.AsSpan(2)), Is.EqualTo(1.5f));
    }

    [Test]
    public void SetDoublePreservesBits()
    {
        var b = new QwpBindValues().SetDouble(0, double.NaN);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_DOUBLE));
        Assert.That(double.IsNaN(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(2))), Is.True);
    }

    [Test]
    public void SetUuidEmitsLoThenHi()
    {
        var b = new QwpBindValues().SetUuid(0, lo: 0x1111_2222_3333_4444L, hi: unchecked((long)0xAAAA_BBBB_CCCC_DDDDL));
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_UUID));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(2, 8)), Is.EqualTo(0x1111_2222_3333_4444L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(10, 8)), Is.EqualTo(unchecked((long)0xAAAA_BBBB_CCCC_DDDDL)));
    }

    [Test]
    public void SetLong256EmitsAllFourLimbs()
    {
        var b = new QwpBindValues().SetLong256(0, 1L, 2L, 3L, 4L);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes.Length, Is.EqualTo(2 + 32));
        for (var i = 0; i < 4; i++)
        {
            Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(2 + i * 8, 8)), Is.EqualTo((long)(i + 1)));
        }
    }

    [Test]
    public void SetDecimal64EmitsScaleThenValue()
    {
        var b = new QwpBindValues().SetDecimal64(0, scale: 4, unscaledValue: 12345L);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_DECIMAL64));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        Assert.That(bytes[2], Is.EqualTo((byte)4));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(3, 8)), Is.EqualTo(12345L));
    }

    [Test]
    public void SetDecimal128EmitsScaleLoHi()
    {
        var b = new QwpBindValues().SetDecimal128(0, scale: 10, lo: 0xABCDL, hi: 0x1234L);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_DECIMAL128));
        Assert.That(bytes[2], Is.EqualTo((byte)10));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(3, 8)), Is.EqualTo(0xABCDL));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(11, 8)), Is.EqualTo(0x1234L));
    }

    [Test]
    public void SetDecimal256EmitsScaleAndAllFourLimbs()
    {
        var b = new QwpBindValues().SetDecimal256(0, scale: 50, ll: 1, lh: 2, hl: 3, hh: 4);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_DECIMAL256));
        Assert.That(bytes[2], Is.EqualTo((byte)50));
        for (var i = 0; i < 4; i++)
        {
            Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(3 + i * 8, 8)),
                Is.EqualTo((long)(i + 1)));
        }
    }

    [Test]
    public void SetGeohashMasksOverflowBits()
    {
        // Precision 5 bits keeps the low 5 bits only; 0xFF -> 0x1F.
        var b = new QwpBindValues().SetGeohash(0, precisionBits: 5, value: 0xFFL);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_GEOHASH));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        // varint(5) is single byte 0x05.
        Assert.That(bytes[2], Is.EqualTo((byte)5));
        Assert.That(bytes[3], Is.EqualTo((byte)0x1F));
        Assert.That(bytes.Length, Is.EqualTo(4));
    }

    [Test]
    public void SetGeohashAt60BitsEmitsEightBytes()
    {
        var b = new QwpBindValues().SetGeohash(0, precisionBits: 60, value: long.MaxValue);
        var bytes = b.BufferSpan.ToArray();
        // header(2) + varint(0x3C, 1B) + 8 bytes = 11.
        Assert.That(bytes.Length, Is.EqualTo(11));
        Assert.That(bytes[2], Is.EqualTo((byte)60));
        // long.MaxValue masked to 60 bits = 0x0FFFFFFFFFFFFFFF.
        var masked = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(3, 8));
        Assert.That(masked, Is.EqualTo(0x0FFF_FFFF_FFFF_FFFFL));
    }

    [Test]
    public void SetVarcharEncodesOffsetThenLengthThenUtf8()
    {
        var b = new QwpBindValues().SetVarchar(0, "hello");
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes[0], Is.EqualTo(QwpConstants.TYPE_VARCHAR));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(2, 4)), Is.EqualTo(0));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(6, 4)), Is.EqualTo(5));
        Assert.That(Encoding.UTF8.GetString(bytes.AsSpan(10, 5)), Is.EqualTo("hello"));
    }

    [Test]
    public void SetVarcharNullBecomesTypedNull()
    {
        var b = new QwpBindValues().SetVarchar(0, null);
        var bytes = b.BufferSpan.ToArray();
        Assert.That(bytes, Is.EqualTo(new byte[] { QwpConstants.TYPE_VARCHAR, 0x01, 0x01 }));
    }

    [Test]
    public void SetVarcharUtf8MultiByte()
    {
        const string s = "héllo🚀";
        var b = new QwpBindValues().SetVarchar(0, s);
        var bytes = b.BufferSpan.ToArray();
        var expectedLen = Encoding.UTF8.GetByteCount(s);
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(6, 4)), Is.EqualTo(expectedLen));
        Assert.That(Encoding.UTF8.GetString(bytes.AsSpan(10, expectedLen)), Is.EqualTo(s));
    }

    [Test]
    public void SetNullBoolean()
    {
        var b = new QwpBindValues().SetNull(0, QwpConstants.TYPE_BOOLEAN);
        Assert.That(b.BufferSpan.ToArray(), Is.EqualTo(new byte[] { QwpConstants.TYPE_BOOLEAN, 0x01, 0x01 }));
    }

    [Test]
    public void SetNullDecimalRoutesToScaleZeroPath()
    {
        var b = new QwpBindValues().SetNull(0, QwpConstants.TYPE_DECIMAL64);
        var bytes = b.BufferSpan.ToArray();
        // header(type, NULL_FLAG=1, NULL_BITMAP=1) + scale(0) = 4 bytes.
        Assert.That(bytes, Is.EqualTo(new byte[] { QwpConstants.TYPE_DECIMAL64, 0x01, 0x01, 0x00 }));
    }

    [Test]
    public void SetNullGeohashRoutesToMinPrecisionPath()
    {
        var b = new QwpBindValues().SetNull(0, QwpConstants.TYPE_GEOHASH);
        var bytes = b.BufferSpan.ToArray();
        // header + varint(1) = 4 bytes.
        Assert.That(bytes, Is.EqualTo(new byte[] { QwpConstants.TYPE_GEOHASH, 0x01, 0x01, 0x01 }));
    }

    [Test]
    public void SetNullDecimal64WithExplicitScale()
    {
        var b = new QwpBindValues().SetNullDecimal64(0, scale: 6);
        Assert.That(b.BufferSpan.ToArray(),
            Is.EqualTo(new byte[] { QwpConstants.TYPE_DECIMAL64, 0x01, 0x01, 0x06 }));
    }

    [Test]
    public void SetNullGeohashWithExplicitPrecision()
    {
        var b = new QwpBindValues().SetNullGeohash(0, precisionBits: 25);
        // varint(25) = 0x19 single byte.
        Assert.That(b.BufferSpan.ToArray(),
            Is.EqualTo(new byte[] { QwpConstants.TYPE_GEOHASH, 0x01, 0x01, 0x19 }));
    }

    [Test]
    public void OutOfOrderIndexThrows()
    {
        var b = new QwpBindValues();
        b.SetInt(0, 1);
        Assert.That(() => b.SetInt(2, 99), Throws.TypeOf<InvalidOperationException>()
            .With.Message.Contains("expected 1, got 2"));
    }

    [Test]
    public void DuplicateIndexThrows()
    {
        var b = new QwpBindValues();
        b.SetInt(0, 1);
        Assert.That(() => b.SetInt(0, 2), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void NegativeIndexThrows()
    {
        var b = new QwpBindValues();
        Assert.That(() => b.SetInt(-1, 0), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void Decimal64ScaleOutOfRangeThrows()
    {
        var b = new QwpBindValues();
        Assert.That(() => b.SetDecimal64(0, scale: -1, unscaledValue: 0L),
            Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => b.SetDecimal64(0, scale: 19, unscaledValue: 0L),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Decimal128ScaleOutOfRangeThrows()
    {
        var b = new QwpBindValues();
        Assert.That(() => b.SetDecimal128(0, scale: 39, lo: 0L, hi: 0L),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Decimal256ScaleOutOfRangeThrows()
    {
        var b = new QwpBindValues();
        Assert.That(() => b.SetDecimal256(0, scale: 77, ll: 0L, lh: 0L, hl: 0L, hh: 0L),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GeohashPrecisionOutOfRangeThrows()
    {
        var b = new QwpBindValues();
        Assert.That(() => b.SetGeohash(0, precisionBits: 0, value: 0L),
            Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => b.SetGeohash(0, precisionBits: 61, value: 0L),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void SetNullRejectsArrayBinaryIpv4()
    {
        var b = new QwpBindValues();
        Assert.That(() => b.SetNull(0, QwpConstants.TYPE_DOUBLE_ARRAY),
            Throws.TypeOf<ArgumentException>());
        Assert.That(() => b.SetNull(0, QwpConstants.TYPE_LONG_ARRAY),
            Throws.TypeOf<ArgumentException>());
        Assert.That(() => b.SetNull(0, QwpConstants.TYPE_BINARY),
            Throws.TypeOf<ArgumentException>());
        Assert.That(() => b.SetNull(0, QwpConstants.TYPE_IPv4),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void ResetClearsCountAndBuffer()
    {
        var b = new QwpBindValues();
        b.SetInt(0, 42);
        b.SetVarchar(1, "hi");
        Assert.That(b.Count, Is.EqualTo(2));
        Assert.That(b.BufferLength, Is.GreaterThan(0));

        b.Reset();
        Assert.That(b.Count, Is.EqualTo(0));
        Assert.That(b.BufferLength, Is.EqualTo(0));
        // After reset, the next index must restart at 0.
        b.SetLong(0, 99);
        Assert.That(b.Count, Is.EqualTo(1));
    }

    [Test]
    public void SequencedMixedBinds()
    {
        var b = new QwpBindValues()
            .SetInt(0, 1)
            .SetVarchar(1, "a")
            .SetNull(2, QwpConstants.TYPE_LONG)
            .SetDouble(3, 0.5);
        Assert.That(b.Count, Is.EqualTo(4));
        // Each header is at least 2 bytes so the buffer should be > 8 bytes.
        Assert.That(b.BufferLength, Is.GreaterThan(20));
    }

    [Test]
    public void TooManyBindsThrows()
    {
        var b = new QwpBindValues();
        for (var i = 0; i < QwpConstants.MAX_COLUMNS_PER_TABLE; i++)
        {
            b.SetInt(i, i);
        }
        Assert.That(() => b.SetInt(QwpConstants.MAX_COLUMNS_PER_TABLE, 0),
            Throws.TypeOf<InvalidOperationException>().With.Message.Contains("too many"));
    }
}

[TestFixture]
public class QwpBindSetterTests
{
    [Test]
    public void DelegateInvokesUnderlyingSink()
    {
        QwpBindSetter setter = b =>
        {
            b.SetInt(0, 7);
            b.SetVarchar(1, "x");
        };
        var sink = new QwpBindValues();
        setter(sink);
        Assert.That(sink.Count, Is.EqualTo(2));
    }
}
