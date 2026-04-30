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

using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp.Query;
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Query;

[TestFixture]
public class QwpBindValuesTests
{
    [Test]
    public void Empty_HasNoBytes()
    {
        var b = new QwpBindValues();
        Assert.That(b.Count, Is.EqualTo(0));
        Assert.That(b.AsMemory().Length, Is.EqualTo(0));
    }

    [Test]
    public void SetLong_OneRow_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetLong(0, 42L);

        Assert.That(b.Count, Is.EqualTo(1));
        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes, Is.EqualTo(new byte[]
        {
            (byte)QwpTypeCode.Long, 0x00,                    // type, null_flag = 0
            0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // 42 as i64 LE
        }));
    }

    [Test]
    public void SetNull_Long_PinnedBytes()
    {
        var b = new QwpBindValues();
        b.SetNull(0, QwpTypeCode.Long);

        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes, Is.EqualTo(new byte[] { (byte)QwpTypeCode.Long, 0x01, 0x01 }));
    }

    [Test]
    public void SetVarchar_AsciiValue_PinnedLayout()
    {
        var b = new QwpBindValues();
        b.SetVarchar(0, "hi");

        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes, Is.EqualTo(new byte[]
        {
            (byte)QwpTypeCode.Varchar, 0x00,                  // type, null_flag = 0
            0x00, 0x00, 0x00, 0x00,                           // offset[0] = 0
            0x02, 0x00, 0x00, 0x00,                           // offset[1] = 2
            (byte)'h', (byte)'i',
        }));
    }

    [Test]
    public void SetVarchar_NullValue_EncodedAsTypedNull()
    {
        var b = new QwpBindValues();
        b.SetVarchar(0, null);

        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes, Is.EqualTo(new byte[] { (byte)QwpTypeCode.Varchar, 0x01, 0x01 }));
    }

    [Test]
    public void SetDecimal128_NonNull_HasScalePrefix()
    {
        var b = new QwpBindValues();
        b.SetDecimal128(0, scale: 4, lo: 1234567890L, hi: 0L);

        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes[0], Is.EqualTo((byte)QwpTypeCode.Decimal128));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        Assert.That(bytes[2], Is.EqualTo((byte)4));
    }

    [Test]
    public void SetNullDecimal128_HasScaleAfterBitmap()
    {
        var b = new QwpBindValues();
        b.SetNullDecimal128(0, scale: 6);

        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes, Is.EqualTo(new byte[] { (byte)QwpTypeCode.Decimal128, 0x01, 0x01, 0x06 }));
    }

    [Test]
    public void SetGeohash_HasPrecisionVarint_AndPackedBytes()
    {
        var b = new QwpBindValues();
        b.SetGeohash(0, precisionBits: 24, value: 0xABCDEF);

        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes[0], Is.EqualTo((byte)QwpTypeCode.Geohash));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        Assert.That(bytes[2], Is.EqualTo((byte)24));
        Assert.That(bytes[3], Is.EqualTo(0xEF));
        Assert.That(bytes[4], Is.EqualTo(0xCD));
        Assert.That(bytes[5], Is.EqualTo(0xAB));
    }

    [Test]
    public void SetUuid_HighLowAreLittleEndian_LowFirst()
    {
        var b = new QwpBindValues();
        b.SetUuid(0, lo: 0x0102030405060708L, hi: 0x1112131415161718L);

        var bytes = b.AsMemory().ToArray();
        Assert.That(bytes[0], Is.EqualTo((byte)QwpTypeCode.Uuid));
        Assert.That(bytes[1], Is.EqualTo(0x00));
        Assert.That(bytes[2..10], Is.EqualTo(new byte[] { 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 }));
        Assert.That(bytes[10..18], Is.EqualTo(new byte[] { 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11 }));
    }

    [Test]
    public void Index_OutOfOrder_Throws()
    {
        var b = new QwpBindValues();
        b.SetLong(0, 1);
        Assert.Throws<IngressError>(() => b.SetLong(2, 2));
    }

    [Test]
    public void Index_Repeated_Throws()
    {
        var b = new QwpBindValues();
        b.SetLong(0, 1);
        Assert.Throws<IngressError>(() => b.SetLong(0, 2));
    }

    [Test]
    public void Decimal64_ScaleOutOfRange_Throws()
    {
        var b = new QwpBindValues();
        Assert.Throws<IngressError>(() => b.SetDecimal64(0, scale: -1, unscaledValue: 0));
        Assert.Throws<IngressError>(() => b.SetDecimal64(0, scale: 19, unscaledValue: 0));
    }

    [Test]
    public void Decimal128_ScaleOutOfRange_Throws()
    {
        var b = new QwpBindValues();
        Assert.Throws<IngressError>(() => b.SetDecimal128(0, scale: 39, lo: 0, hi: 0));
    }

    [Test]
    public void Decimal256_ScaleOutOfRange_Throws()
    {
        var b = new QwpBindValues();
        Assert.Throws<IngressError>(() => b.SetDecimal256(0, scale: 77, ll: 0, lh: 0, hl: 0, hh: 0));
    }

    [Test]
    public void Geohash_PrecisionOutOfRange_Throws()
    {
        var b = new QwpBindValues();
        Assert.Throws<IngressError>(() => b.SetGeohash(0, precisionBits: 0, value: 1));
        Assert.Throws<IngressError>(() => b.SetGeohash(0, precisionBits: 61, value: 1));
    }

    [Test]
    public void SetNull_ForUnsupportedType_Throws()
    {
        var b = new QwpBindValues();
        Assert.Throws<IngressError>(() => b.SetNull(0, QwpTypeCode.Symbol));
    }

    [Test]
    public void Reset_ClearsBuffer()
    {
        var b = new QwpBindValues();
        b.SetLong(0, 1);
        b.SetVarchar(1, "x");
        Assert.That(b.Count, Is.EqualTo(2));

        b.Reset();
        Assert.That(b.Count, Is.EqualTo(0));
        Assert.That(b.AsMemory().Length, Is.EqualTo(0));

        b.SetLong(0, 99);
        Assert.That(b.Count, Is.EqualTo(1));
    }

    [Test]
    public void MultipleBinds_IncrementsCount_AndAppends()
    {
        var b = new QwpBindValues();
        b.SetInt(0, 7);
        b.SetVarchar(1, "abc");
        b.SetLong(2, 999);

        Assert.That(b.Count, Is.EqualTo(3));
        Assert.That(b.AsMemory().Length, Is.GreaterThan(0));
    }

    [Test]
    public void TooManyBinds_RejectedAtBoundary()
    {
        var b = new QwpBindValues();
        for (var i = 0; i < QuestDB.Qwp.QwpConstants.MaxBindParameters; i++)
        {
            b.SetInt(i, i);
        }
        Assert.Throws<QuestDB.Utils.IngressError>(
            () => b.SetInt(QuestDB.Qwp.QwpConstants.MaxBindParameters, 0));
    }
}
