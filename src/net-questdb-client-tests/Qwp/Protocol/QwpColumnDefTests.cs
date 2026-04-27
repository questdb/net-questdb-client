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

using NUnit.Framework;
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp.Protocol;

/// <summary>
///     Mirrors <c>QwpColumnDefTest.java</c> on Java main 64b7ee69.
/// </summary>
[TestFixture]
public class QwpColumnDefTests
{
    [Test]
    public void ValidateAcceptsAllValidTypes()
    {
        // .NET accepts the full 0x01..0x18 range. Java's fixture is 21 entries because
        // its Validate() rejects BINARY (0x17) / IPv4 (0x18) — see QwpColumnDef.Validate
        // doc comment for the divergence rationale.
        byte[] validTypes =
        {
            QwpConstants.TYPE_BOOLEAN,
            QwpConstants.TYPE_BYTE,
            QwpConstants.TYPE_SHORT,
            QwpConstants.TYPE_INT,
            QwpConstants.TYPE_LONG,
            QwpConstants.TYPE_FLOAT,
            QwpConstants.TYPE_DOUBLE,
            QwpConstants.TYPE_SYMBOL,
            QwpConstants.TYPE_TIMESTAMP,
            QwpConstants.TYPE_DATE,
            QwpConstants.TYPE_UUID,
            QwpConstants.TYPE_LONG256,
            QwpConstants.TYPE_GEOHASH,
            QwpConstants.TYPE_VARCHAR,
            QwpConstants.TYPE_TIMESTAMP_NANOS,
            QwpConstants.TYPE_DOUBLE_ARRAY,
            QwpConstants.TYPE_LONG_ARRAY,
            QwpConstants.TYPE_DECIMAL64,
            QwpConstants.TYPE_DECIMAL128,
            QwpConstants.TYPE_DECIMAL256,
            QwpConstants.TYPE_CHAR,
            QwpConstants.TYPE_BINARY, // .NET-only acceptance
            QwpConstants.TYPE_IPv4,   // .NET-only acceptance
        };
        foreach (var type in validTypes)
        {
            var col = new QwpColumnDef("col", type);
            Assert.DoesNotThrow(() => col.Validate());
        }
    }

    [Test]
    public void ValidateCharType()
    {
        var col = new QwpColumnDef("ch", QwpConstants.TYPE_CHAR);
        Assert.DoesNotThrow(() => col.Validate());
        Assert.That(col.TypeName, Is.EqualTo("CHAR"));
        Assert.That(col.TypeCode, Is.EqualTo(QwpConstants.TYPE_CHAR));
    }

    [Test]
    public void ValidateRejectsHighBit()
    {
        // The high bit isn't part of the type-code namespace on main.
        var badType = unchecked((byte)(QwpConstants.TYPE_CHAR | 0x80));
        var col = new QwpColumnDef("ch", badType);
        Assert.That(() => col.Validate(), Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void ValidateRejectsInvalidType()
    {
        // 0x19 is past the highest valid type code (0x18 = TYPE_IPv4).
        var col = new QwpColumnDef("bad", 0x19);
        Assert.That(() => col.Validate(), Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void ValidateRejectsZeroType()
    {
        var col = new QwpColumnDef("bad", 0x00);
        Assert.That(() => col.Validate(), Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void EqualityAndHash()
    {
        var a = new QwpColumnDef("c", QwpConstants.TYPE_INT);
        var b = new QwpColumnDef("c", QwpConstants.TYPE_INT);
        var c = new QwpColumnDef("c", QwpConstants.TYPE_LONG);
        var d = new QwpColumnDef("d", QwpConstants.TYPE_INT);

        Assert.That(a.Equals(b), Is.True);
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        Assert.That(a.Equals(c), Is.False);
        Assert.That(a.Equals(d), Is.False);
        Assert.That(a.Equals((object?)b), Is.True);
        Assert.That(a.Equals((object?)null), Is.False);
    }

    [Test]
    public void ToStringFormatsAsNameColonType()
    {
        var col = new QwpColumnDef("ts", QwpConstants.TYPE_TIMESTAMP);
        Assert.That(col.ToString(), Is.EqualTo("ts:TIMESTAMP"));
    }
}
