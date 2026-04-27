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
///     Mirrors <c>QwpConstantsTest.java</c> on Java main 64b7ee69.
/// </summary>
[TestFixture]
public class QwpConstantsTests
{
    [Test]
    public void FlagBitPositions()
    {
        Assert.That(QwpConstants.FLAG_GORILLA, Is.EqualTo(0x04));
        Assert.That(QwpConstants.FLAG_DELTA_SYMBOL_DICT, Is.EqualTo(0x08));
        Assert.That(QwpConstants.FLAG_ZSTD, Is.EqualTo(0x10));
    }

    [Test]
    public void GetFixedTypeSize()
    {
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_BOOLEAN), Is.EqualTo(0));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_BYTE), Is.EqualTo(1));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_SHORT), Is.EqualTo(2));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_CHAR), Is.EqualTo(2));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_INT), Is.EqualTo(4));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_LONG), Is.EqualTo(8));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_FLOAT), Is.EqualTo(4));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_DOUBLE), Is.EqualTo(8));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_TIMESTAMP), Is.EqualTo(8));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_TIMESTAMP_NANOS), Is.EqualTo(8));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_DATE), Is.EqualTo(8));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_UUID), Is.EqualTo(16));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_LONG256), Is.EqualTo(32));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_DECIMAL64), Is.EqualTo(8));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_DECIMAL128), Is.EqualTo(16));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_DECIMAL256), Is.EqualTo(32));

        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_SYMBOL), Is.EqualTo(-1));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_DOUBLE_ARRAY), Is.EqualTo(-1));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_LONG_ARRAY), Is.EqualTo(-1));

        // .NET divergence from Java main: BINARY is variable-width (-1) and IPv4 is
        // 4 bytes fixed. Java's helpers omit both — see QwpConstants doc comment.
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_BINARY), Is.EqualTo(-1));
        Assert.That(QwpConstants.GetFixedTypeSize(QwpConstants.TYPE_IPv4), Is.EqualTo(4));
    }

    [Test]
    public void GetTypeName()
    {
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_BOOLEAN), Is.EqualTo("BOOLEAN"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_INT), Is.EqualTo("INT"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_VARCHAR), Is.EqualTo("VARCHAR"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_TIMESTAMP), Is.EqualTo("TIMESTAMP"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_TIMESTAMP_NANOS), Is.EqualTo("TIMESTAMP_NANOS"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_DOUBLE_ARRAY), Is.EqualTo("DOUBLE_ARRAY"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_LONG_ARRAY), Is.EqualTo("LONG_ARRAY"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_DECIMAL64), Is.EqualTo("DECIMAL64"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_DECIMAL128), Is.EqualTo("DECIMAL128"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_DECIMAL256), Is.EqualTo("DECIMAL256"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_CHAR), Is.EqualTo("CHAR"));

        // .NET divergence from Java main: BINARY/IPv4 names are returned rather than UNKNOWN.
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_BINARY), Is.EqualTo("BINARY"));
        Assert.That(QwpConstants.GetTypeName(QwpConstants.TYPE_IPv4), Is.EqualTo("IPv4"));

        // The high bit is not part of the type code on main; bytes with it set are unknown.
        var badInt = unchecked((byte)(QwpConstants.TYPE_INT | 0x80));
        Assert.That(QwpConstants.GetTypeName(badInt), Does.StartWith("UNKNOWN"));

        // 0x19 is past the valid range — no name assigned.
        Assert.That(QwpConstants.GetTypeName(0x19), Does.StartWith("UNKNOWN"));
    }

    [Test]
    public void HeaderSize()
    {
        Assert.That(QwpConstants.HEADER_SIZE, Is.EqualTo(12));
    }

    [Test]
    public void IsFixedWidthType()
    {
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_BOOLEAN), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_BYTE), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_SHORT), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_CHAR), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_INT), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_LONG), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_FLOAT), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_DOUBLE), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_TIMESTAMP), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_TIMESTAMP_NANOS), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_DATE), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_UUID), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_LONG256), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_DECIMAL64), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_DECIMAL128), Is.True);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_DECIMAL256), Is.True);

        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_SYMBOL), Is.False);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_GEOHASH), Is.False);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_VARCHAR), Is.False);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_DOUBLE_ARRAY), Is.False);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_LONG_ARRAY), Is.False);

        // .NET divergence from Java main: BINARY is variable-width; IPv4 is fixed (4 bytes).
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_BINARY), Is.False);
        Assert.That(QwpConstants.IsFixedWidthType(QwpConstants.TYPE_IPv4), Is.True);
    }

    [Test]
    public void MagicBytesValue()
    {
        // "QWP1" in ASCII: Q=0x51, W=0x57, P=0x50, 1=0x31. Little-endian: 0x31505751.
        Assert.That(QwpConstants.MAGIC_MESSAGE, Is.EqualTo(0x31505751));

        var expected = new byte[] { (byte)'Q', (byte)'W', (byte)'P', (byte)'1' };
        Assert.That((byte)(QwpConstants.MAGIC_MESSAGE & 0xFF), Is.EqualTo(expected[0]));
        Assert.That((byte)((QwpConstants.MAGIC_MESSAGE >> 8) & 0xFF), Is.EqualTo(expected[1]));
        Assert.That((byte)((QwpConstants.MAGIC_MESSAGE >> 16) & 0xFF), Is.EqualTo(expected[2]));
        Assert.That((byte)((QwpConstants.MAGIC_MESSAGE >> 24) & 0xFF), Is.EqualTo(expected[3]));
    }

    [Test]
    public void MaxColumnsPerTable()
    {
        Assert.That(QwpConstants.MAX_COLUMNS_PER_TABLE, Is.EqualTo(2048));
    }

    [Test]
    public void MaxNameLengths()
    {
        Assert.That(QwpConstants.MAX_TABLE_NAME_LENGTH, Is.EqualTo(127));
        Assert.That(QwpConstants.MAX_COLUMN_NAME_LENGTH, Is.EqualTo(127));
    }

    [Test]
    public void Versions()
    {
        Assert.That(QwpConstants.VERSION_1, Is.EqualTo((byte)1));
        Assert.That(QwpConstants.VERSION_2, Is.EqualTo((byte)2));
        Assert.That(QwpConstants.MAX_SUPPORTED_INGEST_VERSION, Is.EqualTo(QwpConstants.VERSION_1));
        Assert.That(QwpConstants.MAX_SUPPORTED_VERSION, Is.EqualTo(QwpConstants.VERSION_2));
    }

    [Test]
    public void StatusCodes()
    {
        Assert.That(QwpConstants.STATUS_OK, Is.EqualTo(0x00));
        Assert.That(QwpConstants.STATUS_DURABLE_ACK, Is.EqualTo(0x02));
        Assert.That(QwpConstants.STATUS_SCHEMA_MISMATCH, Is.EqualTo(0x03));
        Assert.That(QwpConstants.STATUS_PARSE_ERROR, Is.EqualTo(0x05));
        Assert.That(QwpConstants.STATUS_INTERNAL_ERROR, Is.EqualTo(0x06));
        Assert.That(QwpConstants.STATUS_SECURITY_ERROR, Is.EqualTo(0x08));
        Assert.That(QwpConstants.STATUS_WRITE_ERROR, Is.EqualTo(0x09));
        Assert.That(QwpConstants.STATUS_CANCELLED, Is.EqualTo(0x0A));
        Assert.That(QwpConstants.STATUS_LIMIT_EXCEEDED, Is.EqualTo(0x0B));
    }

    [Test]
    public void SchemaModes()
    {
        Assert.That(QwpConstants.SCHEMA_MODE_FULL, Is.EqualTo(0x00));
        Assert.That(QwpConstants.SCHEMA_MODE_REFERENCE, Is.EqualTo(0x01));
    }
}
