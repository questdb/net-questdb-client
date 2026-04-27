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
using NUnit.Framework;
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Mirrors the wire-format expectations of <c>QwpWebSocketEncoderTest.java</c> on
///     Java main 64b7ee69. The Java test is large (1447 LoC) and exercises a lot of
///     scenarios; this .NET port focuses on the envelope invariants — magic, version,
///     flags, table count, payload-length patching — and the multi-table / delta-dict
///     paths. Per-column encoding correctness is already covered by
///     <see cref="QwpColumnWriterTests"/>.
/// </summary>
[TestFixture]
public class QwpWebSocketEncoderTests
{
    [Test]
    public void EncodeSingleTableEmitsQwp1Magic()
    {
        var table = MakeTable("trades", new (string, byte)[] { ("id", QwpConstants.TYPE_LONG) }, rows: 1);
        var encoder = new QwpWebSocketEncoder();
        var size = encoder.Encode(table, useSchemaRef: false);

        var bytes = encoder.AsReadOnlySpan();
        Assert.That(size, Is.GreaterThanOrEqualTo(QwpConstants.HEADER_SIZE));
        Assert.That(bytes[0], Is.EqualTo((byte)'Q'));
        Assert.That(bytes[1], Is.EqualTo((byte)'W'));
        Assert.That(bytes[2], Is.EqualTo((byte)'P'));
        Assert.That(bytes[3], Is.EqualTo((byte)'1'));
        Assert.That(bytes[4], Is.EqualTo(QwpConstants.VERSION_1));
    }

    [Test]
    public void EncodePatchesPayloadLengthAfterTableData()
    {
        var table = MakeTable("trades", new (string, byte)[] { ("id", QwpConstants.TYPE_LONG) }, rows: 5);
        var encoder = new QwpWebSocketEncoder();
        var size = encoder.Encode(table, useSchemaRef: false);

        var bytes = encoder.AsReadOnlySpan();
        var declaredPayloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(8, 4));
        Assert.That(declaredPayloadLength, Is.EqualTo(size - QwpConstants.HEADER_SIZE),
                    "payload length must equal total bytes minus 12-byte header");
    }

    [Test]
    public void EncodeSetsTableCountToOne()
    {
        var table = MakeTable("t", new (string, byte)[] { ("id", QwpConstants.TYPE_LONG) }, rows: 1);
        var encoder = new QwpWebSocketEncoder();
        encoder.Encode(table, useSchemaRef: false);
        var bytes = encoder.AsReadOnlySpan();
        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(6, 2));
        Assert.That(tableCount, Is.EqualTo(1));
    }

    [Test]
    public void GorillaFlagPersistsAcrossEncodes()
    {
        var encoder = new QwpWebSocketEncoder { IsGorillaEnabled = true };
        var table = MakeTable("t", new (string, byte)[] { ("ts", QwpConstants.TYPE_TIMESTAMP) }, rows: 5);
        encoder.Encode(table, useSchemaRef: false);
        Assert.That(encoder.AsReadOnlySpan()[5] & QwpConstants.FLAG_GORILLA, Is.EqualTo(QwpConstants.FLAG_GORILLA));

        // Encode again — the flag should still be set.
        encoder.Encode(table, useSchemaRef: false);
        Assert.That(encoder.AsReadOnlySpan()[5] & QwpConstants.FLAG_GORILLA, Is.EqualTo(QwpConstants.FLAG_GORILLA));
    }

    [Test]
    public void GorillaFlagCanBeCleared()
    {
        var encoder = new QwpWebSocketEncoder { IsGorillaEnabled = true };
        encoder.IsGorillaEnabled = false;
        var table = MakeTable("t", new (string, byte)[] { ("id", QwpConstants.TYPE_LONG) }, rows: 1);
        encoder.Encode(table, useSchemaRef: false);
        Assert.That(encoder.AsReadOnlySpan()[5] & QwpConstants.FLAG_GORILLA, Is.EqualTo(0));
    }

    [Test]
    public void DeltaDictPathSetsFlagInHeader()
    {
        var dict = new GlobalSymbolDictionary();
        var aapl = dict.GetOrAddSymbol("AAPL");
        var goog = dict.GetOrAddSymbol("GOOG");

        var table = new QwpTableBuffer("trades", dict);
        var ts = table.GetOrCreateColumn("ts", QwpConstants.TYPE_TIMESTAMP, false)!;
        var sym = table.GetOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, false)!;
        ts.AddLong(1_000);
        sym.AddSymbol("AAPL");
        table.NextRow();
        ts.AddLong(2_000);
        sym.AddSymbol("GOOG");
        table.NextRow();

        var encoder = new QwpWebSocketEncoder();
        encoder.EncodeWithDeltaDict(table, dict, confirmedMaxId: -1, batchMaxId: goog, useSchemaRef: false);

        var bytes = encoder.AsReadOnlySpan();
        Assert.That(bytes[5] & QwpConstants.FLAG_DELTA_SYMBOL_DICT,
                    Is.EqualTo(QwpConstants.FLAG_DELTA_SYMBOL_DICT));
    }

    [Test]
    public void DeltaDictHeaderFlagIsScopedToOneMessage()
    {
        // BeginMessage temporarily sets the delta-dict flag in the header but should
        // NOT pollute the encoder's persistent flag state for a subsequent plain Encode.
        var dict = new GlobalSymbolDictionary();
        dict.GetOrAddSymbol("X");
        var table = new QwpTableBuffer("t", dict);
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.NextRow();

        var encoder = new QwpWebSocketEncoder();
        encoder.EncodeWithDeltaDict(table, dict, -1, 0, false);
        Assert.That(encoder.AsReadOnlySpan()[5] & QwpConstants.FLAG_DELTA_SYMBOL_DICT,
                    Is.EqualTo(QwpConstants.FLAG_DELTA_SYMBOL_DICT));

        // Now use plain Encode — header flag should be 0 (or only Gorilla if set).
        var plainTable = MakeTable("plain", new (string, byte)[] { ("id", QwpConstants.TYPE_LONG) }, rows: 1);
        encoder.Encode(plainTable, false);
        Assert.That(encoder.AsReadOnlySpan()[5] & QwpConstants.FLAG_DELTA_SYMBOL_DICT, Is.EqualTo(0));
    }

    [Test]
    public void MultiTableBatching()
    {
        var dict = new GlobalSymbolDictionary();
        var t1 = new QwpTableBuffer("a", dict);
        t1.GetOrCreateColumn("x", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        t1.NextRow();
        var t2 = new QwpTableBuffer("b", dict);
        t2.GetOrCreateColumn("y", QwpConstants.TYPE_LONG, false)!.AddLong(2);
        t2.NextRow();

        var encoder = new QwpWebSocketEncoder();
        encoder.BeginMessage(tableCount: 2, dict, confirmedMaxId: -1, batchMaxId: -1);
        encoder.AddTable(t1, useSchemaRef: false);
        encoder.AddTable(t2, useSchemaRef: false);
        var size = encoder.FinishMessage();

        var bytes = encoder.AsReadOnlySpan();
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(6, 2)), Is.EqualTo(2));
        var declaredPayloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(8, 4));
        Assert.That(declaredPayloadLength, Is.EqualTo(size - QwpConstants.HEADER_SIZE));
    }

    [Test]
    public void VersionByteIsConfigurable()
    {
        var encoder = new QwpWebSocketEncoder { Version = QwpConstants.VERSION_2 };
        var table = MakeTable("t", new (string, byte)[] { ("id", QwpConstants.TYPE_LONG) }, rows: 1);
        encoder.Encode(table, useSchemaRef: false);
        Assert.That(encoder.AsReadOnlySpan()[4], Is.EqualTo(QwpConstants.VERSION_2));
    }

    private static QwpTableBuffer MakeTable(string name, (string Name, byte Type)[] columns, int rows)
    {
        var table = new QwpTableBuffer(name);
        for (var r = 0; r < rows; r++)
        {
            foreach (var (cname, ctype) in columns)
            {
                var col = table.GetOrCreateColumn(cname, ctype, useNullBitmap: false)!;
                switch (ctype)
                {
                    case QwpConstants.TYPE_LONG:
                    case QwpConstants.TYPE_TIMESTAMP:
                        col.AddLong(r);
                        break;
                    default:
                        throw new NotSupportedException($"unhandled type 0x{ctype:X2}");
                }
            }
            table.NextRow();
        }
        return table;
    }
}
