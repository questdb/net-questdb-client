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
///     Unit tests for <see cref="QwpColumnWriter"/>. There is no Java
///     <c>QwpColumnWriterTest.java</c> — the Java side covers it via the integration
///     tests for the WebSocket encoder and the UDP sender. These .NET tests target the
///     writer in isolation: per-type encoding shape, null-header emission, schema-mode
///     toggle, gorilla timestamp encoding round-trip via <see cref="QwpGorillaDecoder"/>,
///     and behaviour against both pinned and segmented buffer sinks.
/// </summary>
[TestFixture]
public class QwpColumnWriterTests
{
    [Test]
    public void EncodesFullSchemaTableHeader()
    {
        var table = new QwpTableBuffer("widgets");
        table.GetOrCreateColumn("id", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.GetOrCreateColumn("name", QwpConstants.TYPE_VARCHAR, true)!.AddString("a");
        table.NextRow();

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: false, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var pos = 0;

        // Header: tableName (varint len + utf8) + rowCount + colCount + schemaMode + schemaId
        AssertVarintAdvance(bytes, ref pos, 7);                                  // utf-8 len of "widgets"
        Assert.That(System.Text.Encoding.UTF8.GetString(bytes, pos, 7), Is.EqualTo("widgets"));
        pos += 7;
        AssertVarintAdvance(bytes, ref pos, 1);                                  // rowCount
        AssertVarintAdvance(bytes, ref pos, 2);                                  // colCount
        Assert.That(bytes[pos], Is.EqualTo(QwpConstants.SCHEMA_MODE_FULL));
        pos++;
        AssertVarintAdvance(bytes, ref pos, 0);                                  // schemaId default

        // Per-column defs
        AssertVarintAdvance(bytes, ref pos, 2); // strlen "id"
        Assert.That(System.Text.Encoding.UTF8.GetString(bytes, pos, 2), Is.EqualTo("id"));
        pos += 2;
        Assert.That(bytes[pos++], Is.EqualTo(QwpConstants.TYPE_LONG));
        AssertVarintAdvance(bytes, ref pos, 4); // strlen "name"
        Assert.That(System.Text.Encoding.UTF8.GetString(bytes, pos, 4), Is.EqualTo("name"));
        pos += 4;
        Assert.That(bytes[pos++], Is.EqualTo(QwpConstants.TYPE_VARCHAR));
    }

    [Test]
    public void EncodesSchemaRefSkipsColumnDefs()
    {
        var table = new QwpTableBuffer("widgets");
        table.GetOrCreateColumn("id", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.NextRow();
        // Sender assigns the schemaId only after the column registry stabilises;
        // CreateColumn invalidates the cached id, so set it AFTER the columns exist.
        table.SetSchemaId(42);

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var pos = 0;
        AssertVarintAdvance(bytes, ref pos, 7); // strlen "widgets"
        pos += 7;
        AssertVarintAdvance(bytes, ref pos, 1); // rowCount
        AssertVarintAdvance(bytes, ref pos, 1); // colCount
        Assert.That(bytes[pos++], Is.EqualTo(QwpConstants.SCHEMA_MODE_REFERENCE));
        AssertVarintAdvance(bytes, ref pos, 42); // schemaId

        // Following bytes are the column body — the per-column name+wireType list is omitted
        // for schemaRef. The next byte is the null-header for column id.
        Assert.That(bytes[pos], Is.EqualTo((byte)0)); // no nulls
    }

    [Test]
    public void NullHeaderEmitsBitmapWhenColumnHasNulls()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, true)!;
        col.AddLong(1);
        table.NextRow();
        col.AddNull();          // bit set on row 1
        table.NextRow();
        col.AddLong(3);
        table.NextRow();

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        // Skip the header (string + 3 varints + 1 byte + 1 varint = a few bytes). Find the
        // null-header byte by scanning for the boundary — we know the layout: tableName
        // ("t" varint=1 + 't'=1 byte = 2 bytes) + rowCount(3) + colCount(1) + mode(1) +
        // schemaId(0) = 6 bytes total before the null header.
        const int headerEnd = 1 + 1 + 1 + 1 + 1 + 1; // varint=1 byte each for the 0..63 values
        Assert.That(bytes[headerEnd], Is.EqualTo((byte)1)); // hasNulls = 1
        // Bitmap: ceil(3/8) = 1 byte. Bit 1 set ⇒ 0b00000010 = 0x02.
        Assert.That(bytes[headerEnd + 1], Is.EqualTo((byte)0b0000_0010));
    }

    [Test]
    public void EncodesLongColumnAsRawLittleEndianBlock()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("v", QwpConstants.TYPE_LONG, false)!;
        col.AddLong(1);
        table.NextRow();
        col.AddLong(0x1234_5678_ABCD_EF01L);
        table.NextRow();
        table.NextRow();   // row 2 omits col, so it gets padded with the long.MinValue sentinel

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        // Find column body — same layout calc as above (6-byte header + 1 null-header byte).
        var dataStart = 6 + 1;
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(dataStart, 8)), Is.EqualTo(1L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(dataStart + 8, 8)),
                    Is.EqualTo(0x1234_5678_ABCD_EF01L));
        // The padded null is a sentinel long (long.MinValue).
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(dataStart + 16, 8)),
                    Is.EqualTo(long.MinValue));
    }

    [Test]
    public void EncodesVarcharColumnWithCumulativeOffsetsAndUtf8Data()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true)!;
        col.AddString("ab");
        table.NextRow();
        col.AddString("xyz");
        table.NextRow();

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var dataStart = 6 + 1; // skipping the header + null-header
        // 3 cumulative int32 LE offsets [0, 2, 5], then concatenated UTF-8 bytes "abxyz".
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(dataStart, 4)), Is.EqualTo(0));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(dataStart + 4, 4)), Is.EqualTo(2));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(dataStart + 8, 4)), Is.EqualTo(5));
        var utf8 = System.Text.Encoding.UTF8.GetString(bytes, dataStart + 12, 5);
        Assert.That(utf8, Is.EqualTo("abxyz"));
    }

    [Test]
    public void EncodesSymbolColumnWithLocalDictionary()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true)!;
        col.AddSymbol("alpha");
        table.NextRow();
        col.AddSymbol("alpha"); // reuse
        table.NextRow();
        col.AddSymbol("beta");
        table.NextRow();

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var pos = 6 + 1; // header + null-header
        AssertVarintAdvance(bytes, ref pos, 2); // dict size
        AssertVarintAdvance(bytes, ref pos, 5); // strlen "alpha"
        Assert.That(System.Text.Encoding.UTF8.GetString(bytes, pos, 5), Is.EqualTo("alpha"));
        pos += 5;
        AssertVarintAdvance(bytes, ref pos, 4); // strlen "beta"
        Assert.That(System.Text.Encoding.UTF8.GetString(bytes, pos, 4), Is.EqualTo("beta"));
        pos += 4;
        // Per-row local indices as varints: 0, 0, 1
        AssertVarintAdvance(bytes, ref pos, 0);
        AssertVarintAdvance(bytes, ref pos, 0);
        AssertVarintAdvance(bytes, ref pos, 1);
    }

    [Test]
    public void EncodesTimestampColumnViaGorillaWhenFlagSet()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("ts", QwpConstants.TYPE_TIMESTAMP, false)!;
        var timestamps = new long[] { 1_000_000_000L, 1_000_000_100L, 1_000_000_200L, 1_000_000_300L, 1_000_000_400L };
        foreach (var t in timestamps) { col.AddLong(t); table.NextRow(); }

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: true);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var dataStart = 6 + 1; // header + null-header
        Assert.That(bytes[dataStart], Is.EqualTo((byte)0x01)); // ENCODING_GORILLA
        // First two timestamps written uncompressed.
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(dataStart + 1, 8)), Is.EqualTo(timestamps[0]));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(dataStart + 9, 8)), Is.EqualTo(timestamps[1]));

        // Round-trip the bitstream via QwpGorillaDecoder.
        // The encoded bitstream length is whatever's left in the sink minus the prefix.
        var encodedSize = sink.Position - dataStart - 1; // exclude flag byte
        var bitstreamLen = encodedSize - 16;
        var bitstreamStart = dataStart + 1 + 16;
        var decoder = new QwpGorillaDecoder();
        decoder.Reset(timestamps[0], timestamps[1],
                      sink.AsReadOnlyMemory().Slice(bitstreamStart, bitstreamLen));
        for (var i = 2; i < timestamps.Length; i++)
        {
            Assert.That(decoder.DecodeNext(), Is.EqualTo(timestamps[i]), $"row {i}");
        }
    }

    [Test]
    public void EncodesViaSegmentedBufferRoundTrips()
    {
        // The deferred test from PR 2 — flips green now that QwpTableBuffer + QwpColumnWriter
        // exist. Exercises the segmented sink with Gorilla encoding to make sure Gorilla's
        // in-place encode targets the live chunk's underlying byte[] (not a stale chunk).
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("ts", QwpConstants.TYPE_TIMESTAMP, true)!;
        var baseTs = 1_000_000_000L;
        for (var i = 0; i < 10; i++) { col.AddLong(baseTs + i * 1_000_000L); table.NextRow(); }

        var sink = new QwpSegmentedBufferWriter();
        // Pre-flush some bytes via a by-reference segment so flushedBytes > 0.
        sink.PutBlockOfBytes(new byte[32]);
        Assert.That(sink.Position, Is.EqualTo(32));

        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: false, useGlobalSymbols: false, useGorilla: true);

        Assert.That(sink.Position, Is.GreaterThan(32),
                    "encoder must produce table bytes after the pre-flushed segment");
        Assert.That(sink.Segments.Count, Is.GreaterThan(1));
    }

    [Test]
    public void EncodesGeohashColumnWithPrecisionVarintAndPackedValues()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, false)!;
        col.AddGeoHash(0xABCDL, 16);   // 16 bits ⇒ 2 bytes per value
        col.AddGeoHash(0x1234L, 16);
        table.NextRow();
        table.NextRow();

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var pos = 6 + 1; // header + null-header
        AssertVarintAdvance(bytes, ref pos, 16); // precision
        // 2 bytes per value, LSB-first: 0xCD, 0xAB, 0x34, 0x12
        Assert.That(bytes[pos++], Is.EqualTo((byte)0xCD));
        Assert.That(bytes[pos++], Is.EqualTo((byte)0xAB));
        Assert.That(bytes[pos++], Is.EqualTo((byte)0x34));
        Assert.That(bytes[pos++], Is.EqualTo((byte)0x12));
    }

    [Test]
    public void EncodesDoubleArrayColumnWithDimsShapesAndValues()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[] { 1.0, 2.0, 3.0 });
        table.NextRow();
        col.AddDoubleArray(new[,] { { 4.0, 5.0 }, { 6.0, 7.0 } });
        table.NextRow();

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var pos = 6 + 1; // header + null-header

        // Row 0: nDims=1, shape=3, then 3 doubles.
        Assert.That(bytes[pos++], Is.EqualTo((byte)1));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos, 4)), Is.EqualTo(3));
        pos += 4;
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(pos, 8)), Is.EqualTo(1.0)); pos += 8;
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(pos, 8)), Is.EqualTo(2.0)); pos += 8;
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(pos, 8)), Is.EqualTo(3.0)); pos += 8;

        // Row 1: nDims=2, shape=[2,2], then 4 doubles.
        Assert.That(bytes[pos++], Is.EqualTo((byte)2));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos, 4)), Is.EqualTo(2)); pos += 4;
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos, 4)), Is.EqualTo(2)); pos += 4;
        for (var i = 0; i < 4; i++)
        {
            var expected = new[] { 4.0, 5.0, 6.0, 7.0 }[i];
            Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(pos, 8)), Is.EqualTo(expected));
            pos += 8;
        }
    }

    [Test]
    public void EncodesDecimal64ColumnWithScalePrefix()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, false)!;
        col.AddDecimal64(12345, 2); // scale latches to 2
        col.AddDecimal64(67890, 2);
        table.NextRow();
        table.NextRow();

        var sink = new QwpPinnedBufferWriter();
        var writer = new QwpColumnWriter();
        writer.SetBuffer(sink);
        writer.EncodeTable(table, useSchemaRef: true, useGlobalSymbols: false, useGorilla: false);

        var bytes = sink.AsReadOnlyMemory().ToArray();
        var pos = 6 + 1; // header + null-header
        Assert.That(bytes[pos++], Is.EqualTo((byte)2)); // scale prefix
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(pos, 8)), Is.EqualTo(12345L)); pos += 8;
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(pos, 8)), Is.EqualTo(67890L));
    }

    /// <summary>Reads a single LEB128-varint at <paramref name="pos"/>, asserts it equals <paramref name="expected"/>, and advances <paramref name="pos"/>.</summary>
    private static void AssertVarintAdvance(byte[] bytes, ref int pos, long expected)
    {
        long value = 0;
        var shift = 0;
        while (true)
        {
            var b = bytes[pos++];
            value |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        Assert.That(value, Is.EqualTo(expected), $"varint at offset {pos}");
    }
}
