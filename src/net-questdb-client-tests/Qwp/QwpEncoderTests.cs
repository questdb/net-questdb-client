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
using System.Text;
using NUnit.Framework;
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpEncoderTests
{
    [Test]
    public void Encode_SingleTable_NoSymbols_ProducesByteExactFrame()
    {
        // Spec §16 example 1 adapted for the .NET WebSocket sender:
        //   table "sensors" with 2 rows; columns id (LONG), value (DOUBLE), and a designated TS.
        //   Wire differences from the spec example: FLAG_DELTA_SYMBOL_DICT is always set, so the
        //   payload begins with an empty delta-dict prelude (0x00 0x00); and the third column is
        //   the designated timestamp (empty wire name).
        var t = new QwpTableBuffer("sensors");
        t.AppendLong("id", 1);
        t.AppendDouble("value", 1.3);
        t.At(10_000_000_000L);

        t.AppendLong("id", 2);
        t.AppendDouble("value", 2.2);
        t.At(400_000L);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());

        // Expected payload layout (78 bytes = 0x4E):
        //   delta dict       : 00 00
        //   table name len/bytes: 07 'sensors'
        //   row count        : 02
        //   column count     : 03
        //   schema mode/id   : 00 00
        //   col defs         : 02 'id' 05 | 05 'value' 07 | 00 0A
        //   col0 (LONG)      : 00 | 01 00 00 00 00 00 00 00 | 02 00 00 00 00 00 00 00
        //   col1 (DOUBLE)    : 00 | <1.3 LE>                 | <2.2 LE>
        //   col2 (TIMESTAMP) : 00 | <1e10 LE>                | <400000 LE>
        var expected = ConcatBytes(
            // header
            new byte[] { 0x51, 0x57, 0x50, 0x31, 0x01, 0x08, 0x01, 0x00, 0x4E, 0x00, 0x00, 0x00 },
            // delta symbol dict prelude (empty)
            new byte[] { 0x00, 0x00 },
            // table header
            new byte[] { 0x07 },
            Encoding.UTF8.GetBytes("sensors"),
            new byte[] { 0x02, 0x03 },
            // schema
            new byte[] { 0x00, 0x00 },
            // column 0 def: "id" LONG
            new byte[] { 0x02 }, Encoding.UTF8.GetBytes("id"), new byte[] { 0x05 },
            // column 1 def: "value" DOUBLE
            new byte[] { 0x05 }, Encoding.UTF8.GetBytes("value"), new byte[] { 0x07 },
            // column 2 def: designated TS (empty name) TIMESTAMP
            new byte[] { 0x00, 0x0A },
            // column 0 data (LONG, no nulls)
            new byte[] { 0x00 },
            LittleEndianInt64(1L),
            LittleEndianInt64(2L),
            // column 1 data (DOUBLE, no nulls)
            new byte[] { 0x00 },
            LittleEndianDouble(1.3),
            LittleEndianDouble(2.2),
            // column 2 data (TIMESTAMP, no nulls)
            new byte[] { 0x00 },
            LittleEndianInt64(10_000_000_000L),
            LittleEndianInt64(400_000L));

        Assert.That(bytes, Is.EqualTo(expected),
            $"\nactual:   {Hex(bytes)}\nexpected: {Hex(expected)}");
    }

    [Test]
    public void Encode_SecondCallSameSchema_UsesReferenceMode()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        var t = new QwpTableBuffer("t");
        t.AppendLong("x", 1);
        t.At(0);

        // First encode → full schema, id 0.
        var first = QwpEncoder.Encode(new[] { t }, cache, symbols);
        Assert.That(t.SchemaId, Is.EqualTo(0));

        // Recycle the buffer: drop row data but preserve column structure and schema id.
        t.Clear();
        Assert.That(t.SchemaId, Is.EqualTo(0), "schema id survives Clear()");

        t.AppendLong("x", 2);
        t.At(1);

        var second = QwpEncoder.Encode(new[] { t }, cache, symbols);

        // Locate the schema-mode byte in each frame:
        //   header (12) + delta dict (2) + table name varint (1) + "t" (1) + row count (1) + column count (1) = 18
        Assert.That(first[18], Is.EqualTo(QwpConstants.SchemaModeFull), "first batch sends full schema");
        Assert.That(second[18], Is.EqualTo(QwpConstants.SchemaModeReference),
            "second batch references schema 0 instead of resending");
        Assert.That(second[19], Is.EqualTo((byte)0), "schema id preserved at 0");
    }

    [Test]
    public void Encode_NewColumnMidStream_ResetsSchemaToFull()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        var t = new QwpTableBuffer("t");
        t.AppendLong("x", 1);
        t.At(0);
        QwpEncoder.Encode(new[] { t }, cache, symbols); // schema id 0 allocated and sent.

        // Recycle and add a new column: SchemaId should be invalidated and a fresh id allocated.
        t.Clear();
        t.AppendLong("x", 2);
        t.AppendDouble("y", 3.14); // new column → invalidates schema id (back to -1)
        t.At(1);

        Assert.That(t.SchemaId, Is.EqualTo(-1), "adding a column resets the schema id");

        var second = QwpEncoder.Encode(new[] { t }, cache, symbols);

        Assert.That(second[18], Is.EqualTo(QwpConstants.SchemaModeFull));
        Assert.That(second[19], Is.EqualTo((byte)1), "fresh schema id allocated");
        Assert.That(t.SchemaId, Is.EqualTo(1));
    }

    [Test]
    public void Encode_WithSymbols_EmitsDeltaDictAndVarintIds()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        var idUs = symbols.Add("us");
        var idEu = symbols.Add("eu");

        var t = new QwpTableBuffer("trades");
        t.AppendSymbol("region", idUs);
        t.At(0);
        t.AppendSymbol("region", idEu);
        t.At(1);
        t.AppendSymbol("region", idUs);
        t.At(2);

        var bytes = QwpEncoder.Encode(new[] { t }, cache, symbols);

        // Header at bytes 0..11:
        Assert.That(bytes[QwpConstants.OffsetVersion], Is.EqualTo(QwpConstants.SupportedIngestVersion));
        Assert.That(bytes[QwpConstants.OffsetFlags], Is.EqualTo(QwpConstants.FlagDeltaSymbolDict));
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(QwpConstants.OffsetTableCount, 2)), Is.EqualTo(1));

        // Delta dict prelude immediately after the 12-byte header:
        //   delta_start = 0  (varint 0x00)
        //   delta_count = 2  (varint 0x02)
        //   "us": len 2, bytes 'u' 's'
        //   "eu": len 2, bytes 'e' 'u'
        Assert.That(bytes[12], Is.EqualTo(0x00), "delta_start = 0");
        Assert.That(bytes[13], Is.EqualTo(0x02), "delta_count = 2");
        Assert.That(bytes[14], Is.EqualTo(0x02), "len('us') = 2");
        Assert.That(bytes.AsSpan(15, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("us")));
        Assert.That(bytes[17], Is.EqualTo(0x02), "len('eu') = 2");
        Assert.That(bytes.AsSpan(18, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("eu")));
    }

    [Test]
    public void Encode_WithCommittedSymbols_EmitsOnlyDelta()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        // First batch: add "us", emit, commit.
        symbols.Add("us");
        var t = new QwpTableBuffer("trades");
        t.AppendSymbol("region", 0);
        t.At(0);
        QwpEncoder.Encode(new[] { t }, cache, symbols);
        symbols.Commit(); // server ACKed.

        // Second batch: add "eu" (delta is just "eu").
        symbols.Add("eu");
        t.Clear();
        t.AppendSymbol("region", 1);
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, cache, symbols);

        // Expect delta_start = 1, delta_count = 1, single entry "eu".
        Assert.That(bytes[12], Is.EqualTo(0x01), "delta_start = 1 (committed_count)");
        Assert.That(bytes[13], Is.EqualTo(0x01), "delta_count = 1");
        Assert.That(bytes[14], Is.EqualTo(0x02), "len('eu') = 2");
        Assert.That(bytes.AsSpan(15, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("eu")));
    }

    [Test]
    public void Encode_NullableVarcharColumn_OffsetsAndBitmapMatchSpecExample2()
    {
        // Spec §16 example 2: nullable string column, 4 rows, row 1 null.
        var t = new QwpTableBuffer("notes");
        t.AppendVarchar("note", "foo");
        t.At(0);
        t.At(1); // row 1: column "note" not touched → null padded.
        t.AppendVarchar("note", "bar");
        t.At(2);
        t.AppendVarchar("note", "baz");
        t.At(3);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());

        // Find the column-data section. Layout:
        //   header (12) + delta dict (2) + table header (1+5+1+1=8) + schema (2)
        //   + col defs: "note" varchar (1+4+1=6) + "" timestamp (1+1=2)
        //   = 12 + 2 + 8 + 2 + 6 + 2 = 32 → first col data at offset 32.
        var pos = 32;

        // Null flag: 0x01 (bitmap follows)
        Assert.That(bytes[pos++], Is.EqualTo(0x01));

        // Bitmap: ceil(4 / 8) = 1 byte, with bit 1 set for the null row → 0b00000010 = 0x02.
        Assert.That(bytes[pos++], Is.EqualTo(0x02));

        // Offset array: 4 uint32 LE values = [0, 3, 6, 9]
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos, 4)), Is.EqualTo(0u));
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos + 4, 4)), Is.EqualTo(3u));
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos + 8, 4)), Is.EqualTo(6u));
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos + 12, 4)), Is.EqualTo(9u));
        pos += 16;

        // String data: "foobarbaz" UTF-8 (9 bytes)
        Assert.That(bytes.AsSpan(pos, 9).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("foobarbaz")));
    }

    [Test]
    public void Encode_MultipleTables_EmitsOneFrameWithBothBlocks()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        var t1 = new QwpTableBuffer("a");
        t1.AppendLong("v", 1);
        t1.At(0);

        var t2 = new QwpTableBuffer("b");
        t2.AppendDouble("v", 2.0);
        t2.At(0);

        var bytes = QwpEncoder.Encode(new[] { t1, t2 }, cache, symbols);

        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(QwpConstants.OffsetTableCount, 2)),
            Is.EqualTo(2), "two tables in one frame");

        // Both tables get distinct, sequential schema ids.
        Assert.That(t1.SchemaId, Is.EqualTo(0));
        Assert.That(t2.SchemaId, Is.EqualTo(1));
    }

    [Test]
    public void Encode_EmptyTablesList_ProducesValidEmptyFrame()
    {
        var bytes = QwpEncoder.Encode(Array.Empty<QwpTableBuffer>(), new QwpSchemaCache(), new QwpSymbolDictionary());

        Assert.That(bytes.Length, Is.EqualTo(QwpConstants.HeaderSize + 2),
            "header + empty delta dict (00 00)");
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(QwpConstants.OffsetTableCount, 2)), Is.Zero);
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(QwpConstants.OffsetPayloadLength, 4)), Is.EqualTo(2u));
    }

    [Test]
    public void Encode_MagicBytesAreCorrect()
    {
        var bytes = QwpEncoder.Encode(Array.Empty<QwpTableBuffer>(), new QwpSchemaCache(), new QwpSymbolDictionary());
        Assert.That(bytes[0], Is.EqualTo((byte)'Q'));
        Assert.That(bytes[1], Is.EqualTo((byte)'W'));
        Assert.That(bytes[2], Is.EqualTo((byte)'P'));
        Assert.That(bytes[3], Is.EqualTo((byte)'1'));
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0, 4)), Is.EqualTo(QwpConstants.Magic));
    }

    [Test]
    public void Encode_Decimal128Column_WritesScalePrefixAndUnscaledBytes()
    {
        var t = new QwpTableBuffer("t");
        t.AppendDecimal128("price", 12.34m);
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());

        // Column-data offset: header (12) + delta (2) + table header "t" 1+1+1+1 = 4
        // + schema 2 + col defs: "price" 1+5+1=7 + "" timestamp 1+1=2 = 29.
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 7);

        // Null flag = 0 (no nulls).
        Assert.That(bytes[pos++], Is.EqualTo(0x00));

        // Scale prefix = 2.
        Assert.That(bytes[pos++], Is.EqualTo((byte)2));

        // 16 bytes LE two's complement of 1234.
        var unscaled = bytes.AsSpan(pos, 16);
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(0, 8)), Is.EqualTo(1234L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(unscaled.Slice(8, 8)), Is.EqualTo(0L));
    }

    [Test]
    public void Encode_CharColumn_WritesTwoBytesLittleEndian()
    {
        var t = new QwpTableBuffer("t");
        t.AppendChar("c", 'A');
        t.At(0);
        t.AppendChar("c", '中');
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pos, 2)), Is.EqualTo((ushort)'A'));
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pos + 2, 2)), Is.EqualTo((ushort)'中'));
    }

    [Test]
    public void Encode_Decimal64Column_WritesScalePrefixAndEightBytesPerValue()
    {
        var t = new QwpTableBuffer("t");
        t.AppendDecimal64("p", 12.34m);
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos++], Is.EqualTo((byte)2), "scale = 2");
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(pos, 8)), Is.EqualTo(1234L));
    }

    [Test]
    public void Encode_Decimal256Column_WritesScalePrefixAnd32BytesPerValue()
    {
        var t = new QwpTableBuffer("t");
        t.AppendDecimal256("p", -1m);
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos++], Is.EqualTo((byte)0), "scale = 0");
        for (var i = 0; i < 32; i++)
        {
            Assert.That(bytes[pos + i], Is.EqualTo((byte)0xFF), $"-1 LE two's-complement byte {i}");
        }
    }

    [Test]
    public void Encode_BinaryColumn_WritesOffsetsThenBytes()
    {
        var t = new QwpTableBuffer("t");
        t.AppendBinary("blob", new byte[] { 0x10, 0x20 });
        t.At(0);
        t.AppendBinary("blob", new byte[] { 0x30 });
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 4 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        // (n+1)=3 offsets × u32 LE: 0, 2, 3
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos, 4)), Is.EqualTo(0u));
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos + 4, 4)), Is.EqualTo(2u));
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos + 8, 4)), Is.EqualTo(3u));
        // Then the 3 bytes of payload.
        Assert.That(bytes[pos + 12], Is.EqualTo(0x10));
        Assert.That(bytes[pos + 13], Is.EqualTo(0x20));
        Assert.That(bytes[pos + 14], Is.EqualTo(0x30));
    }

    [Test]
    public void Encode_IPv4Column_WritesFourBytesLittleEndian()
    {
        var t = new QwpTableBuffer("t");
        t.AppendIPv4("ip", 0x04030201u);
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 2 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos, 4)), Is.EqualTo(0x04030201u));
    }

    [Test]
    public void Encode_Long256Column_Writes32BytesPerValue()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLong256("hash", new BigInteger(0xCAFEBABEUL));
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 4 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pos, 4)), Is.EqualTo(0xCAFEBABEu));
        for (var i = 4; i < 32; i++)
        {
            Assert.That(bytes[pos + i], Is.Zero, $"high byte {i}");
        }
    }

    [Test]
    public void Encode_GeohashColumn_WritesPrecisionVarintAndPackedValues()
    {
        var t = new QwpTableBuffer("t");
        t.AppendGeohash("loc", 0xABCDEF, 24);
        t.At(0);
        t.AppendGeohash("loc", 0x123456, 24);
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 3 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos++], Is.EqualTo((byte)24), "precision varint = 24");

        // 2 values × 3 bytes each, LE packed.
        Assert.That(bytes[pos], Is.EqualTo(0xEF));
        Assert.That(bytes[pos + 1], Is.EqualTo(0xCD));
        Assert.That(bytes[pos + 2], Is.EqualTo(0xAB));
        Assert.That(bytes[pos + 3], Is.EqualTo(0x56));
        Assert.That(bytes[pos + 4], Is.EqualTo(0x34));
        Assert.That(bytes[pos + 5], Is.EqualTo(0x12));
    }

    [Test]
    public void Encode_DoubleArrayColumn_WritesPerRowHeaderAndValues()
    {
        var t = new QwpTableBuffer("t");
        t.AppendDoubleArray("vec", new double[] { 1.0, 2.0 }, new[] { 2 });
        t.At(0);
        t.AppendDoubleArray("vec", new double[] { 9.0 }, new[] { 1 });
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 3 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");

        // Row 0: n_dims=1, dim_lens=[2], values=[1.0, 2.0].
        Assert.That(bytes[pos], Is.EqualTo((byte)1));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos + 1, 4)), Is.EqualTo(2));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(pos + 5, 8)), Is.EqualTo(1.0));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(pos + 13, 8)), Is.EqualTo(2.0));
        pos += 1 + 4 + 16;

        // Row 1: n_dims=1, dim_lens=[1], values=[9.0].
        Assert.That(bytes[pos], Is.EqualTo((byte)1));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos + 1, 4)), Is.EqualTo(1));
        Assert.That(BinaryPrimitives.ReadDoubleLittleEndian(bytes.AsSpan(pos + 5, 8)), Is.EqualTo(9.0));
    }

    [Test]
    public void Encode_GorillaEnabled_SetsHeaderFlagAndPrependsEncodingByte()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLong("v", 1L);
        t.At(1_000_000L);
        t.AppendLong("v", 2L);
        t.At(1_000_001L);
        t.AppendLong("v", 3L);
        t.At(1_000_002L);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary(), gorillaEnabled: true);

        // Header flags must include FLAG_GORILLA on top of FLAG_DELTA_SYMBOL_DICT.
        var expectedFlags = (byte)(QwpConstants.FlagDeltaSymbolDict | QwpConstants.FlagGorilla);
        Assert.That(bytes[QwpConstants.OffsetFlags], Is.EqualTo(expectedFlags));

        // Layout to the long column data:
        //   header(12) + delta(2) + tableNameLen(1) + "t"(1) + rowCount(1) + colCount(1) + schema(2)
        //   + col defs: "v" LONG (1+1+1=3) + "" TIMESTAMP (1+1=2) = 25.
        // The long column is fixed-width (LONG, not TIMESTAMP), so no encoding-flag byte for it.
        var pos = 25;
        Assert.That(bytes[pos++], Is.EqualTo(0x00), "long col null flag");
        // 3 longs = 24 bytes
        pos += 24;

        // Designated TS column: encoding-flag should be present because gorillaEnabled is true.
        Assert.That(bytes[pos++], Is.EqualTo(0x00), "ts col null flag");
        var encodingFlag = bytes[pos];
        Assert.That(encodingFlag is QwpGorilla.EncodingUncompressed or QwpGorilla.EncodingGorilla,
            "encoding flag must be 0x00 or 0x01");
    }

    [Test]
    public void Encode_GorillaDisabled_OmitsEncodingByteForTimestamps()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLong("v", 1L);
        t.At(1_000_000L);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary(), gorillaEnabled: false);

        Assert.That(bytes[QwpConstants.OffsetFlags], Is.EqualTo(QwpConstants.FlagDeltaSymbolDict));

        // Timestamp column data starts at the same offset; first byte is null flag (0), then 8 bytes of TS.
        // Layout: header(12) + delta(2) + tableHeader(1+1+1+1=4) + schema(2) + col defs(3+2=5) = 25.
        // long col data: null flag (1) + 8 bytes = 9. Then ts col data starts at 25 + 9 = 34.
        var pos = 25 + 9;
        Assert.That(bytes[pos++], Is.EqualTo(0x00), "ts null flag");
        // Next 8 bytes should be the timestamp directly (no encoding flag preamble).
        var ts = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(pos, 8));
        Assert.That(ts, Is.EqualTo(1_000_000L));
    }

    [Test]
    public void Encode_LongArrayColumn_WritesInt64Values()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLongArray("v", new long[] { -7, 7 }, new[] { 2 });
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSchemaCache(), new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos], Is.EqualTo((byte)1)); // n_dims
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pos + 1, 4)), Is.EqualTo(2));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(pos + 5, 8)), Is.EqualTo(-7L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(pos + 13, 8)), Is.EqualTo(7L));
    }

    [Test]
    public void Encode_SelfSufficient_AlwaysEmitsFullSchema()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        var t = new QwpTableBuffer("t");
        t.AppendLong("x", 1);
        t.At(0);

        var first = QwpEncoder.Encode(new[] { t }, cache, symbols, selfSufficient: true);
        // SF mode uses frame-local schema ids (0..tables-1) and never mutates the per-connection
        // schemaCache; the table buffer's SchemaId stays unassigned.
        Assert.That(t.SchemaId, Is.EqualTo(QwpSchemaCache.UnassignedSchemaId));
        Assert.That(cache.AllocatedCount, Is.EqualTo(0), "SF must not bump the connection counter");

        t.Clear();
        t.AppendLong("x", 2);
        t.At(1);

        var second = QwpEncoder.Encode(new[] { t }, cache, symbols, selfSufficient: true);

        // Both frames must carry the full schema; receiver may have no prior connection state.
        Assert.That(first[18], Is.EqualTo(QwpConstants.SchemaModeFull), "first frame: full");
        Assert.That(second[18], Is.EqualTo(QwpConstants.SchemaModeFull), "second frame: still full in SF mode");
        Assert.That(second[19], Is.EqualTo((byte)0), "schema id reused");
    }

    [Test]
    public void Encode_SelfSufficient_EmitsFullSymbolDictAfterCommit()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        symbols.Add("us");
        symbols.Add("eu");
        // Pretend the server already acked these in a prior connection.
        symbols.Commit();

        var t = new QwpTableBuffer("trades");
        t.AppendSymbol("region", 0);
        t.At(0);
        t.AppendSymbol("region", 1);
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, cache, symbols, selfSufficient: true);

        // SF replay: every frame restates the dictionary from id 0.
        Assert.That(bytes[12], Is.EqualTo(0x00), "delta_start = 0 in SF mode (committed watermark ignored)");
        Assert.That(bytes[13], Is.EqualTo(0x02), "delta_count = full Count");
        Assert.That(bytes[14], Is.EqualTo(0x02), "len('us')");
        Assert.That(bytes.AsSpan(15, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("us")));
        Assert.That(bytes[17], Is.EqualTo(0x02), "len('eu')");
        Assert.That(bytes.AsSpan(18, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("eu")));
    }

    [Test]
    public void Encode_SelfSufficient_TwoTablesInSameFrame_BothEmitFullSchema()
    {
        var cache = new QwpSchemaCache();
        var symbols = new QwpSymbolDictionary();

        var ta = new QwpTableBuffer("a");
        ta.AppendLong("v", 1);
        ta.At(0);

        var tb = new QwpTableBuffer("b");
        tb.AppendDouble("v", 2.0);
        tb.At(0);

        // Encode once first to push schemas into the cache, then a second time in self-sufficient mode.
        QwpEncoder.Encode(new[] { ta, tb }, cache, symbols);
        ta.Clear();
        tb.Clear();
        ta.AppendLong("v", 11);
        ta.At(1);
        tb.AppendDouble("v", 22.0);
        tb.At(1);

        var bytes = QwpEncoder.Encode(new[] { ta, tb }, cache, symbols, selfSufficient: true);

        // Walk to each table's schema-mode byte and assert Full. Layout per table:
        //   tableName varint(1) + name(1) + rowCount(1) + colCount(1) + schemaMode(1) + schemaId(1)
        //   + columnDef("v"): nameLen(1) + "v"(1) + type(1) = 3
        //   + designatedTsColDef: nameLen(1)=0 + type(1) = 2
        //   + colData("v"): nullFlag(1) + 8 bytes value = 9
        //   + colData(""): nullFlag(1) + 8 bytes value = 9
        //   = 1+1+1+1+1+1+3+2+9+9 = 29 bytes per table block
        // First table starts at offset 14 (header 12 + delta 2). Schema mode byte is at +4 inside the
        // table block (after name varint + name + rowCount + colCount).
        const int firstTableStart = 14;
        const int schemaModeOffsetInTable = 4;
        const int tableBlockSize = 29;

        Assert.That(bytes[firstTableStart + schemaModeOffsetInTable], Is.EqualTo(QwpConstants.SchemaModeFull), "table A: full");
        Assert.That(bytes[firstTableStart + tableBlockSize + schemaModeOffsetInTable], Is.EqualTo(QwpConstants.SchemaModeFull), "table B: full");
    }

    /// <summary>
    ///     Returns the byte offset at which the first user column's data section starts inside an
    ///     encoded frame with one user column followed by a designated TS column.
    /// </summary>
    /// <remarks>
    ///     Layout up to the first column-data section:
    ///     <c>header(12) + delta(2) + tableNameLen(1) + tableName + rowCount(1) + colCount(1) + schema(2)
    ///     + userColDef + tsColDef(1+0+1=2)</c>.
    /// </remarks>
    private static int FindFirstColumnDataOffset(byte[] frame, int tableNameLen, int userColCount, int userColDefSize)
    {
        // userColCount unused for the simple single-user-column tests above; kept for future
        // multi-column encoder fixtures.
        _ = userColCount;
        _ = frame;

        // 12 (header) + 2 (delta dict) + 1 (table name varint) + tableNameLen + 1 (rowCount) + 1 (colCount)
        // + 2 (schema mode + id) + userColDefSize + 2 (designated TS def: empty name varint=0 + TIMESTAMP)
        return 12 + 2 + 1 + tableNameLen + 1 + 1 + 2 + userColDefSize + 2;
    }

    private static byte[] ConcatBytes(params byte[][] parts)
    {
        var total = 0;
        foreach (var p in parts) total += p.Length;
        var result = new byte[total];
        var offset = 0;
        foreach (var p in parts)
        {
            Buffer.BlockCopy(p, 0, result, offset, p.Length);
            offset += p.Length;
        }

        return result;
    }

    private static byte[] LittleEndianInt64(long v)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, v);
        return bytes;
    }

    private static byte[] LittleEndianDouble(double v)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(bytes, v);
        return bytes;
    }

    private static string Hex(byte[] b)
    {
        return BitConverter.ToString(b).Replace('-', ' ');
    }
}
