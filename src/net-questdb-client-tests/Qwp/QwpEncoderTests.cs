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
        var t = new QwpTableBuffer("sensors");
        t.AppendLong("id", 1);
        t.AppendDouble("value", 1.3);
        t.At(10_000_000_000L);

        t.AppendLong("id", 2);
        t.AppendDouble("value", 2.2);
        t.At(400_000L);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());

        var expected = ConcatBytes(
            // header: magic "QWP1", version 1, flags 0x0C (DELTA_SYMBOL_DICT | GORILLA),
            // tableCount 1, payloadLen 0x4D = 77.
            new byte[] { 0x51, 0x57, 0x50, 0x31, 0x01, 0x0C, 0x01, 0x00, 0x4D, 0x00, 0x00, 0x00 },
            new byte[] { 0x00, 0x00 },
            new byte[] { 0x07 },
            Encoding.UTF8.GetBytes("sensors"),
            new byte[] { 0x02, 0x03 },
            new byte[] { 0x02 }, Encoding.UTF8.GetBytes("id"), new byte[] { 0x05 },
            new byte[] { 0x05 }, Encoding.UTF8.GetBytes("value"), new byte[] { 0x07 },
            new byte[] { 0x00, 0x0A },
            new byte[] { 0x00 },
            LittleEndianInt64(1L),
            LittleEndianInt64(2L),
            new byte[] { 0x00 },
            LittleEndianDouble(1.3),
            LittleEndianDouble(2.2),
            new byte[] { 0x00 },                    // designated-TS null flag
            new byte[] { 0x00 },                    // Gorilla encoding flag: 0x00 uncompressed (< 3 values)
            LittleEndianInt64(10_000_000_000L),
            LittleEndianInt64(400_000L));

        Assert.That(bytes, Is.EqualTo(expected),
            $"\nactual:   {Hex(bytes)}\nexpected: {Hex(expected)}");
    }

    [Test]
    public void Encode_SecondCallSameSchema_StillEmitsInlineSchema()
    {
        // Schemas always travel inline now; encoding the same buffer twice produces frames whose
        // table-block schema sections are byte-identical (modulo row data).
        var symbols = new QwpSymbolDictionary();

        var t = new QwpTableBuffer("t");
        t.AppendLong("x", 1);
        t.At(0);

        var first = QwpEncoder.Encode(new[] { t }, symbols);

        t.Clear();
        t.AppendLong("x", 2);
        t.At(1);

        var second = QwpEncoder.Encode(new[] { t }, symbols);

        // Both frames must start with the same prelude+table-name+row_count+col_count+column-defs
        // prefix — i.e. the schema is re-emitted inline on every call.
        //   header (12) + delta dict (2) + table name varint (1) + "t" (1)
        //   + row count (1) + col count (1) + col name varint (1) + "x" (1) + type code (1) = 21
        const int schemaPrefixEnd = 21;
        Assert.That(first.AsSpan(14, schemaPrefixEnd - 14).ToArray(),
            Is.EqualTo(second.AsSpan(14, schemaPrefixEnd - 14).ToArray()),
            "table-block schema section must be byte-identical when re-encoded");
    }

    [Test]
    public void Encode_NewColumnMidStream_ReEmitsFullSchema()
    {
        // A new column changes the inline schema; the next frame's column-defs section grows
        // accordingly. (There is no schema-id machinery to invalidate.)
        var symbols = new QwpSymbolDictionary();

        var t = new QwpTableBuffer("t");
        t.AppendLong("x", 1);
        t.At(0);
        var first = QwpEncoder.Encode(new[] { t }, symbols);

        t.Clear();
        t.AppendLong("x", 2);
        t.AppendDouble("y", 3.14);
        t.At(1);

        var second = QwpEncoder.Encode(new[] { t }, symbols);

        // Second frame must be longer (extra column def + extra column-data section).
        Assert.That(second.Length, Is.GreaterThan(first.Length),
            "adding a column grows the next frame's inline schema and data sections");
    }

    [Test]
    public void Encode_WithSymbols_EmitsDeltaDictAndVarintIds()
    {
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

        var bytes = QwpEncoder.Encode(new[] { t }, symbols);

        // Header at bytes 0..11:
        Assert.That(bytes[QwpConstants.OffsetVersion], Is.EqualTo(QwpConstants.SupportedVersion));
        Assert.That(bytes[QwpConstants.OffsetFlags],
            Is.EqualTo((byte)(QwpConstants.FlagDeltaSymbolDict | QwpConstants.FlagGorilla)));
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
        var symbols = new QwpSymbolDictionary();

        // First batch: add "us", emit, commit.
        symbols.Add("us");
        var t = new QwpTableBuffer("trades");
        t.AppendSymbol("region", 0);
        t.At(0);
        QwpEncoder.Encode(new[] { t }, symbols);
        symbols.Commit(); // server ACKed.

        // Second batch: add "eu" (delta is just "eu").
        symbols.Add("eu");
        t.Clear();
        t.AppendSymbol("region", 1);
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, symbols);

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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());

        // Find the column-data section. Layout:
        //   header (12) + delta dict (2) + table header (1+5+1+1=8)
        //   + col defs: "note" varchar (1+4+1=6) + "" timestamp (1+1=2)
        //   = 12 + 2 + 8 + 6 + 2 = 30 → first col data at offset 30.
        var pos = 30;

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
        var symbols = new QwpSymbolDictionary();

        var t1 = new QwpTableBuffer("a");
        t1.AppendLong("v", 1);
        t1.At(0);

        var t2 = new QwpTableBuffer("b");
        t2.AppendDouble("v", 2.0);
        t2.At(0);

        var bytes = QwpEncoder.Encode(new[] { t1, t2 }, symbols);

        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(QwpConstants.OffsetTableCount, 2)),
            Is.EqualTo(2), "two tables in one frame");
    }

    [Test]
    public void Encode_EmptyTablesList_ProducesValidEmptyFrame()
    {
        var bytes = QwpEncoder.Encode(Array.Empty<QwpTableBuffer>(), new QwpSymbolDictionary());

        Assert.That(bytes.Length, Is.EqualTo(QwpConstants.HeaderSize + 2),
            "header + empty delta dict (00 00)");
        Assert.That(BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(QwpConstants.OffsetTableCount, 2)), Is.Zero);
        Assert.That(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(QwpConstants.OffsetPayloadLength, 4)), Is.EqualTo(2u));
    }

    [Test]
    public void Encode_MagicBytesAreCorrect()
    {
        var bytes = QwpEncoder.Encode(Array.Empty<QwpTableBuffer>(), new QwpSymbolDictionary());
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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());

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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos++], Is.EqualTo((byte)0), "scale = 0");
        for (var i = 0; i < 32; i++)
        {
            Assert.That(bytes[pos + i], Is.EqualTo((byte)0xFF), $"-1 LE two's-complement byte {i}");
        }
    }

    [Test]
    public void Encode_Decimal64Column_LimbForm_WritesScaleAndUnscaledLimb()
    {
        var t = new QwpTableBuffer("t");
        t.AppendDecimal64("p", 1234567890123L, scale: 4);
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos++], Is.EqualTo((byte)4), "scale = 4");
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(pos, 8)),
            Is.EqualTo(1234567890123L));
    }

    [Test]
    public void Encode_Decimal128Column_LimbForm_WritesLoThenHiLittleEndian()
    {
        var t = new QwpTableBuffer("t");
        t.AppendDecimal128("p", lo: 0x0102030405060708L, hi: 0x1112131415161718L, scale: 6);
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos++], Is.EqualTo((byte)6), "scale = 6");
        var span = bytes.AsSpan(pos, 16);
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(0, 8)), Is.EqualTo(0x0102030405060708L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(8, 8)), Is.EqualTo(0x1112131415161718L));
    }

    [Test]
    public void Encode_Decimal256Column_LimbForm_WritesFourLimbsLsbFirst()
    {
        var t = new QwpTableBuffer("t");
        t.AppendDecimal256("p",
            l0: 0x0102030405060708L,
            l1: 0x1112131415161718L,
            l2: 0x2122232425262728L,
            l3: 0x3132333435363738L,
            scale: 12);
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
        var pos = FindFirstColumnDataOffset(bytes, tableNameLen: 1, userColCount: 1, userColDefSize: 1 + 1 + 1);

        Assert.That(bytes[pos++], Is.EqualTo(0x00), "null flag");
        Assert.That(bytes[pos++], Is.EqualTo((byte)12), "scale = 12");
        var span = bytes.AsSpan(pos, 32);
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(0, 8)), Is.EqualTo(0x0102030405060708L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(8, 8)), Is.EqualTo(0x1112131415161718L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(16, 8)), Is.EqualTo(0x2122232425262728L));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(24, 8)), Is.EqualTo(0x3132333435363738L));
    }

    [Test]
    public void Encode_BinaryColumn_WritesOffsetsThenBytes()
    {
        var t = new QwpTableBuffer("t");
        t.AppendBinary("blob", new byte[] { 0x10, 0x20 });
        t.At(0);
        t.AppendBinary("blob", new byte[] { 0x30 });
        t.At(1);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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
    public void Encode_SetsGorillaHeaderFlagAndPrependsEncodingByte()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLong("v", 1L);
        t.At(1_000_000L);
        t.AppendLong("v", 2L);
        t.At(1_000_001L);
        t.AppendLong("v", 3L);
        t.At(1_000_002L);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());

        var expectedFlags = (byte)(QwpConstants.FlagDeltaSymbolDict | QwpConstants.FlagGorilla);
        Assert.That(bytes[QwpConstants.OffsetFlags], Is.EqualTo(expectedFlags));

        // header(12) + delta(2) + tableNameLen(1) + "t"(1) + rowCount(1) + colCount(1)
        // + col defs: "v" LONG (1+1+1=3) + "" TIMESTAMP (1+1=2) = 23.
        var pos = 23;
        Assert.That(bytes[pos++], Is.EqualTo(0x00), "long col null flag");
        // 3 longs = 24 bytes
        pos += 24;

        // Designated TS column: the encoding-flag byte is always present.
        Assert.That(bytes[pos++], Is.EqualTo(0x00), "ts col null flag");
        var encodingFlag = bytes[pos];
        Assert.That(encodingFlag is QwpGorilla.EncodingUncompressed or QwpGorilla.EncodingGorilla,
            "encoding flag must be 0x00 or 0x01");
    }

    [Test]
    public void Encode_LongArrayColumn_WritesInt64Values()
    {
        var t = new QwpTableBuffer("t");
        t.AppendLongArray("v", new long[] { -7, 7 }, new[] { 2 });
        t.At(0);

        var bytes = QwpEncoder.Encode(new[] { t }, new QwpSymbolDictionary());
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
        var symbols = new QwpSymbolDictionary();

        var t = new QwpTableBuffer("t");
        t.AppendLong("x", 1);
        t.At(0);

        var first = QwpEncoder.Encode(new[] { t }, symbols, selfSufficient: true);

        t.Clear();
        t.AppendLong("x", 2);
        t.At(1);

        var second = QwpEncoder.Encode(new[] { t }, symbols, selfSufficient: true);

        // Both frames carry the same inline schema; receiver needs no prior connection state.
        // Schema spans header (12) + dict (2) + name varint (1) + "t" (1) + row count (1)
        // + col count (1) + col name varint (1) + "x" (1) + type code (1) = 21.
        const int schemaPrefixEnd = 21;
        Assert.That(first.AsSpan(14, schemaPrefixEnd - 14).ToArray(),
            Is.EqualTo(second.AsSpan(14, schemaPrefixEnd - 14).ToArray()),
            "SF frames must re-emit identical inline schemas every time");
    }

    [Test]
    public void Encode_SelfSufficient_EmitsFullSymbolDictAfterCommit()
    {
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

        var bytes = QwpEncoder.Encode(new[] { t }, symbols, selfSufficient: true);

        // SF replay: every frame restates the dictionary from id 0.
        Assert.That(bytes[12], Is.EqualTo(0x00), "delta_start = 0 in SF mode (committed watermark ignored)");
        Assert.That(bytes[13], Is.EqualTo(0x02), "delta_count = full Count");
        Assert.That(bytes[14], Is.EqualTo(0x02), "len('us')");
        Assert.That(bytes.AsSpan(15, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("us")));
        Assert.That(bytes[17], Is.EqualTo(0x02), "len('eu')");
        Assert.That(bytes.AsSpan(18, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("eu")));
    }

    [Test]
    public void EncodeInto_SelfSufficient_SymbolDeltaCount_EmitsOnlyDictPrefix()
    {
        var symbols = new QwpSymbolDictionary();
        symbols.Add("us");
        symbols.Add("eu");
        symbols.Add("ap");

        var t = new QwpTableBuffer("trades");
        t.AppendSymbol("region", 0);
        t.At(0);
        t.AppendSymbol("region", 1);
        t.At(1);

        // Self-sufficient with an explicit prefix length of 2: only ids [0, 2) are restated.
        var builder = new QwpEncoder.FrameBuilder(4096);
        var len = QwpEncoder.EncodeInto(builder, new[] { t }, symbols,
            selfSufficient: true, symbolDeltaCount: 2);
        var bytes = builder.AsSpan(0, len).ToArray();

        // Delta dict prelude follows the 12-byte header.
        Assert.That(bytes[12], Is.EqualTo(0x00), "delta_start = 0");
        Assert.That(bytes[13], Is.EqualTo(0x02), "delta_count = symbolDeltaCount, not full Count of 3");
        Assert.That(bytes[14], Is.EqualTo(0x02), "len('us')");
        Assert.That(bytes.AsSpan(15, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("us")));
        Assert.That(bytes[17], Is.EqualTo(0x02), "len('eu')");
        Assert.That(bytes.AsSpan(18, 2).ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("eu")));
        // "ap" (id 2) lies beyond the prefix; byte 20 is the table-name varint, not another dict entry.
        Assert.That(bytes[20], Is.EqualTo((byte)"trades".Length), "table block starts right after the 2-entry prefix");
    }

    [Test]
    public void EncodeInto_SelfSufficient_SymbolDeltaCountZero_EmitsEmptyDict()
    {
        var symbols = new QwpSymbolDictionary();
        symbols.Add("us");
        symbols.Add("eu");

        var t = new QwpTableBuffer("t");
        t.AppendSymbol("region", 0);
        t.At(0);

        var builder = new QwpEncoder.FrameBuilder(4096);
        var len = QwpEncoder.EncodeInto(builder, new[] { t }, symbols,
            selfSufficient: true, symbolDeltaCount: 0);
        var bytes = builder.AsSpan(0, len).ToArray();

        Assert.That(bytes[12], Is.EqualTo(0x00), "delta_start = 0");
        Assert.That(bytes[13], Is.EqualTo(0x00), "delta_count = 0 → no dict entries emitted");
        Assert.That(bytes[14], Is.EqualTo((byte)1), "table block ('t') starts immediately after the empty dict");
    }

    [Test]
    public void Encode_SelfSufficient_TwoTablesInSameFrame_BothEmitFullSchema()
    {
        var symbols = new QwpSymbolDictionary();

        var ta = new QwpTableBuffer("a");
        ta.AppendLong("v", 1);
        ta.At(0);

        var tb = new QwpTableBuffer("b");
        tb.AppendDouble("v", 2.0);
        tb.At(0);

        // Encode once first to push schemas into the cache, then a second time in self-sufficient mode.
        QwpEncoder.Encode(new[] { ta, tb }, symbols);
        ta.Clear();
        tb.Clear();
        ta.AppendLong("v", 11);
        ta.At(1);
        tb.AppendDouble("v", 22.0);
        tb.At(1);

        var bytes = QwpEncoder.Encode(new[] { ta, tb }, symbols, selfSufficient: true);

        // Layout per table block (no schema_mode / schema_id any more):
        //   tableName varint(1) + name(1) + rowCount(1) + colCount(1)
        //   + columnDef("v"): nameLen(1) + "v"(1) + type(1) = 3
        //   + designatedTsColDef: nameLen(1)=0 + type(1) = 2
        //   + colData("v"): nullFlag(1) + 8 bytes value = 9
        //   + colData(""): nullFlag(1) + Gorilla encoding flag(1) + 8 bytes value = 10
        //   = 1+1+1+1+3+2+9+10 = 28 bytes per table block.
        // The designated-TS column carries a 1-byte Gorilla encoding flag (0x00 uncompressed for a
        // single value) ahead of its data, per the always-on FLAG_GORILLA contract.
        // First table starts at offset 14 (header 12 + delta 2). The name varint of the second
        // table is the byte immediately after the first table block.
        const int firstTableStart = 14;
        const int tableBlockSize = 28;
        Assert.That(bytes[firstTableStart], Is.EqualTo((byte)"a".Length),
            "table A name length leads the first table block");
        Assert.That(bytes[firstTableStart + tableBlockSize], Is.EqualTo((byte)"b".Length),
            "table B name length leads the second table block (no shared schema-id between them)");
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
        // + userColDefSize + 2 (designated TS def: empty name varint=0 + TIMESTAMP)
        return 12 + 2 + 1 + tableNameLen + 1 + 1 + userColDefSize + 2;
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
