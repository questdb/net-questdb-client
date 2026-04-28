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

/// <summary>
///     Frame-level tests for the RESULT_BATCH wire decoder. Hand-builds payloads
///     with the same byte layout the server emits and checks the decoder's
///     batch view + error paths.
/// </summary>
[TestFixture]
public class QwpResultBatchDecoderTests
{
    [Test]
    public void DecodesSingleNonNullableLongColumn()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(3)
            .AddNonNullableColumn("v", QwpConstants.TYPE_LONG, valueWriter: w =>
            {
                w.WriteLongLE(10L);
                w.WriteLongLE(20L);
                w.WriteLongLE(30L);
            })
            .Build();
        var batch = Decode(bytes);

        Assert.That(batch.RowCount, Is.EqualTo(3));
        Assert.That(batch.ColumnCount, Is.EqualTo(1));
        Assert.That(batch.GetColumnWireType(0), Is.EqualTo(QwpConstants.TYPE_LONG));
        Assert.That(batch.GetLongValue(0, 0), Is.EqualTo(10L));
        Assert.That(batch.GetLongValue(0, 1), Is.EqualTo(20L));
        Assert.That(batch.GetLongValue(0, 2), Is.EqualTo(30L));
    }

    [Test]
    public void DecodesNullableLongColumn()
    {
        // 4 rows: rows 1 and 3 null. bitmap byte = 0b0000_1010 = 0x0A. dense values: 100, 200.
        var bytes = new FrameBuilder()
            .WithRowCount(4)
            .AddNullableColumn("v", QwpConstants.TYPE_LONG,
                bitmap: new byte[] { 0b0000_1010 },
                valueWriter: w =>
                {
                    w.WriteLongLE(100L);
                    w.WriteLongLE(200L);
                })
            .Build();
        var batch = Decode(bytes);

        Assert.That(batch.IsNull(0, 0), Is.False);
        Assert.That(batch.IsNull(0, 1), Is.True);
        Assert.That(batch.IsNull(0, 2), Is.False);
        Assert.That(batch.IsNull(0, 3), Is.True);
        Assert.That(batch.GetLongValue(0, 0), Is.EqualTo(100L));
        Assert.That(batch.GetLongValue(0, 2), Is.EqualTo(200L));
    }

    [Test]
    public void DecodesMultipleColumnsInOrder()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(2)
            .AddNonNullableColumn("a", QwpConstants.TYPE_INT, w =>
            {
                w.WriteIntLE(7);
                w.WriteIntLE(8);
            })
            .AddNonNullableColumn("b", QwpConstants.TYPE_VARCHAR, w =>
            {
                // offsets: 0, 5, 8. bytes: "alphabd"  wait, need clean strings.
                // "alpha" + "bd" = 5 + 2 = 7 bytes total.
                w.WriteIntLE(0);
                w.WriteIntLE(5);
                w.WriteIntLE(7);
                w.WriteUtf8("alpha");
                w.WriteUtf8("bd");
            })
            .Build();
        var batch = Decode(bytes);

        Assert.That(batch.ColumnCount, Is.EqualTo(2));
        Assert.That(batch.GetIntValue(0, 0), Is.EqualTo(7));
        Assert.That(batch.GetIntValue(0, 1), Is.EqualTo(8));
        Assert.That(batch.GetString(1, 0), Is.EqualTo("alpha"));
        Assert.That(batch.GetString(1, 1), Is.EqualTo("bd"));
    }

    [Test]
    public void DecodesDecimal64ColumnPicksUpScale()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(2)
            .AddDecimalColumn("d", QwpConstants.TYPE_DECIMAL64, scale: 4,
                valueWriter: w =>
                {
                    w.WriteLongLE(12345L);
                    w.WriteLongLE(67890L);
                })
            .Build();
        var batch = Decode(bytes);
        Assert.That(batch.GetLayout(0).Info!.DecimalScale, Is.EqualTo((sbyte)4));
        Assert.That(batch.GetLongValue(0, 0), Is.EqualTo(12345L));
        Assert.That(batch.GetLongValue(0, 1), Is.EqualTo(67890L));
    }

    [Test]
    public void DecodesGeohashColumnPicksUpPrecision()
    {
        // Precision 12 bits = 2 bytes per value.
        var bytes = new FrameBuilder()
            .WithRowCount(2)
            .AddGeohashColumn("g", precisionBits: 12, valueWriter: w =>
            {
                // 0x0FFF, 0x0123 — written low byte first.
                w.WriteByte(0xFF);
                w.WriteByte(0x0F);
                w.WriteByte(0x23);
                w.WriteByte(0x01);
            })
            .Build();
        var batch = Decode(bytes);
        Assert.That(batch.GetLayout(0).Info!.GeohashPrecisionBits, Is.EqualTo(12));
    }

    [Test]
    public void RejectsBadMagic()
    {
        var bytes = new FrameBuilder().WithRowCount(0).Build();
        // Stomp the magic bytes.
        bytes[0] = 0xDE; bytes[1] = 0xAD; bytes[2] = 0xBE; bytes[3] = 0xEF;
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("bad magic"));
    }

    [Test]
    public void RejectsUnsupportedVersion()
    {
        var bytes = new FrameBuilder().WithRowCount(0).Build();
        bytes[4] = 99;
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("unsupported version"));
    }

    [Test]
    public void RejectsZstdFlagWithClearError()
    {
        var bytes = new FrameBuilder().WithRowCount(0).Build();
        bytes[QwpConstants.HEADER_OFFSET_FLAGS] |= QwpConstants.FLAG_ZSTD;
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("FLAG_ZSTD"));
    }

    // RejectsGorillaFlagWithClearError lived here as a placeholder for "decoder
    // rejects FLAG_GORILLA until §3.2b lands". §3.2b is now in; positive tests
    // below cover the raw + Gorilla DoD encoding paths.

    [Test]
    public void DecodesTimestampColumn_FlagGorillaRawEncoding()
    {
        // Encoder discriminator byte 0x00 → values inline as 8-byte raw.
        var timestamps = new[] { 1_000_000_000L, 1_001_000_000L, 1_002_000_000L };
        var bytes = new FrameBuilder()
            .WithRowCount(timestamps.Length)
            .WithFlagGorilla()
            .AddNonNullableColumn("ts", QwpConstants.TYPE_TIMESTAMP, w =>
            {
                w.WriteByte(0x00); // raw encoding
                foreach (var t in timestamps) w.WriteLongLE(t);
            })
            .Build();

        var batch = Decode(bytes);
        for (var i = 0; i < timestamps.Length; i++)
        {
            Assert.That(batch.GetLongValue(0, i), Is.EqualTo(timestamps[i]));
        }
    }

    [Test]
    public void DecodesTimestampColumn_FlagGorillaDoDEncoding_RoundTripsTimestamps()
    {
        // Five timestamps with constant 1ns delta — the encoder hits the 1-bit DoD=0
        // bucket for rows 2-4 after the two raw seeds.
        var timestamps = new[] { 100_000L, 100_001L, 100_002L, 100_003L, 100_004L };
        var encoded = new byte[QwpGorillaEncoder.CalculateEncodedSize(timestamps)];
        var encoder = new QwpGorillaEncoder();
        var encodedLen = encoder.EncodeTimestamps(encoded, 0, timestamps);

        var bytes = new FrameBuilder()
            .WithRowCount(timestamps.Length)
            .WithFlagGorilla()
            .AddNonNullableColumn("ts", QwpConstants.TYPE_TIMESTAMP, w =>
            {
                w.WriteByte(0x01); // Gorilla DoD encoding
                w.WriteRaw(encoded.AsSpan(0, encodedLen).ToArray());
            })
            .Build();

        var batch = Decode(bytes);
        for (var i = 0; i < timestamps.Length; i++)
        {
            Assert.That(batch.GetLongValue(0, i), Is.EqualTo(timestamps[i]),
                $"row {i} must round-trip through Gorilla decode");
        }
    }

    [Test]
    public void DecodesTimestampColumn_FlagGorillaUnknownEncodingByteRejects()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(1)
            .WithFlagGorilla()
            .AddNonNullableColumn("ts", QwpConstants.TYPE_TIMESTAMP, w =>
            {
                w.WriteByte(0x42); // not 0x00 or 0x01
            })
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("unknown TIMESTAMP encoding"));
    }

    // RejectsDeltaSymbolDictFlagWithClearError used to live here as a placeholder for
    // "decoder rejects FLAG_DELTA_SYMBOL_DICT until §3.2c lands". §3.2c is now in;
    // the positive delta-mode tests below exercise the decoder's real behaviour.

    [Test]
    public void RejectsWrongMsgKind()
    {
        var bytes = new FrameBuilder().WithRowCount(0).Build();
        bytes[QwpConstants.HEADER_SIZE] = QwpEgressMsgKind.QUERY_ERROR;
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("expected RESULT_BATCH"));
    }

    [Test]
    public void RejectsTruncatedTooShort()
    {
        var bytes = new byte[QwpConstants.HEADER_SIZE]; // header only, no body
        BinaryPrimitives.WriteInt32LittleEndian(bytes, QwpConstants.MAGIC_MESSAGE);
        bytes[4] = QwpConstants.VERSION_2;
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("too short"));
    }

    [Test]
    public void RejectsRowCountOutOfRange()
    {
        // Row count 2_000_000 > MAX_ROWS_PER_BATCH (1_048_576).
        var bytes = new FrameBuilder()
            .WithRowCount(2_000_000)
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("row_count"));
    }

    // ---- §3.2d — SCHEMA_MODE_REFERENCE + schema fingerprint cache ----

    [Test]
    public void DecodesSchemaModeReference_AfterSchemaFullRegistration()
    {
        // First batch: SCHEMA_MODE_FULL with one LONG column "v" — registers schema_id=0.
        // Second batch: SCHEMA_MODE_REFERENCE with schema_id=0 — resolves to the same schema.
        var first = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("v", QwpConstants.TYPE_LONG, w => w.WriteLongLE(42L))
            .Build();
        var second = new FrameBuilder()
            .WithRowCount(1)
            .WithSchemaMode(QwpConstants.SCHEMA_MODE_REFERENCE)
            .AddNonNullableColumn("v", QwpConstants.TYPE_LONG, w => w.WriteLongLE(99L))
            .Build();

        var decoder = new QwpResultBatchDecoder();
        var buf = new QwpBatchBuffer(Math.Max(first.Length, second.Length));
        buf.CopyFromPayload(first);
        decoder.Decode(buf);

        buf.CopyFromPayload(second);
        decoder.Decode(buf);

        Assert.That(buf.Batch.GetLayout(0).Info!.Name, Is.EqualTo("v"));
        Assert.That(buf.Batch.GetLayout(0).Info!.WireType, Is.EqualTo(QwpConstants.TYPE_LONG));
        Assert.That(buf.Batch.GetLongValue(0, 0), Is.EqualTo(99L));
    }

    [Test]
    public void DecodesSchemaModeReference_RejectsUnregisteredSchemaId()
    {
        // Reference a schema_id that was never registered.
        var bytes = new FrameBuilder()
            .WithRowCount(0)
            .WithSchemaMode(QwpConstants.SCHEMA_MODE_REFERENCE)
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("not registered"));
    }

    [Test]
    public void ApplyCacheReset_WipesSchemaRegistry()
    {
        var first = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("v", QwpConstants.TYPE_LONG, w => w.WriteLongLE(1L))
            .Build();
        var refOnly = new FrameBuilder()
            .WithRowCount(0)
            .WithSchemaMode(QwpConstants.SCHEMA_MODE_REFERENCE)
            .Build();

        var decoder = new QwpResultBatchDecoder();
        var buf = new QwpBatchBuffer(Math.Max(first.Length, refOnly.Length));
        buf.CopyFromPayload(first);
        decoder.Decode(buf);

        decoder.ApplyCacheReset(QwpEgressMsgKind.RESET_MASK_SCHEMAS);

        // Post-reset: the schema-ref message should no longer resolve.
        buf.CopyFromPayload(refOnly);
        Assert.That(() => decoder.Decode(buf),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("not registered"));
    }

    [Test]
    public void RejectsUnknownSchemaMode()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(0)
            .WithSchemaMode((byte)0xEE)
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("unknown schema mode"));
    }

    [Test]
    public void RejectsNonMonotonicStringOffsets()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(2)
            .AddNonNullableColumn("v", QwpConstants.TYPE_VARCHAR, w =>
            {
                w.WriteIntLE(0);
                w.WriteIntLE(5);
                w.WriteIntLE(3);  // goes backwards — must reject.
                w.WriteUtf8("abcde");
            })
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("offset"));
    }

    [Test]
    public void DecodesSymbolColumn_LocalDictAndPerRowIds()
    {
        // Two unique symbols ("AAPL", "MSFT"), three rows assigning ids 0, 1, 0.
        // Wire layout: dict_size_varint(2) | per-entry(varint_len + utf8) | per-row varint(id).
        var bytes = new FrameBuilder()
            .WithRowCount(3)
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w =>
            {
                w.WriteVarint(2);                  // dict_size
                w.WriteVarint(4); w.WriteUtf8("AAPL");
                w.WriteVarint(4); w.WriteUtf8("MSFT");
                w.WriteVarint(0); w.WriteVarint(1); w.WriteVarint(0);
            })
            .Build();

        var batch = Decode(bytes);
        var layout = batch.GetLayout(0);
        Assert.That(layout.SymbolDictSize, Is.EqualTo(2));
        Assert.That(layout.SymbolRowIds, Is.Not.Null);
        Assert.That(layout.SymbolRowIds![0], Is.EqualTo(0));
        Assert.That(layout.SymbolRowIds[1], Is.EqualTo(1));
        Assert.That(layout.SymbolRowIds[2], Is.EqualTo(0));
        Assert.That(layout.OwnedEntries, Is.Not.Null);
        // First packed entry: low 32 = offset (dict-heap-relative), high 32 = length.
        var packed = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(
            layout.OwnedEntries.AsSpan(0, 8));
        var off0 = (int)(uint)packed;
        var len0 = (int)(packed >> 32);
        Assert.That(len0, Is.EqualTo(4));
        Assert.That(off0, Is.GreaterThanOrEqualTo(0)); // varint(4)=1 byte → offset = 1
    }

    [Test]
    public void DecodesSymbolColumn_RejectsOutOfRangeId()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w =>
            {
                w.WriteVarint(1);
                w.WriteVarint(1); w.WriteUtf8("X");
                w.WriteVarint(99);  // id 99 — out of range vs dict_size=1
            })
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("symbol index out of range"));
    }

    // ---- §3.2c — FLAG_DELTA_SYMBOL_DICT + connection-scoped dict ----

    [Test]
    public void DecodesDeltaSymbolColumn_FirstBatchPopulatesConnDict()
    {
        // Delta section adds two entries from index 0; SYMBOL column then references
        // ids 0/1 against the connection dict (no per-column dict on the wire).
        var bytes = new FrameBuilder()
            .WithRowCount(2)
            .WithDeltaSymbolDict(deltaStart: 0, entries: new[] { "AAPL", "MSFT" })
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w =>
            {
                // Delta-mode SYMBOL column has no per-column dict header — just
                // per-row varint(id).
                w.WriteVarint(0); w.WriteVarint(1);
            })
            .Build();

        var decoder = new QwpResultBatchDecoder();
        var buf = new QwpBatchBuffer(bytes.Length);
        buf.CopyFromPayload(bytes);
        decoder.Decode(buf);

        var layout = buf.Batch.GetLayout(0);
        Assert.That(layout.SymbolDictSize, Is.EqualTo(2));
        Assert.That(layout.SymbolHeapBuffer, Is.Not.Null,
            "delta-mode column aliases the decoder's connection dict heap");
        Assert.That(layout.SymbolEntriesBuffer, Is.Not.Null);
        Assert.That(layout.SymbolRowIds![0], Is.EqualTo(0));
        Assert.That(layout.SymbolRowIds[1], Is.EqualTo(1));
    }

    [Test]
    public void DecodesDeltaSymbolColumn_SecondBatchAppendsToConnDict()
    {
        // Reuse the same decoder across two batches: first adds {AAPL, MSFT}, second
        // appends {GOOG} via deltaStart=2. SYMBOL column on second batch references id 2.
        var first = new FrameBuilder()
            .WithRowCount(1)
            .WithDeltaSymbolDict(deltaStart: 0, entries: new[] { "AAPL", "MSFT" })
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w => w.WriteVarint(0))
            .Build();
        var second = new FrameBuilder()
            .WithRowCount(1)
            .WithDeltaSymbolDict(deltaStart: 2, entries: new[] { "GOOG" })
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w => w.WriteVarint(2))
            .Build();

        var decoder = new QwpResultBatchDecoder();
        var buf = new QwpBatchBuffer(Math.Max(first.Length, second.Length));
        buf.CopyFromPayload(first);
        decoder.Decode(buf);
        buf.CopyFromPayload(second);
        decoder.Decode(buf);

        Assert.That(buf.Batch.GetLayout(0).SymbolDictSize, Is.EqualTo(3));
        Assert.That(buf.Batch.GetLayout(0).SymbolRowIds![0], Is.EqualTo(2));
    }

    [Test]
    public void DecodesDeltaSymbolColumn_RejectsOutOfSyncDeltaStart()
    {
        // Fresh decoder has _connDictSize=0, but message claims deltaStart=5.
        var bytes = new FrameBuilder()
            .WithRowCount(0)
            .WithDeltaSymbolDict(deltaStart: 5, entries: new[] { "X" })
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("out of sync"));
    }

    [Test]
    public void ApplyCacheReset_WipesConnDict()
    {
        var first = new FrameBuilder()
            .WithRowCount(1)
            .WithDeltaSymbolDict(deltaStart: 0, entries: new[] { "AAPL" })
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w => w.WriteVarint(0))
            .Build();
        var afterReset = new FrameBuilder()
            .WithRowCount(1)
            .WithDeltaSymbolDict(deltaStart: 0, entries: new[] { "MSFT" })
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w => w.WriteVarint(0))
            .Build();

        var decoder = new QwpResultBatchDecoder();
        var buf = new QwpBatchBuffer(Math.Max(first.Length, afterReset.Length));
        buf.CopyFromPayload(first);
        decoder.Decode(buf);

        // Server emits CACHE_RESET → decoder clears the connection dict; next batch's
        // deltaStart=0 is now in sync.
        decoder.ApplyCacheReset(QwpEgressMsgKind.RESET_MASK_DICT);

        buf.CopyFromPayload(afterReset);
        decoder.Decode(buf);
        Assert.That(buf.Batch.GetLayout(0).SymbolDictSize, Is.EqualTo(1),
            "post-reset dict has only the new entry");
    }

    [Test]
    public void DecodesSymbolColumn_RejectsDictSizeAboveRowCount()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(2)
            .AddNonNullableColumn("sym", QwpConstants.TYPE_SYMBOL, w =>
            {
                w.WriteVarint(99);  // dict claims 99 entries but rowCount=2
            })
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("SYMBOL dict size out of range"));
    }

    // ---- §3.2f — DOUBLE_ARRAY / LONG_ARRAY column decode ----

    [Test]
    public void DecodesDoubleArrayColumn_1D()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(2)
            .AddNonNullableColumn("a", QwpConstants.TYPE_DOUBLE_ARRAY, w =>
            {
                // Row 0: 3-element 1D array.
                w.WriteByte(1); w.WriteIntLE(3);
                w.WriteLongLE(BitConverter.DoubleToInt64Bits(1.0));
                w.WriteLongLE(BitConverter.DoubleToInt64Bits(2.0));
                w.WriteLongLE(BitConverter.DoubleToInt64Bits(3.0));
                // Row 1: 2-element 1D array.
                w.WriteByte(1); w.WriteIntLE(2);
                w.WriteLongLE(BitConverter.DoubleToInt64Bits(10.0));
                w.WriteLongLE(BitConverter.DoubleToInt64Bits(20.0));
            })
            .Build();
        var batch = Decode(bytes);
        var layout = batch.GetLayout(0);
        Assert.That(layout.ArrayRowOffsets, Is.Not.Null);
        Assert.That(layout.ArrayRowLengths, Is.Not.Null);
        Assert.That(layout.ArrayRowLengths![0], Is.EqualTo(1 + 4 + 24)); // header + 3*8
        Assert.That(layout.ArrayRowLengths[1], Is.EqualTo(1 + 4 + 16));  // header + 2*8
    }

    [Test]
    public void DecodesLongArrayColumn_2D()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("a", QwpConstants.TYPE_LONG_ARRAY, w =>
            {
                w.WriteByte(2); w.WriteIntLE(2); w.WriteIntLE(3); // 2x3 array
                for (var v = 1L; v <= 6L; v++) w.WriteLongLE(v);
            })
            .Build();
        var batch = Decode(bytes);
        var layout = batch.GetLayout(0);
        Assert.That(layout.ArrayRowLengths![0], Is.EqualTo(1 + 8 + 48)); // 1 + 4*2 + 8*6
    }

    [Test]
    public void DecodesArrayColumn_RejectsZeroDimSize()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("a", QwpConstants.TYPE_DOUBLE_ARRAY, w =>
            {
                w.WriteByte(1); w.WriteIntLE(0); // dim 0 has size 0
            })
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("must be >= 1"));
    }

    [Test]
    public void DecodesArrayColumn_RejectsRankAboveLimit()
    {
        var bytes = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("a", QwpConstants.TYPE_DOUBLE_ARRAY, w =>
            {
                w.WriteByte(99); // rank > ARRAY_MAX_DIMENSIONS (32)
            })
            .Build();
        Assert.That(() => Decode(bytes),
            Throws.TypeOf<QwpDecodeException>().With.Message.Contains("invalid array dimensions"));
    }

    [Test]
    public void DecoderResetsBatchAcrossSuccessiveDecodes()
    {
        // Decode a 2-column batch, then a 1-column batch with the same buffer; the
        // second decode must not leak the first's column count.
        var first = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("a", QwpConstants.TYPE_INT, w => w.WriteIntLE(1))
            .AddNonNullableColumn("b", QwpConstants.TYPE_INT, w => w.WriteIntLE(2))
            .Build();
        var second = new FrameBuilder()
            .WithRowCount(1)
            .AddNonNullableColumn("only", QwpConstants.TYPE_INT, w => w.WriteIntLE(99))
            .Build();

        var buf = new QwpBatchBuffer(64);
        var dec = new QwpResultBatchDecoder();
        buf.CopyFromPayload(first);
        dec.Decode(buf);
        Assert.That(buf.Batch.ColumnCount, Is.EqualTo(2));

        buf.CopyFromPayload(second);
        dec.Decode(buf);
        Assert.That(buf.Batch.ColumnCount, Is.EqualTo(1));
        Assert.That(buf.Batch.GetIntValue(0, 0), Is.EqualTo(99));
    }

    private static QwpColumnBatch Decode(byte[] bytes)
    {
        var buf = new QwpBatchBuffer(bytes.Length);
        buf.CopyFromPayload(bytes);
        new QwpResultBatchDecoder().Decode(buf);
        return buf.Batch;
    }

    private sealed class FrameBuilder
    {
        private readonly List<Action<PayloadWriter>> _columnEmitters = new();
        private readonly List<(string Name, byte WireType, byte? Scale, int? Precision)> _columnDefs = new();
        private int _rowCount;
        private byte _schemaMode = QwpConstants.SCHEMA_MODE_FULL;

        public FrameBuilder WithRowCount(int rowCount) { _rowCount = rowCount; return this; }
        public FrameBuilder WithSchemaMode(byte mode) { _schemaMode = mode; return this; }

        // §3.2c — when set, the build emits FLAG_DELTA_SYMBOL_DICT in the header and
        // injects the delta section (deltaStart / deltaCount / per-entry-bytes) right
        // after the request_id + batch_seq prelude. Caller passes the deltaStart and
        // the new entries to append.
        private long _deltaStart = -1;
        private List<string>? _deltaEntries;
        public FrameBuilder WithDeltaSymbolDict(long deltaStart, IEnumerable<string> entries)
        {
            _deltaStart = deltaStart;
            _deltaEntries = entries.ToList();
            return this;
        }

        // §3.2b — when set, the build emits FLAG_GORILLA. TIMESTAMP columns then
        // prefix their per-column data with a 1-byte encoding discriminator
        // (0x00 raw / 0x01 Gorilla DoD).
        private bool _gorillaFlag;
        public FrameBuilder WithFlagGorilla() { _gorillaFlag = true; return this; }

        public FrameBuilder AddNonNullableColumn(string name, byte wireType, Action<PayloadWriter> valueWriter)
        {
            _columnDefs.Add((name, wireType, null, null));
            _columnEmitters.Add(w =>
            {
                w.WriteByte(0); // null flag = 0 (no nulls)
                valueWriter(w);
            });
            return this;
        }

        public FrameBuilder AddNullableColumn(string name, byte wireType, byte[] bitmap, Action<PayloadWriter> valueWriter)
        {
            _columnDefs.Add((name, wireType, null, null));
            _columnEmitters.Add(w =>
            {
                w.WriteByte(1); // null flag = 1
                w.WriteRaw(bitmap);
                valueWriter(w);
            });
            return this;
        }

        public FrameBuilder AddDecimalColumn(string name, byte wireType, byte scale, Action<PayloadWriter> valueWriter)
        {
            _columnDefs.Add((name, wireType, scale, null));
            _columnEmitters.Add(w =>
            {
                w.WriteByte(0);    // null flag
                w.WriteByte(scale); // scale prefix
                valueWriter(w);
            });
            return this;
        }

        public FrameBuilder AddGeohashColumn(string name, int precisionBits, Action<PayloadWriter> valueWriter)
        {
            _columnDefs.Add((name, QwpConstants.TYPE_GEOHASH, null, precisionBits));
            _columnEmitters.Add(w =>
            {
                w.WriteByte(0);
                w.WriteVarint(precisionBits);
                valueWriter(w);
            });
            return this;
        }

        public byte[] Build()
        {
            var w = new PayloadWriter();
            // Header (12B): magic(4) + version(1) + flags(1) + 6 zero pad bytes.
            w.WriteIntLE(QwpConstants.MAGIC_MESSAGE);
            w.WriteByte(QwpConstants.VERSION_2);
            var flags = (byte)(
                (_deltaEntries is not null ? QwpConstants.FLAG_DELTA_SYMBOL_DICT : 0)
                | (_gorillaFlag ? QwpConstants.FLAG_GORILLA : 0));
            w.WriteByte(flags);
            for (var i = 0; i < QwpConstants.HEADER_SIZE - 6; i++) w.WriteByte(0);

            // Body.
            w.WriteByte(QwpEgressMsgKind.RESULT_BATCH);
            w.WriteLongLE(0L);   // request_id
            w.WriteVarint(0L);    // batch_seq

            // §3.2c — delta section between batch_seq and the table block.
            if (_deltaEntries is not null)
            {
                w.WriteVarint(_deltaStart);
                w.WriteVarint(_deltaEntries.Count);
                foreach (var e in _deltaEntries)
                {
                    var bytes = Encoding.UTF8.GetBytes(e);
                    w.WriteVarint(bytes.Length);
                    w.WriteRaw(bytes);
                }
            }

            w.WriteVarint(0L);    // table name length (empty)
            w.WriteVarint(_rowCount);
            w.WriteVarint(_columnDefs.Count);
            w.WriteByte(_schemaMode);
            w.WriteVarint(0);     // schema_id

            if (_schemaMode == QwpConstants.SCHEMA_MODE_FULL)
            {
                foreach (var (name, wireType, _, _) in _columnDefs)
                {
                    var nameBytes = Encoding.UTF8.GetBytes(name);
                    w.WriteVarint(nameBytes.Length);
                    w.WriteRaw(nameBytes);
                    w.WriteByte(wireType);
                }
            }

            // Per-column wire bytes.
            foreach (var emit in _columnEmitters)
            {
                emit(w);
            }
            return w.ToArray();
        }
    }

    private sealed class PayloadWriter
    {
        private readonly List<byte> _bytes = new();

        public void WriteByte(byte v) => _bytes.Add(v);
        public void WriteByte(int v) => _bytes.Add((byte)v);
        public void WriteRaw(byte[] data) => _bytes.AddRange(data);
        public void WriteUtf8(string s) => _bytes.AddRange(Encoding.UTF8.GetBytes(s));

        public void WriteIntLE(int v)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(b, v);
            for (var i = 0; i < 4; i++) _bytes.Add(b[i]);
        }

        public void WriteLongLE(long v)
        {
            Span<byte> b = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(b, v);
            for (var i = 0; i < 8; i++) _bytes.Add(b[i]);
        }

        public void WriteVarint(long value)
        {
            var v = (ulong)value;
            while (v > 0x7F)
            {
                _bytes.Add((byte)((v & 0x7F) | 0x80));
                v >>= 7;
            }
            _bytes.Add((byte)v);
        }

        public byte[] ToArray() => _bytes.ToArray();
    }
}
