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
using System.Text;
using NUnit.Framework;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Query;

namespace net_questdb_client_tests.Qwp.Query;

[TestFixture]
public class QwpResultBatchDecoderTests
{
    [Test]
    public void Decode_LongColumnFullSchema_RoundTrips()
    {
        var schema = new ResultSchema
        {
            SchemaId = 7,
            Columns = { new SchemaColumn("id", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 3,
            Columns = { new FixedColumnData { DenseBytes = LongsLe(1L, 2L, 3L) } },
        };

        var (state, batch, headerFlags, payload) = DecodeOneBatch(schema, data);

        Assert.That(batch.RowCount, Is.EqualTo(3));
        Assert.That(batch.ColumnCount, Is.EqualTo(1));
        Assert.That(batch.GetColumnName(0), Is.EqualTo("id"));
        Assert.That(batch.GetColumnWireType(0), Is.EqualTo(QwpTypeCode.Long));
        Assert.That(batch.GetLongValue(0, 0), Is.EqualTo(1L));
        Assert.That(batch.GetLongValue(0, 1), Is.EqualTo(2L));
        Assert.That(batch.GetLongValue(0, 2), Is.EqualTo(3L));
        Assert.That(batch.IsNull(0, 0), Is.False);
    }

    [Test]
    public void Decode_NullsViaBitmap_AreVisibleAndDenseValuesShifted()
    {
        var schema = new ResultSchema
        {
            SchemaId = 1,
            Columns = { new SchemaColumn("v", QwpTypeCode.Int) },
        };
        var data = new ResultBatchData
        {
            RowCount = 4,
            // bits LSB-first per byte: row 0 null, row 1 set, row 2 null, row 3 set → bitmap byte = 0b0000_0101
            Columns = { new FixedColumnData { NullBitmap = new byte[] { 0b0000_0101 }, DenseBytes = IntsLe(20, 40) } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.IsNull(0, 0), Is.True);
        Assert.That(batch.IsNull(0, 1), Is.False);
        Assert.That(batch.IsNull(0, 2), Is.True);
        Assert.That(batch.IsNull(0, 3), Is.False);
        Assert.That(batch.GetIntValue(0, 1), Is.EqualTo(20));
        Assert.That(batch.GetIntValue(0, 3), Is.EqualTo(40));
        Assert.That(batch.GetIntValue(0, 0), Is.EqualTo(0));
    }

    [Test]
    public void Decode_VarcharColumn_RoundTrips()
    {
        var schema = new ResultSchema
        {
            SchemaId = 2,
            Columns = { new SchemaColumn("s", QwpTypeCode.Varchar) },
        };
        var data = new ResultBatchData
        {
            RowCount = 3,
            Columns = { new VarcharColumnData { DenseValues = new[] { "a", "bb", "ccc" } } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.GetString(0, 0), Is.EqualTo("a"));
        Assert.That(batch.GetString(0, 1), Is.EqualTo("bb"));
        Assert.That(batch.GetString(0, 2), Is.EqualTo("ccc"));
        var span = batch.GetStringSpan(0, 1);
        Assert.That(Encoding.UTF8.GetString(span), Is.EqualTo("bb"));
    }

    [Test]
    public void Decode_SymbolWithDeltaDict_BuildsConnDictThenResolves()
    {
        var schema = new ResultSchema
        {
            SchemaId = 3,
            Columns = { new SchemaColumn("sym", QwpTypeCode.Symbol) },
        };
        var data = new ResultBatchData
        {
            RowCount = 4,
            Columns = { new SymbolColumnData { DenseDictIds = new[] { 0, 1, 0, 2 } } },
        };
        var dict = new DeltaSymbolDict { DeltaStart = 0, Entries = { "alpha", "beta", "gamma" } };

        var (state, batch, _, _) = DecodeOneBatch(schema, data, dict);

        Assert.That(batch.GetSymbolDictSize(0), Is.EqualTo(3));
        Assert.That(batch.GetSymbol(0, 0), Is.EqualTo("alpha"));
        Assert.That(batch.GetSymbol(0, 1), Is.EqualTo("beta"));
        Assert.That(batch.GetSymbol(0, 2), Is.EqualTo("alpha"));
        Assert.That(batch.GetSymbol(0, 3), Is.EqualTo("gamma"));
        Assert.That(batch.GetSymbolId(0, 1), Is.EqualTo(1));
    }

    [Test]
    public void Decode_TimestampGorilla_RoundTrips()
    {
        var schema = new ResultSchema
        {
            SchemaId = 4,
            Columns = { new SchemaColumn("ts", QwpTypeCode.Timestamp) },
        };
        var values = new[] { 1_700_000_000_000_000L, 1_700_000_000_000_100L, 1_700_000_000_000_200L, 1_700_000_000_000_300L };
        var data = new ResultBatchData
        {
            RowCount = values.Length,
            Columns = { new TimestampColumnData { DenseValues = values } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        for (var i = 0; i < values.Length; i++)
        {
            Assert.That(batch.GetTimestampValue(0, i), Is.EqualTo(values[i]));
        }
    }

    [Test]
    public void Decode_ReferenceMode_ReusesPriorSchema()
    {
        var state = new QwpEgressConnState();
        var decoder = new QwpResultBatchDecoder(state);

        var schemaFull = new ResultSchema
        {
            Mode = QwpConstants.SchemaModeFull,
            SchemaId = 11,
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var dataA = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongsLe(7L) } } };
        var frameA = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schemaFull, dataA);

        var batchA = new QwpColumnBatch();
        decoder.Decode(PayloadOf(frameA).Span, HeaderFlagsOf(frameA), batchA);
        Assert.That(batchA.GetLongValue(0, 0), Is.EqualTo(7L));

        var schemaRef = new ResultSchema
        {
            Mode = QwpConstants.SchemaModeReference,
            SchemaId = 11,
            Columns = schemaFull.Columns,
        };
        var dataB = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongsLe(42L) } } };
        var frameB = QwpEgressFrameBuilder.BuildResultBatch(1L, 1L, schemaRef, dataB);

        var batchB = new QwpColumnBatch();
        decoder.Decode(PayloadOf(frameB).Span, HeaderFlagsOf(frameB), batchB);
        Assert.That(batchB.GetLongValue(0, 0), Is.EqualTo(42L));
        Assert.That(batchB.GetColumnName(0), Is.EqualTo("c"));
    }

    [Test]
    public void Decode_UnknownSchemaIdInReferenceMode_Throws()
    {
        var state = new QwpEgressConnState();
        var decoder = new QwpResultBatchDecoder(state);

        var schemaRef = new ResultSchema
        {
            Mode = QwpConstants.SchemaModeReference,
            SchemaId = 999,
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData { RowCount = 1, Columns = { new FixedColumnData { DenseBytes = LongsLe(1L) } } };
        var frame = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schemaRef, data);

        Assert.Throws<QwpDecodeException>(() => decoder.Decode(PayloadOf(frame).Span, HeaderFlagsOf(frame), new QwpColumnBatch()));
    }

    [Test]
    public void Decode_TruncatedPayload_Throws()
    {
        var schema = new ResultSchema
        {
            SchemaId = 5,
            Columns = { new SchemaColumn("c", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData { RowCount = 2, Columns = { new FixedColumnData { DenseBytes = LongsLe(1L, 2L) } } };
        var frame = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data);

        var truncatedPayload = PayloadOf(frame);
        var shortPayloadBytes = truncatedPayload.Slice(0, truncatedPayload.Length - 4).ToArray();

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        Assert.Throws<QwpDecodeException>(() => decoder.Decode(shortPayloadBytes, HeaderFlagsOf(frame), new QwpColumnBatch()));
    }

    [Test]
    public void Decode_DecimalColumn_CarriesScalePrefix()
    {
        var schema = new ResultSchema
        {
            SchemaId = 6,
            Columns = { new SchemaColumn("d", QwpTypeCode.Decimal64) },
        };
        var dense = LongsLe(12345L);
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new DecimalColumnData { Scale = 2, DenseBytes = dense } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.GetDecimalScale(0), Is.EqualTo((byte)2));
        Assert.That(batch.GetLongValue(0, 0), Is.EqualTo(12345L));
    }

    [Test]
    public void Decode_DoubleArrayColumn_RoundTripsRowMajor()
    {
        var schema = new ResultSchema
        {
            SchemaId = 9,
            Columns = { new SchemaColumn("a", QwpTypeCode.DoubleArray) },
        };
        var data = new ResultBatchData
        {
            RowCount = 2,
            Columns =
            {
                new DoubleArrayColumnData
                {
                    DenseArrays = new[]
                    {
                        (new[] { 3 }, new[] { 1.0, 2.0, 3.0 }),
                        (new[] { 2, 2 }, new[] { 10.0, 20.0, 30.0, 40.0 }),
                    },
                },
            },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.GetArrayNDims(0, 0), Is.EqualTo(1));
        Assert.That(batch.GetArrayShape(0, 0), Is.EqualTo(new[] { 3 }));
        Assert.That(batch.GetDoubleArrayElements(0, 0), Is.EqualTo(new[] { 1.0, 2.0, 3.0 }));

        Assert.That(batch.GetArrayNDims(0, 1), Is.EqualTo(2));
        Assert.That(batch.GetArrayShape(0, 1), Is.EqualTo(new[] { 2, 2 }));
        Assert.That(batch.GetDoubleArrayElements(0, 1), Is.EqualTo(new[] { 10.0, 20.0, 30.0, 40.0 }));
    }

    [Test]
    public void Decode_LongArrayColumn_RoundTripsRowMajor()
    {
        var schema = new ResultSchema
        {
            SchemaId = 10,
            Columns = { new SchemaColumn("la", QwpTypeCode.LongArray) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns =
            {
                new LongArrayColumnData
                {
                    DenseArrays = new[] { (new[] { 4 }, new[] { 1L, 2L, 3L, 4L }) },
                },
            },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);
        Assert.That(batch.GetLongArrayElements(0, 0), Is.EqualTo(new[] { 1L, 2L, 3L, 4L }));
    }

    [Test]
    public void Decode_GeohashColumn_RoundTripsPrecision()
    {
        var schema = new ResultSchema
        {
            SchemaId = 8,
            Columns = { new SchemaColumn("g", QwpTypeCode.Geohash) },
        };
        var dense = new byte[] { 0xAB, 0xCD, 0xEF }; // 24 bits = 3 bytes per row
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new GeohashColumnData { PrecisionBits = 24, DenseBytes = dense } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.GetGeohashPrecisionBits(0), Is.EqualTo(24));
    }

    private static (QwpEgressConnState State, QwpColumnBatch Batch, byte HeaderFlags, ReadOnlyMemory<byte> Payload)
        DecodeOneBatch(ResultSchema schema, ResultBatchData data, DeltaSymbolDict? dict = null)
    {
        var frame = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data, dict);
        var headerFlags = HeaderFlagsOf(frame);
        var payload = PayloadOf(frame);
        var state = new QwpEgressConnState();
        var decoder = new QwpResultBatchDecoder(state);
        var batch = new QwpColumnBatch();
        decoder.Decode(payload.Span, headerFlags, batch);
        return (state, batch, headerFlags, payload);
    }

    [Test]
    public void Decode_TwoBatchesIntoSameTarget_NoStaleResidueFromFirst()
    {
        // Regression: scratch pooling must not leak prior-batch data into the next read.
        var state = new QwpEgressConnState();
        var decoder = new QwpResultBatchDecoder(state);
        var target = new QwpColumnBatch();

        var schema = new ResultSchema
        {
            SchemaId = 41,
            Columns =
            {
                new SchemaColumn("id", QwpTypeCode.Long),
                new SchemaColumn("label", QwpTypeCode.Varchar),
            },
        };

        var idsA = new long[100];
        var labelsA = new string[100];
        for (var i = 0; i < 100; i++)
        {
            idsA[i] = 1000 + i;
            labelsA[i] = "rA-" + i;
        }
        var dataA = new ResultBatchData
        {
            RowCount = 100,
            Columns =
            {
                new FixedColumnData { DenseBytes = LongsLe(idsA) },
                new VarcharColumnData { DenseValues = labelsA },
            },
        };
        var frameA = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, dataA);
        decoder.Decode(PayloadOf(frameA).Span, HeaderFlagsOf(frameA), target);

        Assert.That(target.RowCount, Is.EqualTo(100));
        Assert.That(target.GetLongValue(0, 0), Is.EqualTo(1000L));
        Assert.That(target.GetLongValue(0, 99), Is.EqualTo(1099L));

        // Batch B: shorter (50 rows) + mid-batch nulls — exercises pooled NonNullIndex / shorter row range.
        var idsB = new List<long>();
        var labelsB = new List<string>();
        var nullBitmap = new byte[(50 + 7) >> 3];
        for (var i = 0; i < 50; i++)
        {
            if (i % 2 == 0)
            {
                nullBitmap[i >> 3] |= (byte)(1 << (i & 7));
                labelsB.Add(""); // doesn't appear; null filtered
            }
            else
            {
                idsB.Add(2000 + i);
                labelsB.Add("rB-" + i);
            }
        }
        var dataB = new ResultBatchData
        {
            RowCount = 50,
            Columns =
            {
                new FixedColumnData { DenseBytes = LongsLe(idsB.ToArray()), NullBitmap = nullBitmap },
                new VarcharColumnData
                {
                    NullBitmap = nullBitmap,
                    DenseValues = Enumerable.Range(0, 50)
                        .Where(i => i % 2 != 0)
                        .Select(i => "rB-" + i)
                        .ToArray(),
                },
            },
        };
        var frameB = QwpEgressFrameBuilder.BuildResultBatch(
            1L, 1L,
            new ResultSchema
            {
                Mode = QwpConstants.SchemaModeReference,
                SchemaId = 41,
                Columns = schema.Columns,
            },
            dataB);
        decoder.Decode(PayloadOf(frameB).Span, HeaderFlagsOf(frameB), target);

        Assert.That(target.RowCount, Is.EqualTo(50));
        for (var i = 0; i < 50; i++)
        {
            if (i % 2 == 0)
            {
                Assert.That(target.IsNull(0, i), Is.True, $"row {i} col 0 should be null");
            }
            else
            {
                Assert.That(target.IsNull(0, i), Is.False, $"row {i} col 0 should not be null");
                Assert.That(target.GetLongValue(0, i), Is.EqualTo(2000L + i),
                    $"row {i} col 0 leaked stale data from batch A");
            }
        }

        for (var i = 0; i < 50; i++)
        {
            if (i % 2 != 0)
            {
                var got = Encoding.UTF8.GetString(target.GetStringSpan(1, i));
                Assert.That(got, Is.EqualTo("rB-" + i),
                    $"row {i} col 1 leaked stale label from batch A");
            }
        }
    }

    [Test]
    public void Decode_IPv4Column_DecodesAsInt()
    {
        var schema = new ResultSchema
        {
            SchemaId = 11,
            Columns = { new SchemaColumn("ip", QwpTypeCode.IPv4) },
        };
        var data = new ResultBatchData
        {
            RowCount = 2,
            Columns = { new FixedColumnData { DenseBytes = IntsLe(unchecked((int)0xC0A80101), 0x7F000001) } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.RowCount, Is.EqualTo(2));
        Assert.That(batch.GetColumnWireType(0), Is.EqualTo(QwpTypeCode.IPv4));
        Assert.That(batch.GetIPv4Value(0, 0), Is.EqualTo(unchecked((int)0xC0A80101)));
        Assert.That(batch.GetIPv4Value(0, 1), Is.EqualTo(0x7F000001));
        Assert.That(batch.GetString(0, 0), Is.EqualTo("192.168.1.1"));
        Assert.That(batch.GetString(0, 1), Is.EqualTo("127.0.0.1"));
    }

    [Test]
    public void Decode_BinaryColumn_RoundTrips()
    {
        var schema = new ResultSchema
        {
            SchemaId = 12,
            Columns = { new SchemaColumn("blob", QwpTypeCode.Binary) },
        };
        var values = new[]
        {
            new byte[] { 0x00, 0x01, 0x02 },
            Array.Empty<byte>(),
            new byte[] { 0xDE, 0xAD, 0xBE, 0xEF },
        };
        var data = new ResultBatchData
        {
            RowCount = values.Length,
            Columns = { new BinaryColumnData { DenseValues = values } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.GetColumnWireType(0), Is.EqualTo(QwpTypeCode.Binary));
        Assert.That(batch.GetBinarySpan(0, 0).ToArray(), Is.EqualTo(values[0]));
        Assert.That(batch.GetBinarySpan(0, 1).ToArray(), Is.EqualTo(values[1]));
        Assert.That(batch.GetBinarySpan(0, 2).ToArray(), Is.EqualTo(values[2]));
        Assert.That(batch.GetString(0, 2), Is.EqualTo("DEADBEEF"));
    }

    [Test]
    public void Decode_UuidColumn_RoundTripsWithGetUuid()
    {
        var lo = 0x0123_4567_89AB_CDEFL;
        var hi = unchecked((long)0xFEDC_BA98_7654_3210UL);
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0, 8), lo);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8, 8), hi);

        var schema = new ResultSchema
        {
            SchemaId = 21,
            Columns = { new SchemaColumn("u", QwpTypeCode.Uuid) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new FixedColumnData { DenseBytes = bytes } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.That(batch.GetColumnWireType(0), Is.EqualTo(QwpTypeCode.Uuid));
        Assert.That(batch.GetUuidLo(0, 0), Is.EqualTo(lo));
        Assert.That(batch.GetUuidHi(0, 0), Is.EqualTo(hi));

        var binds = new QwpBindValues();
        binds.SetUuid(0, batch.GetUuid(0, 0));
        Assert.That(binds.AsMemory().Span[0], Is.EqualTo((byte)QwpTypeCode.Uuid));
        var roundTripped = new byte[16];
        binds.AsMemory().Span.Slice(2, 16).CopyTo(roundTripped);
        Assert.That(roundTripped, Is.EqualTo(bytes));
    }

    [Test]
    public void GetBinarySpan_OnNonBinaryColumn_Throws()
    {
        var schema = new ResultSchema
        {
            SchemaId = 13,
            Columns = { new SchemaColumn("v", QwpTypeCode.Long) },
        };
        var data = new ResultBatchData
        {
            RowCount = 1,
            Columns = { new FixedColumnData { DenseBytes = LongsLe(42L) } },
        };

        var (_, batch, _, _) = DecodeOneBatch(schema, data);

        Assert.Throws<InvalidOperationException>(() => batch.GetBinarySpan(0, 0).ToArray());
    }

    [Test]
    public void Decode_RejectsHugeColumnNameLength()
    {
        // Hand-craft a payload whose column name length varint encodes int.MaxValue.
        var p = new List<byte>
        {
            QwpConstants.MsgKindResultBatch,
        };
        p.AddRange(new byte[8]); // requestId = 0
        p.Add(0x00); // batch_seq = 0
        p.Add(0x00); // empty table name
        p.Add(0x01); // row_count
        p.Add(0x01); // col_count
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x00); // schema_id
        // column name length varint (int.MaxValue)
        WriteVarintTo(p, 0x7FFFFFFFUL);

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() => decoder.Decode(p.ToArray(), 0, new QwpColumnBatch()));
        StringAssert.Contains("column name length out of range", ex!.Message);
    }

    [Test]
    public void Decode_RejectsTooManyArrayDimensions()
    {
        // 200-dim array — exceeds MaxArrayDimensions=32. Use a fully formed but invalid payload.
        var p = new List<byte>
        {
            QwpConstants.MsgKindResultBatch,
        };
        p.AddRange(new byte[8]);
        p.Add(0x00); // batch_seq
        p.Add(0x00); // empty table name
        p.Add(0x01); // row_count = 1
        p.Add(0x01); // col_count = 1
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x00); // schema_id
        p.Add(0x01); // col name len = 1
        p.Add((byte)'a');
        p.Add((byte)QwpTypeCode.DoubleArray);
        p.Add(0x00); // null_flag = 0 (no nulls)
        p.Add(200);  // nDims out of range

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() => decoder.Decode(p.ToArray(), 0, new QwpColumnBatch()));
        StringAssert.Contains("array nDims out of range", ex!.Message);
    }

    [Test]
    public void Decode_RejectsNonZeroVarcharOffset0()
    {
        var p = new List<byte>
        {
            QwpConstants.MsgKindResultBatch,
        };
        p.AddRange(new byte[8]);
        p.Add(0x00); // batch_seq
        p.Add(0x00); // empty table name
        p.Add(0x01); // row_count
        p.Add(0x01); // col_count
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x00); // schema_id
        p.Add(0x01); p.Add((byte)'v');
        p.Add((byte)QwpTypeCode.Varchar);
        p.Add(0x00); // null_flag = 0
        // offsets[0] = 5 (illegal), offsets[1] = 8
        WriteI32(p, 5);
        WriteI32(p, 8);
        for (var i = 0; i < 8; i++) p.Add((byte)i);

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() => decoder.Decode(p.ToArray(), 0, new QwpColumnBatch()));
        StringAssert.Contains("offsets[0] must be 0", ex!.Message);
    }

    [Test]
    public void Decode_RejectsNonMonotonicVarcharOffsets()
    {
        var p = new List<byte>
        {
            QwpConstants.MsgKindResultBatch,
        };
        p.AddRange(new byte[8]);
        p.Add(0x00);
        p.Add(0x00);
        p.Add(0x02); // row_count = 2
        p.Add(0x01);
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x00);
        p.Add(0x01); p.Add((byte)'v');
        p.Add((byte)QwpTypeCode.Varchar);
        p.Add(0x00);
        WriteI32(p, 0);
        WriteI32(p, 5);
        WriteI32(p, 3); // non-monotonic
        for (var i = 0; i < 5; i++) p.Add((byte)i);

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() => decoder.Decode(p.ToArray(), 0, new QwpColumnBatch()));
        StringAssert.Contains("non-monotonic", ex!.Message);
    }

    [Test]
    public void Decode_RejectsArrayShapeOverflowingPayload()
    {
        var p = new List<byte> { QwpConstants.MsgKindResultBatch };
        p.AddRange(new byte[8]);
        p.Add(0x00); // batch_seq
        p.Add(0x00); // empty table name
        p.Add(0x01); // row_count
        p.Add(0x01); // col_count
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x00);
        p.Add(0x01); p.Add((byte)'a');
        p.Add((byte)QwpTypeCode.DoubleArray);
        p.Add(0x00); // null_flag = 0
        p.Add(0x02); // nDims = 2
        WriteI32(p, int.MaxValue);
        WriteI32(p, int.MaxValue);

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() => decoder.Decode(p.ToArray(), 0, new QwpColumnBatch()));
        StringAssert.Contains("array shape exceeds remaining payload", ex!.Message);
    }

    [Test]
    public void Decode_RejectsVarcharHeapLenOverflow()
    {
        var p = new List<byte> { QwpConstants.MsgKindResultBatch };
        p.AddRange(new byte[8]);
        p.Add(0x00);
        p.Add(0x00);
        p.Add(0x01);
        p.Add(0x01);
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x00);
        p.Add(0x01); p.Add((byte)'v');
        p.Add((byte)QwpTypeCode.Varchar);
        p.Add(0x00);
        WriteI32(p, 0);
        WriteI32(p, int.MaxValue);

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() => decoder.Decode(p.ToArray(), 0, new QwpColumnBatch()));
        StringAssert.Contains("truncated varchar heap", ex!.Message);
    }

    [Test]
    public void Decode_RejectsSymbolDictVarintExceedingInt()
    {
        var p = new List<byte> { QwpConstants.MsgKindResultBatch };
        p.AddRange(new byte[8]);
        p.Add(0x00);
        WriteVarintTo(p, ulong.MaxValue);

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() =>
            decoder.Decode(p.ToArray(), QwpConstants.FlagDeltaSymbolDict, new QwpColumnBatch()));
        StringAssert.Contains("varint exceeds int.MaxValue", ex!.Message);
    }

    [Test]
    public void Decode_RejectsBooleanWithNullFlagSet()
    {
        var p = new List<byte> { QwpConstants.MsgKindResultBatch };
        p.AddRange(new byte[8]);
        p.Add(0x00);
        p.Add(0x00);
        p.Add(0x01);
        p.Add(0x01);
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x00);
        p.Add(0x01); p.Add((byte)'b');
        p.Add((byte)QwpTypeCode.Boolean);
        p.Add(0x01); // null_flag = 1 — illegal for BOOLEAN
        p.Add(0x00); // bitmap (unused, decoder must reject before reading)

        var decoder = new QwpResultBatchDecoder(new QwpEgressConnState());
        var ex = Assert.Throws<QwpDecodeException>(() => decoder.Decode(p.ToArray(), 0, new QwpColumnBatch()));
        StringAssert.Contains("cannot carry NULL", ex!.Message);
    }

    [Test]
    public void Decode_FailedBatch_DoesNotPersistSymbolDictAdditions()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("s", QwpTypeCode.Symbol) } };

        var p = new List<byte> { QwpConstants.MsgKindResultBatch };
        p.AddRange(new byte[8]);
        p.Add(0x00);
        WriteVarintTo(p, 0);
        WriteVarintTo(p, 1);
        WriteVarintTo(p, 5);
        for (var i = 0; i < 5; i++) p.Add((byte)('a' + i));

        p.Add(0x00);
        p.Add(0x01);
        p.Add(0x01);
        p.Add(QwpConstants.SchemaModeFull);
        p.Add(0x01);
        p.Add(0x01); p.Add((byte)'s');
        p.Add((byte)QwpTypeCode.Symbol);
        p.Add(0x00);

        var state = new QwpEgressConnState();
        var decoder = new QwpResultBatchDecoder(state);
        Assert.Throws<QwpDecodeException>(() =>
            decoder.Decode(p.ToArray(), QwpConstants.FlagDeltaSymbolDict, new QwpColumnBatch()));
        Assert.That(state.SymbolDict.Size, Is.EqualTo(0));
        Assert.That(state.TryGetSchema(1, out _), Is.False);
    }

    [Test]
    public void Decode_RejectsSymbolDictDeltaStartMismatch()
    {
        var schema = new ResultSchema { SchemaId = 1, Columns = { new SchemaColumn("s", QwpTypeCode.Symbol) } };
        var dict1 = new DeltaSymbolDict { DeltaStart = 0, Entries = { "alpha", "beta" } };
        var data1 = new ResultBatchData { RowCount = 1, Columns = { new SymbolColumnData { DenseDictIds = new[] { 0 } } } };
        var batch1 = QwpEgressFrameBuilder.BuildResultBatch(1L, 0L, schema, data1, dict1);

        var state = new QwpEgressConnState();
        var decoder = new QwpResultBatchDecoder(state);
        decoder.Decode(PayloadOf(batch1).Span, HeaderFlagsOf(batch1), new QwpColumnBatch());
        Assert.That(state.SymbolDict.Size, Is.EqualTo(2));

        // Second batch with deltaStart=0 but cursor is 2 → must throw.
        var dict2 = new DeltaSymbolDict { DeltaStart = 0, Entries = { "gamma" } };
        var data2 = new ResultBatchData { RowCount = 1, Columns = { new SymbolColumnData { DenseDictIds = new[] { 0 } } } };
        var batch2 = QwpEgressFrameBuilder.BuildResultBatch(1L, 1L, schema, data2, dict2);

        Assert.Throws<QwpDecodeException>(() =>
            decoder.Decode(PayloadOf(batch2).Span, HeaderFlagsOf(batch2), new QwpColumnBatch()));
    }

    private static void WriteVarintTo(List<byte> dst, ulong v)
    {
        while (v >= 0x80)
        {
            dst.Add((byte)(v | 0x80));
            v >>= 7;
        }
        dst.Add((byte)v);
    }

    private static void WriteI32(List<byte> dst, int v)
    {
        dst.Add((byte)v);
        dst.Add((byte)(v >> 8));
        dst.Add((byte)(v >> 16));
        dst.Add((byte)(v >> 24));
    }

    private static byte HeaderFlagsOf(byte[] frame) => frame[QwpConstants.OffsetFlags];

    private static ReadOnlyMemory<byte> PayloadOf(byte[] frame)
    {
        var len = (int)BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(QwpConstants.OffsetPayloadLength, 4));
        return frame.AsMemory(QwpConstants.HeaderSize, len);
    }

    private static byte[] LongsLe(params long[] values)
    {
        var bytes = new byte[values.Length * 8];
        for (var i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 8, 8), values[i]);
        }
        return bytes;
    }

    private static byte[] IntsLe(params int[] values)
    {
        var bytes = new byte[values.Length * 4];
        for (var i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4, 4), values[i]);
        }
        return bytes;
    }
}
