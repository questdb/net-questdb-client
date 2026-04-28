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
///     Mirrors the simple-type subset of <c>QwpColumnBatchViewsTest.java</c> on Java
///     main 64b7ee69. The Java suite covers every QWP column type (1632 LoC); PR 9c
///     ships only the simple-type accessors (Long, Double, String, Boolean, Int,
///     Short, Byte, Float, Char, Geohash) — decimals / arrays / symbols / uuid /
///     long256 fill in via PR 9d + PR 11.
/// </summary>
[TestFixture]
public class ColumnViewTests
{
    [Test]
    public void GetLongValueReadsLittleEndianFromValuesOffset()
    {
        // 3 longs at offsets 0/8/16 in the payload.
        var batch = BuildBatchWithLongColumn(new long[] { 0xCAFEL, -1L, long.MinValue });
        var view = new ColumnView();
        view.BindToColumn(batch, columnIndex: 0);
        Assert.That(view.GetLongValue(0), Is.EqualTo(0xCAFEL));
        Assert.That(view.GetLongValue(1), Is.EqualTo(-1L));
        Assert.That(view.GetLongValue(2), Is.EqualTo(long.MinValue));
    }

    [Test]
    public void GetDoubleValueReadsLittleEndianFromValuesOffset()
    {
        var batch = BuildBatchWithDoubleColumn(new[] { 1.5, double.NaN, -3.14 });
        var view = new ColumnView();
        view.BindToColumn(batch, 0);
        Assert.That(view.GetDoubleValue(0), Is.EqualTo(1.5));
        Assert.That(double.IsNaN(view.GetDoubleValue(1)), Is.True);
        Assert.That(view.GetDoubleValue(2), Is.EqualTo(-3.14).Within(1e-12));
    }

    [Test]
    public void GetStringReadsCumulativeOffsetsAndUtf8()
    {
        var batch = BuildBatchWithVarcharColumn(new[] { "hello", "", "world" });
        var view = new ColumnView();
        view.BindToColumn(batch, 0);
        Assert.That(view.GetString(0), Is.EqualTo("hello"));
        Assert.That(view.GetString(1), Is.EqualTo(string.Empty));
        Assert.That(view.GetString(2), Is.EqualTo("world"));
    }

    [Test]
    public void IsNullReadsLsbFirstFromBitmap()
    {
        // 4 rows, with rows 1 and 3 marked null in the bitmap.
        // Bitmap byte: 0b0000_1010 = 0x0A (rows 1 and 3 set).
        var batch = BuildBatchWithNullableLongColumn(
            values: new[] { 100L, 0L, 200L, 0L },
            nullBitmap: new byte[] { 0b0000_1010 },
            denseIndices: new[] { 0, -1, 1, -1 });
        var view = new ColumnView();
        view.BindToColumn(batch, 0);
        Assert.That(view.IsNull(0), Is.False);
        Assert.That(view.IsNull(1), Is.True);
        Assert.That(view.IsNull(2), Is.False);
        Assert.That(view.IsNull(3), Is.True);
        // The non-null rows should still produce the correct dense values.
        Assert.That(view.GetLongValue(0), Is.EqualTo(100L));
        Assert.That(view.GetLongValue(2), Is.EqualTo(200L));
    }

    // ---- §3.1 — accessors for the long-tail wire types (decimal128/256, long256,
    // symbols, uuid, arrays). End-to-end coverage lives on QwpResultBatchDecoderTests
    // (decoder populates the layout state ColumnView reads); this test verifies the
    // four ones with simple-enough fixtures to hand-build a layout.

    [Test]
    public void GetUuidHiLoReadsBothLimbs()
    {
        var lo = unchecked((long)0xDEADBEEF_CAFEBABEUL);
        var hi = unchecked((long)0xFEEDFACE_DEADBEEFUL);
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0, 8), lo);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8, 8), hi);

        var buf = new QwpBatchBuffer(bytes.Length);
        buf.CopyFromPayload(bytes);
        var batch = new QwpColumnBatch();
        batch.Reset(buf, 1);
        batch.Layouts.Add(new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_UUID },
            ValuesOffset = 0,
            NullBitmapOffset = -1,
            NonNullCount = 1,
        });

        var view = new ColumnView();
        view.BindToColumn(batch, 0);
        Assert.That(view.GetUuidLo(0), Is.EqualTo(lo));
        Assert.That(view.GetUuidHi(0), Is.EqualTo(hi));
    }

    [Test]
    public void GetDecimal128HighLowReadsBothLimbs()
    {
        // Storage order: hi at offset 0, lo at offset 8 (matches AddDecimal128).
        var hi = 0x0123_4567_89AB_CDEFL;
        var lo = unchecked((long)0xFEDC_BA98_7654_3210UL);
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0, 8), hi);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8, 8), lo);

        var buf = new QwpBatchBuffer(bytes.Length);
        buf.CopyFromPayload(bytes);
        var batch = new QwpColumnBatch();
        batch.Reset(buf, 1);
        batch.Layouts.Add(new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_DECIMAL128, DecimalScale = 4 },
            ValuesOffset = 0,
            NullBitmapOffset = -1,
            NonNullCount = 1,
        });

        var view = new ColumnView();
        view.BindToColumn(batch, 0);
        Assert.That(view.GetDecimal128High(0), Is.EqualTo(hi));
        Assert.That(view.GetDecimal128Low(0), Is.EqualTo(lo));
    }

    [Test]
    public void GetLong256WordRejectsOutOfRangeIndex()
    {
        var batch = BuildBatchWithLongColumn(new[] { 1L });
        var view = new ColumnView();
        view.BindToColumn(batch, 0);
        Assert.That(() => view.GetLong256Word(0, 4), Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => view.GetLong256Word(0, -1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void RowViewPivotsAcrossColumns()
    {
        // 2 rows × 2 columns: col 0 = LONG, col 1 = VARCHAR.
        var batch = BuildBatchTwoColumns(
            longs: new[] { 7L, 9L },
            strings: new[] { "alpha", "beta" });
        var row = new RowView();
        row.BindToRow(batch, 0);
        Assert.That(row.GetLongValue(0), Is.EqualTo(7L));
        Assert.That(row.GetString(1), Is.EqualTo("alpha"));
        row.BindToRow(batch, 1);
        Assert.That(row.GetLongValue(0), Is.EqualTo(9L));
        Assert.That(row.GetString(1), Is.EqualTo("beta"));
    }

    /// <summary>Hand-builds a batch whose only column is a non-nullable LONG column.</summary>
    private static QwpColumnBatch BuildBatchWithLongColumn(long[] values)
    {
        var buf = new QwpBatchBuffer(values.Length * 8);
        var bytes = new byte[values.Length * 8];
        for (var i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 8, 8), values[i]);
        }
        buf.CopyFromPayload(bytes);

        var layout = new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_LONG },
            ValuesOffset = 0,
            NullBitmapOffset = -1,
            NonNullCount = values.Length,
        };
        var batch = new QwpColumnBatch();
        batch.Reset(buf, values.Length);
        batch.Layouts.Add(layout);
        return batch;
    }

    private static QwpColumnBatch BuildBatchWithDoubleColumn(double[] values)
    {
        var buf = new QwpBatchBuffer(values.Length * 8);
        var bytes = new byte[values.Length * 8];
        for (var i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteDoubleLittleEndian(bytes.AsSpan(i * 8, 8), values[i]);
        }
        buf.CopyFromPayload(bytes);

        var layout = new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_DOUBLE },
            ValuesOffset = 0,
            NullBitmapOffset = -1,
            NonNullCount = values.Length,
        };
        var batch = new QwpColumnBatch();
        batch.Reset(buf, values.Length);
        batch.Layouts.Add(layout);
        return batch;
    }

    private static QwpColumnBatch BuildBatchWithVarcharColumn(string[] values)
    {
        // Wire layout: cumulative int32 offsets (N+1 entries) followed by concatenated UTF-8.
        var concat = string.Join(string.Empty, values);
        var concatBytes = Encoding.UTF8.GetBytes(concat);
        var offsetsLen = (values.Length + 1) * 4;
        var bytes = new byte[offsetsLen + concatBytes.Length];

        var cumulative = 0;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), cumulative);
        for (var i = 0; i < values.Length; i++)
        {
            cumulative += Encoding.UTF8.GetByteCount(values[i]);
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan((i + 1) * 4, 4), cumulative);
        }
        Array.Copy(concatBytes, 0, bytes, offsetsLen, concatBytes.Length);

        var buf = new QwpBatchBuffer(bytes.Length);
        buf.CopyFromPayload(bytes);

        var layout = new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_VARCHAR },
            ValuesOffset = 0,
            StringBytesOffset = offsetsLen,
            NullBitmapOffset = -1,
            NonNullCount = values.Length,
        };
        var batch = new QwpColumnBatch();
        batch.Reset(buf, values.Length);
        batch.Layouts.Add(layout);
        return batch;
    }

    private static QwpColumnBatch BuildBatchWithNullableLongColumn(long[] values, byte[] nullBitmap, int[] denseIndices)
    {
        // Layout: bitmap + dense LONG values. Dense values are only the non-null ones.
        var nonNullValues = new List<long>();
        for (var i = 0; i < denseIndices.Length; i++)
        {
            if (denseIndices[i] >= 0) nonNullValues.Add(values[i]);
        }
        var bytes = new byte[nullBitmap.Length + nonNullValues.Count * 8];
        Array.Copy(nullBitmap, bytes, nullBitmap.Length);
        for (var i = 0; i < nonNullValues.Count; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(
                bytes.AsSpan(nullBitmap.Length + i * 8, 8), nonNullValues[i]);
        }

        var buf = new QwpBatchBuffer(bytes.Length);
        buf.CopyFromPayload(bytes);

        var layout = new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_LONG },
            NullBitmapOffset = 0,
            ValuesOffset = nullBitmap.Length,
            NonNullCount = nonNullValues.Count,
            NonNullIdx = denseIndices,
        };
        var batch = new QwpColumnBatch();
        batch.Reset(buf, values.Length);
        batch.Layouts.Add(layout);
        return batch;
    }

    private static QwpColumnBatch BuildBatchTwoColumns(long[] longs, string[] strings)
    {
        // Combined payload: longs first, then varchar offsets+data.
        var longBytes = new byte[longs.Length * 8];
        for (var i = 0; i < longs.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(longBytes.AsSpan(i * 8, 8), longs[i]);
        }

        var concat = string.Join(string.Empty, strings);
        var concatBytes = Encoding.UTF8.GetBytes(concat);
        var offsetsLen = (strings.Length + 1) * 4;
        var varcharBytes = new byte[offsetsLen + concatBytes.Length];
        var cumulative = 0;
        BinaryPrimitives.WriteInt32LittleEndian(varcharBytes.AsSpan(0, 4), cumulative);
        for (var i = 0; i < strings.Length; i++)
        {
            cumulative += Encoding.UTF8.GetByteCount(strings[i]);
            BinaryPrimitives.WriteInt32LittleEndian(varcharBytes.AsSpan((i + 1) * 4, 4), cumulative);
        }
        Array.Copy(concatBytes, 0, varcharBytes, offsetsLen, concatBytes.Length);

        var combined = new byte[longBytes.Length + varcharBytes.Length];
        Array.Copy(longBytes, 0, combined, 0, longBytes.Length);
        Array.Copy(varcharBytes, 0, combined, longBytes.Length, varcharBytes.Length);

        var buf = new QwpBatchBuffer(combined.Length);
        buf.CopyFromPayload(combined);

        var batch = new QwpColumnBatch();
        batch.Reset(buf, longs.Length);
        batch.Layouts.Add(new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_LONG },
            ValuesOffset = 0,
            NullBitmapOffset = -1,
            NonNullCount = longs.Length,
        });
        batch.Layouts.Add(new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { WireType = QwpConstants.TYPE_VARCHAR },
            ValuesOffset = longBytes.Length,
            StringBytesOffset = longBytes.Length + offsetsLen,
            NullBitmapOffset = -1,
            NonNullCount = strings.Length,
        });
        return batch;
    }
}
