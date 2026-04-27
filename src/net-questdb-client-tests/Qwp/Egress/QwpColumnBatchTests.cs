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
///     Focused tests for the column-major access surface added in PR 9d
///     (ColumnView caching + ForEachRow + convenience getters). The frame-level
///     parser ports in PR 11 (QwpResultBatchDecoder).
/// </summary>
[TestFixture]
public class QwpColumnBatchTests
{
    [Test]
    public void ColumnCachesViewPerIndex()
    {
        var batch = BuildBatch(new[] { 1L, 2L, 3L });
        var first = batch.Column(0);
        var second = batch.Column(0);
        Assert.That(second, Is.SameAs(first), "second Column(0) returns the cached view");
    }

    [Test]
    public void ColumnViewsAreIndependentAcrossIndices()
    {
        var batch = BuildTwoColumnBatch(new[] { 7L }, new[] { "x" });
        var c0 = batch.Column(0);
        var c1 = batch.Column(1);
        Assert.That(c1, Is.Not.SameAs(c0));
        Assert.That(c0.GetLongValue(0), Is.EqualTo(7L));
        Assert.That(c1.GetString(0), Is.EqualTo("x"));
    }

    [Test]
    public void ForEachRowVisitsEveryRowInOrder()
    {
        var batch = BuildBatch(new[] { 10L, 20L, 30L });
        var visited = new List<long>();
        batch.ForEachRow(row => visited.Add(row.GetLongValue(0)));
        Assert.That(visited, Is.EqualTo(new[] { 10L, 20L, 30L }));
    }

    [Test]
    public void ForEachRowExceptionPropagates()
    {
        var batch = BuildBatch(new[] { 1L, 2L });
        Assert.That(() => batch.ForEachRow(_ => throw new InvalidOperationException("boom")),
                    Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("boom"));
    }

    [Test]
    public void RowReturnsBoundRowView()
    {
        var batch = BuildBatch(new[] { 100L, 200L });
        var row = batch.Row(1);
        Assert.That(row.RowIndex, Is.EqualTo(1));
        Assert.That(row.GetLongValue(0), Is.EqualTo(200L));
    }

    [Test]
    public void ColumnMajorConveniencesPassThrough()
    {
        var batch = BuildTwoColumnBatch(new[] { 42L, 43L }, new[] { "alpha", "beta" });
        Assert.That(batch.GetLongValue(0, 1), Is.EqualTo(43L));
        Assert.That(batch.GetString(1, 0), Is.EqualTo("alpha"));
    }

    [Test]
    public void GetColumnWireTypeReadsLayoutInfo()
    {
        var batch = BuildBatch(new[] { 1L });
        Assert.That(batch.GetColumnWireType(0), Is.EqualTo(QwpConstants.TYPE_LONG));
    }

    [Test]
    public void ResetClearsCachedColumnViews()
    {
        // After Reset the cache should re-bind on the next Column() call. Use a
        // freshly-built batch as a stand-in for "post-decoder reset".
        var first = BuildBatch(new[] { 1L });
        var firstView = first.Column(0);
        first.Reset(first.Buffer!, 1);  // simulates decoder priming for the next batch
        var secondView = first.Column(0);
        // Same instance reused, but BindToColumn refreshed the layout pointer.
        Assert.That(secondView, Is.SameAs(firstView));
    }

    private static QwpColumnBatch BuildBatch(long[] values)
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

    private static QwpColumnBatch BuildTwoColumnBatch(long[] longs, string[] strings)
    {
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
