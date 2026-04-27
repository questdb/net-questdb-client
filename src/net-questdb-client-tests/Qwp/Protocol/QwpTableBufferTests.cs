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
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp.Protocol;

/// <summary>
///     Mirrors <c>QwpTableBufferTest.java</c> on Java main 64b7ee69. The Java test class
///     focuses on edge cases — cancel-row state rewinds, decimal scale tracking, symbol
///     dictionary state, array wrapper lifecycles — rather than happy-path per-type writes
///     (those are exercised through the integration test layer). Production code
///     (<c>QwpTableBuffer</c>, <c>QwpTableBuffer.ColumnBuffer</c>) lands in PR 3.
///     <para/>
///     Tests are grouped here by the part of PR 3 each one depends on:
///     <list type="bullet">
///         <item><b>3a</b> — skeleton + simple types (lookup, get-or-create, max-columns,
///             type mismatch, case-insensitive lookup).</item>
///         <item><b>3b</b> — symbol column support + cancel-row state rewinds for symbols
///             (needs the eventual <c>QwpWebSocketSender</c> callback for global symbol IDs;
///             the per-batch local dictionary is in scope for PR 3b).</item>
///         <item><b>3c</b> — decimal column support (Decimal64/128/256 .NET equivalents
///             or System.Decimal mapping). Decimal scale tracking + rescale precision loss
///             tests depend on this.</item>
///         <item><b>3d</b> — array column support (DoubleArray/LongArray wrappers + cancel-row
///             rewinds for array offsets).</item>
///     </list>
/// </summary>
[TestFixture]
public class QwpTableBufferTests
{
    private const string AwaitingSkeleton = "Awaiting PR 3a: QwpTableBuffer skeleton.";
    private const string AwaitingSymbol = "Awaiting PR 3b: symbol column support.";
    private const string AwaitingDecimal = "Awaiting PR 3c: decimal column support.";
    private const string AwaitingArray = "Awaiting PR 3d: array column support.";

    // ---- Lookup / get-or-create / capacity caps (PR 3a) ----

    [Test]
    public void GetExistingColumnReturnsNullWithoutCreatingColumn()
    {
        var table = new QwpTableBuffer("t");
        Assert.That(table.GetExistingColumn("missing", QwpConstants.TYPE_LONG), Is.Null);
        Assert.That(table.ColumnCount, Is.EqualTo(0));
    }

    [Test]
    public void GetExistingColumnReturnsOrderedColumnsAcrossRows()
    {
        var table = new QwpTableBuffer("t");
        var colA = table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!;
        var colB = table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!;
        colA.AddLong(1);
        colB.AddString("x");
        table.NextRow();

        var existingA = table.GetExistingColumn("a", QwpConstants.TYPE_LONG);
        var existingB = table.GetExistingColumn("b", QwpConstants.TYPE_VARCHAR);
        Assert.That(existingA, Is.SameAs(colA));
        Assert.That(existingB, Is.SameAs(colB));

        existingA!.AddLong(2);
        existingB!.AddString("y");
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(2));
        Assert.That(colA.Size, Is.EqualTo(2));
        Assert.That(colA.ValueCount, Is.EqualTo(2));
        Assert.That(colB.Size, Is.EqualTo(2));
        Assert.That(colB.ValueCount, Is.EqualTo(2));
    }

    [Test]
    public void GetExistingColumnReturnsOutOfOrderColumns()
    {
        var table = new QwpTableBuffer("t");
        var colA = table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!;
        var colB = table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!;
        colA.AddLong(1);
        colB.AddString("x");
        table.NextRow();

        // Lookup b before a — out-of-order, falls through to the dictionary path.
        var existingB = table.GetExistingColumn("b", QwpConstants.TYPE_VARCHAR);
        var existingA = table.GetExistingColumn("a", QwpConstants.TYPE_LONG);
        Assert.That(existingB, Is.SameAs(colB));
        Assert.That(existingA, Is.SameAs(colA));
    }

    [Test]
    public void GetExistingColumnTypeMismatchOnHashPathThrows()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!.AddString("x");
        table.NextRow();

        // Skip ahead so the cursor is past index 0 — forces the dictionary path.
        Assert.That(() => table.GetExistingColumn("b", QwpConstants.TYPE_LONG),
                    Throws.TypeOf<IngressError>()
                          .With.Message.Contains("Column type mismatch")
                          .And.Message.Contains("column 'b'"));
    }

    [Test]
    public void GetExistingColumnTypeMismatchOnOrderedPathThrows()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
        table.NextRow();

        Assert.That(() => table.GetExistingColumn("a", QwpConstants.TYPE_VARCHAR),
                    Throws.TypeOf<IngressError>()
                          .With.Message.Contains("Column type mismatch")
                          .And.Message.Contains("column 'a'"));
    }

    [Test]
    public void GetExistingColumnWorksAfterReset()
    {
        var table = new QwpTableBuffer("t");
        var colA = table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!;
        var colB = table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!;
        colA.AddLong(1);
        colB.AddString("x");
        table.NextRow();

        table.Reset();

        Assert.That(table.GetExistingColumn("a", QwpConstants.TYPE_LONG), Is.SameAs(colA));
        Assert.That(table.GetExistingColumn("b", QwpConstants.TYPE_VARCHAR), Is.SameAs(colB));

        colA.AddLong(2);
        colB.AddString("y");
        table.NextRow();
        Assert.That(table.RowCount, Is.EqualTo(1));
        Assert.That(colA.Size, Is.EqualTo(1));
        Assert.That(colB.Size, Is.EqualTo(1));
    }

    [Test]
    public void GetExistingColumnWorksForLateAddedColumnAfterCancelRow()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.NextRow();

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(2);
        var late = table.GetOrCreateColumn("late", QwpConstants.TYPE_VARCHAR, true)!;
        late.AddString("stale");
        table.CancelCurrentRow();

        // The late-added column is still discoverable via GetExistingColumn after the
        // cancel — the column registry isn't rolled back, only its row data.
        Assert.That(table.GetExistingColumn("late", QwpConstants.TYPE_VARCHAR), Is.SameAs(late));
        Assert.That(late.Size, Is.EqualTo(0));
        Assert.That(late.ValueCount, Is.EqualTo(0));
    }

    [Test]
    public void GetOrCreateColumnConflictingTypeFastPath()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("x", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.NextRow();

        Assert.That(() => table.GetOrCreateColumn("x", QwpConstants.TYPE_DOUBLE, false),
                    Throws.TypeOf<IngressError>()
                          .With.Message.EndsWith(
                              $"Column type mismatch for column 'x': columnType={QwpConstants.TYPE_LONG}, sentType={QwpConstants.TYPE_DOUBLE}"));
    }

    [Test]
    public void GetOrCreateColumnConflictingTypeSlowPath()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!.AddString("v");
        table.NextRow();

        // Cursor expects "a" at index 0; we ask for "b" — fast path misses, falls through
        // to the dictionary, which detects the conflict.
        Assert.That(() => table.GetOrCreateColumn("b", QwpConstants.TYPE_LONG, false),
                    Throws.TypeOf<IngressError>()
                          .With.Message.EndsWith(
                              $"Column type mismatch for column 'b': columnType={QwpConstants.TYPE_VARCHAR}, sentType={QwpConstants.TYPE_LONG}"));
    }

    [Test]
    public void GetOrCreateColumnThrowsWhenExceedingMaxColumnCount()
    {
        var table = new QwpTableBuffer("t");
        for (var i = 0; i < QwpConstants.MAX_COLUMNS_PER_TABLE; i++)
        {
            table.GetOrCreateColumn("c" + i, QwpConstants.TYPE_LONG, false);
        }
        Assert.That(table.ColumnCount, Is.EqualTo(QwpConstants.MAX_COLUMNS_PER_TABLE));

        Assert.That(() => table.GetOrCreateColumn("overflow", QwpConstants.TYPE_LONG, false),
                    Throws.TypeOf<IngressError>()
                          .With.Message.EndsWith(
                              $"column count exceeds maximum: {QwpConstants.MAX_COLUMNS_PER_TABLE + 1} (max {QwpConstants.MAX_COLUMNS_PER_TABLE})"));
        Assert.That(table.ColumnCount, Is.EqualTo(QwpConstants.MAX_COLUMNS_PER_TABLE));
    }

    [Test]
    public void NextRowWithPreparedMissingColumnsPadsListedColumns()
    {
        // .NET API doesn't take a prepared missing-columns array; the simpler NextRow()
        // pads every column. Verify the equivalent invariant: missing columns get null
        // padding when the row commits.
        var table = new QwpTableBuffer("t");
        var colA = table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!;
        var colB = table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!;
        colA.AddLong(1);
        colB.AddString("x");
        table.NextRow();

        // Row 2: only set a; b is missing.
        colA.AddLong(2);
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(2));
        Assert.That(colA.Size, Is.EqualTo(2));
        Assert.That(colB.Size, Is.EqualTo(2));
        Assert.That(colB.IsNull(1), Is.True);
    }

    [Test]
    public void NonAsciiColumnNameCaseInsensitive()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("Ñame", QwpConstants.TYPE_LONG, false)!;
        col.AddLong(42);
        table.NextRow();

        // Different case but same Unicode code points — should resolve to the same column.
        var other = table.GetExistingColumn("ñame", QwpConstants.TYPE_LONG);
        Assert.That(other, Is.SameAs(col));
    }

    [Test]
    public void CancelRowTruncatesLateAddedColumn()
    {
        var table = new QwpTableBuffer("t");
        for (var i = 0; i < 3; i++)
        {
            table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(i);
            table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!.AddString("v" + i);
            table.NextRow();
        }

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(3);
        table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!.AddString("v3");
        var colC = table.GetOrCreateColumn("c", QwpConstants.TYPE_VARCHAR, true)!;
        colC.AddString("stale");

        table.CancelCurrentRow();

        Assert.That(colC.Size, Is.EqualTo(0));
        Assert.That(colC.ValueCount, Is.EqualTo(0));

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(3);
        table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!.AddString("v3");
        table.NextRow();

        // Column c was padded with 4 nulls (rows 0..3) by the NextRow walk.
        Assert.That(colC.Size, Is.EqualTo(4));
        Assert.That(colC.ValueCount, Is.EqualTo(0));
        for (var i = 0; i < 4; i++) Assert.That(colC.IsNull(i), Is.True, $"row {i} should be null");
    }

    [Test]
    public void CancelRowTruncatesLateAddedColumnWhenSizeEqualsRowCount()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(0);
        table.NextRow();

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        var colC = table.GetOrCreateColumn("c", QwpConstants.TYPE_VARCHAR, true)!;
        colC.AddString("stale");

        table.CancelCurrentRow();

        Assert.That(colC.Size, Is.EqualTo(0));
        Assert.That(colC.ValueCount, Is.EqualTo(0));
    }

    [Test]
    [Ignore("Awaiting RetainInProgressRow API surface (Java-specific helper not in PR 3a scope; revisit with PR 3b microbatch state retention).")]
    public void RetainInProgressRowFastClearsUnstagedNullableColumn() { }

    [Test]
    public void CancelRowResetsGeohashPrecisionOnLateAddedColumn()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(0);
        table.NextRow();

        // Late-added geohash column gets a precision; cancel-row must reset it.
        var geo = table.GetOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, true)!;
        geo.AddGeoHash(0xABCD, 20);

        table.CancelCurrentRow();

        // After cancel, the column is fully cleared — adding a different precision now
        // must succeed (would have thrown a "precision mismatch" if state had leaked).
        Assert.That(geo.Size, Is.EqualTo(0));
        // Re-add the column for the next row (it was registered before cancel; cancel
        // doesn't remove it from the registry).
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        Assert.That(() => geo.AddGeoHash(0x1234, 30), Throws.Nothing);
    }

    // ---- Symbol column support (PR 3b) ----

    [Test]
    public void AddSymbolNullOnNonNullableColumn()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, false)!;
        col.AddSymbol("server1");
        table.NextRow();
        col.AddSymbol(null);
        table.NextRow();
        col.AddSymbol("server2");
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(3));
        // Non-nullable: every row produces a physical value (sentinel for nulls).
        Assert.That(col.Size, Is.EqualTo(col.ValueCount));
    }

    [Test]
    public void AddSymbolUtf8CancelRowRewindsDictionary()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(0);
        table.NextRow();

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        var col = table.GetOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true)!;
        col.AddSymbolUtf8(System.Text.Encoding.UTF8.GetBytes("stale"));
        table.CancelCurrentRow();

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        col.AddSymbolUtf8(System.Text.Encoding.UTF8.GetBytes("fresh"));
        table.NextRow();

        Assert.That(col.Size, Is.EqualTo(2));
        Assert.That(col.ValueCount, Is.EqualTo(1));
        Assert.That(col.GetSymbolDictionary(), Is.EqualTo(new[] { "fresh" }));
        // First int in the data buffer is the local index for "fresh" (= 0).
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(col.GetDataReadOnlySpan()), Is.EqualTo(0));
    }

    [Test]
    public void AddSymbolUtf8RejectsInvalidUtf8()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)!;
        var invalid = new byte[] { 0xC3, 0x28 };

        Assert.That(() => col.AddSymbolUtf8(invalid),
                    Throws.TypeOf<IngressError>().With.Message.Contains("invalid UTF-8"));
        Assert.That(col.Size, Is.EqualTo(0));
        Assert.That(col.ValueCount, Is.EqualTo(0));
        Assert.That(col.SymbolDictionarySize, Is.EqualTo(0));
    }

    [Test]
    public void AddSymbolUtf8ReusesExistingDictionaryEntry()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)!;
        col.AddSymbolUtf8(System.Text.Encoding.UTF8.GetBytes("東京"));
        table.NextRow();
        col.AddSymbolUtf8(System.Text.Encoding.UTF8.GetBytes("東京"));
        table.NextRow();
        col.AddSymbolUtf8(System.Text.Encoding.UTF8.GetBytes("Αθηνα"));
        table.NextRow();

        Assert.That(col.Size, Is.EqualTo(3));
        Assert.That(col.ValueCount, Is.EqualTo(3));
        Assert.That(col.SymbolDictionarySize, Is.EqualTo(2));
        Assert.That(col.GetSymbolDictionary(), Is.EqualTo(new[] { "東京", "Αθηνα" }));

        var data = col.GetDataReadOnlySpan();
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(data.Slice(0, 4)), Is.EqualTo(0));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(data.Slice(4, 4)), Is.EqualTo(0));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(data.Slice(8, 4)), Is.EqualTo(1));
    }

    [Test]
    public void AddSymbolWithGlobalIdStoresOnlyGlobalIds()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)!;
        col.AddSymbolWithGlobalId("alpha", 7);
        table.NextRow();
        col.AddSymbolWithGlobalId("beta", 11);
        table.NextRow();

        Assert.That(col.Size, Is.EqualTo(2));
        Assert.That(col.ValueCount, Is.EqualTo(2));
        Assert.That(col.SymbolDictionarySize, Is.EqualTo(0));
        Assert.That(col.GetAuxDataReadOnlySpan().IsEmpty, Is.True);
        Assert.That(col.MaxGlobalSymbolId, Is.EqualTo(11));

        var data = col.GetDataReadOnlySpan();
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(data.Slice(0, 4)), Is.EqualTo(7));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(data.Slice(4, 4)), Is.EqualTo(11));
    }

    [Test]
    public void CancelRowResetsSymbolDictOnLateAddedColumn()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(0);
        table.NextRow();

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        var col = table.GetOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true)!;
        col.AddSymbol("stale");
        table.CancelCurrentRow();

        // After cancel, dictionary must be empty — "fresh" gets local id 0, not 1.
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        col.AddSymbol("fresh");
        table.NextRow();

        Assert.That(col.Size, Is.EqualTo(2));
        Assert.That(col.ValueCount, Is.EqualTo(1));
        var dict = col.GetSymbolDictionary();
        Assert.That(dict.Length, Is.EqualTo(1));
        Assert.That(dict[0], Is.EqualTo("fresh"));
    }

    [Test]
    public void CancelRowRetainsGlobalSymbolIdWithoutLocalDictionary()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(0);
        table.NextRow();

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        var col = table.GetOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true)!;
        col.AddSymbolWithGlobalId("stale", 4);
        table.CancelCurrentRow();

        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        col.AddSymbolWithGlobalId("fresh", 9);
        table.NextRow();

        Assert.That(col.Size, Is.EqualTo(2));
        Assert.That(col.ValueCount, Is.EqualTo(1));
        Assert.That(col.SymbolDictionarySize, Is.EqualTo(0));
        Assert.That(col.MaxGlobalSymbolId, Is.EqualTo(9));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(col.GetDataReadOnlySpan()), Is.EqualTo(9));
    }

    // ---- Decimal column support (PR 3c) ----
    [Test] public void AddDecimal64PrecisionLoss() => Assert.Inconclusive(AwaitingDecimal);
    [Test] public void AddDecimal64RescaleOverflow() => Assert.Inconclusive(AwaitingDecimal);
    [Test] public void AddDecimal128PrecisionLoss() => Assert.Inconclusive(AwaitingDecimal);
    [Test] public void AddDecimal128RescaleOverflow() => Assert.Inconclusive(AwaitingDecimal);
    [Test] public void CancelRowResetsDecimalScaleOnLateAddedColumn() => Assert.Inconclusive(AwaitingDecimal);

    // ---- Array column support (PR 3d) ----
    [Test] public void AddDoubleArrayNullOnNonNullableColumn() => Assert.Inconclusive(AwaitingArray);
    [Test] public void AddDoubleArrayPayloadSupportsHigherDimensionalShape() => Assert.Inconclusive(AwaitingArray);
    [Test] public void AddLongArrayNullOnNonNullableColumn() => Assert.Inconclusive(AwaitingArray);
    [Test] public void CancelRowRewindsDoubleArrayOffsets() => Assert.Inconclusive(AwaitingArray);
    [Test] public void CancelRowRewindsLongArrayOffsets() => Assert.Inconclusive(AwaitingArray);
    [Test] public void CancelRowRewindsMultiDimArrayOffsets() => Assert.Inconclusive(AwaitingArray);
    [Test] public void DoubleArrayWrapperMultipleRows() => Assert.Inconclusive(AwaitingArray);
    [Test] public void DoubleArrayWrapperShrinkingSize() => Assert.Inconclusive(AwaitingArray);
    [Test] public void DoubleArrayWrapperVaryingDimensionality() => Assert.Inconclusive(AwaitingArray);
    [Test] public void LongArrayMultipleRows() => Assert.Inconclusive(AwaitingArray);
    [Test] public void LongArrayShrinkingSize() => Assert.Inconclusive(AwaitingArray);
    [Test] public void LongArrayWrapperMultipleRows() => Assert.Inconclusive(AwaitingArray);
}
