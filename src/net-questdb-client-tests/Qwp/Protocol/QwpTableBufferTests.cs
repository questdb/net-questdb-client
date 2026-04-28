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

    // ---- RetainInProgressRow (PR D — UDP flush-and-batch) ----

    [Test]
    public void RetainInProgressRow_NoInProgressData_ClearsAllColumns()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("v", QwpConstants.TYPE_LONG, false)!;
        col.AddLong(1);
        table.NextRow();
        col.AddLong(2);
        table.NextRow();

        // No in-progress row at this point — _size == _rowCount.
        table.RetainInProgressRow();

        Assert.That(table.RowCount, Is.EqualTo(0));
        Assert.That(col.Size, Is.EqualTo(0));
        Assert.That(col.ValueCount, Is.EqualTo(0));
    }

    [Test]
    public void RetainInProgressRow_LongColumn_PreservesLastValue()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("v", QwpConstants.TYPE_LONG, false)!;
        col.AddLong(10);
        table.NextRow();
        col.AddLong(20);
        table.NextRow();
        // Stage a third row.
        col.AddLong(30);

        table.RetainInProgressRow();

        Assert.That(table.RowCount, Is.EqualTo(0));
        Assert.That(col.Size, Is.EqualTo(1));
        Assert.That(col.ValueCount, Is.EqualTo(1));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(col.DataMemory.Span), Is.EqualTo(30L));
    }

    [Test]
    public void RetainInProgressRow_VarcharColumn_PreservesString()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, false)!;
        col.AddString("alpha");
        table.NextRow();
        col.AddString("beta");
        table.NextRow();
        col.AddString("gamma");  // in-progress

        table.RetainInProgressRow();

        Assert.That(col.Size, Is.EqualTo(1));
        // Read back via the string offsets: offset[0]=0, offset[1]=5 (utf8 length of "gamma").
        Assert.That(col.StringDataSize, Is.EqualTo(5));
    }

    [Test]
    public void RetainInProgressRow_DecimalColumn_PreservesScale()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, false)!;
        col.AddDecimal64(12345L, scale: 2);
        table.NextRow();
        col.AddDecimal64(67890L, scale: 2);
        table.NextRow();
        col.AddDecimal64(99999L, scale: 2);  // in-progress

        table.RetainInProgressRow();

        Assert.That(col.Size, Is.EqualTo(1));
        Assert.That(col.DecimalScale, Is.EqualTo(2));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(col.DataMemory.Span), Is.EqualTo(99999L));
    }

    [Test]
    public void RetainInProgressRow_GeohashColumn_PreservesPrecision()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, false)!;
        col.AddGeoHash(0xCAFE, precision: 16);
        table.NextRow();
        col.AddGeoHash(0xBEEF, precision: 16);  // in-progress

        table.RetainInProgressRow();

        Assert.That(col.Size, Is.EqualTo(1));
        Assert.That(col.GeoHashPrecision, Is.EqualTo(16));
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(col.DataMemory.Span), Is.EqualTo(0xBEEFL));
    }

    [Test]
    public void RetainInProgressRow_SymbolColumn_PreservesValueAndResetsLocalDict()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, false)!;
        col.AddSymbol("AAPL");
        table.NextRow();
        col.AddSymbol("MSFT");  // in-progress

        table.RetainInProgressRow();

        Assert.That(col.Size, Is.EqualTo(1));
        Assert.That(col.SymbolDictionarySize, Is.EqualTo(1));
        // The retained "MSFT" lands at local-dict index 0 of the trimmed column.
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(col.DataMemory.Span), Is.EqualTo(0));
    }

    [Test]
    public void RetainInProgressRow_DoubleArrayColumn_PreservesArray()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("a", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[] { 1.0, 2.0 });
        table.NextRow();
        col.AddDoubleArray(new[] { 3.0, 4.0, 5.0 });  // in-progress

        table.RetainInProgressRow();

        Assert.That(col.Size, Is.EqualTo(1));
        Assert.That(col.GetDoubleArrayData(), Is.EqualTo(new[] { 3.0, 4.0, 5.0 }));
    }

    [Test]
    public void RetainInProgressRow_MultiColumnMixedInProgress()
    {
        var table = new QwpTableBuffer("t");
        var colA = table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!;
        var colB = table.GetOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true)!;
        colA.AddLong(1); colB.AddString("alpha");
        table.NextRow();
        // In-progress: write to colA but not colB. NextRow not yet called.
        colA.AddLong(2);

        table.RetainInProgressRow();

        Assert.That(table.RowCount, Is.EqualTo(0));
        Assert.That(colA.Size, Is.EqualTo(1));
        Assert.That(colB.Size, Is.EqualTo(0)); // colB had no in-progress write
        Assert.That(BinaryPrimitives.ReadInt64LittleEndian(colA.DataMemory.Span), Is.EqualTo(2L));
    }

    [Test]
    public void RetainInProgressRow_ResetsRowCountToZero()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("v", QwpConstants.TYPE_LONG, false)!;
        for (var i = 0; i < 5; i++) { col.AddLong(i); table.NextRow(); }

        table.RetainInProgressRow();

        Assert.That(table.RowCount, Is.EqualTo(0));
        Assert.That(col.Size, Is.EqualTo(0));
    }

    // ---- EstimateEncodedDatagramSize (PR D2) ----

    [Test]
    public void EstimateEncodedDatagramSize_ZeroRowsReturnsZero()
    {
        var table = new QwpTableBuffer("t");
        Assert.That(table.EstimateEncodedDatagramSize(0), Is.EqualTo(0));
    }

    [Test]
    public void EstimateEncodedDatagramSize_SingleColumnLongMatchesEncoder()
    {
        var table = new QwpTableBuffer("trades");
        var col = table.GetOrCreateColumn("v", QwpConstants.TYPE_LONG, false)!;
        for (var i = 0; i < 5; i++) { col.AddLong(i); table.NextRow(); }

        var estimate = table.EstimateEncodedDatagramSize(table.RowCount);
        var encoder = new QwpWebSocketEncoder();
        var actual = encoder.Encode(table, useSchemaRef: false);

        Assert.That(estimate, Is.EqualTo(actual));
    }

    [Test]
    public void EstimateEncodedDatagramSize_MixedColumnsMatchesEncoder()
    {
        var table = new QwpTableBuffer("metrics");
        var sym = table.GetOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)!;
        var price = table.GetOrCreateColumn("price", QwpConstants.TYPE_DOUBLE, false)!;
        var name = table.GetOrCreateColumn("name", QwpConstants.TYPE_VARCHAR, true)!;
        sym.AddSymbol("AAPL"); price.AddDouble(123.45); name.AddString("alpha");
        table.NextRow();
        sym.AddSymbol("MSFT"); price.AddDouble(67.89); name.AddString("beta");
        table.NextRow();
        sym.AddSymbol("AAPL"); price.AddDouble(11.11); name.AddString("gamma");
        table.NextRow();

        var estimate = table.EstimateEncodedDatagramSize(table.RowCount);
        var encoder = new QwpWebSocketEncoder();
        var actual = encoder.Encode(table, useSchemaRef: false);

        Assert.That(estimate, Is.EqualTo(actual));
    }

    [Test]
    public void EstimateEncodedDatagramSize_DecimalColumnMatchesEncoder()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("price", QwpConstants.TYPE_DECIMAL64, false)!;
        col.AddDecimal64(12345, scale: 2); table.NextRow();
        col.AddDecimal64(67890, scale: 2); table.NextRow();

        var estimate = table.EstimateEncodedDatagramSize(table.RowCount);
        var encoder = new QwpWebSocketEncoder();
        var actual = encoder.Encode(table, useSchemaRef: false);

        Assert.That(estimate, Is.EqualTo(actual));
    }

    [Test]
    public void EstimateEncodedDatagramSize_DoubleArrayMatchesEncoder()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("a", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[] { 1.0, 2.0 }); table.NextRow();
        col.AddDoubleArray(new[] { 3.0, 4.0, 5.0 }); table.NextRow();

        var estimate = table.EstimateEncodedDatagramSize(table.RowCount);
        var encoder = new QwpWebSocketEncoder();
        var actual = encoder.Encode(table, useSchemaRef: false);

        Assert.That(estimate, Is.EqualTo(actual));
    }

    [Test]
    public void EstimateEncodedDatagramSize_GeohashMatchesEncoder()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, false)!;
        col.AddGeoHash(0xCAFE, precision: 16); table.NextRow();
        col.AddGeoHash(0xBEEF, precision: 16); table.NextRow();

        var estimate = table.EstimateEncodedDatagramSize(table.RowCount);
        var encoder = new QwpWebSocketEncoder();
        var actual = encoder.Encode(table, useSchemaRef: false);

        Assert.That(estimate, Is.EqualTo(actual));
    }

    [Test]
    public void EstimateEncodedDatagramSize_NullableColumnWithSomeNullsMatchesEncoder()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("v", QwpConstants.TYPE_LONG, useNullBitmap: true)!;
        col.AddLong(1); table.NextRow();
        // Skip writing — NextRow pads with null.
        table.NextRow();
        col.AddLong(3); table.NextRow();

        var estimate = table.EstimateEncodedDatagramSize(table.RowCount);
        var encoder = new QwpWebSocketEncoder();
        var actual = encoder.Encode(table, useSchemaRef: false);

        Assert.That(estimate, Is.EqualTo(actual));
    }

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

    [Test]
    public void AddDecimal64PrecisionLoss()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true)!;
        col.AddDecimal64(100, 2);                  // first row latches scale = 2
        table.NextRow();
        // Second row at scale 4 with trailing fractional digits — rescaling to scale 2
        // would lose precision (12345 / 100 = 123 remainder 45).
        Assert.That(() => col.AddDecimal64(12345, 4),
                    Throws.TypeOf<IngressError>().With.Message.Contains("precision loss"));
    }

    [Test]
    public void AddDecimal64RescaleOverflow()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true)!;
        col.AddDecimal64(1, 5);                    // first row latches scale = 5
        table.NextRow();
        // Second row at scale 0 with a large value — multiplying by 10^5 exceeds long range.
        Assert.That(() => col.AddDecimal64(long.MaxValue / 10, 0),
                    Throws.TypeOf<IngressError>().With.Message.EndsWith(
                        "Decimal64 overflow: rescaling from scale 0 to 5 exceeds 64-bit capacity"));
    }

    [Test]
    public void AddDecimal128PrecisionLoss()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("d", QwpConstants.TYPE_DECIMAL128, true)!;
        col.AddDecimal128(0, 100, 2);              // first row latches scale = 2
        table.NextRow();
        // Second row at scale 4 — rescaling 12345 from scale 4 to scale 2 loses precision.
        Assert.That(() => col.AddDecimal128(0, 12345, 4),
                    Throws.TypeOf<IngressError>().With.Message.Contains("precision loss"));
    }

    [Test]
    public void AddDecimal128RescaleOverflow()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("d", QwpConstants.TYPE_DECIMAL128, true)!;
        col.AddDecimal128(0, 1, 10);               // first row latches scale = 10
        table.NextRow();
        // (high=long.MaxValue/2, low=long.MaxValue, scale=0) — multiplying by 10^10
        // exceeds 128-bit signed range.
        Assert.That(() => col.AddDecimal128(long.MaxValue / 2, long.MaxValue, 0),
                    Throws.TypeOf<IngressError>().With.Message.EndsWith(
                        "Decimal128 overflow: rescaling from scale 0 to 10 exceeds 128-bit capacity"));
    }

    [Test]
    public void CancelRowResetsDecimalScaleOnLateAddedColumn()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(0);
        table.NextRow();

        // Late-added decimal column with scale 5 in the in-progress row.
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        var col = table.GetOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true)!;
        col.AddDecimal64(100, 5);
        table.CancelCurrentRow();

        // After cancel: decimalScale must be reset, so a value at a different scale
        // succeeds (no rescale, latches the new scale).
        table.GetOrCreateColumn("a", QwpConstants.TYPE_LONG, false)!.AddLong(1);
        Assert.That(() => col.AddDecimal64(42, 3), Throws.Nothing);
        table.NextRow();

        Assert.That(col.Size, Is.EqualTo(2));
        Assert.That(col.ValueCount, Is.EqualTo(1));
    }

    // ---- Array column support (PR 3d) ----

    [Test]
    public void AddDoubleArrayNullOnNonNullableColumn()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[] { 1.0, 2.0 });
        table.NextRow();
        col.AddDoubleArray((double[]?)null);
        table.NextRow();
        col.AddDoubleArray(new[] { 3.0, 4.0 });
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(3));
        Assert.That(col.ValueCount, Is.EqualTo(3));
        Assert.That(col.Size, Is.EqualTo(col.ValueCount));

        var encoded = ReadDoubleArraysLikeEncoder(col);
        Assert.That(encoded, Is.EqualTo(new[] { 1.0, 2.0, 3.0, 4.0 }));

        var dims = col.GetArrayDims();
        var shapes = col.GetArrayShapes();
        Assert.That(dims[0], Is.EqualTo((byte)1));
        Assert.That(shapes[0], Is.EqualTo(2));
        Assert.That(dims[1], Is.EqualTo((byte)1));
        Assert.That(shapes[1], Is.EqualTo(0));
        Assert.That(dims[2], Is.EqualTo((byte)1));
        Assert.That(shapes[2], Is.EqualTo(2));
    }

    [Test]
    [Ignore("Awaiting raw-payload AddDoubleArrayPayload API (Java's appendToBufPtr hook for >3D arrays). PR 3d only ships the typed 1D/2D overloads.")]
    public void AddDoubleArrayPayloadSupportsHigherDimensionalShape() { }

    [Test]
    public void AddLongArrayNullOnNonNullableColumn()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false)!;
        col.AddLongArray(new long[] { 10, 20 });
        table.NextRow();
        col.AddLongArray((long[]?)null);
        table.NextRow();
        col.AddLongArray(new long[] { 30, 40 });
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(3));
        Assert.That(col.ValueCount, Is.EqualTo(3));

        var encoded = ReadLongArraysLikeEncoder(col);
        Assert.That(encoded, Is.EqualTo(new long[] { 10, 20, 30, 40 }));

        var dims = col.GetArrayDims();
        var shapes = col.GetArrayShapes();
        Assert.That(dims[0], Is.EqualTo((byte)1));
        Assert.That(shapes[0], Is.EqualTo(2));
        Assert.That(dims[1], Is.EqualTo((byte)1));
        Assert.That(shapes[1], Is.EqualTo(0));
        Assert.That(dims[2], Is.EqualTo((byte)1));
        Assert.That(shapes[2], Is.EqualTo(2));
    }

    [Test]
    public void CancelRowRewindsDoubleArrayOffsets()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!.AddDoubleArray(new[] { 1.0, 2.0 });
        table.NextRow();
        table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!.AddDoubleArray(new[] { 3.0, 4.0 });
        table.NextRow();

        // Start row 2 then cancel.
        table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!.AddDoubleArray(new[] { 5.0, 6.0 });
        table.CancelCurrentRow();

        // Replacement row 2.
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[] { 7.0, 8.0 });
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(3));
        Assert.That(col.ValueCount, Is.EqualTo(3));
        Assert.That(ReadDoubleArraysLikeEncoder(col),
                    Is.EqualTo(new[] { 1.0, 2.0, 3.0, 4.0, 7.0, 8.0 }));
    }

    [Test]
    public void CancelRowRewindsLongArrayOffsets()
    {
        var table = new QwpTableBuffer("t");
        table.GetOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false)!.AddLongArray(new long[] { 10, 20 });
        table.NextRow();

        table.GetOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false)!.AddLongArray(new long[] { 30, 40 });
        table.CancelCurrentRow();

        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false)!;
        col.AddLongArray(new long[] { 50, 60 });
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(2));
        Assert.That(col.ValueCount, Is.EqualTo(2));
        Assert.That(ReadLongArraysLikeEncoder(col), Is.EqualTo(new long[] { 10, 20, 50, 60 }));
    }

    [Test]
    public void CancelRowRewindsMultiDimArrayOffsets()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[,] { { 1.0, 2.0 }, { 3.0, 4.0 } });
        table.NextRow();

        col.AddDoubleArray(new[,] { { 5.0, 6.0 }, { 7.0, 8.0 } });
        table.CancelCurrentRow();

        col.AddDoubleArray(new[,] { { 9.0, 10.0 }, { 11.0, 12.0 } });
        table.NextRow();

        Assert.That(table.RowCount, Is.EqualTo(2));
        Assert.That(col.ValueCount, Is.EqualTo(2));

        var dims = col.GetArrayDims();
        var shapes = col.GetArrayShapes();
        Assert.That(dims[0], Is.EqualTo((byte)2));
        Assert.That(dims[1], Is.EqualTo((byte)2));
        Assert.That(shapes[0], Is.EqualTo(2));
        Assert.That(shapes[1], Is.EqualTo(2));
        // Replacement row's shapes must be the new [2, 2], not stale data.
        Assert.That(shapes[2], Is.EqualTo(2));
        Assert.That(shapes[3], Is.EqualTo(2));

        Assert.That(ReadDoubleArraysLikeEncoder(col),
                    Is.EqualTo(new[] { 1.0, 2.0, 3.0, 4.0, 9.0, 10.0, 11.0, 12.0 }));
    }

    [Test]
    public void DoubleArrayWrapperMultipleRows()
    {
        // .NET has no DoubleArray wrapper — the typed double[] overload is the equivalent.
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[] { 1.0, 2.0, 3.0 });
        table.NextRow();
        col.AddDoubleArray(new[] { 4.0, 5.0, 6.0 });
        table.NextRow();
        col.AddDoubleArray(new[] { 7.0, 8.0, 9.0 });
        table.NextRow();

        Assert.That(col.ValueCount, Is.EqualTo(3));
        Assert.That(ReadDoubleArraysLikeEncoder(col),
                    Is.EqualTo(new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 }));
        var dims = col.GetArrayDims();
        var shapes = col.GetArrayShapes();
        for (var i = 0; i < 3; i++)
        {
            Assert.That(dims[i], Is.EqualTo((byte)1));
            Assert.That(shapes[i], Is.EqualTo(3));
        }
    }

    [Test]
    public void DoubleArrayWrapperShrinkingSize()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[] { 1.0, 2.0, 3.0, 4.0, 5.0 });
        table.NextRow();
        col.AddDoubleArray(new[] { 10.0, 20.0 });
        table.NextRow();

        Assert.That(col.ValueCount, Is.EqualTo(2));
        Assert.That(ReadDoubleArraysLikeEncoder(col),
                    Is.EqualTo(new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0 }));
        var shapes = col.GetArrayShapes();
        Assert.That(shapes[0], Is.EqualTo(5));
        Assert.That(shapes[1], Is.EqualTo(2));
    }

    [Test]
    public void DoubleArrayWrapperVaryingDimensionality()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false)!;
        col.AddDoubleArray(new[,] { { 1.0, 2.0 }, { 3.0, 4.0 } });
        table.NextRow();
        col.AddDoubleArray(new[] { 10.0, 20.0, 30.0 });
        table.NextRow();

        Assert.That(col.ValueCount, Is.EqualTo(2));
        var dims = col.GetArrayDims();
        Assert.That(dims[0], Is.EqualTo((byte)2));
        Assert.That(dims[1], Is.EqualTo((byte)1));

        var shapes = col.GetArrayShapes();
        Assert.That(shapes[0], Is.EqualTo(2)); // row 0 dim 0
        Assert.That(shapes[1], Is.EqualTo(2)); // row 0 dim 1
        Assert.That(shapes[2], Is.EqualTo(3)); // row 1 dim 0

        Assert.That(ReadDoubleArraysLikeEncoder(col),
                    Is.EqualTo(new[] { 1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0 }));
    }

    [Test]
    public void LongArrayMultipleRows()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false)!;
        col.AddLongArray(new long[] { 10, 20, 30 });
        table.NextRow();
        col.AddLongArray(new long[] { 40, 50, 60 });
        table.NextRow();
        col.AddLongArray(new long[] { 70, 80, 90 });
        table.NextRow();

        Assert.That(col.ValueCount, Is.EqualTo(3));
        Assert.That(ReadLongArraysLikeEncoder(col),
                    Is.EqualTo(new long[] { 10, 20, 30, 40, 50, 60, 70, 80, 90 }));
        var dims = col.GetArrayDims();
        var shapes = col.GetArrayShapes();
        for (var i = 0; i < 3; i++)
        {
            Assert.That(dims[i], Is.EqualTo((byte)1));
            Assert.That(shapes[i], Is.EqualTo(3));
        }
    }

    [Test]
    public void LongArrayShrinkingSize()
    {
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false)!;
        col.AddLongArray(new long[] { 100, 200, 300, 400 });
        table.NextRow();
        col.AddLongArray(new long[] { 10, 20 });
        table.NextRow();

        Assert.That(col.ValueCount, Is.EqualTo(2));
        Assert.That(ReadLongArraysLikeEncoder(col),
                    Is.EqualTo(new long[] { 100, 200, 300, 400, 10, 20 }));
        var shapes = col.GetArrayShapes();
        Assert.That(shapes[0], Is.EqualTo(4));
        Assert.That(shapes[1], Is.EqualTo(2));
    }

    [Test]
    public void LongArrayWrapperMultipleRows()
    {
        // .NET has no LongArray wrapper — the typed long[] overload is the equivalent.
        var table = new QwpTableBuffer("t");
        var col = table.GetOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false)!;
        col.AddLongArray(new long[] { 10, 20, 30 });
        table.NextRow();
        col.AddLongArray(new long[] { 40, 50, 60 });
        table.NextRow();
        col.AddLongArray(new long[] { 70, 80, 90 });
        table.NextRow();

        Assert.That(col.ValueCount, Is.EqualTo(3));
        Assert.That(ReadLongArraysLikeEncoder(col),
                    Is.EqualTo(new long[] { 10, 20, 30, 40, 50, 60, 70, 80, 90 }));
    }

    /// <summary>
    ///     Walks (dims, shapes, data) to recover per-row arrays, mirroring the way
    ///     <c>QwpColumnWriter</c> emits the wire format. Ports the
    ///     <c>readDoubleArraysLikeEncoder</c> helper from the Java tests.
    /// </summary>
    private static double[] ReadDoubleArraysLikeEncoder(QwpTableBuffer.ColumnBuffer col)
    {
        var dims = col.GetArrayDims();
        var shapes = col.GetArrayShapes();
        var data = col.GetDoubleArrayData();
        var count = col.ValueCount;

        var totalElements = 0;
        var shapeIdx = 0;
        for (var row = 0; row < count; row++)
        {
            var nDims = dims[row];
            var elemCount = 1;
            for (var d = 0; d < nDims; d++) elemCount *= shapes[shapeIdx++];
            totalElements += elemCount;
        }

        var result = new double[totalElements];
        shapeIdx = 0;
        var dataIdx = 0;
        var resultIdx = 0;
        for (var row = 0; row < count; row++)
        {
            var nDims = dims[row];
            var elemCount = 1;
            for (var d = 0; d < nDims; d++) elemCount *= shapes[shapeIdx++];
            for (var i = 0; i < elemCount; i++) result[resultIdx++] = data[dataIdx++];
        }
        return result;
    }

    private static long[] ReadLongArraysLikeEncoder(QwpTableBuffer.ColumnBuffer col)
    {
        var dims = col.GetArrayDims();
        var shapes = col.GetArrayShapes();
        var data = col.GetLongArrayData();
        var count = col.ValueCount;

        var totalElements = 0;
        var shapeIdx = 0;
        for (var row = 0; row < count; row++)
        {
            var nDims = dims[row];
            var elemCount = 1;
            for (var d = 0; d < nDims; d++) elemCount *= shapes[shapeIdx++];
            totalElements += elemCount;
        }

        var result = new long[totalElements];
        shapeIdx = 0;
        var dataIdx = 0;
        var resultIdx = 0;
        for (var row = 0; row < count; row++)
        {
            var nDims = dims[row];
            var elemCount = 1;
            for (var d = 0; d < nDims; d++) elemCount *= shapes[shapeIdx++];
            for (var i = 0; i < elemCount; i++) result[resultIdx++] = data[dataIdx++];
        }
        return result;
    }
}
