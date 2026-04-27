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

using NUnit.Framework;

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
    [Test] public void GetExistingColumnReturnsNullWithoutCreatingColumn() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetExistingColumnReturnsOrderedColumnsAcrossRows() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetExistingColumnReturnsOutOfOrderColumns() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetExistingColumnTypeMismatchOnHashPathThrows() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetExistingColumnTypeMismatchOnOrderedPathThrows() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetExistingColumnWorksAfterReset() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetExistingColumnWorksForLateAddedColumnAfterCancelRow() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetOrCreateColumnConflictingTypeFastPath() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetOrCreateColumnConflictingTypeSlowPath() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void GetOrCreateColumnThrowsWhenExceedingMaxColumnCount() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void NextRowWithPreparedMissingColumnsPadsListedColumns() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void NonAsciiColumnNameCaseInsensitive() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void CancelRowTruncatesLateAddedColumn() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void CancelRowTruncatesLateAddedColumnWhenSizeEqualsRowCount() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void RetainInProgressRowFastClearsUnstagedNullableColumn() => Assert.Inconclusive(AwaitingSkeleton);
    [Test] public void CancelRowResetsGeohashPrecisionOnLateAddedColumn() => Assert.Inconclusive(AwaitingSkeleton);

    // ---- Symbol column support (PR 3b) ----
    [Test] public void AddSymbolNullOnNonNullableColumn() => Assert.Inconclusive(AwaitingSymbol);
    [Test] public void AddSymbolUtf8CancelRowRewindsDictionary() => Assert.Inconclusive(AwaitingSymbol);
    [Test] public void AddSymbolUtf8RejectsInvalidUtf8() => Assert.Inconclusive(AwaitingSymbol);
    [Test] public void AddSymbolUtf8ReusesExistingDictionaryEntry() => Assert.Inconclusive(AwaitingSymbol);
    [Test] public void AddSymbolWithGlobalIdStoresOnlyGlobalIds() => Assert.Inconclusive(AwaitingSymbol);
    [Test] public void CancelRowResetsSymbolDictOnLateAddedColumn() => Assert.Inconclusive(AwaitingSymbol);
    [Test] public void CancelRowRetainsGlobalSymbolIdWithoutLocalDictionary() => Assert.Inconclusive(AwaitingSymbol);

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
