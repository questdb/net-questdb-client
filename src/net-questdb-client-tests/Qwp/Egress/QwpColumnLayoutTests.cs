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
using QuestDB.Qwp;
using QuestDB.Qwp.Egress;

namespace net_questdb_client_tests.Qwp.Egress;

/// <summary>Mirrors <c>QwpColumnLayoutTest.java</c> on Java main 64b7ee69.</summary>
[TestFixture]
public class QwpColumnLayoutTests
{
    [Test]
    public void DenseIndexReturnsRowWhenNoNullBitmap()
    {
        var layout = new QwpColumnLayout { NullBitmapOffset = -1 };
        Assert.That(layout.DenseIndex(0), Is.EqualTo(0));
        Assert.That(layout.DenseIndex(7), Is.EqualTo(7));
        Assert.That(layout.DenseIndex(123), Is.EqualTo(123));
    }

    [Test]
    public void DenseIndexConsultsNonNullIdxWhenBitmapPresent()
    {
        var layout = new QwpColumnLayout
        {
            NullBitmapOffset = 100,
            NonNullIdx = new[] { 0, -1, 1, 2, -1, 3 },
        };
        Assert.That(layout.DenseIndex(0), Is.EqualTo(0));
        Assert.That(layout.DenseIndex(2), Is.EqualTo(1));
        Assert.That(layout.DenseIndex(3), Is.EqualTo(2));
        Assert.That(layout.DenseIndex(5), Is.EqualTo(3));
    }

    [Test]
    public void ClearWipesPerBatchStateButKeepsSymbolCache()
    {
        var layout = new QwpColumnLayout
        {
            Info = new QwpEgressColumnInfo { Name = "x", WireType = 0x05 },
            ValuesOffset = 100,
            NullBitmapOffset = 200,
            NonNullCount = 10,
            StringBytesOffset = 50,
            SymbolDictHeapOffset = 60,
            SymbolDictEntriesOffset = 70,
            SymbolDictSize = 5,
            NextOffset = 999,
        };
        layout.SymbolStringCache.Add("AAPL");
        layout.SymbolStringCache.Add("GOOG");

        layout.Clear();

        Assert.That(layout.Info, Is.Null);
        Assert.That(layout.ValuesOffset, Is.EqualTo(0));
        Assert.That(layout.NullBitmapOffset, Is.EqualTo(-1));
        Assert.That(layout.NonNullCount, Is.EqualTo(0));
        Assert.That(layout.StringBytesOffset, Is.EqualTo(0));
        Assert.That(layout.SymbolDictHeapOffset, Is.EqualTo(0));
        Assert.That(layout.SymbolDictEntriesOffset, Is.EqualTo(0));
        Assert.That(layout.SymbolDictSize, Is.EqualTo(0));
        Assert.That(layout.NextOffset, Is.EqualTo(0));
        // Symbol cache survives Clear — invalidation is via SymbolCacheVersion.
        Assert.That(layout.SymbolStringCache, Is.EqualTo(new[] { "AAPL", "GOOG" }));
    }

    [Test]
    public void EnsureOwnedEntriesAllocatesAtLeastMinimum()
    {
        var layout = new QwpColumnLayout();
        var buf = layout.EnsureOwnedEntries(8);
        Assert.That(buf.Length, Is.GreaterThanOrEqualTo(64), "first call rounds up to a sensible floor");
        Assert.That(layout.OwnedEntries, Is.SameAs(buf));
    }

    [Test]
    public void EnsureOwnedEntriesGrowsByDoubling()
    {
        var layout = new QwpColumnLayout();
        var first = layout.EnsureOwnedEntries(64);
        var firstCap = first.Length;
        var second = layout.EnsureOwnedEntries(firstCap + 1);
        Assert.That(second.Length, Is.GreaterThanOrEqualTo(firstCap * 2));
    }

    [Test]
    public void EnsureOwnedEntriesNoOpWhenSufficient()
    {
        var layout = new QwpColumnLayout();
        var first = layout.EnsureOwnedEntries(128);
        var second = layout.EnsureOwnedEntries(64);
        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void EnsureTimestampDecodeBufferGrows()
    {
        var layout = new QwpColumnLayout();
        var first = layout.EnsureTimestampDecodeBuffer(64);
        Assert.That(first.Length, Is.GreaterThanOrEqualTo(64));
        var second = layout.EnsureTimestampDecodeBuffer(first.Length + 1);
        Assert.That(second.Length, Is.GreaterThan(first.Length));
    }

    [Test]
    public void CloseReleasesBuffers()
    {
        var layout = new QwpColumnLayout();
        layout.EnsureOwnedEntries(64);
        layout.EnsureTimestampDecodeBuffer(64);
        layout.SymbolStringCache.Add("X");
        layout.Close();
        Assert.That(layout.OwnedEntries, Is.Null);
        Assert.That(layout.TimestampDecodeBuffer, Is.Null);
        Assert.That(layout.SymbolStringCache, Is.Empty);
    }

    [Test]
    public void DefaultsAreSane()
    {
        var layout = new QwpColumnLayout();
        Assert.That(layout.NullBitmapOffset, Is.EqualTo(-1));
        Assert.That(layout.Info, Is.Null);
        Assert.That(layout.SymbolStringCache, Is.Empty);
        Assert.That(layout.OwnedEntries, Is.Null);
    }

    [Test]
    public void ColumnInfoCarriesWireTypeAndExtras()
    {
        var info = new QwpEgressColumnInfo
        {
            Name = "ts",
            WireType = QwpConstants.TYPE_TIMESTAMP,
            DecimalScale = 4,
            GeohashPrecisionBits = 30,
            TimestampEncoding = 0x01,
        };
        Assert.That(info.Name, Is.EqualTo("ts"));
        Assert.That(info.WireType, Is.EqualTo(QwpConstants.TYPE_TIMESTAMP));
        Assert.That(info.DecimalScale, Is.EqualTo((sbyte)4));
        Assert.That(info.GeohashPrecisionBits, Is.EqualTo(30));
        Assert.That(info.TimestampEncoding, Is.EqualTo((byte)0x01));

        info.Reset();
        Assert.That(info.Name, Is.EqualTo(string.Empty));
        Assert.That(info.WireType, Is.EqualTo((byte)0));
        Assert.That(info.DecimalScale, Is.EqualTo((sbyte)-1));
        Assert.That(info.GeohashPrecisionBits, Is.EqualTo(-1));
        Assert.That(info.TimestampEncoding, Is.EqualTo((byte)0xFF));
    }
}
