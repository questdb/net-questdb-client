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
using QuestDB.Qwp.Egress;

namespace net_questdb_client_tests.Qwp.Egress;

/// <summary>Mirrors <c>QwpBatchBufferTest.java</c> on Java main 64b7ee69.</summary>
[TestFixture]
public class QwpBatchBufferTests
{
    [Test]
    public void NegativeInitialCapacityRejected()
    {
        Assert.That(() => new QwpBatchBuffer(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ZeroInitialCapacityIsValidAndCanGrow()
    {
        var buf = new QwpBatchBuffer(0);
        Assert.That(buf.Capacity, Is.EqualTo(0));
        var src = new byte[] { 1, 2, 3, 4 };
        buf.CopyFromPayload(src);
        Assert.That(buf.PayloadLength, Is.EqualTo(4));
        Assert.That(buf.Capacity, Is.GreaterThanOrEqualTo(4));
        Assert.That(buf.Payload.SequenceEqual(src), Is.True);
    }

    [Test]
    public void CopyFromPayloadStoresBytes()
    {
        var buf = new QwpBatchBuffer(64);
        var src = new byte[] { 0xAA, 0xBB, 0xCC };
        buf.CopyFromPayload(src);
        Assert.That(buf.PayloadLength, Is.EqualTo(3));
        Assert.That(buf.Payload.SequenceEqual(src), Is.True);
    }

    [Test]
    public void CopyFromPayloadGrowsScratch()
    {
        var buf = new QwpBatchBuffer(8);
        var src = new byte[100];
        for (var i = 0; i < src.Length; i++) src[i] = (byte)(i & 0xFF);
        buf.CopyFromPayload(src);
        Assert.That(buf.Capacity, Is.GreaterThanOrEqualTo(100));
        Assert.That(buf.Payload.SequenceEqual(src), Is.True);
    }

    [Test]
    public void CopyFromPayloadReplacesPriorContents()
    {
        var buf = new QwpBatchBuffer(64);
        buf.CopyFromPayload(new byte[] { 1, 2, 3, 4 });
        buf.CopyFromPayload(new byte[] { 9, 9 });
        Assert.That(buf.PayloadLength, Is.EqualTo(2));
        Assert.That(buf.Payload.ToArray(), Is.EqualTo(new byte[] { 9, 9 }));
    }

    [Test]
    public void PayloadMemoryReflectsCurrentLength()
    {
        var buf = new QwpBatchBuffer(64);
        buf.CopyFromPayload(new byte[] { 1, 2, 3 });
        Assert.That(buf.PayloadMemory.Length, Is.EqualTo(3));
        Assert.That(buf.PayloadMemory.Span.SequenceEqual(buf.Payload), Is.True);
    }

    [Test]
    public void EmptyPayloadIsLengthZero()
    {
        var buf = new QwpBatchBuffer(64);
        buf.CopyFromPayload(ReadOnlySpan<byte>.Empty);
        Assert.That(buf.PayloadLength, Is.EqualTo(0));
        Assert.That(buf.Payload.IsEmpty, Is.True);
    }

    [Test]
    public void LayoutPoolStartsEmpty()
    {
        var buf = new QwpBatchBuffer(64);
        Assert.That(buf.LayoutPool, Is.Empty);
        Assert.That(buf.Batch, Is.Not.Null);
    }

    [Test]
    public void CloseDrainsLayoutPool()
    {
        var buf = new QwpBatchBuffer(64);
        buf.LayoutPool.Add(new QwpColumnLayout());
        buf.LayoutPool.Add(new QwpColumnLayout());
        buf.Close();
        Assert.That(buf.LayoutPool, Is.Empty);
    }
}
