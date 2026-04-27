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

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Mirrors <c>SegmentedNativeBufferWriterTest.java</c> on Java main 64b7ee69, renamed
///     to <c>QwpSegmentedBufferWriter</c> in .NET (the underlying chunks are POH-pinned
///     managed arrays). Java records scatter-gather entries via a native (ptr, len) list;
///     .NET records them as <see cref="System.ReadOnlyMemory{T}"/> over <see cref="byte"/>
///     and exposes them through the writer's <c>Segments</c> view.
/// </summary>
[TestFixture]
public class QwpSegmentedBufferWriterTests
{
    [Test]
    public void GetWritableSpanIsChunkLocalAfterByReferenceFlush()
    {
        var writer = new QwpSegmentedBufferWriter();

        writer.PutInt(1);
        writer.PutInt(2);
        Assert.That(writer.Position, Is.EqualTo(8));

        // Before any flush: GetWritableSpan covers the rest of the current chunk.
        Assert.That(writer.GetWritableSpan().Length, Is.EqualTo(writer.Capacity - 8));
        Assert.That(writer.WritableBytes, Is.EqualTo(writer.Capacity - 8));

        // PutBlockOfBytes flushes the current chunk and appends a by-reference segment.
        var external = new byte[16];
        for (var i = 0; i < external.Length; i++) external[i] = (byte)i;
        writer.PutBlockOfBytes(external);

        // After flush: small writes resume in a fresh chunk; Position is global.
        writer.PutInt(3);
        Assert.That(writer.Position, Is.EqualTo(8 + 16 + 4));

        // GetWritableSpan must be chunk-local — its length is bounded by the chunk capacity,
        // not by the global position.
        Assert.That(writer.WritableBytes, Is.GreaterThan(0));
        Assert.That(writer.WritableBytes, Is.LessThanOrEqualTo(writer.Capacity));
        Assert.That(writer.GetWritableSpan().Length, Is.EqualTo(writer.WritableBytes));
    }

    [Test]
    public void PutBlockOfBytesAppendsByReferenceSegment()
    {
        var writer = new QwpSegmentedBufferWriter();
        var external = new byte[] { 10, 20, 30, 40 };

        writer.PutBlockOfBytes(external);

        var segs = writer.Segments;
        Assert.That(segs.Count, Is.EqualTo(1));
        // By-reference: the segment must point at the same backing array, not a copy.
        Assert.That(segs[0].Span.SequenceEqual(external), Is.True);

        // Mutating the source after the call is observable through the segment — the by-reference
        // contract is what enables zero-copy scatter-gather sends downstream.
        external[0] = 99;
        Assert.That(writer.Segments[0].Span[0], Is.EqualTo((byte)99));
    }

    [Test]
    public void SegmentsExposeAccumulatedWritesInOrder()
    {
        var writer = new QwpSegmentedBufferWriter();

        writer.PutInt(unchecked((int)0xAABBCCDD));    // chunk 0 (small)
        writer.PutBlockOfBytes(new byte[] { 1, 2 }); // by-ref segment 1
        writer.PutInt(0x11223344);                    // chunk 2 (small)
        writer.PutBlockOfBytes(new byte[] { 3, 4 }); // by-ref segment 3

        var segs = writer.Segments;
        Assert.That(segs.Count, Is.EqualTo(4));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(segs[0].Span),
                    Is.EqualTo(unchecked((int)0xAABBCCDD)));
        Assert.That(segs[1].Span.SequenceEqual(new byte[] { 1, 2 }), Is.True);
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(segs[2].Span),
                    Is.EqualTo(0x11223344));
        Assert.That(segs[3].Span.SequenceEqual(new byte[] { 3, 4 }), Is.True);
    }

    [Test]
    public void PositionIsGlobalAcrossSegments()
    {
        var writer = new QwpSegmentedBufferWriter();
        writer.PutLong(1L);                            // 8
        writer.PutBlockOfBytes(new byte[16]);          // 16
        writer.PutShort(2);                            // 2
        writer.PutBlockOfBytes(new byte[64]);          // 64
        writer.PutByte(3);                             // 1

        Assert.That(writer.Position, Is.EqualTo(8 + 16 + 2 + 64 + 1));
    }

    [Test]
    public void PatchIntAcrossFlushedSegmentBoundaryThrows()
    {
        var writer = new QwpSegmentedBufferWriter();
        writer.PutInt(0);                              // chunk 0: placeholder at offset 0
        writer.PutInt(0xCAFE);                          // chunk 0 still
        // Patch within the live chunk is OK.
        writer.PatchInt(0, 0x1234);

        // Flush the current chunk by appending a by-reference block.
        writer.PutBlockOfBytes(new byte[8]);

        // Now offset 0 lives in the flushed segment, not the live chunk — patching must fail.
        Assert.That(() => writer.PatchInt(0, 0x9999),
                    Throws.TypeOf<InvalidOperationException>()
                          .With.Message.Contains("flushed"));
    }

    [Test]
    public void PatchIntRejectsNegativeOffset()
    {
        var writer = new QwpSegmentedBufferWriter();
        writer.PutInt(0xCAFE);
        Assert.That(() => writer.PatchInt(-1, 0),
                    Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ResetReusesUnderlyingChunks()
    {
        var writer = new QwpSegmentedBufferWriter();
        writer.PutInt(1);
        writer.PutBlockOfBytes(new byte[8]);
        writer.PutInt(2);
        Assert.That(writer.Position, Is.GreaterThan(0));
        Assert.That(writer.Segments.Count, Is.GreaterThan(0));

        writer.Reset();
        Assert.That(writer.Position, Is.EqualTo(0));
        Assert.That(writer.Segments.Count, Is.EqualTo(0));

        // Writing again after reset must succeed and produce a fresh segment list.
        writer.PutInt(0x12345678);
        Assert.That(writer.Position, Is.EqualTo(4));
        Assert.That(writer.Segments.Count, Is.EqualTo(1));
        Assert.That(BinaryPrimitives.ReadInt32LittleEndian(writer.Segments[0].Span),
                    Is.EqualTo(0x12345678));
    }

    [Test]
    [Ignore("Awaiting PR 3 (QwpTableBuffer) + PR 6 (QwpColumnWriter): cross-component test for Gorilla encoding via segmented buffer.")]
    public void GorillaEncodingViaSegmentedBufferRoundTrips() { }
}
