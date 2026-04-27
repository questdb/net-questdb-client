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
///     Mirrors <c>WebSocketResponseTest.java</c> on Java main 64b7ee69. Java reaches into
///     native memory via <c>Unsafe</c>; the .NET version uses
///     <see cref="System.ReadOnlySpan{T}"/>/<see cref="System.Span{T}"/> over a
///     <c>byte[]</c> staging buffer.
/// </summary>
[TestFixture]
public class WebSocketResponseTests
{
    [Test]
    public void DurableAckFactory()
    {
        var r = WebSocketResponse.DurableAck("trades", 42);
        Assert.That(r.IsDurableAck, Is.True);
        Assert.That(r.IsSuccess, Is.False);
        Assert.That(r.Sequence, Is.EqualTo(-1L));
        Assert.That(r.TableEntryCount, Is.EqualTo(1));
        Assert.That(r.GetTableName(0), Is.EqualTo("trades"));
        Assert.That(r.GetTableSeqTxn(0), Is.EqualTo(42L));
    }

    [Test]
    public void DurableAckIsStructurallyValid()
    {
        var r = WebSocketResponse.DurableAck("trades", 42);
        var buf = new byte[r.SerializedSize()];
        var written = r.WriteTo(buf);
        Assert.That(WebSocketResponse.IsStructurallyValid(buf.AsSpan(0, written)), Is.True);
    }

    [Test]
    public void DurableAckRoundTripThroughBuffer()
    {
        var r = WebSocketResponse.DurableAck("trades", 42);
        r.AddTableEntry("orders", 99); // multi-entry
        var buf = new byte[r.SerializedSize()];
        var written = r.WriteTo(buf);

        var parsed = new WebSocketResponse();
        Assert.That(parsed.ReadFrom(buf.AsSpan(0, written)), Is.True);
        Assert.That(parsed.IsDurableAck, Is.True);
        Assert.That(parsed.TableEntryCount, Is.EqualTo(2));
        Assert.That(parsed.GetTableName(0), Is.EqualTo("trades"));
        Assert.That(parsed.GetTableSeqTxn(0), Is.EqualTo(42L));
        Assert.That(parsed.GetTableName(1), Is.EqualTo("orders"));
        Assert.That(parsed.GetTableSeqTxn(1), Is.EqualTo(99L));
    }

    [Test]
    public void DurableAckDoesNotCarryErrorMessage()
    {
        var r = WebSocketResponse.DurableAck("trades", 42);
        Assert.That(r.ErrorMessage, Is.Null);
    }

    [Test]
    public void SuccessIsNotDurableAck()
    {
        var r = WebSocketResponse.Success(7);
        Assert.That(r.IsSuccess, Is.True);
        Assert.That(r.IsDurableAck, Is.False);
    }

    [Test]
    public void ErrorIsNotDurableAck()
    {
        var r = WebSocketResponse.Error(7, WebSocketResponse.STATUS_PARSE_ERROR, "boom");
        Assert.That(r.IsDurableAck, Is.False);
        Assert.That(r.IsSuccess, Is.False);
    }

    [Test]
    public void SuccessRoundTripUnchanged()
    {
        var r = WebSocketResponse.Success(123_456L);
        var buf = new byte[r.SerializedSize()];
        var written = r.WriteTo(buf);

        var parsed = new WebSocketResponse();
        Assert.That(parsed.ReadFrom(buf.AsSpan(0, written)), Is.True);
        Assert.That(parsed.IsSuccess, Is.True);
        Assert.That(parsed.Sequence, Is.EqualTo(123_456L));
        Assert.That(parsed.TableEntryCount, Is.EqualTo(0));
    }

    [Test]
    public void SuccessWithTableEntriesRoundTrip()
    {
        var r = WebSocketResponse.Success(7);
        r.AddTableEntry("trades", 100);
        r.AddTableEntry("orders", 200);
        var buf = new byte[r.SerializedSize()];
        var written = r.WriteTo(buf);

        var parsed = new WebSocketResponse();
        Assert.That(parsed.ReadFrom(buf.AsSpan(0, written)), Is.True);
        Assert.That(parsed.IsSuccess, Is.True);
        Assert.That(parsed.Sequence, Is.EqualTo(7L));
        Assert.That(parsed.TableEntryCount, Is.EqualTo(2));
        Assert.That(parsed.GetTableName(0), Is.EqualTo("trades"));
        Assert.That(parsed.GetTableName(1), Is.EqualTo("orders"));
        Assert.That(parsed.GetTableSeqTxn(0), Is.EqualTo(100L));
        Assert.That(parsed.GetTableSeqTxn(1), Is.EqualTo(200L));
    }

    [Test]
    public void EmptyTableNameRejected()
    {
        // Build an OK frame by hand with one table entry whose nameLen=0.
        var buf = new byte[14];
        buf[0] = WebSocketResponse.STATUS_OK;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), 1L);  // sequence
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), 1);  // tableCount = 1
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(11, 2), 0); // nameLen = 0 (invalid)
        // Trailing 8 bytes for seqTxn would be required if nameLen were valid; structural
        // validation must reject before reading them.
        Assert.That(WebSocketResponse.IsStructurallyValid(buf), Is.False);
        Assert.That(new WebSocketResponse().ReadFrom(buf), Is.False);
    }

    [Test]
    public void ErrorRoundTrip()
    {
        var r = WebSocketResponse.Error(123, WebSocketResponse.STATUS_PARSE_ERROR, "syntax");
        var buf = new byte[r.SerializedSize()];
        var written = r.WriteTo(buf);

        var parsed = new WebSocketResponse();
        Assert.That(parsed.ReadFrom(buf.AsSpan(0, written)), Is.True);
        Assert.That(parsed.IsSuccess, Is.False);
        Assert.That(parsed.IsDurableAck, Is.False);
        Assert.That(parsed.Status, Is.EqualTo(WebSocketResponse.STATUS_PARSE_ERROR));
        Assert.That(parsed.Sequence, Is.EqualTo(123L));
        Assert.That(parsed.ErrorMessage, Is.EqualTo("syntax"));
    }

    [Test]
    public void LargeTableCountWithInsufficientPayload()
    {
        var buf = new byte[11];
        buf[0] = WebSocketResponse.STATUS_OK;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), 1L);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), 100); // claims 100 entries
        Assert.That(WebSocketResponse.IsStructurallyValid(buf), Is.False);
        Assert.That(new WebSocketResponse().ReadFrom(buf), Is.False);
    }

    [Test]
    public void TrailingGarbageBytesStatusOk()
    {
        // Valid 11-byte OK frame plus an extra byte that doesn't belong.
        var buf = new byte[12];
        buf[0] = WebSocketResponse.STATUS_OK;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), 1L);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), 0);
        buf[11] = 0xFF;
        Assert.That(WebSocketResponse.IsStructurallyValid(buf), Is.False);
    }

    [Test]
    public void TrailingGarbageBytesWithTableEntries()
    {
        // OK frame with one table entry, plus an extra trailing byte.
        var r = WebSocketResponse.Success(1);
        r.AddTableEntry("t", 5);
        var size = r.SerializedSize();
        var buf = new byte[size + 1];
        r.WriteTo(buf);
        buf[size] = 0xAA; // trailing junk
        Assert.That(WebSocketResponse.IsStructurallyValid(buf), Is.False);
    }

    [Test]
    public void TruncatedTableEntriesStatusOk()
    {
        // Claim 1 table but provide only the nameLen field — no actual name bytes.
        var buf = new byte[13];
        buf[0] = WebSocketResponse.STATUS_OK;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1, 8), 1L);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(9, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(11, 2), 5); // nameLen = 5, no bytes follow
        Assert.That(WebSocketResponse.IsStructurallyValid(buf), Is.False);
    }

    [Test]
    public void TruncatedTableEntriesDurableAck()
    {
        // DurableAck claiming 1 table, missing the seqTxn.
        var name = "t";
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
        var buf = new byte[1 + 2 + 2 + nameBytes.Length]; // status + tableCount + nameLen + name (no seqTxn)
        buf[0] = WebSocketResponse.STATUS_DURABLE_ACK;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(1, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(3, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(buf.AsSpan(5));
        Assert.That(WebSocketResponse.IsStructurallyValid(buf), Is.False);
    }

    [Test]
    public void UnknownStatusByte()
    {
        // Unknown status routes to the error-frame path, which requires 11 bytes minimum.
        var buf = new byte[1] { 0xFF };
        Assert.That(WebSocketResponse.IsStructurallyValid(buf), Is.False);
    }

    [Test]
    public void ZeroLengthPayload()
    {
        Assert.That(WebSocketResponse.IsStructurallyValid(ReadOnlySpan<byte>.Empty), Is.False);
        Assert.That(new WebSocketResponse().ReadFrom(ReadOnlySpan<byte>.Empty), Is.False);
    }

    [Test]
    public void StatusNameForKnownAndUnknown()
    {
        Assert.That(WebSocketResponse.Success(0).GetStatusName(), Is.EqualTo("OK"));
        Assert.That(WebSocketResponse.DurableAck("t", 0).GetStatusName(), Is.EqualTo("DURABLE_ACK"));
        Assert.That(WebSocketResponse.Error(0, WebSocketResponse.STATUS_PARSE_ERROR, "x").GetStatusName(),
                    Is.EqualTo("PARSE_ERROR"));
        // Unknown byte routes through the error-frame parser; just verify the name path.
        var buf = new byte[11];
        buf[0] = 0xEE;
        var r = new WebSocketResponse();
        // Won't parse (zero msgLen), but Status should still be the raw byte we set.
        // Easier: build via Error factory with an arbitrary status byte.
        var customStatus = (byte)0xEE;
        var r2 = WebSocketResponse.Error(0, customStatus, "");
        Assert.That(r2.GetStatusName(), Does.Contain("UNKNOWN(238)"));
    }
}
