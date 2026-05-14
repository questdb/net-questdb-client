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
using QuestDB.Utils;

namespace net_questdb_client_tests.Qwp;

[TestFixture]
public class QwpResponseTests
{
    [Test]
    public void StatusCodes_HaveStableByteValues()
    {
        Assert.That((byte)QwpStatusCode.Ok,             Is.EqualTo((byte)0x00));
        Assert.That((byte)QwpStatusCode.DurableAck,     Is.EqualTo((byte)0x02));
        Assert.That((byte)QwpStatusCode.SchemaMismatch, Is.EqualTo((byte)0x03));
        Assert.That((byte)QwpStatusCode.ParseError,     Is.EqualTo((byte)0x05));
        Assert.That((byte)QwpStatusCode.InternalError,  Is.EqualTo((byte)0x06));
        Assert.That((byte)QwpStatusCode.SecurityError,  Is.EqualTo((byte)0x08));
        Assert.That((byte)QwpStatusCode.WriteError,     Is.EqualTo((byte)0x09));
        Assert.That((byte)QwpStatusCode.Cancelled,      Is.EqualTo((byte)0x0A));
        Assert.That((byte)QwpStatusCode.LimitExceeded,  Is.EqualTo((byte)0x0B));
    }

    [Test]
    public void Parse_OkResponse_ReturnsSequenceAndEmptyMessage()
    {
        var frame = BuildOk(sequence: 42L);

        var r = QwpResponse.Parse(frame);

        Assert.That(r.IsOk, Is.True);
        Assert.That(r.Status, Is.EqualTo(QwpStatusCode.Ok));
        Assert.That(r.Sequence, Is.EqualTo(42L));
        Assert.That(r.Message, Is.EqualTo(string.Empty));
    }

    [Test]
    public void Parse_OkResponse_NegativeSequence_RoundTrips()
    {
        // -1 sentinel is what STATUS_DURABLE_ACK uses, but plain OK can technically carry it too.
        var frame = BuildOk(sequence: -1L);

        var r = QwpResponse.Parse(frame);

        Assert.That(r.Sequence, Is.EqualTo(-1L));
    }

    [Test]
    public void Parse_OkResponse_TooShort_Throws()
    {
        var frame = new byte[5]; // status + 4 bytes (truncated)
        frame[0] = (byte)QwpStatusCode.Ok;

        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_OkResponse_TrailingGarbage_Throws()
    {
        var frame = new byte[QwpConstants.OffsetTableCountInOkAck + 1];
        // OK status + sequence (zero) + extra byte.
        frame[0] = (byte)QwpStatusCode.Ok;

        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_ErrorResponse_DecodesMessage()
    {
        var msg = "table not found: trades";
        var frame = BuildError(QwpStatusCode.WriteError, sequence: 7L, message: msg);

        var r = QwpResponse.Parse(frame);

        Assert.That(r.Status, Is.EqualTo(QwpStatusCode.WriteError));
        Assert.That(r.Sequence, Is.EqualTo(7L));
        Assert.That(r.Message, Is.EqualTo(msg));
        Assert.That(r.IsOk, Is.False);
    }

    [Test]
    public void Parse_ErrorResponse_EmptyMessage_DecodesCleanly()
    {
        var frame = BuildError(QwpStatusCode.ParseError, sequence: 0L, message: string.Empty);

        var r = QwpResponse.Parse(frame);

        Assert.That(r.Status, Is.EqualTo(QwpStatusCode.ParseError));
        Assert.That(r.Message, Is.EqualTo(string.Empty));
    }

    [Test]
    public void Parse_ErrorResponse_HeaderTruncated_Throws()
    {
        var frame = new byte[10]; // 1 byte short of the 11-byte header.
        frame[0] = (byte)QwpStatusCode.WriteError;

        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_ErrorResponse_MessageOversized_Throws()
    {
        var frame = new byte[QwpConstants.ErrorAckHeaderSize];
        frame[0] = (byte)QwpStatusCode.ParseError;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), QwpConstants.MaxErrorMessageBytes + 1);

        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_ErrorResponse_MsgLenLargerThanFrame_Throws()
    {
        // Header claims 100-byte message but frame only carries 10 bytes of message.
        var frame = new byte[QwpConstants.ErrorAckHeaderSize + 10];
        frame[0] = (byte)QwpStatusCode.WriteError;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), 100);

        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_ErrorResponse_TrailingGarbage_Throws()
    {
        // Header says 0-byte message but frame has trailing junk.
        var frame = new byte[QwpConstants.ErrorAckHeaderSize + 1];
        frame[0] = (byte)QwpStatusCode.InternalError;
        // msg_len = 0 by default zero-init.
        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_DurableAck_TruncatedHeader_Throws()
    {
        var frame = new[] { (byte)QwpStatusCode.DurableAck };
        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_DurableAck_NoEntries_DecodesCleanly()
    {
        // status (1) + tableCount=0 (2) = 3 bytes.
        var frame = new byte[3];
        frame[0] = (byte)QwpStatusCode.DurableAck;
        // tableCount little-endian zero (already zeroed).

        var r = QwpResponse.Parse(frame);

        Assert.That(r.IsDurableAck);
        Assert.That(r.Sequence, Is.EqualTo(-1L));
        Assert.That(r.TableEntries, Is.Empty);
    }

    [Test]
    public void Parse_DurableAck_WithEntries_DecodesPerTableSeqTxns()
    {
        var frame = BuildDurableAck(("trades", 7L), ("orders", 12L));

        var r = QwpResponse.Parse(frame);

        Assert.That(r.IsDurableAck);
        Assert.That(r.Sequence, Is.EqualTo(-1L));
        Assert.That(r.TableEntries.Count, Is.EqualTo(2));
        Assert.That(r.TableEntries[0].TableName, Is.EqualTo("trades"));
        Assert.That(r.TableEntries[0].SeqTxn, Is.EqualTo(7L));
        Assert.That(r.TableEntries[1].TableName, Is.EqualTo("orders"));
        Assert.That(r.TableEntries[1].SeqTxn, Is.EqualTo(12L));
    }

    [Test]
    public void Parse_DurableAck_EmptyTableName_Throws()
    {
        // Manually build a frame with nameLen=0 to verify rejection.
        var frame = new byte[] {
            (byte)QwpStatusCode.DurableAck,
            0x01, 0x00, // tableCount = 1
            0x00, 0x00, // nameLen = 0 (invalid)
            0, 0, 0, 0, 0, 0, 0, 0, // seqTxn
        };
        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    [Test]
    public void Parse_DurableAck_TrailingGarbage_Throws()
    {
        var frame = BuildDurableAck(("trades", 7L));
        var bloated = new byte[frame.Length + 1];
        Array.Copy(frame, bloated, frame.Length);
        Assert.Throws<IngressError>(() => QwpResponse.Parse(bloated));
    }

    [Test]
    public void Parse_OkResponse_WithPerTableEntries_DecodesCleanly()
    {
        var frame = BuildOkWithEntries(sequence: 42L, ("trades", 5L), ("orders", 8L));

        var r = QwpResponse.Parse(frame);

        Assert.That(r.IsOk);
        Assert.That(r.Sequence, Is.EqualTo(42L));
        Assert.That(r.TableEntries.Count, Is.EqualTo(2));
        Assert.That(r.TableEntries[0].TableName, Is.EqualTo("trades"));
        Assert.That(r.TableEntries[0].SeqTxn, Is.EqualTo(5L));
    }

    [Test]
    public void Parse_OkResponse_WithZeroEntriesViaTableCount_IsAccepted()
    {
        // status (1) + sequence (8) + tableCount=0 (2) = 11 bytes; valid extended OK with no entries.
        var frame = new byte[QwpConstants.OffsetTableCountInOkAck + 2];
        frame[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), 99L);
        // tableCount left at zero.

        var r = QwpResponse.Parse(frame);
        Assert.That(r.Sequence, Is.EqualTo(99L));
        Assert.That(r.TableEntries, Is.Empty);
    }

    [Test]
    public void Parse_UnknownStatusCode_ParsesAsErrorFrame()
    {
        var frame = new byte[QwpConstants.ErrorAckHeaderSize];
        frame[0] = 0xFE;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), 7L);

        var r = QwpResponse.Parse(frame);
        Assert.That((byte)r.Status, Is.EqualTo(0xFE));
        Assert.That(r.Sequence, Is.EqualTo(7L));
        Assert.That(r.IsOk, Is.False);
        Assert.That(r.IsDurableAck, Is.False);
    }

    [Test]
    public void Parse_EmptyFrame_Throws()
    {
        Assert.Throws<IngressError>(() => QwpResponse.Parse(ReadOnlySpan<byte>.Empty));
    }

    [Test]
    public void ToException_OnError_CarriesStatusAndSequence()
    {
        var r = new QwpResponse(QwpStatusCode.SchemaMismatch, 42L, "type clash", Array.Empty<QwpTableEntry>());
        var ex = r.ToException();

        Assert.That(ex.Status, Is.EqualTo(QwpStatusCode.SchemaMismatch));
        Assert.That(ex.Sequence, Is.EqualTo(42L));
        Assert.That(ex.Message, Does.Contain("type clash"));
    }

    [Test]
    public void RoundTrip_AllErrorStatusCodes()
    {
        foreach (var status in new[]
                 {
                     QwpStatusCode.SchemaMismatch,
                     QwpStatusCode.ParseError,
                     QwpStatusCode.InternalError,
                     QwpStatusCode.SecurityError,
                     QwpStatusCode.WriteError,
                 })
        {
            var msg = $"error from {status}";
            var frame = BuildError(status, sequence: 99L, message: msg);
            var r = QwpResponse.Parse(frame);

            Assert.That(r.Status, Is.EqualTo(status));
            Assert.That(r.Sequence, Is.EqualTo(99L));
            Assert.That(r.Message, Is.EqualTo(msg));
        }
    }

    private static byte[] BuildOk(long sequence)
    {
        var bytes = new byte[QwpConstants.OffsetTableCountInOkAck + 2];
        bytes[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(9, 2), 0);
        return bytes;
    }

    private static byte[] BuildOkWithEntries(long sequence, params (string Name, long SeqTxn)[] entries)
    {
        // status (1) + sequence (8) + tableCount (2) + entries
        var size = QwpConstants.OffsetTableCountInOkAck + 2;
        foreach (var e in entries)
        {
            size += 2 + Encoding.UTF8.GetByteCount(e.Name) + 8;
        }

        var frame = new byte[size];
        frame[0] = (byte)QwpStatusCode.Ok;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCountInOkAck, 2), (ushort)entries.Length);

        var pos = QwpConstants.OffsetTableCountInOkAck + 2;
        foreach (var e in entries)
        {
            var nameBytes = Encoding.UTF8.GetBytes(e.Name);
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(pos, 2), (ushort)nameBytes.Length);
            pos += 2;
            nameBytes.CopyTo(frame, pos);
            pos += nameBytes.Length;
            BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(pos, 8), e.SeqTxn);
            pos += 8;
        }

        return frame;
    }

    private static byte[] BuildDurableAck(params (string Name, long SeqTxn)[] entries)
    {
        // status (1) + tableCount (2) + entries
        var size = 3;
        foreach (var e in entries)
        {
            size += 2 + Encoding.UTF8.GetByteCount(e.Name) + 8;
        }

        var frame = new byte[size];
        frame[0] = (byte)QwpStatusCode.DurableAck;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(1, 2), (ushort)entries.Length);

        var pos = 3;
        foreach (var e in entries)
        {
            var nameBytes = Encoding.UTF8.GetBytes(e.Name);
            BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(pos, 2), (ushort)nameBytes.Length);
            pos += 2;
            nameBytes.CopyTo(frame, pos);
            pos += nameBytes.Length;
            BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(pos, 8), e.SeqTxn);
            pos += 8;
        }

        return frame;
    }

    [Test]
    public void Parse_ErrorResponse_InvalidUtf8Message_ThrowsProtocolError()
    {
        // 0xC3 0x28 is a malformed two-byte sequence. Per-table names already throw on bad UTF-8;
        // the error-message path must match (otherwise a buggy server smuggles U+FFFD into logs).
        var msgBytes = new byte[] { 0xC3, 0x28 };
        var frame = new byte[QwpConstants.ErrorAckHeaderSize + msgBytes.Length];
        frame[0] = (byte)QwpStatusCode.WriteError;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), 5L);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), (ushort)msgBytes.Length);
        msgBytes.CopyTo(frame, QwpConstants.ErrorAckHeaderSize);

        var ex = Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.InvalidUtf8));
    }

    [Test]
    public void Parse_IngestResponse_CancelledStatus_RejectedAsProtocolError()
    {
        // Cancelled (0x0A) is egress-only (QUERY_ERROR). It must not appear on the ingest reply
        // channel; rejecting it loudly catches server bugs instead of masking them as generic errors.
        var frame = new byte[QwpConstants.ErrorAckHeaderSize];
        frame[0] = (byte)QwpStatusCode.Cancelled;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), 0L);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), 0);

        var ex = Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolVersionError));
    }

    [Test]
    public void Parse_IngestResponse_LimitExceededStatus_RejectedAsProtocolError()
    {
        var frame = new byte[QwpConstants.ErrorAckHeaderSize];
        frame[0] = (byte)QwpStatusCode.LimitExceeded;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), 0L);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), 0);

        var ex = Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
        Assert.That(ex!.code, Is.EqualTo(ErrorCode.ProtocolVersionError));
    }

    [Test]
    public void Parse_IngestResponse_GenuinelyUnknownStatusByte_StillParsedForwardCompat()
    {
        // Truly unknown bytes (not the known-egress-only 0x0A/0x0B) must keep flowing through
        // ParseError so future server-side status codes don't need a client update — the engine's
        // error classifier maps them to category=Unknown which always halts, no data corruption risk.
        var frame = new byte[QwpConstants.ErrorAckHeaderSize];
        frame[0] = 0x7F;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), 0L);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), 0);

        var r = QwpResponse.Parse(frame);
        Assert.That((byte)r.Status, Is.EqualTo(0x7F));
    }

    [Test]
    public void Parse_DurableAck_InvalidUtf8TableName_Throws()
    {
        // status (1) + tableCount (2) + entry: nameLen (2) + name (2 bytes invalid UTF-8) + seqTxn (8)
        var nameBytes = new byte[] { 0xC3, 0x28 };
        var frame = new byte[3 + 2 + nameBytes.Length + 8];
        frame[0] = (byte)QwpStatusCode.DurableAck;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(1, 2), 1);
        var pos = 3;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(pos, 2), (ushort)nameBytes.Length);
        pos += 2;
        nameBytes.CopyTo(frame, pos);
        pos += nameBytes.Length;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(pos, 8), 7L);

        Assert.Throws<IngressError>(() => QwpResponse.Parse(frame));
    }

    private static byte[] BuildError(QwpStatusCode status, long sequence, string message)
    {
        var msgBytes = Encoding.UTF8.GetBytes(message);
        var frame = new byte[QwpConstants.ErrorAckHeaderSize + msgBytes.Length];
        frame[0] = (byte)status;
        BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1, 8), sequence);
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(9, 2), (ushort)msgBytes.Length);
        msgBytes.CopyTo(frame, QwpConstants.ErrorAckHeaderSize);
        return frame;
    }
}
