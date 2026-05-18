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
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Per-table seqTxn entry carried in OK and durable-ACK frames when
///     <c>request_durable_ack</c> is enabled.
/// </summary>
internal readonly record struct QwpTableEntry(string TableName, long SeqTxn);

/// <summary>
///     A parsed QWP server response.
/// </summary>
/// <remarks>
///     Three on-wire shapes are supported:
///     <list type="bullet">
///         <item>
///             <b>OK</b> — 11+ bytes: <c>uint8 status (0x00)</c> + <c>int64 sequence</c> +
///             <c>uint16 tableCount</c> + <c>tableCount</c> repeating
///             <c>[uint16 nameLen + name + int64 seqTxn]</c> entries. The sequence is the
///             cumulative ACK watermark; every batch with seq ≤ <see cref="Sequence" /> has
///             succeeded.
///         </item>
///         <item>
///             <b>Durable-ACK</b> — <c>uint8 status (0x02)</c> + <c>uint16 tableCount</c> + entries.
///             Carries no batch sequence; <see cref="Sequence" /> is set to <c>-1</c>.
///         </item>
///         <item>
///             <b>Error</b> — 11 + <c>msg_len</c> bytes: status + sequence + uint16 msg_len + UTF-8
///             message. The decoded <see cref="Message" /> is truncated to 1024 bytes (with a
///             trailing ellipsis) when the server sends a longer one; the frame still parses as
///             a normal recoverable error.
///         </item>
///     </list>
///     <para />
///     <b>Strict validation</b>: the parser rejects empty table names, lying lengths, and any
///     trailing bytes after the last entry.
/// </remarks>
internal readonly struct QwpResponse
{
    private static readonly QwpTableEntry[] EmptyEntries = Array.Empty<QwpTableEntry>();
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public QwpResponse(QwpStatusCode status, long sequence, string message, QwpTableEntry[] tableEntries)
    {
        Status = status;
        Sequence = sequence;
        Message = message;
        TableEntries = tableEntries;
    }

    /// <summary>The status code from the response frame.</summary>
    public QwpStatusCode Status { get; }

    /// <summary>
    ///     For OK and error frames, the request sequence number being acknowledged. Cumulative for
    ///     OK; specific to the failing batch for errors. <c>-1</c> for durable-ACK frames.
    /// </summary>
    public long Sequence { get; }

    /// <summary>UTF-8 decoded message text. Empty for OK and durable-ACK frames.</summary>
    public string Message { get; }

    /// <summary>
    ///     Per-table seqTxn watermarks when present. Empty for OK frames with <c>tableCount=0</c>
    ///     and for error frames.
    /// </summary>
    public IReadOnlyList<QwpTableEntry> TableEntries { get; }

    /// <summary>True when this is a successful response.</summary>
    public bool IsOk => Status == QwpStatusCode.Ok;

    /// <summary>True when this carries durable-upload watermarks.</summary>
    public bool IsDurableAck => Status == QwpStatusCode.DurableAck;

    /// <summary>Builds a <see cref="QwpException" /> describing this error response.</summary>
    public QwpException ToException()
    {
        return new QwpException(Status, Sequence, Message);
    }

    /// <summary>Parses a single QWP response frame from <paramref name="frame" />.</summary>
    /// <exception cref="IngressError">If the frame is malformed or carries an unsupported status.</exception>
    public static QwpResponse Parse(ReadOnlySpan<byte> frame)
    {
        if (frame.Length < 1)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError, "QWP response frame is empty");
        }

        var statusByte = frame[0];
        var status = (QwpStatusCode)statusByte;

        if (status == QwpStatusCode.Ok)
        {
            return ParseOk(frame);
        }

        if (status == QwpStatusCode.DurableAck)
        {
            return ParseDurableAck(frame);
        }

        if (IsKnownEgressOnlyStatus(status))
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP ingest response carries egress-only status 0x{statusByte:X2}");
        }

        return ParseError(status, frame);
    }

    // Cancelled (0x0A) / LimitExceeded (0x0B) are defined for egress QUERY_ERROR; they must not
    // appear on the ingest reply channel. Other undefined bytes are left through so future status
    // codes the server adds are forward-compatible — the engine routes them via the Unknown
    // category which always halts (see QwpErrorClassifier).
    private static bool IsKnownEgressOnlyStatus(QwpStatusCode status)
    {
        return status is QwpStatusCode.Cancelled or QwpStatusCode.LimitExceeded;
    }

    private static QwpResponse ParseOk(ReadOnlySpan<byte> frame)
    {
        // Spec: status (1) + sequence (8) + tableCount (2) + entries. Minimum 11 bytes; matches Java.
        const int headerSize = QwpConstants.OffsetTableCountInOkAck + 2;
        if (frame.Length < headerSize)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP OK response has invalid size {frame.Length}; must be ≥ {headerSize}");
        }

        var sequence = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(1, 8));
        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(QwpConstants.OffsetTableCountInOkAck, 2));
        var entries = ParseTableEntries(frame.Slice(headerSize), tableCount);
        return new QwpResponse(QwpStatusCode.Ok, sequence, string.Empty, entries);
    }

    private static QwpResponse ParseDurableAck(ReadOnlySpan<byte> frame)
    {
        // Durable-ACK: status (1) + tableCount (2) + entries. No batch sequence.
        const int headerSize = 3;
        if (frame.Length < headerSize)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP durable-ACK response truncated: got {frame.Length} bytes, header alone needs {headerSize}");
        }

        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(1, 2));
        var entries = ParseTableEntries(frame.Slice(headerSize), tableCount);
        return new QwpResponse(QwpStatusCode.DurableAck, sequence: -1L, string.Empty, entries);
    }

    private static QwpResponse ParseError(QwpStatusCode status, ReadOnlySpan<byte> frame)
    {
        if (frame.Length < QwpConstants.ErrorAckHeaderSize)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP error response truncated: got {frame.Length} bytes, header alone needs {QwpConstants.ErrorAckHeaderSize}");
        }

        var seq = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(1, 8));
        var msgLen = BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(9, 2));

        var expectedTotal = QwpConstants.ErrorAckHeaderSize + msgLen;
        if (frame.Length != expectedTotal)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP error response size mismatch: header+message expects {expectedTotal} bytes, got {frame.Length}");
        }

        string message;
        if (msgLen == 0)
        {
            message = string.Empty;
        }
        else
        {
            // An over-cap message is a legitimate (recoverable) server error, not a protocol
            // violation. Decode only the first MaxErrorMessageBytes; framing already consumed
            // the full msgLen above.
            var truncated = msgLen > QwpConstants.MaxErrorMessageBytes;
            var msgBytes = frame.Slice(QwpConstants.ErrorAckHeaderSize, (int)msgLen);
            if (truncated)
            {
                // Back the cut off any UTF-8 continuation bytes so the slice ends on a whole
                // code point — otherwise strict decoding would misreport a clean truncation as
                // an invalid-UTF-8 protocol error.
                var decodeLen = QwpConstants.MaxErrorMessageBytes;
                while (decodeLen > 0 && (msgBytes[decodeLen] & 0xC0) == 0x80)
                {
                    decodeLen--;
                }
                msgBytes = msgBytes.Slice(0, decodeLen);
            }

            try
            {
                message = StrictUtf8.GetString(msgBytes);
            }
            catch (DecoderFallbackException ex)
            {
                throw new IngressError(ErrorCode.InvalidUtf8,
                    "QWP error response: invalid UTF-8 in message bytes", ex);
            }

            if (truncated)
            {
                message += "…";
            }
        }

        return new QwpResponse(status, seq, message, EmptyEntries);
    }

    private static QwpTableEntry[] ParseTableEntries(ReadOnlySpan<byte> bytes, int expectedCount)
    {
        if (expectedCount == 0)
        {
            if (bytes.Length != 0)
            {
                throw new IngressError(ErrorCode.ProtocolVersionError,
                    $"QWP response: tableCount=0 but {bytes.Length} trailing bytes follow");
            }

            return EmptyEntries;
        }

        var entries = new QwpTableEntry[expectedCount];
        var pos = 0;
        for (var i = 0; i < expectedCount; i++)
        {
            if (pos + 2 > bytes.Length)
            {
                throw new IngressError(ErrorCode.ProtocolVersionError,
                    $"QWP per-table entry {i}: truncated before name length");
            }

            var nameLen = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(pos, 2));
            pos += 2;

            if (nameLen == 0)
            {
                // Empty table names would silently merge into a single bucket and poison per-table
                // tracking. Reject as a server protocol violation.
                throw new IngressError(ErrorCode.ProtocolVersionError,
                    $"QWP per-table entry {i}: empty table name");
            }

            if (pos + nameLen + 8 > bytes.Length)
            {
                throw new IngressError(ErrorCode.ProtocolVersionError,
                    $"QWP per-table entry {i}: declared length {nameLen} runs past frame end");
            }

            string name;
            try
            {
                name = StrictUtf8.GetString(bytes.Slice(pos, nameLen));
            }
            catch (DecoderFallbackException ex)
            {
                throw new IngressError(ErrorCode.InvalidUtf8,
                    $"QWP per-table entry {i}: invalid UTF-8 table name", ex);
            }
            pos += nameLen;

            var seqTxn = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(pos, 8));
            pos += 8;

            entries[i] = new QwpTableEntry(name, seqTxn);
        }

        if (pos != bytes.Length)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP response: {bytes.Length - pos} trailing bytes after the last per-table entry");
        }

        return entries;
    }

}
