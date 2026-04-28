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
///             <b>OK (legacy)</b> — 9 bytes: <c>uint8 status (0x00)</c> + <c>int64 sequence</c>.
///             The sequence is the cumulative ACK watermark; every batch with seq ≤
///             <see cref="Sequence" /> has succeeded. <see cref="TableEntries" /> is empty.
///         </item>
///         <item>
///             <b>OK (with per-table seqTxns)</b> — same prefix as legacy, plus a <c>uint16 tableCount</c>
///             and <c>tableCount</c> repeating <c>[uint16 nameLen + name + int64 seqTxn]</c> entries.
///             Servers send this shape when the client opted in via <c>request_durable_ack=on</c>.
///         </item>
///         <item>
///             <b>Durable-ACK</b> — <c>uint8 status (0x02)</c> + <c>uint16 tableCount</c> + entries.
///             Carries no batch sequence; <see cref="Sequence" /> is set to <c>-1</c>.
///         </item>
///         <item>
///             <b>Error</b> — 11 + <c>msg_len</c> bytes: status + sequence + uint16 msg_len + UTF-8
///             message (capped at 1024 bytes).
///         </item>
///     </list>
///     <para />
///     <b>Strict validation</b>: the parser rejects empty table names, lying lengths, and any
///     trailing bytes after the last entry.
/// </remarks>
internal readonly struct QwpResponse
{
    private static readonly QwpTableEntry[] EmptyEntries = Array.Empty<QwpTableEntry>();

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
    ///     Per-table seqTxn watermarks when present. Empty for legacy 9-byte OK frames and for
    ///     error frames.
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

    // -- Parser ------------------------------------------------------------------

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

        if (!IsKnownErrorStatus(status))
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP response carries unknown status code 0x{statusByte:X2}");
        }

        return ParseError(status, frame);
    }

    private static QwpResponse ParseOk(ReadOnlySpan<byte> frame)
    {
        // Legacy form: status (1) + sequence (8) = 9 bytes, no per-table entries.
        if (frame.Length == QwpConstants.OkAckMinSize)
        {
            var seqOnly = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(1, 8));
            return new QwpResponse(QwpStatusCode.Ok, seqOnly, string.Empty, EmptyEntries);
        }

        // Extended form: status (1) + sequence (8) + tableCount (2) + entries.
        const int extendedHeaderSize = QwpConstants.OkAckMinSize + 2;
        if (frame.Length < extendedHeaderSize)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP OK response has invalid size {frame.Length}; must be {QwpConstants.OkAckMinSize} (legacy) or ≥ {extendedHeaderSize} (with per-table entries)");
        }

        var sequence = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(1, 8));
        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(frame.Slice(QwpConstants.OkAckMinSize, 2));
        var entries = ParseTableEntries(frame.Slice(extendedHeaderSize), tableCount);
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

        if (msgLen > QwpConstants.MaxErrorMessageBytes)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP error message length {msgLen} exceeds the {QwpConstants.MaxErrorMessageBytes}-byte cap");
        }

        var expectedTotal = QwpConstants.ErrorAckHeaderSize + msgLen;
        if (frame.Length != expectedTotal)
        {
            throw new IngressError(ErrorCode.ProtocolVersionError,
                $"QWP error response size mismatch: header+message expects {expectedTotal} bytes, got {frame.Length}");
        }

        var message = msgLen == 0
            ? string.Empty
            : Encoding.UTF8.GetString(frame.Slice(QwpConstants.ErrorAckHeaderSize, msgLen));

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

            var name = Encoding.UTF8.GetString(bytes.Slice(pos, nameLen));
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

    private static bool IsKnownErrorStatus(QwpStatusCode status)
    {
        return status is QwpStatusCode.SchemaMismatch
            or QwpStatusCode.ParseError
            or QwpStatusCode.InternalError
            or QwpStatusCode.SecurityError
            or QwpStatusCode.WriteError;
    }
}
