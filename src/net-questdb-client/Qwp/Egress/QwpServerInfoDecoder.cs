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
using System.Text;

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Parses a SERVER_INFO frame payload off the wire into an immutable
///     <see cref="QwpServerInfo"/>. The .NET counterpart of Java's
///     <c>QwpServerInfoDecoder</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     The input span starts at the QWP message header (12 bytes), followed by the
///     SERVER_INFO body. Bounds are validated on every read; a malformed length
///     prefix throws <see cref="QwpDecodeException"/> rather than reading past the
///     end of the span.
/// </remarks>
internal static class QwpServerInfoDecoder
{
    /// <summary>
    ///     Decodes a SERVER_INFO frame from <paramref name="payload"/>. The span must
    ///     begin with the 12-byte QWP header followed by the message body.
    /// </summary>
    /// <exception cref="QwpDecodeException">
    ///     Thrown when the frame is truncated, the msg_kind byte is not
    ///     <see cref="QwpEgressMsgKind.SERVER_INFO"/>, or a length prefix exceeds the
    ///     remainder.
    /// </exception>
    public static QwpServerInfo Decode(ReadOnlySpan<byte> payload)
    {
        const int fixedBytes = QwpConstants.HEADER_SIZE + 1 + 1 + 8 + 4 + 8 + 2;
        if (payload.Length < fixedBytes)
        {
            throw new QwpDecodeException(
                $"SERVER_INFO frame truncated [payloadLen={payload.Length}, minRequired={fixedBytes}]");
        }

        var p = QwpConstants.HEADER_SIZE;
        var msgKind = payload[p];
        if (msgKind != QwpEgressMsgKind.SERVER_INFO)
        {
            throw new QwpDecodeException(
                $"expected SERVER_INFO msg_kind 0x{QwpEgressMsgKind.SERVER_INFO:x2} got 0x{msgKind:x2}");
        }
        p += 1;
        var role = payload[p];
        p += 1;
        var epoch = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(p, 8));
        p += 8;
        var capabilities = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(p, 4));
        p += 4;
        var serverWallNs = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(p, 8));
        p += 8;

        var clusterId = ReadUtf8U16(payload, ref p, "cluster_id");
        var nodeId = ReadUtf8U16(payload, ref p, "node_id");
        return new QwpServerInfo(role, epoch, capabilities, serverWallNs, clusterId, nodeId);
    }

    private static string ReadUtf8U16(ReadOnlySpan<byte> payload, ref int p, string fieldName)
    {
        if (p + 2 > payload.Length)
        {
            throw new QwpDecodeException($"SERVER_INFO truncated before {fieldName} length");
        }
        int len = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(p, 2));
        p += 2;
        if (p + len > payload.Length)
        {
            throw new QwpDecodeException(
                $"SERVER_INFO {fieldName} length {len} exceeds frame remainder {payload.Length - p}");
        }
        if (len == 0) return string.Empty;
        var s = Encoding.UTF8.GetString(payload.Slice(p, len));
        p += len;
        return s;
    }
}
