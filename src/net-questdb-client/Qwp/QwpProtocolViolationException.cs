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

using System;
using System.Net.WebSockets;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Terminal exception raised when the server sends a WebSocket CLOSE frame carrying a
///     protocol-violation close code. The cursor engine MUST NOT reconnect on this — the
///     server is signalling a structural problem with the bytes that just left the client,
///     and replaying them produces the same outcome.
/// </summary>
/// <remarks>
///     The qualifying close codes are 1002 <c>PROTOCOL_ERROR</c>, 1003 <c>UNSUPPORTED_DATA</c>,
///     1007 <c>INVALID_PAYLOAD_DATA</c>, 1008 <c>POLICY_VIOLATION</c>, 1009 <c>MESSAGE_TOO_BIG</c>,
///     and 1010 <c>MANDATORY_EXTENSION</c>. All other CLOSE codes are reconnect-eligible.
/// </remarks>
public sealed class QwpProtocolViolationException : IngressError
{
    /// <summary>Constructs the exception from the server-sent CLOSE status and description.</summary>
    public QwpProtocolViolationException(WebSocketCloseStatus status, string? reason)
        : base(ErrorCode.ProtocolViolation, FormatMessage(status, reason))
    {
        CloseStatus = status;
        Reason = reason;
    }

    /// <summary>The qualifying WebSocket CLOSE status code.</summary>
    public WebSocketCloseStatus CloseStatus { get; }

    /// <summary>Operator-supplied CLOSE description; <c>null</c> when the server omitted one.</summary>
    public string? Reason { get; }

    /// <summary>
    ///     <c>true</c> for the close-code set that terminates the cursor engine; <c>false</c>
    ///     for codes routed back into the reconnect loop.
    /// </summary>
    public static bool IsProtocolViolationCode(WebSocketCloseStatus status) => status is
        WebSocketCloseStatus.ProtocolError
        or WebSocketCloseStatus.InvalidMessageType
        or WebSocketCloseStatus.InvalidPayloadData
        or WebSocketCloseStatus.PolicyViolation
        or WebSocketCloseStatus.MessageTooBig
        or WebSocketCloseStatus.MandatoryExtension;

    private static string FormatMessage(WebSocketCloseStatus status, string? reason)
    {
        var code = ((int)status).ToString(System.Globalization.CultureInfo.InvariantCulture);
        return string.IsNullOrEmpty(reason)
            ? $"ws-close[{code}]: "
            : $"ws-close[{code}]: {reason}";
    }
}
