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

using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Exception raised when the QWP server reports a non-OK status.
/// </summary>
/// <remarks>
///     Wraps the QWP-level <see cref="Status" /> and request <see cref="Sequence" /> alongside the
///     existing <see cref="IngressError" /> machinery, so callers that already handle
///     <c>IngressError</c> see this as one of those, while callers that want the QWP detail can
///     downcast.
/// </remarks>
public sealed class QwpException : IngressError
{
    /// <summary>
    ///     Constructs a new <see cref="QwpException" />.
    /// </summary>
    /// <param name="status">QWP status code from the server frame.</param>
    /// <param name="sequence">Batch sequence the server was responding to. <c>-1</c> if not applicable.</param>
    /// <param name="message">UTF-8 message decoded from the server frame, or a synthetic message.</param>
    public QwpException(QwpStatusCode status, long sequence, string message)
        : base(MapToErrorCode(status), Format(status, sequence, message))
    {
        Status = status;
        Sequence = sequence;
    }

    /// <summary>QWP status code received from the server.</summary>
    public QwpStatusCode Status { get; }

    /// <summary>
    ///     Request sequence the server was responding to. <c>-1</c> when the response carries no
    ///     sequence (for example, durable-ack frames).
    /// </summary>
    public long Sequence { get; }

    private static string Format(QwpStatusCode status, long sequence, string message)
    {
        return sequence < 0
            ? $"{status}: {message}"
            : $"{status} (seq={sequence}): {message}";
    }

    private static ErrorCode MapToErrorCode(QwpStatusCode status)
    {
        return status switch
        {
            QwpStatusCode.SchemaMismatch => ErrorCode.InvalidApiCall,
            QwpStatusCode.ParseError => ErrorCode.ProtocolVersionError,
            QwpStatusCode.SecurityError => ErrorCode.AuthError,
            // WriteError, InternalError, and any unrecognised codes all map to the generic
            // server-flush error.
            _ => ErrorCode.ServerFlushError,
        };
    }
}
