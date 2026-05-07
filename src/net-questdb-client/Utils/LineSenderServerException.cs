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

namespace QuestDB.Utils;

/// <summary>
///     Thrown from the producer thread when the SF cursor engine has latched a
///     <see cref="SenderErrorPolicy.Halt" />-policy <see cref="SenderError" />.
///     The structured payload is on <see cref="Error" />.
/// </summary>
public sealed class LineSenderServerException : IngressError
{
    public LineSenderServerException(SenderError error)
        : base(MapErrorCode(error.Category), Format(error))
    {
        Error = error;
    }

    /// <summary>The structured error payload from the server (or engine).</summary>
    public SenderError Error { get; }

    private static string Format(SenderError e)
    {
        var msg = string.IsNullOrEmpty(e.ServerMessage) ? "(no message)" : e.ServerMessage;
        return $"server rejected batch [category={e.Category}, status=0x{e.ServerStatusByte & 0xFF:X2}, " +
               $"fsn=[{e.FromFsn},{e.ToFsn}], table={e.TableName ?? "(none)"}]: {msg}";
    }

    private static ErrorCode MapErrorCode(SenderErrorCategory category) =>
        category switch
        {
            SenderErrorCategory.SecurityError => ErrorCode.AuthError,
            SenderErrorCategory.ParseError => ErrorCode.InvalidApiCall,
            _ => ErrorCode.ServerFlushError,
        };
}
