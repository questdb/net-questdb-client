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

#if NET7_0_OR_GREATER

using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp.Query;

/// <summary>
///     A query-level failure: the server terminated the request with a <c>QUERY_ERROR</c> frame
///     (bad SQL, permission failure, resource limit, …). Thrown by
///     <see cref="IQwpQueryReader.ReadBatchAsync" />. Unless the error was connection-scoped, the
///     stream ended at a clean frame boundary and the client remains usable for the next query.
/// </summary>
public sealed class QwpQueryException : IngressError
{
    internal QwpQueryException(QwpStatusCode status, string serverMessage)
        : base(ErrorCode.ServerQueryError, $"{status}: {serverMessage}")
    {
        Status = status;
        ServerMessage = serverMessage;
    }

    /// <summary>The QWP status code carried by the QUERY_ERROR frame.</summary>
    public QwpStatusCode Status { get; }

    /// <summary>The server's error message, without the status/code prefixes added to <see cref="Exception.Message" />.</summary>
    public string ServerMessage { get; }
}

#endif
