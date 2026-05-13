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
///     Raised when the WebSocket upgrade is rejected by the server with an authentication-related
///     status (HTTP 401 or 403). Distinct from generic <see cref="IngressError" /> so callers can
///     handle credential failures separately from transport/protocol errors.
/// </summary>
/// <remarks>
///     Inherits <see cref="IngressError" /> with <see cref="ErrorCode.AuthError" />, so existing
///     <c>catch (IngressError) when (e.code == ErrorCode.AuthError)</c> handlers keep matching.
///     Auth failures are terminal — failover does not retry rejected credentials, since flooding
///     the server with the same bad credentials rarely recovers and pollutes server logs.
/// </remarks>
public sealed class QwpAuthFailedException : IngressError
{
    /// <summary>HTTP status code returned by the server on the WebSocket upgrade response.</summary>
    public int HttpStatusCode { get; }

    /// <summary>The endpoint the auth attempt was made against; <c>null</c> when not available.</summary>
    public Uri? Endpoint { get; }

    /// <summary>Constructs a new <see cref="QwpAuthFailedException" /> from the rejecting endpoint and HTTP status.</summary>
    public QwpAuthFailedException(int httpStatusCode, Uri? endpoint)
        : base(ErrorCode.AuthError, BuildMessage(httpStatusCode, endpoint))
    {
        HttpStatusCode = httpStatusCode;
        Endpoint = endpoint;
    }

    /// <inheritdoc cref="QwpAuthFailedException(int, Uri)" />
    public QwpAuthFailedException(int httpStatusCode, Uri? endpoint, Exception inner)
        : base(ErrorCode.AuthError, BuildMessage(httpStatusCode, endpoint), inner)
    {
        HttpStatusCode = httpStatusCode;
        Endpoint = endpoint;
    }

    private static string BuildMessage(int status, Uri? endpoint)
    {
        return endpoint is null
            ? $"WebSocket upgrade rejected with HTTP {status}"
            : $"WebSocket upgrade rejected with HTTP {status} for {endpoint}";
    }
}
