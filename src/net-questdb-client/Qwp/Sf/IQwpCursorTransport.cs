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

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Minimal wire-level abstraction the SF cursor engine depends on. Every reconnect attempt
///     uses a fresh instance.
/// </summary>
/// <remarks>
///     Implementations must surface auth/upgrade rejections (HTTP 401/403, non-101 upgrade) by
///     throwing <see cref="IngressError" /> with <see cref="ErrorCode.AuthError" /> from
///     <see cref="ConnectAsync" /> — the engine treats these as immediate terminal failures and
///     does not retry. All other transport failures are surfaced as transient and retried under
///     the configured reconnect budget.
/// </remarks>
internal interface IQwpCursorTransport : IDisposable
{
    /// <summary>
    ///     The endpoint this transport will (or did) connect to. <c>null</c> when the transport
    ///     is not bound to a specific endpoint (e.g. test stubs).
    /// </summary>
    (string Host, int Port)? Endpoint { get; }

    /// <summary>
    ///     Server-advertised hard cap on QWP ingest payload bytes from the upgrade response;
    ///     <c>0</c> when the server did not advertise it. Valid only after <see cref="ConnectAsync" />.
    /// </summary>
    int NegotiatedMaxBatchSize => 0;

    /// <summary>Connects to the server and completes the QWP upgrade.</summary>
    Task ConnectAsync(CancellationToken cancellationToken);

    /// <summary>Sends a single QWP request frame.</summary>
    Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken);

    /// <summary>Reads the next QWP response frame into <paramref name="destination" />.</summary>
    /// <returns>The number of bytes written.</returns>
    Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken);

    /// <summary>Sends a graceful close. Must tolerate being called from any state.</summary>
    Task CloseAsync(CancellationToken cancellationToken);
}
