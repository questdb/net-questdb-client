/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
// ReSharper disable CommentTypo

namespace QuestDB.Enums;

/// <summary>
///     Standard error codes for QuestDB ILP clients.
/// </summary>
public enum ErrorCode
{
    /// <summary>
    ///     The host, port, or interface was incorrect.
    /// </summary>
    CouldNotResolveAddr,

    /// <summary>
    ///     Called methods in the wrong order. E.g. `symbol` after `column`.
    /// </summary>
    InvalidApiCall,

    /// <summary>
    ///     A network error connecting or flushing data out.
    /// </summary>
    SocketError,

    /// <summary>
    ///     The string or symbol field is not encoded in valid UTF-8.
    ///     <br />
    ///     *This error is reserved for the
    ///     C and C++ API.
    /// </summary>
    InvalidUtf8,

    /// <summary>
    ///     The table name or column name contains bad characters.
    /// </summary>
    InvalidName,

    /// <summary>
    ///     The supplied timestamp is invalid.
    /// </summary>
    InvalidTimestamp,

    /// <summary>
    ///     Error during the authentication process.
    /// </summary>
    AuthError,

    /// <summary>
    ///     Error during TLS handshake.
    /// </summary>
    TlsError,

    /// <summary>
    ///     The server does not support ILP-over-HTTP.
    /// </summary>
    HttpNotSupported,

    /// <summary>
    ///     Error sent back from the server during flush.
    /// </summary>
    ServerFlushError,

    /// <summary>
    ///     Bad configuration.
    /// </summary>
    ConfigError,

    /// <summary>
    ///     Issues relating to protocol version mismatch.
    /// </summary>
    ProtocolVersionError,

    /// <summary>
    ///     Bad configuration.
    /// </summary>
    InvalidArrayShapeError,

    /// <summary>
    ///     The server closed the WebSocket with a protocol-violation code
    ///     (RFC 6455 1002 / 1003 / 1007 / 1008 / 1009 / 1010). Terminal; the
    ///     cursor engine must not reconnect.
    /// </summary>
    ProtocolViolation,

    /// <summary>
    ///     <c>request_durable_ack=on</c> was set but the server did not echo
    ///     <c>X-QWP-Durable-Ack: enabled</c>. Terminal — without it the SF log would
    ///     grow unbounded waiting for ack frames that will never arrive.
    /// </summary>
    DurableAckNotSupported,
}