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

namespace QuestDB.Enums;

/// <summary>
///     Server-distinguishable rejection categories. Aligned 1:1 with the stable QWP
///     wire status bytes for ingress, plus <see cref="ProtocolViolation" /> for
///     WebSocket-level close frames and <see cref="Unknown" /> for forward compatibility.
/// </summary>
public enum SenderErrorCategory
{
    /// <summary>Schema mismatch (column missing, type clash, NOT NULL violated, no such table). Wire 0x03.</summary>
    SchemaMismatch = 0,

    /// <summary>QWP-level malformed payload — most likely a client bug. Wire 0x05.</summary>
    ParseError = 1,

    /// <summary>Server-side fault, catch-all. Wire 0x06.</summary>
    InternalError = 2,

    /// <summary>Authentication or authorization failure. Wire 0x08.</summary>
    SecurityError = 3,

    /// <summary>Non-critical Cairo error, table not accepting writes. Wire 0x09.</summary>
    WriteError = 4,

    /// <summary>WebSocket-layer close frame with a terminal code.</summary>
    ProtocolViolation = 5,

    /// <summary>Status byte the client does not recognize — forward compatibility for new server codes.</summary>
    Unknown = 6,

    /// <summary>
    ///     The connected node cannot currently accept writes (for example a replica or demoting
    ///     primary). Wire 0x0C. Reconnecting allows endpoint rotation.
    /// </summary>
    NotWritable = 7,

    /// <summary>
    ///     A delta began above the server's connection-scoped symbol dictionary. Wire 0x0D.
    ///     Reconnecting and sending dictionary catch-up makes the same data frame valid.
    /// </summary>
    DictionaryGap = 8,

    /// <summary>
    ///     Buffered store-and-forward data was permanently abandoned: an unreplayable slot was
    ///     set aside at <see cref="Utils.SenderError.QuarantinedPath" /> for inspection and
    ///     resend. Client-side verdict, never carried on the wire; only
    ///     <see cref="Utils.SenderError.DataLoss" /> constructs it. This is the event to page on.
    /// </summary>
    DataLoss = 9,
}
