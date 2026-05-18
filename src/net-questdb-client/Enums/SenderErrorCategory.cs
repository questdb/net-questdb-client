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
    SchemaMismatch,

    /// <summary>QWP-level malformed payload — most likely a client bug. Wire 0x05.</summary>
    ParseError,

    /// <summary>Server-side fault, catch-all. Wire 0x06.</summary>
    InternalError,

    /// <summary>Authentication or authorization failure. Wire 0x08.</summary>
    SecurityError,

    /// <summary>Non-critical Cairo error, table not accepting writes. Wire 0x09.</summary>
    WriteError,

    /// <summary>WebSocket-layer close frame with a terminal code.</summary>
    ProtocolViolation,

    /// <summary>Status byte the client does not recognize — forward compatibility for new server codes.</summary>
    Unknown,
}
