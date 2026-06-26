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
///     QWP server response status codes.
/// </summary>
public enum QwpStatusCode : byte
{
    /// <summary>Cumulative ACK. Sequence in the frame is the highest acknowledged batch.</summary>
    Ok = 0x00,

    /// <summary>
    ///     Object-store durability watermark. Per-table; the frame carries no batch sequence.
    ///     Only emitted when the client requested durable acks during the upgrade.
    /// </summary>
    DurableAck = 0x02,

    /// <summary>Column type incompatible with the existing table schema.</summary>
    SchemaMismatch = 0x03,

    /// <summary>Malformed message.</summary>
    ParseError = 0x05,

    /// <summary>Server-side internal error.</summary>
    InternalError = 0x06,

    /// <summary>Authorization failure.</summary>
    SecurityError = 0x08,

    /// <summary>Write failure (for example, table not accepting writes).</summary>
    WriteError = 0x09,

    /// <summary>Query terminated in response to a CANCEL frame.</summary>
    Cancelled = 0x0A,

    /// <summary>A protocol limit was hit.</summary>
    LimitExceeded = 0x0B,
}
