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

namespace QuestDB.Qwp.Query;

/// <summary>
///     Raised by the egress decoder (<see cref="QwpResultBatchDecoder" /> and
///     <see cref="QwpColumnBatch" />) when a <c>RESULT_BATCH</c> frame is malformed or corrupt:
///     truncated payloads, lengths or counts out of range, unsupported column type codes,
///     overlong varints, or a bad Gorilla timestamp column. The query loop in
///     <c>QwpQueryWebSocketClient</c> catches it and re-wraps it as an
///     <c>IngressError</c> with <c>ErrorCode.ProtocolViolation</c>, so callers of the query
///     API observe a protocol violation rather than this internal decode failure.
/// </summary>
public sealed class QwpDecodeException : Exception
{
    /// <summary>Constructs the exception with a description of the malformed frame.</summary>
    public QwpDecodeException(string message) : base(message) { }

    /// <summary>
    ///     Constructs the exception with a description and the underlying runtime failure
    ///     (e.g. a varint or Gorilla decode error), so the raw exception isn't surfaced directly.
    /// </summary>
    public QwpDecodeException(string message, Exception inner) : base(message, inner) { }
}
