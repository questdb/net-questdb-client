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
///     Policy applied by the client when a <see cref="SenderErrorCategory" /> fires. The client
///     never silently drops data: a rejection is either retried or latched terminal with the
///     bytes preserved in the store-and-forward log.
///     <see cref="SenderErrorCategory.SchemaMismatch" />, <see cref="SenderErrorCategory.ParseError" />,
///     <see cref="SenderErrorCategory.SecurityError" /> and
///     <see cref="SenderErrorCategory.ProtocolViolation" /> are forced <see cref="Terminal" />;
///     <see cref="SenderErrorCategory.Unknown" /> is fail-open <see cref="Retriable" />.
/// </summary>
public enum SenderErrorPolicy
{
    /// <summary>
    ///     Recycle the connection and replay the rejected frame from the ack watermark
    ///     (<c>ackedFsn+1</c>) through the reconnect machinery. The watermark does not advance —
    ///     nothing is dropped. A frame that deterministically kills the connection is escalated to
    ///     <see cref="Terminal" /> by the poison-frame detector, not by dropping it.
    /// </summary>
    Retriable,

    /// <summary>
    ///     Latch the error as terminal — the rejection is deterministic under byte-identical replay.
    ///     The next producer-thread API call throws <see cref="Utils.LineSenderServerException" />;
    ///     the rejected bytes stay in the SF store. The sender does not drain further until the
    ///     caller closes and rebuilds it.
    /// </summary>
    Terminal,

    /// <summary>
    ///     Issued only with <see cref="SenderErrorCategory.DataLoss" />: the affected bytes are
    ///     preserved on disk at <see cref="Utils.SenderError.QuarantinedPath" /> but will never be
    ///     retried. Not resolvable by policy configuration.
    /// </summary>
    Abandoned,
}
