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

namespace QuestDB.Senders;

/// <summary>
///     Categorisation of <see cref="SenderConnectionEvent" /> transitions observed by the QWP
///     ingest client.
/// </summary>
public enum SenderConnectionEventKind
{
    /// <summary>The very first successful connect of this sender's lifetime.</summary>
    Connected,

    /// <summary>The active wire connection dropped and the reconnect loop is about to start.</summary>
    Disconnected,

    /// <summary>A reconnect attempt succeeded (against any endpoint).</summary>
    Reconnected,

    /// <summary>
    ///     A reconnect attempt succeeded against an endpoint different from the previously-active
    ///     one. Mutually exclusive with <see cref="Reconnected" />.
    /// </summary>
    FailedOver,

    /// <summary>A single endpoint connect or upgrade attempt failed; client will try the next.</summary>
    EndpointAttemptFailed,

    /// <summary>Every endpoint in the configured address list was attempted and none accepted.</summary>
    AllEndpointsUnreachable,

    /// <summary>
    ///     Terminal: server-rejected credentials. The sender will halt; the next producer-thread API
    ///     call surfaces an <see cref="QuestDB.Utils.IngressError" />.
    /// </summary>
    AuthFailed,

    /// <summary>
    ///     Terminal: the configured reconnect time budget was exhausted without a successful reconnect.
    /// </summary>
    ReconnectBudgetExhausted,
}
