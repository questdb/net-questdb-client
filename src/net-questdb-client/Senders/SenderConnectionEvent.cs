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
///     Immutable description of a single connection-state transition observed by the QWP ingest
///     client. Delivered to a <see cref="ISenderConnectionListener" /> registered via
///     <c>SenderOptions.ConnectionListener</c>.
/// </summary>
public sealed class SenderConnectionEvent
{
    /// <summary>Sentinel for <see cref="AttemptNumber" /> / <see cref="RoundNumber" /> when not applicable.</summary>
    public const long NoCounter = -1L;

    /// <summary>Sentinel for <see cref="Port" /> when not applicable.</summary>
    public const int NoPort = -1;

    /// <summary>Constructs a new event. Internal callers populate every field; sentinels mark missing data.</summary>
    public SenderConnectionEvent(
        SenderConnectionEventKind kind,
        string? host,
        int port,
        long attemptNumber,
        long roundNumber,
        Exception? cause,
        DateTimeOffset timestamp)
    {
        Kind = kind;
        Host = host;
        Port = port;
        AttemptNumber = attemptNumber;
        RoundNumber = roundNumber;
        Cause = cause;
        Timestamp = timestamp;
    }

    /// <summary>The kind of state transition.</summary>
    public SenderConnectionEventKind Kind { get; }

    /// <summary>The endpoint host involved, or <c>null</c> when not applicable.</summary>
    public string? Host { get; }

    /// <summary>The endpoint port involved, or <see cref="NoPort" /> when not applicable.</summary>
    public int Port { get; }

    /// <summary>Reconnect-attempt counter snapshot, or <see cref="NoCounter" /> for events without one.</summary>
    public long AttemptNumber { get; }

    /// <summary>Sweep counter snapshot, or <see cref="NoCounter" /> for events without one.</summary>
    public long RoundNumber { get; }

    /// <summary>
    ///     Underlying cause for failure / terminal events; <c>null</c> for success transitions
    ///     (<see cref="SenderConnectionEventKind.Connected" />, <see cref="SenderConnectionEventKind.Reconnected" />).
    /// </summary>
    public Exception? Cause { get; }

    /// <summary>Wall-clock time the event was generated.</summary>
    public DateTimeOffset Timestamp { get; }
}
