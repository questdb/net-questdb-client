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
///     Immutable description of a single background-drain transition observed while a store-and-forward
///     sender adopts and drains crashed sibling slots. Delivered to an
///     <see cref="IBackgroundDrainerListener" /> registered via <c>SenderOptions.DrainerListener</c>.
/// </summary>
public sealed class BackgroundDrainerEvent
{
    /// <summary>Constructs a new event. Internal callers populate every field.</summary>
    public BackgroundDrainerEvent(
        BackgroundDrainerEventKind kind,
        string slotDirectory,
        Exception? cause,
        DateTimeOffset timestamp)
    {
        Kind = kind;
        SlotDirectory = slotDirectory;
        Cause = cause;
        Timestamp = timestamp;
    }

    /// <summary>The kind of drain transition.</summary>
    public BackgroundDrainerEventKind Kind { get; }

    /// <summary>Absolute path of the adopted sibling slot directory the transition concerns.</summary>
    public string SlotDirectory { get; }

    /// <summary>
    ///     Underlying cause for <see cref="BackgroundDrainerEventKind.DrainRetrying" /> /
    ///     <see cref="BackgroundDrainerEventKind.DrainQuarantined" /> outcomes; <c>null</c> for
    ///     <see cref="BackgroundDrainerEventKind.SlotAdopted" /> /
    ///     <see cref="BackgroundDrainerEventKind.DrainCompleted" />.
    /// </summary>
    public Exception? Cause { get; }

    /// <summary>Wall-clock time the event was generated.</summary>
    public DateTimeOffset Timestamp { get; }
}
