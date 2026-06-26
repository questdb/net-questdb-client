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
///     User-supplied callback invoked when the QWP ingest client observes a connection-state
///     transition. Registered via <c>SenderOptions.ConnectionListener</c>.
/// </summary>
/// <remarks>
///     <b>Threading.</b> Implementations are invoked on a dedicated daemon dispatcher thread, never
///     on the I/O thread or the producer thread. Slow listeners cannot stall publishing or
///     reconnect; if the bounded inbox fills up, surplus events are dropped (visible via
///     <see cref="IQwpWebSocketSender.DroppedConnectionNotifications" />).
///     <para />
///     <b>Exceptions.</b> Any exception thrown by the listener is caught and traced. The dispatcher
///     and the sender continue running.
///     <para />
///     <b>Delivery.</b> Best-effort and rare relative to the data path. Success transitions
///     (<see cref="SenderConnectionEventKind.Connected" />, <see cref="SenderConnectionEventKind.Reconnected" />)
///     are guaranteed to fire. Failure events may be coalesced under inbox pressure.
/// </remarks>
public interface ISenderConnectionListener
{
    /// <summary>Invoked once per observed connection-state transition.</summary>
    void OnEvent(SenderConnectionEvent evt);
}
