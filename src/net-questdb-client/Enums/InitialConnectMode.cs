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

/// <summary>First-connect retry policy for the SF cursor engine.</summary>
/// <remarks>
///     <list type="bullet">
///         <item><see cref="off" /> — try the configured address list once; throw at construction
///             if every host fails. Default. Surfaces "couldn't reach server" eagerly.</item>
///         <item><see cref="on" /> — block the constructor; retry with exponential backoff until
///             one host accepts the upgrade or <c>reconnect_max_duration_millis</c> exhausts.</item>
///         <item><see cref="async" /> — return from the constructor immediately; the background
///             I/O thread drives the reconnect loop. Subsequent <c>append</c> calls buffer to the
///             SF dir without blocking on connect. Intended for unattended producers where the
///             dir may already carry segments queued from a prior process and the server may
///             come up later.</item>
///     </list>
///     The connect string accepts <c>off</c>/<c>false</c>, <c>on</c>/<c>true</c>/<c>sync</c>,
///     and <c>async</c>; <c>sync</c> is an alias for <c>on</c>.
/// </remarks>
public enum InitialConnectMode
{
    off,
    on,
    async,
}
