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

using System;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Raised when the server rejects a <c>/write/v4</c> WebSocket upgrade with
///     <c>421 Misdirected Request</c> + <c>X-QuestDB-Role</c>. Carries the role so
///     the host-health tracker can distinguish transient (<c>PRIMARY_CATCHUP</c>) from
///     structural (<c>REPLICA</c>) topology rejects.
/// </summary>
public sealed class QwpIngressRoleRejectedException : IngressError
{
    public QwpIngressRoleRejectedException(string role, Uri uri, Exception? innerException = null)
        : base(ErrorCode.SocketError,
            $"WebSocket ingress upgrade rejected by role={role} at {uri}",
            innerException)
    {
        Role = role;
        Uri = uri;
    }

    /// <summary><c>X-QuestDB-Role</c> value as advertised by the server (uppercase ASCII).</summary>
    public string Role { get; }

    /// <summary>The endpoint that returned the role-reject response.</summary>
    public Uri Uri { get; }

    /// <summary>
    ///     <c>true</c> when the role indicates a transient promotion-in-progress state
    ///     (<c>PRIMARY_CATCHUP</c>); the same endpoint is likely to accept writes once
    ///     the catchup completes.
    /// </summary>
    public bool IsTransient => Role == QwpConstants.RolePrimaryCatchupName;

    /// <summary>
    ///     <c>true</c> when the role is structurally unable to accept writes (<c>REPLICA</c>);
    ///     retrying the same endpoint will not help until topology changes.
    /// </summary>
    public bool IsTopological => Role == QwpConstants.RoleReplicaName;
}
