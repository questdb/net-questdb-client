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

/// <summary>Decoded SERVER_INFO frame (v2 only).</summary>
public sealed class QwpServerInfo
{
    /// <summary>Server role byte; see <c>QwpConstants.Role*</c> for the defined values.</summary>
    public byte Role { get; init; }
    /// <summary>Server epoch advanced on every primary/replica failover; lets the client detect topology changes.</summary>
    public ulong Epoch { get; init; }
    /// <summary>Bitmask of optional features advertised by the server.</summary>
    public uint Capabilities { get; init; }
    /// <summary>Server wall clock at the time the frame was emitted, in nanoseconds since Unix epoch.</summary>
    public long ServerWallNs { get; init; }
    /// <summary>Cluster identifier (stable across primary/replica failover).</summary>
    public string ClusterId { get; init; } = string.Empty;
    /// <summary>Node identifier within the cluster.</summary>
    public string NodeId { get; init; } = string.Empty;
    /// <summary>
    ///     Server-advertised zone identifier; non-null only when <see cref="Capabilities" /> has the
    ///     <see cref="QwpConstants.CapZone" /> bit set. Compared case-insensitively against the client's
    ///     configured <c>zone=</c> for same-zone routing preference.
    /// </summary>
    public string? ZoneId { get; init; }

    /// <summary>Human-readable name of <see cref="Role" />; <c>UNKNOWN(n)</c> for unrecognised codes.</summary>
    public string RoleName => Role switch
    {
        QwpConstants.RoleStandalone => "STANDALONE",
        QwpConstants.RolePrimary => "PRIMARY",
        QwpConstants.RoleReplica => "REPLICA",
        QwpConstants.RolePrimaryCatchup => "PRIMARY_CATCHUP",
        _ => $"UNKNOWN({Role})",
    };
}
