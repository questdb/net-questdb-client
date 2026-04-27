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
 ******************************************************************************/

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Immutable snapshot of the SERVER_INFO frame body delivered as the first QWP
///     message on every v2 WebSocket connection. The .NET counterpart of Java's
///     <c>QwpServerInfo</c> on java-questdb-client main 64b7ee69.
/// </summary>
internal sealed class QwpServerInfo
{
    public QwpServerInfo(
        byte role,
        long epoch,
        int capabilities,
        long serverWallNs,
        string clusterId,
        string nodeId)
    {
        Role = role;
        Epoch = epoch;
        Capabilities = capabilities;
        ServerWallNs = serverWallNs;
        ClusterId = clusterId;
        NodeId = nodeId;
    }

    /// <summary>Server-advertised capability bitfield.</summary>
    public int Capabilities { get; }

    /// <summary>Cluster identifier (UTF-8, may be empty).</summary>
    public string ClusterId { get; }

    /// <summary>Server epoch counter — increments on every primary failover.</summary>
    public long Epoch { get; }

    /// <summary>Stable node identifier within the cluster (UTF-8, may be empty).</summary>
    public string NodeId { get; }

    /// <summary>Role byte. See <see cref="QwpEgressMsgKind.ROLE_STANDALONE"/> et al.</summary>
    public byte Role { get; }

    /// <summary>Server wall-clock at the time the frame was emitted (nanoseconds since epoch).</summary>
    public long ServerWallNs { get; }

    /// <summary>Returns the human-readable name for a role byte (e.g. <c>"PRIMARY"</c>).</summary>
    public static string RoleName(byte role) => role switch
    {
        QwpEgressMsgKind.ROLE_STANDALONE => "STANDALONE",
        QwpEgressMsgKind.ROLE_PRIMARY => "PRIMARY",
        QwpEgressMsgKind.ROLE_REPLICA => "REPLICA",
        QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP => "PRIMARY_CATCHUP",
        _ => $"UNKNOWN({role & 0xFF})",
    };

    public override string ToString() =>
        $"QwpServerInfo[role={RoleName(Role)}, epoch={Epoch}, capabilities=0x{Capabilities:X8}, " +
        $"serverWallNs={ServerWallNs}, clusterId='{ClusterId}', nodeId='{NodeId}']";
}
