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
///     §3.4 — Failover knobs for <see cref="QwpQueryClient"/>'s multi-endpoint mode.
///     Defaults match Java's <c>QwpQueryClient</c> on java-questdb-client main
///     64b7ee69 (<c>failover=on</c>, max 8 attempts, 250 ms initial backoff capped
///     at 16 s).
/// </summary>
internal readonly record struct QwpFailoverOptions(
    bool Enabled,
    int MaxAttempts,
    int InitialBackoffMs,
    int MaxBackoffMs,
    QwpTargetRole TargetRole)
{
    /// <summary>Default settings — failover on, 8 attempts, 250 ms → 16 s backoff, target=any.</summary>
    public static QwpFailoverOptions Default => new(
        Enabled: true,
        MaxAttempts: 8,
        InitialBackoffMs: 250,
        MaxBackoffMs: 16_000,
        TargetRole: QwpTargetRole.Any);

    /// <summary>Disabled: a single attempt, no retries — equivalent to pre-failover behaviour.</summary>
    public static QwpFailoverOptions Disabled => new(
        Enabled: false,
        MaxAttempts: 1,
        InitialBackoffMs: 0,
        MaxBackoffMs: 0,
        TargetRole: QwpTargetRole.Any);
}

/// <summary>
///     §3.4 — Endpoint role filter applied during connect using the SERVER_INFO
///     frame's role byte. Mirrors Java's <c>QwpTargetRole</c> on
///     java-questdb-client main 64b7ee69.
/// </summary>
internal enum QwpTargetRole : byte
{
    /// <summary>Accept any role — any reachable endpoint will do.</summary>
    Any = 0,
    /// <summary>Only the cluster's authoritative write node.</summary>
    Primary = 1,
    /// <summary>Only a read replica.</summary>
    Replica = 2,
}
