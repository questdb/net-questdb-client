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

// ReSharper disable InconsistentNaming

namespace QuestDB.Qwp.Egress;

/// <summary>
///     QWP egress message-kind discriminator bytes. The .NET counterpart of Java's
///     <c>QwpEgressMsgKind</c> on java-questdb-client main 64b7ee69. The first byte
///     of every egress payload identifies which message it carries.
/// </summary>
internal static class QwpEgressMsgKind
{
    /// <summary>Server -&gt; client. Connection-scoped cache reset (body: <c>reset_mask:u8</c>).</summary>
    public const byte CACHE_RESET = 0x17;
    public const byte CANCEL = 0x14;
    public const byte CREDIT = 0x15;
    /// <summary>Server -&gt; client. Ack for a successful non-SELECT query.</summary>
    public const byte EXEC_DONE = 0x16;
    public const byte QUERY_ERROR = 0x13;
    public const byte QUERY_REQUEST = 0x10;
    /// <summary>Reset-mask bit: clear the connection-scoped SYMBOL dictionary.</summary>
    public const byte RESET_MASK_DICT = 0x01;
    /// <summary>Reset-mask bit: clear the connection-scoped schema-fingerprint cache.</summary>
    public const byte RESET_MASK_SCHEMAS = 0x02;
    public const byte RESULT_BATCH = 0x11;
    public const byte RESULT_END = 0x12;

    /// <summary>Role on <c>SERVER_INFO.role</c>: the authoritative write node.</summary>
    public const byte ROLE_PRIMARY = 1;
    /// <summary>Role on <c>SERVER_INFO.role</c>: promotion-in-progress.</summary>
    public const byte ROLE_PRIMARY_CATCHUP = 3;
    /// <summary>Role on <c>SERVER_INFO.role</c>: read-only replica.</summary>
    public const byte ROLE_REPLICA = 2;
    /// <summary>Role on <c>SERVER_INFO.role</c>: standalone (no replication configured).</summary>
    public const byte ROLE_STANDALONE = 0;

    /// <summary>
    ///     Server -&gt; client. Unsolicited frame delivered as the first QWP message on every
    ///     v2 WebSocket connection. Body (LE): <c>msg_kind:u8, role:u8, epoch:u64,
    ///     capabilities:u32, server_wall_ns:i64, cluster_id:u16_len+utf8,
    ///     node_id:u16_len+utf8</c>.
    /// </summary>
    public const byte SERVER_INFO = 0x18;
}
