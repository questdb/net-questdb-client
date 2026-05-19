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

namespace QuestDB.Qwp;

/// <summary>
///     QWP connect-string key vocabulary, split by side. A <c>ws::</c> / <c>wss::</c> connect
///     string is a single user input shared between the ingress sender and the egress query
///     client; each parser accepts the full union and silently ignores the keys belonging to
///     the other side, so one connect string drives both clients without erroring.
/// </summary>
internal static class QwpConnectStringKeys
{
    /// <summary>Ingress-sender-only keys. The egress query client accepts and ignores these.</summary>
    internal static readonly string[] IngressOnly =
    {
        "auto_flush", "auto_flush_rows", "auto_flush_bytes", "auto_flush_interval",
        "init_buf_size", "max_buf_size", "max_name_len",
        "max_schemas_per_connection", "request_durable_ack", "gorilla",
        "sf_dir", "sender_id", "sf_max_bytes", "sf_max_total_bytes", "sf_durability",
        "sf_append_deadline_millis",
        "reconnect_max_duration_millis", "reconnect_initial_backoff_millis", "reconnect_max_backoff_millis",
        "initial_connect_retry", "close_flush_timeout_millis",
        "drain_orphans", "max_background_drainers", "ping_timeout",
        "durable_ack_keepalive_interval_millis", "proxy",
        "error_inbox_capacity", "connection_listener_inbox_capacity",
        "on_server_error", "on_schema_mismatch_error", "on_schema_error", "on_parse_error",
        "on_internal_error", "on_security_error", "on_write_error",
    };

    /// <summary>Egress-query-client-only keys. The ingress sender accepts and ignores these.</summary>
    internal static readonly string[] EgressOnly =
    {
        "path", "auth", "compression", "compression_level",
        "failover", "failover_max_attempts", "failover_backoff_initial_ms",
        "failover_backoff_max_ms", "failover_max_duration_ms",
        "max_batch_rows", "client_id", "buffer_pool_size",
    };
}
