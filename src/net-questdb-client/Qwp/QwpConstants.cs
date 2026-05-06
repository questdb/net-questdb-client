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
///     Wire-format constants and limits for the QWP v1 protocol.
/// </summary>
internal static class QwpConstants
{
    /// <summary>The 4-byte magic that opens every QWP v1 frame: ASCII "QWP1" stored little-endian.</summary>
    public const uint Magic = 0x31_50_57_51u;

    /// <summary>Total size of the fixed message header in bytes.</summary>
    public const int HeaderSize = 12;

    /// <summary>Byte offset of the magic value within the header.</summary>
    public const int OffsetMagic = 0;

    /// <summary>Byte offset of the version byte within the header.</summary>
    public const int OffsetVersion = 4;

    /// <summary>Byte offset of the flags byte within the header.</summary>
    public const int OffsetFlags = 5;

    /// <summary>Byte offset of the 16-bit table count within the header.</summary>
    public const int OffsetTableCount = 6;

    /// <summary>Byte offset of the 32-bit payload length within the header.</summary>
    public const int OffsetPayloadLength = 8;

    /// <summary>The only ingest protocol version this client speaks.</summary>
    public const byte SupportedIngestVersion = 0x01;

    /// <summary>Timestamp columns may use Gorilla delta-of-delta encoding.</summary>
    public const byte FlagGorilla = 0x04;

    /// <summary>
    ///     Symbol columns use the connection-global delta dictionary instead of per-table dictionaries.
    ///     The WebSocket sender always sets this flag.
    /// </summary>
    public const byte FlagDeltaSymbolDict = 0x08;

    /// <summary>Schema-id followed by inline column definitions.</summary>
    public const byte SchemaModeFull = 0x00;

    /// <summary>Schema-id only; the server resolves columns from its registry.</summary>
    public const byte SchemaModeReference = 0x01;

    /// <summary>Size of an OK response without per-table entries: 1-byte status + 8-byte sequence.</summary>
    public const int OkAckMinSize = 9;

    /// <summary>Size of an error response header: 1-byte status + 8-byte sequence + 2-byte message length.</summary>
    public const int ErrorAckHeaderSize = 11;

    /// <summary>Hard cap on the length of a server-supplied error message.</summary>
    public const int MaxErrorMessageBytes = 1024;

    /// <summary>Maximum size of a single batch payload, in bytes.</summary>
    public const int MaxBatchBytes = 16 * 1024 * 1024;

    /// <summary>Hard ceiling on the number of tables in one frame; the wire field is a uint16.</summary>
    public const int MaxTablesPerMessage = 0xFFFF;

    /// <summary>Maximum number of rows in a single table block.</summary>
    public const int MaxRowsPerTable = 1_000_000;

    /// <summary>Maximum number of columns in a single table.</summary>
    public const int MaxColumnsPerTable = 2048;

    /// <summary>Maximum table or column name length, in UTF-8 bytes.</summary>
    public const int MaxNameLengthBytes = 127;

    /// <summary>Maximum number of dimensions for ARRAY columns (matches existing client convention).</summary>
    public const int MaxArrayDimensions = 32;

    /// <summary>Inclusive lower bound on geohash precision (in bits).</summary>
    public const int MinGeohashPrecisionBits = 1;

    /// <summary>Inclusive upper bound on geohash precision (in bits).</summary>
    public const int MaxGeohashPrecisionBits = 60;

    /// <summary>LONG256 wire size, in bytes.</summary>
    public const int Long256SizeBytes = 32;

    /// <summary>DECIMAL128 unscaled value size on the wire, in bytes.</summary>
    public const int Decimal128SizeBytes = 16;

    /// <summary>Default port for <c>ws::</c> and <c>wss::</c>; shared with HTTP.</summary>
    public const int DefaultPort = 9000;

    /// <summary>Hard-coded WebSocket endpoint path for QWP ingest.</summary>
    public const string WritePath = "/write/v4";

    /// <summary>Hard-coded WebSocket endpoint path for QWP egress (query).</summary>
    public const string ReadPath = "/read/v1";

    /// <summary>RESULT_BATCH payload is zstd-compressed after the prelude.</summary>
    public const byte FlagZstd = 0x10;

    /// <summary>QWP egress message kinds (the first byte of every egress payload).</summary>
    public const byte MsgKindQueryRequest = 0x10;
    public const byte MsgKindResultBatch = 0x11;
    public const byte MsgKindResultEnd = 0x12;
    public const byte MsgKindQueryError = 0x13;
    public const byte MsgKindCancel = 0x14;
    public const byte MsgKindCredit = 0x15;
    public const byte MsgKindExecDone = 0x16;
    public const byte MsgKindCacheReset = 0x17;
    public const byte MsgKindServerInfo = 0x18;

    /// <summary>CACHE_RESET reset_mask bits.</summary>
    public const byte ResetMaskDict = 0x01;
    public const byte ResetMaskSchemas = 0x02;

    /// <summary>SERVER_INFO role bytes.</summary>
    public const byte RoleStandalone = 0x00;
    public const byte RolePrimary = 0x01;
    public const byte RoleReplica = 0x02;
    public const byte RolePrimaryCatchup = 0x03;

    /// <summary>QWP egress status codes (in QUERY_ERROR frames). No STATUS_OK — egress success is RESULT_END.</summary>
    public const byte StatusSchemaMismatch = 0x03;
    public const byte StatusParseError = 0x05;
    public const byte StatusInternalError = 0x06;
    public const byte StatusSecurityError = 0x08;
    public const byte StatusCancelled = 0x0A;
    public const byte StatusLimitExceeded = 0x0B;

    /// <summary>Connection-level errors (parse failure on the message frame, auth failure) carry this id.</summary>
    public const long RequestIdWildcard = -1L;

    /// <summary>Egress wire limits.</summary>
    public const int MaxSqlLengthBytes = 1024 * 1024;

    /// <summary>
    ///     Server cap is <c>MAX_COLUMNS_PER_TABLE = 2048</c> (the Java client uses the same value);
    ///     the egress spec doc §16 quotes 1024 but is stale relative to the server.
    /// </summary>
    public const int MaxBindParameters = 2048;

    public const int MaxResultBatchWireBytes = 16 * 1024 * 1024;

    /// <summary>Server soft caps; clients must accept any policy and tolerate <c>CACHE_RESET</c> at any query boundary.</summary>
    public const int SymbolDictEntriesSoftCap = 100_000;
    public const int SymbolDictHeapBytesSoftCap = 8 * 1024 * 1024;
    public const int SchemaRegistrySoftCap = 4096;

    /// <summary>Server-side zstd level clamp. Client may send any level; server rounds.</summary>
    public const int ZstdLevelMin = 1;
    public const int ZstdLevelMax = 9;

    /// <summary>Egress upgrade headers.</summary>
    public const string HeaderAcceptEncoding = "X-QWP-Accept-Encoding";
    public const string HeaderContentEncoding = "X-QWP-Content-Encoding";
    public const string HeaderMaxBatchRows = "X-QWP-Max-Batch-Rows";

    /// <summary>QWP egress version handed out by Phase-1 servers.</summary>
    public const byte SupportedEgressVersion = 0x02;

    /// <summary>Default auto-flush threshold by row count.</summary>
    public const int DefaultAutoFlushRows = 1000;

    /// <summary>Default auto-flush interval, in milliseconds.</summary>
    public const int DefaultAutoFlushIntervalMs = 100;

    /// <summary>Default in-flight window size; 1 collapses to synchronous mode.</summary>
    public const int DefaultInFlightWindow = 128;

    /// <summary>Default cap on per-connection schema slots; matches the wire schema-id range.</summary>
    public const int DefaultMaxSchemasPerConnection = 65535;

    /// <summary>Default close-drain timeout, in milliseconds.</summary>
    public const int DefaultCloseTimeoutMs = 5000;

    /// <summary>Default ACK timeout for in-flight batches, in milliseconds.</summary>
    public const int DefaultAckTimeoutMs = 30_000;

    /// <summary>Client → server: maximum QWP version the client supports.</summary>
    public const string HeaderMaxVersion = "X-QWP-Max-Version";

    /// <summary>Client → server: free-form client identifier.</summary>
    public const string HeaderClientId = "X-QWP-Client-Id";

    /// <summary>Server → client: negotiated QWP version.</summary>
    public const string HeaderVersion = "X-QWP-Version";

    /// <summary>Server → client: replication role on both 101 (diagnostic) and 503 (role-reject) responses.</summary>
    public const string HeaderQuestDbRole = "X-QuestDB-Role";

    public const string RoleStandaloneName = "STANDALONE";
    public const string RolePrimaryName = "PRIMARY";
    public const string RoleReplicaName = "REPLICA";
    public const string RolePrimaryCatchupName = "PRIMARY_CATCHUP";

    /// <summary>Client → server: opt-in for STATUS_DURABLE_ACK frames.</summary>
    public const string HeaderRequestDurableAck = "X-QWP-Request-Durable-Ack";
}
