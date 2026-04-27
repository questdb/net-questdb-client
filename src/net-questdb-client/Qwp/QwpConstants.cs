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

// ReSharper disable InconsistentNaming
// ReSharper disable IdentifierTypo

namespace QuestDB.Qwp;

/// <summary>
///     Constants for the QWP v1 binary protocol.
/// </summary>
/// <remarks>
///     Experimental. Mirrors <c>core/src/main/java/io/questdb/client/cutlass/qwp/protocol/QwpConstants.java</c>
///     on java-questdb-client main 64b7ee69. All multi-byte values on the wire are little-endian.
/// </remarks>
internal static class QwpConstants
{
    /// <summary>Client identifier sent in the X-QWP-Client-Id upgrade header.</summary>
    public const string CLIENT_ID = "dotnet/3.2.0";

    /// <summary>Flag bit: Gorilla timestamp encoding enabled.</summary>
    public const byte FLAG_GORILLA = 0x04;

    /// <summary>Flag bit: Delta symbol dictionary encoding enabled.</summary>
    public const byte FLAG_DELTA_SYMBOL_DICT = 0x08;

    /// <summary>Flag bit: payload region is zstd-compressed (set only when handshake negotiated zstd).</summary>
    public const byte FLAG_ZSTD = 0x10;

    /// <summary>Offset of the flags byte in the message header.</summary>
    public const int HEADER_OFFSET_FLAGS = 5;

    /// <summary>Size of the QWP message header in bytes.</summary>
    public const int HEADER_SIZE = 12;

    /// <summary>Magic bytes "QWP1" (ASCII), little-endian: 0x31505751.</summary>
    public const int MAGIC_MESSAGE = 0x31505751;

    /// <summary>Maximum columns per table. Mirrors the server-side constant.</summary>
    public const int MAX_COLUMNS_PER_TABLE = 2048;

    /// <summary>Maximum column name length in bytes.</summary>
    public const int MAX_COLUMN_NAME_LENGTH = 127;

    /// <summary>Maximum table name length in bytes.</summary>
    public const int MAX_TABLE_NAME_LENGTH = 127;

    /// <summary>Schema mode: full schema included in the message.</summary>
    public const byte SCHEMA_MODE_FULL = 0x00;

    /// <summary>Schema mode: schema reference (sender-assigned schemaId lookup).</summary>
    public const byte SCHEMA_MODE_REFERENCE = 0x01;

    // Wire status codes (WS server response frames).
    public const byte STATUS_OK = 0x00;
    public const byte STATUS_DURABLE_ACK = 0x02;
    public const byte STATUS_SCHEMA_MISMATCH = 0x03;
    public const byte STATUS_PARSE_ERROR = 0x05;
    public const byte STATUS_INTERNAL_ERROR = 0x06;
    public const byte STATUS_SECURITY_ERROR = 0x08;
    public const byte STATUS_WRITE_ERROR = 0x09;

    /// <summary>Egress: query was cancelled (extends ingress STATUS_* namespace).</summary>
    public const byte STATUS_CANCELLED = 0x0A;

    /// <summary>Egress: a server-side limit was hit (timeout, memory cap, OOM).</summary>
    public const byte STATUS_LIMIT_EXCEEDED = 0x0B;

    // Column type codes. NB: TYPE_STRING (0x08) does not exist; VARCHAR (0x0F) replaces it.
    public const byte TYPE_BOOLEAN = 0x01;
    public const byte TYPE_BYTE = 0x02;
    public const byte TYPE_SHORT = 0x03;
    public const byte TYPE_INT = 0x04;
    public const byte TYPE_LONG = 0x05;
    public const byte TYPE_FLOAT = 0x06;
    public const byte TYPE_DOUBLE = 0x07;
    public const byte TYPE_SYMBOL = 0x09;
    public const byte TYPE_TIMESTAMP = 0x0A;
    public const byte TYPE_DATE = 0x0B;
    public const byte TYPE_UUID = 0x0C;
    public const byte TYPE_LONG256 = 0x0D;
    public const byte TYPE_GEOHASH = 0x0E;
    public const byte TYPE_VARCHAR = 0x0F;
    public const byte TYPE_TIMESTAMP_NANOS = 0x10;
    public const byte TYPE_DOUBLE_ARRAY = 0x11;
    public const byte TYPE_LONG_ARRAY = 0x12;
    public const byte TYPE_DECIMAL64 = 0x13;
    public const byte TYPE_DECIMAL128 = 0x14;
    public const byte TYPE_DECIMAL256 = 0x15;
    public const byte TYPE_CHAR = 0x16;
    public const byte TYPE_BINARY = 0x17;
    public const byte TYPE_IPv4 = 0x18;

    /// <summary>Current ingest protocol version.</summary>
    public const byte VERSION_1 = 1;

    /// <summary>Egress protocol v2 (adds the unsolicited SERVER_INFO control frame).</summary>
    public const byte VERSION_2 = 2;

    /// <summary>Maximum protocol version the ingest path advertises / accepts.</summary>
    public const byte MAX_SUPPORTED_INGEST_VERSION = VERSION_1;

    /// <summary>Maximum protocol version supported by the egress path.</summary>
    public const byte MAX_SUPPORTED_VERSION = VERSION_2;

    /// <summary>
    ///     Returns the per-value size in bytes as encoded on the wire. <see cref="TYPE_BOOLEAN"/>
    ///     returns <c>0</c> because it is bit-packed. <see cref="TYPE_GEOHASH"/> and other
    ///     variable-width types return <c>-1</c>.
    /// </summary>
    public static int GetFixedTypeSize(byte typeCode)
    {
        switch (typeCode)
        {
            case TYPE_BOOLEAN: return 0;
            case TYPE_BYTE: return 1;
            case TYPE_SHORT:
            case TYPE_CHAR:
                return 2;
            case TYPE_INT:
            case TYPE_FLOAT:
                return 4;
            case TYPE_LONG:
            case TYPE_DOUBLE:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
            case TYPE_DATE:
            case TYPE_DECIMAL64:
                return 8;
            case TYPE_UUID:
            case TYPE_DECIMAL128:
                return 16;
            case TYPE_LONG256:
            case TYPE_DECIMAL256:
                return 32;
            case TYPE_IPv4:
                return 4;
            case TYPE_GEOHASH:
                return -1;
            default:
                return -1;
        }
    }

    /// <summary>Returns a human-readable name for the type code.</summary>
    public static string GetTypeName(byte typeCode)
    {
        switch (typeCode)
        {
            case TYPE_BOOLEAN: return "BOOLEAN";
            case TYPE_BYTE: return "BYTE";
            case TYPE_SHORT: return "SHORT";
            case TYPE_CHAR: return "CHAR";
            case TYPE_INT: return "INT";
            case TYPE_LONG: return "LONG";
            case TYPE_FLOAT: return "FLOAT";
            case TYPE_DOUBLE: return "DOUBLE";
            case TYPE_SYMBOL: return "SYMBOL";
            case TYPE_TIMESTAMP: return "TIMESTAMP";
            case TYPE_TIMESTAMP_NANOS: return "TIMESTAMP_NANOS";
            case TYPE_DATE: return "DATE";
            case TYPE_UUID: return "UUID";
            case TYPE_LONG256: return "LONG256";
            case TYPE_GEOHASH: return "GEOHASH";
            case TYPE_VARCHAR: return "VARCHAR";
            case TYPE_DOUBLE_ARRAY: return "DOUBLE_ARRAY";
            case TYPE_LONG_ARRAY: return "LONG_ARRAY";
            case TYPE_DECIMAL64: return "DECIMAL64";
            case TYPE_DECIMAL128: return "DECIMAL128";
            case TYPE_DECIMAL256: return "DECIMAL256";
            // .NET deliberately diverges from Java main, which omits BINARY and IPv4 here
            // (a latent bug — both codes are emitted elsewhere in the protocol). When
            // upstream catches up, this case becomes a no-op.
            case TYPE_BINARY: return "BINARY";
            case TYPE_IPv4: return "IPv4";
            default: return "UNKNOWN(" + typeCode + ")";
        }
    }

    /// <summary>
    ///     Returns true if the type code represents a fixed-width type (i.e. <see cref="GetFixedTypeSize"/>
    ///     is positive or zero for it).
    /// </summary>
    public static bool IsFixedWidthType(byte typeCode)
    {
        switch (typeCode)
        {
            case TYPE_BOOLEAN:
            case TYPE_BYTE:
            case TYPE_SHORT:
            case TYPE_CHAR:
            case TYPE_INT:
            case TYPE_LONG:
            case TYPE_FLOAT:
            case TYPE_DOUBLE:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
            case TYPE_DATE:
            case TYPE_UUID:
            case TYPE_LONG256:
            case TYPE_DECIMAL64:
            case TYPE_DECIMAL128:
            case TYPE_DECIMAL256:
            case TYPE_IPv4:  // 4 bytes fixed; .NET adds, Java main omits (latent bug).
                return true;
            default:
                return false;
        }
    }
}
