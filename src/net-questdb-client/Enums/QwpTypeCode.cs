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

/// <summary>
///     QWP wire-format column type codes.
/// </summary>
/// <remarks>
///     The legacy <c>STRING (0x08)</c> type is intentionally absent: the client emits
///     <see cref="Varchar" /> for all string columns. Both shapes are byte-compatible on
///     the wire; the client simply standardises on the newer code.
/// </remarks>
public enum QwpTypeCode : byte
{
    /// <summary>Bit-packed boolean.</summary>
    Boolean = 0x01,

    /// <summary>Signed 8-bit integer.</summary>
    Byte = 0x02,

    /// <summary>Signed 16-bit integer.</summary>
    Short = 0x03,

    /// <summary>Signed 32-bit integer.</summary>
    Int = 0x04,

    /// <summary>Signed 64-bit integer.</summary>
    Long = 0x05,

    /// <summary>IEEE 754 single-precision float.</summary>
    Float = 0x06,

    /// <summary>IEEE 754 double-precision float.</summary>
    Double = 0x07,

    /// <summary>Dictionary-encoded string. Uses delta dictionary mode on WebSocket.</summary>
    Symbol = 0x09,

    /// <summary>Microseconds since the Unix epoch.</summary>
    Timestamp = 0x0A,

    /// <summary>Milliseconds since the Unix epoch.</summary>
    Date = 0x0B,

    /// <summary>RFC 4122 UUID; 16 bytes (low 8 then high 8).</summary>
    Uuid = 0x0C,

    /// <summary>256-bit unsigned integer; 32 bytes little-endian.</summary>
    Long256 = 0x0D,

    /// <summary>Geospatial hash with explicit precision.</summary>
    Geohash = 0x0E,

    /// <summary>Length-prefixed UTF-8 string with auxiliary offset storage.</summary>
    Varchar = 0x0F,

    /// <summary>Nanoseconds since the Unix epoch.</summary>
    TimestampNanos = 0x10,

    /// <summary>N-dimensional double array.</summary>
    DoubleArray = 0x11,

    /// <summary>N-dimensional long array.</summary>
    LongArray = 0x12,

    /// <summary>64-bit decimal (18-digit precision).</summary>
    Decimal64 = 0x13,

    /// <summary>128-bit decimal (38-digit precision).</summary>
    Decimal128 = 0x14,

    /// <summary>256-bit decimal (77-digit precision).</summary>
    Decimal256 = 0x15,

    /// <summary>UTF-16 code unit (2 bytes little-endian).</summary>
    Char = 0x16,
}
