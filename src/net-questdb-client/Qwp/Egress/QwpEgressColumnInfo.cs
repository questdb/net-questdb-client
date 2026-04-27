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
///     Per-column schema metadata attached to each <see cref="QwpColumnLayout"/> while
///     decoding an inbound RESULT_BATCH. The .NET counterpart of Java's
///     <c>QwpEgressColumnInfo</c> on java-questdb-client main 64b7ee69. Carries the
///     column's wire type plus type-specific extras (decimal scale, geohash precision,
///     timestamp encoding flag).
/// </summary>
internal sealed class QwpEgressColumnInfo
{
    public string Name { get; set; } = string.Empty;

    /// <summary>Wire type code from the schema header (one of <see cref="QwpConstants"/>'s TYPE_* values).</summary>
    public byte WireType { get; set; }

    /// <summary>Decimal scale prefix for DECIMAL64 / DECIMAL128 / DECIMAL256 columns. -1 if unused.</summary>
    public sbyte DecimalScale { get; set; } = -1;

    /// <summary>Geohash precision in bits (1-60), or -1 if unused.</summary>
    public int GeohashPrecisionBits { get; set; } = -1;

    /// <summary>
    ///     Timestamp encoding discriminator: <c>0x00</c> (uncompressed), <c>0x01</c> (Gorilla DoD),
    ///     or <c>0xFF</c> (n/a — column is not a timestamp).
    /// </summary>
    public byte TimestampEncoding { get; set; } = 0xFF;

    public void Reset()
    {
        Name = string.Empty;
        WireType = 0;
        DecimalScale = -1;
        GeohashPrecisionBits = -1;
        TimestampEncoding = 0xFF;
    }
}
