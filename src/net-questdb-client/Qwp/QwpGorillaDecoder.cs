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

namespace QuestDB.Qwp;

/// <summary>
///     Client-side Gorilla delta-of-delta decoder for timestamp columns in QWP egress
///     <c>RESULT_BATCH</c> frames. Counterpart to <see cref="QwpGorillaEncoder"/>; mirrors
///     Java's <c>QwpGorillaDecoder</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Experimental. Encoding buckets:
///     <list type="bullet">
///         <item><c>'0'</c> ⇒ DoD = 0 (1 bit)</item>
///         <item><c>'10'</c> + 7-bit signed ⇒ DoD ∈ [-64, 63] (9 bits)</item>
///         <item><c>'110'</c> + 9-bit signed ⇒ DoD ∈ [-256, 255] (12 bits)</item>
///         <item><c>'1110'</c> + 12-bit signed ⇒ DoD ∈ [-2048, 2047] (16 bits)</item>
///         <item><c>'1111'</c> + 32-bit signed ⇒ otherwise (36 bits)</item>
///     </list>
/// </remarks>
internal sealed class QwpGorillaDecoder
{
    private readonly QwpBitReader _bitReader = new();
    private long _prevDelta;
    private long _prevTimestamp;

    /// <summary>Current bit position into the bitstream (bits consumed since reset).</summary>
    public long BitPosition => _bitReader.BitPosition;

    /// <summary>
    ///     Binds the decoder to a column's bitstream. The first two timestamps are shipped
    ///     uncompressed at the head of the column's wire bytes; pass them as
    ///     <paramref name="firstTimestamp"/> and <paramref name="secondTimestamp"/>, and
    ///     pass the bytes that follow them as <paramref name="bitstream"/>.
    /// </summary>
    public void Reset(long firstTimestamp, long secondTimestamp, ReadOnlyMemory<byte> bitstream)
    {
        _prevTimestamp = secondTimestamp;
        _prevDelta = unchecked(secondTimestamp - firstTimestamp);
        _bitReader.Reset(bitstream);
    }

    /// <summary>Decodes the next timestamp.</summary>
    /// <exception cref="QwpDecodeException">Bitstream is exhausted or malformed.</exception>
    public long DecodeNext()
    {
        var dod = DecodeDoD();
        var delta = unchecked(_prevDelta + dod);
        var timestamp = unchecked(_prevTimestamp + delta);
        _prevDelta = delta;
        _prevTimestamp = timestamp;
        return timestamp;
    }

    private long DecodeDoD()
    {
        var bit = _bitReader.ReadBit();
        if (bit == 0) return 0;
        bit = _bitReader.ReadBit();
        if (bit == 0) return _bitReader.ReadSigned(7);
        bit = _bitReader.ReadBit();
        if (bit == 0) return _bitReader.ReadSigned(9);
        bit = _bitReader.ReadBit();
        if (bit == 0) return _bitReader.ReadSigned(12);
        return _bitReader.ReadSigned(32);
    }
}
