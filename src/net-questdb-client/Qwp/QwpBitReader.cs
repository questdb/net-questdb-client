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
///     LSB-first bit reader for QWP v1. The .NET counterpart of Java's <c>QwpBitReader</c>
///     on java-questdb-client main 64b7ee69, reimplemented over <see cref="ReadOnlyMemory{T}"/>
///     instead of a raw native pointer.
/// </summary>
/// <remarks>
///     Experimental. Bits are pulled LSB-first within each byte: the byte 0b1010_0001
///     yields the sequence 1, 0, 0, 0, 0, 1, 0, 1. Up to 64 bits are buffered before
///     refill; <see cref="ReadBits"/> is capped at 64 per call.
/// </remarks>
internal sealed class QwpBitReader
{
    private ReadOnlyMemory<byte> _source;
    private int _bytePosition;
    private ulong _bitBuffer;
    private int _bitsInBuffer;

    /// <summary>Number of bits read since the last <see cref="Reset"/>.</summary>
    public long BitPosition { get; private set; }

    public void Reset(ReadOnlyMemory<byte> source)
    {
        _source = source;
        _bytePosition = 0;
        _bitBuffer = 0;
        _bitsInBuffer = 0;
        BitPosition = 0;
    }

    /// <summary>Reads a single bit and returns 0 or 1.</summary>
    /// <exception cref="QwpDecodeException">No more bits to read.</exception>
    public int ReadBit()
    {
        EnsureBits(1);
        var bit = (int)(_bitBuffer & 1UL);
        _bitBuffer >>= 1;
        _bitsInBuffer--;
        BitPosition++;
        return bit;
    }

    /// <summary>
    ///     Reads <paramref name="numBits"/> bits (0 to 64 inclusive) and returns them
    ///     packed into the low bits of a <see cref="long"/>. Asking for 0 bits returns
    ///     0 without advancing the position.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="numBits"/> is outside 0..64.</exception>
    /// <exception cref="QwpDecodeException">Fewer than <paramref name="numBits"/> bits remain.</exception>
    public long ReadBits(int numBits)
    {
        if ((uint)numBits > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(numBits),
                "numBits must be in 0..64");
        }
        if (numBits == 0) return 0;

        EnsureBits(numBits);

        var mask = numBits == 64 ? ulong.MaxValue : (1UL << numBits) - 1;
        var result = (long)(_bitBuffer & mask);

        // For numBits == 64, `>>= 64` is a no-op (shift count is masked to 6 bits ⇒ 0).
        // We must clear explicitly or the next refill OR-fills onto the stale buffer.
        if (numBits == 64) _bitBuffer = 0;
        else _bitBuffer >>= numBits;

        _bitsInBuffer -= numBits;
        BitPosition += numBits;
        return result;
    }

    /// <summary>
    ///     Reads <paramref name="numBits"/> bits and sign-extends to a <see cref="long"/>.
    ///     For <c>numBits == 64</c> the value already occupies the full long; no extension.
    /// </summary>
    public long ReadSigned(int numBits)
    {
        var value = ReadBits(numBits);
        if (numBits < 64 && (value & (1L << (numBits - 1))) != 0)
        {
            value |= -1L << numBits;
        }
        return value;
    }

    private void EnsureBits(int needed)
    {
        var availableTotal = (long)_bitsInBuffer + ((long)(_source.Length - _bytePosition) * 8);
        if (availableTotal < needed)
        {
            throw new QwpDecodeException(
                $"read past end of QWP bitstream (need {needed} bits, have {availableTotal})");
        }
        var src = _source.Span;
        while (_bitsInBuffer < needed && _bitsInBuffer <= 56 && _bytePosition < src.Length)
        {
            var b = src[_bytePosition++];
            _bitBuffer |= (ulong)b << _bitsInBuffer;
            _bitsInBuffer += 8;
        }
    }
}
