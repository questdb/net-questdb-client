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

using System.Buffers.Binary;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Bit-level writer for QWP v1. Bits are written LSB-first within each byte and packed
///     sequentially across byte boundaries. Output is to a caller-supplied <see cref="byte"/>
///     array slice (typically backed by <see cref="PinnedAppendBuffer"/>).
/// </summary>
/// <remarks>
///     Experimental. Mirrors <c>QwpBitWriter.java</c> on Java main 64b7ee69. Up to 64 bits
///     are buffered before flushing to memory. <see cref="Flush"/> must be called before
///     reading the output.
/// </remarks>
internal sealed class QwpBitWriter
{
    private byte[] _destination = Array.Empty<byte>();
    private int _start;
    private int _end;
    private int _position;
    private ulong _bitBuffer;
    private int _bitsInBuffer;

    /// <summary>
    ///     Resets the writer to write into <paramref name="destination"/> at <paramref name="offset"/>
    ///     for up to <paramref name="capacity"/> bytes.
    /// </summary>
    public void Reset(byte[] destination, int offset, int capacity)
    {
        if (destination is null) throw new ArgumentNullException(nameof(destination));
        if ((uint)offset > (uint)destination.Length ||
            (uint)capacity > (uint)(destination.Length - offset))
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity), "destination range is out of bounds");
        }
        _destination = destination;
        _start = offset;
        _end = offset + capacity;
        _position = offset;
        _bitBuffer = 0;
        _bitsInBuffer = 0;
    }

    /// <summary>Current write position as an offset into the destination array.</summary>
    public int Position => _position;

    /// <summary>Bytes written since the last <see cref="Reset"/> (excluding any partial bit buffer).</summary>
    public int BytesWritten => _position - _start;

    /// <summary>Aligns to the next byte boundary by padding with zeros. No-op if already aligned.</summary>
    public void AlignToByte()
    {
        if (_bitsInBuffer > 0)
        {
            Flush();
        }
    }

    /// <summary>Flushes any partial bits in the buffer to memory. Throws on overflow.</summary>
    /// <exception cref="IngressError">Buffer is full.</exception>
    public void Flush()
    {
        if (_bitsInBuffer > 0)
        {
            if (_position >= _end)
            {
                throw new IngressError(ErrorCode.BufferOverflow, "QwpBitWriter buffer overflow");
            }
            _destination[_position++] = (byte)_bitBuffer;
            _bitBuffer = 0;
            _bitsInBuffer = 0;
        }
    }

    /// <summary>Flushes and returns the number of bytes written since <see cref="Reset"/>.</summary>
    public int Finish()
    {
        Flush();
        return _position - _start;
    }

    /// <summary>Writes a single bit (only the LSB of <paramref name="bit"/> is used).</summary>
    public void WriteBit(int bit) => WriteBits((long)(bit & 1), 1);

    /// <summary>
    ///     Writes <paramref name="numBits"/> bits from the LSB of <paramref name="value"/>.
    ///     For example, value=0b1101, numBits=4 writes 1, 0, 1, 1 (LSB to MSB).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">numBits is not in 1..64.</exception>
    /// <exception cref="IngressError">Buffer overflow.</exception>
    public void WriteBits(long value, int numBits)
    {
        if (numBits <= 0 || numBits > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(numBits),
                "Asked to write more than 64 bits of a long");
        }

        var v = (ulong)value;
        if (numBits < 64)
        {
            v &= (1UL << numBits) - 1;
        }

        var bitsToWrite = numBits;
        while (bitsToWrite > 0)
        {
            var availableInBuffer = 64 - _bitsInBuffer;
            var bitsThisRound = Math.Min(bitsToWrite, availableInBuffer);

            var mask = bitsThisRound == 64 ? ulong.MaxValue : (1UL << bitsThisRound) - 1;
            _bitBuffer |= (v & mask) << _bitsInBuffer;
            _bitsInBuffer += bitsThisRound;
            v >>= bitsThisRound;
            bitsToWrite -= bitsThisRound;

            while (_bitsInBuffer >= 8)
            {
                if (_position >= _end)
                {
                    throw new IngressError(ErrorCode.BufferOverflow, "QwpBitWriter buffer overflow");
                }
                _destination[_position++] = (byte)_bitBuffer;
                _bitBuffer >>= 8;
                _bitsInBuffer -= 8;
            }
        }
    }

    /// <summary>Writes a complete byte after byte-aligning. Throws on overflow.</summary>
    public void WriteByte(int value)
    {
        AlignToByte();
        if (_position >= _end)
        {
            throw new IngressError(ErrorCode.BufferOverflow, "QwpBitWriter buffer overflow");
        }
        _destination[_position++] = (byte)value;
    }

    /// <summary>Writes a 32-bit little-endian integer after byte-aligning. Throws on overflow.</summary>
    public void WriteInt(int value)
    {
        AlignToByte();
        if (_position + 4 > _end)
        {
            throw new IngressError(ErrorCode.BufferOverflow, "QwpBitWriter buffer overflow");
        }
        BinaryPrimitives.WriteInt32LittleEndian(_destination.AsSpan(_position, 4), value);
        _position += 4;
    }

    /// <summary>Writes a 64-bit little-endian long after byte-aligning. Throws on overflow.</summary>
    public void WriteLong(long value)
    {
        AlignToByte();
        if (_position + 8 > _end)
        {
            throw new IngressError(ErrorCode.BufferOverflow, "QwpBitWriter buffer overflow");
        }
        BinaryPrimitives.WriteInt64LittleEndian(_destination.AsSpan(_position, 8), value);
        _position += 8;
    }

    /// <summary>Writes a signed value as two's complement in <paramref name="numBits"/> bits.</summary>
    public void WriteSigned(long value, int numBits) => WriteBits(value, numBits);
}
