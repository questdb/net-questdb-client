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

using System.Runtime.CompilerServices;

namespace QuestDB.Qwp;

/// <summary>
///     Writes a bit-packed stream LSB-first within each byte (specification §12).
/// </summary>
/// <remarks>
///     The first written bit lands in bit 0 of byte 0; subsequent bits fill bit 1, bit 2, … of the
///     same byte before advancing to the next. A partially-filled trailing byte is padded with
///     zeros at <see cref="FinishToByteBoundary" />.
///     <para />
///     <c>ref struct</c> for stack allocation; cannot escape the calling method or be stored in a
///     field. Construct one per encode pass.
/// </remarks>
internal ref struct QwpBitWriter
{
    private readonly Span<byte> _buffer;
    private readonly int _startOffset;
    private int _byteIndex;
    private int _bitIndex;

    public QwpBitWriter(Span<byte> buffer, int startOffset)
    {
        _buffer = buffer;
        _startOffset = startOffset;
        _byteIndex = startOffset;
        _bitIndex = 0;
        if (_byteIndex < _buffer.Length)
        {
            // Zero the first byte so subsequent OR operations are well-defined regardless of any
            // stale bytes the buffer carries from prior frames.
            _buffer[_byteIndex] = 0;
        }
    }

    /// <summary>
    ///     Writes the low <paramref name="bitCount" /> bits of <paramref name="value" />, LSB-first.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBits(ulong value, int bitCount)
    {
        for (var i = 0; i < bitCount; i++)
        {
            if (((value >> i) & 1UL) != 0)
            {
                _buffer[_byteIndex] |= (byte)(1 << _bitIndex);
            }

            _bitIndex++;
            if (_bitIndex == 8)
            {
                _byteIndex++;
                _bitIndex = 0;
                if (_byteIndex < _buffer.Length)
                {
                    _buffer[_byteIndex] = 0;
                }
            }
        }
    }

    /// <summary>
    ///     Pads the current byte with zeros up to the next byte boundary and returns the total
    ///     number of bytes written since construction.
    /// </summary>
    public int FinishToByteBoundary()
    {
        var bytes = _byteIndex - _startOffset;
        if (_bitIndex > 0)
        {
            bytes++;
        }

        return bytes;
    }
}

/// <summary>
///     Reads a bit-packed stream LSB-first within each byte; mirror of <see cref="QwpBitWriter" />.
/// </summary>
internal ref struct QwpBitReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _byteIndex;
    private int _bitIndex;

    public QwpBitReader(ReadOnlySpan<byte> buffer, int startOffset)
    {
        _buffer = buffer;
        _byteIndex = startOffset;
        _bitIndex = 0;
    }

    /// <summary>Reads <paramref name="bitCount" /> bits as an unsigned integer, LSB-first.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ReadBits(int bitCount)
    {
        ulong value = 0;
        for (var i = 0; i < bitCount; i++)
        {
            if (_byteIndex >= _buffer.Length)
            {
                throw new InvalidOperationException("bit reader exhausted");
            }

            if (((_buffer[_byteIndex] >> _bitIndex) & 1) != 0)
            {
                value |= 1UL << i;
            }

            _bitIndex++;
            if (_bitIndex == 8)
            {
                _byteIndex++;
                _bitIndex = 0;
            }
        }

        return value;
    }
}
