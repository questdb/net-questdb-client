/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

using System.Buffers.Binary;
using QuestDB.Enums;

namespace QuestDB.Buffers;

/// <summary />
public class BufferV3 : BufferV2
{
    /// <summary />
    public BufferV3(int bufferSize, int maxNameLen, int maxBufSize) : base(bufferSize, maxNameLen, maxBufSize)
    {
    }

    // Sign mask for the flags field. A value of zero in this bit indicates a
    // positive Decimal value, and a value of one in this bit indicates a
    // negative Decimal value.
    private const int SignMask = unchecked((int)0x80000000);

    // Scale mask for the flags field. This byte in the flags field contains
    // the power of 10 to divide the Decimal value by. The scale byte must
    // contain a value between 0 and 28 inclusive.
    private const int ScaleMask = 0x00FF0000;

    // Number of bits scale is shifted by.
    private const int ScaleShift = 16;

    /// <inheritdoc />
    public override IBuffer Column(ReadOnlySpan<char> name, decimal? value)
    {
        // # Binary Format
        // 1. Binary format marker: `'='` (0x3D)
        // 2. Type identifier: BinaryFormatType.DECIMAL byte
        // 3. Scale: 1 byte (0-76 inclusive) - number of decimal places
        // 4. Length: 1 byte - number of bytes in the unscaled value
        // 5. Unscaled value: variable-length byte array in two's complement format, big-endian
        SetTableIfAppropriate();
        Column(name)
            .PutAscii(Constants.BINARY_FORMAT_FLAG)
            .Put((byte)BinaryFormatType.DECIMAL);
        if (value is null)
        {
            Put(0); // Scale
            Put(0); // Length
            return this;
        }

        Span<int> parts = stackalloc int[4];
        decimal.GetBits(value.Value, parts);

        var flags = parts[3];
        var scale = (byte)((flags & ScaleMask) >> ScaleShift);

        // 3. Scale
        Put(scale);

        var low = parts[0];
        var mid = parts[1];
        var high = parts[2];
        var negative = (flags & SignMask) != 0 && value != 0;

        if (negative)
        {
            // QuestDB expects negative mantissas in two's complement.
            low = ~low + 1;
            var c = low == 0 ? 1 : 0;
            mid = ~mid + c;
            c = mid == 0 && c == 1 ? 1 : 0;
            high = ~high + c;
        }

        // We write the byte array on the stack first so that we can compress (remove unnecessary bytes) it later.
        Span<byte> span = stackalloc byte[13];
        var signByte = (byte)(negative ? 255 : 0);
        span[0] = signByte;
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(1, 4), high);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(5, 4), mid);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(9, 4), low);
        
        // Compress
        var start = 0;
        for (;
             // We can strip prefix bits that are 0 (if positive) or 1 (if negative) as long as we keep at least
             // one of it in front to convey the sign.
             start < span.Length - 1 && span[start] == signByte && ((span[start + 1] ^ signByte) & 0x80) == 0;
             start++) ;
        
        // 4. Length
        var size = span.Length - start;
        Put((byte)size);

        // 5. Unscaled value
        EnsureCapacity(size);
        span.Slice(start, size).CopyTo(Chunk.AsSpan(Position, size));
        Advance(size);

        return this;
    }
}
