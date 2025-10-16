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
using System.Runtime.InteropServices;
using QuestDB.Enums;
using QuestDB.Utils;

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
            Put((byte)0); // Scale
            Put((byte)0); // Length
            return this;
        }

        Span<int> parts = stackalloc int[4];
        decimal.GetBits(value.Value, parts);

        int flags = parts[3];
        byte scale = (byte)((flags & ScaleMask) >> ScaleShift);

        // 3. Scale
        Put(scale);

        int low = parts[0];
        int mid = parts[1];
        int high = parts[2];
        bool bitSign = false;
        bool negative = (flags & SignMask) != 0;

        if (negative)
        {
            // QuestDB expects negative mantissas in two's complement.
            low = ~low + 1;
            int c = low == 0 ? 1 : 0;
            mid = ~mid + c;
            c = mid == 0 && c == 1 ? 1 : 0;
            high = ~high + c;
            // We may overflow, we need an extra byte to convey the sign.
            bitSign = high == 0 && c == 1;
        }
        else if ((high & 0x80000000) != 0)
        {
            // If the highest bit is set, we need an extra byte of 0 to convey the sign.
            bitSign = true;
        }

        var size = bitSign ? 13 : 12;

        // 4. Length
        Put((byte)size);

        // 5. Unscaled value
        EnsureCapacity(size);
        var span = Chunk.AsSpan(Position, size);
        var offset = 0;
        if (bitSign)
        {
            span[offset++] = (byte)(negative ? 255 : 0);
        }
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(offset, 4), high);
        offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(offset, 4), mid);
        offset += 4;
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(offset, 4), low);
        Advance(size);

        return this;
    }
}
