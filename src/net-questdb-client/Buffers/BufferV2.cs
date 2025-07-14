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

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Buffers;

/// <summary />
public class BufferV2 : BufferV1
{
    /// <summary />
    public BufferV2(int bufferSize, int maxNameLen, int maxBufSize) : base(bufferSize, maxNameLen, maxBufSize)
    {
    }

    /// <summary />
    public override IBuffer Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape)
        where T : struct
    {
        GuardAgainstNonDoubleTypes(typeof(T));
        SetTableIfAppropriate();
        PutArrayOfDoubleHeader(name);

        PutBinaryDeferred(out Span<byte> dimsSlot);
        dimsSlot[0] = byte.MinValue;


        var expectedLength = 1;
        foreach (var i in shape)
        {
            try
            {
                PutBinary(Convert.ToUInt32(i));
            }
            catch (OverflowException)
            {
                throw new IngressError(ErrorCode.InvalidArrayShapeError, "array shape is invalid");
            }

            dimsSlot[0]++;
            expectedLength *= i;
        }

        var startLength = Length;
        foreach (var d in (IEnumerable<double>)value)
        {
            PutBinary(d);
        }

        var enumerableLength = (Length - startLength) / sizeof(double);

        if (expectedLength != enumerableLength)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "shape does not match enumerable length");
        }

        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardAgainstNonDoubleTypes(Type t)
    {
        if (t != typeof(double))
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "only double arrays are supported");
        }
    }

    // ReSharper disable once InconsistentNaming
    private void PutBinaryLE<T>(T value) where T : struct
    {
        var size = Marshal.SizeOf<T>();
        EnsureCapacity(size);
        var length = Marshal.SizeOf<T>();
        var mem    = MemoryMarshal.Cast<byte, T>(Chunk.AsSpan(Position, length));
        mem[0] = value;
        Advance(length);
    }

    // ReSharper disable once InconsistentNaming
    private void PutBinaryBE<T>(T value) where T : struct
    {
        var size = Marshal.SizeOf<T>();
        EnsureCapacity(size);
        PutBinaryDeferred(out Span<T> slot);
        slot[0] = value;
        MemoryMarshal.Cast<T, byte>(slot).Reverse();
    }

    // ReSharper disable once InconsistentNaming
    private void PutBinaryManyLE<T>(ReadOnlySpan<T> value) where T : struct
    {
        var srcSpan  = MemoryMarshal.Cast<T, byte>(value);
        var byteSize = Marshal.SizeOf<T>();

        while (srcSpan.Length > 0)
        {
            var dstLength   = GetSpareCapacity();               // length
            if (dstLength < byteSize)
            {
                NextBuffer();
                dstLength   = GetSpareCapacity();
            }
            var availLength = dstLength - dstLength % byteSize; // rounded length

            if (srcSpan.Length < availLength)
            {
                srcSpan.CopyTo(Chunk.AsSpan(Position, srcSpan.Length));
                Advance(srcSpan.Length);
                return;
            }
            var dstSpan     = Chunk.AsSpan(Position, availLength);
            srcSpan.Slice(0, availLength).CopyTo(dstSpan);
            Advance(availLength);
            srcSpan = srcSpan.Slice(availLength);
        }
    }

    // ReSharper disable once InconsistentNaming
    private void PutBinaryManyBE<T>(ReadOnlySpan<T> value) where T : struct
    {
        foreach (var t in value)
        {
            PutBinaryBE(t);
        }
    }

    private void PutBinaryMany<T>(ReadOnlySpan<T> value) where T : struct
    {
        if (BitConverter.IsLittleEndian)
        {
            PutBinaryManyLE(value);
        }
        else
        {
            PutBinaryManyBE(value);
        }
    }

    private void PutBinary<T>(T value) where T : struct
    {
        if (BitConverter.IsLittleEndian)
        {
            PutBinaryLE(value);
        }
        else
        {
            PutBinaryBE(value);
        }
    }

    /// <summary />
    public override IBuffer Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        GuardAgainstNonDoubleTypes(typeof(T));
        return PutDoubleArray(name, value);
    }

    private IBuffer PutDoubleArray<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value)  where T : struct
    {
        SetTableIfAppropriate();
        PutArrayOfDoubleHeader(name);
        Put(1);
        PutBinary(Convert.ToUInt32(value.Length));
        PutBinaryMany(value);

        return this;
    }

    /// <summary />
    public override IBuffer Column(ReadOnlySpan<char> name, Array? value)
    {
        if (value == null)
        {
            // The value is null, do not include the column in the message
            return this;
        }
        
        var type = value.GetType().GetElementType();
        GuardAgainstNonDoubleTypes(type ?? throw new InvalidOperationException());
        if (value.Rank == 1)
        {
            // Fast path, one dim array
            return PutDoubleArray(name, (ReadOnlySpan<double>)value!);
        }
        
        SetTableIfAppropriate();
        PutArrayOfDoubleHeader(name);

        Put((byte)value.Rank); // dims count

        for (var i = 0; i < value.Rank; i++)
        {
            PutBinary(value.GetLength(i));
        }

        foreach (double d in value)
        {
            PutBinary(d);
        }

        return this;
    }

    private void PutBinaryDeferred<T>(out Span<T> span) where T : struct
    {
        var length = Marshal.SizeOf<T>();
        EnsureCapacity(length);
        span = MemoryMarshal.Cast<byte, T>(Chunk.AsSpan(Position, length));
        Advance(length);
    }

    private void PutArrayOfDoubleHeader(ReadOnlySpan<char> name)
    {
        Column(name)
            .PutAscii(Constants.BINARY_FORMAT_FLAG)
            .Put((byte)BinaryFormatType.ARRAY)
            .Put((byte)DataType.DOUBLE);
    }


    private void PutDoubleHeader(ReadOnlySpan<char> name)
    {
        Column(name)
            .PutAscii(Constants.BINARY_FORMAT_FLAG)
            .Put((byte)BinaryFormatType.DOUBLE);
    }

    private int GetSpareCapacity()
    {
        return Chunk.Length - Position;
    }

    /// <inheritdoc />
    public override IBuffer Column(ReadOnlySpan<char> name, double value)
    {
        SetTableIfAppropriate();
        PutDoubleHeader(name);
        PutBinary(value);
        return this;
    }
}