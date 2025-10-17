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

    /// <summary>
    /// Writes the provided value into the buffer as little-endian raw bytes and advances the buffer position by the value's size.
    /// </summary>
    /// <param name="value">A value whose raw bytes will be written into the buffer in little-endian order.</param>
    private void PutBinaryLE<T>(T value) where T : struct
    {
        var size = Marshal.SizeOf<T>();
        EnsureCapacity(size);
        var mem = MemoryMarshal.Cast<byte, T>(Chunk.AsSpan(Position, size));
        mem[0] = value;
        Advance(size);
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

    /// <summary>
    /// Writes a sequence of values into the buffer in little-endian binary form, handling chunk boundaries and advancing the buffer position.
    /// </summary>
    /// <param name="value">A span of values whose raw bytes will be written as little-endian binary (elements are written whole; partial element writes are not performed).</param>
    private void PutBinaryManyLE<T>(ReadOnlySpan<T> value) where T : struct
    {
        var srcSpan = MemoryMarshal.Cast<T, byte>(value);
        var byteSize = Marshal.SizeOf<T>();

        while (srcSpan.Length > 0)
        {
            var dstLength = GetSpareCapacity();               // length
            if (dstLength < byteSize)
            {
                NextBuffer();
                dstLength = GetSpareCapacity();
            }
            var availLength = dstLength - dstLength % byteSize; // rounded length

            if (srcSpan.Length < availLength)
            {
                srcSpan.CopyTo(Chunk.AsSpan(Position, srcSpan.Length));
                Advance(srcSpan.Length);
                return;
            }
            var dstSpan = Chunk.AsSpan(Position, availLength);
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

    /// <summary>
    /// Writes a column whose value is the provided span of doubles encoded as a binary double array.
    /// </summary>
    /// <returns>The current buffer instance.</returns>
    public override IBuffer Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        GuardAgainstNonDoubleTypes(typeof(T));
        return PutDoubleArray(name, value);
    }

    /// <summary>
    /// Writes a one-dimensional double array column encoded in the buffer's binary double-array format.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">A span of elements representing the array; elements must be of type `double`.</param>
    /// <returns>The current buffer instance.</returns>
    private IBuffer PutDoubleArray<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        SetTableIfAppropriate();
        PutArrayOfDoubleHeader(name);
        Put(1);
        PutBinary(Convert.ToUInt32(value.Length));
        PutBinaryMany(value);

        return this;
    }

    /// <summary>
    /// Add a column with the given name whose value is provided by the specified double array (1D or multi-dimensional).
    /// </summary>
    /// <param name="name">The column name to write.</param>
    /// <param name="value">An array of doubles to write. If null the column is omitted. For a 1D array the values are written as a single-dimension double array; for multi-dimensional arrays the rank and each dimension length are written followed by the elements in row-major order.</param>
    /// <returns>This buffer instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the array's element type cannot be determined.</exception>
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