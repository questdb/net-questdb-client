using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Buffers;

public class BufferV2 : BufferV1
{
    public BufferV2(int bufferSize, int maxNameLen, int maxBufSize) : base(bufferSize, maxNameLen, maxBufSize)
    {
    }

    public override IBuffer Column<T>(ReadOnlySpan<char> name, T[] value) where T : struct
    {
        return Column(name, (ReadOnlySpan<T>)value.AsSpan());
    }

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
            PutBinary(Convert.ToUInt32(i));
            dimsSlot[0]++;
            expectedLength *= i;
        }

        var startLength = Length;
        foreach (var d in (IEnumerable<double>)value) PutBinary(d);

        var enumerableLength = (Length - startLength) / sizeof(double);

        if (expectedLength != enumerableLength)
            throw new IngressError(ErrorCode.InvalidApiCall, "shape does not match enumerable length");

        return this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardAgainstNonDoubleTypes(Type t)
    {
        if (t != typeof(double)) throw new IngressError(ErrorCode.InvalidApiCall, "only double arrays are supported");
    }

    private IBuffer PutBinaryLE<T>(T value) where T : struct
    {
        var size = Marshal.SizeOf<T>();
        EnsureCapacity(size);
        PutBinaryDeferred(out Span<T> slot);
        slot[0] = value;
        return this;
    }

    private IBuffer PutBinaryBE<T>(T value) where T : struct
    {
        var size = Marshal.SizeOf<T>();
        EnsureCapacity(size);
        PutBinaryDeferred(out Span<T> slot);
        slot[0] = value;
        MemoryMarshal.Cast<T, byte>(slot).Reverse();
        return this;
    }

    private IBuffer PutBinary<T>(T value, in Span<T> span) where T : struct
    {
        span[0] = value;
        if (!BitConverter.IsLittleEndian) span.Reverse();
        return this;
    }

    private void PutBinaryManyLE<T>(ReadOnlySpan<T> value) where T : struct
    {
        var srcSpan = MemoryMarshal.Cast<T, byte>(value);
        var byteSize = Marshal.SizeOf<T>();

        while (srcSpan.Length > 0)
        {
            var dstLength = GetSpareCapacity(); // length
            var availLength = dstLength - dstLength % byteSize; // rounded length

            if (srcSpan.Length < availLength)
            {
                var dstSpan = SendBuffer.AsSpan(Position, srcSpan.Length);
                srcSpan.CopyTo(dstSpan);
                Advance(srcSpan.Length);
                srcSpan = srcSpan.Slice(srcSpan.Length);
            }
            else
            {
                var dstSpan = SendBuffer.AsSpan(Position, availLength);
                var reducedSpan = srcSpan.Slice(0, availLength);
                reducedSpan.CopyTo(dstSpan);
                Advance(availLength);
                srcSpan = srcSpan.Slice(availLength);
            }
        }
    }

    private void PutBinaryManyBE<T>(ReadOnlySpan<T> value) where T : struct
    {
        foreach (var t in value) PutBinaryBE(t);
    }

    private void PutBinaryMany<T>(ReadOnlySpan<T> value) where T : struct
    {
        if (BitConverter.IsLittleEndian)
            PutBinaryManyLE(value);
        else
            PutBinaryManyBE(value);
    }

    private void PutBinary<T>(T value) where T : struct
    {
        if (BitConverter.IsLittleEndian)
            PutBinaryLE(value);
        else
            PutBinaryBE(value);
    }

    public override IBuffer Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        GuardAgainstNonDoubleTypes(typeof(T));
        SetTableIfAppropriate();
        PutArrayOfDoubleHeader(name);
        Put(1);
        PutBinary(Convert.ToUInt32(value.Length));
        PutBinaryMany(value);

        return this;
    }

    public override IBuffer Column(ReadOnlySpan<char> name, Array value)
    {
        var type = value.GetType().GetElementType();
        GuardAgainstNonDoubleTypes(type);
        SetTableIfAppropriate();
        PutArrayOfDoubleHeader(name);

        Put((byte)value.Rank); // dims count

        for (var i = 0; i < value.Rank; i++) PutBinary(Convert.ToUInt32(value.GetLength(i)));

        foreach (double d in value) PutBinary(d);

        return this;
    }

    private void PutBinaryDeferred<T>(out Span<T> span) where T : struct
    {
        var length = Marshal.SizeOf<T>();
        EnsureCapacity(length);
        span = MemoryMarshal.Cast<byte, T>(SendBuffer.AsSpan(Position, length));
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
        return SendBuffer.Length - Position;
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