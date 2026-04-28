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

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Shared encoding helpers used by <c>QwpUdpSender</c> and <c>QwpWebSocketSender</c>
///     to lower the four typed Column overloads (decimal + array variants) onto the
///     <see cref="QwpTableBuffer.ColumnBuffer"/> Add* surface.
/// </summary>
internal static class QwpColumnEncoding
{
    private const int DecimalSignMask = unchecked((int)0x8000_0000);
    private const int DecimalScaleMask = 0x00FF_0000;
    private const int DecimalScaleShift = 16;

    /// <summary>
    ///     Splits a .NET <see cref="decimal"/> into the QWP Decimal64/Decimal128 fields.
    ///     The .NET decimal carries a 96-bit unsigned mantissa plus sign and a 0..28
    ///     scale, so the value always fits in a 128-bit signed integer — there is no
    ///     overflow path. <paramref name="fitsIn64"/> is true when the 128-bit signed
    ///     value would be losslessly representable as a 64-bit signed integer; the
    ///     caller routes those to <c>AddDecimal64</c> and the rest to <c>AddDecimal128</c>.
    /// </summary>
    public static void EncodeDecimal(
        decimal value,
        out byte scale,
        out long high,
        out long low,
        out bool fitsIn64)
    {
        Span<int> parts = stackalloc int[4];
        decimal.GetBits(value, parts);

        var flags = parts[3];
        scale = (byte)((flags & DecimalScaleMask) >> DecimalScaleShift);
        var negative = (flags & DecimalSignMask) != 0 && value != 0m;

        var low32 = (uint)parts[0];
        var mid32 = (uint)parts[1];
        var high32 = (uint)parts[2];

        // 96-bit unsigned mantissa packed into a 128-bit unsigned (high half: only
        // bits 64..95 carry data, top 32 bits are zero).
        var lowU = ((ulong)mid32 << 32) | low32;
        var highU = (ulong)high32;

        if (negative)
        {
            // Two's-complement negation across the full 128-bit value: invert both
            // halves, add 1 to the low half, propagate carry to the high half.
            unchecked
            {
                var notLow = ~lowU;
                var notHigh = ~highU;
                var negLow = notLow + 1UL;
                var carry = negLow == 0UL ? 1UL : 0UL;
                var negHigh = notHigh + carry;
                lowU = negLow;
                highU = negHigh;
            }
        }

        high = unchecked((long)highU);
        low = unchecked((long)lowU);

        // The 128-bit value fits in a signed long when the top 64 bits are the
        // sign-extension of bit 63 of the bottom 64 bits. Equivalently:
        //   positive small => high == 0 and low >= 0
        //   negative small => high == -1 (all ones) and low < 0
        fitsIn64 = (high == 0L && low >= 0L) || (high == -1L && low < 0L);
    }

    /// <summary>
    ///     Returns the QWP wire-type code for an array element type. Throws
    ///     <see cref="IngressError"/> for unsupported element types so callers can
    ///     surface a clear error rather than silently dropping values.
    /// </summary>
    public static byte WireTypeForArrayElement(Type elementType)
    {
        if (elementType == typeof(double)) return QwpConstants.TYPE_DOUBLE_ARRAY;
        if (elementType == typeof(long)) return QwpConstants.TYPE_LONG_ARRAY;
        throw new IngressError(ErrorCode.InvalidApiCall,
            $"unsupported array element type {elementType.Name}; QWP supports double and long arrays");
    }

    /// <summary>
    ///     Routes an <see cref="Array"/> (rank 1 or 2) to the matching
    ///     <see cref="QwpTableBuffer.ColumnBuffer"/> Add overload. Caller has
    ///     already created the column with the correct wire type via
    ///     <see cref="WireTypeForArrayElement"/>.
    /// </summary>
    public static void AddArray(QwpTableBuffer.ColumnBuffer col, Array value)
    {
        var elementType = value.GetType().GetElementType()
            ?? throw new IngressError(ErrorCode.InvalidApiCall, "array has no element type");

        if (elementType == typeof(double))
        {
            switch (value.Rank)
            {
                case 1: col.AddDoubleArray((double[])value); return;
                case 2: col.AddDoubleArray((double[,])value); return;
                default: throw RankUnsupported(value.Rank);
            }
        }
        if (elementType == typeof(long))
        {
            switch (value.Rank)
            {
                case 1: col.AddLongArray((long[])value); return;
                case 2: col.AddLongArray((long[,])value); return;
                default: throw RankUnsupported(value.Rank);
            }
        }
        throw new IngressError(ErrorCode.InvalidApiCall,
            $"unsupported array element type {elementType.Name}; QWP supports double and long arrays");
    }

    /// <summary>
    ///     Routes an <see cref="IEnumerable{T}"/> + shape to the column's array Add
    ///     path. Materialises the enumerable once and validates that its element
    ///     count matches the product of the supplied shape.
    /// </summary>
    /// <remarks>
    ///     §1.5 fast paths skip boxing + double-allocation when the input is already
    ///     a <c>T[]</c> or <c>List&lt;T&gt;</c>:
    ///     <list type="bullet">
    ///         <item><c>T[]</c> — pass straight to the column's span overload.</item>
    ///         <item><c>List&lt;T&gt;</c> — alias the backing span via
    ///             <see cref="CollectionsMarshal.AsSpan{T}"/>, no copy.</item>
    ///         <item>generic <c>IEnumerable&lt;T&gt;</c> — copy into a typed
    ///             <c>List&lt;double&gt;</c> / <c>List&lt;long&gt;</c> directly via
    ///             <see cref="Unsafe.As{TFrom,TTo}"/> reinterpret, skipping the
    ///             <c>(double)(object)item</c> box-and-unbox round-trip.</item>
    ///     </list>
    /// </remarks>
    public static void AddArray<T>(QwpTableBuffer.ColumnBuffer col, IEnumerable<T> value, IEnumerable<int> shape)
        where T : struct
    {
        if (typeof(T) != typeof(double) && typeof(T) != typeof(long))
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"unsupported array element type {typeof(T).Name}; QWP supports double and long arrays");
        }

        var shapeList = new List<int>();
        long expected = 1;
        foreach (var dim in shape)
        {
            if (dim <= 0)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    $"array shape dimensions must be > 0, got {dim}");
            }
            shapeList.Add(dim);
            expected *= dim;
        }

        if (typeof(T) == typeof(double))
        {
            ReadOnlySpan<double> dataSpan;
            List<double>? owned = null;
            if (value is double[] arr)
            {
                dataSpan = arr;
            }
            else if (value is List<double> list)
            {
                dataSpan = CollectionsMarshal.AsSpan(list);
            }
            else
            {
                owned = new List<double>();
                foreach (var item in value)
                {
                    var tmp = item;
                    owned.Add(Unsafe.As<T, double>(ref tmp));
                }
                dataSpan = CollectionsMarshal.AsSpan(owned);
            }
            if (dataSpan.Length != expected)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    $"array element count {dataSpan.Length} does not match shape product {expected}");
            }
            ApplyDoubleArray(col, shapeList, dataSpan);
        }
        else
        {
            ReadOnlySpan<long> dataSpan;
            List<long>? owned = null;
            if (value is long[] arr)
            {
                dataSpan = arr;
            }
            else if (value is List<long> list)
            {
                dataSpan = CollectionsMarshal.AsSpan(list);
            }
            else
            {
                owned = new List<long>();
                foreach (var item in value)
                {
                    var tmp = item;
                    owned.Add(Unsafe.As<T, long>(ref tmp));
                }
                dataSpan = CollectionsMarshal.AsSpan(owned);
            }
            if (dataSpan.Length != expected)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    $"array element count {dataSpan.Length} does not match shape product {expected}");
            }
            ApplyLongArray(col, shapeList, dataSpan);
        }
    }

    /// <summary>
    ///     Routes a 1D <see cref="ReadOnlySpan{T}"/> to the column's array Add path.
    ///     §1.6 — when <c>T == double</c> / <c>long</c>, reinterprets via
    ///     <see cref="MemoryMarshal.Cast{TFrom,TTo}(ReadOnlySpan{TFrom})"/> and feeds
    ///     the new span overload on <see cref="QwpTableBuffer.ColumnBuffer"/>, skipping
    ///     the per-element copy + array allocation the previous path imposed.
    /// </summary>
    public static void AddArray<T>(QwpTableBuffer.ColumnBuffer col, ReadOnlySpan<T> value)
        where T : struct
    {
        if (typeof(T) == typeof(double))
        {
            col.AddDoubleArray(MemoryMarshal.Cast<T, double>(value));
            return;
        }
        if (typeof(T) == typeof(long))
        {
            col.AddLongArray(MemoryMarshal.Cast<T, long>(value));
            return;
        }
        throw new IngressError(ErrorCode.InvalidApiCall,
            $"unsupported array element type {typeof(T).Name}; QWP supports double and long arrays");
    }

    private static void ApplyDoubleArray(QwpTableBuffer.ColumnBuffer col, List<int> shape, ReadOnlySpan<double> data)
    {
        if (shape.Count == 1)
        {
            col.AddDoubleArray(data);
            return;
        }
        if (shape.Count == 2)
        {
            var arr = new double[shape[0], shape[1]];
            var idx = 0;
            for (var i = 0; i < shape[0]; i++)
            for (var j = 0; j < shape[1]; j++)
                arr[i, j] = data[idx++];
            col.AddDoubleArray(arr);
            return;
        }
        throw RankUnsupported(shape.Count);
    }

    private static void ApplyLongArray(QwpTableBuffer.ColumnBuffer col, List<int> shape, ReadOnlySpan<long> data)
    {
        if (shape.Count == 1)
        {
            col.AddLongArray(data);
            return;
        }
        if (shape.Count == 2)
        {
            var arr = new long[shape[0], shape[1]];
            var idx = 0;
            for (var i = 0; i < shape[0]; i++)
            for (var j = 0; j < shape[1]; j++)
                arr[i, j] = data[idx++];
            col.AddLongArray(arr);
            return;
        }
        throw RankUnsupported(shape.Count);
    }

    private static IngressError RankUnsupported(int rank) =>
        new IngressError(ErrorCode.InvalidApiCall,
            $"array rank {rank} not supported by QWP; only 1D and 2D arrays are accepted");
}
