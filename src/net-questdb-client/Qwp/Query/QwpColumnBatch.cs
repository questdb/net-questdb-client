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

using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using QuestDB.Enums;
using QuestDB.Qwp;

namespace QuestDB.Qwp.Query;

/// <summary>
///     Column-major view over a single decoded RESULT_BATCH. The instance — and every span
///     it returns — is reused across batches: do not store a reference past the
///     <c>onBatch</c> handler invocation, and copy any string / array data you need to keep.
/// </summary>
public sealed class QwpColumnBatch
{
    private readonly List<ColumnView> _columns = new();

    /// <summary>Server-assigned request id this batch belongs to.</summary>
    public long RequestId { get; internal set; }
    /// <summary>Monotonic batch sequence within the request; first batch is 0.</summary>
    public long BatchSeq { get; internal set; }
    /// <summary>Number of rows in this batch.</summary>
    public int RowCount { get; internal set; }

    /// <summary>Number of columns in the result set.</summary>
    public int ColumnCount => _columns.Count;

    /// <summary>Returns the wire-level column name at index <paramref name="col" />.</summary>
    public string GetColumnName(int col) => Col(col).Name;
    /// <summary>Returns the wire <see cref="QwpTypeCode" /> for column <paramref name="col" />.</summary>
    public QwpTypeCode GetColumnWireType(int col) => Col(col).TypeCode;
    /// <summary>Returns the decimal scale locked on this column (0 for non-decimal columns).</summary>
    public byte GetDecimalScale(int col) => Col(col).Scale;
    /// <summary>Returns the geohash precision in bits for this column (0 for non-geohash columns).</summary>
    public int GetGeohashPrecisionBits(int col) => Col(col).PrecisionBits;

    /// <summary>True if the value at (<paramref name="col" />, <paramref name="row" />) is NULL.</summary>
    public bool IsNull(int col, int row)
    {
        var c = Col(col);
        if ((uint)row >= (uint)RowCount)
        {
            throw new ArgumentOutOfRangeException(nameof(row),
                $"row index {row} out of range [0, {RowCount})");
        }
        if (c.NonNullIndex is null) return false;
        return c.NonNullIndex[row] < 0;
    }

    /// <summary>Returns the BOOLEAN at (<paramref name="col" />, <paramref name="row" />); <c>false</c> for NULL.</summary>
    public bool GetBoolValue(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return false;
        var byteIdx = i >> 3;
        var bit = i & 7;
        return (c.ValueBytes[byteIdx] & (1 << bit)) != 0;
    }

    /// <summary>Returns the BYTE (uint8) value; <c>0</c> for NULL.</summary>
    public byte GetByteValue(int col, int row) => GetFixedByte(col, row);
    /// <summary>Returns the BYTE reinterpreted as int8; <c>0</c> for NULL.</summary>
    public sbyte GetSByteValue(int col, int row) => unchecked((sbyte)GetFixedByte(col, row));

    /// <summary>Returns the SHORT (int16); <c>0</c> for NULL.</summary>
    public short GetShortValue(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return 0;
        return BinaryPrimitives.ReadInt16LittleEndian(c.ValueBytes.AsSpan(i * 2, 2));
    }

    /// <summary>Returns the CHAR (UTF-16 code unit); <c>'\0'</c> for NULL.</summary>
    public char GetCharValue(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return '\0';
        return (char)BinaryPrimitives.ReadUInt16LittleEndian(c.ValueBytes.AsSpan(i * 2, 2));
    }

    /// <summary>Returns the INT (int32); <c>0</c> for NULL.</summary>
    public int GetIntValue(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return 0;
        return BinaryPrimitives.ReadInt32LittleEndian(c.ValueBytes.AsSpan(i * 4, 4));
    }

    /// <summary>Returns the LONG (int64); <c>0</c> for NULL.</summary>
    public long GetLongValue(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return 0;
        return BinaryPrimitives.ReadInt64LittleEndian(c.ValueBytes.AsSpan(i * 8, 8));
    }

    /// <summary>Returns the FLOAT (32-bit); <c>0</c> for NULL.</summary>
    public float GetFloatValue(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return 0f;
        return BitConverter.Int32BitsToSingle(
            BinaryPrimitives.ReadInt32LittleEndian(c.ValueBytes.AsSpan(i * 4, 4)));
    }

    /// <summary>Returns the DOUBLE (64-bit); <c>0</c> for NULL.</summary>
    public double GetDoubleValue(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return 0d;
        return BitConverter.Int64BitsToDouble(
            BinaryPrimitives.ReadInt64LittleEndian(c.ValueBytes.AsSpan(i * 8, 8)));
    }

    /// <summary>Returns the lower 64 bits of a UUID; <c>0</c> for NULL.</summary>
    public long GetUuidLo(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Uuid)
        {
            throw new InvalidOperationException($"GetUuidLo requires a UUID column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0) return 0L;
        return BinaryPrimitives.ReadInt64LittleEndian(c.ValueBytes.AsSpan(i * 16, 8));
    }

    /// <summary>Returns the upper 64 bits of a UUID; <c>0</c> for NULL.</summary>
    public long GetUuidHi(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Uuid)
        {
            throw new InvalidOperationException($"GetUuidHi requires a UUID column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0) return 0L;
        return BinaryPrimitives.ReadInt64LittleEndian(c.ValueBytes.AsSpan(i * 16 + 8, 8));
    }

    /// <summary>Returns the unscaled int64 of a DECIMAL64 value; <c>0</c> for NULL.</summary>
    public long GetDecimal64UnscaledValue(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Decimal64)
        {
            throw new InvalidOperationException(
                $"GetDecimal64UnscaledValue requires a DECIMAL64 column, got {c.TypeCode}");
        }
        return GetLongValue(col, row);
    }

    /// <summary>Returns the lower 64 bits of a DECIMAL128 unscaled value; <c>0</c> for NULL.</summary>
    public long GetDecimal128Lo(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Decimal128)
        {
            throw new InvalidOperationException(
                $"GetDecimal128Lo requires a DECIMAL128 column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0) return 0L;
        return BinaryPrimitives.ReadInt64LittleEndian(c.ValueBytes.AsSpan(i * QwpConstants.Decimal128SizeBytes, 8));
    }

    /// <summary>Returns the upper 64 bits of a DECIMAL128 unscaled value; <c>0</c> for NULL.</summary>
    public long GetDecimal128Hi(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Decimal128)
        {
            throw new InvalidOperationException(
                $"GetDecimal128Hi requires a DECIMAL128 column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0) return 0L;
        return BinaryPrimitives.ReadInt64LittleEndian(c.ValueBytes.AsSpan(i * QwpConstants.Decimal128SizeBytes + 8, 8));
    }

    /// <summary>Returns the four 64-bit limbs (least to most significant) of a DECIMAL256 unscaled value; all zero for NULL.</summary>
    public void GetDecimal256(int col, int row, out long ll, out long lh, out long hl, out long hh)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Decimal256)
        {
            throw new InvalidOperationException(
                $"GetDecimal256 requires a DECIMAL256 column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0)
        {
            ll = lh = hl = hh = 0L;
            return;
        }
        var baseOff = i * QwpConstants.Decimal256SizeBytes;
        var bytes = c.ValueBytes.AsSpan(baseOff, QwpConstants.Decimal256SizeBytes);
        ll = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(0, 8));
        lh = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8, 8));
        hl = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(16, 8));
        hh = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(24, 8));
    }

    /// <summary>Returns the four 64-bit limbs of a LONG256 value (least to most significant); all zero for NULL.</summary>
    public void GetLong256(int col, int row, out long w0, out long w1, out long w2, out long w3)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Long256)
        {
            throw new InvalidOperationException(
                $"GetLong256 requires a LONG256 column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0)
        {
            w0 = w1 = w2 = w3 = 0L;
            return;
        }
        var baseOff = i * QwpConstants.Long256SizeBytes;
        var bytes = c.ValueBytes.AsSpan(baseOff, QwpConstants.Long256SizeBytes);
        w0 = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(0, 8));
        w1 = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8, 8));
        w2 = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(16, 8));
        w3 = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(24, 8));
    }

    /// <summary>Returns a LONG256 value as a non-negative <see cref="BigInteger" />; <see cref="BigInteger.Zero" /> for NULL.</summary>
    /// <remarks>
    ///     <see cref="BigInteger.Zero" /> is also a legal non-null LONG256 value. Use <see cref="IsNull" />
    ///     to disambiguate.
    /// </remarks>
    public BigInteger GetLong256(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Long256)
        {
            throw new InvalidOperationException(
                $"GetLong256 requires a LONG256 column, got {c.TypeCode}");
        }
        if (IsNull(col, row)) return BigInteger.Zero;
        Span<byte> bytes = stackalloc byte[QwpConstants.Long256SizeBytes];
        var i = DenseIndex(c, row);
        c.ValueBytes.AsSpan(i * QwpConstants.Long256SizeBytes, QwpConstants.Long256SizeBytes).CopyTo(bytes);
        return new BigInteger(bytes, isUnsigned: true, isBigEndian: false);
    }

    /// <summary>Returns the GEOHASH bits packed into a long; <c>-1</c> for NULL (matches QuestDB's all-bits-set sentinel).</summary>
    public long GetGeohashValue(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Geohash)
        {
            throw new InvalidOperationException(
                $"GetGeohashValue requires a GEOHASH column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0) return -1L;
        var stride = (c.PrecisionBits + 7) >> 3;
        var baseOff = i * stride;
        long v = 0;
        for (var b = 0; b < stride; b++)
        {
            v |= ((long)c.ValueBytes[baseOff + b] & 0xFF) << (b * 8);
        }
        return v;
    }

    /// <summary>Returns a UUID as <see cref="Guid" />; <see cref="Guid.Empty" /> for NULL. Inverse of <c>QwpBindValues.SetUuid(int, Guid)</c>.</summary>
    /// <remarks>
    ///     <see cref="Guid.Empty" /> is also a legal non-null UUID value (the spec defines NULL as both halves
    ///     equal to <c>long.MinValue</c>, not all zeros). Use <see cref="IsNull" /> to disambiguate.
    /// </remarks>
    public Guid GetUuid(int col, int row)
    {
        if (IsNull(col, row)) return Guid.Empty;
        var lo = GetUuidLo(col, row);
        var hi = GetUuidHi(col, row);
        Span<byte> wire = stackalloc byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(wire.Slice(0, 8), lo);
        BinaryPrimitives.WriteInt64LittleEndian(wire.Slice(8, 8), hi);
        Span<byte> ms = stackalloc byte[16];
        ms[0] = wire[12]; ms[1] = wire[13]; ms[2] = wire[14]; ms[3] = wire[15];
        ms[4] = wire[10]; ms[5] = wire[11];
        ms[6] = wire[8];  ms[7] = wire[9];
        ms[8] = wire[7];  ms[9] = wire[6];
        ms[10] = wire[5]; ms[11] = wire[4]; ms[12] = wire[3]; ms[13] = wire[2]; ms[14] = wire[1]; ms[15] = wire[0];
        return new Guid(ms);
    }

    /// <summary>Returns a TIMESTAMP / TIMESTAMP_NANOS as int64; caller must consult <see cref="GetColumnWireType" /> to know the unit.</summary>
    public long GetTimestampValue(int col, int row) => GetLongValue(col, row);

    /// <summary>Returns a DATE as milliseconds since Unix epoch; <c>0</c> for NULL.</summary>
    public long GetDateValue(int col, int row) => GetLongValue(col, row);

    /// <summary>IPv4 address as a packed int (4 bytes little-endian on the wire).</summary>
    public int GetIPv4Value(int col, int row) => GetIntValue(col, row);

    /// <summary>Returns the raw bytes of a BINARY value. Span is valid for the duration of the handler.</summary>
    public ReadOnlySpan<byte> GetBinarySpan(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode is not QwpTypeCode.Binary)
        {
            throw new InvalidOperationException(
                $"GetBinarySpan requires a BINARY column, got {c.TypeCode}");
        }

        var i = DenseIndex(c, row);
        if (i < 0) return ReadOnlySpan<byte>.Empty;

        var start = c.StringOffsets![i];
        var end = c.StringOffsets[i + 1];
        return c.StringHeap.AsSpan(start, end - start);
    }

    /// <summary>Returns the UTF-8 bytes of a VARCHAR / SYMBOL value. Span is valid for the duration of the handler.</summary>
    public ReadOnlySpan<byte> GetStringSpan(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode is not (QwpTypeCode.Varchar or QwpTypeCode.Symbol))
        {
            throw new InvalidOperationException(
                $"GetStringSpan requires a VARCHAR or SYMBOL column, got {c.TypeCode}");
        }

        var i = DenseIndex(c, row);
        if (i < 0) return ReadOnlySpan<byte>.Empty;

        if (c.TypeCode == QwpTypeCode.Symbol)
        {
            return c.SymbolDict!.GetUtf8(c.SymbolIds![i]);
        }

        var start = c.StringOffsets![i];
        var end = c.StringOffsets[i + 1];
        return c.StringHeap.AsSpan(start, end - start);
    }

    /// <summary>
    ///     Best-effort string rendering of any column. Allocates. Use the typed accessors
    ///     (<see cref="GetLongValue" /> etc.) when you know the column type.
    /// </summary>
    public string? GetString(int col, int row)
    {
        if (IsNull(col, row)) return null;
        var c = Col(col);
        return c.TypeCode switch
        {
            QwpTypeCode.Varchar or QwpTypeCode.Symbol => QwpConstants.StrictUtf8.GetString(GetStringSpan(col, row)),
            QwpTypeCode.Boolean => GetBoolValue(col, row).ToString(),
            QwpTypeCode.Byte => GetByteValue(col, row).ToString(),
            QwpTypeCode.Short => GetShortValue(col, row).ToString(),
            QwpTypeCode.Char => GetCharValue(col, row).ToString(),
            QwpTypeCode.Int => GetIntValue(col, row).ToString(),
            QwpTypeCode.IPv4 => FormatIPv4(GetIPv4Value(col, row)),
            QwpTypeCode.Long or QwpTypeCode.Date or QwpTypeCode.Timestamp or QwpTypeCode.TimestampNanos
                => GetLongValue(col, row).ToString(),
            QwpTypeCode.Float => GetFloatValue(col, row).ToString("R"),
            QwpTypeCode.Double => GetDoubleValue(col, row).ToString("R"),
            QwpTypeCode.Binary => Convert.ToHexString(GetBinarySpan(col, row)),
            QwpTypeCode.Uuid => GetUuid(col, row).ToString(),
            _ => $"<{c.TypeCode}>",
        };
    }

    private static string FormatIPv4(int packed)
    {
        return $"{(byte)(packed >> 24)}.{(byte)(packed >> 16)}.{(byte)(packed >> 8)}.{(byte)packed}";
    }

    /// <summary>Renders a SYMBOL value as a managed string; <c>null</c> for NULL.</summary>
    public string? GetSymbol(int col, int row) => GetString(col, row);

    /// <summary>Returns the dictionary id of a SYMBOL value; <c>-1</c> for NULL.</summary>
    public int GetSymbolId(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode != QwpTypeCode.Symbol)
        {
            throw new InvalidOperationException(
                $"GetSymbolId requires a SYMBOL column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0) return -1;
        return c.SymbolIds![i];
    }

    /// <summary>Returns the dimensionality of a *_ARRAY value; <c>0</c> for NULL.</summary>
    public int GetArrayNDims(int col, int row)
    {
        var (_, _, _, nDims) = ArraySpan(col, row);
        return nDims < 0 ? 0 : nDims;
    }

    /// <summary>
    /// Returns the DOUBLE_ARRAY element bytes as a span over the column scratch; valid only for the duration of the handler.
    /// <para><b>Alignment:</b> the underlying byte offset is not guaranteed to be aligned to <c>sizeof(double)</c>.
    /// Indexing the span (<c>span[i]</c>) works on x86_64 and ARM64 (unaligned loads are single-cycle) but is undefined
    /// behaviour on strict-alignment targets. For portable bulk consumption copy via <c>span.ToArray()</c> or read with
    /// <c>BinaryPrimitives.ReadDoubleLittleEndian</c> on the underlying byte span.</para>
    /// </summary>
    public ReadOnlySpan<double> GetDoubleArraySpan(int col, int row)
    {
        var (heap, start, end, nDims) = ArraySpan(col, row);
        if (nDims < 0) return ReadOnlySpan<double>.Empty;
        var valueByteCount = ArrayValueByteCount(start, end, nDims, elementBytes: 8);
        return System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, double>(heap.AsSpan(start + 1 + nDims * 4, valueByteCount));
    }

    /// <summary>
    /// Returns the LONG_ARRAY element bytes as a span over the column scratch; valid only for the duration of the handler.
    /// <para><b>Alignment:</b> same caveat as <see cref="GetDoubleArraySpan"/> — the span start may not be aligned to
    /// <c>sizeof(long)</c>; safe on x86_64 / ARM64, UB on strict-alignment targets. Use <c>span.ToArray()</c> or
    /// <c>BinaryPrimitives.ReadInt64LittleEndian</c> for portable reads.</para>
    /// </summary>
    public ReadOnlySpan<long> GetLongArraySpan(int col, int row)
    {
        var (heap, start, end, nDims) = ArraySpan(col, row);
        if (nDims < 0) return ReadOnlySpan<long>.Empty;
        var valueByteCount = ArrayValueByteCount(start, end, nDims, elementBytes: 8);
        return System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, long>(heap.AsSpan(start + 1 + nDims * 4, valueByteCount));
    }

    private static int ArrayValueByteCount(int start, int end, int nDims, int elementBytes)
    {
        var valuesStart = start + 1 + nDims * 4;
        var valueByteCount = end - valuesStart;
        if (valueByteCount < 0 || valueByteCount % elementBytes != 0)
        {
            throw new QwpDecodeException(
                $"array value bytes {valueByteCount} not a whole multiple of element size {elementBytes}");
        }
        return valueByteCount;
    }

    /// <summary>Allocates and returns the elements of a DOUBLE_ARRAY value; empty array for NULL. Prefer <see cref="GetDoubleArraySpan" /> on hot paths.</summary>
    public double[] GetDoubleArrayElements(int col, int row)
    {
        var span = GetDoubleArraySpan(col, row);
        return span.IsEmpty ? Array.Empty<double>() : span.ToArray();
    }

    /// <summary>Allocates and returns the elements of a LONG_ARRAY value; empty array for NULL. Prefer <see cref="GetLongArraySpan" /> on hot paths.</summary>
    public long[] GetLongArrayElements(int col, int row)
    {
        var span = GetLongArraySpan(col, row);
        return span.IsEmpty ? Array.Empty<long>() : span.ToArray();
    }

    /// <summary>Allocates and returns the per-dimension shape of an array column; empty array for NULL.</summary>
    public int[] GetArrayShape(int col, int row)
    {
        var (heap, start, _, nDims) = ArraySpan(col, row);
        if (nDims <= 0) return Array.Empty<int>();
        var shape = new int[nDims];
        for (var d = 0; d < nDims; d++)
        {
            shape[d] = BinaryPrimitives.ReadInt32LittleEndian(heap.AsSpan(start + 1 + d * 4, 4));
        }
        return shape;
    }

    private (byte[] Heap, int Start, int End, int NDims) ArraySpan(int col, int row)
    {
        var c = Col(col);
        if (c.TypeCode is not (QwpTypeCode.DoubleArray or QwpTypeCode.LongArray))
        {
            throw new InvalidOperationException(
                $"array accessors require a DOUBLE_ARRAY or LONG_ARRAY column, got {c.TypeCode}");
        }
        var i = DenseIndex(c, row);
        if (i < 0) return (Array.Empty<byte>(), 0, 0, -1);
        var start = c.StringOffsets![i];
        var end = c.StringOffsets[i + 1];
        if (start < 0 || start >= end || end > c.ValueBytes.Length)
        {
            throw new QwpDecodeException(
                $"array row offsets out of range: start={start} end={end} heapLen={c.ValueBytes.Length}");
        }
        return (c.ValueBytes, start, end, c.ValueBytes[start]);
    }

    /// <summary>Returns the number of entries in this column's symbol dictionary; <c>0</c> for non-symbol columns.</summary>
    public int GetSymbolDictSize(int col) => Col(col).SymbolDict?.Size ?? 0;

    /// <summary>Looks up a symbol by dictionary id; returns empty span for non-symbol columns.</summary>
    public ReadOnlySpan<byte> GetSymbolForId(int col, int dictId)
    {
        var dict = Col(col).SymbolDict;
        return dict is null ? ReadOnlySpan<byte>.Empty : dict.GetUtf8(dictId);
    }

    internal void Reset()
    {
        RequestId = 0;
        BatchSeq = 0;
        RowCount = 0;
        for (var i = 0; i < _columns.Count; i++) _columns[i].Reset();
    }

    internal ColumnView ConfigureColumn(int idx, string name, QwpTypeCode typeCode, byte scale, byte precisionBits)
    {
        if (idx < _columns.Count)
        {
            var existing = _columns[idx];
            existing.Reconfigure(name, typeCode, scale, precisionBits);
            return existing;
        }

        var c = new ColumnView(name, typeCode, scale, precisionBits);
        _columns.Add(c);
        return c;
    }

    internal ColumnView GetColumn(int col) => _columns[col];

    internal void TrimToColumnCount(int desired)
    {
        if (_columns.Count <= desired) return;
        _columns.RemoveRange(desired, _columns.Count - desired);
    }

    private ColumnView Col(int col)
    {
        if ((uint)col >= (uint)_columns.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(col), $"column index {col} out of range [0, {_columns.Count})");
        }
        return _columns[col];
    }

    private int DenseIndex(ColumnView c, int row)
    {
        // Bounds check guards against reading residue from a previous batch — scratches survive Reset.
        if ((uint)row >= (uint)RowCount)
        {
            throw new ArgumentOutOfRangeException(nameof(row),
                $"row index {row} out of range [0, {RowCount})");
        }
        if (c.NonNullIndex is null) return row;
        return c.NonNullIndex[row];
    }

    private byte GetFixedByte(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return 0;
        return c.ValueBytes[i];
    }
}

internal sealed class ColumnView
{
    public ColumnView(string name, QwpTypeCode typeCode, byte scale, byte precisionBits)
    {
        Name = name;
        TypeCode = typeCode;
        Scale = scale;
        PrecisionBits = precisionBits;
    }

    public string Name { get; private set; }
    public QwpTypeCode TypeCode { get; private set; }
    public byte Scale { get; internal set; }
    public byte PrecisionBits { get; internal set; }

    public int[]? NonNullIndex { get; set; }
    public int[]? StringOffsets { get; set; }
    public int[]? SymbolIds { get; set; }

    public byte[] ValueBytes { get; set; } = Array.Empty<byte>();
    public byte[] StringHeap { get; set; } = Array.Empty<byte>();

    internal int[] NonNullIndexBuf = Array.Empty<int>();
    internal int[] StringOffsetsBuf = Array.Empty<int>();
    internal int[] SymbolIdsBuf = Array.Empty<int>();

    public QwpEgressSymbolDict? SymbolDict { get; set; }

    public void Reconfigure(string name, QwpTypeCode typeCode, byte scale, byte precisionBits)
    {
        Name = name;
        TypeCode = typeCode;
        Scale = scale;
        PrecisionBits = precisionBits;
        Reset();
    }

    // Per-batch sentinels reset; scratch buffers survive so the decoder pools across batches.
    public void Reset()
    {
        NonNullIndex = null;
        StringOffsets = null;
        SymbolIds = null;
        SymbolDict = null;
    }
}

internal sealed class QwpEgressSymbolDict
{
    private readonly List<int> _offsets = new() { 0 };
    private byte[] _heap = Array.Empty<byte>();
    private int _heapLen;

    public int Size => _offsets.Count - 1;

    public void Reset()
    {
        _offsets.Clear();
        _offsets.Add(0);
        _heapLen = 0;
    }

    public void TruncateTo(int size)
    {
        if (size < 0 || size > Size)
        {
            throw new ArgumentOutOfRangeException(nameof(size));
        }
        if (size == Size) return;
        _offsets.RemoveRange(size + 1, _offsets.Count - (size + 1));
        _heapLen = _offsets[^1];
    }

    public void AppendEntry(ReadOnlySpan<byte> utf8)
    {
        if (Size >= QwpConstants.MaxConnSymbolDictEntries)
        {
            throw new QwpDecodeException(
                $"symbol dict entry count exceeds {QwpConstants.MaxConnSymbolDictEntries}");
        }
        var needed = _heapLen + utf8.Length;
        if (needed > QwpConstants.MaxConnSymbolDictHeapBytes)
        {
            throw new QwpDecodeException(
                $"symbol dict heap exceeds {QwpConstants.MaxConnSymbolDictHeapBytes} bytes");
        }
        if (needed > _heap.Length)
        {
            var grown = new byte[Math.Max(needed, Math.Max(64, _heap.Length * 2))];
            Buffer.BlockCopy(_heap, 0, grown, 0, _heapLen);
            _heap = grown;
        }
        utf8.CopyTo(_heap.AsSpan(_heapLen));
        _heapLen += utf8.Length;
        _offsets.Add(_heapLen);
    }

    public ReadOnlySpan<byte> GetUtf8(int id)
    {
        if ((uint)id >= (uint)Size)
        {
            throw new ArgumentOutOfRangeException(nameof(id), $"symbol id {id} out of range [0, {Size})");
        }

        var start = _offsets[id];
        var end = _offsets[id + 1];
        return _heap.AsSpan(start, end - start);
    }
}
