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
using System.Text;
using QuestDB.Enums;

namespace QuestDB.Qwp.Query;

/// <summary>
///     Column-major view over a single decoded RESULT_BATCH. Lifetime is bounded by the
///     <c>onBatch</c> handler invocation: spans returned from string accessors are invalidated
///     when the handler returns.
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
    public short GetShortValue(int col, int row) => GetFixed<short>(col, row, sizeof(short));
    /// <summary>Returns the CHAR (UTF-16 code unit); <c>'\0'</c> for NULL.</summary>
    public char GetCharValue(int col, int row) => (char)GetFixed<ushort>(col, row, sizeof(ushort));
    /// <summary>Returns the INT (int32); <c>0</c> for NULL.</summary>
    public int GetIntValue(int col, int row) => GetFixed<int>(col, row, sizeof(int));
    /// <summary>Returns the LONG (int64); <c>0</c> for NULL.</summary>
    public long GetLongValue(int col, int row) => GetFixed<long>(col, row, sizeof(long));
    /// <summary>Returns the FLOAT (32-bit); <c>0</c> for NULL.</summary>
    public float GetFloatValue(int col, int row) => GetFixed<float>(col, row, sizeof(float));
    /// <summary>Returns the DOUBLE (64-bit); <c>0</c> for NULL.</summary>
    public double GetDoubleValue(int col, int row) => GetFixed<double>(col, row, sizeof(double));

    /// <summary>Returns a TIMESTAMP / TIMESTAMP_NANOS as int64; caller must consult <see cref="GetColumnWireType" /> to know the unit.</summary>
    public long GetTimestampValue(int col, int row) => GetFixed<long>(col, row, sizeof(long));

    /// <summary>Returns a DATE as milliseconds since Unix epoch; <c>0</c> for NULL.</summary>
    public long GetDateValue(int col, int row) => GetFixed<long>(col, row, sizeof(long));

    /// <summary>IPv4 address as a packed int (4 bytes little-endian on the wire).</summary>
    public int GetIPv4Value(int col, int row) => GetFixed<int>(col, row, sizeof(int));

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
            var id = BinaryPrimitives.ReadInt32LittleEndian(c.ValueBytes.AsSpan(i * 4, 4));
            return c.SymbolDict!.GetUtf8(id);
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
            QwpTypeCode.Varchar or QwpTypeCode.Symbol => Encoding.UTF8.GetString(GetStringSpan(col, row)),
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
        var i = DenseIndex(c, row);
        if (i < 0) return -1;
        return BinaryPrimitives.ReadInt32LittleEndian(c.ValueBytes.AsSpan(i * 4, 4));
    }

    /// <summary>Returns the dimensionality of a *_ARRAY value; <c>0</c> for NULL.</summary>
    public int GetArrayNDims(int col, int row)
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return 0;
        var start = c.StringOffsets![i];
        return c.ValueBytes[start];
    }

    /// <summary>Allocates and returns the elements of a DOUBLE_ARRAY value; empty array for NULL.</summary>
    public double[] GetDoubleArrayElements(int col, int row)
    {
        var (heap, start, end, nDims) = ArraySpan(col, row);
        if (nDims < 0) return Array.Empty<double>();
        var valuesStart = start + 1 + nDims * 4;
        var valueByteCount = end - valuesStart;
        var elementCount = valueByteCount / 8;
        var result = new double[elementCount];
        for (var i = 0; i < elementCount; i++)
        {
            result[i] = BitConverter.Int64BitsToDouble(
                BinaryPrimitives.ReadInt64LittleEndian(heap.AsSpan(valuesStart + i * 8, 8)));
        }
        return result;
    }

    /// <summary>Allocates and returns the elements of a LONG_ARRAY value; empty array for NULL.</summary>
    public long[] GetLongArrayElements(int col, int row)
    {
        var (heap, start, end, nDims) = ArraySpan(col, row);
        if (nDims < 0) return Array.Empty<long>();
        var valuesStart = start + 1 + nDims * 4;
        var valueByteCount = end - valuesStart;
        var elementCount = valueByteCount / 8;
        var result = new long[elementCount];
        for (var i = 0; i < elementCount; i++)
        {
            result[i] = BinaryPrimitives.ReadInt64LittleEndian(heap.AsSpan(valuesStart + i * 8, 8));
        }
        return result;
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
        var i = DenseIndex(c, row);
        if (i < 0) return (Array.Empty<byte>(), 0, 0, -1);
        var start = c.StringOffsets![i];
        var end = c.StringOffsets[i + 1];
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

    private T GetFixed<T>(int col, int row, int stride) where T : struct
    {
        var c = Col(col);
        var i = DenseIndex(c, row);
        if (i < 0) return default;
        var span = c.ValueBytes.AsSpan(i * stride, stride);
        return ReadFixedLittleEndian<T>(span);
    }

    private static T ReadFixedLittleEndian<T>(ReadOnlySpan<byte> span) where T : struct
    {
        if (typeof(T) == typeof(short)) return (T)(object)BinaryPrimitives.ReadInt16LittleEndian(span);
        if (typeof(T) == typeof(ushort)) return (T)(object)BinaryPrimitives.ReadUInt16LittleEndian(span);
        if (typeof(T) == typeof(int)) return (T)(object)BinaryPrimitives.ReadInt32LittleEndian(span);
        if (typeof(T) == typeof(uint)) return (T)(object)BinaryPrimitives.ReadUInt32LittleEndian(span);
        if (typeof(T) == typeof(long)) return (T)(object)BinaryPrimitives.ReadInt64LittleEndian(span);
        if (typeof(T) == typeof(ulong)) return (T)(object)BinaryPrimitives.ReadUInt64LittleEndian(span);
        if (typeof(T) == typeof(float))
            return (T)(object)BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(span));
        if (typeof(T) == typeof(double))
            return (T)(object)BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(span));
        throw new NotSupportedException(typeof(T).Name);
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

    public byte[] ValueBytes { get; set; } = Array.Empty<byte>();
    public byte[] StringHeap { get; set; } = Array.Empty<byte>();

    internal int[] NonNullIndexBuf = Array.Empty<int>();
    internal int[] StringOffsetsBuf = Array.Empty<int>();

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

    public void AppendEntry(ReadOnlySpan<byte> utf8)
    {
        var needed = _heapLen + utf8.Length;
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
