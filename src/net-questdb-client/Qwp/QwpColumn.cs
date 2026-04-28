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
using System.Runtime.InteropServices;
using System.Text;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     A single columnar accumulator inside a <see cref="QwpTableBuffer" />.
/// </summary>
/// <remarks>
///     Storage strategy: separate slices per concern (fixed-width data, bit-packed booleans,
///     string offsets+data, symbol ids, null bitmap). One column type uses one or two of these
///     slots — never all of them.
///     <para />
///     Non-null values are stored densely; null status is tracked in <see cref="NullBitmap" />,
///     allocated lazily on the first null. For columns that never see a null, no bitmap is
///     allocated and the on-wire <c>null_flag</c> stays at <c>0x00</c>.
///     <para />
///     The first non-null append locks the column's type code; subsequent appends of the wrong
///     type throw <see cref="IngressError" />.
/// </remarks>
internal sealed class QwpColumn
{
    private const int InitialFixedCapacity = 64;
    private const int InitialStringCapacity = 64;
    private const int InitialSymbolCapacity = 32;

    /// <summary>
    ///     Constructs a new column with the given name. Type is set by the first non-null append.
    /// </summary>
    /// <param name="name">The column name as it appears on the wire (UTF-8). Empty string denotes the designated timestamp.</param>
    /// <param name="initialNullRows">
    ///     Number of leading null rows to backfill — the table's row count at the moment the column
    ///     was created. Each leading row gets a null bit set.
    /// </param>
    public QwpColumn(string name, int initialNullRows)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        if (initialNullRows < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialNullRows));
        }

        if (initialNullRows > 0)
        {
            EnsureBitmapCapacity(initialNullRows);
            for (var r = 0; r < initialNullRows; r++)
            {
                MarkBit(r);
            }

            RowCount = initialNullRows;
            NullCount = initialNullRows;
        }
    }

    /// <summary>Column name as it appears on the wire.</summary>
    public string Name { get; }

    /// <summary>Column type code, set on the first non-null append.</summary>
    public QwpTypeCode TypeCode { get; private set; }

    /// <summary>Whether <see cref="TypeCode" /> has been set.</summary>
    public bool IsTyped { get; private set; }

    /// <summary>Total rows tracked by this column, including nulls.</summary>
    public int RowCount { get; private set; }

    /// <summary>Number of nulls observed so far.</summary>
    public int NullCount { get; private set; }

    /// <summary>Non-null value count (<c>RowCount - NullCount</c>).</summary>
    public int NonNullCount => RowCount - NullCount;

    /// <summary>
    ///     Per-row null bitmap (1 = null), LSB-first within each byte. Allocated lazily on the first null.
    ///     Length = <c>ceil(RowCount/8)</c>.
    /// </summary>
    /// <remarks>Public field so <see cref="Array.Resize" /> can take it by <c>ref</c>.</remarks>
    public byte[]? NullBitmap;

    // -- Fixed-width storage (BYTE/SHORT/INT/LONG/FLOAT/DOUBLE/DATE/TIMESTAMP/TIMESTAMP_NANOS/UUID/CHAR) --

    /// <summary>Raw bytes for fixed-width types. Length is bounded by <see cref="FixedLen" />.</summary>
    public byte[]? FixedData;

    /// <summary>Number of valid bytes in <see cref="FixedData" />.</summary>
    public int FixedLen;

    // -- Boolean (bit-packed) -----------------------------------------------------

    /// <summary>Bit-packed booleans, LSB-first within each byte. Length = <c>ceil(NonNullCount/8)</c>.</summary>
    public byte[]? BoolData;

    // -- VARCHAR storage ---------------------------------------------------------

    /// <summary>Offset array; length = <c>NonNullCount + 1</c> once at least one value present.</summary>
    public uint[]? StrOffsets;

    /// <summary>Concatenated UTF-8 string data, length = last offset.</summary>
    public byte[]? StrData;

    /// <summary>Number of bytes used in <see cref="StrData" />.</summary>
    public int StrLen;

    // -- SYMBOL storage (global ids) ---------------------------------------------

    /// <summary>Global symbol ids in append order; varint-encoded at frame time.</summary>
    public int[]? SymbolIds;

    // -- DECIMAL scale (locked on first non-null append) ------------------------

    /// <summary>Per-column decimal scale; emitted as a 1-byte prefix before the values on the wire.</summary>
    public byte DecimalScale;

    /// <summary>Whether <see cref="DecimalScale" /> has been set by the first non-null append.</summary>
    public bool DecimalScaleSet;

    // -- GEOHASH precision (locked on first non-null append) --------------------

    /// <summary>Geohash precision in bits; emitted as a varint prefix before the values on the wire.</summary>
    public int GeohashPrecisionBits;

    /// <summary>Whether <see cref="GeohashPrecisionBits" /> has been set by the first non-null append.</summary>
    public bool GeohashPrecisionSet;

    // -- Append API --------------------------------------------------------------

    /// <summary>Appends a null marker for a single row.</summary>
    public void AppendNull()
    {
        EnsureBitmapCapacity(RowCount + 1);
        MarkBit(RowCount);
        RowCount++;
        NullCount++;
    }

    /// <summary>Appends a boolean value.</summary>
    public void AppendBool(bool value)
    {
        AssertOrSetType(QwpTypeCode.Boolean);
        var bitIndex = NonNullCount;
        EnsureBoolCapacity(bitIndex + 1);
        if (value)
        {
            BoolData![bitIndex >> 3] |= (byte)(1 << (bitIndex & 7));
        }
        // else leave bit at 0; EnsureBoolCapacity grows zero-filled.

        AdvanceNonNull();
    }

    /// <summary>Appends a single signed byte.</summary>
    public void AppendByte(sbyte value)
    {
        AssertOrSetType(QwpTypeCode.Byte);
        EnsureFixedCapacity(FixedLen + 1);
        FixedData![FixedLen++] = (byte)value;
        AdvanceNonNull();
    }

    /// <summary>Appends a 16-bit signed integer (little-endian).</summary>
    public void AppendShort(short value)
    {
        AssertOrSetType(QwpTypeCode.Short);
        EnsureFixedCapacity(FixedLen + 2);
        BinaryPrimitives.WriteInt16LittleEndian(FixedData.AsSpan(FixedLen, 2), value);
        FixedLen += 2;
        AdvanceNonNull();
    }

    /// <summary>Appends a 32-bit signed integer (little-endian).</summary>
    public void AppendInt(int value)
    {
        AssertOrSetType(QwpTypeCode.Int);
        EnsureFixedCapacity(FixedLen + 4);
        BinaryPrimitives.WriteInt32LittleEndian(FixedData.AsSpan(FixedLen, 4), value);
        FixedLen += 4;
        AdvanceNonNull();
    }

    /// <summary>Appends a 64-bit signed integer (little-endian).</summary>
    public void AppendLong(long value)
    {
        AssertOrSetType(QwpTypeCode.Long);
        EnsureFixedCapacity(FixedLen + 8);
        BinaryPrimitives.WriteInt64LittleEndian(FixedData.AsSpan(FixedLen, 8), value);
        FixedLen += 8;
        AdvanceNonNull();
    }

    /// <summary>Appends an IEEE-754 single-precision float (little-endian).</summary>
    public void AppendFloat(float value)
    {
        AssertOrSetType(QwpTypeCode.Float);
        EnsureFixedCapacity(FixedLen + 4);
        BinaryPrimitives.WriteSingleLittleEndian(FixedData.AsSpan(FixedLen, 4), value);
        FixedLen += 4;
        AdvanceNonNull();
    }

    /// <summary>Appends an IEEE-754 double-precision float (little-endian).</summary>
    public void AppendDouble(double value)
    {
        AssertOrSetType(QwpTypeCode.Double);
        EnsureFixedCapacity(FixedLen + 8);
        BinaryPrimitives.WriteDoubleLittleEndian(FixedData.AsSpan(FixedLen, 8), value);
        FixedLen += 8;
        AdvanceNonNull();
    }

    /// <summary>Appends a TIMESTAMP value (microseconds since epoch).</summary>
    public void AppendTimestampMicros(long micros)
    {
        AssertOrSetType(QwpTypeCode.Timestamp);
        EnsureFixedCapacity(FixedLen + 8);
        BinaryPrimitives.WriteInt64LittleEndian(FixedData.AsSpan(FixedLen, 8), micros);
        FixedLen += 8;
        AdvanceNonNull();
    }

    /// <summary>Appends a TIMESTAMP_NANOS value (nanoseconds since epoch).</summary>
    public void AppendTimestampNanos(long nanos)
    {
        AssertOrSetType(QwpTypeCode.TimestampNanos);
        EnsureFixedCapacity(FixedLen + 8);
        BinaryPrimitives.WriteInt64LittleEndian(FixedData.AsSpan(FixedLen, 8), nanos);
        FixedLen += 8;
        AdvanceNonNull();
    }

    /// <summary>Appends a DATE value (milliseconds since epoch).</summary>
    public void AppendDateMillis(long millis)
    {
        AssertOrSetType(QwpTypeCode.Date);
        EnsureFixedCapacity(FixedLen + 8);
        BinaryPrimitives.WriteInt64LittleEndian(FixedData.AsSpan(FixedLen, 8), millis);
        FixedLen += 8;
        AdvanceNonNull();
    }

    /// <summary>Appends a UUID. Wire layout is low 8 bytes followed by high 8 bytes (per spec §10).</summary>
    public void AppendUuid(Guid value)
    {
        AssertOrSetType(QwpTypeCode.Uuid);
        EnsureFixedCapacity(FixedLen + 16);
        var dest = FixedData.AsSpan(FixedLen, 16);

        // .NET's Guid.TryWriteBytes(span) emits the "Microsoft" mixed-endian form:
        //   bytes 0..3 = field a (int32) little-endian
        //   bytes 4..5 = field b (int16) little-endian
        //   bytes 6..7 = field c (int16) little-endian
        //   bytes 8..15 = fields d..k (raw)
        // RFC 4122 / Java UUID writes a/b/c big-endian and d..k unchanged, giving
        // a 16-byte sequence that splits cleanly into a 64-bit "high" and "low" half.
        // The QWP wire format wants those two halves stored little-endian, low half first.
        Span<byte> ms = stackalloc byte[16];
        if (!value.TryWriteBytes(ms))
        {
            throw new InvalidOperationException("failed to serialise Guid");
        }

        // Low 64 bits = RFC bytes 8..15 read big-endian. LE encoding of that int64 = reverse those 8 bytes.
        for (var i = 0; i < 8; i++)
        {
            dest[i] = ms[15 - i];
        }

        // High 64 bits = RFC bytes 0..7 read big-endian. The RFC representation of those bytes
        // is the Microsoft form's first 8 bytes with each field group byte-reversed:
        //   rfc[0..3] = ms[3..0], rfc[4..5] = ms[5..4], rfc[6..7] = ms[7..6]
        // LE encoding of the resulting int64 = reverse rfc[0..7] = ms[6], ms[7], ms[4], ms[5], ms[0..3].
        dest[8] = ms[6];
        dest[9] = ms[7];
        dest[10] = ms[4];
        dest[11] = ms[5];
        dest[12] = ms[0];
        dest[13] = ms[1];
        dest[14] = ms[2];
        dest[15] = ms[3];

        FixedLen += 16;
        AdvanceNonNull();
    }

    /// <summary>Appends a CHAR (single UTF-16 code unit) as 2 bytes little-endian.</summary>
    public void AppendChar(char value)
    {
        AssertOrSetType(QwpTypeCode.Char);
        EnsureFixedCapacity(FixedLen + 2);
        BinaryPrimitives.WriteUInt16LittleEndian(FixedData.AsSpan(FixedLen, 2), value);
        FixedLen += 2;
        AdvanceNonNull();
    }

    /// <summary>Appends a VARCHAR value, UTF-8 encoded.</summary>
    public void AppendVarchar(ReadOnlySpan<char> value)
    {
        AssertOrSetType(QwpTypeCode.Varchar);

        // Lazily allocate offsets; the first slot is the leading 0 offset.
        if (StrOffsets is null)
        {
            StrOffsets = new uint[InitialSymbolCapacity];
            StrOffsets[0] = 0;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        EnsureStringCapacity(StrLen + byteCount);
        var written = Encoding.UTF8.GetBytes(value, StrData.AsSpan(StrLen, byteCount));
        StrLen += written;

        // Offsets array carries one trailing offset per non-null value, so length = NonNullCount + 1.
        // The slot we need to fill is at index NonNullCount + 1 (the new "end" offset).
        EnsureOffsetCapacity(NonNullCount + 2);
        StrOffsets[NonNullCount + 1] = (uint)StrLen;

        AdvanceNonNull();
    }

    /// <summary>
    ///     Drops all row data while preserving the column's name, type, and backing-buffer
    ///     allocations. Intended for reuse across batches by the sender — the schema definition
    ///     stays valid, only the contents are recycled.
    /// </summary>
    public void Clear()
    {
        RowCount = 0;
        NullCount = 0;
        FixedLen = 0;
        StrLen = 0;
        NullBitmap = null;
        DecimalScaleSet = false;
        DecimalScale = 0;
        GeohashPrecisionSet = false;
        GeohashPrecisionBits = 0;
        // FixedData / BoolData / StrOffsets / StrData / SymbolIds keep their allocations for
        // amortised cost. The non-null counters bound reads so stale bytes are invisible.
        // TypeCode and IsTyped remain so the schema stays describable.
    }

    /// <summary>Appends a SYMBOL value as a global dictionary id. Dictionary lookup is the caller's responsibility.</summary>
    public void AppendSymbol(int globalId)
    {
        AssertOrSetType(QwpTypeCode.Symbol);
        if (globalId < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(globalId), "symbol id must be non-negative");
        }

        if (SymbolIds is null || NonNullCount == SymbolIds.Length)
        {
            GrowSymbolArray();
        }

        SymbolIds![NonNullCount] = globalId;
        AdvanceNonNull();
    }

    /// <summary>
    ///     Appends a DECIMAL128 value. The first non-null call locks the column's scale; subsequent
    ///     values must use the same scale or this method throws.
    /// </summary>
    public void AppendDecimal128(decimal value)
    {
        AssertOrSetType(QwpTypeCode.Decimal128);

        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        var lo = (uint)bits[0];
        var mid = (uint)bits[1];
        var hi = (uint)bits[2];
        var flags = bits[3];
        var negative = (flags & unchecked((int)0x80000000)) != 0;
        var scale = (byte)((flags >> 16) & 0x7F);

        if (!DecimalScaleSet)
        {
            DecimalScale = scale;
            DecimalScaleSet = true;
        }
        else if (DecimalScale != scale)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' decimal scale mismatch: previously {DecimalScale}, now {scale}. " +
                "Pre-scale values to a uniform scale (e.g. via decimal arithmetic) before appending.");
        }

        EnsureFixedCapacity(FixedLen + QwpConstants.Decimal128SizeBytes);
        var dest = FixedData.AsSpan(FixedLen, QwpConstants.Decimal128SizeBytes);

        if (!negative)
        {
            // Sign-extend the 96-bit unsigned magnitude with a zero high word.
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(0, 4), lo);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(4, 4), mid);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(8, 4), hi);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(12, 4), 0u);
        }
        else
        {
            // Two's-complement of the 128-bit value: ~v + 1, with sign-extension across the high word.
            var b0 = ~lo;
            var b1 = ~mid;
            var b2 = ~hi;
            var b3 = ~0u;

            var sum = (ulong)b0 + 1ul;
            b0 = (uint)sum;
            var carry = (uint)(sum >> 32);
            sum = (ulong)b1 + carry;
            b1 = (uint)sum;
            carry = (uint)(sum >> 32);
            sum = (ulong)b2 + carry;
            b2 = (uint)sum;
            carry = (uint)(sum >> 32);
            b3 += carry;

            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(0, 4), b0);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(4, 4), b1);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(8, 4), b2);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(12, 4), b3);
        }

        FixedLen += QwpConstants.Decimal128SizeBytes;
        AdvanceNonNull();
    }

    /// <summary>
    ///     Appends a LONG256 value. The value must be non-negative and fit in 256 bits unsigned.
    /// </summary>
    public void AppendLong256(BigInteger value)
    {
        AssertOrSetType(QwpTypeCode.Long256);

        if (value.Sign < 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' Long256 values must be non-negative");
        }

        // Unsigned LE bytes; available since .NET 5.
        var magnitude = value.ToByteArray(isUnsigned: true, isBigEndian: false);
        if (magnitude.Length > QwpConstants.Long256SizeBytes)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' Long256 value exceeds 256 bits ({magnitude.Length * 8} bits supplied)");
        }

        EnsureFixedCapacity(FixedLen + QwpConstants.Long256SizeBytes);
        var dest = FixedData.AsSpan(FixedLen, QwpConstants.Long256SizeBytes);

        magnitude.CopyTo(dest);
        if (magnitude.Length < QwpConstants.Long256SizeBytes)
        {
            // Zero-pad the high bytes; FixedData may carry stale values from a prior allocation.
            dest.Slice(magnitude.Length).Clear();
        }

        FixedLen += QwpConstants.Long256SizeBytes;
        AdvanceNonNull();
    }

    /// <summary>
    ///     Appends a GEOHASH value with the given precision. The first non-null call locks the
    ///     column precision; subsequent values must use the same precision.
    /// </summary>
    /// <param name="hash">The geohash bits packed into the low <paramref name="precisionBits" /> bits.</param>
    /// <param name="precisionBits">Precision in bits, in the range [1, 60].</param>
    public void AppendGeohash(ulong hash, int precisionBits)
    {
        AssertOrSetType(QwpTypeCode.Geohash);

        if (precisionBits < QwpConstants.MinGeohashPrecisionBits || precisionBits > QwpConstants.MaxGeohashPrecisionBits)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' geohash precision must be in [{QwpConstants.MinGeohashPrecisionBits}, " +
                $"{QwpConstants.MaxGeohashPrecisionBits}] bits, got {precisionBits}");
        }

        if (!GeohashPrecisionSet)
        {
            GeohashPrecisionBits = precisionBits;
            GeohashPrecisionSet = true;
        }
        else if (GeohashPrecisionBits != precisionBits)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' geohash precision mismatch: previously {GeohashPrecisionBits}, now {precisionBits}");
        }

        var byteCount = (precisionBits + 7) >> 3;
        EnsureFixedCapacity(FixedLen + byteCount);
        var dest = FixedData.AsSpan(FixedLen, byteCount);

        for (var i = 0; i < byteCount; i++)
        {
            dest[i] = (byte)(hash >> (i * 8));
        }

        FixedLen += byteCount;
        AdvanceNonNull();
    }

    /// <summary>
    ///     Appends a DOUBLE_ARRAY row. Wire layout: <c>uint8 nDims</c> + <c>nDims × int32 LE</c>
    ///     dimension lengths + <c>product(shape) × float64 LE</c> values.
    /// </summary>
    public void AppendDoubleArray(ReadOnlySpan<double> values, ReadOnlySpan<int> shape)
    {
        AssertOrSetType(QwpTypeCode.DoubleArray);
        AppendArrayCore(MemoryMarshal.AsBytes(values), values.Length, shape, elementSize: 8);
    }

    /// <summary>
    ///     Appends a LONG_ARRAY row. Wire layout: <c>uint8 nDims</c> + <c>nDims × int32 LE</c>
    ///     dimension lengths + <c>product(shape) × int64 LE</c> values.
    /// </summary>
    public void AppendLongArray(ReadOnlySpan<long> values, ReadOnlySpan<int> shape)
    {
        AssertOrSetType(QwpTypeCode.LongArray);
        AppendArrayCore(MemoryMarshal.AsBytes(values), values.Length, shape, elementSize: 8);
    }

    private void AppendArrayCore(ReadOnlySpan<byte> valueBytes, int valueCount, ReadOnlySpan<int> shape, int elementSize)
    {
        if (!BitConverter.IsLittleEndian)
        {
            // .NET runs on x64 and arm64 in practice; both little-endian. A big-endian host would
            // need per-element byte swapping, which we don't ship until there's a real demand.
            throw new PlatformNotSupportedException("QWP array encoding requires a little-endian host");
        }

        if (shape.Length < 1 || shape.Length > QwpConstants.MaxArrayDimensions)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"array dimensions must be in [1, {QwpConstants.MaxArrayDimensions}], got {shape.Length}");
        }

        long expected = 1;
        for (var i = 0; i < shape.Length; i++)
        {
            if (shape[i] < 0)
            {
                throw new IngressError(ErrorCode.InvalidApiCall, "array shape dimensions must be non-negative");
            }

            expected *= shape[i];
        }

        if (expected != valueCount)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"array shape product ({expected}) does not match value count ({valueCount})");
        }

        var byteCount = 1 + shape.Length * 4 + valueCount * elementSize;
        EnsureFixedCapacity(FixedLen + byteCount);
        var dest = FixedData.AsSpan(FixedLen, byteCount);

        dest[0] = (byte)shape.Length;
        var pos = 1;
        for (var i = 0; i < shape.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(pos, 4), shape[i]);
            pos += 4;
        }

        valueBytes.CopyTo(dest.Slice(pos));

        FixedLen += byteCount;
        AdvanceNonNull();
    }

    // -- Internal helpers --------------------------------------------------------

    private void AssertOrSetType(QwpTypeCode code)
    {
        if (!IsTyped)
        {
            TypeCode = code;
            IsTyped = true;
            return;
        }

        if (TypeCode != code)
        {
            throw new IngressError(
                ErrorCode.InvalidApiCall,
                $"column '{Name}' was first written as {TypeCode} but is now being written as {code}");
        }
    }

    private void AdvanceNonNull()
    {
        // Bitmap stays unallocated until the first null arrives, but we must extend its conceptual
        // length whenever the row count grows. EnsureBitmapCapacity is a no-op when no bitmap exists.
        EnsureBitmapCapacity(RowCount + 1);
        // Non-null bit defaults to 0; nothing to write.
        RowCount++;
    }

    private void EnsureFixedCapacity(int required)
    {
        if (FixedData is null)
        {
            FixedData = new byte[Math.Max(InitialFixedCapacity, required)];
            return;
        }

        if (FixedData.Length < required)
        {
            var newSize = FixedData.Length;
            while (newSize < required)
            {
                newSize *= 2;
            }

            Array.Resize(ref FixedData, newSize);
        }
    }

    private void EnsureBoolCapacity(int requiredBits)
    {
        var requiredBytes = (requiredBits + 7) >> 3;
        if (BoolData is null)
        {
            BoolData = new byte[Math.Max(8, requiredBytes)];
            return;
        }

        if (BoolData.Length < requiredBytes)
        {
            var newSize = BoolData.Length;
            while (newSize < requiredBytes)
            {
                newSize *= 2;
            }

            Array.Resize(ref BoolData, newSize);
        }
    }

    private void EnsureStringCapacity(int required)
    {
        if (StrData is null)
        {
            StrData = new byte[Math.Max(InitialStringCapacity, required)];
            return;
        }

        if (StrData.Length < required)
        {
            var newSize = StrData.Length;
            while (newSize < required)
            {
                newSize *= 2;
            }

            Array.Resize(ref StrData, newSize);
        }
    }

    private void EnsureOffsetCapacity(int requiredCount)
    {
        if (StrOffsets!.Length < requiredCount)
        {
            var newSize = StrOffsets.Length;
            while (newSize < requiredCount)
            {
                newSize *= 2;
            }

            Array.Resize(ref StrOffsets, newSize);
        }
    }

    private void GrowSymbolArray()
    {
        if (SymbolIds is null)
        {
            SymbolIds = new int[InitialSymbolCapacity];
            return;
        }

        Array.Resize(ref SymbolIds, SymbolIds.Length * 2);
    }

    private void EnsureBitmapCapacity(int rowCount)
    {
        if (NullBitmap is null)
        {
            return; // bitmap is created lazily on first null in MarkBit.
        }

        var requiredBytes = (rowCount + 7) >> 3;
        if (NullBitmap.Length < requiredBytes)
        {
            var newSize = NullBitmap.Length;
            while (newSize < requiredBytes)
            {
                newSize *= 2;
            }

            Array.Resize(ref NullBitmap, newSize);
        }
    }

    private void MarkBit(int rowIndex)
    {
        if (NullBitmap is null)
        {
            // First null: allocate bitmap sized for the current row count + this bit.
            var bytes = ((rowIndex + 1) + 7) >> 3;
            NullBitmap = new byte[Math.Max(8, bytes)];
        }
        else
        {
            var requiredBytes = ((rowIndex + 1) + 7) >> 3;
            if (NullBitmap.Length < requiredBytes)
            {
                var newSize = NullBitmap.Length;
                while (newSize < requiredBytes)
                {
                    newSize *= 2;
                }

                Array.Resize(ref NullBitmap, newSize);
            }
        }

        NullBitmap[rowIndex >> 3] |= (byte)(1 << (rowIndex & 7));
    }
}
