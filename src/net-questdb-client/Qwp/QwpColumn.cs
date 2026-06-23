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

    private static readonly BigInteger Long256MinSigned = -(BigInteger.One << 255);
    private static readonly BigInteger Long256Pow2_256 = BigInteger.One << 256;

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

    /// <summary>
    ///     Raw bytes for fixed-width types (BYTE / SHORT / INT / LONG / FLOAT / DOUBLE / DATE /
    ///     TIMESTAMP / TIMESTAMP_NANOS / UUID / CHAR). Length is bounded by <see cref="FixedLen" />.
    /// </summary>
    public byte[]? FixedData;

    /// <summary>Number of valid bytes in <see cref="FixedData" />.</summary>
    public int FixedLen;

    /// <summary>Bit-packed booleans, LSB-first within each byte. Length = <c>ceil(NonNullCount/8)</c>.</summary>
    public byte[]? BoolData;

    /// <summary>VARCHAR offset array; length = <c>NonNullCount + 1</c> once at least one value present.</summary>
    public uint[]? StrOffsets;

    /// <summary>Concatenated VARCHAR UTF-8 string data, length = last offset.</summary>
    public byte[]? StrData;

    /// <summary>Number of bytes used in <see cref="StrData" />.</summary>
    public int StrLen;

    /// <summary>SYMBOL global ids in append order; varint-encoded at frame time.</summary>
    public int[]? SymbolIds;

    /// <summary>DECIMAL scale; emitted as a 1-byte prefix before the values on the wire. Locked on first non-null.</summary>
    public byte DecimalScale;

    /// <summary>Whether <see cref="DecimalScale" /> has been set by the first non-null append.</summary>
    public bool DecimalScaleSet;

    /// <summary>GEOHASH precision in bits; varint prefix before values. Locked on first non-null.</summary>
    public int GeohashPrecisionBits;

    /// <summary>Whether <see cref="GeohashPrecisionBits" /> has been set by the first non-null append.</summary>
    public bool GeohashPrecisionSet;

    /// <summary>
    ///     Raw bytes accumulated across this column's backing buffers. A conservative proxy for the
    ///     column's contribution to the wire payload, used for byte-based auto-flush accounting.
    /// </summary>
    public long BufferedBytes
    {
        get
        {
            long bytes = FixedLen + StrLen;
            if (StrOffsets is not null) bytes += (long)(NonNullCount + 1) * sizeof(uint);
            if (SymbolIds is not null) bytes += (long)NonNullCount * sizeof(int);
            if (BoolData is not null) bytes += (NonNullCount + 7) >> 3;
            return bytes;
        }
    }

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
        var byteIndex = bitIndex >> 3;
        var bitInByte = bitIndex & 7;
        if (bitInByte == 0)
        {
            BoolData![byteIndex] = 0;
        }
        var mask = (byte)(1 << bitInByte);
        if (value)
        {
            BoolData![byteIndex] |= mask;
        }

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

    /// <summary>Appends a UUID. Wire layout is low 8 bytes followed by high 8 bytes.</summary>
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
        // RFC 4122 writes a/b/c big-endian and d..k unchanged, giving a 16-byte sequence that
        // splits cleanly into a 64-bit "high" and "low" half. QWP stores those two halves
        // little-endian, low half first.
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

        // Reserve worst-case UTF-8 footprint so we encode in one pass; trim by actual length below.
        var maxBytes = QwpConstants.StrictUtf8.GetMaxByteCount(value.Length);
        EnsureStringCapacity(StrLen + maxBytes);
        var written = QwpConstants.StrictUtf8.GetBytes(value, StrData.AsSpan(StrLen, maxBytes));
        StrLen += written;

        // Offsets array carries one trailing offset per non-null value, so length = NonNullCount + 1.
        // The slot we need to fill is at index NonNullCount + 1 (the new "end" offset).
        EnsureOffsetCapacity(NonNullCount + 2);
        StrOffsets[NonNullCount + 1] = (uint)StrLen;

        AdvanceNonNull();
    }

    internal readonly struct Savepoint
    {
        public readonly int RowCount;
        public readonly int NullCount;
        public readonly int FixedLen;
        public readonly int StrLen;
        public readonly QwpTypeCode TypeCode;
        public readonly byte DecimalScale;
        public readonly int GeohashPrecisionBits;
        public readonly bool IsTyped;
        public readonly bool DecimalScaleSet;
        public readonly bool GeohashPrecisionSet;

        public Savepoint(QwpColumn col)
        {
            RowCount = col.RowCount;
            NullCount = col.NullCount;
            FixedLen = col.FixedLen;
            StrLen = col.StrLen;
            TypeCode = col.TypeCode;
            DecimalScale = col.DecimalScale;
            GeohashPrecisionBits = col.GeohashPrecisionBits;
            IsTyped = col.IsTyped;
            DecimalScaleSet = col.DecimalScaleSet;
            GeohashPrecisionSet = col.GeohashPrecisionSet;
        }
    }

    internal Savepoint Snapshot() => new Savepoint(this);

    internal void Restore(Savepoint sp)
    {
        var oldRowCount = RowCount;
        RowCount = sp.RowCount;
        NullCount = sp.NullCount;
        FixedLen = sp.FixedLen;
        StrLen = sp.StrLen;
        TypeCode = sp.TypeCode;
        DecimalScale = sp.DecimalScale;
        GeohashPrecisionBits = sp.GeohashPrecisionBits;
        IsTyped = sp.IsTyped;
        DecimalScaleSet = sp.DecimalScaleSet;
        GeohashPrecisionSet = sp.GeohashPrecisionSet;

        // MarkBit ORs null bits in by row index; clear any set by the rolled-back row so a later
        // real value at the same dense index doesn't encode a spurious null.
        if (NullBitmap is { } nullBitmap)
        {
            for (var r = RowCount; r < oldRowCount; r++)
            {
                nullBitmap[r >> 3] &= (byte)~(1 << (r & 7));
            }
        }

        // AppendBool ORs without clearing on non-aligned slots; mask off post-rollback tail.
        if (BoolData is { Length: > 0 } boolData)
        {
            var nonNull = RowCount - NullCount;
            var keepBits = nonNull & 7;
            var byteIndex = nonNull >> 3;
            if (keepBits != 0 && byteIndex < boolData.Length)
            {
                boolData[byteIndex] &= (byte)((1 << keepBits) - 1);
            }
        }
    }

    /// <summary>
    ///     Drops all row data while preserving the column's name, type, and backing-buffer
    ///     allocations. Intended for reuse across batches by the sender — the schema definition
    ///     stays valid, only the contents are recycled.
    /// </summary>
    public void TrimToCurrent()
    {
        if (FixedData is { Length: > 0 } fixedData && fixedData.Length > FixedLen)
        {
            Array.Resize(ref FixedData, FixedLen);
        }
        if (StrData is { Length: > 0 } strData && strData.Length > StrLen)
        {
            Array.Resize(ref StrData, StrLen);
        }
        if (StrOffsets is { Length: > 0 } strOffs)
        {
            var needed = NonNullCount + 1;
            if (strOffs.Length > needed)
            {
                Array.Resize(ref StrOffsets, needed);
            }
        }
        if (SymbolIds is { Length: > 0 } symbolIds && symbolIds.Length > NonNullCount)
        {
            Array.Resize(ref SymbolIds, NonNullCount);
        }
        if (BoolData is { Length: > 0 } boolData)
        {
            var needed = (RowCount + 7) >> 3;
            if (boolData.Length > needed)
            {
                Array.Resize(ref BoolData, needed);
            }
        }
        if (NullBitmap is { Length: > 0 } nb)
        {
            var needed = (RowCount + 7) >> 3;
            if (nb.Length > needed)
            {
                Array.Resize(ref NullBitmap, needed);
            }
        }
    }

    public void Clear()
    {
        RowCount = 0;
        NullCount = 0;
        FixedLen = 0;
        StrLen = 0;
        if (NullBitmap is not null)
        {
            Array.Clear(NullBitmap, 0, NullBitmap.Length);
        }
        // Type / scale / precision pinned for column lifetime so server-side schema doesn't drift.
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

    /// <summary>Appends a DECIMAL64 value. The first non-null call locks the column's scale.</summary>
    public void AppendDecimal64(decimal value)
    {
        AppendDecimalAtSize(QwpTypeCode.Decimal64, value, QwpConstants.Decimal64SizeBytes);
    }

    /// <summary>
    ///     Appends a DECIMAL128 value. The first non-null call locks the column's scale; subsequent
    ///     values must use the same scale or this method throws.
    /// </summary>
    public void AppendDecimal128(decimal value)
    {
        AppendDecimalAtSize(QwpTypeCode.Decimal128, value, QwpConstants.Decimal128SizeBytes);
    }

    /// <summary>Appends a DECIMAL256 value. The first non-null call locks the column's scale.</summary>
    public void AppendDecimal256(decimal value)
    {
        AppendDecimalAtSize(QwpTypeCode.Decimal256, value, QwpConstants.Decimal256SizeBytes);
    }

    public void AppendDecimal64(long unscaledValue, byte scale)
    {
        ValidateDecimalScale(scale, QwpConstants.MaxDecimal64Scale, "Decimal64");
        AppendDecimalFromMantissa(
            QwpTypeCode.Decimal64,
            new BigInteger(unscaledValue),
            scale,
            QwpConstants.Decimal64SizeBytes);
    }

    public void AppendDecimal128(long lo, long hi, byte scale)
    {
        ValidateDecimalScale(scale, QwpConstants.MaxDecimal128Scale, "Decimal128");
        var mantissa = (new BigInteger(hi) << 64) + new BigInteger((ulong)lo);
        AppendDecimalFromMantissa(
            QwpTypeCode.Decimal128,
            mantissa,
            scale,
            QwpConstants.Decimal128SizeBytes);
    }

    public void AppendDecimal256(long l0, long l1, long l2, long l3, byte scale)
    {
        ValidateDecimalScale(scale, QwpConstants.MaxDecimal256Scale, "Decimal256");
        var mantissa = (new BigInteger(l3) << 192)
            + (new BigInteger((ulong)l2) << 128)
            + (new BigInteger((ulong)l1) << 64)
            + new BigInteger((ulong)l0);
        AppendDecimalFromMantissa(
            QwpTypeCode.Decimal256,
            mantissa,
            scale,
            QwpConstants.Decimal256SizeBytes);
    }

    private static void ValidateDecimalScale(byte scale, int maxScale, string typeName)
    {
        if (scale > maxScale)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"{typeName} scale {scale} exceeds maximum {maxScale}");
        }
    }

    private void AppendDecimalAtSize(QwpTypeCode code, decimal value, int sizeBytes)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        var flags = bits[3];
        var negative = (flags & unchecked((int)0x80000000)) != 0;
        var scale = (byte)((flags >> 16) & 0x7F);

        var (maxScale, typeName) = code switch
        {
            QwpTypeCode.Decimal64 => (QwpConstants.MaxDecimal64Scale, "Decimal64"),
            QwpTypeCode.Decimal128 => (QwpConstants.MaxDecimal128Scale, "Decimal128"),
            QwpTypeCode.Decimal256 => (QwpConstants.MaxDecimal256Scale, "Decimal256"),
            _ => (int.MaxValue, code.ToString()),
        };
        ValidateDecimalScale(scale, maxScale, typeName);

        // Fast path: no rescale needed — either this is the first non-null write (which locks the
        // column scale to this value's scale) or the value's scale already matches the locked scale.
        // Serialise the 96-bit mantissa straight into FixedData as fixed-width two's-complement.
        // BigInteger is a struct, but the slow path still allocates on the heap per value: its uint[]
        // backing store (whenever the magnitude is past int range) plus the byte[] that ToByteArray
        // returns. A scale mismatch falls back to that BigInteger rescale arithmetic below.
        if (!DecimalScaleSet || scale == DecimalScale)
        {
            AssertOrSetType(code);
            if (!DecimalScaleSet)
            {
                DecimalScale = scale;
                DecimalScaleSet = true;
            }
            AppendDecimal96TwosComplement((uint)bits[0], (uint)bits[1], (uint)bits[2], negative, sizeBytes);
            return;
        }

        var mantissa = (new BigInteger((uint)bits[2]) << 64)
            | (new BigInteger((uint)bits[1]) << 32)
            | new BigInteger((uint)bits[0]);
        if (negative) mantissa = -mantissa;

        AppendDecimalFromMantissa(code, mantissa, scale, sizeBytes);
    }

    // Writes a 96-bit magnitude (three little-endian 32-bit words) with the given sign into FixedData
    // as a fixed-width little-endian two's-complement value. Zero-allocation companion to the
    // BigInteger tail of AppendDecimalFromMantissa; produces byte-identical output for the no-rescale
    // case (verified by QwpColumnDecimalEncodingTests).
    private void AppendDecimal96TwosComplement(uint w0, uint w1, uint w2, bool negative, int sizeBytes)
    {
        if (!Magnitude96FitsSigned(w0, w1, w2, negative, sizeBytes))
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' decimal at scale {DecimalScale} overflows {sizeBytes * 8}-bit decimal range");
        }

        EnsureFixedCapacity(FixedLen + sizeBytes);
        var dest = FixedData.AsSpan(FixedLen, sizeBytes);
        dest.Clear();
        // sizeBytes is 8, 16, or 32 — always >= 8, so w0/w1 fit; w2 only when the field is >= 12 bytes
        // wide (the 8-byte case guarantees w2 == 0 via the overflow check above).
        BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(0, 4), w0);
        BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(4, 4), w1);
        if (sizeBytes >= 12)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(8, 4), w2);
        }
        if (negative)
        {
            NegateLittleEndian(dest);
        }

        FixedLen += sizeBytes;
        AdvanceNonNull();
    }

    // True when the unsigned 96-bit magnitude fits the signed range of `sizeBytes` bytes under the
    // given sign. A 96-bit magnitude always fits 128-/256-bit; only Decimal64 (8 bytes) can overflow.
    private static bool Magnitude96FitsSigned(uint w0, uint w1, uint w2, bool negative, int sizeBytes)
    {
        if (sizeBytes >= 16)
        {
            return true;
        }
        if (w2 != 0)
        {
            return false; // magnitude >= 2^64, cannot fit signed 64-bit
        }
        if ((w1 & 0x80000000u) == 0)
        {
            return true; // |value| < 2^63 fits either sign
        }
        // Top bit of the 64-bit magnitude is set: only -2^63 (exactly) is representable.
        return negative && w1 == 0x80000000u && w0 == 0u;
    }

    // In-place little-endian two's-complement negation (invert all bytes, add one).
    private static void NegateLittleEndian(Span<byte> bytes)
    {
        var carry = 1;
        for (var i = 0; i < bytes.Length; i++)
        {
            var v = (byte)~bytes[i] + carry;
            bytes[i] = (byte)v;
            carry = v >> 8;
        }
    }

    private void AppendDecimalFromMantissa(QwpTypeCode code, BigInteger mantissa, byte sourceScale, int sizeBytes)
    {
        AssertOrSetType(code);

        byte targetScale;
        if (!DecimalScaleSet)
        {
            targetScale = sourceScale;
            DecimalScale = sourceScale;
            DecimalScaleSet = true;
        }
        else
        {
            targetScale = DecimalScale;
        }

        if (sourceScale != targetScale)
        {
            if (sourceScale < targetScale)
            {
                mantissa *= BigInteger.Pow(10, targetScale - sourceScale);
            }
            else
            {
                var divisor = BigInteger.Pow(10, sourceScale - targetScale);
                if (!(mantissa % divisor).IsZero)
                {
                    throw new IngressError(ErrorCode.InvalidApiCall,
                        $"column '{Name}' cannot rescale decimal from scale {sourceScale} to {targetScale} without precision loss");
                }
                mantissa /= divisor;
            }
        }

        EnsureFixedCapacity(FixedLen + sizeBytes);
        var dest = FixedData.AsSpan(FixedLen, sizeBytes);
        var bytes = mantissa.ToByteArray(isUnsigned: false, isBigEndian: false);
        if (bytes.Length > sizeBytes)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' decimal at scale {targetScale} overflows {sizeBytes * 8}-bit decimal range");
        }
        var fill = mantissa.Sign < 0 ? (byte)0xFF : (byte)0x00;
        dest.Fill(fill);
        bytes.AsSpan().CopyTo(dest);
        FixedLen += sizeBytes;
        AdvanceNonNull();
    }

    /// <summary>Appends a BINARY value as length-prefixed opaque bytes (same wire layout as VARCHAR).</summary>
    public void AppendBinary(ReadOnlySpan<byte> value)
    {
        AssertOrSetType(QwpTypeCode.Binary);

        if (StrOffsets is null)
        {
            StrOffsets = new uint[InitialSymbolCapacity];
            StrOffsets[0] = 0;
        }

        EnsureStringCapacity(StrLen + value.Length);
        value.CopyTo(StrData.AsSpan(StrLen, value.Length));
        StrLen += value.Length;

        EnsureOffsetCapacity(NonNullCount + 2);
        StrOffsets[NonNullCount + 1] = (uint)StrLen;

        AdvanceNonNull();
    }

    /// <summary>Appends an IPv4 address as 4 bytes little-endian (same wire layout as INT).</summary>
    public void AppendIPv4(uint addr)
    {
        AssertOrSetType(QwpTypeCode.IPv4);
        EnsureFixedCapacity(FixedLen + 4);
        BinaryPrimitives.WriteUInt32LittleEndian(FixedData.AsSpan(FixedLen, 4), addr);
        FixedLen += 4;
        AdvanceNonNull();
    }

    /// <summary>
    ///     Appends a LONG256 value as four little-endian 64-bit words. Zero-allocation; matches the
    ///     canonical wire layout exactly.
    /// </summary>
    public void AppendLong256(long l0, long l1, long l2, long l3)
    {
        AssertOrSetType(QwpTypeCode.Long256);
        EnsureFixedCapacity(FixedLen + QwpConstants.Long256SizeBytes);
        var dest = FixedData.AsSpan(FixedLen, QwpConstants.Long256SizeBytes);
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(0, 8), l0);
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(8, 8), l1);
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(16, 8), l2);
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(24, 8), l3);
        FixedLen += QwpConstants.Long256SizeBytes;
        AdvanceNonNull();
    }

    /// <summary>
    ///     Appends a LONG256 value from a <see cref="BigInteger" />. Accepts the full 256-bit
    ///     range under either interpretation: <c>[0, 2^256 - 1]</c> unsigned or
    ///     <c>[-(2^255), 2^255 - 1]</c> signed. Negative values are encoded as two's-complement
    ///     low 32 bytes.
    /// </summary>
    public void AppendLong256(BigInteger value)
    {
        AssertOrSetType(QwpTypeCode.Long256);
        EnsureFixedCapacity(FixedLen + QwpConstants.Long256SizeBytes);
        var dest = FixedData.AsSpan(FixedLen, QwpConstants.Long256SizeBytes);

        var raw = value;
        if (raw.Sign < 0)
        {
            if (raw < Long256MinSigned)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    $"column '{Name}' Long256 value below the 256-bit signed minimum -(2^255)");
            }
            raw = Long256Pow2_256 + raw;
        }

        if (!raw.TryWriteBytes(dest, out var bytesWritten, isUnsigned: true, isBigEndian: false))
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"column '{Name}' Long256 value exceeds the 256-bit unsigned maximum 2^256 - 1");
        }

        if (bytesWritten < QwpConstants.Long256SizeBytes)
        {
            dest.Slice(bytesWritten).Clear();
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

        if (precisionBits < 64)
        {
            hash &= (1UL << precisionBits) - 1UL;
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

        var byteCountLong = 1L + (long)shape.Length * 4L + (long)valueCount * elementSize;
        if (byteCountLong > QwpConstants.MaxBatchBytes)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"array payload ({byteCountLong} bytes) exceeds the {QwpConstants.MaxBatchBytes}-byte batch limit");
        }
        var byteCount = (int)byteCountLong;
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
            Array.Resize(ref FixedData, GrowTo(FixedData.Length, required, "fixed-width column"));
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
            Array.Resize(ref BoolData, GrowTo(BoolData.Length, requiredBytes, "boolean column"));
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
            Array.Resize(ref StrData, GrowTo(StrData.Length, required, "string column"));
        }
    }

    private void EnsureOffsetCapacity(int requiredCount)
    {
        if (StrOffsets is null)
        {
            StrOffsets = new uint[Math.Max(InitialSymbolCapacity, requiredCount)];
            StrOffsets[0] = 0;
            return;
        }

        if (StrOffsets.Length < requiredCount)
        {
            Array.Resize(ref StrOffsets, GrowTo(StrOffsets.Length, requiredCount, "string-offset array"));
        }
    }

    private void GrowSymbolArray()
    {
        if (SymbolIds is null)
        {
            SymbolIds = new int[InitialSymbolCapacity];
            return;
        }

        Array.Resize(ref SymbolIds, GrowTo(SymbolIds.Length, SymbolIds.Length + 1, "symbol-id array"));
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
            Array.Resize(ref NullBitmap, GrowTo(NullBitmap.Length, requiredBytes, "null bitmap"));
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
                Array.Resize(ref NullBitmap, GrowTo(NullBitmap.Length, requiredBytes, "null bitmap"));
            }
        }

        NullBitmap[rowIndex >> 3] |= (byte)(1 << (rowIndex & 7));
    }

    // Doubles currentLength until it covers required, accumulating in long so the doubling
    // can't overflow negative; clamps at int.MaxValue and rejects a genuinely oversized column.
    private static int GrowTo(int currentLength, int required, string what)
    {
        if (required < 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"QWP {what} capacity requirement overflowed int.MaxValue");
        }

        var newSize = (long)currentLength;
        while (newSize < required)
        {
            newSize *= 2;
            if (newSize > int.MaxValue)
            {
                newSize = int.MaxValue;
                break;
            }
        }

        if (newSize < required)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"QWP {what} required size {required} exceeds the {int.MaxValue}-byte cap");
        }

        return (int)newSize;
    }
}
