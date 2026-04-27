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

using System.Text;

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Typed bind-parameter sink for a single QWP egress query. The .NET counterpart
///     of Java's <c>QwpBindValues</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Writes the per-bind wire layout into a reusable POH-pinned buffer:
///     <c>type_code(1B) | null_flag(1B) | [bitmap(1B) if null] | [value bytes if non-null]</c>.
///     <para/>
///     Indexes must be assigned in strictly ascending order starting at 0; gaps and
///     duplicates throw <see cref="InvalidOperationException"/>. SQL parameter
///     placeholders are 1-based (<c>$1, $2, ...</c>); indexes here are 0-based and
///     map to <c>$(index + 1)</c>.
///     <para/>
///     Multi-byte values are little-endian. DECIMAL scales are validated against
///     the per-width caps (18 / 38 / 76); GEOHASH precision against [1, 60].
///     <para/>
///     Not thread-safe. One instance per <c>QwpQueryClient</c>; <see cref="Reset"/>
///     is called by the client at the start of every <c>Execute</c>.
/// </remarks>
internal sealed class QwpBindValues
{
    private const int DECIMAL64_MAX_SCALE = 18;
    private const int DECIMAL128_MAX_SCALE = 38;
    private const int DECIMAL256_MAX_SCALE = 76;
    private const int GEOHASH_MIN_BITS = 1;
    private const int GEOHASH_MAX_BITS = 60;
    private const byte NULL_BITMAP = 0x01;
    private const byte NULL_FLAG = 0x01;
    private const byte NON_NULL_FLAG = 0x00;

    private readonly QwpPinnedBufferWriter _writer = new(initialCapacity: 256);
    private int _count;
    private int _expectedNextIndex;

    /// <summary>Number of binds written since the last <see cref="Reset"/>.</summary>
    public int Count => _count;

    /// <summary>Number of bytes of encoded bind payload currently in the buffer.</summary>
    public int BufferLength => _writer.Position;

    /// <summary>
    ///     Read-only view over the encoded bind payload. Used by <c>QwpQueryClient</c>
    ///     to hand the payload to the IO thread; valid until the next <see cref="Reset"/>.
    /// </summary>
    public ReadOnlySpan<byte> BufferSpan => _writer.AsReadOnlySpan();

    public QwpBindValues SetBoolean(int index, bool value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_BOOLEAN, isNull: false);
        _writer.PutByte(value ? (byte)1 : (byte)0);
        return this;
    }

    public QwpBindValues SetByte(int index, byte value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_BYTE, isNull: false);
        _writer.PutByte(value);
        return this;
    }

    public QwpBindValues SetShort(int index, short value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_SHORT, isNull: false);
        _writer.PutShort(value);
        return this;
    }

    public QwpBindValues SetChar(int index, char value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_CHAR, isNull: false);
        _writer.PutShort((short)value);
        return this;
    }

    public QwpBindValues SetInt(int index, int value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_INT, isNull: false);
        _writer.PutInt(value);
        return this;
    }

    public QwpBindValues SetLong(int index, long value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_LONG, isNull: false);
        _writer.PutLong(value);
        return this;
    }

    public QwpBindValues SetFloat(int index, float value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_FLOAT, isNull: false);
        _writer.PutFloat(value);
        return this;
    }

    public QwpBindValues SetDouble(int index, double value)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DOUBLE, isNull: false);
        _writer.PutDouble(value);
        return this;
    }

    public QwpBindValues SetDate(int index, long millisSinceEpoch)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DATE, isNull: false);
        _writer.PutLong(millisSinceEpoch);
        return this;
    }

    public QwpBindValues SetTimestampMicros(int index, long microsSinceEpoch)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_TIMESTAMP, isNull: false);
        _writer.PutLong(microsSinceEpoch);
        return this;
    }

    public QwpBindValues SetTimestampNanos(int index, long nanosSinceEpoch)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_TIMESTAMP_NANOS, isNull: false);
        _writer.PutLong(nanosSinceEpoch);
        return this;
    }

    public QwpBindValues SetUuid(int index, long lo, long hi)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_UUID, isNull: false);
        _writer.PutLong(lo);
        _writer.PutLong(hi);
        return this;
    }

    public QwpBindValues SetLong256(int index, long l0, long l1, long l2, long l3)
    {
        Advance(index);
        WriteHeader(QwpConstants.TYPE_LONG256, isNull: false);
        _writer.PutLong(l0);
        _writer.PutLong(l1);
        _writer.PutLong(l2);
        _writer.PutLong(l3);
        return this;
    }

    public QwpBindValues SetDecimal64(int index, int scale, long unscaledValue)
    {
        CheckScale64(scale);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DECIMAL64, isNull: false);
        _writer.PutByte((byte)scale);
        _writer.PutLong(unscaledValue);
        return this;
    }

    public QwpBindValues SetDecimal128(int index, int scale, long lo, long hi)
    {
        CheckScale128(scale);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DECIMAL128, isNull: false);
        _writer.PutByte((byte)scale);
        _writer.PutLong(lo);
        _writer.PutLong(hi);
        return this;
    }

    public QwpBindValues SetDecimal256(int index, int scale, long ll, long lh, long hl, long hh)
    {
        CheckScale256(scale);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DECIMAL256, isNull: false);
        _writer.PutByte((byte)scale);
        _writer.PutLong(ll);
        _writer.PutLong(lh);
        _writer.PutLong(hl);
        _writer.PutLong(hh);
        return this;
    }

    /// <summary>
    ///     Encodes a GEOHASH bind. <paramref name="value"/> is masked to
    ///     <paramref name="precisionBits"/> before encoding so bits above the declared
    ///     precision cannot leak into the top wire byte.
    /// </summary>
    public QwpBindValues SetGeohash(int index, int precisionBits, long value)
    {
        CheckGeohashPrecision(precisionBits);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_GEOHASH, isNull: false);
        _writer.PutVarint(precisionBits);
        var masked = MaskGeohashBits(value, precisionBits);
        var byteCount = (precisionBits + 7) >> 3;
        for (var b = 0; b < byteCount; b++)
        {
            _writer.PutByte((byte)((ulong)masked >> (b * 8)));
        }
        return this;
    }

    /// <summary>
    ///     Encodes a VARCHAR bind. A null value is written as a typed NULL.
    ///     Layout: <c>u32 offset0=0 | u32 length_bytes | UTF-8 bytes</c>.
    /// </summary>
    public QwpBindValues SetVarchar(int index, string? value)
    {
        if (value is null) return SetNull(index, QwpConstants.TYPE_VARCHAR);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_VARCHAR, isNull: false);
        var utf8Len = Encoding.UTF8.GetByteCount(value);
        _writer.PutInt(0);
        _writer.PutInt(utf8Len);
        _writer.PutUtf8(value);
        return this;
    }

    /// <summary>
    ///     Binds an explicit NULL with the given QWP wire type. ARRAY, BINARY, and
    ///     IPv4 are rejected because the server decoder does not accept them as
    ///     binds. DECIMAL64/128/256 NULLs use scale 0 and GEOHASH NULLs precision 1
    ///     bit; use <see cref="SetNullDecimal64"/>, <see cref="SetNullDecimal128"/>,
    ///     <see cref="SetNullDecimal256"/>, or <see cref="SetNullGeohash"/> when the
    ///     scale/precision matters.
    /// </summary>
    public QwpBindValues SetNull(int index, byte qwpTypeCode)
    {
        CheckBindType(qwpTypeCode);
        if (qwpTypeCode == QwpConstants.TYPE_DECIMAL64) return SetNullDecimal64(index, 0);
        if (qwpTypeCode == QwpConstants.TYPE_DECIMAL128) return SetNullDecimal128(index, 0);
        if (qwpTypeCode == QwpConstants.TYPE_DECIMAL256) return SetNullDecimal256(index, 0);
        if (qwpTypeCode == QwpConstants.TYPE_GEOHASH) return SetNullGeohash(index, GEOHASH_MIN_BITS);
        Advance(index);
        WriteHeader(qwpTypeCode, isNull: true);
        return this;
    }

    /// <summary>
    ///     Binds an explicit NULL with DECIMAL64 type and the given scale. The server
    ///     reads the scale byte even on NULL since it becomes part of the bound
    ///     variable's type.
    /// </summary>
    public QwpBindValues SetNullDecimal64(int index, int scale)
    {
        CheckScale64(scale);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DECIMAL64, isNull: true);
        _writer.PutByte((byte)scale);
        return this;
    }

    /// <summary>Binds an explicit NULL with DECIMAL128 type and the given scale.</summary>
    public QwpBindValues SetNullDecimal128(int index, int scale)
    {
        CheckScale128(scale);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DECIMAL128, isNull: true);
        _writer.PutByte((byte)scale);
        return this;
    }

    /// <summary>Binds an explicit NULL with DECIMAL256 type and the given scale.</summary>
    public QwpBindValues SetNullDecimal256(int index, int scale)
    {
        CheckScale256(scale);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_DECIMAL256, isNull: true);
        _writer.PutByte((byte)scale);
        return this;
    }

    /// <summary>
    ///     Binds an explicit NULL with GEOHASH type and the given precision (bits).
    ///     The server reads the precision_bits varint even on NULL.
    /// </summary>
    public QwpBindValues SetNullGeohash(int index, int precisionBits)
    {
        CheckGeohashPrecision(precisionBits);
        Advance(index);
        WriteHeader(QwpConstants.TYPE_GEOHASH, isNull: true);
        _writer.PutVarint(precisionBits);
        return this;
    }

    /// <summary>
    ///     Clears prior state so this instance can accumulate binds for a new query.
    ///     Called by <c>QwpQueryClient</c> at the start of every <c>Execute</c>.
    /// </summary>
    public void Reset()
    {
        _writer.Reset();
        _count = 0;
        _expectedNextIndex = 0;
    }

    private static long MaskGeohashBits(long value, int precisionBits)
    {
        return precisionBits >= 64 ? value : (long)((ulong)value & ((1UL << precisionBits) - 1UL));
    }

    private static void CheckGeohashPrecision(int precisionBits)
    {
        if (precisionBits < GEOHASH_MIN_BITS || precisionBits > GEOHASH_MAX_BITS)
        {
            throw new ArgumentOutOfRangeException(nameof(precisionBits),
                $"GEOHASH precision must be in [{GEOHASH_MIN_BITS}, {GEOHASH_MAX_BITS}], got {precisionBits}");
        }
    }

    private static void CheckScale64(int scale)
    {
        if (scale < 0 || scale > DECIMAL64_MAX_SCALE)
        {
            throw new ArgumentOutOfRangeException(nameof(scale),
                $"DECIMAL64 scale must be in [0, {DECIMAL64_MAX_SCALE}], got {scale}");
        }
    }

    private static void CheckScale128(int scale)
    {
        if (scale < 0 || scale > DECIMAL128_MAX_SCALE)
        {
            throw new ArgumentOutOfRangeException(nameof(scale),
                $"DECIMAL128 scale must be in [0, {DECIMAL128_MAX_SCALE}], got {scale}");
        }
    }

    private static void CheckScale256(int scale)
    {
        if (scale < 0 || scale > DECIMAL256_MAX_SCALE)
        {
            throw new ArgumentOutOfRangeException(nameof(scale),
                $"DECIMAL256 scale must be in [0, {DECIMAL256_MAX_SCALE}], got {scale}");
        }
    }

    private static void CheckBindType(byte type)
    {
        switch (type)
        {
            case QwpConstants.TYPE_BOOLEAN:
            case QwpConstants.TYPE_BYTE:
            case QwpConstants.TYPE_SHORT:
            case QwpConstants.TYPE_CHAR:
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_FLOAT:
            case QwpConstants.TYPE_DOUBLE:
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
            case QwpConstants.TYPE_UUID:
            case QwpConstants.TYPE_LONG256:
            case QwpConstants.TYPE_GEOHASH:
            case QwpConstants.TYPE_VARCHAR:
            case QwpConstants.TYPE_DECIMAL64:
            case QwpConstants.TYPE_DECIMAL128:
            case QwpConstants.TYPE_DECIMAL256:
                return;
            default:
                throw new ArgumentException($"unsupported bind type 0x{type:x2}", nameof(type));
        }
    }

    private void Advance(int index)
    {
        if (index != _expectedNextIndex)
        {
            throw new InvalidOperationException(
                $"bind index out of order: expected {_expectedNextIndex}, got {index}");
        }
        if (_count >= QwpConstants.MAX_COLUMNS_PER_TABLE)
        {
            throw new InvalidOperationException(
                $"too many binds: exceeds {QwpConstants.MAX_COLUMNS_PER_TABLE}");
        }
        _expectedNextIndex++;
        _count++;
    }

    private void WriteHeader(byte type, bool isNull)
    {
        _writer.PutByte(type);
        if (isNull)
        {
            _writer.PutByte(NULL_FLAG);
            _writer.PutByte(NULL_BITMAP);
        }
        else
        {
            _writer.PutByte(NON_NULL_FLAG);
        }
    }
}
