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
using QuestDB.Utils;

namespace QuestDB.Qwp.Query;

/// <summary>
///     Typed builder for QWP bind parameters. Each bind is encoded as a one-row column under the
///     ingress wire format: <c>[type_code u8][null_flag u8][optional bitmap byte 0x01 if null]</c>
///     followed by type-specific bytes (DECIMAL prefixes scale, GEOHASH prefixes precision varint).
/// </summary>
public sealed class QwpBindValues
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);

    private const byte NullFlagOff = 0x00;
    private const byte NullFlagOn = 0x01;
    private const byte NullBitmap = 0x01;

    private const int Decimal64MaxScale = 18;
    private const int Decimal128MaxScale = 38;
    private const int Decimal256MaxScale = 76;

    private byte[] _buffer = new byte[256];
    private int _length;
    private int _expectedNextIndex;
    private int _count;

    /// <summary>Number of bind parameters set so far.</summary>
    public int Count => _count;

    /// <summary>Returns the encoded bind buffer as a slice of the internal storage.</summary>
    public ReadOnlyMemory<byte> AsMemory() => _buffer.AsMemory(0, _length);

    /// <summary>Clears all bind state so the instance can be reused for the next query.</summary>
    public void Reset()
    {
        _length = 0;
        _expectedNextIndex = 0;
        _count = 0;
    }

    /// <summary>Binds a BOOLEAN at <paramref name="index" />.</summary>
    public QwpBindValues SetBoolean(int index, bool value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Boolean, isNull: false);
        WriteByte(value ? (byte)1 : (byte)0);
        return this;
    }

    /// <summary>Binds a BYTE (uint8) at <paramref name="index" />.</summary>
    public QwpBindValues SetByte(int index, byte value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Byte, isNull: false);
        WriteByte(value);
        return this;
    }

    /// <summary>Binds a SHORT (int16) at <paramref name="index" />.</summary>
    public QwpBindValues SetShort(int index, short value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Short, isNull: false);
        WriteI16(value);
        return this;
    }

    /// <summary>Binds a CHAR (UTF-16 code unit) at <paramref name="index" />.</summary>
    public QwpBindValues SetChar(int index, char value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Char, isNull: false);
        WriteU16(value);
        return this;
    }

    /// <summary>Binds an INT (int32) at <paramref name="index" />.</summary>
    public QwpBindValues SetInt(int index, int value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Int, isNull: false);
        WriteI32(value);
        return this;
    }

    /// <summary>Binds a LONG (int64) at <paramref name="index" />.</summary>
    public QwpBindValues SetLong(int index, long value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Long, isNull: false);
        WriteI64(value);
        return this;
    }

    /// <summary>Binds a FLOAT (32-bit IEEE-754) at <paramref name="index" />.</summary>
    public QwpBindValues SetFloat(int index, float value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Float, isNull: false);
        WriteI32(BitConverter.SingleToInt32Bits(value));
        return this;
    }

    /// <summary>Binds a DOUBLE (64-bit IEEE-754) at <paramref name="index" />.</summary>
    public QwpBindValues SetDouble(int index, double value)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Double, isNull: false);
        WriteI64(BitConverter.DoubleToInt64Bits(value));
        return this;
    }

    /// <summary>Binds a DATE (milliseconds since Unix epoch) at <paramref name="index" />.</summary>
    public QwpBindValues SetDate(int index, long millisSinceEpoch)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Date, isNull: false);
        WriteI64(millisSinceEpoch);
        return this;
    }

    /// <summary>Binds a TIMESTAMP (microseconds since Unix epoch) at <paramref name="index" />.</summary>
    public QwpBindValues SetTimestampMicros(int index, long microsSinceEpoch)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Timestamp, isNull: false);
        WriteI64(microsSinceEpoch);
        return this;
    }

    /// <summary>Binds a TIMESTAMP_NANOS (nanoseconds since Unix epoch) at <paramref name="index" />.</summary>
    public QwpBindValues SetTimestampNanos(int index, long nanosSinceEpoch)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.TimestampNanos, isNull: false);
        WriteI64(nanosSinceEpoch);
        return this;
    }

    /// <summary>Binds a UUID at <paramref name="index" /> from explicit lo/hi 64-bit halves (little-endian wire order).</summary>
    public QwpBindValues SetUuid(int index, long lo, long hi)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Uuid, isNull: false);
        WriteI64(lo);
        WriteI64(hi);
        return this;
    }

    /// <summary>Binds a UUID at <paramref name="index" /> from a <see cref="Guid" />, reordering bytes to QWP wire layout.</summary>
    public QwpBindValues SetUuid(int index, Guid value)
    {
        Span<byte> ms = stackalloc byte[16];
        if (!value.TryWriteBytes(ms))
        {
            throw new InvalidOperationException("Guid.TryWriteBytes failed");
        }

        Span<byte> wire = stackalloc byte[16];
        wire[0] = ms[15]; wire[1] = ms[14]; wire[2] = ms[13]; wire[3] = ms[12];
        wire[4] = ms[11]; wire[5] = ms[10]; wire[6] = ms[9]; wire[7] = ms[8];
        wire[8] = ms[6]; wire[9] = ms[7]; wire[10] = ms[4]; wire[11] = ms[5];
        wire[12] = ms[0]; wire[13] = ms[1]; wire[14] = ms[2]; wire[15] = ms[3];

        var lo = BinaryPrimitives.ReadInt64LittleEndian(wire.Slice(0, 8));
        var hi = BinaryPrimitives.ReadInt64LittleEndian(wire.Slice(8, 8));
        return SetUuid(index, lo, hi);
    }

    /// <summary>Binds a LONG256 at <paramref name="index" /> from four 64-bit words (little-endian, w0 = least significant).</summary>
    public QwpBindValues SetLong256(int index, long w0, long w1, long w2, long w3)
    {
        Advance(index);
        WriteHeader(QwpTypeCode.Long256, isNull: false);
        WriteI64(w0);
        WriteI64(w1);
        WriteI64(w2);
        WriteI64(w3);
        return this;
    }

    /// <summary>Binds a LONG256 at <paramref name="index" /> from a non-negative <see cref="BigInteger" /> ≤ 256 bits.</summary>
    public QwpBindValues SetLong256(int index, BigInteger value)
    {
        if (value.Sign < 0)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, "LONG256 binds must be non-negative");
        }

        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: false);
        if (bytes.Length > 32)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, $"LONG256 bind exceeds 256 bits ({bytes.Length * 8})");
        }

        Span<byte> padded = stackalloc byte[32];
        bytes.CopyTo(padded);
        var w0 = BinaryPrimitives.ReadInt64LittleEndian(padded.Slice(0, 8));
        var w1 = BinaryPrimitives.ReadInt64LittleEndian(padded.Slice(8, 8));
        var w2 = BinaryPrimitives.ReadInt64LittleEndian(padded.Slice(16, 8));
        var w3 = BinaryPrimitives.ReadInt64LittleEndian(padded.Slice(24, 8));
        return SetLong256(index, w0, w1, w2, w3);
    }

    /// <summary>Binds a GEOHASH at <paramref name="index" /> with the given <paramref name="precisionBits" /> in <c>[1, 60]</c>.</summary>
    public QwpBindValues SetGeohash(int index, int precisionBits, long value)
    {
        CheckGeohashPrecision(precisionBits);
        Advance(index);
        WriteHeader(QwpTypeCode.Geohash, isNull: false);
        WriteVarint((ulong)precisionBits);
        var byteCount = (precisionBits + 7) >> 3;
        var masked = precisionBits >= 64 ? value : value & ((1L << precisionBits) - 1L);
        for (var b = 0; b < byteCount; b++)
        {
            WriteByte((byte)(masked >> (b * 8)));
        }
        return this;
    }

    /// <summary>Binds a VARCHAR at <paramref name="index" />; <c>null</c> input emits a NULL bind.</summary>
    public QwpBindValues SetVarchar(int index, string? value)
    {
        if (value is null) return SetNull(index, QwpTypeCode.Varchar);

        Advance(index);
        WriteHeader(QwpTypeCode.Varchar, isNull: false);
        var byteCount = StrictUtf8.GetByteCount(value);
        WriteI32(0);
        WriteI32(byteCount);
        EnsureCapacity(byteCount);
        var written = StrictUtf8.GetBytes(value, _buffer.AsSpan(_length, byteCount));
        if (written != byteCount)
        {
            throw new InvalidOperationException("UTF-8 byte count mismatch");
        }
        _length += byteCount;
        return this;
    }

    /// <summary>Binds a DECIMAL64 at <paramref name="index" /> with the given <paramref name="scale" /> (0–18) and unscaled int64 value.</summary>
    public QwpBindValues SetDecimal64(int index, int scale, long unscaledValue)
    {
        CheckScale(scale, Decimal64MaxScale, "DECIMAL64");
        Advance(index);
        WriteHeader(QwpTypeCode.Decimal64, isNull: false);
        WriteByte((byte)scale);
        WriteI64(unscaledValue);
        return this;
    }

    /// <summary>Binds a DECIMAL128 at <paramref name="index" /> with <paramref name="scale" /> (0–38) and a 128-bit two's-complement value (lo/hi halves).</summary>
    public QwpBindValues SetDecimal128(int index, int scale, long lo, long hi)
    {
        CheckScale(scale, Decimal128MaxScale, "DECIMAL128");
        Advance(index);
        WriteHeader(QwpTypeCode.Decimal128, isNull: false);
        WriteByte((byte)scale);
        WriteI64(lo);
        WriteI64(hi);
        return this;
    }

    /// <summary>Binds a DECIMAL256 at <paramref name="index" /> with <paramref name="scale" /> (0–76) and four 64-bit words in little-endian order.</summary>
    public QwpBindValues SetDecimal256(int index, int scale, long ll, long lh, long hl, long hh)
    {
        CheckScale(scale, Decimal256MaxScale, "DECIMAL256");
        Advance(index);
        WriteHeader(QwpTypeCode.Decimal256, isNull: false);
        WriteByte((byte)scale);
        WriteI64(ll);
        WriteI64(lh);
        WriteI64(hl);
        WriteI64(hh);
        return this;
    }

    /// <summary>Binds a NULL at <paramref name="index" /> with the given <paramref name="typeCode" />; for DECIMAL/GEOHASH variants use the type-specific overloads.</summary>
    public QwpBindValues SetNull(int index, QwpTypeCode typeCode)
    {
        switch (typeCode)
        {
            case QwpTypeCode.Decimal64: return SetNullDecimal64(index, 0);
            case QwpTypeCode.Decimal128: return SetNullDecimal128(index, 0);
            case QwpTypeCode.Decimal256: return SetNullDecimal256(index, 0);
            case QwpTypeCode.Geohash: return SetNullGeohash(index, QwpConstants.MinGeohashPrecisionBits);
        }

        if (!IsBindableType(typeCode))
        {
            throw new IngressError(ErrorCode.InvalidApiCall, $"type {typeCode} (0x{(byte)typeCode:X2}) is not bindable");
        }

        Advance(index);
        WriteHeader(typeCode, isNull: true);
        return this;
    }

    /// <summary>Binds a NULL DECIMAL64 at <paramref name="index" />; <paramref name="scale" /> is encoded so the server can route the placeholder.</summary>
    public QwpBindValues SetNullDecimal64(int index, int scale)
    {
        CheckScale(scale, Decimal64MaxScale, "DECIMAL64");
        Advance(index);
        WriteHeader(QwpTypeCode.Decimal64, isNull: true);
        WriteByte((byte)scale);
        return this;
    }

    /// <summary>Binds a NULL DECIMAL128 at <paramref name="index" />; <paramref name="scale" /> is encoded for routing.</summary>
    public QwpBindValues SetNullDecimal128(int index, int scale)
    {
        CheckScale(scale, Decimal128MaxScale, "DECIMAL128");
        Advance(index);
        WriteHeader(QwpTypeCode.Decimal128, isNull: true);
        WriteByte((byte)scale);
        return this;
    }

    /// <summary>Binds a NULL DECIMAL256 at <paramref name="index" />; <paramref name="scale" /> is encoded for routing.</summary>
    public QwpBindValues SetNullDecimal256(int index, int scale)
    {
        CheckScale(scale, Decimal256MaxScale, "DECIMAL256");
        Advance(index);
        WriteHeader(QwpTypeCode.Decimal256, isNull: true);
        WriteByte((byte)scale);
        return this;
    }

    /// <summary>Binds a NULL GEOHASH at <paramref name="index" /> with the given <paramref name="precisionBits" />.</summary>
    public QwpBindValues SetNullGeohash(int index, int precisionBits)
    {
        CheckGeohashPrecision(precisionBits);
        Advance(index);
        WriteHeader(QwpTypeCode.Geohash, isNull: true);
        WriteVarint((ulong)precisionBits);
        return this;
    }

    private void Advance(int index)
    {
        if (index != _expectedNextIndex)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"bind index out of order: expected {_expectedNextIndex}, got {index}");
        }
        if (_count >= QwpConstants.MaxBindParameters)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"too many bind parameters: exceeds {QwpConstants.MaxBindParameters}");
        }
        _expectedNextIndex++;
        _count++;
    }

    private void WriteHeader(QwpTypeCode typeCode, bool isNull)
    {
        if (!IsBindableType(typeCode))
        {
            throw new IngressError(ErrorCode.InvalidApiCall, $"type {typeCode} (0x{(byte)typeCode:X2}) is not bindable");
        }

        WriteByte((byte)typeCode);
        if (isNull)
        {
            WriteByte(NullFlagOn);
            WriteByte(NullBitmap);
        }
        else
        {
            WriteByte(NullFlagOff);
        }
    }

    private static bool IsBindableType(QwpTypeCode t)
    {
        switch (t)
        {
            case QwpTypeCode.Boolean:
            case QwpTypeCode.Byte:
            case QwpTypeCode.Short:
            case QwpTypeCode.Char:
            case QwpTypeCode.Int:
            case QwpTypeCode.Long:
            case QwpTypeCode.Float:
            case QwpTypeCode.Double:
            case QwpTypeCode.Date:
            case QwpTypeCode.Timestamp:
            case QwpTypeCode.TimestampNanos:
            case QwpTypeCode.Uuid:
            case QwpTypeCode.Long256:
            case QwpTypeCode.Geohash:
            case QwpTypeCode.Varchar:
            case QwpTypeCode.Decimal64:
            case QwpTypeCode.Decimal128:
            case QwpTypeCode.Decimal256:
                return true;
            default:
                return false;
        }
    }

    private static void CheckScale(int scale, int max, string typeName)
    {
        if (scale < 0 || scale > max)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"{typeName} scale must be in [0, {max}], got {scale}");
        }
    }

    private static void CheckGeohashPrecision(int precisionBits)
    {
        if (precisionBits < QwpConstants.MinGeohashPrecisionBits
            || precisionBits > QwpConstants.MaxGeohashPrecisionBits)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"GEOHASH precision must be in [{QwpConstants.MinGeohashPrecisionBits}, {QwpConstants.MaxGeohashPrecisionBits}], got {precisionBits}");
        }
    }

    private void WriteByte(byte b)
    {
        EnsureCapacity(1);
        _buffer[_length++] = b;
    }

    private void WriteI16(short v)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteInt16LittleEndian(_buffer.AsSpan(_length, 2), v);
        _length += 2;
    }

    private void WriteU16(ushort v)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.AsSpan(_length, 2), v);
        _length += 2;
    }

    private void WriteI32(int v)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(_length, 4), v);
        _length += 4;
    }

    private void WriteI64(long v)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.AsSpan(_length, 8), v);
        _length += 8;
    }

    private void WriteVarint(ulong value)
    {
        EnsureCapacity(QwpVarint.MaxBytes);
        _length += QwpVarint.Write(_buffer.AsSpan(_length), value);
    }

    private void EnsureCapacity(int additional)
    {
        var required = _length + additional;
        if (required <= _buffer.Length) return;
        var grown = new byte[Math.Max(required, _buffer.Length * 2)];
        Buffer.BlockCopy(_buffer, 0, grown, 0, _length);
        _buffer = grown;
    }
}
