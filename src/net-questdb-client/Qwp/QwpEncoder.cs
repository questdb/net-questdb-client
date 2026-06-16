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

using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Serialises one or more <see cref="QwpTableBuffer" />s into a single QWP v1 frame.
/// </summary>
/// <remarks>
///     <b>Single multi-table message per flush</b>: one frame holds every non-empty table, with a
///     shared delta-symbol-dictionary prelude. The wire <c>tableCount</c> field is a uint16,
///     capped at <see cref="QwpConstants.MaxTablesPerMessage" />.
///     <para />
///     <b>Inline schemas only.</b> Each table block carries its column definitions inline; the wire
///     no longer has a schema-mode byte or schema-id. The encoder is stateless across flushes.
///     <para />
///     <b>FLAG_DELTA_SYMBOL_DICT</b> is always set; symbol columns reference connection-global ids.
///     In self-sufficient mode the prelude carries the dictionary prefix from id 0 covering every
///     id the frame references; in delta mode it carries only the delta since the last flush.
///     When a frame references no symbols, the prelude is empty (<c>0x00 0x00</c>) but still written.
///     <para />
///     <b>FLAG_GORILLA</b> is set when <c>gorillaEnabled</c> is requested. Each TIMESTAMP /
///     TIMESTAMP_NANOS column body is then prefixed with an <c>encoding_flag</c> byte
///     (<c>0x00</c> uncompressed, <c>0x01</c> Gorilla DoD); the encoder transparently falls back to
///     uncompressed when DoDs overflow int32, and always emits the flag (even for all-null columns).
///     <para />
///     The encoder reads the symbol dictionary but never mutates it; in self-sufficient mode it
///     re-emits the dictionary prefix from id 0 on every frame, so there is no per-flush watermark
///     to advance.
/// </remarks>
internal static class QwpEncoder
{
    private const int InitialCapacity = 4096;

    /// <summary>
    ///     Encodes the supplied tables into a single QWP frame.
    /// </summary>
    /// <param name="tables">Non-empty tables to include. The caller is expected to filter out tables with zero rows.</param>
    /// <param name="symbolDictionary">Connection-global symbol dictionary; only the delta is emitted.</param>
    /// <param name="selfSufficient">
    ///     If <c>true</c>, the symbol delta prelude starts at id 0 — the receiver needs no prior
    ///     connection state. Required by store-and-forward mode where each frame must be replayable
    ///     in isolation. Column schemas always travel inline regardless of this flag. Defaults to <c>false</c>.
    /// </param>
    /// <param name="gorillaEnabled">
    ///     If <c>true</c>, the frame's flags byte carries <c>FLAG_GORILLA</c> and every TIMESTAMP /
    ///     TIMESTAMP_NANOS column is preceded by an <c>encoding_flag</c> byte. The encoder
    ///     transparently falls back to uncompressed per column when DoDs overflow int32.
    ///     Defaults to <c>false</c>.
    /// </param>
    /// <returns>The complete QWP frame, including the 12-byte header.</returns>
    /// <remarks>Allocates per call. Production paths use <see cref="EncodeInto" /> directly.</remarks>
    internal static byte[] Encode(
        IReadOnlyList<QwpTableBuffer> tables,
        QwpSymbolDictionary symbolDictionary,
        bool selfSufficient = false,
        bool gorillaEnabled = false)
    {
        var builder = new FrameBuilder(InitialCapacity);
        var len = EncodeInto(builder, tables, symbolDictionary, selfSufficient, gorillaEnabled);
        var result = new byte[len];
        builder.AsSpan(0, len).CopyTo(result);
        return result;
    }

    /// <summary>
    ///     Encodes a frame into the provided <see cref="FrameBuilder" />, reusing the builder's
    ///     internal buffer across calls. Used by the double-buffered async path to avoid a
    ///     per-flush allocation.
    /// </summary>
    /// <returns>Number of bytes written into the builder; the caller reads <c>builder.AsSpan(0, length)</c>.</returns>
    internal static int EncodeInto(
        FrameBuilder builder,
        IReadOnlyList<QwpTableBuffer> tables,
        QwpSymbolDictionary symbolDictionary,
        bool selfSufficient = false,
        bool gorillaEnabled = false,
        int symbolDeltaCount = -1)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(symbolDictionary);

        if (tables.Count > QwpConstants.MaxTablesPerMessage)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"too many tables ({tables.Count}); the wire ceiling is {QwpConstants.MaxTablesPerMessage}");
        }

        builder.Reset();

        // Reserve the 12-byte header; we patch it at the end once the payload length is known.
        builder.Allocate(QwpConstants.HeaderSize);

        WriteDeltaSymbolDict(builder, symbolDictionary, selfSufficient, symbolDeltaCount);

        var emittedTableCount = 0;
        for (var i = 0; i < tables.Count; i++)
        {
            var t = tables[i];
            if (t.RowCount == 0 || t.TotalColumnCount == 0)
            {
                continue;
            }

            WriteTableBlock(builder, t, gorillaEnabled);
            emittedTableCount++;
        }

        var payloadLength = builder.Length - QwpConstants.HeaderSize;
        if (payloadLength > QwpConstants.MaxBatchBytes)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"payload size {payloadLength} bytes exceeds the {QwpConstants.MaxBatchBytes}-byte limit; flush more often");
        }

        // Patch header.
        var header = builder.AsSpan(0, QwpConstants.HeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(QwpConstants.OffsetMagic, 4), QwpConstants.Magic);
        header[QwpConstants.OffsetVersion] = QwpConstants.SupportedVersion;
        var flags = QwpConstants.FlagDeltaSymbolDict;
        if (gorillaEnabled)
        {
            flags |= QwpConstants.FlagGorilla;
        }

        header[QwpConstants.OffsetFlags] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(QwpConstants.OffsetTableCount, 2), (ushort)emittedTableCount);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(QwpConstants.OffsetPayloadLength, 4), (uint)payloadLength);

        return builder.Length;
    }

    private static void WriteDeltaSymbolDict(FrameBuilder buf, QwpSymbolDictionary symbols, bool selfSufficient, int symbolDeltaCount)
    {
        int deltaStart, deltaCount;
        if (selfSufficient)
        {
            // Re-emit the dictionary prefix [0, symbolDeltaCount) every frame;
            // symbolDeltaCount < 0 means the whole dictionary.
            deltaStart = 0;
            deltaCount = symbolDeltaCount >= 0 ? symbolDeltaCount : symbols.Count;
        }
        else
        {
            deltaStart = symbols.DeltaStart;
            deltaCount = symbols.DeltaCount;
        }

        if (deltaStart + deltaCount > symbols.Count)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"symbol delta range [{deltaStart}, {deltaStart + deltaCount}) exceeds dictionary size {symbols.Count}");
        }

        buf.WriteVarint((ulong)deltaStart);
        buf.WriteVarint((ulong)deltaCount);

        for (var i = deltaStart; i < deltaStart + deltaCount; i++)
        {
            var sym = symbols.GetSymbol(i);
            WriteString(buf, sym);
        }
    }

    private static void WriteTableBlock(FrameBuilder buf, QwpTableBuffer table, bool gorillaEnabled)
    {
        WriteString(buf, table.TableName);
        buf.WriteVarint((ulong)table.RowCount);
        buf.WriteVarint((ulong)table.TotalColumnCount);

        // Column definitions are always inline. User columns first, designated timestamp last.
        for (var i = 0; i < table.Columns.Count; i++)
        {
            WriteColumnDef(buf, table.Columns[i]);
        }

        if (table.DesignatedTimestampColumn is not null)
        {
            WriteColumnDef(buf, table.DesignatedTimestampColumn);
        }

        // Column data sections (in the same order as definitions: user columns first, designated TS last).
        for (var i = 0; i < table.Columns.Count; i++)
        {
            WriteColumnData(buf, table.Columns[i], table.RowCount, gorillaEnabled);
        }

        if (table.DesignatedTimestampColumn is not null)
        {
            WriteColumnData(buf, table.DesignatedTimestampColumn, table.RowCount, gorillaEnabled);
        }
    }

    private static void WriteColumnDef(FrameBuilder buf, QwpColumn col)
    {
        if (!col.IsTyped)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, $"column '{col.Name}' has no type assigned");
        }

        WriteString(buf, col.Name);
        buf.WriteByte((byte)col.TypeCode);
    }

    private static void WriteColumnData(FrameBuilder buf, QwpColumn col, int rowCount, bool gorillaEnabled)
    {
        // An untyped column has no per-type metadata to emit; a short body would desync the wire.
        if (!col.IsTyped)
        {
            throw new IngressError(ErrorCode.InvalidApiCall, $"column '{col.Name}' has no type assigned");
        }

        // Null flag + optional bitmap
        if (col.NullCount == 0)
        {
            buf.WriteByte(0);
        }
        else
        {
            buf.WriteByte(1);
            var bitmapBytes = (rowCount + 7) >> 3;
            buf.WriteBytes(col.NullBitmap!.AsSpan(0, bitmapBytes));
        }

        var n = col.NonNullCount;

        // FLAG_GORILLA promises a per-column encoding-flag byte; emit it even when all values are null.
        if (gorillaEnabled && col.TypeCode is QwpTypeCode.Timestamp or QwpTypeCode.TimestampNanos && n == 0)
        {
            buf.WriteByte(QwpGorilla.EncodingUncompressed);
            return;
        }

        // Several types carry per-column metadata (offset table for VARCHAR/BINARY, scale byte for
        // DECIMAL*, precision varint for GEOHASH) that the wire format requires regardless of
        // value count. Skip the early-return for those.
        if (n == 0)
        {
            switch (col.TypeCode)
            {
                case QwpTypeCode.Varchar:
                case QwpTypeCode.Binary:
                    buf.WriteUInt32LittleEndian(0);
                    return;
                case QwpTypeCode.Decimal64:
                case QwpTypeCode.Decimal128:
                case QwpTypeCode.Decimal256:
                    Debug.Assert(col.DecimalScaleSet,
                        "DECIMAL column with no non-null values must still have a scale set on first typing");
                    buf.WriteByte(col.DecimalScale);
                    return;
                case QwpTypeCode.Geohash:
                    Debug.Assert(col.GeohashPrecisionBits >= 1 && col.GeohashPrecisionBits <= 60,
                        "GEOHASH column precision must be in [1, 60] before encode");
                    buf.WriteVarint((ulong)col.GeohashPrecisionBits);
                    return;
                default:
                    return;
            }
        }

        switch (col.TypeCode)
        {
            case QwpTypeCode.Boolean:
                var boolBytes = (n + 7) >> 3;
                buf.WriteBytes(col.BoolData!.AsSpan(0, boolBytes));
                break;

            case QwpTypeCode.Timestamp:
            case QwpTypeCode.TimestampNanos:
                if (gorillaEnabled)
                {
                    WriteTimestampColumnGorilla(buf, col, n);
                }
                else
                {
                    buf.WriteBytes(col.FixedData!.AsSpan(0, col.FixedLen));
                }

                break;

            case QwpTypeCode.Byte:
            case QwpTypeCode.Short:
            case QwpTypeCode.Int:
            case QwpTypeCode.Long:
            case QwpTypeCode.Float:
            case QwpTypeCode.Double:
            case QwpTypeCode.Date:
            case QwpTypeCode.Uuid:
            case QwpTypeCode.Char:
            case QwpTypeCode.IPv4:
            case QwpTypeCode.Long256:
            case QwpTypeCode.DoubleArray:
            case QwpTypeCode.LongArray:
                // Fixed-width primitives, LONG256, and arrays all store their wire-ready bytes
                // back-to-back in FixedData. The encoder dumps them verbatim.
                buf.WriteBytes(col.FixedData!.AsSpan(0, col.FixedLen));
                break;

            case QwpTypeCode.Varchar:
            case QwpTypeCode.Binary:
                // (n + 1) uint32 LE offsets — bulk-copy on LE hosts, scalar fallback on BE.
                if (BitConverter.IsLittleEndian)
                {
                    var offsetsBytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(
                        col.StrOffsets.AsSpan(0, n + 1));
                    buf.WriteBytes(offsetsBytes);
                }
                else
                {
                    for (var i = 0; i <= n; i++)
                    {
                        buf.WriteUInt32LittleEndian(col.StrOffsets![i]);
                    }
                }

                buf.WriteBytes(col.StrData!.AsSpan(0, col.StrLen));
                break;

            case QwpTypeCode.Symbol:
                // varint global ids, one per non-null row.
                for (var i = 0; i < n; i++)
                {
                    buf.WriteVarint((ulong)col.SymbolIds![i]);
                }

                break;

            case QwpTypeCode.Decimal64:
            case QwpTypeCode.Decimal128:
            case QwpTypeCode.Decimal256:
                // 1-byte scale prefix + (8|16|32) bytes per value, LE two's complement.
                buf.WriteByte(col.DecimalScale);
                buf.WriteBytes(col.FixedData!.AsSpan(0, col.FixedLen));
                break;

            case QwpTypeCode.Geohash:
                // varint precision_bits + ceil(precision/8) × value_count packed bytes.
                buf.WriteVarint((ulong)col.GeohashPrecisionBits);
                buf.WriteBytes(col.FixedData!.AsSpan(0, col.FixedLen));
                break;

            default:
                throw new IngressError(ErrorCode.InvalidApiCall,
                    $"encoder does not yet support type {col.TypeCode}");
        }
    }

    private static void WriteTimestampColumnGorilla(FrameBuilder buf, QwpColumn col, int valueCount)
    {
        // FixedData stores values as little-endian int64. On LE hosts we can reinterpret the byte
        // span directly; on BE hosts we'd need per-element byte-swapping. Use the LE branch and
        // fall back to a per-element read otherwise so Gorilla works regardless of host endianness.
        var bytes = col.FixedData!.AsSpan(0, valueCount * 8);

        Span<long> timestamps;
        long[]? rentedTimestamps = null;

        if (BitConverter.IsLittleEndian)
        {
            timestamps = MemoryMarshal.Cast<byte, long>(bytes);
        }
        else
        {
            rentedTimestamps = ArrayPool<long>.Shared.Rent(valueCount);
            timestamps = rentedTimestamps.AsSpan(0, valueCount);
            for (var i = 0; i < valueCount; i++)
            {
                timestamps[i] = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(i * 8, 8));
            }
        }

        var maxSize = QwpGorilla.MaxEncodedSize(valueCount);
        var rented = ArrayPool<byte>.Shared.Rent(maxSize);
        try
        {
            var written = QwpGorilla.Encode(rented.AsSpan(0, maxSize), timestamps);
            buf.WriteBytes(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
            if (rentedTimestamps is not null)
            {
                ArrayPool<long>.Shared.Return(rentedTimestamps);
            }
        }
    }

    private static void WriteString(FrameBuilder buf, string value)
    {
        if (value.Length == 0)
        {
            buf.WriteVarint(0);
            return;
        }

        var maxBytes = QwpConstants.StrictUtf8.GetMaxByteCount(value.Length);
        if (maxBytes <= 256)
        {
            Span<byte> scratch = stackalloc byte[256];
            var written = QwpConstants.StrictUtf8.GetBytes(value, scratch);
            buf.WriteVarint((ulong)written);
            buf.WriteBytes(scratch.Slice(0, written));
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(maxBytes);
        try
        {
            var written = QwpConstants.StrictUtf8.GetBytes(value, rented);
            buf.WriteVarint((ulong)written);
            buf.WriteBytes(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    ///     Internal byte buffer with simple grow-by-doubling and span access. Class (not struct) so
    ///     the encoder can pass it by reference without ref-struct contortions.
    /// </summary>
    internal sealed class FrameBuilder
    {
        private byte[] _buf;

        public FrameBuilder(int initialCapacity)
        {
            _buf = new byte[initialCapacity];
        }

        public int Length { get; private set; }

        /// <summary>Returns the underlying buffer's <see cref="ReadOnlyMemory{T}" /> view of the encoded frame.</summary>
        public ReadOnlyMemory<byte> WrittenMemory => _buf.AsMemory(0, Length);

        public void Reset()
        {
            // Buffer stays; only the write head moves back. Capacity grows monotonically across flushes.
            Length = 0;
        }

        public Span<byte> Allocate(int count)
        {
            EnsureCapacity(Length + count);
            var span = _buf.AsSpan(Length, count);
            Length += count;
            return span;
        }

        public void WriteByte(byte b)
        {
            EnsureCapacity(Length + 1);
            _buf[Length++] = b;
        }

        public void WriteBytes(ReadOnlySpan<byte> bytes)
        {
            bytes.CopyTo(Allocate(bytes.Length));
        }

        public void WriteUInt32LittleEndian(uint v)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(Allocate(4), v);
        }

        public void WriteVarint(ulong v)
        {
            EnsureCapacity(Length + QwpVarint.MaxBytes);
            Length += QwpVarint.Write(_buf.AsSpan(Length), v);
        }

        public Span<byte> AsSpan(int start, int length)
        {
            return _buf.AsSpan(start, length);
        }

        public byte[] ToArray()
        {
            var result = new byte[Length];
            Array.Copy(_buf, result, Length);
            return result;
        }

        private void EnsureCapacity(int required)
        {
            if (required < 0)
            {
                throw new IngressError(ErrorCode.InvalidApiCall,
                    "encoder buffer requirement overflowed int.MaxValue");
            }

            if (_buf.Length >= required)
            {
                return;
            }

            var newSize = (long)_buf.Length;
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
                    $"encoder buffer required size {required} exceeds the {int.MaxValue}-byte cap");
            }

            Array.Resize(ref _buf, (int) newSize);
        }
    }
}
