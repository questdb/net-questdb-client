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
///     <b>FLAG_DELTA_SYMBOL_DICT</b> is always set; symbol columns reference connection-global ids
///     and the prelude carries only the delta since the last successful flush. When a frame
///     contains no symbol columns, the delta is empty (<c>0x00 0x00</c>) but the prelude is still
///     written.
///     <para />
///     <b>FLAG_GORILLA</b> is never set in v1; timestamp columns are written as plain little-endian
///     int64 arrays.
///     <para />
///     The encoder reads the symbol dictionary and schema cache but does not advance their
///     committed watermarks. The caller (<c>QwpWebSocketSender</c>) must call
///     <see cref="QwpSymbolDictionary.Commit" /> after a successful flush.
/// </remarks>
internal static class QwpEncoder
{
    private const int InitialCapacity = 4096;

    /// <summary>
    ///     Encodes the supplied tables into a single QWP frame.
    /// </summary>
    /// <param name="tables">Non-empty tables to include. The caller is expected to filter out tables with zero rows.</param>
    /// <param name="schemaCache">Per-connection schema id allocator and reuse cache.</param>
    /// <param name="symbolDictionary">Connection-global symbol dictionary; only the delta is emitted.</param>
    /// <param name="selfSufficient">
    ///     If <c>true</c>, the frame emits every table's schema in full mode and the symbol delta
    ///     starts at id 0 — the receiver needs no prior connection state. Required by store-and-forward
    ///     mode where each frame must be replayable in isolation. Defaults to <c>false</c>.
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
        QwpSchemaCache schemaCache,
        QwpSymbolDictionary symbolDictionary,
        bool selfSufficient = false,
        bool gorillaEnabled = false)
    {
        var builder = new FrameBuilder(InitialCapacity);
        var len = EncodeInto(builder, tables, schemaCache, symbolDictionary, selfSufficient, gorillaEnabled);
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
        QwpSchemaCache schemaCache,
        QwpSymbolDictionary symbolDictionary,
        bool selfSufficient = false,
        bool gorillaEnabled = false)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(schemaCache);
        ArgumentNullException.ThrowIfNull(symbolDictionary);

        if (tables.Count > QwpConstants.MaxTablesPerMessage)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"too many tables ({tables.Count}); the wire ceiling is {QwpConstants.MaxTablesPerMessage}");
        }

        builder.Reset();

        // Reserve the 12-byte header; we patch it at the end once the payload length is known.
        builder.Allocate(QwpConstants.HeaderSize);

        WriteDeltaSymbolDict(builder, symbolDictionary, selfSufficient);

        for (var i = 0; i < tables.Count; i++)
        {
            // In self-sufficient mode the receiver has no prior connection state, so every frame
            // re-registers schemas using frame-local indices (0..tables.Count-1). The shared
            // QwpSchemaCache stays untouched; frame-local ids never collide because every frame
            // emits FULL.
            var localSchemaId = selfSufficient ? i : -1;
            WriteTableBlock(builder, tables[i], schemaCache, selfSufficient, gorillaEnabled, localSchemaId);
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
        header[QwpConstants.OffsetVersion] = QwpConstants.SupportedIngestVersion;
        var flags = QwpConstants.FlagDeltaSymbolDict;
        if (gorillaEnabled)
        {
            flags |= QwpConstants.FlagGorilla;
        }

        header[QwpConstants.OffsetFlags] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(QwpConstants.OffsetTableCount, 2), (ushort)tables.Count);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(QwpConstants.OffsetPayloadLength, 4), (uint)payloadLength);

        return builder.Length;
    }

    private static void WriteDeltaSymbolDict(FrameBuilder buf, QwpSymbolDictionary symbols, bool selfSufficient)
    {
        var deltaStart = selfSufficient ? 0 : symbols.DeltaStart;
        var deltaCount = selfSufficient ? symbols.Count : symbols.DeltaCount;

        buf.WriteVarint((ulong)deltaStart);
        buf.WriteVarint((ulong)deltaCount);

        for (var i = deltaStart; i < deltaStart + deltaCount; i++)
        {
            var sym = symbols.GetSymbol(i);
            WriteString(buf, sym);
        }
    }

    private static void WriteTableBlock(FrameBuilder buf, QwpTableBuffer table, QwpSchemaCache schemaCache, bool selfSufficient, bool gorillaEnabled, int localSchemaId)
    {
        WriteString(buf, table.TableName);
        buf.WriteVarint((ulong)table.RowCount);
        buf.WriteVarint((ulong)table.TotalColumnCount);

        byte mode;
        int schemaId;
        if (selfSufficient)
        {
            // Frame-local id, FULL schema, no schemaCache mutation. Each SF frame is replayable
            // standalone — no per-connection counter dependency.
            schemaId = localSchemaId;
            mode = QwpConstants.SchemaModeFull;
        }
        else
        {
            (mode, schemaId) = schemaCache.PrepareSchema(table);
        }

        buf.WriteByte(mode);
        buf.WriteVarint((ulong)schemaId);

        if (mode == QwpConstants.SchemaModeFull)
        {
            for (var i = 0; i < table.Columns.Count; i++)
            {
                WriteColumnDef(buf, table.Columns[i]);
            }

            if (table.DesignatedTimestampColumn is not null)
            {
                WriteColumnDef(buf, table.DesignatedTimestampColumn);
            }
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
        if (n == 0)
        {
            return;
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
            case QwpTypeCode.Long256:
            case QwpTypeCode.DoubleArray:
            case QwpTypeCode.LongArray:
                // Fixed-width primitives, LONG256, and arrays all store their wire-ready bytes
                // back-to-back in FixedData. The encoder dumps them verbatim.
                buf.WriteBytes(col.FixedData!.AsSpan(0, col.FixedLen));
                break;

            case QwpTypeCode.Varchar:
                // (n + 1) uint32 LE offsets, then concatenated UTF-8 bytes.
                for (var i = 0; i <= n; i++)
                {
                    buf.WriteUInt32LittleEndian(col.StrOffsets![i]);
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

            case QwpTypeCode.Decimal128:
                // 1-byte scale prefix + 16 bytes per value LE two's complement.
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
        var byteCount = Encoding.UTF8.GetByteCount(value);
        buf.WriteVarint((ulong)byteCount);
        if (byteCount == 0)
        {
            return;
        }

        var dest = buf.Allocate(byteCount);
        Encoding.UTF8.GetBytes(value, dest);
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
            if (_buf.Length >= required)
            {
                return;
            }

            var newSize = _buf.Length;
            while (newSize < required)
            {
                newSize *= 2;
            }

            Array.Resize(ref _buf, newSize);
        }
    }
}
