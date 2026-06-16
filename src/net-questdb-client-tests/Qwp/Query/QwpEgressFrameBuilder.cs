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
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp.Query;

/// <summary>Test-side encoder that produces server-shaped egress frames.</summary>
internal static class QwpEgressFrameBuilder
{
    public static byte[] BuildResultBatch(
        long requestId,
        long batchSeq,
        ResultSchema schema,
        ResultBatchData data,
        DeltaSymbolDict? symbolDict = null,
        bool gorillaEnabled = true)
    {
        var payload = new MemoryStream();
        payload.WriteByte(QwpConstants.MsgKindResultBatch);
        WriteI64Le(payload, requestId);
        WriteVarint(payload, (ulong)batchSeq);

        var flags = QwpConstants.FlagDeltaSymbolDict;
        if (gorillaEnabled) flags |= QwpConstants.FlagGorilla;

        if (symbolDict is not null)
        {
            WriteVarint(payload, (ulong)symbolDict.DeltaStart);
            WriteVarint(payload, (ulong)symbolDict.Entries.Count);
            foreach (var e in symbolDict.Entries)
            {
                var bytes = Encoding.UTF8.GetBytes(e);
                WriteVarint(payload, (ulong)bytes.Length);
                payload.Write(bytes, 0, bytes.Length);
            }
        }
        else
        {
            WriteVarint(payload, 0);
            WriteVarint(payload, 0);
        }

        WriteTableBlock(payload, schema, data, gorillaEnabled, batchSeq);

        return WrapFrame(flags, tableCount: 1, payload.ToArray());
    }

    public static byte[] BuildResultEnd(long requestId, long finalSeq, long totalRows)
    {
        var payload = new MemoryStream();
        payload.WriteByte(QwpConstants.MsgKindResultEnd);
        WriteI64Le(payload, requestId);
        WriteVarint(payload, (ulong)finalSeq);
        WriteVarint(payload, (ulong)totalRows);
        return WrapFrame(flags: 0, tableCount: 0, payload.ToArray());
    }

    public static byte[] BuildQueryError(long requestId, byte status, string message)
    {
        var msgBytes = Encoding.UTF8.GetBytes(message);
        var payload = new MemoryStream();
        payload.WriteByte(QwpConstants.MsgKindQueryError);
        WriteI64Le(payload, requestId);
        payload.WriteByte(status);
        var lenBuf = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(lenBuf, (ushort)msgBytes.Length);
        payload.Write(lenBuf, 0, 2);
        payload.Write(msgBytes, 0, msgBytes.Length);
        return WrapFrame(flags: 0, tableCount: 0, payload.ToArray());
    }

    public static byte[] BuildExecDone(long requestId, byte opType, long rowsAffected)
    {
        var payload = new MemoryStream();
        payload.WriteByte(QwpConstants.MsgKindExecDone);
        WriteI64Le(payload, requestId);
        payload.WriteByte(opType);
        WriteVarint(payload, (ulong)rowsAffected);
        return WrapFrame(flags: 0, tableCount: 0, payload.ToArray());
    }

    public static byte[] BuildCacheReset(byte resetMask)
    {
        var payload = new byte[] { QwpConstants.MsgKindCacheReset, resetMask };
        return WrapFrame(flags: 0, tableCount: 0, payload);
    }

#if NET7_0_OR_GREATER
    public static byte[] CompressResultBatch(byte[] uncompressedFrame)
    {
        var existingPayloadLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(
            uncompressedFrame.AsSpan(QwpConstants.OffsetPayloadLength, 4));
        var existingFlags = uncompressedFrame[QwpConstants.OffsetFlags];
        var payload = uncompressedFrame.AsSpan(QwpConstants.HeaderSize, existingPayloadLen);

        QwpVarint.Read(payload.Slice(9), out var seqVarintLen);
        var preludeLen = 1 + 8 + seqVarintLen;

        using var compressor = new ZstdSharp.Compressor(level: 3);
        var rawBody = payload.Slice(preludeLen).ToArray();
        var compressedBody = new byte[ZstdSharp.Compressor.GetCompressBound(rawBody.Length)];
        var written = compressor.Wrap(rawBody, compressedBody);

        var newPayloadLen = preludeLen + written;
        var newPayload = new byte[newPayloadLen];
        payload.Slice(0, preludeLen).CopyTo(newPayload);
        compressedBody.AsSpan(0, written).CopyTo(newPayload.AsSpan(preludeLen));

        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(
            uncompressedFrame.AsSpan(QwpConstants.OffsetTableCount, 2));
        return WrapFrame((byte)(existingFlags | QwpConstants.FlagZstd), tableCount, newPayload);
    }
#endif

    public static byte[] BuildServerInfo(byte role, ulong epoch, uint capabilities, long serverWallNs,
        string clusterId, string nodeId, string? zoneId = null)
    {
        var clusterBytes = Encoding.UTF8.GetBytes(clusterId);
        var nodeBytes = Encoding.UTF8.GetBytes(nodeId);
        var payload = new MemoryStream();
        payload.WriteByte(QwpConstants.MsgKindServerInfo);
        payload.WriteByte(role);
        WriteU64Le(payload, epoch);
        WriteU32Le(payload, capabilities);
        WriteI64Le(payload, serverWallNs);
        WriteU16Le(payload, (ushort)clusterBytes.Length);
        payload.Write(clusterBytes, 0, clusterBytes.Length);
        WriteU16Le(payload, (ushort)nodeBytes.Length);
        payload.Write(nodeBytes, 0, nodeBytes.Length);
        if ((capabilities & QwpConstants.CapZone) != 0)
        {
            var zoneBytes = Encoding.UTF8.GetBytes(zoneId ?? string.Empty);
            WriteU16Le(payload, (ushort)zoneBytes.Length);
            payload.Write(zoneBytes, 0, zoneBytes.Length);
        }
        return WrapFrame(flags: 0, tableCount: 0, payload.ToArray());
    }

    private static byte[] WrapFrame(byte flags, ushort tableCount, byte[] payload)
    {
        var frame = new byte[QwpConstants.HeaderSize + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(QwpConstants.OffsetMagic, 4), QwpConstants.Magic);
        frame[QwpConstants.OffsetVersion] = QwpConstants.SupportedVersion;
        frame[QwpConstants.OffsetFlags] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(QwpConstants.OffsetTableCount, 2), tableCount);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(QwpConstants.OffsetPayloadLength, 4), (uint)payload.Length);
        Buffer.BlockCopy(payload, 0, frame, QwpConstants.HeaderSize, payload.Length);
        return frame;
    }

    private static void WriteTableBlock(
        MemoryStream payload, ResultSchema schema, ResultBatchData data, bool gorillaEnabled, long batchSeq)
    {
        WriteVarint(payload, 0); // empty table name
        WriteVarint(payload, (ulong)data.RowCount);

        // batch_seq == 0 carries the inline schema; continuation batches carry only rows.
        if (batchSeq == 0)
        {
            WriteVarint(payload, (ulong)schema.Columns.Count);
            foreach (var c in schema.Columns)
            {
                var nameBytes = Encoding.UTF8.GetBytes(c.Name);
                WriteVarint(payload, (ulong)nameBytes.Length);
                payload.Write(nameBytes, 0, nameBytes.Length);
                payload.WriteByte((byte)c.TypeCode);
            }
        }

        for (var i = 0; i < schema.Columns.Count; i++)
        {
            data.Columns[i].WriteTo(payload, schema.Columns[i].TypeCode, data.RowCount, gorillaEnabled);
        }
    }

    private static void WriteVarint(MemoryStream s, ulong value)
    {
        Span<byte> buf = stackalloc byte[10];
        var n = QwpVarint.Write(buf, value);
        s.Write(buf.Slice(0, n));
    }

    private static void WriteI64Le(MemoryStream s, long value)
    {
        Span<byte> buf = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buf, value);
        s.Write(buf);
    }

    private static void WriteU64Le(MemoryStream s, ulong value)
    {
        Span<byte> buf = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buf, value);
        s.Write(buf);
    }

    private static void WriteU32Le(MemoryStream s, uint value)
    {
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buf, value);
        s.Write(buf);
    }

    private static void WriteU16Le(MemoryStream s, ushort value)
    {
        Span<byte> buf = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, value);
        s.Write(buf);
    }
}

internal sealed class ResultSchema
{
    public List<SchemaColumn> Columns { get; init; } = new();
}

internal sealed record SchemaColumn(string Name, QwpTypeCode TypeCode);

internal sealed class ResultBatchData
{
    public int RowCount { get; init; }
    public List<TestColumnData> Columns { get; init; } = new();
}

internal sealed class DeltaSymbolDict
{
    public int DeltaStart { get; init; }
    public List<string> Entries { get; init; } = new();
}

internal abstract class TestColumnData
{
    public byte[]? NullBitmap { get; init; }

    public void WriteTo(MemoryStream s, QwpTypeCode wireType, int rowCount, bool gorillaEnabled)
    {
        if (NullBitmap is null)
        {
            s.WriteByte(0);
        }
        else
        {
            s.WriteByte(1);
            var expected = (rowCount + 7) >> 3;
            if (NullBitmap.Length < expected)
            {
                throw new InvalidOperationException("null bitmap too short");
            }
            s.Write(NullBitmap, 0, expected);
        }

        WriteValueRegion(s, wireType, gorillaEnabled);
    }

    protected abstract void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled);
}

internal sealed class FixedColumnData : TestColumnData
{
    public byte[] DenseBytes { get; init; }

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        s.Write(DenseBytes, 0, DenseBytes.Length);
    }
}

internal sealed class TimestampColumnData : TestColumnData
{
    public long[] DenseValues { get; init; }

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        if (!gorillaEnabled)
        {
            foreach (var v in DenseValues)
            {
                Span<byte> buf = stackalloc byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(buf, v);
                s.Write(buf);
            }
            return;
        }

        var dest = new byte[QwpGorilla.MaxEncodedSize(DenseValues.Length)];
        var n = QwpGorilla.Encode(dest, DenseValues);
        s.Write(dest, 0, n);
    }
}

internal sealed class VarcharColumnData : TestColumnData
{
    public string[] DenseValues { get; init; }

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        var bytes = new byte[DenseValues.Length][];
        var totalLen = 0;
        for (var i = 0; i < DenseValues.Length; i++)
        {
            bytes[i] = Encoding.UTF8.GetBytes(DenseValues[i]);
            totalLen += bytes[i].Length;
        }

        var offsets = new int[DenseValues.Length + 1];
        for (var i = 0; i < DenseValues.Length; i++)
        {
            offsets[i + 1] = offsets[i] + bytes[i].Length;
        }

        Span<byte> intBuf = stackalloc byte[4];
        for (var i = 0; i <= DenseValues.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(intBuf, offsets[i]);
            s.Write(intBuf);
        }
        for (var i = 0; i < DenseValues.Length; i++)
        {
            s.Write(bytes[i], 0, bytes[i].Length);
        }
    }
}

internal sealed class BinaryColumnData : TestColumnData
{
    public byte[][] DenseValues { get; init; }

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        var offsets = new int[DenseValues.Length + 1];
        for (var i = 0; i < DenseValues.Length; i++)
        {
            offsets[i + 1] = offsets[i] + DenseValues[i].Length;
        }

        Span<byte> intBuf = stackalloc byte[4];
        for (var i = 0; i <= DenseValues.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(intBuf, offsets[i]);
            s.Write(intBuf);
        }
        for (var i = 0; i < DenseValues.Length; i++)
        {
            s.Write(DenseValues[i], 0, DenseValues[i].Length);
        }
    }
}

internal sealed class SymbolColumnData : TestColumnData
{
    public int[] DenseDictIds { get; init; }

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        Span<byte> buf = stackalloc byte[10];
        foreach (var id in DenseDictIds)
        {
            var n = QwpVarint.Write(buf, (ulong)id);
            s.Write(buf.Slice(0, n));
        }
    }
}

internal sealed class DecimalColumnData : TestColumnData
{
    public byte Scale { get; init; }
    public byte[] DenseBytes { get; init; }

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        s.WriteByte(Scale);
        s.Write(DenseBytes, 0, DenseBytes.Length);
    }
}

internal sealed class GeohashColumnData : TestColumnData
{
    public int PrecisionBits { get; init; }
    public byte[] DenseBytes { get; init; }

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        Span<byte> buf = stackalloc byte[10];
        var n = QwpVarint.Write(buf, (ulong)PrecisionBits);
        s.Write(buf.Slice(0, n));
        s.Write(DenseBytes, 0, DenseBytes.Length);
    }
}

internal sealed class DoubleArrayColumnData : TestColumnData
{
    public (int[] Shape, double[] Values)[] DenseArrays { get; init; } = Array.Empty<(int[], double[])>();

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        Span<byte> dim = stackalloc byte[4];
        Span<byte> v = stackalloc byte[8];
        foreach (var (shape, values) in DenseArrays)
        {
            s.WriteByte((byte)shape.Length);
            foreach (var d in shape)
            {
                BinaryPrimitives.WriteInt32LittleEndian(dim, d);
                s.Write(dim);
            }
            foreach (var x in values)
            {
                BinaryPrimitives.WriteInt64LittleEndian(v, BitConverter.DoubleToInt64Bits(x));
                s.Write(v);
            }
        }
    }
}

internal sealed class LongArrayColumnData : TestColumnData
{
    public (int[] Shape, long[] Values)[] DenseArrays { get; init; } = Array.Empty<(int[], long[])>();

    protected override void WriteValueRegion(MemoryStream s, QwpTypeCode wireType, bool gorillaEnabled)
    {
        Span<byte> dim = stackalloc byte[4];
        Span<byte> v = stackalloc byte[8];
        foreach (var (shape, values) in DenseArrays)
        {
            s.WriteByte((byte)shape.Length);
            foreach (var d in shape)
            {
                BinaryPrimitives.WriteInt32LittleEndian(dim, d);
                s.Write(dim);
            }
            foreach (var x in values)
            {
                BinaryPrimitives.WriteInt64LittleEndian(v, x);
                s.Write(v);
            }
        }
    }
}
