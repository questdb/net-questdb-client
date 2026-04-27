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

using System.Buffers.Binary;
using System.Text;

namespace QuestDB.Qwp;

/// <summary>
///     Binary response framing for the QWP v1 WebSocket protocol. The .NET counterpart
///     of Java's <c>WebSocketResponse</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Experimental. Frame layouts (all multi-byte fields little-endian):
///     <list type="bullet">
///         <item><b>OK (0x00)</b>: <c>status(1) + sequence(8) + tableCount(2) + (nameLen(2)+name+seqTxn(8))×N</c>. Min 11 bytes.</item>
///         <item><b>DURABLE_ACK (0x02)</b>: <c>status(1) + tableCount(2) + (nameLen(2)+name+seqTxn(8))×N</c>. Min 3 bytes.</item>
///         <item><b>Error</b>: <c>status(1) + sequence(8) + messageLen(2) + utf8 message</c>. Min 11 bytes.</item>
///     </list>
/// </remarks>
internal sealed class WebSocketResponse
{
    public const int MAX_ERROR_MESSAGE_LENGTH = 1024;
    public const int MIN_DURABLE_ACK_SIZE = 3;
    public const int MIN_ERROR_RESPONSE_SIZE = 11;
    public const int MIN_OK_RESPONSE_SIZE = 11;

    public const byte STATUS_OK = 0x00;
    public const byte STATUS_DURABLE_ACK = 0x02;
    public const byte STATUS_SCHEMA_MISMATCH = 0x03;
    public const byte STATUS_PARSE_ERROR = 0x05;
    public const byte STATUS_INTERNAL_ERROR = 0x06;
    public const byte STATUS_SECURITY_ERROR = 0x08;
    public const byte STATUS_WRITE_ERROR = 0x09;

    private readonly List<string> _tableNames = new();
    private readonly List<long> _tableSeqTxns = new();
    private string? _errorMessage;
    private long _sequence;
    private byte _status;

    public WebSocketResponse()
    {
        _status = STATUS_OK;
        _sequence = 0;
        _errorMessage = null;
    }

    public byte Status => _status;

    public long Sequence => _sequence;

    public string? ErrorMessage => _errorMessage;

    public bool IsSuccess => _status == STATUS_OK;

    public bool IsDurableAck => _status == STATUS_DURABLE_ACK;

    public int TableEntryCount => _tableNames.Count;

    public string GetTableName(int index) => _tableNames[index];

    public long GetTableSeqTxn(int index) => _tableSeqTxns[index];

    public string GetStatusName() => _status switch
    {
        STATUS_OK => "OK",
        STATUS_DURABLE_ACK => "DURABLE_ACK",
        STATUS_PARSE_ERROR => "PARSE_ERROR",
        STATUS_SCHEMA_MISMATCH => "SCHEMA_MISMATCH",
        STATUS_WRITE_ERROR => "WRITE_ERROR",
        STATUS_SECURITY_ERROR => "SECURITY_ERROR",
        STATUS_INTERNAL_ERROR => "INTERNAL_ERROR",
        _ => $"UNKNOWN({_status & 0xFF})",
    };

    /// <summary>
    ///     Parses <paramref name="source"/> into this response instance. Returns
    ///     <c>false</c> if the frame is truncated or malformed (callers can also use
    ///     <see cref="IsStructurallyValid"/> to pre-check).
    /// </summary>
    public bool ReadFrom(ReadOnlySpan<byte> source)
    {
        _tableNames.Clear();
        _tableSeqTxns.Clear();

        if (source.Length < 1) return false;
        _status = source[0];

        if (_status == STATUS_OK)
        {
            if (source.Length < MIN_OK_RESPONSE_SIZE) return false;
            _sequence = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(1, 8));
            _errorMessage = null;
            return ReadTableEntries(source.Slice(9));
        }

        if (_status == STATUS_DURABLE_ACK)
        {
            if (source.Length < MIN_DURABLE_ACK_SIZE) return false;
            _sequence = -1;
            _errorMessage = null;
            return ReadTableEntries(source.Slice(1));
        }

        // Error response
        if (source.Length < MIN_ERROR_RESPONSE_SIZE) return false;
        _sequence = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(1, 8));
        var msgLen = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(9, 2));
        if (source.Length < MIN_ERROR_RESPONSE_SIZE + msgLen) return false;
        _errorMessage = msgLen > 0 ? Encoding.UTF8.GetString(source.Slice(11, msgLen)) : null;
        return true;
    }

    /// <summary>Wire size when serialised by <see cref="WriteTo"/>.</summary>
    public int SerializedSize()
    {
        if (_status == STATUS_OK) return MIN_OK_RESPONSE_SIZE + TableEntriesSize();
        if (_status == STATUS_DURABLE_ACK) return MIN_DURABLE_ACK_SIZE + TableEntriesSize();
        var msgBytes = _errorMessage is null ? 0 : Encoding.UTF8.GetByteCount(_errorMessage);
        return MIN_ERROR_RESPONSE_SIZE + Math.Min(msgBytes, MAX_ERROR_MESSAGE_LENGTH);
    }

    /// <summary>Serialises this response into <paramref name="destination"/>. Returns bytes written.</summary>
    public int WriteTo(Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = _status;

        if (_status == STATUS_OK)
        {
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset, 8), _sequence);
            offset += 8;
            offset += WriteTableEntries(destination.Slice(offset));
        }
        else if (_status == STATUS_DURABLE_ACK)
        {
            offset += WriteTableEntries(destination.Slice(offset));
        }
        else
        {
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset, 8), _sequence);
            offset += 8;
            var msgBytes = _errorMessage is null ? 0 : Encoding.UTF8.GetByteCount(_errorMessage);
            msgBytes = Math.Min(msgBytes, MAX_ERROR_MESSAGE_LENGTH);
            BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset, 2), (ushort)msgBytes);
            offset += 2;
            if (msgBytes > 0)
            {
                Encoding.UTF8.GetBytes(_errorMessage!, destination.Slice(offset, msgBytes));
                offset += msgBytes;
            }
        }
        return offset;
    }

    /// <summary>
    ///     Validates the frame structure without allocating string state. Used by the
    ///     receiver loop to drop malformed frames before paying for the parse.
    /// </summary>
    public static bool IsStructurallyValid(ReadOnlySpan<byte> source)
    {
        if (source.Length < 1) return false;
        var status = source[0];
        if (status == STATUS_OK)
        {
            if (source.Length < MIN_OK_RESPONSE_SIZE) return false;
            return ValidateTableEntries(source.Slice(9));
        }
        if (status == STATUS_DURABLE_ACK)
        {
            if (source.Length < MIN_DURABLE_ACK_SIZE) return false;
            return ValidateTableEntries(source.Slice(1));
        }
        if (source.Length < MIN_ERROR_RESPONSE_SIZE) return false;
        var msgLen = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(9, 2));
        return source.Length == MIN_ERROR_RESPONSE_SIZE + msgLen;
    }

    public override string ToString()
    {
        if (IsSuccess) return $"WebSocketResponse{{status=OK, seq={_sequence}, tables={_tableNames.Count}}}";
        if (IsDurableAck) return $"WebSocketResponse{{status=DURABLE_ACK, tables={_tableNames.Count}}}";
        return $"WebSocketResponse{{status={GetStatusName()}, seq={_sequence}, error={_errorMessage}}}";
    }

    // --- Test-only factories ---

    /// <summary>Builds a STATUS_OK response with no per-table entries.</summary>
    public static WebSocketResponse Success(long sequence)
    {
        return new WebSocketResponse { _status = STATUS_OK, _sequence = sequence };
    }

    /// <summary>Builds a STATUS_DURABLE_ACK response with a single per-table entry.</summary>
    public static WebSocketResponse DurableAck(string tableName, long seqTxn)
    {
        var r = new WebSocketResponse { _status = STATUS_DURABLE_ACK, _sequence = -1 };
        r._tableNames.Add(tableName);
        r._tableSeqTxns.Add(seqTxn);
        return r;
    }

    /// <summary>Builds an error response.</summary>
    public static WebSocketResponse Error(long sequence, byte status, string errorMessage)
    {
        return new WebSocketResponse { _status = status, _sequence = sequence, _errorMessage = errorMessage };
    }

    /// <summary>Adds an OK or DURABLE_ACK table entry. Used by tests building multi-table responses.</summary>
    internal void AddTableEntry(string name, long seqTxn)
    {
        _tableNames.Add(name);
        _tableSeqTxns.Add(seqTxn);
    }

    private bool ReadTableEntries(ReadOnlySpan<byte> source)
    {
        if (source.Length < 2) return false;
        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(0, 2));
        var offset = 2;
        for (var i = 0; i < tableCount; i++)
        {
            if (source.Length < offset + 2) return false;
            var nameLen = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(offset, 2));
            offset += 2;
            if (nameLen == 0 || source.Length < offset + nameLen + 8) return false;
            var name = Encoding.UTF8.GetString(source.Slice(offset, nameLen));
            offset += nameLen;
            var seqTxn = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(offset, 8));
            offset += 8;
            _tableNames.Add(name);
            _tableSeqTxns.Add(seqTxn);
        }
        return source.Length == offset;
    }

    private static bool ValidateTableEntries(ReadOnlySpan<byte> source)
    {
        if (source.Length < 2) return false;
        var tableCount = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(0, 2));
        var offset = 2;
        for (var i = 0; i < tableCount; i++)
        {
            if (source.Length < offset + 2) return false;
            var nameLen = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(offset, 2));
            offset += 2;
            if (nameLen == 0 || source.Length < offset + nameLen + 8) return false;
            offset += nameLen + 8;
        }
        return source.Length == offset;
    }

    private int WriteTableEntries(Span<byte> destination)
    {
        var offset = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset, 2), (ushort)_tableNames.Count);
        offset += 2;
        for (var i = 0; i < _tableNames.Count; i++)
        {
            var name = _tableNames[i];
            var nameBytes = Encoding.UTF8.GetByteCount(name);
            BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset, 2), (ushort)nameBytes);
            offset += 2;
            Encoding.UTF8.GetBytes(name, destination.Slice(offset, nameBytes));
            offset += nameBytes;
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset, 8), _tableSeqTxns[i]);
            offset += 8;
        }
        return offset;
    }

    private int TableEntriesSize()
    {
        var size = 2; // tableCount
        for (var i = 0; i < _tableNames.Count; i++)
        {
            size += 2 + Encoding.UTF8.GetByteCount(_tableNames[i]) + 8;
        }
        return size;
    }
}
