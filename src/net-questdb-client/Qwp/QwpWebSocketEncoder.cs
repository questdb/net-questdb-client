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

namespace QuestDB.Qwp;

/// <summary>
///     Encodes QWP v1 messages for WebSocket transport. The .NET counterpart of Java's
///     <c>QwpWebSocketEncoder</c> on java-questdb-client main 64b7ee69. Wraps
///     <see cref="QwpColumnWriter"/> with the 12-byte QWP1 envelope (magic + version +
///     flags + tableCount + payloadLength). Supports multi-table batching via
///     <see cref="BeginMessage"/> / <see cref="AddTable"/> / <see cref="FinishMessage"/>.
/// </summary>
/// <remarks>
///     Experimental. Wire layout (all multi-byte fields little-endian):
///     <list type="bullet">
///         <item>byte 0..3: <c>'Q' 'W' 'P' '1'</c> magic</item>
///         <item>byte 4: version (<see cref="QwpConstants.VERSION_1"/>)</item>
///         <item>byte 5: flags (<see cref="QwpConstants.FLAG_GORILLA"/> /
///             <see cref="QwpConstants.FLAG_DELTA_SYMBOL_DICT"/> / <see cref="QwpConstants.FLAG_ZSTD"/>)</item>
///         <item>byte 6..7: tableCount (u16)</item>
///         <item>byte 8..11: payloadLength (i32, patched at <see cref="FinishMessage"/>)</item>
///     </list>
/// </remarks>
internal sealed class QwpWebSocketEncoder
{
    private const int DEFAULT_BUFFER_SIZE = 8192;
    private const int HEADER_PAYLOAD_LENGTH_OFFSET = 8;

    private readonly QwpColumnWriter _columnWriter = new();
    private readonly QwpPinnedBufferWriter _buffer;

    private byte _flags;
    private int _payloadStart;
    private byte _version = QwpConstants.VERSION_1;

    public QwpWebSocketEncoder() : this(DEFAULT_BUFFER_SIZE) { }

    public QwpWebSocketEncoder(int bufferSize)
    {
        _buffer = new QwpPinnedBufferWriter(bufferSize);
    }

    public bool IsGorillaEnabled
    {
        get => (_flags & QwpConstants.FLAG_GORILLA) != 0;
        set
        {
            if (value) _flags |= QwpConstants.FLAG_GORILLA;
            else _flags &= unchecked((byte)~QwpConstants.FLAG_GORILLA);
        }
    }

    public byte Version
    {
        get => _version;
        set => _version = value;
    }

    /// <summary>Number of bytes currently buffered (header + payload-so-far).</summary>
    public int Position => _buffer.Position;

    /// <summary>Read-only view over the encoded message.</summary>
    public ReadOnlyMemory<byte> AsReadOnlyMemory() => _buffer.AsReadOnlyMemory();

    /// <summary>Read-only span over the encoded message.</summary>
    public ReadOnlySpan<byte> AsReadOnlySpan() => _buffer.AsReadOnlySpan();

    /// <summary>
    ///     Single-table convenience encode. No delta-dict prefix; the payloadLength is
    ///     patched in place. Returns total message size.
    /// </summary>
    public int Encode(QwpTableBuffer tableBuffer, bool useSchemaRef)
    {
        _buffer.Reset();
        WriteHeader(tableCount: 1, payloadLength: 0);
        var payloadStart = _buffer.Position;
        _columnWriter.SetBuffer(_buffer);
        _columnWriter.EncodeTable(tableBuffer, useSchemaRef, useGlobalSymbols: false, useGorilla: IsGorillaEnabled);
        var payloadLength = _buffer.Position - payloadStart;
        _buffer.PatchInt(HEADER_PAYLOAD_LENGTH_OFFSET, payloadLength);
        return _buffer.Position;
    }

    /// <summary>
    ///     Begins a multi-table message. The header is written with the FLAG_DELTA_SYMBOL_DICT
    ///     bit set; the body opens with the deltaStart / deltaCount varints + new symbols
    ///     added since <paramref name="confirmedMaxId"/>. Subsequent
    ///     <see cref="AddTable"/> calls append per-table data; <see cref="FinishMessage"/>
    ///     patches the payload length.
    /// </summary>
    public void BeginMessage(
        int tableCount,
        GlobalSymbolDictionary globalDict,
        int confirmedMaxId,
        int batchMaxId)
    {
        _buffer.Reset();
        var deltaStart = confirmedMaxId + 1;
        var deltaCount = Math.Max(0, batchMaxId - confirmedMaxId);

        // Header carries the delta-dict flag. We restore the writer's persistent flags
        // afterwards so caller-set Gorilla state isn't clobbered.
        var origFlags = _flags;
        _flags = (byte)(origFlags | QwpConstants.FLAG_DELTA_SYMBOL_DICT);
        WriteHeader(tableCount, payloadLength: 0);
        _flags = origFlags;

        _payloadStart = _buffer.Position;
        _buffer.PutVarint(deltaStart);
        _buffer.PutVarint(deltaCount);
        for (var id = deltaStart; id < deltaStart + deltaCount; id++)
        {
            _buffer.PutString(globalDict.GetSymbol(id));
        }
        _columnWriter.SetBuffer(_buffer);
    }

    /// <summary>Appends a table's encoded data to a message in progress (between Begin/Finish).</summary>
    public void AddTable(QwpTableBuffer tableBuffer, bool useSchemaRef)
    {
        _columnWriter.EncodeTable(tableBuffer, useSchemaRef, useGlobalSymbols: true, useGorilla: IsGorillaEnabled);
    }

    /// <summary>Finalises the message — patches payloadLength and returns total bytes.</summary>
    public int FinishMessage()
    {
        var payloadLength = _buffer.Position - _payloadStart;
        _buffer.PatchInt(HEADER_PAYLOAD_LENGTH_OFFSET, payloadLength);
        return _buffer.Position;
    }

    /// <summary>One-shot encode of a single table with the delta-dict path.</summary>
    public int EncodeWithDeltaDict(
        QwpTableBuffer tableBuffer,
        GlobalSymbolDictionary globalDict,
        int confirmedMaxId,
        int batchMaxId,
        bool useSchemaRef)
    {
        BeginMessage(tableCount: 1, globalDict, confirmedMaxId, batchMaxId);
        AddTable(tableBuffer, useSchemaRef);
        return FinishMessage();
    }

    /// <summary>Writes the 12-byte QWP1 header. <paramref name="payloadLength"/> may be 0 when patched later.</summary>
    public void WriteHeader(int tableCount, int payloadLength)
    {
        _buffer.PutByte((byte)'Q');
        _buffer.PutByte((byte)'W');
        _buffer.PutByte((byte)'P');
        _buffer.PutByte((byte)'1');
        _buffer.PutByte(_version);
        _buffer.PutByte(_flags);
        _buffer.PutShort((short)tableCount);
        _buffer.PutInt(payloadLength);
    }
}
