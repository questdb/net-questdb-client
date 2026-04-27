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
///     Multi-chunk QWP buffer writer that supports scatter-gather sends. The .NET
///     counterpart of Java's <c>SegmentedNativeBufferWriter</c> on java-questdb-client
///     main 64b7ee69.
/// </summary>
/// <remarks>
///     Experimental. Small writes (<see cref="PutByte"/>, <see cref="PutInt"/>, etc.) flow
///     into a current <see cref="QwpPinnedBufferWriter"/> chunk. Calling
///     <see cref="PutBlockOfBytes(ReadOnlyMemory{byte})"/> flushes the current chunk and
///     appends the source as a by-reference segment, avoiding the copy. Iterate
///     <see cref="Segments"/> at send time to feed each entry to
///     <see cref="System.Net.WebSockets.ClientWebSocket.SendAsync(ReadOnlyMemory{byte}, System.Net.WebSockets.WebSocketMessageType, bool, System.Threading.CancellationToken)"/>.
///     <para/>
///     <see cref="IQwpBufferWriter.Position"/> is global (cumulative), while
///     <see cref="IQwpBufferWriter.Capacity"/>, <see cref="IQwpBufferWriter.WritableBytes"/>,
///     and <see cref="IQwpBufferWriter.GetWritableSpan"/> are chunk-local.
/// </remarks>
internal sealed class QwpSegmentedBufferWriter : IQwpBufferWriter
{
    private readonly List<QwpPinnedBufferWriter> _chunks = new();
    private readonly List<ReadOnlyMemory<byte>> _flushedSegments = new();

    private QwpPinnedBufferWriter _currentChunk;
    private long _flushedBytes;
    private int _nextChunkIndex;

    public QwpSegmentedBufferWriter()
    {
        _currentChunk = new QwpPinnedBufferWriter();
        _chunks.Add(_currentChunk);
    }

    public int Position => checked((int)(_flushedBytes + _currentChunk.Position));

    public int Capacity => _currentChunk.Capacity;

    public int WritableBytes => _currentChunk.WritableBytes;

    public Span<byte> GetWritableSpan() => _currentChunk.GetWritableSpan();

    /// <summary>
    ///     Snapshot of the writer's contents as an ordered list of byte spans. Includes
    ///     all flushed by-reference segments and the live chunk's written bytes (if any).
    ///     The returned list is a fresh copy — safe to enumerate while the writer continues
    ///     to append. After <see cref="Reset"/>, however, the chunk-backed memory views in
    ///     a previously-returned snapshot may be overwritten by new writes; consume any
    ///     snapshot fully before resetting.
    /// </summary>
    public IReadOnlyList<ReadOnlyMemory<byte>> Segments
    {
        get
        {
            var liveLen = _currentChunk.Position;
            if (liveLen == 0) return _flushedSegments.ToArray();
            var snapshot = new List<ReadOnlyMemory<byte>>(_flushedSegments.Count + 1);
            snapshot.AddRange(_flushedSegments);
            snapshot.Add(_currentChunk.AsReadOnlyMemory());
            return snapshot;
        }
    }

    public void EnsureCapacity(int additionalBytes) =>
        _currentChunk.EnsureCapacity(additionalBytes);

    public void PutByte(byte value) => _currentChunk.PutByte(value);

    public void PutShort(short value) => _currentChunk.PutShort(value);

    public void PutInt(int value) => _currentChunk.PutInt(value);

    public void PutLong(long value) => _currentChunk.PutLong(value);

    public void PutFloat(float value) => _currentChunk.PutFloat(value);

    public void PutDouble(double value) => _currentChunk.PutDouble(value);

    public void PutVarint(long value) => _currentChunk.PutVarint(value);

    public void PutString(string? value) => _currentChunk.PutString(value);

    public void PutUtf8(string? value) => _currentChunk.PutUtf8(value);

    /// <summary>
    ///     Flushes the current chunk and appends <paramref name="source"/> as a
    ///     by-reference segment. The caller is responsible for keeping the source's
    ///     backing memory alive until the writer is reset or its segments have been
    ///     consumed.
    /// </summary>
    public void PutBlockOfBytes(ReadOnlyMemory<byte> source)
    {
        if (source.IsEmpty) return;
        FlushCurrentChunk();
        _flushedSegments.Add(source);
        _flushedBytes += source.Length;
    }

    public void PatchInt(int offset, int value)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }
        // Patching is only legal within the live (un-flushed) chunk — flushed segments
        // are immutable from the writer's perspective (especially by-reference ones).
        // Cast to long for the upper-bound arithmetic so a near-int.MaxValue offset
        // can't wrap and false-pass the check.
        if (offset < _flushedBytes ||
            (long)offset > _flushedBytes + _currentChunk.Position - 4)
        {
            throw new InvalidOperationException("cannot patch flushed segment data");
        }
        _currentChunk.PatchInt(offset - (int)_flushedBytes, value);
    }

    public void Skip(int bytes) => _currentChunk.Skip(bytes);

    public void Reset()
    {
        _flushedSegments.Clear();
        _flushedBytes = 0;
        _nextChunkIndex = 0;
        foreach (var chunk in _chunks) chunk.Reset();
        _currentChunk = _chunks[0];
    }

    private void FlushCurrentChunk()
    {
        var written = _currentChunk.Position;
        if (written == 0) return;

        _flushedSegments.Add(_currentChunk.AsReadOnlyMemory());
        _flushedBytes += written;
        _currentChunk = NextChunk();
    }

    private QwpPinnedBufferWriter NextChunk()
    {
        _nextChunkIndex++;
        if (_nextChunkIndex < _chunks.Count)
        {
            var reused = _chunks[_nextChunkIndex];
            reused.Reset();
            return reused;
        }

        var fresh = new QwpPinnedBufferWriter();
        _chunks.Add(fresh);
        return fresh;
    }
}
