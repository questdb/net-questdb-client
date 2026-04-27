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

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Pooled per-batch container owned by the egress IO thread. Holds a POH-pinned
///     scratch buffer the inbound RESULT_BATCH payload is copied into, plus the
///     per-column <see cref="QwpColumnLayout"/> pool used while decoding and the
///     <see cref="QwpColumnBatch"/> view the user's handler sees. The .NET
///     counterpart of Java's <c>QwpBatchBuffer</c> on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     Experimental. Lifecycle: IO thread takes a buffer from the free pool, calls
///     <see cref="CopyFromPayload"/>, hands it to the decoder, pushes the resulting
///     batch onto the event queue. User thread pops, runs the handler, releases the
///     buffer to the pool. While the user thread holds it the IO thread is free to
///     grab a different buffer for the next frame.
/// </remarks>
internal sealed class QwpBatchBuffer
{
    private readonly List<QwpColumnLayout> _layoutPool = new();
    private readonly QwpColumnBatch _batch = new();

    private byte[] _scratch;
    private int _payloadLen;

    public QwpBatchBuffer(int initialCapacity)
    {
        if (initialCapacity < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialCapacity), "must be non-negative");
        }
        _scratch = initialCapacity == 0
            ? Array.Empty<byte>()
            : GC.AllocateUninitializedArray<byte>(initialCapacity, pinned: true);
    }

    /// <summary>The shared <see cref="QwpColumnBatch"/> view; set up by the decoder.</summary>
    public QwpColumnBatch Batch => _batch;

    /// <summary>Pool of column layouts the decoder reuses across batches.</summary>
    public List<QwpColumnLayout> LayoutPool => _layoutPool;

    /// <summary>Number of bytes copied in by the most recent <see cref="CopyFromPayload"/>.</summary>
    public int PayloadLength => _payloadLen;

    /// <summary>Read-only span over the staging buffer's current payload.</summary>
    public ReadOnlySpan<byte> Payload => _scratch.AsSpan(0, _payloadLen);

    /// <summary>Read-only memory over the staging buffer's current payload.</summary>
    public ReadOnlyMemory<byte> PayloadMemory => _scratch.AsMemory(0, _payloadLen);

    /// <summary>Current scratch capacity in bytes (may exceed <see cref="PayloadLength"/>).</summary>
    public int Capacity => _scratch.Length;

    /// <summary>
    ///     Copies <paramref name="source"/> into the staging buffer, growing if needed.
    ///     Call once per inbound frame before handing the buffer to the decoder.
    /// </summary>
    public void CopyFromPayload(ReadOnlySpan<byte> source)
    {
        EnsureCapacity(source.Length);
        source.CopyTo(_scratch);
        _payloadLen = source.Length;
    }

    /// <summary>Releases pooled column layouts. Pinned scratch is GC-managed.</summary>
    public void Close()
    {
        foreach (var l in _layoutPool) l.Close();
        _layoutPool.Clear();
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _scratch.Length) return;
        // Doubling, capped at int.MaxValue. Start from max(current,1) so a 0-cap
        // initial buffer can grow.
        var newCap = Math.Max(_scratch.Length, 1);
        while (newCap < required)
        {
            if (newCap > int.MaxValue / 2)
            {
                newCap = int.MaxValue;
                break;
            }
            newCap <<= 1;
        }
        if (newCap < required)
        {
            throw new OutOfMemoryException(
                $"QwpBatchBuffer required capacity {required} exceeds Int32.MaxValue");
        }
        var bigger = GC.AllocateUninitializedArray<byte>(newCap, pinned: true);
        _scratch.AsSpan(0, _payloadLen).CopyTo(bigger);
        _scratch = bigger;
    }
}
