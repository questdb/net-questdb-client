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
///     Buffer writer for QWP v1 message encoding. The .NET counterpart of Java's
///     <c>QwpBufferWriter</c> on java-questdb-client main 64b7ee69. All multi-byte
///     integers are written little-endian.
/// </summary>
/// <remarks>
///     Experimental. Two implementations:
///     <list type="bullet">
///         <item><see cref="QwpPinnedBufferWriter"/> — single contiguous POH-pinned array.</item>
///         <item><see cref="QwpSegmentedBufferWriter"/> — chunked, supports scatter-gather
///         via by-reference <see cref="PutBlockOfBytes(System.ReadOnlyMemory{byte})"/>.</item>
///     </list>
///     <see cref="Position"/> is global (cumulative across flushed segments for the segmented
///     writer), while <see cref="Capacity"/>, <see cref="WritableBytes"/>, and
///     <see cref="GetWritableSpan"/> are chunk-local.
/// </remarks>
internal interface IQwpBufferWriter
{
    /// <summary>Number of bytes written since the last <see cref="Reset"/> (global).</summary>
    int Position { get; }

    /// <summary>Capacity of the current chunk in bytes.</summary>
    int Capacity { get; }

    /// <summary>Number of bytes available for direct writing in the current chunk.</summary>
    int WritableBytes { get; }

    /// <summary>
    ///     Returns a span over the writable region of the current chunk. The span has length
    ///     <see cref="WritableBytes"/>. After writing into it, call <see cref="Skip"/> to
    ///     advance <see cref="Position"/>. Invalidated by any operation that grows the
    ///     underlying buffer.
    /// </summary>
    Span<byte> GetWritableSpan();

    /// <summary>
    ///     Ensures the current chunk has room for at least <paramref name="additionalBytes"/>
    ///     beyond the current position. May reallocate.
    /// </summary>
    void EnsureCapacity(int additionalBytes);

    void PutByte(byte value);

    /// <summary>Writes a 16-bit value little-endian.</summary>
    void PutShort(short value);

    /// <summary>Writes a 32-bit value little-endian.</summary>
    void PutInt(int value);

    /// <summary>Writes a 64-bit value little-endian.</summary>
    void PutLong(long value);

    /// <summary>Writes a 32-bit float little-endian.</summary>
    void PutFloat(float value);

    /// <summary>Writes a 64-bit double little-endian.</summary>
    void PutDouble(double value);

    /// <summary>Writes an unsigned LEB128 varint.</summary>
    void PutVarint(long value);

    /// <summary>
    ///     Writes a varint length prefix followed by the UTF-8 bytes of <paramref name="value"/>.
    ///     Null or empty input writes a single zero byte.
    /// </summary>
    void PutString(string? value);

    /// <summary>Writes raw UTF-8 bytes without a length prefix. Null or empty is a no-op.</summary>
    void PutUtf8(string? value);

    /// <summary>
    ///     Writes a block of bytes. The pinned implementation copies into the current chunk;
    ///     the segmented implementation flushes the current chunk and records the source by
    ///     reference for later scatter-gather sends.
    /// </summary>
    void PutBlockOfBytes(ReadOnlyMemory<byte> source);

    /// <summary>
    ///     Patches a 32-bit little-endian integer at the given offset, used to backfill
    ///     length placeholders. Offset must be within already-written data of the current
    ///     chunk (segmented writers cannot patch flushed segments).
    /// </summary>
    void PatchInt(int offset, int value);

    /// <summary>Advances the position by <paramref name="bytes"/> without writing.</summary>
    void Skip(int bytes);

    /// <summary>Resets the writer for reuse without freeing memory.</summary>
    void Reset();
}
