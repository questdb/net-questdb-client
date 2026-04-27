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
///     Per-column parsed layout for one inbound RESULT_BATCH. The .NET counterpart of
///     Java's <c>QwpColumnLayout</c> on java-questdb-client main 64b7ee69, with native
///     pointers into the WS payload buffer replaced by integer offsets into
///     <see cref="QwpBatchBuffer.Payload"/>.
/// </summary>
/// <remarks>
///     Experimental. Pooled across batches — the per-row index arrays grow to the
///     maximum row count observed and never shrink. <see cref="Clear"/> wipes all
///     fields except the symbol string cache; the cache is invalidated lazily via
///     the <see cref="SymbolDictVersion"/> / <see cref="SymbolCacheVersion"/> compare
///     so connection-stable dict entries survive across batches.
/// </remarks>
internal sealed class QwpColumnLayout
{
    /// <summary>
    ///     Lazy String cache indexed by SYMBOL dict ID. Populated on first lookup so a
    ///     query over millions of rows with N distinct symbols materialises only N
    ///     strings per batch.
    /// </summary>
    public List<string?> SymbolStringCache { get; } = new();

    /// <summary>Schema column metadata (name, wire type, scale, precision).</summary>
    public QwpEgressColumnInfo? Info { get; set; }

    /// <summary>
    ///     Offset (into <see cref="QwpBatchBuffer.Payload"/>) where this column's
    ///     non-null values start. For fixed-width types this is the dense values
    ///     array; for strings/varchars it's the offsets array; for symbols it's
    ///     the dict header.
    /// </summary>
    public int ValuesOffset { get; set; }

    /// <summary>Offset of the null bitmap, or -1 when the column has no nulls.</summary>
    public int NullBitmapOffset { get; set; } = -1;

    /// <summary>Number of non-null rows in this column.</summary>
    public int NonNullCount { get; set; }

    /// <summary>Offset of the next column's data — used to walk to the next layout.</summary>
    public int NextOffset { get; set; }

    /// <summary>VARCHAR / BINARY: offset of the concatenated UTF-8 bytes after the offsets array.</summary>
    public int StringBytesOffset { get; set; }

    /// <summary>
    ///     SYMBOL: offset of the UTF-8 heap holding all dict entries. In delta mode this
    ///     points into the connection-scoped dictionary buffer; in non-delta mode it
    ///     points into the payload directly.
    /// </summary>
    public int SymbolDictHeapOffset { get; set; }

    /// <summary>
    ///     SYMBOL: offset of the packed entries array. Each entry is a 64-bit pair
    ///     <c>(offset:i32 | length:i32&lt;&lt;32)</c> relative to <see cref="SymbolDictHeapOffset"/>.
    /// </summary>
    public int SymbolDictEntriesOffset { get; set; }

    /// <summary>SYMBOL: number of valid entries.</summary>
    public int SymbolDictSize { get; set; }

    /// <summary>
    ///     Version of the dict currently bound to this layout. Encoding:
    ///     <list type="bullet">
    ///         <item>delta mode: <c>connDictGeneration &lt;&lt; 1</c> (bit 0 clear)</item>
    ///         <item>non-delta: <c>dictBase | 1</c> (bit 0 set)</item>
    ///     </list>
    ///     Consumers compare against <see cref="SymbolCacheVersion"/> to decide whether
    ///     <see cref="SymbolStringCache"/> is still valid.
    /// </summary>
    public long SymbolDictVersion { get; set; }

    /// <summary>Last <see cref="SymbolDictVersion"/> the cache was known valid under.</summary>
    public long SymbolCacheVersion { get; set; }

    /// <summary>SYMBOL: per-row dictionary IDs. Sized to row count; null rows hold stale values.</summary>
    public int[]? SymbolRowIds { get; set; }

    /// <summary>
    ///     Per-row dense lookup: <c>NonNullIdx[row]</c> is the index of <c>row</c> within
    ///     the non-null values, or -1 for null rows. Null when the column has no nulls
    ///     (the dense index then equals the row index).
    /// </summary>
    public int[]? NonNullIdx { get; set; }

    /// <summary>ARRAY: per-row offset of the array bytes; -1 for null rows.</summary>
    public int[]? ArrayRowOffsets { get; set; }

    /// <summary>ARRAY: per-row payload length in bytes.</summary>
    public int[]? ArrayRowLengths { get; set; }

    /// <summary>
    ///     SYMBOL non-delta only: managed buffer holding the per-batch packed entries
    ///     when the column carries its own dict inline. Null in delta mode.
    /// </summary>
    public byte[]? OwnedEntries { get; private set; }

    /// <summary>
    ///     TIMESTAMP / TIMESTAMP_NANOS / DATE Gorilla decode buffer. Allocated when the
    ///     column's encoding discriminator is <c>0x01</c> (DoD). Null when the column
    ///     was shipped uncompressed.
    /// </summary>
    public byte[]? TimestampDecodeBuffer { get; private set; }

    /// <summary>
    ///     Returns the dense index of <paramref name="row"/> within the non-null values.
    ///     Caller MUST have null-checked the cell first.
    /// </summary>
    public int DenseIndex(int row) =>
        NullBitmapOffset < 0 ? row : NonNullIdx![row];

    /// <summary>Wipes per-batch state. Symbol string cache is intentionally retained.</summary>
    public void Clear()
    {
        Info = null;
        ValuesOffset = 0;
        NullBitmapOffset = -1;
        NonNullCount = 0;
        StringBytesOffset = 0;
        SymbolDictHeapOffset = 0;
        SymbolDictEntriesOffset = 0;
        SymbolDictSize = 0;
        NextOffset = 0;
        // SymbolStringCache + SymbolDictVersion / SymbolCacheVersion intentionally left
        // alone — the lazy invalidation in QwpColumnBatch will compare and wipe on first
        // mismatched lookup.
    }

    /// <summary>
    ///     Ensures the owned entries buffer is at least <paramref name="requiredBytes"/>
    ///     long. Returns the buffer (caller writes into it).
    /// </summary>
    public byte[] EnsureOwnedEntries(int requiredBytes)
    {
        if (OwnedEntries is null || OwnedEntries.Length < requiredBytes)
        {
            var newCap = Math.Max(
                (OwnedEntries?.Length ?? 0) * 2,
                Math.Max(64, requiredBytes));
            OwnedEntries = new byte[newCap];
        }
        return OwnedEntries;
    }

    /// <summary>
    ///     Ensures the Gorilla timestamp-decode buffer is at least
    ///     <paramref name="requiredBytes"/> long.
    /// </summary>
    public byte[] EnsureTimestampDecodeBuffer(int requiredBytes)
    {
        if (TimestampDecodeBuffer is null || TimestampDecodeBuffer.Length < requiredBytes)
        {
            var newCap = Math.Max(
                (TimestampDecodeBuffer?.Length ?? 0) * 2,
                Math.Max(64, requiredBytes));
            TimestampDecodeBuffer = new byte[newCap];
        }
        return TimestampDecodeBuffer;
    }

    /// <summary>
    ///     Releases owned managed buffers. Called by <see cref="QwpBatchBuffer.Close"/>.
    ///     POH allocation isn't required for these — they're decoder-internal scratch.
    /// </summary>
    public void Close()
    {
        OwnedEntries = null;
        TimestampDecodeBuffer = null;
        SymbolStringCache.Clear();
    }
}
