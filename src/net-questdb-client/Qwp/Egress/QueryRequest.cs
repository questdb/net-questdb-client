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
///     User-thread-to-IO-thread hand-off carrying a query that the IO thread should
///     transmit. The .NET counterpart of Java's <c>QwpEgressIoThread.QueryRequest</c>
///     on java-questdb-client main 64b7ee69.
/// </summary>
/// <remarks>
///     <see cref="EncodedFrame"/> carries the fully-built QUERY_REQUEST frame produced
///     by <c>QwpQueryClient</c> on the user thread. The IO thread sends it verbatim.
/// </remarks>
internal sealed class QueryRequest
{
    public QueryRequest(long requestId, ReadOnlyMemory<byte> encodedFrame)
    {
        RequestId = requestId;
        EncodedFrame = encodedFrame;
    }

    /// <summary>Server-correlated request id (echoed back on RESULT_BATCH/END/ERROR).</summary>
    public long RequestId { get; }

    /// <summary>Fully-encoded QWP QUERY_REQUEST frame ready for transmission.</summary>
    public ReadOnlyMemory<byte> EncodedFrame { get; }
}
