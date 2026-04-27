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
///     Tagged event pushed from the egress IO thread onto the user-facing event queue.
///     One event per RESULT_BATCH / RESULT_END / QUERY_ERROR received from the server,
///     plus a synthetic transport-error event when the connection drops mid-query.
///     The .NET counterpart of Java's <c>QueryEvent</c> on java-questdb-client main 64b7ee69.
/// </summary>
internal sealed class QueryEvent
{
    public const int KIND_BATCH = 0;
    public const int KIND_END = 1;
    public const int KIND_ERROR = 2;
    public const int KIND_EXEC_DONE = 3;

    /// <summary>
    ///     Synthesised on the IO thread when the server closes the socket, the receive
    ///     loop raises, or the IO thread exits abnormally. Distinct from <see cref="KIND_ERROR"/>
    ///     (server-emitted QUERY_ERROR) so <c>QwpQueryClient.Execute</c> can decide
    ///     whether to trigger failover without reconstructing the classification later.
    /// </summary>
    public const int KIND_TRANSPORT_ERROR = 4;

    public int Kind { get; private set; } = -1;
    public QwpBatchBuffer? Buffer { get; private set; }
    public string? ErrorMessage { get; private set; }
    public byte ErrorStatus { get; private set; }
    public short OpType { get; private set; }
    public long RowsAffected { get; private set; }
    public long TotalRows { get; private set; }

    public QueryEvent AsBatch(QwpBatchBuffer buffer)
    {
        Reset();
        Kind = KIND_BATCH;
        Buffer = buffer;
        return this;
    }

    public QueryEvent AsEnd(long totalRows)
    {
        Reset();
        Kind = KIND_END;
        TotalRows = totalRows;
        return this;
    }

    public QueryEvent AsError(byte status, string? message)
    {
        Reset();
        Kind = KIND_ERROR;
        ErrorStatus = status;
        ErrorMessage = message;
        return this;
    }

    public QueryEvent AsExecDone(short opType, long rowsAffected)
    {
        Reset();
        Kind = KIND_EXEC_DONE;
        OpType = opType;
        RowsAffected = rowsAffected;
        return this;
    }

    public QueryEvent AsTransportError(byte status, string? message)
    {
        Reset();
        Kind = KIND_TRANSPORT_ERROR;
        ErrorStatus = status;
        ErrorMessage = message;
        return this;
    }

    /// <summary>
    ///     Drops object references and resets primitives so a pooled event can be
    ///     reused across queries. The <c>AsX</c> builders call this internally; tests
    ///     call it directly to verify the pool-safety contract.
    /// </summary>
    public void Reset()
    {
        Kind = -1;
        Buffer = null;
        ErrorMessage = null;
        ErrorStatus = 0;
        OpType = 0;
        RowsAffected = 0;
        TotalRows = 0;
    }
}
