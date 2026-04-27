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

using System.Text;

namespace QuestDB.Qwp.Egress;

/// <summary>
///     QWP egress (query results) client. The .NET counterpart of Java's
///     <c>QwpQueryClient</c> on java-questdb-client main 64b7ee69 — scoped to the
///     foundation that wires up <see cref="QwpBindValues"/>, the QUERY_REQUEST frame
///     encoder, and <see cref="QwpEgressIoThread"/>.
/// </summary>
/// <remarks>
///     Experimental. PR 11c covers the foundation: <see cref="Execute(string, IQwpColumnBatchHandler)"/>
///     and the binds overload, with handler dispatch driven off the IO thread's
///     event queue. Features deferred to follow-up sub-PRs and the public API:
///     <list type="bullet">
///         <item>multi-endpoint routing + connection-string parsing,</item>
///         <item>SERVER_INFO-based role filtering during connect,</item>
///         <item>failover on transport failure (transport errors surface as
///             <see cref="IQwpColumnBatchHandler.OnError"/> for now),</item>
///         <item>cancel + cache-reset propagation.</item>
///     </list>
///     Thread safety: not thread-safe for concurrent queries on the same client.
/// </remarks>
internal sealed class QwpQueryClient : IDisposable
{
    private readonly QwpBindValues _bindValues = new();
    private readonly QwpEgressIoThread _ioThread;
    private readonly QwpPinnedBufferWriter _frameScratch = new();
    private long _nextRequestId;
    private bool _closed;

    public QwpQueryClient(IWebSocketChannel channel, int bufferPoolSize = 4)
    {
        if (channel is null) throw new ArgumentNullException(nameof(channel));
        _ioThread = new QwpEgressIoThread(channel, bufferPoolSize);
        _ioThread.Start();
    }

    /// <summary>Number of QUERY_REQUEST frames the client has submitted since construction.</summary>
    public long QueriesSubmitted => Interlocked.Read(ref _nextRequestId);

    /// <summary>Executes <paramref name="sql"/> with no binds.</summary>
    public void Execute(string sql, IQwpColumnBatchHandler handler) =>
        Execute(sql, binds: null, handler);

    /// <summary>
    ///     Executes <paramref name="sql"/> with optional bind parameters supplied via
    ///     <paramref name="binds"/>. Drives the supplied <paramref name="handler"/>
    ///     synchronously until the query reaches a terminal frame (RESULT_END,
    ///     EXEC_DONE, QUERY_ERROR, or TRANSPORT_ERROR).
    /// </summary>
    public void Execute(string sql, QwpBindSetter? binds, IQwpColumnBatchHandler handler)
    {
        if (_closed) throw new InvalidOperationException("QwpQueryClient is closed");
        if (sql is null) throw new ArgumentNullException(nameof(sql));
        if (handler is null) throw new ArgumentNullException(nameof(handler));

        // 1. Encode binds on the user thread.
        _bindValues.Reset();
        binds?.Invoke(_bindValues);

        // 2. Build the QUERY_REQUEST frame.
        var requestId = Interlocked.Increment(ref _nextRequestId);
        var frame = BuildQueryFrame(sql, requestId, _bindValues);

        // 3. Hand the frame to the IO thread.
        _ioThread.SubmitQueryAsync(new QueryRequest(requestId, frame)).GetAwaiter().GetResult();

        // 4. Drain events until we hit a terminal one.
        while (true)
        {
            var ev = _ioThread.TakeEvent();
            try
            {
                switch (ev.Kind)
                {
                    case QueryEvent.KIND_BATCH:
                        var buf = ev.Buffer!;
                        try { handler.OnBatch(buf.Batch); }
                        finally { _ioThread.ReleaseBuffer(buf); }
                        break;
                    case QueryEvent.KIND_END:
                        handler.OnEnd(ev.TotalRows);
                        return;
                    case QueryEvent.KIND_ERROR:
                        handler.OnError(ev.ErrorStatus, ev.ErrorMessage);
                        return;
                    case QueryEvent.KIND_EXEC_DONE:
                        handler.OnExecDone(ev.OpType, ev.RowsAffected);
                        return;
                    case QueryEvent.KIND_TRANSPORT_ERROR:
                        // No failover yet — surface as a plain handler error.
                        handler.OnError(ev.ErrorStatus, ev.ErrorMessage);
                        return;
                    default:
                        throw new InvalidOperationException(
                            $"unexpected QueryEvent.Kind={ev.Kind} from IO thread");
                }
            }
            finally
            {
                _ioThread.ReleaseEvent(ev);
            }
        }
    }

    public void Dispose()
    {
        if (_closed) return;
        _closed = true;
        _ioThread.Dispose();
    }

    private ReadOnlyMemory<byte> BuildQueryFrame(string sql, long requestId, QwpBindValues binds)
    {
        // Wire layout: msg_kind(1B) | request_id(8B LE) | sql_len(varint) | sql(UTF-8)
        // | initial_credit(varint, 0=unbounded) | bind_count(varint) | bind_payload(...).
        // No 12-byte QWP1 envelope: the egress send path is asymmetric (responses
        // carry the envelope, queries don't — matches Java's sendQueryRequest).
        _frameScratch.Reset();
        _frameScratch.PutByte(QwpEgressMsgKind.QUERY_REQUEST);
        _frameScratch.PutLong(requestId);
        var sqlBytes = Encoding.UTF8.GetBytes(sql);
        _frameScratch.PutVarint(sqlBytes.Length);
        _frameScratch.PutBlockOfBytes(sqlBytes);
        _frameScratch.PutVarint(0);            // initial_credit
        _frameScratch.PutVarint(binds.Count);
        if (binds.Count > 0)
        {
            _frameScratch.PutBlockOfBytes(binds.BufferSpan.ToArray());
        }
        // Snapshot to a fresh array so the caller can safely retain it past the
        // next Execute (the IO thread copies on send, but cheap to be defensive).
        return _frameScratch.AsReadOnlySpan().ToArray();
    }
}
