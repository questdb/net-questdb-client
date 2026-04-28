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

using System.Net.WebSockets;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace QuestDB.Senders;

/// <summary>
///     QWP v1 sender over WebSocket. The .NET counterpart of Java's
///     <c>QwpWebSocketSender</c> on java-questdb-client main 64b7ee69 (sync subset
///     only — PR 7 ships connect + sync flush; PR 8 will add async pipelining,
///     durable acks, and the in-flight window orchestration).
/// </summary>
/// <remarks>
///     Experimental. Composes existing QWP building blocks:
///     <list type="bullet">
///         <item><see cref="ClientWebSocketChannel"/> wraps <see cref="ClientWebSocket"/>.</item>
///         <item><see cref="QwpWebSocketEncoder"/> encodes per-table data into
///             QWP1 envelope frames.</item>
///         <item><see cref="WebSocketSendQueue"/> pumps frames asynchronously
///             and routes inbound responses to the optional <see cref="InFlightWindow"/>.</item>
///         <item><see cref="MicrobatchBuffer"/> holds the encoded frame for the
///             queue's slot.</item>
///     </list>
///     Sync mode = <c>in_flight_window = 1</c>: every <see cref="Send"/> blocks
///     until the prior message is acknowledged before sending the next.
/// </remarks>
internal sealed class QwpWebSocketSender : ISender
{
    private readonly Dictionary<string, QwpTableBuffer> _tables = new(StringComparer.Ordinal);
    private readonly QwpWebSocketEncoder _encoder = new();
    private readonly ClientWebSocket _socket;
    private readonly ClientWebSocketChannel _channel;
    private readonly InFlightWindow? _inFlightWindow;
    private readonly WebSocketSendQueue _queue;

    private QwpTableBuffer? _currentTable;
    private bool _disposed;
    private DateTime _lastFlush = DateTime.MinValue;

    public QwpWebSocketSender(SenderOptions options) : this(options, requestDurableAck: false) { }

    /// <summary>
    ///     Constructs a sender with optional durable-ack opt-in. When
    ///     <paramref name="requestDurableAck"/> is true, the upgrade request carries
    ///     <c>X-QWP-Request-Durable-Ack: true</c> so the server interleaves
    ///     STATUS_DURABLE_ACK frames with the regular STATUS_OK acks; the durable
    ///     seqTxn watermarks are then exposed via <see cref="GetHighestDurableSeqTxn"/>.
    /// </summary>
    public QwpWebSocketSender(SenderOptions options, bool requestDurableAck)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));

        var addr = options.addr ?? throw new IngressError(ErrorCode.InvalidApiCall,
            "ws::/wss:: requires an addr= configuration entry");
        var scheme = options.protocol == ProtocolType.wss ? "wss" : "ws";
        var url = new Uri($"{scheme}://{addr}/qwp/v1");

        _socket = new ClientWebSocket();
        if (requestDurableAck)
        {
            _socket.Options.SetRequestHeader("X-QWP-Request-Durable-Ack", "true");
        }
        _channel = new ClientWebSocketChannel(_socket);

        try
        {
            _socket.ConnectAsync(url, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _socket.Dispose();
            throw new IngressError(ErrorCode.SocketError,
                $"WebSocket connect to {url} failed", ex);
        }

        _inFlightWindow = options.in_flight_window > 0
            ? new InFlightWindow(options.in_flight_window, InFlightWindow.DEFAULT_TIMEOUT_MS)
            : null;
        _queue = new WebSocketSendQueue(_channel, _inFlightWindow);
    }

    public SenderOptions Options { get; }

    public int Length
    {
        get
        {
            var total = 0;
            foreach (var t in _tables.Values) total += t.RowCount > 0 ? 1 : 0;
            return total;
        }
    }

    public int RowCount
    {
        get
        {
            var total = 0;
            foreach (var t in _tables.Values) total += t.RowCount;
            return total;
        }
    }

    public bool WithinTransaction => false;

    public DateTime LastFlush => _lastFlush;

    public ISender Transaction(ReadOnlySpan<char> tableName) =>
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpWebSocketSender does not support transactions");

    public void Rollback() =>
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpWebSocketSender does not support transactions");

    public Task CommitAsync(CancellationToken ct = default) =>
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpWebSocketSender does not support transactions");

    public void Commit(CancellationToken ct = default) =>
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpWebSocketSender does not support transactions");

    public ISender Table(ReadOnlySpan<char> name)
    {
        ThrowIfDisposed();
        var nameStr = name.ToString();
        if (!_tables.TryGetValue(nameStr, out var table))
        {
            table = new QwpTableBuffer(nameStr);
            _tables[nameStr] = table;
        }
        _currentTable = table;
        return this;
    }

    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_SYMBOL, useNullBitmap: true);
        col?.AddSymbol(value.ToString());
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_VARCHAR, useNullBitmap: true);
        col?.AddString(value.ToString());
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_LONG, useNullBitmap: false);
        col?.AddLong(value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, int value) => Column(name, (long)value);

    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_BOOLEAN, useNullBitmap: false);
        col?.AddBoolean(value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_DOUBLE, useNullBitmap: false);
        col?.AddDouble(value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, DateTime value) =>
        ColumnNanos(name, (value.Ticks - 621355968000000000L) * 100);

    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value) =>
        Column(name, value.UtcDateTime);

    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_TIMESTAMP_NANOS, useNullBitmap: false);
        col?.AddLong(timestampNanos);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, char value)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_CHAR, useNullBitmap: false);
        col?.AddShort((short)value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        if (!value.TryWriteBytes(bytes)) throw new InvalidOperationException("Guid.TryWriteBytes failed");
        var lo = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(0, 8));
        var hi = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8, 8));
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_UUID, useNullBitmap: false);
        if (col is not null)
        {
            col.AddLong(lo);
            col.AddLong(hi);
        }
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, decimal value)
    {
        QwpColumnEncoding.EncodeDecimal(value, out var scale, out var high, out var low, out var fitsIn64);
        if (fitsIn64)
        {
            var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_DECIMAL64, useNullBitmap: true);
            col?.AddDecimal64(low, scale);
        }
        else
        {
            var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_DECIMAL128, useNullBitmap: true);
            col?.AddDecimal128(high, low, scale);
        }
        return this;
    }

    public ISender ColumnDecimal256(ReadOnlySpan<char> name, long hh, long hl, long lh, long ll, int scale)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_DECIMAL256, useNullBitmap: true);
        col?.AddDecimal256(hh, hl, lh, ll, scale);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct
    {
        var wireType = QwpColumnEncoding.WireTypeForArrayElement(typeof(T));
        var col = RequireTable().GetOrCreateColumn(name.ToString(), wireType, useNullBitmap: true);
        if (col is not null) QwpColumnEncoding.AddArray(col, value, shape);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Array value)
    {
        if (value is null) return this;  // matches HTTP/TCP no-op semantics
        var elementType = value.GetType().GetElementType()
            ?? throw new IngressError(ErrorCode.InvalidApiCall, "array has no element type");
        var wireType = QwpColumnEncoding.WireTypeForArrayElement(elementType);
        var col = RequireTable().GetOrCreateColumn(name.ToString(), wireType, useNullBitmap: true);
        if (col is not null) QwpColumnEncoding.AddArray(col, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        var wireType = QwpColumnEncoding.WireTypeForArrayElement(typeof(T));
        var col = RequireTable().GetOrCreateColumn(name.ToString(), wireType, useNullBitmap: true);
        if (col is not null) QwpColumnEncoding.AddArray(col, value);
        return this;
    }

    public void At(DateTime value, CancellationToken ct = default) =>
        AtNanos((value.Ticks - 621355968000000000L) * 100, ct);

    public void At(DateTimeOffset value, CancellationToken ct = default) => At(value.UtcDateTime, ct);

    public void At(long value, CancellationToken ct = default) => AtNanos(value * 1_000L, ct);

    public ValueTask AtAsync(DateTime value, CancellationToken ct = default)
    {
        At(value, ct);
        return ValueTask.CompletedTask;
    }

    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default)
    {
        At(value, ct);
        return ValueTask.CompletedTask;
    }

    public ValueTask AtAsync(long value, CancellationToken ct = default)
    {
        At(value, ct);
        return ValueTask.CompletedTask;
    }

    public void AtNanos(long timestampNanos, CancellationToken ct = default)
    {
        var col = RequireTable().GetOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP_NANOS);
        col.AddLong(timestampNanos);
        RequireTable().NextRow();
        MaybeAutoFlush(ct);
    }

    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default)
    {
        var col = RequireTable().GetOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP_NANOS);
        col.AddLong(timestampNanos);
        RequireTable().NextRow();
        return MaybeAutoFlushAsync(ct);
    }

    /// <summary>
    ///     §2.1 — checks the configured auto-flush thresholds and ships the buffered
    ///     rows when any tripped. Mirrors <c>AbstractSender.FlushIfNecessary</c>:
    ///     <list type="bullet">
    ///         <item><see cref="SenderOptions.auto_flush_rows"/> — buffered row count cap.</item>
    ///         <item><see cref="SenderOptions.auto_flush_bytes"/> — buffered byte budget
    ///             (mirrors <c>AbstractSender</c>'s <c>Length</c>; for QWP we use the
    ///             count of tables-with-rows since the wire-byte total is paid only at
    ///             encode time).</item>
    ///         <item><see cref="SenderOptions.auto_flush_interval"/> — wall-clock interval
    ///             since the last flush.</item>
    ///     </list>
    ///     Disabled when <see cref="SenderOptions.auto_flush"/> = <c>off</c> or any individual
    ///     threshold is non-positive. WebSocket has no transaction concept so no
    ///     <c>WithinTransaction</c> guard is required.
    /// </summary>
    private void MaybeAutoFlush(CancellationToken ct)
    {
        if (ShouldAutoFlush()) Send(ct);
    }

    private ValueTask MaybeAutoFlushAsync(CancellationToken ct)
    {
        return ShouldAutoFlush() ? new ValueTask(SendAsync(ct)) : ValueTask.CompletedTask;
    }

    private bool ShouldAutoFlush()
    {
        if (Options.auto_flush != Enums.AutoFlushType.on) return false;
        if (Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows) return true;
        if (Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes) return true;
        if (Options.auto_flush_interval > TimeSpan.Zero)
        {
            // Seed the timer on the first commit so the interval is measured from
            // sender construction-or-flush, not DateTime.MinValue (which would trip
            // the threshold instantly). Mirrors AbstractSender.GuardLastFlushNotSet.
            if (_lastFlush == DateTime.MinValue) _lastFlush = DateTime.UtcNow;
            else if (DateTime.UtcNow - _lastFlush >= Options.auto_flush_interval) return true;
        }
        return false;
    }

    public void AtNow(CancellationToken ct = default) => AtNanos(DateTime.UtcNow.Ticks * 100, ct);

    public ValueTask AtNowAsync(CancellationToken ct = default)
    {
        AtNow(ct);
        return ValueTask.CompletedTask;
    }

    public void Send(CancellationToken ct = default) => SendAsync(ct).GetAwaiter().GetResult();

    public Task SendAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        foreach (var table in _tables.Values)
        {
            if (table.RowCount == 0) continue;
            var size = _encoder.Encode(table, useSchemaRef: false);
            // Stage the encoded frame in a MicrobatchBuffer the queue can consume.
            var buffer = new MicrobatchBuffer(size);
            buffer.Write(_encoder.AsReadOnlySpan().Slice(0, size));
            buffer.IncrementRowCount();
            buffer.Seal();
            // Enqueue is the back-pressure point: in sync mode (in_flight_window=1) the
            // next Enqueue blocks until the previous batch is acked; in async mode
            // (in_flight_window>1) up to N batches sit in-flight before back-pressure
            // kicks in.
            _queue.Enqueue(buffer);
            table.Reset();
        }
        // Wait for the IO thread to actually place the bytes on the wire.
        _queue.Flush();
        _lastFlush = DateTime.UtcNow;
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Blocks until every in-flight batch has been acknowledged by the server.
    ///     Use this for at-least-once durability guarantees; for sync mode (in_flight_window=1)
    ///     the natural back-pressure on <see cref="SendAsync"/> already serialises ack
    ///     visibility, but explicit waits still help on shutdown.
    /// </summary>
    public void AwaitPendingAcks() => _queue.AwaitPendingAcks();

    /// <summary>
    ///     Round-trips a WebSocket-level ping, blocking until the matching pong arrives.
    ///     The server flushes pending durable acks before sending the pong, so a
    ///     successful Ping leaves the durable-seqTxn watermarks at their post-ping state.
    /// </summary>
    public void Ping(TimeSpan? timeout = null) => _queue.Ping(timeout);

    /// <summary>Highest committed seqTxn observed for <paramref name="tableName"/> (0 if absent).</summary>
    public long GetHighestAckedSeqTxn(string tableName) => _queue.GetCommittedSeqTxn(tableName);

    /// <summary>Highest durable seqTxn observed for <paramref name="tableName"/> (0 if absent).</summary>
    public long GetHighestDurableSeqTxn(string tableName) => _queue.GetDurableSeqTxn(tableName);

    public void Truncate() { /* no-op; per-table buffers grow on demand */ }

    public void CancelRow() => _currentTable?.CancelCurrentRow();

    public void Clear()
    {
        foreach (var table in _tables.Values) table.Reset();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try { _queue.Close(); } catch { }
        try { _socket.Dispose(); } catch { }
        _channel.Dispose();
        _inFlightWindow?.Dispose();
        _queue.Dispose();
    }

    private QwpTableBuffer RequireTable() => _currentTable ??
        throw new IngressError(ErrorCode.InvalidApiCall, "Table(name) must be called before adding columns");

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(QwpWebSocketSender));
    }

}
