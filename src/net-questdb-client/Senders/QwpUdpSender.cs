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

using System.Net;
using System.Net.Sockets;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace QuestDB.Senders;

/// <summary>
///     Fire-and-forget QWP v1 sender over UDP. The .NET counterpart of Java's
///     <c>QwpUdpSender</c> on java-questdb-client main 64b7ee69 (skeleton scope —
///     PR 6b ships the public surface and a single-table round-trip; the long tail
///     of per-type Column overloads, datagram fragmentation, and adaptive headroom
///     ship in PR 6c).
/// </summary>
/// <remarks>
///     Experimental. Builds rows into a per-table <see cref="QwpTableBuffer"/>;
///     <see cref="Send"/> encodes each table via <see cref="QwpWebSocketEncoder"/>
///     and ships it as a UDP datagram. Local symbol dictionaries (no global / delta
///     dict). Full schema (no schema refs) — UDP is fire-and-forget so there's no
///     server state to reference.
/// </remarks>
internal sealed class QwpUdpSender : ISender
{
    private readonly Dictionary<string, QwpTableBuffer> _tables = new(StringComparer.Ordinal);
    private readonly QwpWebSocketEncoder _encoder = new();
    private readonly Socket _socket;
    private readonly EndPoint _remoteEndPoint;

    private QwpTableBuffer? _currentTable;
    private string? _currentTableName;
    private bool _disposed;
    private DateTime _lastFlush = DateTime.MinValue;

    public QwpUdpSender(SenderOptions options)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));

        var addr = options.addr ?? throw new IngressError(ErrorCode.InvalidApiCall,
            "udp:: requires an addr= configuration entry");
        var (host, port) = ParseAddress(addr, defaultPort: 9007);
        _remoteEndPoint = new IPEndPoint(ResolveAddress(host), port);

        _socket = new Socket(_remoteEndPoint.AddressFamily, SocketType.Dgram, System.Net.Sockets.ProtocolType.Udp);
        if (options.multicast_ttl > 0)
        {
            // Multicast TTL applies even for unicast endpoints; ignored unless the IP
            // is actually a multicast address. Wraps in try because some environments
            // block setsockopt on raw sockets.
            try { _socket.Ttl = (short)options.multicast_ttl; }
            catch (SocketException) { /* best-effort */ }
        }
    }

    public SenderOptions Options { get; }

    public int MaxDatagramSize => Options.max_datagram_size;

    public int MulticastTtl => Options.multicast_ttl;

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
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpUdpSender does not support transactions");

    public void Rollback() =>
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpUdpSender does not support transactions");

    public Task CommitAsync(CancellationToken ct = default) =>
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpUdpSender does not support transactions");

    public void Commit(CancellationToken ct = default) =>
        throw new IngressError(ErrorCode.InvalidApiCall, "QwpUdpSender does not support transactions");

    public ISender Table(ReadOnlySpan<char> name)
    {
        ThrowIfDisposed();
        var nameStr = name.ToString();
        _currentTableName = nameStr;
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
        ColumnNanos(name, ((value.Ticks - 621355968000000000L) * 100));

    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value) =>
        Column(name, value.UtcDateTime);

    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_TIMESTAMP_NANOS, useNullBitmap: false);
        col?.AddLong(timestampNanos);
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

    public ISender Column(ReadOnlySpan<char> name, char value)
    {
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_CHAR, useNullBitmap: false);
        col?.AddShort((short)value);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Guid value)
    {
        // QWP UUID is wire (lo, hi). System.Guid.ToByteArray is mixed endian; use the
        // existing IBuffer convention via raw long pair.
        Span<byte> bytes = stackalloc byte[16];
        if (!value.TryWriteBytes(bytes))
            throw new InvalidOperationException("Guid.TryWriteBytes failed");
        // Reinterpret as two little-endian longs to match the column's storage layout.
        var lo = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(0, 8));
        var hi = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8, 8));
        var col = RequireTable().GetOrCreateColumn(name.ToString(), QwpConstants.TYPE_UUID, useNullBitmap: false);
        if (col is not null)
        {
            // ColumnBuffer stores UUID as 16 LE bytes via two PutLong calls; AddLong twice.
            col.AddLong(lo);
            col.AddLong(hi);
        }
        return this;
    }

    public void At(DateTime value, CancellationToken ct = default) =>
        AtNanos((value.Ticks - 621355968000000000L) * 100, ct);

    public void At(DateTimeOffset value, CancellationToken ct = default) => At(value.UtcDateTime, ct);

    public void At(long value, CancellationToken ct = default)
    {
        // Java's QwpUdpSender treats At(long) as microseconds. Convert to nanos.
        AtNanos(value * 1_000L, ct);
    }

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
    }

    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default)
    {
        AtNanos(timestampNanos, ct);
        return ValueTask.CompletedTask;
    }

    public void AtNow(CancellationToken ct = default) => AtNanos(DateTime.UtcNow.Ticks * 100, ct);

    public ValueTask AtNowAsync(CancellationToken ct = default)
    {
        AtNow(ct);
        return ValueTask.CompletedTask;
    }

    public void Send(CancellationToken ct = default) => SendAsync(ct).GetAwaiter().GetResult();

    public async Task SendAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        foreach (var table in _tables.Values)
        {
            if (table.RowCount == 0) continue;
            var size = _encoder.Encode(table, useSchemaRef: false);
            var datagram = _encoder.AsReadOnlyMemory();
            if (Options.max_datagram_size > 0 && size > Options.max_datagram_size)
            {
                throw new IngressError(ErrorCode.ServerFlushError,
                    $"encoded datagram {size} bytes exceeds max_datagram_size={Options.max_datagram_size}; fragmentation lands in PR 6c");
            }
            await _socket.SendToAsync(datagram, SocketFlags.None, _remoteEndPoint, ct).ConfigureAwait(false);
            table.Reset();
        }
        _lastFlush = DateTime.UtcNow;
    }

    public void Truncate()
    {
        // Datagram-based, no oversize buffer to shrink. No-op.
    }

    public void CancelRow()
    {
        _currentTable?.CancelCurrentRow();
    }

    public void Clear()
    {
        foreach (var table in _tables.Values) table.Reset();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _socket.Dispose();
    }

    private QwpTableBuffer RequireTable() => _currentTable ??
        throw new IngressError(ErrorCode.InvalidApiCall, "Table(name) must be called before adding columns");

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(QwpUdpSender));
    }

    private static (string Host, int Port) ParseAddress(string addr, int defaultPort)
    {
        var idx = addr.LastIndexOf(':');
        if (idx < 0) return (addr, defaultPort);
        var host = addr.Substring(0, idx);
        return int.TryParse(addr.Substring(idx + 1), out var port)
            ? (host, port)
            : (addr, defaultPort);
    }

    private static IPAddress ResolveAddress(string host)
    {
        if (IPAddress.TryParse(host, out var ip)) return ip;
        var entry = Dns.GetHostAddresses(host);
        return entry.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork)
               ?? entry.First();
    }
}
