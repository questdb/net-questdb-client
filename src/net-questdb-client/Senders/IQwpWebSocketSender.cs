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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

namespace QuestDB.Senders;

/// <summary>
///     Extends <see cref="ISender" /> with WebSocket-only operations.
/// </summary>
/// <remarks>
///     <see cref="Sender.New(string)" /> returns an <see cref="ISender" /> for every transport. Users that
///     opted into <c>ws::</c> or <c>wss::</c> can cast to this interface to access ping and per-table
///     <c>seqTxn</c> watermarks. Durable watermarks require <c>request_durable_ack=on</c> on the
///     connect string; the committed watermark is always populated once the server has ACKed any
///     batch on a connection.
///     <para />
///     <b>Authentication errors are terminal</b> on both the initial connect and SF reconnect
///     loops: a 401/403 against any failover candidate aborts the loop instead of cycling through
///     the remaining hosts. Retrying a rejected credential floods server logs and rarely recovers.
/// </remarks>
public interface IQwpWebSocketSender : ISender
{
    /// <summary>
    ///     Highest committed <c>seqTxn</c> the server has acknowledged for the given table on this
    ///     connection. Returns <c>-1</c> when the server has not yet sent a watermark for the table.
    /// </summary>
    /// <remarks>
    ///     Per-table watermarks arrive only when <c>request_durable_ack</c> is enabled. Without
    ///     opt-in the value stays at <c>-1</c>.
    /// </remarks>
    long GetHighestAckedSeqTxn(string tableName);

    /// <summary>
    ///     Highest <c>seqTxn</c> the server has reported as durably uploaded (object-store watermark).
    ///     Returns <c>-1</c> when no durable watermark has been observed.
    /// </summary>
    long GetHighestDurableSeqTxn(string tableName);

    /// <summary>
    ///     Drains the in-flight ACK window. After it returns successfully every batch sent so far has
    ///     been acknowledged by the server and per-table seqTxn watermarks reflect that. Bounded by
    ///     <c>ping_timeout</c>; on an idle connection with nothing in flight it returns immediately
    ///     and is NOT a wire-level liveness probe (ClientWebSocket exposes no PING API).
    /// </summary>
    void Ping(CancellationToken ct = default);

    /// <inheritdoc cref="Ping" />
    ValueTask PingAsync(CancellationToken ct = default);

    /// <summary>
    ///     Highest cursor frame sequence number (FSN) the server has acknowledged on the current
    ///     connection. Returns <c>-1</c> when nothing has been ack'd yet. Snapshot accessor; pair
    ///     with <see cref="AwaitAckedFsnAsync" /> for a bounded wait.
    /// </summary>
    long AckedFsn { get; }

    /// <summary>
    ///     Same as <see cref="ISender.SendAsync" /> but returns the FSN of the highest frame
    ///     published into the cursor engine by this call. Pair with <see cref="AwaitAckedFsnAsync" />
    ///     to confirm a specific publish has been acknowledged. Returns the current
    ///     <see cref="AckedFsn" />-equivalent watermark when nothing was published.
    /// </summary>
    Task<long> FlushAndGetSequenceAsync(CancellationToken ct = default);

    /// <summary>
    ///     Blocks until <see cref="AckedFsn" /> reaches <paramref name="targetFsn" /> or
    ///     <paramref name="timeout" /> elapses. Returns <c>true</c> if the watermark caught up,
    ///     <c>false</c> on timeout. A non-positive <paramref name="targetFsn" /> returns <c>true</c>
    ///     immediately.
    /// </summary>
    /// <exception cref="QuestDB.Utils.IngressError">if the I/O loop has latched a terminal failure.</exception>
    Task<bool> AwaitAckedFsnAsync(long targetFsn, TimeSpan timeout, CancellationToken ct = default);

    /// <summary>Append a DECIMAL64 value to the named column (8-byte mantissa). First call locks the scale.</summary>
    IQwpWebSocketSender ColumnDecimal64(ReadOnlySpan<char> name, decimal value);

    /// <summary>
    ///     Append a DECIMAL64 value as the unscaled int64 mantissa with explicit scale (0–18). Locks
    ///     the column scale on first call. Use this overload to access the full 18-digit range that
    ///     <see cref="System.Decimal" /> cannot always represent.
    /// </summary>
    IQwpWebSocketSender ColumnDecimal64(ReadOnlySpan<char> name, long unscaledValue, byte scale);

    /// <summary>Append a DECIMAL256 value to the named column (32-byte mantissa). First call locks the scale.</summary>
    IQwpWebSocketSender ColumnDecimal256(ReadOnlySpan<char> name, decimal value);

    /// <summary>
    ///     Append a DECIMAL128 value as the two two's-complement 64-bit limbs of the unscaled
    ///     integer: <paramref name="lo" /> is the low 64 bits (unsigned magnitude), <paramref name="hi" />
    ///     is the signed high 64 bits — i.e. the value is <c>(hi ≪ 64) | (ulong)lo</c>. Explicit
    ///     scale (0–38); locks the column scale on first call. Use this overload for the full
    ///     38-digit range beyond <see cref="System.Decimal" />'s ~28-digit limit.
    /// </summary>
    IQwpWebSocketSender ColumnDecimal128(ReadOnlySpan<char> name, long lo, long hi, byte scale);

    /// <summary>
    ///     Append a DECIMAL256 value as the four two's-complement 64-bit limbs of the unscaled
    ///     integer: <c>l0</c>–<c>l2</c> are unsigned magnitude limbs and <c>l3</c> is the signed
    ///     high limb — the value is <c>(ulong)l0 | (ulong)l1≪64 | (ulong)l2≪128 | l3≪192</c>.
    ///     Explicit scale (0–76); locks the column scale on first call. The
    ///     <see cref="System.Decimal" /> overload is capped at ~28 digits; this overload exposes
    ///     the full 76-digit DECIMAL256 range.
    /// </summary>
    IQwpWebSocketSender ColumnDecimal256(ReadOnlySpan<char> name, long l0, long l1, long l2, long l3, byte scale);

    /// <summary>Append a BINARY value to the named column (opaque bytes; no UTF-8 contract).</summary>
    IQwpWebSocketSender ColumnBinary(ReadOnlySpan<char> name, ReadOnlySpan<byte> value);

    /// <summary>Append an IPv4 address to the named column.</summary>
    IQwpWebSocketSender ColumnIPv4(ReadOnlySpan<char> name, System.Net.IPAddress addr);

    /// <summary>Append a BYTE value (signed 8-bit integer) to the named column.</summary>
    IQwpWebSocketSender ColumnByte(ReadOnlySpan<char> name, sbyte value);

    /// <summary>Append a SHORT value (signed 16-bit integer) to the named column.</summary>
    IQwpWebSocketSender ColumnShort(ReadOnlySpan<char> name, short value);

    /// <summary>Append a FLOAT value (32-bit IEEE-754) to the named column.</summary>
    IQwpWebSocketSender ColumnFloat(ReadOnlySpan<char> name, float value);

    /// <summary>Append a DATE value as milliseconds since the Unix epoch to the named column.</summary>
    IQwpWebSocketSender ColumnDate(ReadOnlySpan<char> name, long millisSinceEpoch);

    /// <summary>
    ///     Append a GEOHASH value to the named column. <paramref name="precisionBits" /> (1–60) is
    ///     locked on the first non-null write; <paramref name="hash" /> carries that many low bits.
    /// </summary>
    IQwpWebSocketSender ColumnGeohash(ReadOnlySpan<char> name, ulong hash, int precisionBits);

    /// <summary>Append a LONG256 value (256-bit unsigned integer) to the named column.</summary>
    IQwpWebSocketSender ColumnLong256(ReadOnlySpan<char> name, System.Numerics.BigInteger value);

    /// <summary>
    ///     Number of <see cref="QuestDB.Utils.SenderError" /> notifications dropped because the
    ///     async error inbox was full. Non-zero indicates the user-supplied error_handler can't
    ///     keep up with the error rate. SF mode only; <c>0</c> otherwise.
    /// </summary>
    long DroppedErrorNotifications { get; }

    /// <summary>
    ///     Number of <see cref="SenderConnectionEvent" /> notifications dropped because the
    ///     listener inbox was full. Non-zero indicates the registered
    ///     <see cref="ISenderConnectionListener" /> can't keep up. <c>0</c> when no listener is
    ///     registered.
    /// </summary>
    long DroppedConnectionNotifications { get; }

    /// <summary>
    ///     Total <see cref="QuestDB.Utils.SenderError" /> notifications delivered to the
    ///     user-supplied (or default) error_handler. SF mode only; <c>0</c> otherwise.
    /// </summary>
    long TotalErrorNotificationsDelivered { get; }

    /// <summary>Total frames sent.</summary>
    long TotalFramesSent { get; }

    /// <summary>Total OK ack frames received. Excludes durable-ack and error frames.</summary>
    long TotalAcks { get; }

    /// <summary>Total server error frames received.</summary>
    long TotalServerErrors { get; }

    /// <summary>Total connect attempts (succeeded plus failed).</summary>
    long TotalReconnectAttempts { get; }

    /// <summary>Total successful connects.</summary>
    long TotalReconnectsSucceeded { get; }
}
