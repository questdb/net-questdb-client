/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Senders;

/// <summary>
///     Interface representing <see cref="Sender" /> implementations. For <c>ws::</c> / <c>wss::</c>
///     senders prefer <c>await using var</c> so the close-time ACK drain doesn't block the caller.
/// </summary>
public interface ISender : IDisposable, IAsyncDisposable
{
    ValueTask IAsyncDisposable.DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Approximate buffer size in bytes. For HTTP/TCP (ILP text) this is the exact UTF-8
    ///     byte count of the pending payload. For WS/WSS (QWP columnar) this is an estimated
    ///     footprint of the per-column buffers — close to but not identical to the wire size,
    ///     because schema/symbol-dictionary deltas are added at flush time.
    /// </summary>
    public int Length { get; }

    /// <summary>
    ///     Represents the number of rows currently stored in the buffer.
    /// </summary>
    public int RowCount { get; }

    /// <summary>
    ///     Represents whether the Sender is in a transactional state.
    /// </summary>
    public bool WithinTransaction { get; }

    /// <summary>
    ///     Records the last time the sender was flushed.
    /// </summary>
    public DateTime LastFlush { get; }

    /// <inheritdoc cref="SenderOptions" />
    public SenderOptions Options { get; }

    /// <summary>
    ///     Starts a new transaction.
    /// </summary>
    /// <remarks>
    ///     This function starts a transaction. Within a transaction, only one table can be specified, which
    ///     applies to all ILP rows in the batch. The batch will not be sent until explicitly committed.
    /// </remarks>
    /// <param name="tableName">The name of the table for all the rows in this transaction.</param>
    /// <returns>Itself</returns>
    /// <exception cref="IngressError">When transactions are unsupported, or an invalid name is provided.</exception>
    public ISender Transaction(ReadOnlySpan<char> tableName);

    /// <summary>
    ///     Clears the transaction.
    /// </summary>
    /// <exception cref="IngressError">When transactions are unsupported.</exception>
    public void Rollback();

    /// <summary>
    ///     Commits the current transaction, sending the buffer contents to the database.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="IngressError">Thrown by <see cref="SendAsync" />, or when transactions are unsupported.</exception>
    public Task CommitAsync(CancellationToken ct = default);

    /// <inheritdoc cref="CommitAsync" />
    public void Commit(CancellationToken ct = default);

    /// <summary>
    ///     Sends data to the QuestDB server.
    /// </summary>
    /// <remarks>
    ///     Only usable outside of a transaction. If there are no pending rows, then this is a no-op.
    /// </remarks>
    /// <exception cref="IngressError">When the request fails.</exception>
    public Task SendAsync(CancellationToken ct = default);

    /// <inheritdoc cref="SendAsync" />
    public void Send(CancellationToken ct = default);

    /// <summary>
    ///     Set table (measurement) name for the next row.
    ///     Each row may have a different table name within a batch.
    ///     Cannot be used within a transaction.
    /// </summary>
    /// <param name="name">The table name</param>
    /// <returns>Itself</returns>
    public ISender Table(ReadOnlySpan<char> name);

    /// <summary>
    ///     Adds a symbol (tag) to the current row.
    /// </summary>
    /// <param name="name">The name of the symbol column</param>
    /// <param name="value">The value for the symbol column</param>
    /// <returns>Itself</returns>
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value);

    /// <summary>
    ///     Adds a column (field) to the current row.
    /// </summary>
    /// <param name="name">The name of the column</param>
    /// <param name="value">The value for the column</param>
    /// <summary>
    ///     Adds a column (field) with the specified string value to the current row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The column value as a character span.</param>
    /// <returns>The sender instance for fluent chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value);

    /// <summary>
    ///     Adds a column with the specified name and 64-bit integer value to the current row.
    /// </summary>
    /// <param name="name">The column (field) name.</param>
    /// <param name="value">The 64-bit integer value for the column.</param>
    /// <returns>The current sender instance for method chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, long value);

    /// <inheritdoc cref="Column(ReadOnlySpan{char},ReadOnlySpan{char})" />
    public ISender Column(ReadOnlySpan<char> name, int value);

    /// <summary>
    ///     Adds a boolean field column with the specified name and value to the current row.
    /// </summary>
    /// <param name="name">The column (field) name.</param>
    /// <param name="value">The boolean value to store in the column.</param>
    /// <returns>The same <see cref="ISender" /> instance to allow fluent chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, bool value);

    /// <summary>
    ///     Adds a double-precision field column to the current row.
    /// </summary>
    /// <param name="name">The column (field) name.</param>
    /// <param name="value">The column's double-precision value.</param>
    /// <returns>The same <see cref="ISender" /> instance for fluent chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, double value);

    /// <summary>
    ///     Adds a column (field) with the specified DateTime value to the current row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The DateTime value to add.</param>
    /// <returns>The same <see cref="ISender" /> instance for fluent chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, DateTime value);

    /// <summary>
    ///     Adds a column with the specified name and DateTimeOffset value to the current row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The DateTimeOffset value to store for the column (used as a timestamp value).</param>
    /// <returns>The sender instance for fluent chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value);

    /// <summary>
    ///     Adds a timestamp column with exact nanosecond precision.
    /// </summary>
    /// <param name="name">The name of the column</param>
    /// <param name="timestampNanos">Nanoseconds since Unix epoch</param>
    /// <returns>Itself</returns>
    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos);

    /// <summary>
    ///     Adds a value for the designated timestamp column.
    /// </summary>
    /// <param name="value">A timestamp</param>
    /// <param name="ct">A cancellation token applied requests caused by auto-flushing</param>
    /// <returns></returns>
    public ValueTask AtAsync(DateTime value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync(DateTime, CancellationToken)" />
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync(DateTime, CancellationToken)" />
    public ValueTask AtAsync(long value, CancellationToken ct = default);

    /// <summary>
    ///     Allow the server to set a designated timestamp value.
    /// </summary>
    /// <param name="ct">A cancellation token applied requests caused by auto-flushing</param>
    /// <returns></returns>
    [Obsolete("Not compatible with deduplication. Please use `AtAsync(DateTime.UtcNow)` instead.")]
    public ValueTask AtNowAsync(CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync(DateTime, CancellationToken)" />
    public void At(DateTime value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync(DateTime, CancellationToken)" />
    public void At(DateTimeOffset value, CancellationToken ct = default);

    /// <inheritdoc cref="AtAsync(DateTime, CancellationToken)" />
    public void At(long value, CancellationToken ct = default);

    /// <summary>
    ///     Adds exact nanosecond precision timestamp for the designated timestamp column.
    /// </summary>
    /// <param name="timestampNanos">Nanoseconds since Unix epoch</param>
    /// <param name="ct">A cancellation token applied requests caused by auto-flushing</param>
    /// <returns>Itself</returns>
    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default);

    /// <inheritdoc cref="AtNanosAsync" />
    public void AtNanos(long timestampNanos, CancellationToken ct = default);

    /// <inheritdoc cref="AtNowAsync" />
    [Obsolete("Not compatible with deduplication. Please use `At(DateTime.UtcNow)` instead.")]
    public void AtNow(CancellationToken ct = default);

    /// <summary>
    ///     Removes unused extra buffer space.
    /// </summary>
    public void Truncate();

    /// <summary>
    ///     Cancels the current, partially formed ILP row.
    /// </summary>
    public void CancelRow();

    /// <summary>
    ///     Clears the sender's internal buffer and resets buffer-related state, removing all pending rows.
    /// </summary>
    public void Clear();

    /// <summary>
    ///     Adds a column to the current row using a sequence of value-type elements and an explicit multidimensional shape.
    /// </summary>
    /// <typeparam name="T">The element value type stored in the column.</typeparam>
    /// <param name="name">The column name.</param>
    /// <param name="value">A sequence of elements that form the column's data.</param>
    /// <param name="shape">
    ///     A sequence of integers describing the dimensions of the array representation; dimension lengths
    ///     must match the number of elements in <paramref name="value" /> when multiplied together.
    /// </param>
    /// <returns>The same <see cref="ISender" /> instance for fluent chaining.</returns>
    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct;

    /// <summary>
    ///     Adds a column whose value is provided as a native array; multidimensional (non-jagged) arrays are supported.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">
    ///     A native array containing the column data. Multidimensional arrays are treated as shaped data (do
    ///     not pass jagged arrays).
    /// </param>
    /// <returns>The sender instance for fluent chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, Array value);

    /// <summary>
    ///     Adds a column with the specified name and a sequence of value-type elements from a span to the current row.
    /// </summary>
    /// <param name="name">The column (field) name.</param>
    /// <param name="value">A contiguous sequence of value-type elements representing the column data.</param>
    /// <returns>The same <see cref="ISender" /> instance to allow fluent chaining.</returns>
    public ISender Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct;

    /// <summary>
    ///     Adds a column whose value is a sequence of value-type elements with the given multidimensional shape when both
    ///     <paramref name="value" /> and <paramref name="shape" /> are provided; no action is taken if either is null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The sequence of elements for the column, or null to skip adding the column.</param>
    /// <param name="shape">The dimensions describing the array shape, or null to skip adding the column.</param>
    /// <returns>This sender instance for fluent chaining.</returns>
    public ISender NullableColumn<T>(ReadOnlySpan<char> name, IEnumerable<T>? value, IEnumerable<int>? shape)
        where T : struct
    {
        if (value != null && shape != null)
        {
            Column(name, value, shape);
        }

        return this;
    }

    /// <summary>
    ///     Adds a column using a native array value when the provided array is non-null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">
    ///     The array to use as the column value; if null, no column is added. Multidimensional arrays are
    ///     supported (non-jagged).
    /// </param>
    /// <returns>The same <see cref="ISender" /> instance for fluent chaining.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, Array? value)
    {
        if (value != null)
        {
            Column(name, value);
        }

        return this;
    }

    /// <summary>
    ///     Adds a string column with the given name when the provided value is not null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The string value to add; if null, no column is added.</param>
    /// <returns>The current sender instance for fluent chaining.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, string? value)
    {
        if (value != null)
        {
            Column(name, value);
        }

        return this;
    }

    /// <summary>
    ///     Adds a long column with the specified name when the provided nullable value has a value; does nothing when the
    ///     value is null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The nullable long value to add as a column; if null the sender is unchanged.</param>
    /// <returns>The current sender instance for fluent chaining.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, long? value)
    {
        if (value != null)
        {
            return Column(name, value ?? throw new InvalidOperationException());
        }

        return this;
    }

    /// <summary>
    ///     Adds a boolean column with the given name when a value is provided; does nothing if the value is null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The nullable boolean value to add as a column.</param>
    /// <returns>The current sender instance to allow fluent chaining.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, bool? value)
    {
        if (value != null)
        {
            return Column(name, value ?? throw new InvalidOperationException());
        }

        return this;
    }

    /// <summary>
    ///     Adds a column with the given double value when the value is non-null; otherwise no column is added and the sender
    ///     is unchanged.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The column value; if non-null, the value is written as a double field.</param>
    /// <returns>The sender instance after the operation.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, double? value)
    {
        if (value != null)
        {
            return Column(name, value ?? throw new InvalidOperationException());
        }

        return this;
    }

    /// <summary>
    ///     Adds a DateTime column with the specified name when a value is provided; no action is taken if the value is null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The nullable DateTime value to add as a column.</param>
    /// <returns>
    ///     The current <see cref="ISender" /> instance for fluent chaining; unchanged if <paramref name="value" /> is
    ///     null.
    /// </returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, DateTime? value)
    {
        if (value != null)
        {
            return Column(name, value ?? throw new InvalidOperationException());
        }

        return this;
    }

    /// <summary>
    ///     Adds a column with the given name and DateTimeOffset value when a value is provided; does nothing if the value is
    ///     null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The DateTimeOffset value to add; if null the column is not added.</param>
    /// <returns>The same <see cref="ISender" /> instance to allow fluent chaining.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, DateTimeOffset? value)
    {
        if (value != null)
        {
            return Column(name, value ?? throw new InvalidOperationException());
        }

        return this;
    }

    /// <summary>
    ///     Adds a decimal column in binary format to the current row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The decimal value to add; if null is required, use the `NullableColumn` variant.</param>
    /// <returns>The sender instance for fluent call chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, decimal value);


    /// <summary>
    ///     Writes a DECIMAL column with the specified name using the ILP binary decimal layout.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The decimal value to write, or `null` to write a NULL column.</param>
    /// <returns>The buffer instance for method chaining.</returns>
    /// <remarks>
    ///     This method requires protocol version 3 or later. It will throw an <see cref="IngressError" /> with
    ///     <see cref="ErrorCode.ProtocolVersionError" /> if used with protocol version 1 or 2.
    /// </remarks>
    public ISender NullableColumn(ReadOnlySpan<char> name, decimal? value)
    {
        if (value != null)
        {
            return Column(name, value ?? throw new InvalidOperationException());
        }

        return this;
    }

    /// <summary>
    ///     Adds a DECIMAL64 column, coercing <paramref name="value" /> to <paramref name="scale" />
    ///     (rounded half away from zero). On <c>ws::</c> / <c>wss::</c> (QWP) this writes a fixed
    ///     8-byte mantissa and locks the column scale on the first non-null write. On HTTP/TCP (ILP)
    ///     it writes the variable-length binary decimal and requires protocol version 3.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The value, rounded half away from zero to <paramref name="scale" /> digits.</param>
    /// <param name="scale">Number of fractional digits (0–18).</param>
    /// <returns>The sender instance for fluent call chaining.</returns>
    /// <exception cref="IngressError">If <paramref name="scale" /> is out of range or the value's integer part overflows the type width.</exception>
    public ISender ColumnDecimal64(ReadOnlySpan<char> name, decimal value, byte scale);

    /// <summary>
    ///     Adds a DECIMAL128 column, coercing <paramref name="value" /> to <paramref name="scale" />
    ///     (rounded half away from zero). Same transport behaviour as
    ///     <see cref="ColumnDecimal64(ReadOnlySpan{char},decimal,byte)" /> with a 16-byte QWP mantissa.
    ///     Over ILP, values whose scale exceeds 28 or whose magnitude exceeds <see cref="System.Decimal" />'s
    ///     96-bit mantissa are rejected — use <c>ws::</c> / <c>wss::</c> for the full range.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The value, rounded half away from zero to <paramref name="scale" /> digits.</param>
    /// <param name="scale">Number of fractional digits (0–38).</param>
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, decimal value, byte scale);

    /// <summary>
    ///     Adds a DECIMAL256 column, coercing <paramref name="value" /> to <paramref name="scale" />
    ///     (rounded half away from zero). Same transport behaviour as
    ///     <see cref="ColumnDecimal64(ReadOnlySpan{char},decimal,byte)" /> with a 32-byte QWP mantissa.
    ///     Over ILP, values whose scale exceeds 28 or whose magnitude exceeds <see cref="System.Decimal" />'s
    ///     96-bit mantissa are rejected — use <c>ws::</c> / <c>wss::</c> for the full range.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The value, rounded half away from zero to <paramref name="scale" /> digits.</param>
    /// <param name="scale">Number of fractional digits (0–76).</param>
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, decimal value, byte scale);

    /// <summary>
    ///     Adds a DECIMAL64 column from the raw unscaled int64 mantissa with an explicit
    ///     <paramref name="scale" />. Exposes the full 18-digit range that <see cref="System.Decimal" />
    ///     cannot always represent. Over ILP this is reconstructed as a <see cref="System.Decimal" /> and
    ///     requires protocol version 3.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="unscaledValue">The unscaled integer mantissa; the value is <c>unscaledValue / 10^scale</c>.</param>
    /// <param name="scale">Number of fractional digits (0–18).</param>
    public ISender ColumnDecimal64(ReadOnlySpan<char> name, long unscaledValue, byte scale);

    /// <summary>
    ///     Adds a DECIMAL128 column from the two two's-complement 64-bit limbs of the unscaled integer:
    ///     <paramref name="lo" /> is the low 64 bits (unsigned magnitude), <paramref name="hi" /> the
    ///     signed high 64 bits — i.e. <c>(hi ≪ 64) | (ulong)lo</c>. On <c>ws::</c> / <c>wss::</c> this
    ///     uses the full 38-digit range; over ILP values beyond <see cref="System.Decimal" />'s 96-bit
    ///     mantissa or scale 28 are rejected.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="lo">Low 64 bits of the unscaled integer (unsigned magnitude).</param>
    /// <param name="hi">High 64 bits of the unscaled integer (signed).</param>
    /// <param name="scale">Number of fractional digits (0–38).</param>
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, long lo, long hi, byte scale);

    /// <summary>
    ///     Adds a DECIMAL256 column from the four two's-complement 64-bit limbs of the unscaled integer:
    ///     <c>l0</c>–<c>l2</c> are unsigned magnitude limbs and <c>l3</c> is the signed high limb —
    ///     i.e. <c>(ulong)l0 | (ulong)l1≪64 | (ulong)l2≪128 | l3≪192</c>. On <c>ws::</c> / <c>wss::</c>
    ///     this uses the full 76-digit range; over ILP values beyond <see cref="System.Decimal" />'s
    ///     96-bit mantissa or scale 28 are rejected.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="l0">Bits 0–63 of the unscaled integer (unsigned magnitude limb).</param>
    /// <param name="l1">Bits 64–127 of the unscaled integer (unsigned magnitude limb).</param>
    /// <param name="l2">Bits 128–191 of the unscaled integer (unsigned magnitude limb).</param>
    /// <param name="l3">Bits 192–255 of the unscaled integer (signed high limb).</param>
    /// <param name="scale">Number of fractional digits (0–76).</param>
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, long l0, long l1, long l2, long l3, byte scale);

    /// <summary>
    ///     Adds a GUID column with the specified name and value to the current row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The GUID value to store in the column.</param>
    /// <returns>The sender instance for fluent call chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, Guid value);

    /// <summary>
    ///     Adds a character column with the specified name and value to the current row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The character value to store in the column.</param>
    /// <returns>The sender instance for fluent call chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, char value);

    /// <summary>
    ///     Adds a nullable GUID column with the specified name when a value is provided; does nothing if the value is null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The nullable GUID value to add as a column; if null, the column is not added.</param>
    /// <returns>The sender instance for fluent call chaining.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, Guid? value)
    {
        if (value != null)
        {
            return Column(name, value.Value);
        }

        return this;
    }

    /// <summary>
    ///     Adds a nullable character column with the specified name when a value is provided; does nothing if the value is
    ///     null.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The nullable character value to add as a column; if null, the column is not added.</param>
    /// <returns>The sender instance for fluent call chaining.</returns>
    public ISender NullableColumn(ReadOnlySpan<char> name, char? value)
    {
        if (value != null)
        {
            return Column(name, value.Value);
        }

        return this;
    }
}