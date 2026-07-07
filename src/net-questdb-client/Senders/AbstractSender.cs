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

using System.Numerics;
using System.Runtime.CompilerServices;
using QuestDB.Buffers;
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Utils;

namespace QuestDB.Senders;

internal abstract class AbstractSender : ISender
{
    protected IBuffer Buffer = null!;
    protected bool CommittingTransaction { get; set; }

    /// <inheritdoc />
    public SenderOptions Options { get; protected init; } = null!;

    public int Length => Buffer.Length;
    public int RowCount => Buffer.RowCount;
    public bool WithinTransaction => Buffer.WithinTransaction;
    public DateTime LastFlush { get; protected set; } = DateTime.MinValue;

    /// <inheritdoc />
    public virtual ISender Transaction(ReadOnlySpan<char> tableName)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, $"`{GetType().Name}` does not support transactions.");
    }

    /// <inheritdoc />
    public virtual void Rollback()
    {
        throw new IngressError(ErrorCode.InvalidApiCall, $"`{GetType().Name}` does not support transactions.");
    }

    /// <inheritdoc />
    public virtual Task CommitAsync(CancellationToken ct = default)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, $"`{GetType().Name}` does not support transactions.");
    }

    /// <inheritdoc />
    public virtual void Commit(CancellationToken ct = default)
    {
        throw new IngressError(ErrorCode.InvalidApiCall, $"`{GetType().Name}` does not support transactions.");
    }

    /// <inheritdoc />
    public ISender Table(ReadOnlySpan<char> name)
    {
        Buffer.Table(name);
        return this;
    }

    /// <inheritdoc />
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        Buffer.Symbol(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        ((BufferV1)Buffer).Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <summary>
    ///     Appends an integer-valued column with the specified name to the current buffered row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The integer value to append for the column.</param>
    /// <returns>The same <see cref="ISender" /> instance to allow fluent chaining.</returns>
    public ISender Column(ReadOnlySpan<char> name, int value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender ColumnNanos(ReadOnlySpan<char> name, long timestampNanos)
    {
        Buffer.ColumnNanos(name, timestampNanos);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct
    {
        Buffer.Column(name, value, shape);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Array? value)
    {
        Buffer.Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, ReadOnlySpan<T> value) where T : struct
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ValueTask AtAsync(DateTime value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.At(value);
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.At(value);
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public ValueTask AtAsync(long value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.At(value);
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public ValueTask AtNowAsync(CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.AtNow();
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public void At(DateTime value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.At(value);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void At(DateTimeOffset value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.At(value);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void At(long value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.At(value);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public ValueTask AtNanosAsync(long timestampNanos, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.AtNanos(timestampNanos);
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public void AtNanos(long timestampNanos, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.AtNanos(timestampNanos);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void AtNow(CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        Buffer.AtNow();
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void Truncate()
    {
        Buffer.TrimExcessBuffers();
    }

    /// <inheritdoc />
    public void CancelRow()
    {
        Buffer.CancelRow();
    }

    /// <inheritdoc />
    public void Clear()
    {
        Buffer.Clear();
    }

    /// <inheritdoc />
    public abstract void Dispose();

    /// <inheritdoc />
    public abstract Task SendAsync(CancellationToken ct = default);

    /// <inheritdoc />
    public abstract void Send(CancellationToken ct = default);

    /// <summary>
    ///     Adds a nullable decimal column value to the current row in the buffer.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">The decimal value to write, or <c>null</c> to emit a null for the column.</param>
    /// <returns>The same <see cref="ISender" /> instance for fluent chaining.</returns>
    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, decimal value)
    {
        Buffer.Column(name, value);
        return this;
    }

    // QWP carries an explicit per-column decimal width + scale; ILP has a single variable-length
    // binary decimal. These overloads honour the same (value, scale) contract over ILP by rounding to
    // `scale` (half away from zero, as the QWP path and mainstream SQL do) and emitting the V3 binary
    // decimal. The width name (64/128/256) only bounds the accepted scale here — the ILP wire form is
    // width-agnostic. The raw-limb overloads route through System.Decimal and therefore reject values
    // whose magnitude exceeds its 96-bit mantissa or whose scale exceeds 28; use ws/wss for the full
    // DECIMAL128 / DECIMAL256 range.

    /// <inheritdoc />
    public ISender ColumnDecimal64(ReadOnlySpan<char> name, decimal value, byte scale)
        => AppendIlpDecimalScaled(name, value, scale, QwpConstants.MaxDecimal64Scale, "Decimal64");

    /// <inheritdoc />
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, decimal value, byte scale)
        => AppendIlpDecimalScaled(name, value, scale, QwpConstants.MaxDecimal128Scale, "Decimal128");

    /// <inheritdoc />
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, decimal value, byte scale)
        => AppendIlpDecimalScaled(name, value, scale, QwpConstants.MaxDecimal256Scale, "Decimal256");

    /// <inheritdoc />
    public ISender ColumnDecimal128(ReadOnlySpan<char> name, long lo, long hi, byte scale)
    {
        ValidateDecimalScale(scale, QwpConstants.MaxDecimal128Scale, "Decimal128");
        var mantissa = (new BigInteger(hi) << 64) + new BigInteger((ulong)lo);
        return AppendIlpDecimalFromMantissa(name, mantissa, scale);
    }

    /// <inheritdoc />
    public ISender ColumnDecimal256(ReadOnlySpan<char> name, long l0, long l1, long l2, long l3, byte scale)
    {
        ValidateDecimalScale(scale, QwpConstants.MaxDecimal256Scale, "Decimal256");
        var mantissa = (new BigInteger(l3) << 192)
            + (new BigInteger((ulong)l2) << 128)
            + (new BigInteger((ulong)l1) << 64)
            + new BigInteger((ulong)l0);
        return AppendIlpDecimalFromMantissa(name, mantissa, scale);
    }

    private ISender AppendIlpDecimalScaled(ReadOnlySpan<char> name, decimal value, byte scale, int maxScale,
        string typeName)
    {
        ValidateDecimalScale(scale, maxScale, typeName);
        // Round to the requested scale, half away from zero, matching the QWP path. Math.Round caps the
        // digit count at 28; a System.Decimal can't carry more than 28 fractional digits, so a target
        // scale above 28 needs no rounding and the value is written as-is — the server rescales to the
        // column's declared scale.
        var rounded = Math.Round(value, Math.Min((int)scale, MaxDecimalScale), MidpointRounding.AwayFromZero);
        Buffer.Column(name, rounded);
        return this;
    }

    // Routes a raw two's-complement unscaled mantissa + scale through the ILP binary decimal encoder by
    // reconstructing a System.Decimal. Rejects values ILP cannot represent: a scale beyond
    // System.Decimal's 28-digit limit, or a magnitude beyond its 96-bit mantissa. Both are representable
    // over ws/wss, so the error points the caller there.
    private ISender AppendIlpDecimalFromMantissa(ReadOnlySpan<char> name, BigInteger mantissa, byte scale)
    {
        if (scale > MaxDecimalScale)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"decimal scale {scale} exceeds the {MaxDecimalScale} digits representable over ILP; use a ws/wss sender");
        }

        var negative = mantissa.Sign < 0;
        var magnitude = BigInteger.Abs(mantissa);
        if (magnitude > Max96BitMagnitude)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                "decimal magnitude exceeds the 96-bit range representable over ILP; use a ws/wss sender");
        }

        var lo = (uint)(magnitude & uint.MaxValue);
        var mid = (uint)((magnitude >> 32) & uint.MaxValue);
        var hi = (uint)((magnitude >> 64) & uint.MaxValue);
        Buffer.Column(name, new decimal((int)lo, (int)mid, (int)hi, negative, scale));
        return this;
    }

    private static void ValidateDecimalScale(byte scale, int maxScale, string typeName)
    {
        if (scale > maxScale)
        {
            throw new IngressError(ErrorCode.InvalidApiCall,
                $"{typeName} scale {scale} exceeds maximum {maxScale}");
        }
    }

    // Largest scale System.Decimal can carry, and its largest unsigned 96-bit mantissa (2^96 - 1).
    private const int MaxDecimalScale = 28;
    private static readonly BigInteger Max96BitMagnitude = (BigInteger.One << 96) - BigInteger.One;

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, Guid value)
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, char value)
    {
        Buffer.Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, T[] value) where T : struct
    {
        Buffer.Column(name, value);
        return this;
    }

    /// <summary>
    ///     Handles auto-flushing logic.
    /// </summary>
    /// <remarks>
    ///     Auto-flushing is a feature which triggers the submission of data to the database
    ///     based upon certain thresholds.
    ///     <para />
    ///     <see cref="SenderOptions.auto_flush_rows" /> - the number of buffered ILP rows.
    ///     <para />
    ///     <see cref="SenderOptions.auto_flush_bytes" /> - the current length of the buffer in UTF-8 bytes.
    ///     <para />
    ///     <see cref="SenderOptions.auto_flush_interval" /> - the elapsed time interval since the last flush.
    ///     <para />
    ///     These functionalities can be disabled entirely by setting <see cref="SenderOptions.auto_flush" />
    ///     to <see cref="AutoFlushType.off" />, or individually by setting their values to `-1`.
    /// </remarks>
    /// <param name="ct">A user-provided cancellation token.</param>
    private ValueTask FlushIfNecessaryAsync(CancellationToken ct = default)
    {
        if (Options.auto_flush == AutoFlushType.on && !WithinTransaction &&
            ((Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows)
             || (Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes)
             || (Options.auto_flush_interval > TimeSpan.Zero &&
                 DateTime.UtcNow - LastFlush >= Options.auto_flush_interval)))
        {
            return new ValueTask(SendAsync(ct));
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Synchronously checks auto-flush conditions and sends the buffer if thresholds are met.
    /// </summary>
    /// <param name="ct">A user-provided cancellation token.</param>
    /// <remarks>
    ///     Auto-flushing is triggered based on:
    ///     <list type="bullet">
    ///         <item><see cref="SenderOptions.auto_flush_rows" /> - the number of buffered ILP rows.</item>
    ///         <item><see cref="SenderOptions.auto_flush_bytes" /> - the current length of the buffer in UTF-8 bytes.</item>
    ///         <item><see cref="SenderOptions.auto_flush_interval" /> - the elapsed time interval since the last flush.</item>
    ///     </list>
    ///     Has no effect within a transaction or if <see cref="SenderOptions.auto_flush" /> is set to
    ///     <see cref="AutoFlushType.off" />.
    /// </remarks>
    private void FlushIfNecessary(CancellationToken ct = default)
    {
        if (Options.auto_flush == AutoFlushType.on && !WithinTransaction &&
            ((Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows)
             || (Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes)
             || (Options.auto_flush_interval > TimeSpan.Zero &&
                 DateTime.UtcNow - LastFlush >= Options.auto_flush_interval)))
        {
            Send(ct);
        }
    }

    /// <summary>
    ///     Sets <see cref="LastFlush" /> to the current UTC time if it has not been initialized.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardLastFlushNotSet()
    {
        if (LastFlush == DateTime.MinValue)
        {
            LastFlush = DateTime.UtcNow;
        }
    }
}