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

using System.Runtime.CompilerServices;
using QuestDB.Buffers;
using QuestDB.Enums;
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

    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct
    {
        Buffer.Column(name, value, shape);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Array value)
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

    /// <inheritdoc cref="FlushIfNecessaryAsync" />
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardLastFlushNotSet()
    {
        if (LastFlush == DateTime.MinValue)
        {
            LastFlush = DateTime.UtcNow;
        }
    }
}