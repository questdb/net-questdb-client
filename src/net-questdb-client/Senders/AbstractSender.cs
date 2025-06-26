using System.Runtime.CompilerServices;
using QuestDB.Enums;
using QuestDB.Utils;
using Buffer = QuestDB.Buffers.Buffer;

namespace QuestDB.Senders;

internal abstract class AbstractSender : ISender
{
    protected Buffer _buffer = null!;
    protected bool CommittingTransaction { get; set; }

    /// <inheritdoc />
    public SenderOptions Options { get; protected init; }

    public int Length => _buffer.Length;
    public int RowCount => _buffer.RowCount;
    public bool WithinTransaction => _buffer.WithinTransaction;
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
        _buffer.Table(name);
        return this;
    }

    /// <inheritdoc />
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Symbol(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, long value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, bool value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, double value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTime value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value)
    {
        _buffer.Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, T value) where T : IEnumerable<double>
    {
        _buffer.Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, T[] value) where T : struct
    {
        _buffer.Column(name, value);
        return this;
    }

    public ISender Column<T>(ReadOnlySpan<char> name, IEnumerable<T> value, IEnumerable<int> shape) where T : struct
    {
        _buffer.Column(name, value, shape);
        return this;
    }

    public ISender Column(ReadOnlySpan<char> name, Array value)
    {
        _buffer.Column(name, value);
        return this;
    }

    /// <inheritdoc />
    public ValueTask AtAsync(DateTime value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.At(value);
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public ValueTask AtAsync(DateTimeOffset value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.At(value);
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public ValueTask AtAsync(long value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.At(value);
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public ValueTask AtNowAsync(CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.AtNow();
        return FlushIfNecessaryAsync(ct);
    }

    /// <inheritdoc />
    public void At(DateTime value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.At(value);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void At(DateTimeOffset value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.At(value);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void At(long value, CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.At(value);
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void AtNow(CancellationToken ct = default)
    {
        GuardLastFlushNotSet();
        _buffer.AtNow();
        FlushIfNecessary(ct);
    }

    /// <inheritdoc />
    public void Truncate()
    {
        _buffer.TrimExcessBuffers();
    }

    /// <inheritdoc />
    public void CancelRow()
    {
        _buffer.CancelRow();
    }

    /// <inheritdoc />
    public void Clear()
    {
        _buffer.Clear();
    }

    /// <inheritdoc />
    public abstract void Dispose();

    /// <inheritdoc />
    public abstract Task SendAsync(CancellationToken ct = default);

    /// <inheritdoc />
    public abstract void Send(CancellationToken ct = default);

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
    public ValueTask FlushIfNecessaryAsync(CancellationToken ct = default)
    {
        if (Options.auto_flush == AutoFlushType.on && !WithinTransaction &&
            ((Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows)
             || (Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes)
             || (Options.auto_flush_interval > TimeSpan.Zero &&
                 DateTime.UtcNow - LastFlush >= Options.auto_flush_interval)))
            return new ValueTask(SendAsync(ct));

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc cref="FlushIfNecessaryAsync" />
    public void FlushIfNecessary(CancellationToken ct = default)
    {
        if (Options.auto_flush == AutoFlushType.on && !WithinTransaction &&
            ((Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows)
             || (Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes)
             || (Options.auto_flush_interval > TimeSpan.Zero &&
                 DateTime.UtcNow - LastFlush >= Options.auto_flush_interval)))
            Send(ct);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void GuardLastFlushNotSet()
    {
        if (LastFlush == DateTime.MinValue) LastFlush = DateTime.UtcNow;
    }
}