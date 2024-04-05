using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Utils;


namespace QuestDB.Ingress.Senders;

public interface ISender : IDisposable, IAsyncDisposable
{
    // Config
    public ISender Configure(QuestDBOptions options);

    public ISender Build();
    
    // Transaction
    public ISender Transaction(ReadOnlySpan<char> tableName) 
        => throw new IngressError(ErrorCode.InvalidApiCall, $"`{GetType().Name}` does not support transactions.");
    public Task CommitAsync(CancellationToken ct = default) 
        => throw new IngressError(ErrorCode.InvalidApiCall, $"`{GetType().Name}` does not support transactions.");

    public void Commit() => CommitAsync().Wait();
    
    //Transport
    public Task SendAsync(CancellationToken ct = default);
    public void Send() => SendAsync().Wait();
    
    
    public int Length { get; }
    public int RowCount { get; }
    public bool WithinTransaction { get; }
    public DateTime LastFlush { get; } 
    public QuestDBOptions Options { get; }
    
    
    public ISender Table(ReadOnlySpan<char> name);
    public ISender Symbol(ReadOnlySpan<char> name, ReadOnlySpan<char> value);
    public ISender Column(ReadOnlySpan<char> name, ReadOnlySpan<char> value);
    public ISender Column(ReadOnlySpan<char> name, long value);
    public ISender Column(ReadOnlySpan<char> name, bool value);
    public ISender Column(ReadOnlySpan<char> name, double value);
    public ISender Column(ReadOnlySpan<char> name, DateTime value);
    public ISender Column(ReadOnlySpan<char> name, DateTimeOffset value);
    public Task At(DateTime value, CancellationToken ct = default);
    public Task At(DateTimeOffset value, CancellationToken ct = default);
    public Task At(long value, CancellationToken ct = default);
    public Task AtNow(CancellationToken ct = default);

    public void Truncate();

    public void CancelRow();
    internal async Task FlushIfNecessary(CancellationToken ct = default)
    {
        if (Options.auto_flush == AutoFlushType.on && !WithinTransaction &&
        ((Options.auto_flush_rows > 0 && RowCount >= Options.auto_flush_rows) 
         || (Options.auto_flush_bytes > 0 && Length >= Options.auto_flush_bytes)
         || (Options.auto_flush_interval > TimeSpan.Zero && DateTime.UtcNow - LastFlush >= Options.auto_flush_interval)))
        {
            await SendAsync(ct);
        }
    }
}