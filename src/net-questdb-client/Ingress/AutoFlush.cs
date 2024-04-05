using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Timers;
using QuestDB.Ingress.Enums;
using QuestDB.Ingress.Senders;

namespace QuestDB.Ingress;

public class AutoFlush
{
    private ISender _sender;

    public AutoFlush(ISender sender)
    {
        _sender = sender;
    }

    internal async Task FlushIfNecessary(CancellationToken ct = default)
    {
        if (ShouldAutoFlush(_sender.RowCount, _sender.Length, _sender.LastFlush))
        {
            await _sender.SendAsync(ct);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool ShouldAutoFlush(int rowCount, int byteCount, DateTime lastFlush)
    {
        return (_sender.Options.auto_flush == AutoFlushType.on) && (CheckRows(rowCount) || CheckBytes(byteCount) || CheckInterval(lastFlush));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CheckRows(int rowCount)
        => _sender.Options.auto_flush_rows > 0 && rowCount >= _sender.Options.auto_flush_rows;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CheckBytes(int byteCount)
        => _sender.Options.auto_flush_bytes > 0 && _sender.Options.auto_flush_bytes >= byteCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CheckInterval(DateTime lastFlush)
        => _sender.Options.auto_flush_interval > TimeSpan.Zero && DateTime.UtcNow - lastFlush >= _sender.Options.auto_flush_interval;
}