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

using System.Diagnostics;
using System.Threading.Channels;
using QuestDB.Senders;

namespace QuestDB.Qwp;

internal sealed class QwpConnectionEventDispatcher : IDisposable
{
    private readonly Channel<SenderConnectionEvent> _inbox;
    private readonly ISenderConnectionListener _listener;
    private readonly CancellationTokenSource _shutdown = new();
    private long _dropped;
    private long _delivered;
    private Task? _loop;
    private int _started;
    private int _disposed;

    public QwpConnectionEventDispatcher(ISenderConnectionListener listener, int capacity)
    {
        ArgumentNullException.ThrowIfNull(listener);
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity));
        _listener = listener;
        _inbox = Channel.CreateBounded<SenderConnectionEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
        });
    }

    public long DroppedNotifications => Volatile.Read(ref _dropped);
    public long TotalDelivered => Volatile.Read(ref _delivered);

    public bool Offer(SenderConnectionEvent evt)
    {
        if (Volatile.Read(ref _disposed) != 0) return false;
        if (!_inbox.Writer.TryWrite(evt))
        {
            Interlocked.Increment(ref _dropped);
            return false;
        }
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _loop = Task.Run(DispatchLoopAsync);
        }
        return true;
    }

    private async Task DispatchLoopAsync()
    {
        try
        {
            while (await _inbox.Reader.WaitToReadAsync(_shutdown.Token).ConfigureAwait(false))
            {
                while (_inbox.Reader.TryRead(out var evt))
                {
                    Interlocked.Increment(ref _delivered);
                    try { _listener.OnEvent(evt); }
                    catch (Exception t) { Trace.TraceError($"ISenderConnectionListener.OnEvent threw: {t}"); }
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _inbox.Writer.TryComplete();
        try { _shutdown.Cancel(); } catch { }
        try { _loop?.Wait(TimeSpan.FromSeconds(2)); } catch { }
        _shutdown.Dispose();
    }
}
