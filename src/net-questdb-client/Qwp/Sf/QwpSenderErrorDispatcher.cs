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
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp.Sf;

internal sealed class QwpSenderErrorDispatcher : IDisposable
{
    private readonly Channel<SenderError> _inbox;
    private readonly object _offerLock = new();
    private readonly object _lifecycleLock = new();
    private readonly SenderErrorHandler _handler;
    private readonly bool _hasCustomHandler;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly int _capacity;
    private long _dropped;
    private long _delivered;
    private Task? _loop;
    private int _started;
    private int _disposed;

    public QwpSenderErrorDispatcher(SenderErrorHandler? handler, int capacity)
    {
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity));
        _capacity = capacity;
        _hasCustomHandler = handler != null;
        _handler = handler ?? DefaultHandler;
        // Unbounded — capacity is enforced manually in Offer; both Offer and the loop read.
        _inbox = Channel.CreateUnbounded<SenderError>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
        });
    }

    public long DroppedNotifications => Volatile.Read(ref _dropped);
    public long TotalDelivered => Volatile.Read(ref _delivered);

    public bool HasDeliveredToCustomHandler =>
        _hasCustomHandler && Volatile.Read(ref _delivered) > 0;

    public bool Offer(SenderError error)
    {
        if (Volatile.Read(ref _disposed) != 0) return false;

        // Manual DropOldest: evict-then-write under the lock keeps _dropped exact.
        lock (_offerLock)
        {
            while (_inbox.Reader.Count >= _capacity && _inbox.Reader.TryRead(out _))
            {
                Interlocked.Increment(ref _dropped);
            }
            _inbox.Writer.TryWrite(error);
        }

        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            // _lifecycleLock pairs with Dispose's _loop read: Dispose joins this loop, or this
            // branch sees the disposal first and never starts on an already-disposed CTS.
            lock (_lifecycleLock)
            {
                if (Volatile.Read(ref _disposed) == 0)
                {
                    _loop = Task.Run(DispatchLoopAsync);
                }
            }
        }
        return true;
    }

    private async Task DispatchLoopAsync()
    {
        try
        {
            while (await _inbox.Reader.WaitToReadAsync(_shutdown.Token).ConfigureAwait(false))
            {
                while (_inbox.Reader.TryRead(out var err))
                {
                    Interlocked.Increment(ref _delivered);
                    try { _handler(err); }
                    catch (Exception t) { Trace.TraceError($"SenderErrorHandler threw: {t}"); }
                }
            }
        }
        catch (OperationCanceledException) { }
        // A racing Dispose can dispose _shutdown while WaitToReadAsync observes its token; treat
        // the resulting ObjectDisposedException as a clean shutdown rather than a faulted task.
        catch (ObjectDisposedException) { }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _inbox.Writer.TryComplete();
        Task? loop;
        lock (_lifecycleLock)
        {
            loop = _loop;
        }
        // Prefer a natural drain — WaitToReadAsync returns false once the completed inbox
        // empties — so a report offered just before teardown (e.g. a construction-time DataLoss)
        // is delivered, not dropped. Cancellation is only the fallback for a wedged handler.
        try { loop?.Wait(TimeSpan.FromMilliseconds(200)); } catch { }
        try { _shutdown.Cancel(); } catch { }
        try { loop?.Wait(TimeSpan.FromMilliseconds(50)); } catch { }
        _shutdown.Dispose();
    }

    public static readonly SenderErrorHandler DefaultHandler = static err =>
    {
        switch (err.AppliedPolicy)
        {
            case SenderErrorPolicy.Terminal:
                Trace.TraceError($"QuestDB sender TERMINAL: {err}");
                break;
            case SenderErrorPolicy.Abandoned:
                Trace.TraceError($"QuestDB sender DATA LOSS (abandoned): {err}");
                break;
            default:
                Trace.TraceWarning($"QuestDB sender RETRIABLE (replaying): {err}");
                break;
        }
    };
}
