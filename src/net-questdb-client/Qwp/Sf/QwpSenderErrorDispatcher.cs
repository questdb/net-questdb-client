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
    private readonly SenderErrorHandler _handler;
    private readonly bool _hasCustomHandler;
    private readonly CancellationTokenSource _shutdown = new();
    private long _dropped;
    private long _delivered;
    private Task? _loop;
    private int _started;
    private int _disposed;

    public QwpSenderErrorDispatcher(SenderErrorHandler? handler, int capacity)
    {
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity));
        _hasCustomHandler = handler != null;
        _handler = handler ?? DefaultHandler;
        _inbox = Channel.CreateBounded<SenderError>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
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
        var written = _inbox.Writer.TryWrite(error);
        if (!written)
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
                while (_inbox.Reader.TryRead(out var err))
                {
                    Interlocked.Increment(ref _delivered);
                    try { _handler(err); }
                    catch (Exception t) { Trace.TraceError($"SenderErrorHandler threw: {t}"); }
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
        try { _loop?.Wait(TimeSpan.FromMilliseconds(200)); } catch { }
        _shutdown.Dispose();
    }

    public static readonly SenderErrorHandler DefaultHandler = static err =>
    {
        if (err.AppliedPolicy == SenderErrorPolicy.Halt)
        {
            Trace.TraceError($"QuestDB sender HALT: {err}");
        }
        else
        {
            Trace.TraceWarning($"QuestDB sender DROP: {err}");
        }
    };
}
