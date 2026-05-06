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

using System;
using System.Threading;
using System.Threading.Tasks;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Decorator that records the outcome of <see cref="ConnectAsync" /> against a
///     <see cref="QwpHostHealthTracker" /> entry so the SF cursor engine's reconnect
///     loop can rotate through configured addresses without owning the tracker
///     directly. Auth failures are *not* classified — the engine treats them as
///     terminal regardless of host.
/// </summary>
internal sealed class QwpTrackedCursorTransport : IQwpCursorTransport
{
    private readonly int _hostIndex;
    private readonly IQwpCursorTransport _inner;
    private readonly QwpHostHealthTracker _tracker;

    public QwpTrackedCursorTransport(IQwpCursorTransport inner, QwpHostHealthTracker tracker, int hostIndex)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
        _hostIndex = hostIndex;
    }

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _inner.ConnectAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (QwpIngressRoleRejectedException ex)
        {
            _tracker.RecordRoleReject(_hostIndex, ex.IsTransient);
            throw;
        }
        catch (IngressError ex) when (ex.code == ErrorCode.AuthError)
        {
            throw;
        }
        catch
        {
            _tracker.RecordTransportError(_hostIndex);
            throw;
        }

        _tracker.RecordSuccess(_hostIndex);
    }

    public Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        => _inner.SendBinaryAsync(data, cancellationToken);

    public Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken)
        => _inner.ReceiveFrameAsync(destination, cancellationToken);

    public Task CloseAsync(CancellationToken cancellationToken)
        => _inner.CloseAsync(cancellationToken);

    public void Dispose() => _inner.Dispose();
}
