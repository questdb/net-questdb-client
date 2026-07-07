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
    private readonly TimeSpan _connectTimeout;
    private readonly int _hostIndex;
    private readonly IQwpCursorTransport _inner;
    private readonly QwpHostHealthTracker _tracker;

    public QwpTrackedCursorTransport(IQwpCursorTransport inner, QwpHostHealthTracker tracker, int hostIndex)
        : this(inner, tracker, hostIndex, Timeout.InfiniteTimeSpan)
    {
    }

    public QwpTrackedCursorTransport(IQwpCursorTransport inner, QwpHostHealthTracker tracker, int hostIndex,
        TimeSpan connectTimeout)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
        _hostIndex = hostIndex;
        _connectTimeout = connectTimeout;
    }

    public (string Host, int Port)? Endpoint => _inner.Endpoint;

    public int NegotiatedMaxBatchSize => _inner.NegotiatedMaxBatchSize;

    public string? NegotiatedZone => _inner.NegotiatedZone;

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        CancellationTokenSource? timeoutCts = null;
        CancellationToken effectiveCt = cancellationToken;
        if (_connectTimeout != Timeout.InfiniteTimeSpan && _connectTimeout > TimeSpan.Zero)
        {
            timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_connectTimeout);
            effectiveCt = timeoutCts.Token;
        }

        try
        {
            await _inner.ConnectAsync(effectiveCt).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts is not null && timeoutCts.IsCancellationRequested
                                                                       && !cancellationToken.IsCancellationRequested)
        {
            _tracker.RecordTransportError(_hostIndex);
            throw new IngressError(ErrorCode.SocketError,
                $"WebSocket upgrade exceeded connect_timeout={_connectTimeout.TotalMilliseconds}ms");
        }
        catch (QwpIngressRoleRejectedException ex)
        {
            // Record the zone before the reject so a host that role-rejects but exposes a zone
            // still contributes to the same-zone vs other-zone tiering for later walks.
            if (!string.IsNullOrEmpty(ex.Zone))
            {
                _tracker.RecordZone(_hostIndex, ex.Zone);
            }
            _tracker.RecordRoleReject(_hostIndex, ex.IsTransient);
            throw;
        }
        catch (IngressError ex) when (ex.code is ErrorCode.AuthError or ErrorCode.DurableAckNotSupported)
        {
            throw;
        }
        catch
        {
            _tracker.RecordTransportError(_hostIndex);
            throw;
        }
        finally
        {
            timeoutCts?.Dispose();
        }

        // Record the zone on the success path too (not only on role-reject) so same-zone tiering
        // engages among healthy hosts; RecordZone is a no-op when the server advertised no zone.
        _tracker.RecordZone(_hostIndex, _inner.NegotiatedZone);
        _tracker.RecordSuccess(_hostIndex);
    }

    public async Task SendBinaryAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        try
        {
            await _inner.SendBinaryAsync(data, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _tracker.RecordMidStreamFailure(_hostIndex);
            throw;
        }
    }

    public async Task<int> ReceiveFrameAsync(Memory<byte> destination, CancellationToken cancellationToken)
    {
        try
        {
            return await _inner.ReceiveFrameAsync(destination, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _tracker.RecordMidStreamFailure(_hostIndex);
            throw;
        }
    }

    public async Task<(int Read, byte[] Buffer)> ReceiveFrameAsync(
        byte[] initialBuffer, int maxBytes, CancellationToken cancellationToken)
    {
        try
        {
            return await _inner.ReceiveFrameAsync(initialBuffer, maxBytes, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _tracker.RecordMidStreamFailure(_hostIndex);
            throw;
        }
    }

    public Task CloseAsync(CancellationToken cancellationToken)
        => _inner.CloseAsync(cancellationToken);

    public void Dispose() => _inner.Dispose();
}
