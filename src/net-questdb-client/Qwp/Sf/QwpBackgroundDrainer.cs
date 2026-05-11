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

using QuestDB.Enums;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     SF orphan-slot drainer that reuses the live <see cref="QwpCursorSendEngine" /> in
///     "no-lock" mode. Inherits the engine's reconnect + replay machinery for free.
/// </summary>
/// <remarks>
///     The drainer is invoked by <see cref="QwpBackgroundDrainerPool" /> after
///     <see cref="QwpOrphanScanner" /> has claimed the slot lock. The pool keeps the lock alive
///     for the drain duration; the engine constructed here is given <c>slotLock=null</c> so it
///     does not touch the lock on dispose.
///     <para />
///     Drain sequence:
///     <list type="number">
///         <item>open the segment ring on the slot directory;</item>
///         <item>if empty, return immediately (engine.Dispose still unlinks any stragglers);</item>
///         <item>start the engine and call <see cref="QwpCursorSendEngine.FlushAsync" /> with the
///             configured drain timeout — this returns once the server has acked every envelope;</item>
///         <item>dispose the engine, which trims residual <c>.sfa</c> files thanks to its
///             full-drain cleanup (see <see cref="QwpCursorSendEngine.Dispose" />).</item>
///     </list>
/// </remarks>
internal sealed class QwpBackgroundDrainer : IQwpSlotDrainer
{
    private readonly Func<DrainContext> _contextBuilder;
    private readonly QwpReconnectPolicy _reconnectPolicy;
    private readonly long _segmentCapacity;
    private readonly TimeSpan _drainTimeout;
    private readonly bool _durableAckMode;

    // Per-drain context isolates host-health state across concurrent drains; the foreground engine
    // and each pooled drainer task get their own tracker so a BeginRound by one does not clear the
    // round-attempted flags of another.
    public QwpBackgroundDrainer(
        Func<DrainContext> contextBuilder,
        QwpReconnectPolicy reconnectPolicy,
        long segmentCapacity,
        TimeSpan drainTimeout,
        bool durableAckMode = false)
    {
        ArgumentNullException.ThrowIfNull(contextBuilder);
        ArgumentNullException.ThrowIfNull(reconnectPolicy);
        if (segmentCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(segmentCapacity), "must be positive");
        }

        if (drainTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(drainTimeout), "must be positive");
        }

        _contextBuilder = contextBuilder;
        _reconnectPolicy = reconnectPolicy;
        _segmentCapacity = segmentCapacity;
        _drainTimeout = drainTimeout;
        _durableAckMode = durableAckMode;
    }

    public QwpBackgroundDrainer(
        Func<IQwpCursorTransport> transportFactory,
        QwpReconnectPolicy reconnectPolicy,
        long segmentCapacity,
        TimeSpan drainTimeout,
        Func<bool>? skipBackoffPredicate = null,
        bool durableAckMode = false)
        : this(
            () => new DrainContext(transportFactory, skipBackoffPredicate),
            reconnectPolicy,
            segmentCapacity,
            drainTimeout,
            durableAckMode)
    {
        ArgumentNullException.ThrowIfNull(transportFactory);
    }

    public async Task DrainAsync(string slotDirectory, CancellationToken cancellationToken)
    {
        var ctx = _contextBuilder();
        var ring = QwpSegmentRing.Open(slotDirectory, segmentCapacity: _segmentCapacity);
        QwpCursorSendEngine? engine = null;
        try
        {
            // Construct the engine even when the ring is empty so engine.Dispose's full-drain
            // branch still unlinks residual sf-*.sfa files. A slot with empty .sfa survivors
            // would otherwise be re-adopted by every subsequent scan and never make progress.
            engine = new QwpCursorSendEngine(
                slotLock: null,
                ring,
                ctx.TransportFactory,
                _reconnectPolicy,
                appendDeadline: TimeSpan.FromSeconds(30),
                initialConnectMode: InitialConnectMode.off,
                skipBackoffPredicate: ctx.SkipBackoffPredicate,
                durableAckMode: _durableAckMode);

            if (ring.NextFsn > ring.OldestFsn)
            {
                engine.Start();
                await engine.FlushAsync(_drainTimeout, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (engine is not null)
            {
                engine.Dispose();
            }
            else
            {
                ring.Dispose();
            }
        }
    }
}

internal readonly record struct DrainContext(
    Func<IQwpCursorTransport> TransportFactory,
    Func<bool>? SkipBackoffPredicate);
