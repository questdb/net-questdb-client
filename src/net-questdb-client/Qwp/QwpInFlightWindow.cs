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

namespace QuestDB.Qwp;

/// <summary>
///     Tracks the high-water marks of in-flight batches with cumulative-ACK semantics.
/// </summary>
/// <remarks>
///     This is the bookkeeping side of the in-flight pipeline. The slot-count gate (i.e. how many
///     unacked batches are allowed at once) lives on a <see cref="System.Threading.SemaphoreSlim" />
///     in the sender; this class only tracks "what is the highest seq I sent" vs "what is the
///     highest seq the server acknowledged".
///     <para />
///     <b>Sentinel values:</b> both <see cref="AckedSequence" /> and <see cref="HighestSentSequence" />
///     start at <c>-1</c>. This disambiguates "never sent / never ACKed" from "ACKed at sequence 0".
///     <para />
///     <b>Cumulative ACK:</b> <see cref="AcknowledgeUpTo" /> with sequence <c>S</c> means every
///     batch with seq ≤ S has succeeded. Out-of-order arrivals are tolerated; lower sequences are
///     silently absorbed. Sequences past <see cref="HighestSentSequence" /> are a server bug and
///     throw.
///     <para />
///     <b>Terminal failure:</b> <see cref="FailAll" /> records the first failure; subsequent
///     <see cref="AwaitEmpty" /> calls rethrow it. Idempotent — only the first failure wins.
/// </remarks>
internal sealed class QwpInFlightWindow
{
    /// <summary>Polling quantum used to keep <c>AwaitEmpty</c> responsive to cancellation.</summary>
    private const int CancellationPollMs = 20;

    private readonly object _lock = new();
    private long _ackedSequence = -1L;
    private long _highestSentSequence = -1L;
    private Exception? _failure;

    // Allocated lazily on the first AwaitEmptyAsync call; consumed (set to null) by the next
    // signal fire so a steady-state pipeline with no awaiter pays zero TCS allocations.
    private TaskCompletionSource<bool>? _changeSignal;

    /// <summary>Highest sequence the server has acknowledged. Starts at <c>-1</c>.</summary>
    public long AckedSequence => Volatile.Read(ref _ackedSequence);

    /// <summary>Highest sequence the client has sent. Starts at <c>-1</c>.</summary>
    public long HighestSentSequence => Volatile.Read(ref _highestSentSequence);

    /// <summary>True when no batches are in flight (every sent sequence has been acked).</summary>
    public bool IsEmpty
    {
        get
        {
            lock (_lock)
            {
                return _ackedSequence == _highestSentSequence;
            }
        }
    }

    /// <summary>Number of batches currently in flight.</summary>
    public int InFlightCount
    {
        get
        {
            lock (_lock)
            {
                return (int)(_highestSentSequence - _ackedSequence);
            }
        }
    }

    /// <summary>True iff <see cref="FailAll" /> has been called.</summary>
    public bool HasFailure => Volatile.Read(ref _failure) is not null;

    /// <summary>
    ///     Records that the batch with sequence <paramref name="sequence" /> has been transmitted.
    ///     Sequences must be strictly ascending and start at 0.
    /// </summary>
    public void Add(long sequence)
    {
        TaskCompletionSource<bool>? wakeup;
        lock (_lock)
        {
            if (_failure is not null)
            {
                throw _failure;
            }

            if (sequence != _highestSentSequence + 1)
            {
                throw new InvalidOperationException(
                    $"non-sequential add: expected {_highestSentSequence + 1}, got {sequence}");
            }

            _highestSentSequence = sequence;
            Monitor.PulseAll(_lock);
            wakeup = ConsumeChangeSignalLocked();
        }
        wakeup?.TrySetResult(true);
    }

    /// <summary>
    ///     Cumulatively acknowledges every batch with sequence ≤ <paramref name="sequence" />.
    /// </summary>
    /// <remarks>
    ///     Re-arrivals (sequences ≤ the current ack watermark) are absorbed silently. Sequences past
    ///     <see cref="HighestSentSequence" /> are treated as a server protocol violation.
    /// </remarks>
    public void AcknowledgeUpTo(long sequence)
    {
        if (sequence < 0)
        {
            throw new InvalidOperationException(
                $"ack sequence must be ≥ 0; got {sequence}");
        }

        TaskCompletionSource<bool>? wakeup;
        lock (_lock)
        {
            if (_failure is not null)
            {
                return;
            }

            if (sequence > _highestSentSequence)
            {
                throw new InvalidOperationException(
                    $"ack {sequence} exceeds highest sent {_highestSentSequence}");
            }

            if (sequence <= _ackedSequence)
            {
                return;
            }

            _ackedSequence = sequence;
            Monitor.PulseAll(_lock);
            wakeup = ConsumeChangeSignalLocked();
        }
        wakeup?.TrySetResult(true);
    }

    /// <summary>
    ///     Records a terminal failure; rejects subsequent <see cref="Add" /> and propagates from
    ///     <see cref="AwaitEmpty" />. Only the first call takes effect.
    /// </summary>
    public void FailAll(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        TaskCompletionSource<bool>? wakeup;
        lock (_lock)
        {
            _failure ??= failure;
            Monitor.PulseAll(_lock);
            wakeup = ConsumeChangeSignalLocked();
        }
        wakeup?.TrySetResult(true);
    }

    /// <summary>
    ///     Blocks until the window is empty, a failure has been recorded, the cancellation token is
    ///     triggered, or <paramref name="timeout" /> elapses.
    /// </summary>
    /// <exception cref="TimeoutException">If the window did not drain within <paramref name="timeout" />.</exception>
    /// <exception cref="OperationCanceledException">If <paramref name="ct" /> fires.</exception>
    /// <exception cref="Exception">The recorded failure exception, if <see cref="FailAll" /> was called.</exception>
    public void AwaitEmpty(TimeSpan timeout, CancellationToken ct = default)
    {
        var hasDeadline = timeout >= TimeSpan.Zero;
        var totalMs = hasDeadline
            ? (int)Math.Min(timeout.TotalMilliseconds, int.MaxValue)
            : -1;
        var sw = hasDeadline ? Stopwatch.StartNew() : null;

        lock (_lock)
        {
            while (true)
            {
                // Check drained before failure: if every sent batch is acked, this AwaitEmpty call
                // is satisfied even if a subsequent failure (for a future batch) was just recorded.
                if (_ackedSequence >= _highestSentSequence)
                {
                    return;
                }

                if (_failure is not null)
                {
                    throw _failure;
                }

                ct.ThrowIfCancellationRequested();

                int waitMs;
                if (hasDeadline)
                {
                    var remaining = totalMs - (int)sw!.ElapsedMilliseconds;
                    if (remaining <= 0)
                    {
                        throw new TimeoutException(
                            $"in-flight window did not drain within {timeout.TotalMilliseconds:F0} ms (in-flight={_highestSentSequence - _ackedSequence})");
                    }

                    waitMs = remaining < CancellationPollMs ? remaining : CancellationPollMs;
                }
                else
                {
                    waitMs = CancellationPollMs;
                }

                Monitor.Wait(_lock, waitMs);
            }
        }
    }

    /// <summary>
    ///     Async counterpart of <see cref="AwaitEmpty" />: returns a Task that completes when the
    ///     window drains, throws on recorded failure, cancellation, or timeout.
    /// </summary>
    public async Task AwaitEmptyAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        var hasDeadline = timeout >= TimeSpan.Zero;
        var totalMs = hasDeadline
            ? (long)Math.Min(timeout.TotalMilliseconds, long.MaxValue)
            : -1L;
        var sw = hasDeadline ? Stopwatch.StartNew() : null;

        while (true)
        {
            Task waitTask;
            lock (_lock)
            {
                if (_ackedSequence >= _highestSentSequence)
                {
                    return;
                }

                if (_failure is not null)
                {
                    throw _failure;
                }

                ct.ThrowIfCancellationRequested();
                _changeSignal ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                waitTask = _changeSignal.Task;
            }

            if (hasDeadline)
            {
                var remainingMs = totalMs - sw!.ElapsedMilliseconds;
                if (remainingMs <= 0)
                {
                    throw new TimeoutException(
                        $"in-flight window did not drain within {timeout.TotalMilliseconds:F0} ms");
                }

                var slice = remainingMs > int.MaxValue
                    ? TimeSpan.FromMilliseconds(int.MaxValue)
                    : TimeSpan.FromMilliseconds(remainingMs);

                try
                {
                    await waitTask.WaitAsync(slice, ct).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                }
                catch (OperationCanceledException)
                {
                }
            }
            else
            {
                try
                {
                    await waitTask.WaitAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }

    private TaskCompletionSource<bool>? ConsumeChangeSignalLocked()
    {
        var prev = _changeSignal;
        _changeSignal = null;
        return prev;
    }
}
