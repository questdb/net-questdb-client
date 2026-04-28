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

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Bounded concurrent worker pool that drains sibling slot directories adopted by
///     <see cref="QwpOrphanScanner" />.
/// </summary>
/// <remarks>
///     Each <see cref="Enqueue" /> hands a held <see cref="QwpSlotLock" /> to the pool. A
///     <see cref="SemaphoreSlim" /> caps the number of drains running concurrently
///     (<c>max_background_drainers</c>, default 4). For each task:
///     <list type="bullet">
///         <item>drain runs through the configured <see cref="IQwpSlotDrainer" />;</item>
///         <item>on success, the slot lock is disposed — the slot is empty so it may be reclaimed
///             by any sender;</item>
///         <item>on terminal failure, a <c>.failed</c> sentinel is written before the lock is
///             released, so subsequent <see cref="QwpOrphanScanner" /> sweeps will skip the slot
///             and the user can manually inspect it;</item>
///         <item>on cancellation, the slot lock is disposed without dropping a sentinel — drain
///             will be retried on the next sender startup.</item>
///     </list>
/// </remarks>
internal sealed class QwpBackgroundDrainerPool : IDisposable
{
    private const string FailedSentinel = ".failed";

    private readonly IQwpSlotDrainer _drainer;
    private readonly SemaphoreSlim _slots;
    private readonly object _trackingLock = new();
    private readonly List<Task> _runningTasks = new();
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly TimeSpan _shutdownWait;
    private bool _disposed;

    public QwpBackgroundDrainerPool(int maxConcurrent, IQwpSlotDrainer drainer, TimeSpan? shutdownWait = null)
    {
        if (maxConcurrent <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrent), "must be ≥ 1");
        }

        _drainer = drainer ?? throw new ArgumentNullException(nameof(drainer));
        _slots = new SemaphoreSlim(maxConcurrent, maxConcurrent);
        _shutdownWait = shutdownWait ?? TimeSpan.FromSeconds(5);
    }

    /// <summary>Submits a drain. Lock ownership transfers to the pool.</summary>
    public void Enqueue(QwpSlotLock slotLock, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(slotLock);

        // Atomic against Dispose: hold _trackingLock for both the disposal check and the task
        // registration so a Task created here can never escape Dispose's snapshot.
        CancellationTokenSource linked;
        Task task;
        lock (_trackingLock)
        {
            EnsureNotDisposed();
            linked = CancellationTokenSource.CreateLinkedTokenSource(_shutdownCts.Token, cancellationToken);
            task = Task.Run(async () =>
            {
                try
                {
                    await RunDrainAsync(slotLock, linked.Token).ConfigureAwait(false);
                }
                finally
                {
                    linked.Dispose();
                }
            }, linked.Token);
            _runningTasks.Add(task);
        }

        // Schedule a continuation that prunes the completed task from the tracking list, bounded
        // memory regardless of how many slots get drained over a sender's lifetime.
        _ = task.ContinueWith(t =>
        {
            lock (_trackingLock)
            {
                _runningTasks.Remove(t);
            }
        }, TaskScheduler.Default);
    }

    /// <summary>Awaits all currently-enqueued drains. Subsequent <see cref="Enqueue" /> calls are independent.</summary>
    public Task WaitForAllAsync()
    {
        Task[] snapshot;
        lock (_trackingLock)
        {
            snapshot = _runningTasks.ToArray();
        }

        return Task.WhenAll(snapshot);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Task[] snapshot;
        lock (_trackingLock)
        {
            if (_disposed) return;
            _disposed = true;
            snapshot = _runningTasks.ToArray();
        }

        // Two-phase shutdown: give in-flight drains a chance to finish naturally, then cancel
        // and join. Dispose is sync so we cap the wait — orphans land on the next sender startup.
        if (snapshot.Length > 0)
        {
            try
            {
                Task.WhenAll(snapshot).Wait(_shutdownWait);
            }
            catch (Exception)
            {
                // Drain failures already wrote .failed sentinels; swallow here.
            }

            SfCleanup.Run(() => _shutdownCts.Cancel());

            try
            {
                Task.WhenAll(snapshot).Wait(TimeSpan.FromSeconds(2));
            }
            catch (Exception)
            {
                // Best-effort joined; tasks may finish later but the lock is theirs to release.
            }
        }

        SfCleanup.Dispose(_shutdownCts);
        _slots.Dispose();
    }

    private async Task RunDrainAsync(QwpSlotLock slotLock, CancellationToken cancellationToken)
    {
        try
        {
            await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _drainer.DrainAsync(slotLock.SlotDirectory, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Cooperative cancellation — leave the slot for the next sender startup.
                throw;
            }
            catch (Exception ex)
            {
                TryDropFailedSentinel(slotLock.SlotDirectory, ex);
            }
            finally
            {
                _slots.Release();
            }
        }
        finally
        {
            slotLock.Dispose();
        }
    }

    private static void TryDropFailedSentinel(string slotDirectory, Exception ex)
    {
        try
        {
            File.WriteAllText(Path.Combine(slotDirectory, FailedSentinel), ex.ToString());
        }
        catch (Exception)
        {
            // Best-effort sentinel; swallow any I/O failure rather than mask the real drain error.
        }
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpBackgroundDrainerPool));
        }
    }
}
