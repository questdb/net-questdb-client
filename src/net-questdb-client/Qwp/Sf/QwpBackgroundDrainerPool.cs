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
    private readonly HashSet<QwpSlotLock> _liveLocks = new();
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly TimeSpan _shutdownWait;
    private bool _disposed;

    public QwpBackgroundDrainerPool(int maxConcurrent, IQwpSlotDrainer drainer, TimeSpan? shutdownWait = null)
    {
        try
        {
            if (maxConcurrent <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxConcurrent), "must be ≥ 1");
            }

            _drainer = drainer ?? throw new ArgumentNullException(nameof(drainer));
            _slots = new SemaphoreSlim(maxConcurrent, maxConcurrent);
            _shutdownWait = shutdownWait ?? TimeSpan.FromSeconds(5);
        }
        catch
        {
            _shutdownCts.Dispose();
            throw;
        }
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
            _liveLocks.Add(slotLock);
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
            });
            _runningTasks.Add(task);
        }

        // Schedule a continuation that prunes the completed task from the tracking list, bounded
        // memory regardless of how many slots get drained over a sender's lifetime.
        _ = task.ContinueWith(static (t, state) =>
        {
            var self = (QwpBackgroundDrainerPool)state!;
            lock (self._trackingLock)
            {
                self._runningTasks.Remove(t);
            }
        }, this, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
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

        var allJoined = snapshot.Length == 0;
        if (snapshot.Length > 0)
        {
            var joinTask = Task.WhenAll(snapshot);
            try
            {
                allJoined = joinTask.Wait(_shutdownWait);
            }
            catch (Exception)
            {
            }

            SfCleanup.Run(() => _shutdownCts.Cancel());

            try
            {
                allJoined = joinTask.Wait(TimeSpan.FromSeconds(2));
            }
            catch (Exception)
            {
            }
        }

        // Release slot locks even on join timeout, otherwise a wedged drainer keeps the .lock file
        // held and blocks future senders from claiming the same sender_id. QwpSlotLock.Dispose is
        // idempotent so the drainer's own finally can still run safely.
        QwpSlotLock[] stragglers;
        lock (_trackingLock)
        {
            stragglers = _liveLocks.ToArray();
            _liveLocks.Clear();
        }
        foreach (var l in stragglers)
        {
            SfCleanup.Dispose(l);
        }

        // On wedge, leak _shutdownCts and _slots so a still-running drainer can't NRE on them.
        if (allJoined)
        {
            SfCleanup.Dispose(_shutdownCts);
            _slots.Dispose();
        }
    }

    private async Task RunDrainAsync(QwpSlotLock slotLock, CancellationToken cancellationToken)
    {
        try
        {
            try
            {
                await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // Pool's _shutdownCts already disposed; treat as cancellation.
                return;
            }

            try
            {
                await _drainer.DrainAsync(slotLock.SlotDirectory, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (ObjectDisposedException)
            {
                // Late teardown of a CTS / drainer dependency is shutdown noise, not a slot failure.
            }
            catch (Exception ex) when (IsReplayImpossible(ex))
            {
                TryDropFailedSentinel(slotLock.SlotDirectory, ex);
            }
            catch (Exception)
            {
                // Transient (timeout, transport, IO) — leave the slot for next attempt.
            }
            finally
            {
                try
                {
                    _slots.Release();
                }
                catch (ObjectDisposedException)
                {
                }
                catch (SemaphoreFullException)
                {
                }
            }
        }
        finally
        {
            lock (_trackingLock)
            {
                _liveLocks.Remove(slotLock);
            }
            slotLock.Dispose();
        }
    }

    private static bool IsReplayImpossible(Exception ex)
    {
        if (ex is QwpException q)
        {
            return q.Status switch
            {
                Enums.QwpStatusCode.SchemaMismatch => true,
                Enums.QwpStatusCode.SecurityError => true,
                Enums.QwpStatusCode.ParseError => true,
                _ => false,
            };
        }

        return false;
    }

    private const int FailedSentinelMaxBytes = 4096;

    private static void TryDropFailedSentinel(string slotDirectory, Exception ex)
    {
        try
        {
            var content = ex.ToString();
            if (content.Length > FailedSentinelMaxBytes)
            {
                content = content.Substring(0, FailedSentinelMaxBytes) + "\n... [truncated]";
            }
            File.WriteAllText(Path.Combine(slotDirectory, FailedSentinel), content);
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
