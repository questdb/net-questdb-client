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
///     Background worker that owns hot-spare provisioning and trim for one
///     <see cref="QwpSegmentRing" />. Producer and I/O receive-pump never touch the disk for
///     spare creation, file deletion or mmap teardown; they only signal the manager via the
///     wakeup callback registered through <see cref="QwpSegmentRing.SetManagerWakeup" />.
/// </summary>
internal sealed class QwpSegmentManager : IDisposable
{
    public static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan DefaultShutdownWait = TimeSpan.FromSeconds(5);

    private readonly QwpSegmentRing _ring;
    private readonly long _maxTotalBytes;
    private readonly TimeSpan _shutdownWait;
    private readonly TimeSpan _heartbeatInterval;
    private readonly SemaphoreSlim _wakeup = new(0, 1);
    private readonly CancellationTokenSource _cts = new();

    private Task? _workerTask;
    private long _committedBytes;
    private volatile bool _disposed;
    private Exception? _lastServiceError;
    private Action? _heartbeatCallback;
    private QwpAckWatermark? _ackWatermark;
    private long _lastPersistedAck = long.MinValue;

    public QwpSegmentManager(
        QwpSegmentRing ring,
        long maxTotalBytes,
        TimeSpan? shutdownWait = null,
        TimeSpan? heartbeatInterval = null)
    {
        try
        {
            _ring = ring ?? throw new ArgumentNullException(nameof(ring));
            if (maxTotalBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxTotalBytes), "must be > 0");
            }

            _maxTotalBytes = maxTotalBytes;
            _shutdownWait = shutdownWait ?? DefaultShutdownWait;
            _heartbeatInterval = heartbeatInterval ?? DefaultHeartbeatInterval;
            _committedBytes = ring.TotalCapacityBytes;
            ring.SetMaxTotalBytes(maxTotalBytes);
        }
        catch
        {
            _wakeup.Dispose();
            _cts.Dispose();
            throw;
        }
    }

    public long CommittedBytes => Volatile.Read(ref _committedBytes);
    public long MaxTotalBytes => _maxTotalBytes;
    public TimeSpan HeartbeatInterval => _heartbeatInterval;

    public void SetHeartbeatCallback(Action? callback)
    {
        Volatile.Write(ref _heartbeatCallback, callback);
    }

    public void SetAckWatermark(QwpAckWatermark? watermark)
    {
        Volatile.Write(ref _ackWatermark, watermark);
    }
    internal long TrimCycles { get; private set; }
    internal long SparesInstalled { get; private set; }

    public void Start()
    {
        if (_workerTask is not null)
        {
            throw new InvalidOperationException("manager already started");
        }

        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(QwpSegmentManager));
        }

        _ring.SetManagerWakeup(Wake);
        // Adoption failure means the spare's bytes are gone from disk; decrement explicitly so the
        // next provisioning gate doesn't see a phantom commit and refuse a replacement.
        _ring.SetSpareAdoptionFailedCallback(OnSpareAdoptionFailed);
        _workerTask = Task.Run(() => RunAsync(_cts.Token));
    }

    public void Wake()
    {
        try
        {
            _wakeup.Release();
        }
        catch (SemaphoreFullException)
        {
        }
        catch (ObjectDisposedException)
        {
            // Ring callbacks aren't unregistered on Dispose; ignoring late wakes is safe.
        }
    }

    private void OnSpareAdoptionFailed()
    {
        Interlocked.Add(ref _committedBytes, -_ring.SegmentCapacity);
        Wake();
    }

    internal Task? WorkerTask => _workerTask;

    internal void RequestShutdown()
    {
        SfCleanup.Run(() => _cts.Cancel());
        SfCleanup.Run(() => _wakeup.Release());
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        RequestShutdown();

        if (_workerTask is not null)
        {
            SfCleanup.Run(() => _workerTask.Wait(_shutdownWait));
        }

        SfCleanup.Dispose(_cts);
        SfCleanup.Dispose(_wakeup);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        while (!_disposed && !ct.IsCancellationRequested)
        {
            try
            {
                ServiceRing();
            }
            catch (Exception ex)
            {
                Volatile.Write(ref _lastServiceError, ex);
            }

            try
            {
                await _wakeup.WaitAsync(_heartbeatInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
        }

        try
        {
            ServiceRing();
        }
        catch (Exception ex)
        {
            Volatile.Write(ref _lastServiceError, ex);
        }
    }

    /// <summary>Last unexpected exception thrown by <c>ServiceRing</c>; null when never faulted.</summary>
    public Exception? LastServiceError => Volatile.Read(ref _lastServiceError);

    private void ServiceRing()
    {
        // Snapshot under-counts during the producer's adopt window (hotSparePath cleared before
        // active is published); never let it shrink committed, otherwise the gate below would
        // re-provision and breach maxTotalBytes. Decrements come from explicit trim and
        // OnSpareAdoptionFailed.
        var (capacity, hasSpare) = _ring.SnapshotCapacity();
        var snapshot = capacity + (hasSpare ? _ring.SegmentCapacity : 0);
        // CAS loop: a concurrent Interlocked.Add from OnSpareAdoptionFailed must not be lost
        // to a non-atomic read-modify-write here.
        long committed;
        while (true)
        {
            var prev = Volatile.Read(ref _committedBytes);
            committed = snapshot > prev ? snapshot : prev;
            if (Interlocked.CompareExchange(ref _committedBytes, committed, prev) == prev)
            {
                break;
            }
        }

        if (_ring.NeedsHotSpare())
        {
            if (committed + _ring.SegmentCapacity <= _maxTotalBytes)
            {
                ProvisionHotSpare();
            }
        }

        DrainAndDisposeTrimmable();
        _ring.FlushActive();
        // Persist the watermark only after segment data is flushed so the persisted point
        // can only under-estimate (never claim more durable than is actually on disk).
        PersistAckWatermark();
        try { Volatile.Read(ref _heartbeatCallback)?.Invoke(); }
        catch (Exception ex) { Volatile.Write(ref _lastServiceError, ex); }
    }

    private void PersistAckWatermark()
    {
        var watermark = Volatile.Read(ref _ackWatermark);
        if (watermark is null) return;
        var currentAck = _ring.AckedFsn;
        if (currentAck > _lastPersistedAck)
        {
            watermark.Write(currentAck);
            watermark.Flush();
            _lastPersistedAck = currentAck;
        }
    }

    private void ProvisionHotSpare()
    {
        var directory = _ring.Directory;
        if (directory is null) return;
        var sparePath = Path.Combine(
            directory,
            QwpSegmentRing.SparePrefix + Guid.NewGuid().ToString("N") + QwpSegmentRing.SpareSuffix);
        var capacity = _ring.SegmentCapacity;

        try
        {
            using var fs = QwpFiles.OpenExclusive(sparePath);
            fs.SetLength(capacity);
            // Force block allocation so a producer mmap-write can't trigger SIGBUS / EFAULT later
            // when the disk turns out to be full.
            ReserveDiskBlocks(fs, capacity);
            fs.Flush(flushToDisk: true);
        }
        catch (Exception ex)
        {
            Volatile.Write(ref _lastServiceError, ex);
            SfCleanup.DeleteFile(sparePath);
            return;
        }

        if (_ring.InstallHotSpare(sparePath))
        {
            Interlocked.Add(ref _committedBytes, capacity);
            SparesInstalled++;
        }
        else
        {
            SfCleanup.DeleteFile(sparePath);
        }
    }

    private static void ReserveDiskBlocks(FileStream fs, long length) =>
        QwpFallocate.Reserve(fs, length);

    private void DrainAndDisposeTrimmable()
    {
        var trim = _ring.DrainTrimmable();
        if (trim is null)
        {
            return;
        }

        var memoryBacked = _ring.IsMemoryBacked;
        long freed = 0;
        for (var i = 0; i < trim.Count; i++)
        {
            var seg = trim[i];
            var path = seg.Path;
            var size = seg.Capacity;
            SfCleanup.Dispose(seg);
            if (!memoryBacked)
            {
                // File-mode: unlink failure leaves the file for next sender startup recovery.
                SfCleanup.DeleteFile(path);
            }
            freed += size;
        }

        if (freed > 0)
        {
            Interlocked.Add(ref _committedBytes, -freed);
        }

        TrimCycles++;
    }
}
