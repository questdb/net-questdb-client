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
    public static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan DefaultShutdownWait = TimeSpan.FromSeconds(5);

    private readonly QwpSegmentRing _ring;
    private readonly long _maxTotalBytes;
    private readonly TimeSpan _shutdownWait;
    private readonly SemaphoreSlim _wakeup = new(0, 1);
    private readonly CancellationTokenSource _cts = new();

    private Task? _workerTask;
    private long _committedBytes;
    private bool _disposed;

    public QwpSegmentManager(QwpSegmentRing ring, long maxTotalBytes, TimeSpan? shutdownWait = null)
    {
        _ring = ring ?? throw new ArgumentNullException(nameof(ring));
        if (maxTotalBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxTotalBytes), "must be > 0");
        }

        _maxTotalBytes = maxTotalBytes;
        _shutdownWait = shutdownWait ?? DefaultShutdownWait;
        _committedBytes = ring.TotalCapacityBytes;
    }

    public long CommittedBytes => Volatile.Read(ref _committedBytes);
    public long MaxTotalBytes => _maxTotalBytes;
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
        // Producer signals here when File.Move on a spare path failed; the spare's bytes are gone
        // from disk but our committed-bytes accounting still includes them. Wake the worker so it
        // reconciles on the next tick.
        _ring.SetSpareAdoptionFailedCallback(Wake);
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
            // already pending; the next tick will pick up the latest state
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        SfCleanup.Run(() => _cts.Cancel());
        SfCleanup.Run(() => _wakeup.Release());

        if (_workerTask is not null)
        {
            SfCleanup.Run(() => _workerTask.Wait(_shutdownWait));
        }

        SfCleanup.Dispose(_cts);
        SfCleanup.Dispose(_wakeup);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { ServiceRing(); }
            catch (Exception)
            {
                // a broken manager is a backpressure source, not a crash — never propagate
            }

            try
            {
                await _wakeup.WaitAsync(HeartbeatInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                // Dispose hit its shutdown timeout and disposed _wakeup while we were running.
                break;
            }
        }

        // post-shutdown drain so the slot directory ends up clean
        SfCleanup.Run(ServiceRing);
    }

    private void ServiceRing()
    {
        // Reconcile committed-bytes against the ring's actual on-disk footprint. Producer's
        // adoption failures (rare File.Move errors) and any other accounting drift get corrected
        // here; the manager is the single writer of _committedBytes outside the constructor.
        var actual = _ring.TotalCapacityBytes + (_ring.HasHotSpare ? _ring.SegmentCapacity : 0);
        Volatile.Write(ref _committedBytes, actual);

        if (_ring.NeedsHotSpare())
        {
            if (actual + _ring.SegmentCapacity <= _maxTotalBytes)
            {
                ProvisionHotSpare();
            }
        }

        DrainAndDisposeTrimmable();
    }

    private void ProvisionHotSpare()
    {
        var sparePath = Path.Combine(
            _ring.Directory,
            QwpSegmentRing.SparePrefix + Guid.NewGuid().ToString("N") + QwpSegmentRing.SpareSuffix);
        var capacity = _ring.SegmentCapacity;

        try
        {
            using var fs = QwpFiles.OpenExclusive(sparePath);
            fs.SetLength(capacity);
            fs.Flush();
        }
        catch (Exception)
        {
            try
            {
                if (File.Exists(sparePath)) File.Delete(sparePath);
            }
            catch (Exception) { /* best-effort */ }
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

    private void DrainAndDisposeTrimmable()
    {
        var trim = _ring.DrainTrimmable();
        if (trim is null)
        {
            return;
        }

        long freed = 0;
        for (var i = 0; i < trim.Count; i++)
        {
            var seg = trim[i];
            var path = seg.Path;
            var size = seg.Capacity;
            SfCleanup.Dispose(seg);
            try
            {
                if (File.Exists(path)) File.Delete(path);
            }
            catch (Exception)
            {
                // file persists; next sender startup will pick it up via recovery
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
