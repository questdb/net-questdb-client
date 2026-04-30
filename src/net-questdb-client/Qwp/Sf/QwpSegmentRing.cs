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

using System.Globalization;

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Chained ring of <see cref="QwpMmapSegment" /> files in one directory. Three writers:
///     producer (<see cref="TryAppend" /> + sealed-list add on rotation), engine receive-pump
///     (<see cref="Acknowledge" />), and the manager (<see cref="InstallHotSpare" /> +
///     <see cref="DrainTrimmable" />). Hot path is lock-free; the lock guards sealed-list
///     mutations and cross-thread iteration.
/// </summary>
internal sealed class QwpSegmentRing : IDisposable
{
    private const string FilenamePrefix = "sf-";
    private const string FilenameSuffix = ".sfa";
    internal const string SparePrefix = "sf-spare-";
    internal const string SpareSuffix = ".tmp";

    private readonly string _directory;
    private readonly long _segmentCapacity;
    private readonly int _maxFrameLength;
    private readonly bool _flushOnAppend;
    private readonly long _highWaterTrigger;
    private readonly object _lock = new();
    private readonly List<QwpMmapSegment> _sealedSegments = new();

    private QwpMmapSegment? _active;
    private string? _hotSparePath;
    private long _publishedFsn;
    private long _ackedFsn;
    private Action? _managerWakeup;
    private Action? _spareInstalledCallback;
    private Action? _spareAdoptionFailed;
    private bool _wakeRequestedForActive;
    private volatile bool _closed;

    private QwpSegmentRing(string directory, long segmentCapacity, int maxFrameLength, bool flushOnAppend)
    {
        _directory = directory;
        _segmentCapacity = segmentCapacity;
        _maxFrameLength = maxFrameLength;
        _flushOnAppend = flushOnAppend;
        // 75%: leaves a quarter-segment of producer runway for the manager to provision a spare.
        _highWaterTrigger = (segmentCapacity >> 2) * 3;
        _publishedFsn = -1L;
        _ackedFsn = -1L;
    }

    public long PublishedFsn => Volatile.Read(ref _publishedFsn);
    public long AckedFsn => Volatile.Read(ref _ackedFsn);
    public long NextFsn => PublishedFsn + 1;

    public long OldestFsn
    {
        get
        {
            lock (_lock)
            {
                if (_sealedSegments.Count > 0)
                {
                    return _sealedSegments[0].BaseFsn;
                }

                return Volatile.Read(ref _active)?.BaseFsn ?? 0L;
            }
        }
    }

    public long SegmentCapacity => _segmentCapacity;
    public int MaxFrameLength => _maxFrameLength;
    public string Directory => _directory;

    public int SegmentCount
    {
        get
        {
            lock (_lock)
            {
                return _sealedSegments.Count + (Volatile.Read(ref _active) is null ? 0 : 1);
            }
        }
    }

    public long TotalCapacityBytes
    {
        get
        {
            lock (_lock)
            {
                long total = 0;
                for (var i = 0; i < _sealedSegments.Count; i++)
                {
                    total += _sealedSegments[i].Capacity;
                }
                var active = Volatile.Read(ref _active);
                if (active is not null)
                {
                    total += active.Capacity;
                }
                return total;
            }
        }
    }

    public bool NeedsHotSpare()
    {
        if (Volatile.Read(ref _hotSparePath) is not null) return false;
        var active = Volatile.Read(ref _active);
        if (active is null) return true;
        if (active.IsSealed) return true;
        return active.WritePosition >= _highWaterTrigger;
    }

    /// <summary>True iff a hot-spare path is currently installed and not yet adopted.</summary>
    public bool HasHotSpare => Volatile.Read(ref _hotSparePath) is not null;

    /// <summary>Atomic read of (TotalCapacityBytes, HasHotSpare) — both under the same lock.</summary>
    public (long TotalCapacityBytes, bool HasHotSpare) SnapshotCapacity()
    {
        lock (_lock)
        {
            long total = 0;
            for (var i = 0; i < _sealedSegments.Count; i++)
            {
                total += _sealedSegments[i].Capacity;
            }
            var active = Volatile.Read(ref _active);
            if (active is not null)
            {
                total += active.Capacity;
            }
            return (total, _hotSparePath is not null);
        }
    }

    public static QwpSegmentRing Open(
        string directory,
        long segmentCapacity = 64L * 1024 * 1024,
        int maxFrameLength = QwpMmapSegment.DefaultMaxFrameLength,
        bool flushOnAppend = false)
    {
        QwpFiles.EnsureDirectory(directory);
        var ring = new QwpSegmentRing(directory, segmentCapacity, maxFrameLength, flushOnAppend);

        try
        {
            CleanupStaleSpares(directory);

            // Filename is enumerate-only; ordering comes from the on-disk header baseSeq.
            var opened = new List<QwpMmapSegment>();
            try
            {
                foreach (var path in QwpFiles.EnumerateFiles(directory, FilenamePrefix + "*" + FilenameSuffix))
                {
                    if (Path.GetFileName(path).StartsWith(SparePrefix, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var seg = QwpMmapSegment.OpenExisting(path, segmentCapacity, maxFrameLength, flushOnAppend);
                    if (seg is null)
                    {
                        SfCleanup.DeleteFile(path);
                        continue;
                    }
                    opened.Add(seg);
                }

                opened.Sort((a, b) => a.BaseFsn.CompareTo(b.BaseFsn));

                for (var i = 0; i < opened.Count; i++)
                {
                    var seg = opened[i];
                    if (i < opened.Count - 1)
                    {
                        seg.Seal();
                        ring._sealedSegments.Add(seg);
                    }
                    else
                    {
                        Volatile.Write(ref ring._active, seg);
                    }
                }
            }
            catch
            {
                foreach (var s in opened) SfCleanup.Dispose(s);
                throw;
            }

            var lastRecovered = Volatile.Read(ref ring._active)
                ?? (ring._sealedSegments.Count > 0 ? ring._sealedSegments[^1] : null);
            ring._publishedFsn = lastRecovered is null
                ? -1L
                : lastRecovered.BaseFsn + lastRecovered.EnvelopeCount - 1;

            return ring;
        }
        catch (Exception)
        {
            ring.Dispose();
            throw;
        }
    }

    public void SetManagerWakeup(Action wakeup)
    {
        Volatile.Write(ref _managerWakeup, wakeup);
    }

    /// <summary>
    ///     Engine subscribes here to get woken when the manager installs a hot spare; pairs with
    ///     producer's TryAppend returning false on no-spare.
    /// </summary>
    public void SetSpareInstalledCallback(Action callback)
    {
        Volatile.Write(ref _spareInstalledCallback, callback);
    }

    /// <summary>
    ///     Manager subscribes here to learn that a spare it installed could not be adopted by the
    ///     producer (file move failed). Used to reconcile committed-bytes accounting.
    /// </summary>
    public void SetSpareAdoptionFailedCallback(Action callback)
    {
        Volatile.Write(ref _spareAdoptionFailed, callback);
    }

    public bool TryAppend(ReadOnlySpan<byte> frame)
    {
        if (frame.Length == 0)
        {
            throw new ArgumentException("empty frames are not permitted on the wire", nameof(frame));
        }

        if (frame.Length + QwpMmapSegment.EnvelopeHeaderSize > _segmentCapacity)
        {
            throw new ArgumentException(
                $"frame ({frame.Length} bytes) exceeds segment capacity ({_segmentCapacity} bytes); raise sf_max_bytes",
                nameof(frame));
        }

        EnsureNotClosed();

        // Seal-before-rotate (independent of spare availability) so trim → cap-free → spare-install
        // can make progress when the disk cap is tight.
        var active = Volatile.Read(ref _active);
        if (active is not null && !active.IsSealed && active.WritePosition + QwpMmapSegment.EnvelopeHeaderSize + frame.Length > active.Capacity)
        {
            SealAndAddCurrentToSealed();
            active = null;
        }

        if (active is null)
        {
            if (!TryAllocateNewActive())
            {
                return false;
            }

            active = Volatile.Read(ref _active)!;
        }

        if (!active.TryAppend(frame))
        {
            throw new InvalidOperationException("freshly allocated segment cannot accommodate the frame");
        }

        BumpPublishedFsn();
        CheckHighWaterAndWakeManager(active);
        return true;
    }

    public int TryReadFrame(long fsn, Span<byte> destination)
    {
        QwpMmapSegment? seg;
        lock (_lock)
        {
            if (_closed) return -1;
            seg = FindSegmentLocked(fsn);
        }

        if (seg is null) return -1;

        var envelopeIndex = fsn - seg.BaseFsn;
        var offset = seg.OffsetOfEnvelope(envelopeIndex);
        if (offset is null) return -1;

        // Defensive: cursor-pump and trim never overlap on the same FSN range, but a stale
        // segment reference shouldn't crash a read.
        try
        {
            return seg.TryReadFrame(offset.Value, destination, out _);
        }
        catch (ObjectDisposedException)
        {
            return -1;
        }
    }

    public void Acknowledge(long fsn)
    {
        var current = Volatile.Read(ref _ackedFsn);
        if (fsn > current)
        {
            Volatile.Write(ref _ackedFsn, fsn);
            Volatile.Read(ref _managerWakeup)?.Invoke();
        }
    }

    /// <summary>
    ///     Caller (manager) takes ownership of returned segments and is responsible for Dispose +
    ///     file unlink. Returns null when nothing is eligible (no list allocation on no-op).
    /// </summary>
    public List<QwpMmapSegment>? DrainTrimmable()
    {
        var acked = Volatile.Read(ref _ackedFsn);
        List<QwpMmapSegment>? drained = null;
        lock (_lock)
        {
            while (_sealedSegments.Count > 0)
            {
                var oldest = _sealedSegments[0];
                var lastFsn = oldest.BaseFsn + oldest.EnvelopeCount - 1;
                if (lastFsn > acked)
                {
                    break;
                }

                drained ??= new List<QwpMmapSegment>(_sealedSegments.Count);
                drained.Add(oldest);
                _sealedSegments.RemoveAt(0);
            }
        }

        return drained;
    }

    /// <summary>
    ///     Returns false (caller cleans up) if the ring is closed or already has a spare.
    /// </summary>
    public bool InstallHotSpare(string sparePath)
    {
        ArgumentNullException.ThrowIfNull(sparePath);

        lock (_lock)
        {
            if (_closed) return false;
            if (_hotSparePath is not null) return false;
            _hotSparePath = sparePath;
        }

        Volatile.Read(ref _spareInstalledCallback)?.Invoke();
        return true;
    }

    public int SealedSegmentCount
    {
        get
        {
            lock (_lock) return _sealedSegments.Count;
        }
    }

    public QwpMmapSegment? FindSegmentContaining(long fsn)
    {
        lock (_lock)
        {
            return _closed ? null : FindSegmentLocked(fsn);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        QwpMmapSegment? active;
        List<QwpMmapSegment> sealedSnapshot;
        string? sparePath;
        lock (_lock)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            active = Volatile.Read(ref _active);
            Volatile.Write(ref _active, null);
            sparePath = _hotSparePath;
            _hotSparePath = null;
            sealedSnapshot = new List<QwpMmapSegment>(_sealedSegments);
            _sealedSegments.Clear();
        }

        for (var i = 0; i < sealedSnapshot.Count; i++)
        {
            SfCleanup.Dispose(sealedSnapshot[i]);
        }

        SfCleanup.Dispose(active);

        if (sparePath is not null)
        {
            try
            {
                if (File.Exists(sparePath)) File.Delete(sparePath);
            }
            catch (Exception)
            {
                // next sender startup cleans stray .tmp via CleanupStaleSpares
            }
        }
    }

    private void BumpPublishedFsn()
    {
        // Atomic increment doubles as a release barrier: mmap bytes are visible before the FSN.
        Interlocked.Increment(ref _publishedFsn);
    }

    private void CheckHighWaterAndWakeManager(QwpMmapSegment active)
    {
        if (_wakeRequestedForActive) return;
        if (Volatile.Read(ref _hotSparePath) is not null) return;
        if (active.WritePosition < _highWaterTrigger) return;
        _wakeRequestedForActive = true;
        Volatile.Read(ref _managerWakeup)?.Invoke();
    }

    private bool TryAllocateNewActive()
    {
        var baseFsn = Volatile.Read(ref _publishedFsn) + 1;
        var realPath = Path.Combine(_directory, BuildFileName(baseFsn));

        var sparePath = Interlocked.Exchange(ref _hotSparePath, null);
        if (sparePath is not null && TryAdoptSpare(sparePath, realPath, baseFsn))
        {
            _wakeRequestedForActive = false;
            Volatile.Read(ref _managerWakeup)?.Invoke();
            return true;
        }

        var wakeup = Volatile.Read(ref _managerWakeup);
        if (wakeup is not null)
        {
            // Adoption failed (rare File.Move error) → spare bytes are gone but the manager's
            // committed accounting still includes them. Signal so it reconciles next tick.
            if (sparePath is not null)
            {
                Volatile.Read(ref _spareAdoptionFailed)?.Invoke();
            }
            wakeup();
            return false;
        }

        // Standalone mode (no manager) — ring-only unit tests.
        QwpMmapSegment? seg = null;
        try
        {
            seg = QwpMmapSegment.Open(realPath, _segmentCapacity, baseFsn, _maxFrameLength, _flushOnAppend);
            if (!PublishActive(seg, ref seg))
            {
                return false;
            }
            _wakeRequestedForActive = false;
            return true;
        }
        catch (Exception)
        {
            if (seg is not null) SfCleanup.Dispose(seg);
            return false;
        }
    }

    private void SealAndAddCurrentToSealed()
    {
        var active = Volatile.Read(ref _active);
        if (active is null) return;
        if (!active.IsSealed) active.Seal();
        lock (_lock)
        {
            if (!_closed)
            {
                _sealedSegments.Add(active);
            }
            Volatile.Write(ref _active, null);
        }
    }

    private bool TryAdoptSpare(string sparePath, string realPath, long baseFsn)
    {
        QwpMmapSegment? seg = null;
        try
        {
            if (!File.Exists(sparePath)) return false;
            File.Move(sparePath, realPath);
            seg = QwpMmapSegment.Open(realPath, _segmentCapacity, baseFsn, _maxFrameLength, _flushOnAppend);
            return PublishActive(seg, ref seg);
        }
        catch (Exception)
        {
            if (seg is not null) SfCleanup.Dispose(seg);
            SfCleanup.DeleteFile(sparePath);
            return false;
        }
    }

    private bool PublishActive(QwpMmapSegment seg, ref QwpMmapSegment? handoff)
    {
        lock (_lock)
        {
            if (_closed)
            {
                return false;
            }

            Volatile.Write(ref _active, seg);
            handoff = null;
            return true;
        }
    }

    private QwpMmapSegment? FindSegmentLocked(long fsn)
    {
        for (var i = 0; i < _sealedSegments.Count; i++)
        {
            var s = _sealedSegments[i];
            if (fsn >= s.BaseFsn && fsn < s.NextFsn) return s;
        }

        var active = Volatile.Read(ref _active);
        if (active is not null && fsn >= active.BaseFsn && fsn < active.NextFsn)
        {
            return active;
        }

        return null;
    }

    private void EnsureNotClosed()
    {
        // Volatile read keeps the producer hot path lock-free; the write in Dispose runs under _lock.
        if (_closed) throw new ObjectDisposedException(nameof(QwpSegmentRing));
    }

    internal static string BuildFileName(long baseFsn)
    {
        return FilenamePrefix + baseFsn.ToString("x16", CultureInfo.InvariantCulture) + FilenameSuffix;
    }

    private static void CleanupStaleSpares(string directory)
    {
        try
        {
            foreach (var path in QwpFiles.EnumerateFiles(directory, SparePrefix + "*" + SpareSuffix))
            {
                SfCleanup.DeleteFile(path);
            }
        }
        catch (Exception) { /* best-effort */ }
    }
}
