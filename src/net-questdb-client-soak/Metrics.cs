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
using QuestDB.Senders;

namespace QuestDB.Soak;

/// <summary>Global cumulative counters, updated from every producer.</summary>
internal sealed class Metrics
{
    private long _bytes;
    private long _rows;

    public long Rows => Interlocked.Read(ref _rows);
    public long Bytes => Interlocked.Read(ref _bytes);

    public void Record(long rows, long bytes)
    {
        Interlocked.Add(ref _rows, rows);
        Interlocked.Add(ref _bytes, bytes);
    }
}

/// <summary>
///     Periodic stdout report. This is the whole point of the harness: rows/s and MB/s to prove
///     the load profile, pool + QWP counters to watch reconnect / error churn, SF disk usage to
///     confirm the throughput cap is holding, and process working-set / GC / allocation trend to
///     surface a slow leak. A monotonic working-set climb past the configured threshold trips a WARN.
/// </summary>
internal sealed class Reporter
{
    private readonly IReadOnlyList<IQuestDBClient> _clients;
    private readonly SoakConfig _cfg;
    private readonly Metrics _metrics;
    private readonly IReadOnlyList<IQwpWebSocketSender> _pinned;
    private readonly Process _proc = Process.GetCurrentProcess();
    private readonly IReadOnlyList<TableSpec> _specs;
    private readonly Stopwatch _wall = Stopwatch.StartNew();
    private readonly Queue<long> _wsHistory = new();

    private long _baselineWorkingSet;
    private long _lastBytes;
    private long _lastRows;
    private long _lastTicks;
    private bool _leakWarned;

    public Reporter(SoakConfig cfg, Metrics metrics, IReadOnlyList<IQuestDBClient> clients,
        IReadOnlyList<IQwpWebSocketSender> pinned, IReadOnlyList<TableSpec> specs)
    {
        _cfg = cfg;
        _metrics = metrics;
        _clients = clients;
        _pinned = pinned;
        _specs = specs;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(_cfg.ReportEverySec), ct).ConfigureAwait(false);
                PrintOnce();
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public void PrintOnce()
    {
        var nowTicks = _wall.ElapsedTicks;
        var rows = _metrics.Rows;
        var bytes = _metrics.Bytes;
        var elapsed = (nowTicks - _lastTicks) / (double)Stopwatch.Frequency;
        if (elapsed <= 0) elapsed = 0.001;

        var dRows = rows - _lastRows;
        var dBytes = bytes - _lastBytes;
        _lastRows = rows;
        _lastBytes = bytes;
        _lastTicks = nowTicks;

        var instRows = dRows / elapsed;
        var instMib = dBytes / elapsed / (1024 * 1024);
        var avgMib = bytes / _wall.Elapsed.TotalSeconds / (1024 * 1024);

        long frames = 0, acks = 0, srvErr = 0, reconn = 0, reconnOk = 0, dropped = 0;
        foreach (var s in _pinned)
        {
            frames += s.TotalFramesSent;
            acks += s.TotalAcks;
            srvErr += s.TotalServerErrors;
            reconn += s.TotalReconnectAttempts;
            reconnOk += s.TotalReconnectsSucceeded;
            dropped += s.DroppedErrorNotifications;
        }

        int idle = 0, total = 0;
        foreach (var c in _clients)
        {
            idle += c.AvailableSenderCount;
            total += c.TotalSenderCount;
        }

        var sfBytes = DirectorySize(_cfg.SfDir);
        _proc.Refresh();
        var ws = _proc.WorkingSet64;
        if (_baselineWorkingSet == 0) _baselineWorkingSet = ws;

        var totalAlloc = GC.GetTotalAllocatedBytes();
        var gcMem = GC.GetTotalMemory(false);

        Console.WriteLine(
            $"[{FormatElapsed(_wall.Elapsed)}] " +
            $"rows={rows:N0} ({instRows:N0}/s) | " +
            $"wire={FormatBytes(bytes)} ({instMib:0.0} MiB/s inst, {avgMib:0.0} avg) | " +
            $"pool={idle}/{total} idle/total");
        Console.WriteLine(
            $"           qwp: frames={frames:N0} acks={acks:N0} srvErr={srvErr} " +
            $"reconnect={reconnOk}/{reconn} droppedErr={dropped} | sf_disk={FormatBytes(sfBytes)}");
        Console.WriteLine(
            $"           proc: rss={FormatBytes(ws)} (base {FormatBytes(_baselineWorkingSet)}) " +
            $"gcHeap={FormatBytes(gcMem)} alloc={FormatBytes(totalAlloc)} " +
            $"gc[{GC.CollectionCount(0)}/{GC.CollectionCount(1)}/{GC.CollectionCount(2)}] " +
            $"threads={ThreadPool.ThreadCount} pending={ThreadPool.PendingWorkItemCount}");

        CheckLeak(ws);
    }

    private void CheckLeak(long workingSet)
    {
        _wsHistory.Enqueue(workingSet);
        while (_wsHistory.Count > 6) _wsHistory.Dequeue();

        var growthMib = (workingSet - _baselineWorkingSet) / (1024.0 * 1024);
        if (growthMib < _cfg.LeakWarnMib || _wsHistory.Count < 6) return;

        // Monotonic across the whole window => sustained climb, not a transient spike.
        var monotonic = true;
        long prev = 0;
        foreach (var v in _wsHistory)
        {
            if (prev != 0 && v < prev) monotonic = false;
            prev = v;
        }

        if (monotonic && !_leakWarned)
        {
            Console.WriteLine(
                $"  ** LEAK WARNING: working set grew +{growthMib:0} MiB over baseline and is climbing " +
                "monotonically across the last 6 samples. Capture a heap/handle snapshot now. **");
            _leakWarned = true;
        }
        else if (!monotonic)
        {
            _leakWarned = false;
        }
    }

    private static long DirectorySize(string dir)
    {
        try
        {
            if (!Directory.Exists(dir)) return 0;
            long sum = 0;
            foreach (var f in Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories))
                try { sum += new FileInfo(f).Length; } catch { /* file vanished mid-scan */ }
            return sum;
        }
        catch
        {
            return 0;
        }
    }

    public static string FormatBytes(long b)
    {
        string[] u = { "B", "KiB", "MiB", "GiB", "TiB" };
        double v = b;
        var i = 0;
        while (v >= 1024 && i < u.Length - 1)
        {
            v /= 1024;
            i++;
        }

        return $"{v:0.##} {u[i]}";
    }

    private static string FormatElapsed(TimeSpan t) =>
        $"{(int)t.TotalHours:D2}:{t.Minutes:D2}:{t.Seconds:D2}";
}
