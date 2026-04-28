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

using NUnit.Framework;
using QuestDB.Qwp.Sf;

namespace net_questdb_client_tests.Qwp.Sf;

[TestFixture]
public class QwpBackgroundDrainerPoolTests
{
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "qwp-pool-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Test]
    public void Constructor_NonPositiveConcurrency_Throws()
    {
        var drainer = new SuccessDrainer();
        Assert.Throws<ArgumentOutOfRangeException>(() => new QwpBackgroundDrainerPool(0, drainer));
        Assert.Throws<ArgumentOutOfRangeException>(() => new QwpBackgroundDrainerPool(-1, drainer));
    }

    [Test]
    public async Task Enqueue_RunsDrainAndReleasesLock()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new SuccessDrainer();

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(drainer.Drained, Has.Member(slotDir));
        // Lock must have been disposed → can re-acquire.
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));
        // No .failed sentinel on success.
        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.False);
    }

    [Test]
    public async Task Enqueue_DrainerThrows_DropsFailedSentinelAndReleasesLock()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new ThrowingDrainer(new InvalidOperationException("boom"));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.True);
        var sentinel = await File.ReadAllTextAsync(Path.Combine(slotDir, ".failed"));
        Assert.That(sentinel, Does.Contain("boom"));
        // Lock released even after failure.
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));
    }

    [Test]
    public async Task Enqueue_RespectsConcurrencyCap()
    {
        const int cap = 2;
        const int submissions = 6;

        var drainer = new GatedDrainer();
        using var pool = new QwpBackgroundDrainerPool(cap, drainer);

        var locks = new List<QwpSlotLock>();
        for (var i = 0; i < submissions; i++)
        {
            locks.Add(QwpSlotLock.Acquire(Path.Combine(_root, $"slot-{i}")));
        }

        foreach (var l in locks)
        {
            pool.Enqueue(l);
        }

        // Wait until exactly `cap` drains are in flight, then verify the rest are queued.
        await drainer.WaitForInFlightAsync(cap);
        await Task.Delay(50);
        Assert.That(drainer.PeakInFlight, Is.LessThanOrEqualTo(cap));
        Assert.That(drainer.InFlight, Is.EqualTo(cap));

        drainer.ReleaseAll();
        await pool.WaitForAllAsync();

        Assert.That(drainer.PeakInFlight, Is.LessThanOrEqualTo(cap));
        Assert.That(drainer.CompletedCount, Is.EqualTo(submissions));
    }

    [Test]
    public async Task Enqueue_CooperativelyCancelled_ReleasesLockWithoutSentinel()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new GatedDrainer();
        using var cts = new CancellationTokenSource();

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock, cts.Token);
        await drainer.WaitForInFlightAsync(1);

        cts.Cancel();
        drainer.ReleaseAll();

        try { await pool.WaitForAllAsync(); }
        catch (OperationCanceledException) { /* expected */ }

        // Cancellation must not drop a sentinel — the next sender startup retries the slot.
        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.False);
        // Lock is released.
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));
    }

    [Test]
    public async Task ConcurrentEnqueue_AllDrainsComplete_NoBookkeepingRace()
    {
        const int slotCount = 64;
        const int concurrency = 4;

        var drainer = new SuccessDrainer();
        using var pool = new QwpBackgroundDrainerPool(concurrency, drainer);

        var locks = Enumerable.Range(0, slotCount)
            .Select(i => QwpSlotLock.Acquire(Path.Combine(_root, $"slot-{i}")))
            .ToList();

        Parallel.ForEach(locks, l => pool.Enqueue(l));
        await pool.WaitForAllAsync();

        Assert.That(drainer.Drained, Has.Count.EqualTo(slotCount));
        Assert.That(drainer.Drained.ToHashSet(), Has.Count.EqualTo(slotCount),
            "every slot must be drained exactly once");
    }

    [Test]
    public void Enqueue_AfterDispose_Throws()
    {
        var pool = new QwpBackgroundDrainerPool(2, new SuccessDrainer());
        pool.Dispose();

        var slotLock = QwpSlotLock.Acquire(Path.Combine(_root, "slot"));
        try
        {
            Assert.Throws<ObjectDisposedException>(() => pool.Enqueue(slotLock));
        }
        finally
        {
            slotLock.Dispose();
        }
    }

    private sealed class SuccessDrainer : IQwpSlotDrainer
    {
        private readonly List<string> _drained = new();
        private readonly object _lock = new();

        public IReadOnlyList<string> Drained
        {
            get
            {
                lock (_lock) { return _drained.ToArray(); }
            }
        }

        public Task DrainAsync(string slotDirectory, CancellationToken cancellationToken)
        {
            lock (_lock) { _drained.Add(slotDirectory); }
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingDrainer : IQwpSlotDrainer
    {
        private readonly Exception _ex;
        public ThrowingDrainer(Exception ex) => _ex = ex;
        public Task DrainAsync(string slotDirectory, CancellationToken cancellationToken) => Task.FromException(_ex);
    }

    private sealed class GatedDrainer : IQwpSlotDrainer
    {
        private readonly TaskCompletionSource<bool> _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _inFlight;
        private int _peakInFlight;
        private int _completedCount;

        public int InFlight => Volatile.Read(ref _inFlight);
        public int PeakInFlight => Volatile.Read(ref _peakInFlight);
        public int CompletedCount => Volatile.Read(ref _completedCount);

        public async Task DrainAsync(string slotDirectory, CancellationToken cancellationToken)
        {
            var current = Interlocked.Increment(ref _inFlight);
            UpdatePeak(current);
            try
            {
                await _gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _completedCount);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        public void ReleaseAll() => _gate.TrySetResult(true);

        public async Task WaitForInFlightAsync(int target)
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (Volatile.Read(ref _inFlight) < target)
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"only {InFlight} of {target} drains in flight");
                }

                await Task.Delay(10);
            }
        }

        private void UpdatePeak(int current)
        {
            int peak;
            do
            {
                peak = Volatile.Read(ref _peakInFlight);
                if (current <= peak)
                {
                    return;
                }
            }
            while (Interlocked.CompareExchange(ref _peakInFlight, current, peak) != peak);
        }
    }
}
