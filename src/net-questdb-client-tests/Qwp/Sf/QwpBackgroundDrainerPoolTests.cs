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
using QuestDB.Enums;
using QuestDB.Qwp;
using QuestDB.Qwp.Sf;
using QuestDB.Senders;
using QuestDB.Utils;

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
    public async Task Enqueue_ReplayImpossibleError_DropsFailedSentinelAndReleasesLock()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new ThrowingDrainer(new QwpException(QwpStatusCode.SchemaMismatch, sequence: 0, message: "schema-mismatch"));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.True);
        var sentinel = await File.ReadAllTextAsync(Path.Combine(slotDir, ".failed"));
        Assert.That(sentinel, Does.Contain("schema-mismatch"));
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));
    }

    [Test]
    public async Task Enqueue_ServerFlushError_DropsFailedSentinel()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new ThrowingDrainer(new IngressError(ErrorCode.ServerFlushError, "drain timeout"));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.True);
    }

    [Test]
    public async Task Enqueue_GenericException_DropsFailedSentinel()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new ThrowingDrainer(new InvalidOperationException("transport-glitch"));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.True);
    }

    [Test]
    public async Task Enqueue_QwpAuthError_DropsFailedSentinel()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new ThrowingDrainer(new QwpException(QwpStatusCode.SecurityError, 0, "auth"));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.True);
    }

    [Test]
    public async Task Enqueue_DrainTimeout_LeavesNoSentinelAndReleasesLockForReadoption()
    {
        // A drain that exceeds its budget throws TimeoutException (QwpCursorSendEngine.FlushAsync).
        // This is transient — a slow/unreachable server or a backlog larger than the window — so the
        // orphan's recoverable frames must NOT be permanently quarantined with a .failed sentinel.
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new ThrowingDrainer(new TimeoutException(
            "close_flush_timeout (5000 ms) expired with un-acked frames pending"));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.False,
            "a transient drain timeout must not write a permanent .failed sentinel");
        // Lock released → QwpOrphanScanner can re-adopt the slot on a later sweep.
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));
    }

    [Test]
    public async Task Enqueue_ReconnectBudgetExhaustedFromTransientOutage_LeavesNoSentinel()
    {
        // Reconnect-budget exhaustion re-wraps the connectivity cause as IngressError(..., inner)
        // (QwpCursorSendEngine.WrapTerminalForProducer). A non-terminal inner means the outage was
        // transient → retryable, so no sentinel.
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var transientCause = new IOException("connection reset by peer");
        var drainer = new ThrowingDrainer(new IngressError(ErrorCode.ServerFlushError,
            "QWP cursor engine has terminally failed; see inner exception", transientCause));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.False,
            "a transient outage that exhausted the reconnect budget must not quarantine the slot");
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));
    }

    [Test]
    public async Task Enqueue_WrappedDeterministicTerminal_DropsFailedSentinel()
    {
        // A deterministic terminal (here auth) is wrapped the same way, but its inner IS terminal —
        // a retry will fail identically, so the slot is quarantined.
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var terminalCause = new IngressError(ErrorCode.AuthError, "401 unauthorized");
        var drainer = new ThrowingDrainer(new IngressError(ErrorCode.AuthError,
            "QWP cursor engine has terminally failed; see inner exception", terminalCause));

        using var pool = new QwpBackgroundDrainerPool(2, drainer);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.True);
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
    public void Dispose_WedgedDrainer_StillReleasesSlotLock()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new GatedDrainer();

        var pool = new QwpBackgroundDrainerPool(2, drainer, shutdownWait: TimeSpan.FromMilliseconds(50));
        pool.Enqueue(slotLock);

        pool.Dispose();

        // Slot lock must be released even though the drainer never returned.
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));

        drainer.ReleaseAll();
    }

    [Test]
    public async Task Dispose_WedgedDrainerThrowsLater_NoSentinelWrittenToEvictedSlot()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new GatedThrowingDrainer();
        var pool = new QwpBackgroundDrainerPool(2, drainer, shutdownWait: TimeSpan.FromMilliseconds(50));
        pool.Enqueue(slotLock);
        Assert.That(drainer.DrainStarted.Wait(TimeSpan.FromSeconds(2)), Is.True);

        pool.Dispose();
        using var newOwner = QwpSlotLock.Acquire(slotDir);

        drainer.AllowThrow.Set();
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (drainer.Completed.CurrentCount > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.False);
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

    // ---- DrainerListener (WP7 / M5) observability -----------------------------------------------
    // The outcome-path tests above assert only side effects (.failed sentinel presence, lock release).
    // These pin the emitted BackgroundDrainerEvent stream itself — kind sequence, wired Cause, and
    // SlotDirectory — for every terminal outcome, so a mis-wired kind/cause can't ship undetected.

    [Test]
    public async Task Enqueue_Success_EmitsSlotAdoptedThenDrainCompleted()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var listener = new RecordingDrainerListener();

        using var pool = new QwpBackgroundDrainerPool(2, new SuccessDrainer(), listener: listener);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        var events = listener.Events;
        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            BackgroundDrainerEventKind.SlotAdopted,
            BackgroundDrainerEventKind.DrainCompleted,
        }), "success emits adoption then completion, in that order");
        Assert.That(events.All(e => e.SlotDirectory == slotDir), Is.True);
        Assert.That(events.All(e => e.Cause is null), Is.True, "adopted/completed carry no cause");
        Assert.That(events.All(e => e.Timestamp != default), Is.True);
    }

    [Test]
    public async Task Enqueue_TransientFault_EmitsDrainRetryingWithCause_AndNoSentinel()
    {
        // TimeoutException is a retryable drain fault: DrainRetrying, no .failed sentinel, cause wired.
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var cause = new TimeoutException("close_flush_timeout (5000 ms) expired with un-acked frames pending");
        var listener = new RecordingDrainerListener();

        using var pool = new QwpBackgroundDrainerPool(2, new ThrowingDrainer(cause), listener: listener);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        var events = listener.Events;
        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            BackgroundDrainerEventKind.SlotAdopted,
            BackgroundDrainerEventKind.DrainRetrying,
        }), "a transient fault emits adoption then DrainRetrying (never Quarantined)");

        var terminal = events[^1];
        Assert.That(terminal.Cause, Is.SameAs(cause), "the transient fault must be surfaced as the event cause");
        Assert.That(terminal.SlotDirectory, Is.EqualTo(slotDir));
        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.False,
            "DrainRetrying must not accompany a permanent sentinel");
    }

    [Test]
    public async Task Enqueue_DeterministicTerminal_EmitsDrainQuarantinedWithCause_AndSentinel()
    {
        // A schema-mismatch is a deterministic terminal: DrainQuarantined + .failed sentinel, cause wired.
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var cause = new QwpException(QwpStatusCode.SchemaMismatch, sequence: 0, message: "schema-mismatch");
        var listener = new RecordingDrainerListener();

        using var pool = new QwpBackgroundDrainerPool(2, new ThrowingDrainer(cause), listener: listener);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        var events = listener.Events;
        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            BackgroundDrainerEventKind.SlotAdopted,
            BackgroundDrainerEventKind.DrainQuarantined,
        }), "a deterministic terminal emits adoption then DrainQuarantined (never Retrying)");

        var terminal = events[^1];
        Assert.That(terminal.Cause, Is.SameAs(cause), "the terminal fault must be surfaced as the event cause");
        Assert.That(terminal.SlotDirectory, Is.EqualTo(slotDir));
        Assert.That(File.Exists(Path.Combine(slotDir, ".failed")), Is.True,
            "DrainQuarantined must accompany the .failed sentinel");
    }

    [Test]
    public async Task Enqueue_CooperativelyCancelled_EmitsSlotAdoptedThenDrainCancelled()
    {
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new GatedDrainer();
        var listener = new RecordingDrainerListener();
        using var cts = new CancellationTokenSource();

        using var pool = new QwpBackgroundDrainerPool(2, drainer, listener: listener);
        pool.Enqueue(slotLock, cts.Token);
        await drainer.WaitForInFlightAsync(1); // drain is parked inside DrainAsync → SlotAdopted already fired.

        // Cancel WITHOUT releasing the gate: the only way out of the wait is cancellation, so the
        // outcome is deterministically DrainCancelled (never a racy DrainCompleted).
        cts.Cancel();

        try { await pool.WaitForAllAsync(); }
        catch (OperationCanceledException) { /* expected — the drain task faults with OCE */ }

        var events = listener.Events;
        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            BackgroundDrainerEventKind.SlotAdopted,
            BackgroundDrainerEventKind.DrainCancelled,
        }), "cancellation of a running drain emits adoption then DrainCancelled");
        Assert.That(events[^1].Cause, Is.Null, "cancellation carries no cause");
        Assert.That(events[^1].SlotDirectory, Is.EqualTo(slotDir));
    }

    [Test]
    public async Task Enqueue_CancelledBeforeSlotAcquired_EmitsNothingForThatSlot()
    {
        // Adoption is signalled on the worker thread only once the drain actually starts (after the
        // concurrency semaphore is acquired). A slot cancelled while still queued must emit no events.
        var drainer = new GatedDrainer();
        var listener = new RecordingDrainerListener();
        using var pool = new QwpBackgroundDrainerPool(1, drainer, listener: listener); // single slot

        var slotA = Path.Combine(_root, "slot-a");
        var slotB = Path.Combine(_root, "slot-b");
        var lockA = QwpSlotLock.Acquire(slotA);
        var lockB = QwpSlotLock.Acquire(slotB);
        using var ctsB = new CancellationTokenSource();

        pool.Enqueue(lockA); // A takes the only slot and parks in DrainAsync.
        await drainer.WaitForInFlightAsync(1);
        pool.Enqueue(lockB, ctsB.Token); // B blocks in _slots.WaitAsync — no slot free.
        ctsB.Cancel(); // B is cancelled while queued, before it ever adopts.

        drainer.ReleaseAll(); // let A finish cleanly.
        try { await pool.WaitForAllAsync(); }
        catch (OperationCanceledException) { /* B faulted with OCE from the queued wait */ }

        var events = listener.Events;
        Assert.That(events.Any(e => e.SlotDirectory == slotB), Is.False,
            "a slot cancelled before its drain started must emit no events");
        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            BackgroundDrainerEventKind.SlotAdopted,
            BackgroundDrainerEventKind.DrainCompleted,
        }), "only the slot that actually ran (A) is reported");
        Assert.That(events.All(e => e.SlotDirectory == slotA), Is.True);
    }

    [Test]
    public async Task Enqueue_NoListener_DrainStillCompletes()
    {
        // The listener is optional; a null listener must not perturb the drain outcome.
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var drainer = new SuccessDrainer();

        using var pool = new QwpBackgroundDrainerPool(2, drainer, listener: null);
        pool.Enqueue(slotLock);
        await pool.WaitForAllAsync();

        Assert.That(drainer.Drained, Has.Member(slotDir));
    }

    [Test]
    public async Task Enqueue_ThrowingListener_IsContained_AndDrainStillReleasesLock()
    {
        // A slow/throwing listener must never fault the drain or leak out of RunDrainAsync.
        var slotDir = Path.Combine(_root, "slot");
        var slotLock = QwpSlotLock.Acquire(slotDir);
        var listener = new ThrowingDrainerListener();

        using var pool = new QwpBackgroundDrainerPool(2, new SuccessDrainer(), listener: listener);
        pool.Enqueue(slotLock);
        Assert.DoesNotThrowAsync(async () => await pool.WaitForAllAsync());

        Assert.That(listener.InvocationCount, Is.EqualTo(2), "both adoption and completion still fired");
        // Drain succeeded despite the throwing listener → lock released, re-acquirable.
        using var reacquired = QwpSlotLock.Acquire(slotDir);
        Assert.That(reacquired.SlotDirectory, Is.EqualTo(slotDir));
    }

    private sealed class RecordingDrainerListener : IBackgroundDrainerListener
    {
        private readonly List<BackgroundDrainerEvent> _events = new();
        private readonly object _lock = new();

        public IReadOnlyList<BackgroundDrainerEvent> Events
        {
            get
            {
                lock (_lock) { return _events.ToArray(); }
            }
        }

        public void OnEvent(BackgroundDrainerEvent evt)
        {
            lock (_lock) { _events.Add(evt); }
        }
    }

    private sealed class ThrowingDrainerListener : IBackgroundDrainerListener
    {
        private int _invocationCount;

        public int InvocationCount => Volatile.Read(ref _invocationCount);

        public void OnEvent(BackgroundDrainerEvent evt)
        {
            Interlocked.Increment(ref _invocationCount);
            throw new InvalidOperationException("listener blew up");
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

    private sealed class GatedThrowingDrainer : IQwpSlotDrainer
    {
        public ManualResetEventSlim DrainStarted { get; } = new();
        public ManualResetEventSlim AllowThrow { get; } = new();
        public CountdownEvent Completed { get; } = new(1);

        public Task DrainAsync(string slotDirectory, CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                DrainStarted.Set();
                AllowThrow.Wait();
                try
                {
                    throw new InvalidOperationException("wedged drainer woke up and threw");
                }
                finally
                {
                    Completed.Signal();
                }
            });
        }
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
