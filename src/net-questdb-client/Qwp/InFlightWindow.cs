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
 ******************************************************************************/

using System.Threading;
using QuestDB.Enums;
using QuestDB.Utils;

namespace QuestDB.Qwp;

/// <summary>
///     Lock-free sliding-window tracker for in-flight QWP batches awaiting acknowledgment.
///     The .NET counterpart of Java's <c>InFlightWindow</c> on java-questdb-client main
///     64b7ee69. The Java VarHandle / LockSupport pair becomes
///     <see cref="Interlocked"/> for atomic counters and <see cref="ManualResetEventSlim"/>
///     for waiter parking — equivalent semantics, .NET-idiomatic.
/// </summary>
/// <remarks>
///     Experimental. Concurrency model:
///     <list type="bullet">
///         <item>Single producer (the IO thread on async, or the caller on sync) updates
///             <c>highestSent</c> via <see cref="TryAddInFlight"/> /
///             <see cref="AddInFlight"/>.</item>
///         <item>Single consumer (the ack-handling IO thread) updates <c>highestAcked</c>
///             via <see cref="AcknowledgeUpTo"/> — cumulative ack semantics.</item>
///         <item>Optional flush-thread waiter calls <see cref="AwaitEmpty"/> to block
///             until <c>highestAcked == highestSent</c>.</item>
///     </list>
///     <c>fail</c> / <c>failAll</c> latch a terminal error which is rethrown to any waiter.
/// </remarks>
internal sealed class InFlightWindow : IDisposable
{
    public const long DEFAULT_TIMEOUT_MS = 30_000;
    public const int DEFAULT_WINDOW_SIZE = 8;

    private readonly ManualResetEventSlim _spaceEvent = new(false);
    private readonly ManualResetEventSlim _emptyEvent = new(true); // initially empty
    private readonly int _maxWindowSize;
    private readonly long _timeoutMs;

    private long _failedBatchId = -1;
    private long _highestAcked = -1;
    private long _highestSent = -1;
    private long _totalAcked;
    private long _totalFailed;
    private Exception? _lastError;

    public InFlightWindow() : this(DEFAULT_WINDOW_SIZE, DEFAULT_TIMEOUT_MS) { }

    public InFlightWindow(int maxWindowSize, long timeoutMs)
    {
        if (maxWindowSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxWindowSize), "must be positive");
        }
        _maxWindowSize = maxWindowSize;
        _timeoutMs = timeoutMs;
    }

    public int MaxWindowSize => _maxWindowSize;

    public long TimeoutMs => _timeoutMs;

    public int InFlightCount
    {
        get
        {
            var sent = Interlocked.Read(ref _highestSent);
            var acked = Interlocked.Read(ref _highestAcked);
            return (int)Math.Max(0, sent - acked);
        }
    }

    public bool IsEmpty => InFlightCount == 0;

    public bool IsFull => InFlightCount >= _maxWindowSize;

    public bool HasWindowSpace => InFlightCount < _maxWindowSize;

    /// <summary>Highest acknowledged sequence number, or -1 if no ack has been received.</summary>
    public long HighestAckedSequence => Interlocked.Read(ref _highestAcked);

    public long TotalAcked => Interlocked.Read(ref _totalAcked);

    public long TotalFailed => Interlocked.Read(ref _totalFailed);

    public Exception? LastError => Volatile.Read(ref _lastError);

    /// <summary>
    ///     Tries to add <paramref name="batchId"/> to the window without blocking. Lock-free
    ///     under single-producer assumption (only the IO thread or the sync caller calls).
    /// </summary>
    public bool TryAddInFlight(long batchId)
    {
        var sent = Interlocked.Read(ref _highestSent);
        var acked = Interlocked.Read(ref _highestAcked);
        if (sent - acked >= _maxWindowSize) return false;

        Interlocked.Exchange(ref _highestSent, batchId);
        // From empty → non-empty: drop the empty signal so awaitEmpty waiters re-block.
        if (sent == acked) _emptyEvent.Reset();
        return true;
    }

    /// <summary>
    ///     Adds <paramref name="batchId"/> to the window, blocking until space becomes
    ///     available or the timeout elapses. Throws on timeout or latched error.
    /// </summary>
    public void AddInFlight(long batchId)
    {
        CheckError();
        if (TryAddInFlight(batchId)) return;

        var deadline = Environment.TickCount64 + _timeoutMs;
        while (true)
        {
            CheckError();
            if (TryAddInFlight(batchId)) return;

            var remaining = deadline - Environment.TickCount64;
            if (remaining <= 0)
            {
                throw new IngressError(ErrorCode.ServerFlushError,
                    $"Timeout waiting for window space, window full with {InFlightCount} batches");
            }

            // Reset before wait so an Acknowledge that fires before our wait is observed.
            _spaceEvent.Reset();
            // Re-check after reset — Acknowledge may have set the event between TryAdd
            // and Reset, in which case we must not block.
            if (TryAddInFlight(batchId)) return;
            _spaceEvent.Wait(TimeSpan.FromMilliseconds(Math.Min(remaining, 50)));
        }
    }

    /// <summary>
    ///     Cumulative ack: marks all batches with sequence ≤ <paramref name="sequence"/> as
    ///     acknowledged. Returns the number newly acknowledged.
    /// </summary>
    public int AcknowledgeUpTo(long sequence)
    {
        var sent = Interlocked.Read(ref _highestSent);
        if (sent < 0) return 0;

        var effective = Math.Min(sequence, sent);
        var prev = Interlocked.Read(ref _highestAcked);
        if (effective <= prev) return 0;

        Interlocked.Exchange(ref _highestAcked, effective);
        var acknowledged = (int)(effective - prev);
        Interlocked.Add(ref _totalAcked, acknowledged);

        // Wake any waiters: space is freed, and possibly drained to empty.
        _spaceEvent.Set();
        if (InFlightCount == 0) _emptyEvent.Set();
        return acknowledged;
    }

    /// <summary>
    ///     Single-batch ack convenience wrapper. Returns true if the batch was in flight or
    ///     had been previously acked.
    /// </summary>
    public bool Acknowledge(long batchId)
    {
        return AcknowledgeUpTo(batchId) > 0 || HighestAckedSequence >= batchId;
    }

    /// <summary>Blocks until <see cref="InFlightCount"/> reaches 0, or until timeout.</summary>
    public void AwaitEmpty()
    {
        CheckError();
        if (InFlightCount == 0) { CheckError(); return; }

        var deadline = Environment.TickCount64 + _timeoutMs;
        while (InFlightCount > 0)
        {
            CheckError();
            var remaining = deadline - Environment.TickCount64;
            if (remaining <= 0)
            {
                throw new IngressError(ErrorCode.ServerFlushError,
                    $"Timeout waiting for batch acknowledgments, {InFlightCount} batches still in flight");
            }
            _emptyEvent.Wait(TimeSpan.FromMilliseconds(Math.Min(remaining, 50)));
        }
        // Window may have drained while an error is pending — re-check.
        CheckError();
    }

    /// <summary>Latches a terminal error against <paramref name="batchId"/> and wakes waiters.</summary>
    public void Fail(long batchId, Exception error)
    {
        Interlocked.Exchange(ref _failedBatchId, batchId);
        Volatile.Write(ref _lastError, error);
        Interlocked.Increment(ref _totalFailed);
        WakeWaiters();
    }

    /// <summary>
    ///     Marks every currently in-flight batch as failed, advances <c>highestAcked</c> to
    ///     match <c>highestSent</c> so <see cref="InFlightCount"/> drops to 0, and propagates
    ///     <paramref name="error"/> to waiters.
    /// </summary>
    public void FailAll(Exception error)
    {
        var sent = Interlocked.Read(ref _highestSent);
        var acked = Interlocked.Read(ref _highestAcked);

        Volatile.Write(ref _lastError, error);

        if (sent < 0)
        {
            WakeWaiters();
            return;
        }

        var inFlight = Math.Max(0, sent - acked);
        Interlocked.Exchange(ref _failedBatchId, sent);
        Interlocked.Add(ref _totalFailed, inFlight);
        Interlocked.Exchange(ref _highestAcked, sent);

        WakeWaiters();
    }

    public void ClearError()
    {
        Volatile.Write(ref _lastError, null);
        Interlocked.Exchange(ref _failedBatchId, -1);
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _highestSent, -1);
        Interlocked.Exchange(ref _highestAcked, -1);
        Volatile.Write(ref _lastError, null);
        Interlocked.Exchange(ref _failedBatchId, -1);
        WakeWaiters();
    }

    public void Dispose()
    {
        _spaceEvent.Dispose();
        _emptyEvent.Dispose();
    }

    private void CheckError()
    {
        var error = Volatile.Read(ref _lastError);
        if (error is not null)
        {
            var batchId = Interlocked.Read(ref _failedBatchId);
            throw new IngressError(ErrorCode.ServerFlushError,
                $"Batch {batchId} failed: {error.Message}", error);
        }
    }

    private void WakeWaiters()
    {
        _spaceEvent.Set();
        _emptyEvent.Set();
    }
}
