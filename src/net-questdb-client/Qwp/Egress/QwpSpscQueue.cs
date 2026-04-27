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

namespace QuestDB.Qwp.Egress;

/// <summary>
///     Bounded single-producer single-consumer queue with spin-then-park blocking.
///     The .NET counterpart of Java's <c>QwpSpscQueue</c> on java-questdb-client
///     main 64b7ee69.
/// </summary>
/// <remarks>
///     Purpose-built for the QwpEgressIoThread hand-off hot paths in PR 11. Lock-free
///     on the fast path; parks via <see cref="ManualResetEventSlim"/> only after a
///     short spin window so a consumer whose producer arrives within the budget
///     skips park/unpark entirely.
///     <para/>
///     Contract: exactly one producer thread may call <see cref="Offer"/>; exactly
///     one consumer thread may call <see cref="TryPoll"/> or <see cref="Take"/>.
///     Capacity rounds up to the next power of two. Behaviour outside these
///     assumptions is undefined.
/// </remarks>
internal sealed class QwpSpscQueue<T> where T : class
{
    // Tuned for latency-sensitive localhost workloads: long enough to catch a
    // typical round-trip inside the spin window, short enough not to dominate
    // CPU on idle queues. Spin loop exits immediately on every iteration that
    // observes a non-empty ring, so this is an upper bound, not a fixed cost.
    private const int SpinIterations = 2048;

    private readonly int _mask;
    private readonly ManualResetEventSlim _signal = new(initialState: false);
    private readonly T?[] _slots;
    // 0 when consumer running (no signal needed); 1 when parked (producer must Set).
    // Interlocked.Exchange on park/unpark provides full fence so the producer's
    // subsequent read sees the latest value.
    private int _consumerParked;
    private long _head;
    private long _tail;

    public QwpSpscQueue(int capacity)
    {
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be >= 1");
        var pow2 = 1;
        while (pow2 < capacity) pow2 <<= 1;
        _slots = new T?[pow2];
        _mask = pow2 - 1;
    }

    /// <summary>Capacity rounded up to the next power of two.</summary>
    public int Capacity => _slots.Length;

    /// <summary>
    ///     Publishes <paramref name="value"/> to the consumer. Returns false when the
    ///     ring is full. Never blocks.
    /// </summary>
    public bool Offer(T value)
    {
        var h = _head;
        // Producer-only read of head; consumer publishes tail via Volatile.Write.
        if (h - Volatile.Read(ref _tail) >= _slots.Length) return false;
        _slots[(int)(h & _mask)] = value;
        Volatile.Write(ref _head, h + 1); // publish slot to consumer
        // StoreLoad fence: ensure the subsequent _consumerParked read is fresh
        // and not reordered before the head publication.
        Interlocked.MemoryBarrier();
        if (Volatile.Read(ref _consumerParked) == 1)
        {
            _signal.Set();
        }
        return true;
    }

    /// <summary>Non-blocking read. Returns false when the ring is empty.</summary>
    public bool TryPoll(out T value)
    {
        var t = _tail;
        if (t == Volatile.Read(ref _head))
        {
            value = null!;
            return false;
        }
        var idx = (int)(t & _mask);
        value = _slots[idx]!;
        _slots[idx] = null; // release reference
        Volatile.Write(ref _tail, t + 1);
        return true;
    }

    /// <summary>
    ///     Spin-then-park take. Returns the next value; throws
    ///     <see cref="OperationCanceledException"/> if <paramref name="ct"/> is
    ///     cancelled while waiting.
    /// </summary>
    public T Take(CancellationToken ct = default)
    {
        if (TryPoll(out var value)) return value;

        var spinner = new SpinWait();
        for (var i = 0; i < SpinIterations; i++)
        {
            spinner.SpinOnce(sleep1Threshold: -1); // never call Thread.Sleep(1)
            if (TryPoll(out value)) return value;
        }

        // Park: publish ourselves so a subsequent Offer signals us. Re-poll after
        // publishing to close the race where the producer offered between our
        // last poll and the parked-flag publish.
        Interlocked.Exchange(ref _consumerParked, 1); // full fence
        try
        {
            while (true)
            {
                if (TryPoll(out value)) return value;
                ct.ThrowIfCancellationRequested();
                _signal.Reset();
                // Re-poll after Reset to close the Reset/Set race.
                if (TryPoll(out value)) return value;
                _signal.Wait(ct);
            }
        }
        finally
        {
            Interlocked.Exchange(ref _consumerParked, 0);
        }
    }
}
