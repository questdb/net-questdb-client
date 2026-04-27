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

using System.Diagnostics;

namespace QuestDB.Qwp;

/// <summary>
///     Buffer state machine for accumulating QWP data into microbatches before sending.
///     The .NET counterpart of Java's <c>MicrobatchBuffer</c> on java-questdb-client main
///     64b7ee69. Storage is a POH-pinned managed array (via
///     <see cref="PinnedAppendBuffer"/>) so the bytes can feed
///     <see cref="System.Net.WebSockets.ClientWebSocket.SendAsync(ReadOnlyMemory{byte}, System.Net.WebSockets.WebSocketMessageType, bool, System.Threading.CancellationToken)"/>
///     directly without bridging code.
/// </summary>
/// <remarks>
///     Experimental. Supports the user-thread / IO-thread hand-over used by the
///     WebSocket sender's double-buffering scheme:
///     <pre>
///         FILLING ──seal()──► SEALED ──MarkSending()──► SENDING
///            ▲                                              │
///            └──────────────MarkRecycled()──────────────────┘
///     </pre>
///     State transitions go through <see cref="Volatile.Read{T}"/> / <see cref="Volatile.Write"/>
///     so each side observes the other's transitions promptly. Concurrent writes from the
///     same side are NOT supported.
/// </remarks>
internal sealed class MicrobatchBuffer : IDisposable
{
    public const int STATE_FILLING = 0;
    public const int STATE_SEALED = 1;
    public const int STATE_SENDING = 2;
    public const int STATE_RECYCLED = 3;

    private static long _nextBatchId;

    private readonly PinnedAppendBuffer _buffer;
    private readonly System.Threading.ManualResetEventSlim _recycledEvent = new(false);

    private long _batchId;
    private long _firstRowTimestampTicks;
    private int _rowCount;
    private int _state = STATE_FILLING;

    public MicrobatchBuffer(int initialCapacity)
    {
        if (initialCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialCapacity), "must be positive");
        }
        _buffer = new PinnedAppendBuffer(initialCapacity);
        _batchId = System.Threading.Interlocked.Increment(ref _nextBatchId) - 1;
    }

    public long BatchId => _batchId;

    public int State => System.Threading.Volatile.Read(ref _state);

    public bool IsFilling => State == STATE_FILLING;

    public bool IsSealed => State == STATE_SEALED;

    public bool IsSending => State == STATE_SENDING;

    public bool IsRecycled => State == STATE_RECYCLED;

    public bool IsInUse
    {
        get { var s = State; return s == STATE_SEALED || s == STATE_SENDING; }
    }

    public bool HasData => _buffer.Length > 0;

    public int BufferPos => _buffer.Length;

    public int BufferCapacity => _buffer.Capacity;

    public int RowCount => _rowCount;

    /// <summary>
    ///     Returns the duration since the first row was added, or <see cref="TimeSpan.Zero"/>
    ///     when no rows are buffered yet.
    /// </summary>
    public TimeSpan Age
    {
        get
        {
            if (_rowCount == 0) return TimeSpan.Zero;
            var elapsed = Stopwatch.GetTimestamp() - _firstRowTimestampTicks;
            return TimeSpan.FromSeconds((double)elapsed / Stopwatch.Frequency);
        }
    }

    public ReadOnlyMemory<byte> AsReadOnlyMemory() => _buffer.AsReadOnlyMemory(0, _buffer.Length);

    public ReadOnlySpan<byte> AsReadOnlySpan() => _buffer.AsReadOnlySpan(0, _buffer.Length);

    /// <summary>
    ///     Returns the underlying <see cref="PinnedAppendBuffer"/>. Used by encoders that
    ///     drive the buffer directly (bypassing <see cref="Write"/> / <see cref="WriteByte"/>).
    ///     Only valid in <see cref="STATE_FILLING"/>.
    /// </summary>
    internal PinnedAppendBuffer GetUnderlyingBufferForFilling()
    {
        var s = State;
        if (s != STATE_FILLING)
        {
            throw new InvalidOperationException($"buffer state is {StateName(s)}; cannot drive directly");
        }
        return _buffer;
    }

    public void EnsureCapacity(int requiredTotalCapacity)
    {
        var s = State;
        if (s != STATE_FILLING)
        {
            throw new InvalidOperationException($"Cannot resize when state is {StateName(s)}");
        }
        if (requiredTotalCapacity > _buffer.Capacity)
        {
            // PinnedAppendBuffer's grow takes a delta beyond Length; convert.
            var additional = requiredTotalCapacity - _buffer.Length;
            if (additional > 0) _buffer.EnsureCapacityFor(additional);
        }
    }

    public void IncrementRowCount()
    {
        var s = State;
        if (s != STATE_FILLING)
        {
            throw new InvalidOperationException($"Cannot increment row count when state is {StateName(s)}");
        }
        if (_rowCount == 0) _firstRowTimestampTicks = Stopwatch.GetTimestamp();
        _rowCount++;
    }

    public void Write(ReadOnlySpan<byte> source)
    {
        var s = State;
        if (s != STATE_FILLING)
        {
            throw new InvalidOperationException($"Cannot write when state is {StateName(s)}");
        }
        _buffer.PutBlockOfBytes(source);
    }

    public void WriteByte(byte value)
    {
        var s = State;
        if (s != STATE_FILLING)
        {
            throw new InvalidOperationException($"Cannot write when state is {StateName(s)}");
        }
        _buffer.PutByte(value);
    }

    public void SetBufferPos(int pos)
    {
        var s = State;
        if (s != STATE_FILLING)
        {
            throw new InvalidOperationException($"Cannot set position when state is {StateName(s)}");
        }
        if (pos < 0 || pos > _buffer.Capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(pos), $"Position out of bounds: {pos}");
        }
        _buffer.JumpTo(pos);
    }

    /// <summary>FILLING → SEALED. User thread.</summary>
    public void Seal() => Transition(STATE_FILLING, STATE_SEALED, "seal");

    /// <summary>SEALED → FILLING (rollback when enqueue fails).</summary>
    public void RollbackSealForRetry() => Transition(STATE_SEALED, STATE_FILLING, "rollback seal");

    /// <summary>SEALED → SENDING. IO thread.</summary>
    public void MarkSending() => Transition(STATE_SEALED, STATE_SENDING, "mark sending");

    /// <summary>SENDING → RECYCLED. IO thread. Wakes any user-thread <see cref="AwaitRecycled()"/>.</summary>
    public void MarkRecycled()
    {
        Transition(STATE_SENDING, STATE_RECYCLED, "mark recycled");
        _recycledEvent.Set();
    }

    /// <summary>RECYCLED (or fresh FILLING) → FILLING with cleared state. New batch id.</summary>
    public void Reset()
    {
        var s = State;
        if (s == STATE_SEALED || s == STATE_SENDING)
        {
            throw new InvalidOperationException($"Cannot reset buffer in state {StateName(s)}");
        }
        _buffer.Truncate();
        _rowCount = 0;
        _firstRowTimestampTicks = 0;
        _batchId = System.Threading.Interlocked.Increment(ref _nextBatchId) - 1;
        _recycledEvent.Reset();
        System.Threading.Volatile.Write(ref _state, STATE_FILLING);
    }

    /// <summary>Blocks the calling thread until the buffer reaches <see cref="STATE_RECYCLED"/>.</summary>
    public void AwaitRecycled() => _recycledEvent.Wait();

    /// <summary>Waits up to <paramref name="timeout"/>. Returns <c>true</c> on recycled, <c>false</c> on timeout.</summary>
    public bool AwaitRecycled(TimeSpan timeout) => _recycledEvent.Wait(timeout);

    public void Dispose() => _recycledEvent.Dispose();

    public override string ToString() =>
        $"MicrobatchBuffer{{batchId={_batchId}, state={StateName(State)}, rows={_rowCount}, bytes={_buffer.Length}, capacity={_buffer.Capacity}}}";

    public static string StateName(int state) => state switch
    {
        STATE_FILLING => "FILLING",
        STATE_SEALED => "SEALED",
        STATE_SENDING => "SENDING",
        STATE_RECYCLED => "RECYCLED",
        _ => $"UNKNOWN({state})",
    };

    private void Transition(int expected, int next, string action)
    {
        var current = System.Threading.Volatile.Read(ref _state);
        if (current != expected)
        {
            throw new InvalidOperationException(
                $"Cannot {action} in state {StateName(current)} (expected {StateName(expected)})");
        }
        System.Threading.Volatile.Write(ref _state, next);
    }
}
