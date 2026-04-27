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

using NUnit.Framework;
using QuestDB.Qwp;

namespace net_questdb_client_tests.Qwp.Client;

/// <summary>
///     Mirrors <c>MicrobatchBufferTest.java</c> on Java main 64b7ee69. Java uses
///     <c>LockSupport.park</c>; the .NET equivalent is <see cref="System.Threading.ManualResetEventSlim"/>,
///     so the recycle-await tests are recast around it.
/// </summary>
[TestFixture]
public class MicrobatchBufferTests
{
    [Test]
    public void InitialState()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(b.IsFilling, Is.True);
        Assert.That(b.State, Is.EqualTo(MicrobatchBuffer.STATE_FILLING));
        Assert.That(b.RowCount, Is.EqualTo(0));
        Assert.That(b.BufferPos, Is.EqualTo(0));
        Assert.That(b.HasData, Is.False);
        Assert.That(b.IsInUse, Is.False);
    }

    [Test]
    public void ConstructionWithDefaultThresholds()
    {
        using var b = new MicrobatchBuffer(128);
        Assert.That(b.BufferCapacity, Is.EqualTo(128));
    }

    [Test]
    public void ConstructionWithZeroCapacity() =>
        Assert.That(() => new MicrobatchBuffer(0), Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void ConstructionWithNegativeCapacity() =>
        Assert.That(() => new MicrobatchBuffer(-1), Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void StateName()
    {
        Assert.That(MicrobatchBuffer.StateName(MicrobatchBuffer.STATE_FILLING), Is.EqualTo("FILLING"));
        Assert.That(MicrobatchBuffer.StateName(MicrobatchBuffer.STATE_SEALED), Is.EqualTo("SEALED"));
        Assert.That(MicrobatchBuffer.StateName(MicrobatchBuffer.STATE_SENDING), Is.EqualTo("SENDING"));
        Assert.That(MicrobatchBuffer.StateName(MicrobatchBuffer.STATE_RECYCLED), Is.EqualTo("RECYCLED"));
        Assert.That(MicrobatchBuffer.StateName(99), Does.Contain("UNKNOWN"));
    }

    [Test]
    public void ToStringIncludesState()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(b.ToString(), Does.Contain("FILLING"));
        b.Seal();
        Assert.That(b.ToString(), Does.Contain("SEALED"));
    }

    // ---- State transitions ----

    [Test]
    public void SealTransition()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        Assert.That(b.IsSealed, Is.True);
        Assert.That(b.IsInUse, Is.True);
    }

    [Test]
    public void SealWhenNotFilling()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        Assert.That(() => b.Seal(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void RollbackSealForRetry()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        b.RollbackSealForRetry();
        Assert.That(b.IsFilling, Is.True);
    }

    [Test]
    public void RollbackSealWhenNotSealed()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(() => b.RollbackSealForRetry(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void MarkSendingTransition()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        b.MarkSending();
        Assert.That(b.IsSending, Is.True);
        Assert.That(b.IsInUse, Is.True);
    }

    [Test]
    public void MarkSendingWhenNotSealed()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(() => b.MarkSending(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void MarkRecycledTransition()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        b.MarkSending();
        b.MarkRecycled();
        Assert.That(b.IsRecycled, Is.True);
        Assert.That(b.IsInUse, Is.False);
    }

    [Test]
    public void MarkRecycledWhenNotSending()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(() => b.MarkRecycled(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void ResetFromRecycled()
    {
        using var b = new MicrobatchBuffer(64);
        b.WriteByte(0x42);
        b.IncrementRowCount();
        b.Seal();
        b.MarkSending();
        b.MarkRecycled();

        b.Reset();

        Assert.That(b.IsFilling, Is.True);
        Assert.That(b.RowCount, Is.EqualTo(0));
        Assert.That(b.BufferPos, Is.EqualTo(0));
    }

    [Test]
    public void ResetWhenSealed()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        Assert.That(() => b.Reset(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void ResetWhenSending()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        b.MarkSending();
        Assert.That(() => b.Reset(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void FullStateLifecycle()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(b.IsFilling, Is.True);
        b.WriteByte(1);
        b.IncrementRowCount();
        b.Seal(); Assert.That(b.IsSealed, Is.True);
        b.MarkSending(); Assert.That(b.IsSending, Is.True);
        b.MarkRecycled(); Assert.That(b.IsRecycled, Is.True);
        b.Reset(); Assert.That(b.IsFilling, Is.True);
    }

    // ---- Capacity / write ----

    [Test]
    public void EnsureCapacityNoGrowth()
    {
        using var b = new MicrobatchBuffer(128);
        b.EnsureCapacity(100);
        Assert.That(b.BufferCapacity, Is.EqualTo(128));
    }

    [Test]
    public void EnsureCapacityGrows()
    {
        using var b = new MicrobatchBuffer(8);
        b.EnsureCapacity(64);
        Assert.That(b.BufferCapacity, Is.GreaterThanOrEqualTo(64));
    }

    [Test]
    public void WriteByte()
    {
        using var b = new MicrobatchBuffer(64);
        b.WriteByte(0x42);
        Assert.That(b.BufferPos, Is.EqualTo(1));
        Assert.That(b.AsReadOnlySpan()[0], Is.EqualTo((byte)0x42));
    }

    [Test]
    public void WriteMultipleBytes()
    {
        using var b = new MicrobatchBuffer(64);
        var src = new byte[] { 1, 2, 3, 4, 5 };
        b.Write(src);
        Assert.That(b.BufferPos, Is.EqualTo(5));
        Assert.That(b.AsReadOnlySpan().SequenceEqual(src), Is.True);
    }

    [Test]
    public void WriteFromNativeMemory()
    {
        // .NET equivalent of Java's "write from native ptr": we use a managed Span source.
        using var b = new MicrobatchBuffer(64);
        Span<byte> source = stackalloc byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
        b.Write(source);
        Assert.That(b.BufferPos, Is.EqualTo(4));
        Assert.That(b.AsReadOnlySpan().SequenceEqual(source), Is.True);
    }

    [Test]
    public void WriteBeyondInitialCapacity()
    {
        using var b = new MicrobatchBuffer(8);
        var src = new byte[100];
        for (var i = 0; i < src.Length; i++) src[i] = (byte)(i & 0xFF);
        b.Write(src);
        Assert.That(b.BufferPos, Is.EqualTo(100));
        Assert.That(b.BufferCapacity, Is.GreaterThanOrEqualTo(100));
        Assert.That(b.AsReadOnlySpan().SequenceEqual(src), Is.True);
    }

    [Test]
    public void WriteWhenSealed()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        Assert.That(() => b.WriteByte(0), Throws.TypeOf<InvalidOperationException>());
        Assert.That(() => b.Write(new byte[] { 1 }), Throws.TypeOf<InvalidOperationException>());
    }

    // ---- SetBufferPos ----

    [Test]
    public void SetBufferPos()
    {
        using var b = new MicrobatchBuffer(64);
        b.WriteByte(1); b.WriteByte(2); b.WriteByte(3);
        b.SetBufferPos(1);
        Assert.That(b.BufferPos, Is.EqualTo(1));
    }

    [Test]
    public void SetBufferPosNegative()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(() => b.SetBufferPos(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void SetBufferPosOutOfBounds()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(() => b.SetBufferPos(65), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    // ---- Row tracking ----

    [Test]
    public void IncrementRowCount()
    {
        using var b = new MicrobatchBuffer(64);
        b.IncrementRowCount();
        b.IncrementRowCount();
        Assert.That(b.RowCount, Is.EqualTo(2));
    }

    [Test]
    public void IncrementRowCountWhenSealed()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        Assert.That(() => b.IncrementRowCount(), Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void FirstRowTimeIsRecorded()
    {
        using var b = new MicrobatchBuffer(64);
        Assert.That(b.Age, Is.EqualTo(TimeSpan.Zero));
        b.IncrementRowCount();
        // Age strictly positive once a row exists; sleep briefly to avoid 0-tick race.
        System.Threading.Thread.Sleep(5);
        Assert.That(b.Age, Is.GreaterThan(TimeSpan.Zero));
    }

    // ---- Batch IDs ----

    [Test]
    public void BatchIdIncrementsOnReset()
    {
        using var b = new MicrobatchBuffer(64);
        var firstId = b.BatchId;
        b.Seal();
        b.MarkSending();
        b.MarkRecycled();
        b.Reset();
        Assert.That(b.BatchId, Is.GreaterThan(firstId));
    }

    [Test]
    public void ConcurrentBatchIdUniqueness()
    {
        // Ten threads constructing a buffer concurrently — every batchId must be unique.
        const int threadCount = 10;
        const int perThread = 50;
        var ids = new System.Collections.Concurrent.ConcurrentBag<long>();
        var tasks = new System.Threading.Tasks.Task[threadCount];
        for (var t = 0; t < threadCount; t++)
        {
            tasks[t] = System.Threading.Tasks.Task.Run(() =>
            {
                for (var i = 0; i < perThread; i++)
                {
                    using var b = new MicrobatchBuffer(8);
                    ids.Add(b.BatchId);
                }
            });
        }
        System.Threading.Tasks.Task.WaitAll(tasks);
        Assert.That(ids.Count, Is.EqualTo(threadCount * perThread));
        Assert.That(ids.Distinct().Count(), Is.EqualTo(ids.Count));
    }

    [Test]
    public void ConcurrentResetBatchIdUniqueness()
    {
        // Multiple buffers reset concurrently; each Reset() must produce a unique new id.
        const int bufferCount = 10;
        const int resetsPerBuffer = 30;
        var buffers = new MicrobatchBuffer[bufferCount];
        for (var i = 0; i < bufferCount; i++) buffers[i] = new MicrobatchBuffer(8);

        var ids = new System.Collections.Concurrent.ConcurrentBag<long>();
        var tasks = new System.Threading.Tasks.Task[bufferCount];
        for (var t = 0; t < bufferCount; t++)
        {
            var b = buffers[t];
            tasks[t] = System.Threading.Tasks.Task.Run(() =>
            {
                for (var r = 0; r < resetsPerBuffer; r++)
                {
                    b.Seal();
                    b.MarkSending();
                    b.MarkRecycled();
                    b.Reset();
                    ids.Add(b.BatchId);
                }
            });
        }
        System.Threading.Tasks.Task.WaitAll(tasks);
        foreach (var b in buffers) b.Dispose();
        Assert.That(ids.Distinct().Count(), Is.EqualTo(ids.Count));
    }

    [Test]
    public void ConcurrentStateTransitions()
    {
        // User thread fills + seals; IO thread marks sending + recycled; user thread resets.
        // A loop validates the state machine doesn't go backwards under contention.
        using var b = new MicrobatchBuffer(64);
        const int rounds = 200;
        for (var r = 0; r < rounds; r++)
        {
            b.WriteByte((byte)(r & 0xFF));
            b.Seal();
            var ioTask = System.Threading.Tasks.Task.Run(() =>
            {
                b.MarkSending();
                b.MarkRecycled();
            });
            ioTask.Wait();
            b.Reset();
        }
        Assert.That(b.IsFilling, Is.True);
    }

    // ---- AwaitRecycled ----

    [Test]
    public void AwaitRecycled()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        b.MarkSending();
        var ioTask = System.Threading.Tasks.Task.Run(() =>
        {
            System.Threading.Thread.Sleep(50);
            b.MarkRecycled();
        });
        b.AwaitRecycled(); // blocks until ioTask completes
        ioTask.Wait();
        Assert.That(b.IsRecycled, Is.True);
    }

    [Test]
    public void AwaitRecycledWithTimeout()
    {
        using var b = new MicrobatchBuffer(64);
        b.Seal();
        b.MarkSending();
        // Without anyone marking recycled, the wait must time out.
        var got = b.AwaitRecycled(TimeSpan.FromMilliseconds(50));
        Assert.That(got, Is.False);
        Assert.That(b.IsSending, Is.True);

        // Now mark recycled and try again — must observe true immediately.
        b.MarkRecycled();
        Assert.That(b.AwaitRecycled(TimeSpan.FromMilliseconds(50)), Is.True);
    }
}
