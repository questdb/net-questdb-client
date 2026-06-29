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

namespace QuestDB.Pooling;

/// <summary>
///     Background sweeper that periodically reaps idle / over-age senders from a
///     <see cref="SenderPool" />. Runs on a pooled <see cref="Task" /> driven by a
///     <see cref="PeriodicTimer" />; the loop swallows every fault (the C# analogue of Java's
///     <c>catch (Throwable)</c>) so a single bad delegate teardown can never kill all future reaping.
/// </summary>
internal sealed class PoolHousekeeper : IDisposable
{
    private static readonly TimeSpan JoinBudget = TimeSpan.FromSeconds(2);

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private readonly SenderPool _pool;
    private readonly PeriodicTimer _timer;
    private int _stopped;

    internal PoolHousekeeper(SenderPool pool, TimeSpan interval)
    {
        _pool = pool;
        _timer = new PeriodicTimer(interval);
        _loop = Task.Run(RunAsync);
    }

    public void Dispose()
    {
        Stop();
        _cts.Dispose();
    }

    /// <summary>Signals the loop to stop and joins it (bounded), like Java's housekeeper.stop().</summary>
    internal void Stop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) == 0)
        {
            SignalStop();
        }

        try
        {
            // Bounded join: a reap in flight finishes well within this; we never block close for long.
            _loop.Wait(JoinBudget);
        }
        catch
        {
            // the loop swallows its own faults; nothing actionable here
        }
    }

    /// <summary>Async variant of <see cref="Stop" /> so the async client-dispose path does not block on the join.</summary>
    internal async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) == 0)
        {
            SignalStop();
        }

        try
        {
            await _loop.WaitAsync(JoinBudget).ConfigureAwait(false);
        }
        catch
        {
            // bounded; the loop swallows its own faults
        }
    }

    private void SignalStop()
    {
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // already stopped
        }

        _timer.Dispose();
    }

    private async Task RunAsync()
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(_cts.Token).ConfigureAwait(false))
            {
                try
                {
                    _pool.ReapIdle();
                }
                catch
                {
                    // Best-effort housekeeping; a delegate teardown fault must not stop the sweeper.
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Stop() cancelled the loop.
        }
        catch (ObjectDisposedException)
        {
            // timer disposed by Stop()
        }
    }
}
