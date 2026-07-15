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

namespace QuestDB.Soak;

/// <summary>
///     Token bucket over wire-bytes. Refills at a fixed average rate but permits a large burst,
///     which is exactly what produces the uneven "spike then pause" load profile: a 1M-row/s burst
///     drains the bucket in a second or two, then producers stall until it refills at the average
///     rate. Shared across every producer thread.
/// </summary>
internal sealed class RateGovernor
{
    private readonly object _lock = new();
    private readonly double _ratePerSec;
    private readonly double _capacity;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private double _available;
    private long _lastTicks;

    public RateGovernor(double bytesPerSecond, long burstCapacityBytes)
    {
        _ratePerSec = bytesPerSecond;
        _capacity = burstCapacityBytes;
        _available = burstCapacityBytes;
        _lastTicks = _clock.ElapsedTicks;
    }

    /// <summary>Blocks (cooperatively) until <paramref name="bytes" /> of budget is available, then spends it.</summary>
    public async Task AcquireAsync(long bytes, CancellationToken ct)
    {
        while (true)
        {
            double deficit;
            lock (_lock)
            {
                Refill();
                if (_available >= bytes || bytes >= _capacity && _available >= _capacity)
                {
                    _available -= bytes;
                    return;
                }

                deficit = bytes - _available;
            }

            var waitMs = (int)Math.Clamp(deficit / _ratePerSec * 1000.0, 1, 2000);
            await Task.Delay(waitMs, ct).ConfigureAwait(false);
        }
    }

    private void Refill()
    {
        var now = _clock.ElapsedTicks;
        var elapsedSec = (now - _lastTicks) / (double)Stopwatch.Frequency;
        _lastTicks = now;
        _available = Math.Min(_capacity, _available + elapsedSec * _ratePerSec);
    }
}
