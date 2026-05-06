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

namespace QuestDB.Qwp.Sf;

/// <summary>
///     Pure-function exponential-backoff math used by the SF reconnect loop.
/// </summary>
/// <remarks>
///     The policy doesn't perform any I/O or sleeping itself; the caller passes in elapsed times
///     and the policy returns the next backoff or signals "give up". This keeps the loop logic
///     testable in isolation from the wall clock.
///     <para />
///     Backoff schedule: start at <see cref="InitialBackoff" />, double each attempt, cap at
///     <see cref="MaxBackoff" />. Per-outage budget <see cref="MaxOutageDuration" /> bounds total
///     wait time across the entire reconnect run.
/// </remarks>
internal sealed class QwpReconnectPolicy
{
    private readonly Func<TimeSpan, TimeSpan> _jitter;

    /// <param name="initialBackoff">Wait before the first reconnect attempt.</param>
    /// <param name="maxBackoff">Cap on per-attempt wait — exponential growth saturates here.</param>
    /// <param name="maxOutageDuration">Total time budget across all attempts in one outage; engine becomes terminal once exceeded.</param>
    /// <param name="jitter">
    ///     Optional jitter transform applied after exponential growth and max-clamping. Pass
    ///     <see cref="UniformDoubleJitter" /> to spread backoff over <c>[base, 2·base)</c>.
    ///     Default: identity (deterministic — used by tests).
    /// </param>
    public QwpReconnectPolicy(
        TimeSpan initialBackoff,
        TimeSpan maxBackoff,
        TimeSpan maxOutageDuration,
        Func<TimeSpan, TimeSpan>? jitter = null)
    {
        if (initialBackoff <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(initialBackoff), "must be positive");
        }

        if (maxBackoff < initialBackoff)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBackoff), "must be ≥ initialBackoff");
        }

        if (maxOutageDuration < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(maxOutageDuration), "must be ≥ 0");
        }

        InitialBackoff = initialBackoff;
        MaxBackoff = maxBackoff;
        MaxOutageDuration = maxOutageDuration;
        _jitter = jitter ?? (b => b);
    }

    /// <summary>Backoff for the first reconnect attempt.</summary>
    public TimeSpan InitialBackoff { get; }

    /// <summary>Upper bound on any single backoff after exponential growth.</summary>
    public TimeSpan MaxBackoff { get; }

    /// <summary>Total wall-clock wait budget across the whole reconnect run.</summary>
    public TimeSpan MaxOutageDuration { get; }

    /// <summary>Full-jitter transform that picks uniformly from <c>[0, base]</c> using <see cref="Random.Shared" />.</summary>
    public static TimeSpan UniformDoubleJitter(TimeSpan baseBackoff)
    {
        if (baseBackoff <= TimeSpan.Zero)
        {
            return baseBackoff;
        }

        var ticks = (long)(Random.Shared.NextDouble() * (baseBackoff.Ticks + 1));
        return TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    ///     Computes the backoff for attempt <paramref name="attemptIndex" /> (0-based). Doubling
    ///     starts at <see cref="InitialBackoff" />, is clamped to <see cref="MaxBackoff" />, and
    ///     finally passed through the configured jitter transform.
    /// </summary>
    public TimeSpan ComputeBackoff(int attemptIndex)
    {
        if (attemptIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(attemptIndex));
        }

        // Saturate on ticks to avoid long overflow when InitialBackoff is days-scale.
        var ticks = InitialBackoff.Ticks;
        var maxTicks = MaxBackoff.Ticks;
        for (var i = 0; i < attemptIndex && ticks < maxTicks; i++)
        {
            if (ticks > maxTicks / 2)
            {
                ticks = maxTicks;
                break;
            }
            ticks <<= 1;
        }

        var clampedTicks = ticks > maxTicks ? maxTicks : ticks;
        var jittered = _jitter(TimeSpan.FromTicks(clampedTicks));
        return jittered > MaxBackoff ? MaxBackoff : jittered;
    }

    /// <summary>
    ///     Returns the next backoff to sleep for, or <c>null</c> if the per-outage budget is exhausted.
    /// </summary>
    /// <param name="attemptIndex">0 for the first reconnect, 1 for the second, etc.</param>
    /// <param name="elapsedSinceOutage">Total wall-clock elapsed since the connection failed.</param>
    public TimeSpan? NextBackoffOrGiveUp(int attemptIndex, TimeSpan elapsedSinceOutage)
    {
        if (elapsedSinceOutage > MaxOutageDuration)
        {
            return null;
        }

        var backoff = ComputeBackoff(attemptIndex);

        // Don't sleep past the outage budget — clip to whatever remains.
        var remaining = MaxOutageDuration - elapsedSinceOutage;
        if (backoff > remaining)
        {
            return remaining > TimeSpan.Zero ? remaining : null;
        }

        return backoff;
    }
}
