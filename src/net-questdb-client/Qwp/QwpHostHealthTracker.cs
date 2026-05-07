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

using System;
using System.Collections.Generic;

namespace QuestDB.Qwp;

internal enum QwpHostState
{
    Unknown,
    Healthy,
    TransientReject,
    TransportError,
    TopologyReject,
}

/// <summary>
///     Per-sender bookkeeping that ranks the configured <c>addr=</c> list when
///     selecting the next endpoint to try. Classifications are populated from
///     the outcome of each connect attempt: a <c>421 + X-QuestDB-Role:
///     PRIMARY_CATCHUP</c> reject becomes <see cref="QwpHostState.TransientReject" />,
///     a <c>REPLICA</c> reject becomes <see cref="QwpHostState.TopologyReject" />,
///     any other transport failure becomes <see cref="QwpHostState.TransportError" />,
///     and a successful upgrade becomes <see cref="QwpHostState.Healthy" />.
///     <para>
///         Within a round, <see cref="PickNext" /> returns the highest-priority host
///         that has not yet been attempted. The caller calls <see cref="BeginRound" />
///         to clear the attempted-this-round flags either keeping classifications
///         (sticky-recovery after a previously-healthy connection drops) or
///         discarding them (re-evaluate after backoff completes a failed round).
///     </para>
/// </summary>
internal sealed class QwpHostHealthTracker
{
    private static readonly QwpHostState[] PriorityOrder =
    {
        QwpHostState.Healthy,
        QwpHostState.Unknown,
        QwpHostState.TransientReject,
        QwpHostState.TransportError,
        QwpHostState.TopologyReject,
    };

    // Shared between the SF reconnect thread and drainer-pool tasks; guards _states + _attemptedThisRound.
    private readonly object _lock = new();
    private readonly bool[] _attemptedThisRound;
    private readonly string[] _hosts;
    private readonly QwpHostState[] _states;
    private readonly long[] _lastSuccessEpoch;
    private long _successCounter;

    public QwpHostHealthTracker(IReadOnlyList<string> hosts)
    {
        if (hosts == null) throw new ArgumentNullException(nameof(hosts));
        if (hosts.Count == 0) throw new ArgumentException("hosts must be non-empty", nameof(hosts));
        _hosts = new string[hosts.Count];
        for (var i = 0; i < hosts.Count; i++) _hosts[i] = hosts[i];
        _states = new QwpHostState[_hosts.Length];
        _attemptedThisRound = new bool[_hosts.Length];
        _lastSuccessEpoch = new long[_hosts.Length];
    }

    public int Count => _hosts.Length;

    /// <summary>True once every host has been attempted in the current round.</summary>
    public bool IsRoundExhausted
    {
        get
        {
            lock (_lock)
            {
                for (var i = 0; i < _attemptedThisRound.Length; i++)
                {
                    if (!_attemptedThisRound[i]) return false;
                }
                return true;
            }
        }
    }

    public string GetHost(int index) => _hosts[index];

    public QwpHostState GetState(int index)
    {
        lock (_lock) return _states[index];
    }

    /// <summary>Returns the highest-priority host not yet attempted this round, or -1 when exhausted.</summary>
    public int PickNext()
    {
        lock (_lock)
        {
            foreach (var priority in PriorityOrder)
            {
                for (var i = 0; i < _hosts.Length; i++)
                {
                    if (!_attemptedThisRound[i] && _states[i] == priority) return i;
                }
            }

            return -1;
        }
    }

    public void RecordSuccess(int hostIndex)
    {
        lock (_lock)
        {
            _states[hostIndex] = QwpHostState.Healthy;
            _attemptedThisRound[hostIndex] = true;
            _lastSuccessEpoch[hostIndex] = ++_successCounter;
        }
    }

    public void RecordRoleReject(int hostIndex, bool transient)
    {
        lock (_lock)
        {
            _states[hostIndex] = transient ? QwpHostState.TransientReject : QwpHostState.TopologyReject;
            _attemptedThisRound[hostIndex] = true;
        }
    }

    public void RecordTransportError(int hostIndex)
    {
        lock (_lock)
        {
            _states[hostIndex] = QwpHostState.TransportError;
            _attemptedThisRound[hostIndex] = true;
        }
    }

    /// <summary>
    ///     Records that a previously-successful connection failed mid-stream (send or receive). Demotes
    ///     <see cref="QwpHostState.Healthy" /> to <see cref="QwpHostState.TransportError" /> so the next
    ///     <see cref="BeginRound" /> with <c>forgetClassifications=true</c> doesn't preserve the dead
    ///     host as the sticky-priority entry.
    /// </summary>
    public void RecordMidStreamFailure(int hostIndex)
    {
        lock (_lock)
        {
            if (_states[hostIndex] == QwpHostState.Healthy)
            {
                _states[hostIndex] = QwpHostState.TransportError;
            }
        }
    }

    /// <summary>
    ///     Clears the attempted-this-round flags. With <paramref name="forgetClassifications" />,
    ///     every host except the last-known <see cref="QwpHostState.Healthy" /> entry is reset
    ///     to <see cref="QwpHostState.Unknown" /> — the sticky-Healthy keeps the previously-good
    ///     host first in line on the next round while letting the rest re-evaluate.
    /// </summary>
    public void BeginRound(bool forgetClassifications)
    {
        lock (_lock)
        {
            var stickyIndex = -1;
            if (forgetClassifications)
            {
                var bestEpoch = 0L;
                for (var i = 0; i < _hosts.Length; i++)
                {
                    if (_states[i] == QwpHostState.Healthy && _lastSuccessEpoch[i] > bestEpoch)
                    {
                        bestEpoch = _lastSuccessEpoch[i];
                        stickyIndex = i;
                    }
                }
            }

            for (var i = 0; i < _hosts.Length; i++)
            {
                _attemptedThisRound[i] = false;
                if (forgetClassifications && i != stickyIndex) _states[i] = QwpHostState.Unknown;
            }
        }
    }
}
