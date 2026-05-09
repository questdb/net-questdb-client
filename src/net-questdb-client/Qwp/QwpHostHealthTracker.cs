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

internal enum QwpZoneTier
{
    Same,
    Unknown,
    Other,
}

/// <summary>
///     Per-sender host-ranking bookkeeping. <see cref="PickNext" /> returns the highest-priority
///     <c>(state, zone)</c> host not yet tried this round.
/// </summary>
internal sealed class QwpHostHealthTracker
{
    private static readonly QwpHostState[] StatePriorityOrder =
    {
        QwpHostState.Healthy,
        QwpHostState.Unknown,
        QwpHostState.TransientReject,
        QwpHostState.TransportError,
        QwpHostState.TopologyReject,
    };

    private static readonly QwpZoneTier[] ZonePriorityOrder =
    {
        QwpZoneTier.Same,
        QwpZoneTier.Unknown,
        QwpZoneTier.Other,
    };

    // Shared between the SF reconnect thread and drainer-pool tasks; guards _states + _attemptedThisRound + _zoneTiers.
    private readonly object _lock = new();
    private readonly bool[] _attemptedThisRound;
    private readonly string[] _hosts;
    private readonly QwpHostState[] _states;
    private readonly QwpZoneTier[] _zoneTiers;
    private readonly long[] _lastSuccessEpoch;
    private readonly string? _clientZone;
    private readonly bool _zoneCollapseToSame;
    private long _successCounter;

    public QwpHostHealthTracker(IReadOnlyList<string> hosts)
        : this(hosts, clientZone: null, targetIsPrimary: false)
    {
    }

    public QwpHostHealthTracker(IReadOnlyList<string> hosts, string? clientZone, bool targetIsPrimary)
    {
        if (hosts == null) throw new ArgumentNullException(nameof(hosts));
        if (hosts.Count == 0) throw new ArgumentException("hosts must be non-empty", nameof(hosts));
        _hosts = new string[hosts.Count];
        for (var i = 0; i < hosts.Count; i++) _hosts[i] = hosts[i];
        _states = new QwpHostState[_hosts.Length];
        _attemptedThisRound = new bool[_hosts.Length];
        _lastSuccessEpoch = new long[_hosts.Length];
        _zoneTiers = new QwpZoneTier[_hosts.Length];

        _clientZone = string.IsNullOrEmpty(clientZone) ? null : clientZone;
        _zoneCollapseToSame = _clientZone is null || targetIsPrimary;
        var defaultTier = _zoneCollapseToSame ? QwpZoneTier.Same : QwpZoneTier.Unknown;
        for (var i = 0; i < _zoneTiers.Length; i++) _zoneTiers[i] = defaultTier;
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

    public QwpZoneTier GetZoneTier(int index)
    {
        lock (_lock) return _zoneTiers[index];
    }

    /// <summary>Returns the highest-priority host not yet attempted this round, or -1 when exhausted.</summary>
    public int PickNext()
    {
        lock (_lock)
        {
            foreach (var statePriority in StatePriorityOrder)
            {
                foreach (var zonePriority in ZonePriorityOrder)
                {
                    for (var i = 0; i < _hosts.Length; i++)
                    {
                        if (!_attemptedThisRound[i]
                            && _states[i] == statePriority
                            && _zoneTiers[i] == zonePriority)
                        {
                            return i;
                        }
                    }
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
    ///     Records the server-advertised zone for a host; null/empty is a no-op. Persists across
    ///     <see cref="BeginRound" />.
    /// </summary>
    public void RecordZone(int hostIndex, string? zoneId)
    {
        if (string.IsNullOrEmpty(zoneId)) return;
        lock (_lock)
        {
            _zoneTiers[hostIndex] = _zoneCollapseToSame
                || string.Equals(zoneId, _clientZone, StringComparison.OrdinalIgnoreCase)
                ? QwpZoneTier.Same
                : QwpZoneTier.Other;
        }
    }

    /// <summary>
    ///     Clears the attempted-this-round flags. With <paramref name="forgetClassifications" />, also
    ///     resets non-Healthy states to <see cref="QwpHostState.Unknown" /> while preserving the last
    ///     same-zone <see cref="QwpHostState.Healthy" /> as sticky pick.
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
                    if (_states[i] == QwpHostState.Healthy
                        && _zoneTiers[i] == QwpZoneTier.Same
                        && _lastSuccessEpoch[i] > bestEpoch)
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
