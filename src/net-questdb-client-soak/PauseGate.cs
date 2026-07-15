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

namespace QuestDB.Soak;

/// <summary>
///     Cooperative quiescence barrier. Producers bracket each unit of work in
///     <see cref="EnterAsync" />/<see cref="Exit" />; the verifier calls <see cref="QuiesceAsync" />
///     to close the gate and wait for all in-flight units to drain, giving it an exact,
///     race-free view of what has been appended before it queries the server.
/// </summary>
internal sealed class PauseGate
{
    private readonly object _lock = new();
    private volatile bool _paused;
    private int _inFlight;

    public async Task EnterAsync(CancellationToken ct)
    {
        while (_paused)
            await Task.Delay(5, ct).ConfigureAwait(false);
        Interlocked.Increment(ref _inFlight);
    }

    public void Exit() => Interlocked.Decrement(ref _inFlight);

    /// <summary>Closes the gate and waits until every producer has left its work bracket.</summary>
    public async Task QuiesceAsync(CancellationToken ct)
    {
        _paused = true;
        while (Volatile.Read(ref _inFlight) > 0)
            await Task.Delay(5, ct).ConfigureAwait(false);
    }

    public void Resume() => _paused = false;
}
