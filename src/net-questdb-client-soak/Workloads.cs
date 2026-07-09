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

using QuestDB.Senders;

namespace QuestDB.Soak;

/// <summary>
///     Pooled churn: a fleet of workers borrow a sender per unit of work, write a random-sized
///     batch to a random table, send, and return it. The number of *active* borrowers follows a
///     mean-reverting random walk that occasionally spikes to the pool max, so the live connection
///     count averages ~<c>mean</c> and spikes toward <c>max</c> — the requested connection profile.
/// </summary>
internal sealed class ChurnWorkload
{
    private readonly SoakConfig _cfg;
    private readonly IQuestDBClient _client;
    private readonly PauseGate _gate;
    private readonly RateGovernor _governor;
    private readonly Metrics _metrics;
    private readonly IReadOnlyList<TableSpec> _specs;
    private long _errors;
    private int _target;

    public ChurnWorkload(SoakConfig cfg, IQuestDBClient client, RateGovernor governor, Metrics metrics,
        PauseGate gate, IReadOnlyList<TableSpec> specs)
    {
        _cfg = cfg;
        _client = client;
        _governor = governor;
        _metrics = metrics;
        _gate = gate;
        _specs = specs;
        _target = cfg.MeanConnections;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var tasks = new List<Task> { ControllerAsync(ct) };
        for (var i = 0; i < _cfg.MaxConnections; i++)
        {
            var idx = i;
            tasks.Add(Task.Run(() => WorkerAsync(idx, ct), ct));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private async Task ControllerAsync(CancellationToken ct)
    {
        var rnd = new Random(12345);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(1000, ct).ConfigureAwait(false);
                int target;
                if (rnd.NextDouble() < 0.06)
                    target = rnd.Next(_cfg.MeanConnections * 3, _cfg.MaxConnections + 1); // spike
                else
                    target = _target + (int)Math.Round((_cfg.MeanConnections - _target) * 0.5 + (rnd.NextDouble() * 4 - 2));

                _target = Math.Clamp(target, 1, _cfg.MaxConnections);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task WorkerAsync(int index, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await _gate.EnterAsync(ct).ConfigureAwait(false);
                try
                {
                    if (index >= Volatile.Read(ref _target))
                    {
                        await Task.Delay(Random.Shared.Next(30, 120), ct).ConfigureAwait(false);
                        continue;
                    }

                    var spec = _specs[Random.Shared.Next(_specs.Count)];
                    var batch = Random.Shared.Next(50, 2000);
                    await _governor.AcquireAsync((long)batch * spec.ApproxRowBytes, ct).ConfigureAwait(false);

                    using var sender = await _client.BorrowSenderAsync(ct).ConfigureAwait(false);
                    var ws = (IQwpWebSocketSender)sender;
                    for (var k = 0; k < batch; k++)
                        RowFactory.BuildRow(ws, spec, spec.NextRowId());
                    await sender.SendAsync(ct).ConfigureAwait(false);
                    _metrics.Record(batch, (long)batch * spec.ApproxRowBytes);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // The pool discards a terminally-failed sender on return and mints a fresh one on
                    // the next borrow, so logging and continuing lets the churn self-heal.
                    var n = Interlocked.Increment(ref _errors);
                    if (n % 50 == 1)
                        Console.WriteLine($"  churn worker {index}: {ex.GetType().Name}: {ex.Message} (err #{n})");
                    await Task.Delay(200, ct).ConfigureAwait(false);
                }
                finally
                {
                    _gate.Exit();
                }

                await Task.Delay(Random.Shared.Next(2, 40), ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}

/// <summary>
///     A single connection held for the whole run (never returned to a pool). It cycles through
///     burst / trickle / pause phases — a ~1M-row/s spike, then a slow trickle, then an idle gap —
///     to stress the store-and-forward ring and the reconnect/ack path on one long-lived socket.
///     With <c>sf_dir</c> set the ring is mmap-backed (disk SF); without it, RAM-backed (in-memory SF).
/// </summary>
internal sealed class PinnedWorkload
{
    private readonly SoakConfig _cfg;
    private readonly PauseGate _gate;
    private readonly RateGovernor _governor;
    private readonly string _label;
    private readonly Metrics _metrics;
    private readonly IQwpWebSocketSender _sender;
    private readonly TableSpec _spec;

    public PinnedWorkload(string label, IQwpWebSocketSender sender, TableSpec spec, SoakConfig cfg,
        RateGovernor governor, Metrics metrics, PauseGate gate)
    {
        _label = label;
        _sender = sender;
        _spec = spec;
        _cfg = cfg;
        _governor = governor;
        _metrics = metrics;
        _gate = gate;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var rnd = new Random(_label.GetHashCode());
        var errors = 0L;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await BurstAsync(TimeSpan.FromSeconds(rnd.Next(1, 4)), ct).ConfigureAwait(false);
                await TrickleAsync(TimeSpan.FromSeconds(rnd.Next(5, 16)), ct).ConfigureAwait(false);
                await FlushQuietlyAsync(ct).ConfigureAwait(false);
                await PauseAsync(TimeSpan.FromSeconds(rnd.Next(3, 11)), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                // A transient disconnect self-heals inside the sender; a terminal fault keeps throwing
                // here — log it (throttled) and back off rather than tearing the whole harness down.
                errors++;
                if (errors % 20 == 1)
                    Console.WriteLine($"  pinned {_label}: {ex.GetType().Name}: {ex.Message} (err #{errors})");
                try { await Task.Delay(2000, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
    }

    private async Task BurstAsync(TimeSpan duration, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + duration;
        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            await _gate.EnterAsync(ct).ConfigureAwait(false);
            try
            {
                const int chunk = 5000;
                await _governor.AcquireAsync((long)chunk * _spec.ApproxRowBytes, ct).ConfigureAwait(false);
                for (var k = 0; k < chunk; k++)
                    RowFactory.BuildRow(_sender, _spec, _spec.NextRowId());
                _metrics.Record(chunk, (long)chunk * _spec.ApproxRowBytes);
                // No per-chunk drain: auto_flush ships to the ring and the pump keeps the wire busy,
                // which is what lets the burst momentarily peak.
            }
            finally
            {
                _gate.Exit();
            }
        }
    }

    private async Task TrickleAsync(TimeSpan duration, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + duration;
        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            await _gate.EnterAsync(ct).ConfigureAwait(false);
            try
            {
                var chunk = Random.Shared.Next(10, 200);
                await _governor.AcquireAsync((long)chunk * _spec.ApproxRowBytes, ct).ConfigureAwait(false);
                for (var k = 0; k < chunk; k++)
                    RowFactory.BuildRow(_sender, _spec, _spec.NextRowId());
                _metrics.Record(chunk, (long)chunk * _spec.ApproxRowBytes);
            }
            finally
            {
                _gate.Exit();
            }

            await Task.Delay(Random.Shared.Next(50, 250), ct).ConfigureAwait(false);
        }
    }

    private async Task PauseAsync(TimeSpan duration, CancellationToken ct)
    {
        // Idle window: hold the socket open but send nothing. Not bracketed in the gate so the
        // verifier can quiesce instantly during a pause.
        try
        {
            await _sender.PingAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Console.WriteLine($"  pinned {_label}: ping failed ({ex.GetType().Name})");
        }

        await Task.Delay(duration, ct).ConfigureAwait(false);
    }

    private async Task FlushQuietlyAsync(CancellationToken ct)
    {
        await _gate.EnterAsync(ct).ConfigureAwait(false);
        try
        {
            await _sender.SendAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Console.WriteLine($"  pinned {_label}: flush failed ({ex.GetType().Name}: {ex.Message})");
        }
        finally
        {
            _gate.Exit();
        }
    }
}
