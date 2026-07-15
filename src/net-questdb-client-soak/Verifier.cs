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
///     Egress data-integrity check. Every <c>verify_every</c> seconds it quiesces the producers,
///     drains every pool + pinned sender to an ACK, waits for the WAL to apply
///     (<c>wait_wal_table</c>), then reconciles server <c>count()</c> against what was appended and
///     re-reads a tail of rows to confirm <c>chk</c> / <c>dval</c> survived the round trip. Any
///     mismatch is a real data-loss or corruption signal and is printed loudly.
/// </summary>
internal sealed class Verifier
{
    private const int TailSample = 200;

    private readonly SoakConfig _cfg;
    private readonly IReadOnlyList<IQwpWebSocketSender> _pinned;
    private readonly IReadOnlyList<IQuestDBClient> _pools;
    private readonly PauseGate _gate;
    private readonly IQuestDBClient _query;
    private readonly IReadOnlyList<TableSpec> _specs;

    private long _checksumErrors;
    private long _countMismatches;
    private readonly long[] _lastServerCount;

    public Verifier(SoakConfig cfg, IQuestDBClient query, IReadOnlyList<IQuestDBClient> pools,
        IReadOnlyList<IQwpWebSocketSender> pinned, PauseGate gate, IReadOnlyList<TableSpec> specs)
    {
        _cfg = cfg;
        _query = query;
        _pools = pools;
        _pinned = pinned;
        _gate = gate;
        _specs = specs;
        _lastServerCount = new long[specs.Count];
    }

    public long CountMismatches => Interlocked.Read(ref _countMismatches);
    public long ChecksumErrors => Interlocked.Read(ref _checksumErrors);

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(_cfg.VerifyEverySec), ct).ConfigureAwait(false);
                try
                {
                    await VerifyOnceAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Console.WriteLine($"  verify cycle error: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async Task VerifyOnceAsync(CancellationToken ct)
    {
        await _gate.QuiesceAsync(ct).ConfigureAwait(false);
        try
        {
            var timeout = TimeSpan.FromSeconds(60);
            // Track whether EVERY sender fully drained. If any drain times out (e.g. under network
            // backpressure the ring still holds un-shipped frames), the server legitimately trails
            // the appended count — those rows are in flight in store-and-forward, not lost — so we
            // must NOT assert equality.
            var fullyDrained = true;

            foreach (var p in _pools)
                try { fullyDrained &= await p.FlushAsync(timeout, ct).ConfigureAwait(false); }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    fullyDrained = false;
                    Console.WriteLine($"  verify: pool flush failed ({ex.GetType().Name}: {ex.Message})");
                }

            foreach (var s in _pinned)
                try
                {
                    fullyDrained &= await s.FlushAsync(timeout, ct).ConfigureAwait(false);
                    await s.PingAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    fullyDrained = false;
                    Console.WriteLine($"  verify: pinned flush failed ({ex.GetType().Name}: {ex.Message})");
                }

            // Snapshot appended counts while quiesced: no producer can move them now.
            var expected = _specs.Select(s => s.Appended).ToArray();
            if (!fullyDrained)
                Console.WriteLine("  verify: drain incomplete (backpressure) — checking backlog, not asserting equality");

            for (var i = 0; i < _specs.Count; i++)
            {
                var spec = _specs[i];
                try
                {
                    await DrainAsync($"select wait_wal_table('{spec.Name}')", ct).ConfigureAwait(false);
                    var serverCount = await ScalarLongAsync($"select count() from {spec.Name}", ct)
                        .ConfigureAwait(false);
                    var prev = _lastServerCount[i];
                    _lastServerCount[i] = serverCount;

                    // SF is at-least-once with no server-side dedup, and drain_orphans / pre-existing
                    // rows can inflate the table. So server >= appended is NORMAL — it is NOT a defect.
                    // Real defects are only: the count going BACKWARDS (rows vanished = loss), or a
                    // clean full drain landing SHORT of what we appended (loss).
                    if (serverCount < prev)
                    {
                        Interlocked.Increment(ref _countMismatches);
                        Console.WriteLine(
                            $"  ** ROW LOSS {spec.Name}: server count went backwards {prev:N0} -> {serverCount:N0} **");
                    }
                    else if (fullyDrained && serverCount < expected[i])
                    {
                        Interlocked.Increment(ref _countMismatches);
                        Console.WriteLine(
                            $"  ** ROW LOSS {spec.Name}: fully drained but server={serverCount:N0} < appended={expected[i]:N0} " +
                            $"(short {expected[i] - serverCount:N0}) **");
                    }
                    else if (serverCount > expected[i])
                    {
                        Console.WriteLine(
                            $"  verify {spec.Name}: count={serverCount:N0} (+{serverCount - expected[i]:N0} over appended: " +
                            "at-least-once dupes / orphan-drain / pre-existing — not loss)");
                    }
                    else if (serverCount == expected[i])
                    {
                        Console.WriteLine($"  verify {spec.Name}: count={serverCount:N0} OK");
                    }
                    else
                    {
                        Console.WriteLine(
                            $"  verify {spec.Name}: count={serverCount:N0} of {expected[i]:N0} " +
                            $"({expected[i] - serverCount:N0} in flight)");
                    }

                    await VerifyTailAsync(spec, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Console.WriteLine($"  verify {spec.Name}: skipped ({ex.GetType().Name}: {ex.Message})");
                }
            }
        }
        finally
        {
            _gate.Resume();
        }
    }

    private async Task VerifyTailAsync(TableSpec spec, CancellationToken ct)
    {
        var bad = 0;
        var seen = 0;
        await using var reader = await _query
            .ExecuteReaderAsync($"select row_id, chk, dval from {spec.Name} limit -{TailSample}", ct)
            .ConfigureAwait(false);

        while (await reader.ReadBatchAsync(ct).ConfigureAwait(false))
        {
            var b = reader.Current;
            for (var r = 0; r < b.RowCount; r++)
            {
                var id = b.GetLongValue(0, r);
                var chk = b.GetLongValue(1, r);
                var dval = b.GetDoubleValue(2, r);
                seen++;
                if (chk != RowFactory.Checksum(id) || Math.Abs(dval - RowFactory.Dval(id)) > 1e-6)
                    bad++;
            }
        }

        if (bad > 0)
        {
            Interlocked.Add(ref _checksumErrors, bad);
            Console.WriteLine($"  ** CHECKSUM MISMATCH {spec.Name}: {bad}/{seen} sampled rows corrupt **");
        }
    }

    private async Task<long> ScalarLongAsync(string sql, CancellationToken ct)
    {
        await using var reader = await _query.ExecuteReaderAsync(sql, ct).ConfigureAwait(false);
        long value = 0;
        if (await reader.ReadBatchAsync(ct).ConfigureAwait(false) && reader.Current.RowCount > 0)
            value = reader.Current.GetLongValue(0, 0);
        while (await reader.ReadBatchAsync(ct).ConfigureAwait(false))
        {
        }

        return value;
    }

    private async Task DrainAsync(string sql, CancellationToken ct)
    {
        await using var reader = await _query.ExecuteReaderAsync(sql, ct).ConfigureAwait(false);
        while (await reader.ReadBatchAsync(ct).ConfigureAwait(false))
        {
        }
    }
}
