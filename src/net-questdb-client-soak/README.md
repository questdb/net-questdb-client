# net-questdb-client-soak

A standalone, long-running load / profiling harness for the QWP WebSocket sender, the
`QuestDBClient` connection pool and store-and-forward. It runs forever (or for a fixed
duration) against a live QuestDB **master** build so you can attach a profiler and hunt
for slow leaks, unbounded growth and data-loss bugs that the short unit / e2e tests
never surface.

## What it exercises

- **Multiple connections** — a pooled churn workload whose *active* borrower count follows
  a mean-reverting random walk (mean ~5, occasional spikes toward `sender_pool_max`=30).
- **Multiple tables** — 1 wide + 3 narrow tables, written concurrently.
- **Every QWP column type** — the `soak_wide` table covers symbol, varchar, bool, byte,
  short, int, long, float, double, char, timestamp (micros + nanos), date, uuid, ipv4,
  geohash, long256, decimal64/128/256, binary and double arrays. (Long arrays are opt-in;
  the current engine rejects them — see below.)
- **Sender pool + store-and-forward** — the churn pool runs disk-backed SF (`sf_dir`);
  one pinned sender runs disk SF and another runs RAM-backed (in-memory) SF.
- **Spiky load on a held-forever connection** — two pinned senders (never returned to a
  pool) cycle through burst → trickle → pause phases, momentarily peaking, then idling.
- **Bounded average throughput** — a shared byte token-bucket caps the long-run average
  (default 20 GiB/h) while permitting large bursts, so it won't fill your disk.

## Observability (printed to stdout every `report` seconds)

- rows/s (instant), wire MiB/s (instant + average), pool idle/total sender count.
- Aggregated QWP counters from the pinned senders: frames, acks, server errors,
  reconnect attempts/successes, dropped error notifications.
- SF directory disk usage (confirms the throughput cap is holding).
- Process health for leak-hunting: working set + baseline, GC heap, total allocated
  bytes, gen0/1/2 collection counts, thread-pool threads. A **monotonic** working-set
  climb past `QDB_SOAK_LEAK_WARN_MIB` over baseline trips a `LEAK WARNING`.

## Data-integrity verification (every `verify` seconds)

Quiesces the producers, drains every pool + pinned sender to an ACK, calls
`wait_wal_table('...')` per table, then via the egress query client:

- reconciles server `count()` against the number of appended rows, and
- re-reads the last 200 rows and recomputes each row's `chk` (FNV-1a of `row_id`) and
  `dval`, flagging any corruption.

Because SF is **at-least-once** (no server-side dedup on auto-created tables), the server count
can legitimately be **>=** appended — reconnect replay, `drain_orphans`, and rows left by earlier
runs all add rows this run didn't append. So verification asserts **no loss** and **no corruption**,
not an exact count: it flags a defect only when the count goes **backwards** (rows vanished) or a
**fully-drained** table lands **short** of what was appended, or a tail checksum fails. Excess over
appended is reported as informational (`+N over appended`). `QDB_SOAK_RESET_TABLES=on` (default)
drops the tables at startup so a clean run reconciles exactly. Real defects are tallied in the
final summary.

## Running

Requires a live QuestDB **master** build (the `/write/v4` + `/read/v1` endpoints), e.g.
on `localhost:9000`.

```bash
dotnet run -c Release --project src/net-questdb-client-soak --framework net10.0
```

Ctrl-C drains everything, runs one last verification and prints a final summary.

## Environment knobs

| Variable | Default | Meaning |
|---|---|---|
| `QDB_SOAK_ADDR` | `localhost:9000` | QuestDB host:port |
| `QDB_SOAK_AUTH` | `on` | Basic auth on/off |
| `QDB_SOAK_USER` / `QDB_SOAK_PASS` | `admin` / `quest` | Basic auth credentials |
| `QDB_SOAK_SF_DIR` | `<tmp>/qdb-soak-sf` | store-and-forward directory |
| `QDB_SOAK_GB_PER_HOUR` | `20` | long-run average wire throughput ceiling |
| `QDB_SOAK_BURST_MIB` | `1024` | token-bucket burst capacity |
| `QDB_SOAK_MAX_CONN` | `30` | pool max (spike ceiling) |
| `QDB_SOAK_MIN_CONN` | `1` | pool min; set `== QDB_SOAK_MAX_CONN` to pin the pool (no sender create/reap churn) |
| `QDB_SOAK_MEAN_CONN` | `5` | mean active connection count |
| `QDB_SOAK_REPORT_SEC` | `10` | stdout report interval |
| `QDB_SOAK_VERIFY_SEC` | `60` | egress verification interval (`0` disables) |
| `QDB_SOAK_DURATION_SEC` | `0` | `0` = run forever |
| `QDB_SOAK_LEAK_WARN_MIB` | `512` | working-set growth over baseline that warns |
| `QDB_SOAK_TABLE_PREFIX` | `soak_` | table name prefix |
| `QDB_SOAK_RESET_TABLES` | `on` | drop the soak_ tables at startup so verification counts this run only |
| `QDB_SOAK_DURABLE_ACK` | `off` | request durable ACK (needs enterprise replication) |
| `QDB_SOAK_LONG_ARRAYS` | `off` | include a `long[]` column (rejected by current engine) |

## Profiling the library's allocations

The row-building hot path is **allocation-free**, so an allocation profile shows the *library*, not
the harness:

- `long256` values are pre-built once into a fixed pool and reused (a `BigInteger` for a >int value
  otherwise allocates a backing `uint[]` per row).
- `double[]` arrays, the binary column and the `uuid` bytes use `stackalloc` (no heap).
- symbols / varchars are interned static strings; `IPv4` addresses come from a small static ring.
- decimals are `System.Decimal` structs; ids/timestamps are longs — no boxing.

The final summary prints `generated(ids)` vs `appended` (should stay `gap 0`). The only remaining
harness allocations are **per-batch, not per-row** (the pacing `Task.Delay`s that shape the bursts)
plus the library's own per-borrow / per-flush objects — which is exactly what you want to see.

For the cleanest profile, quiet the periodic machinery: set `QDB_SOAK_VERIFY_SEC=0` (the egress
verifier allocates SQL strings / readers) and a large `QDB_SOAK_REPORT_SEC` (the reporter allocates
report strings and walks the SF dir).

**Periodic big-buffer allocations = the pool creating senders.** The pool is elastic between
`QDB_SOAK_MIN_CONN` (1) and `QDB_SOAK_MAX_CONN` (30): as the spiky concurrency walk climbs, the pool
creates new `QwpWebSocketSender`s (each allocating a cursor engine, segment ring, and encode /
receive / column buffers), and reaps idle ones after `idle_timeout`, recreating them on the next
spike. To profile the steady data path *without* this create/reap churn, **pin the pool** — set
`QDB_SOAK_MIN_CONN` equal to `QDB_SOAK_MAX_CONN`: every sender is created once at startup and reused
for the process lifetime (reaping and lifetime rotation only ever act on senders above `min`). The
spiky *load* shape is preserved; only the connection churn goes away.

## Notes / findings

- **Long arrays**: the client advertises `long[]` columns, but the current QuestDB engine
  rejects them over QWP (`long arrays are not supported, only double arrays`), which
  cascades into a poison-frame terminal failure. The column is therefore opt-in
  (`QDB_SOAK_LONG_ARRAYS=on`) and off by default.
- **Durable ACK** (`request_durable_ack`) requires enterprise primary replication and is
  off by default so the harness works against an OSS master build.
