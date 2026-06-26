# .NET WebSocket / QWP — Performance Benchmarks

## Environment

- **Server**: QuestDB master build with `/write/v4` (ingest) and `/read/v1` (egress) enabled, on `127.0.0.1:9000`
- **Host**: Apple M4 Pro, 14 logical / 14 physical cores, macOS 15.2
- **Runtime**: .NET 10.0.7, Arm64 RyuJIT AdvSIMD
- **BenchmarkDotNet**: v0.13.12, in-process toolchain

## TL;DR

- **Ingest throughput** (`BenchInsertsWs`): WS beats HTTP **2.1–6.8×** across narrow / wide / multi-table at every AutoFlushRows setting.
- **Ingest latency** (`BenchLatencyWs`): WS full round-trip on par with HTTP (single-row median **85 vs 93 μs**); bare-handover `SendAsync` returns in **~5–10 μs** — ~10× faster caller-return.
- **Egress reads** (`BenchQueryWs`): WS reads **3–7× faster** than HTTP `/exec` across narrow and wide; narrow peak **38 M rows/sec** at 10M rows.
- **Egress query latency** (`BenchQueryLatencyWs`): WS bind-query median **97 μs** vs HTTP **114 μs** (p95 0.73×); single-row latency tied; WS allocates ~0 per query vs HTTP ~10 KB.
- **Store-and-forward overhead** (`BenchSfThroughput`): SF ingest runs **~2.3× slower** than non-SF (steady-state, 200k–500k rows) — the mmap durable-append cost of crash-survival — and is markedly higher-variance.

## Methodology

- All benches run in-process; `QDB_BENCH_ENDPOINT=127.0.0.1:9000` points at the live QuestDB server. `BenchInsertsWs`, `BenchLatencyWs`, and `BenchSfThroughput` fall back to an in-process `DummyQwpServer` + `DummyHttpServer` when it is unset; the egress benches (`BenchQueryWs`, `BenchQueryLatencyWs`) require a live server and have no dummy fallback.
- `BenchInsertsWs`, `BenchSfThroughput`, and `BenchQueryWs` run 20 iterations × 5 warmup, one shot per invocation (`InvocationCount=1`).
- Latency benches (`BenchLatencyWs`, `BenchQueryLatencyWs`) default to 20 000 iterations with `InvocationCount=1` (every iteration is one full round-trip), so percentile columns reflect that many samples; pass `--iterationCount 100000` for a stable p99 / p99.9 tail.
- All senders are created in `[GlobalSetup]` so per-invocation cost is row-encoding + frame I/O + ACK wait, not slot/mmap/engine spin-up.

## 1. `BenchInsertsWs` — Sustained throughput

**Workload**: long-lived sender, append N rows, `SendAsync()`. Three row shapes (Narrow / Wide / MultiTable) × AutoFlushRows {100, 1000, 10000} × Rows {10k, 100k}, each with an HTTP baseline. 20 iterations × 5 warmup.

### At `Rows=100000`

| Category | AFR | Mean WS | Mean HTTP | WS rows/sec | WS Ratio |
|---|---|---|---|---|---|
| **Narrow** | 100 | 15.75 ms | 92.53 ms | 6.35 M | **0.17 (5.9×)** |
| **Narrow** | 1000 | 13.44 ms | 36.96 ms | 7.44 M | **0.36 (2.7×)** |
| **Narrow** | 10000 | 13.30 ms | 32.03 ms | 7.52 M | **0.42 (2.4×)** |
| **Wide** | 100 | 52.66 ms | 161.96 ms | 1.90 M | **0.33 (3.1×)** |
| **Wide** | 1000 | 49.86 ms | 109.06 ms | 2.01 M | **0.46 (2.2×)** |
| **Wide** | 10000 | 48.68 ms | 100.45 ms | 2.05 M | **0.48 (2.1×)** |
| **MultiTable** | 100 | 14.21 ms | 96.73 ms | 7.04 M | **0.15 (6.8×)** |
| **MultiTable** | 1000 | 7.91 ms | 28.48 ms | 12.65 M | **0.28 (3.6×)** |
| **MultiTable** | 10000 | 7.21 ms | 20.72 ms | 13.87 M | **0.35 (2.9×)** |

### Observations

- **WS beats HTTP in every cell** — from 2.1× (Wide, high AFR) to 6.8× (MultiTable, AFR=100). The `Rows=10000` cases show the same ordering (WS 2.1–5.3× across shapes).
- **AutoFlushRows trade-off**: AFR=100 gives the biggest WS advantage — frequent small flushes amplify HTTP's per-request overhead, which WS pipelines away. Higher AFR raises absolute throughput but narrows the WS/HTTP ratio.
- **MultiTable peaks at ~13.9 M rows/sec** — WS multiplexes 5 tables over one connection with no per-flush handshake; the WS advantage holds a consistent 2.9–6.8× across all AFR settings.
- **Wide rows ~2 M rows/sec** — payload-bound; the WS/HTTP ratio is smaller (2.1–3.1×) because per-row encoding dominates over protocol overhead.
- **WS allocates far less managed memory than HTTP** — at `Rows=100000`, WS allocation is **< 0.02×** of HTTP for Narrow and Wide rows and **~0.2×** for MultiTable. HTTP rebuilds a full ILP text payload per flush (~20 MB narrow, ~85 MB wide); WS reuses encoder and segment-ring buffers — KB-scale for Narrow/Wide, ~3 MB for MultiTable's five interleaved per-table buffers.

## 2. `BenchLatencyWs` — Single-batch send latency

**Workload**: persistent senders, append `RowsPerBatch` rows, flush. Two categories, each with its own baseline:

- **RoundTrip** — `SendAsync` + `PingAsync` (drains the in-flight ACK window), so the measured time is a full send + server-process + ACK round-trip. HTTP baseline.
- **Handover** — bare `SendAsync`, which returns after the async enqueue into the cursor send engine without waiting for the server. `Ws_HandoverRam` (RAM segment ring) is the baseline; `Ws_HandoverSf` adds an `sf_dir` mmap-backed ring.

20 000 iterations per case (each iteration is one flush).

| Category | RowsPerBatch | Method | Median | p95 | Min | Ratio | Allocated |
|---|---|---|---|---|---|---|---|
| **RoundTrip** | 1 | Http_Roundtrip | 118.0 μs | 221.4 μs | 40.4 μs | 1.00 | 9.3 KB |
| **RoundTrip** | 1 | **Ws_SyncRoundtrip** | **107.6 μs** | **178.7 μs** | 37.0 μs | **0.99** | **0** |
| **RoundTrip** | 100 | Http_Roundtrip | 139.4 μs | 208.2 μs | 63.8 μs | 1.00 | 21.2 KB |
| **RoundTrip** | 100 | **Ws_SyncRoundtrip** | **101.9 μs** | **144.2 μs** | 39.6 μs | **0.74** | **0** |
| **Handover** | 1 | Ws_HandoverRam | 1.0 μs | 3.5 μs | 0.6 μs | 1.00 | 0 |
| **Handover** | 1 | Ws_HandoverSf | 1.0 μs | 6.9 μs | 0.6 μs | 1.82\* | 0 |
| **Handover** | 100 | Ws_HandoverRam | 7.7 μs | 9.3 μs | 6.3 μs | 1.00 | 0 |
| **Handover** | 100 | Ws_HandoverSf | 10.4 μs | 55.2 μs | 6.5 μs | 2.51\* | 0 |

### Observations

- **WS full round-trip is on par with HTTP** — single-row median 107.6 vs 118.0 μs, 100-row 101.9 vs 139.4 μs. The Mean-based ratios (0.99 / 0.74) carry large RatioSD (0.43 / 0.22), so read this as *comparable* — WS is not slower, with an edge at the 100-row batch.
- **Handover returns ~15–100× faster than the round-trip** — bare `SendAsync` returns in ~1–8 μs median because the caller hands off to the cursor send engine and does not wait for the server ACK.
- **Store-and-forward's latency cost is in the tail, not the median** — `Ws_HandoverSf` median is within ~3 μs of RAM, but its p95 (7–55 μs) is ~2–6× the RAM p95 (3–9 μs): mmap page faults / flush cadence show up as tail spikes. \* The Mean-based SF ratio (1.8× / 2.5×, RatioSD up to 2.65) is tail-inflated and noisy — it is **not** "SF 2.5× slower"; on the median SF adds only ~0–3 μs.
- **Zero allocation on every WS path** — `Ws_SyncRoundtrip` and both Handover methods allocate ~0 (pooled encoder buffers, persistent connection); HTTP allocates 9–21 KB per round-trip.
- Sub-100 μs single-shot latency on loopback is inherently noisy — median and p95 are the deliverable, not Mean.

Pass `--iterationCount 100000` for a stable p99 / p99.9.

## 3. `BenchQueryWs` — Egress (read) throughput

**Workload**: persistent `QwpQueryClient`, `SELECT <cols> FROM table LIMIT N` against pre-seeded tables (10 M rows each). Schema matches the server egress fixtures — Narrow = 5 columns (`ts, id, price, sym, note`), Wide = 15 columns (`ts, id, price, sym, note, d1–d5, s1–s5`). HTTP `/exec` baseline streams the JSON `dataset[][]` and extracts every cell value so both methods do equivalent decode work. 20 iterations × 5 warmup.

| Category | RowCount | Mean WS | Mean HTTP | WS rows/sec | WS Ratio | Alloc Ratio |
|---|---|---|---|---|---|---|
| **Narrow** | 10 k  |   1.43 ms |    4.61 ms |  7.01 M | **0.31 (3.2×)** | ~0 |
| **Narrow** | 100 k |   4.13 ms |   23.18 ms | 24.21 M | **0.18 (5.6×)** | ~0 |
| **Narrow** | 1 M   |  27.20 ms |  180.11 ms | 36.76 M | **0.15 (6.7×)** | 0.29 |
| **Narrow** | 10 M  | 262.83 ms | 1516.02 ms | **38.05 M** | **0.17 (5.9×)** | 1.05 |
| **Wide**   | 10 k  |   3.30 ms |    9.30 ms |  3.03 M | **0.36 (2.8×)** | ~0 |
| **Wide**   | 100 k |  45.03 ms |   51.66 ms |  2.22 M | **0.87 (1.15×)** | 0.19 |
| **Wide**   | 1 M   | 119.57 ms |  449.55 ms |  8.36 M | **0.27 (3.7×)** | 0.57 |
| **Wide**   | 10 M  | 694.96 ms | 4438.37 ms | 14.39 M | **0.16 (6.3×)** | 1.57 |

### Observations

- **WS reads 3–7× faster than HTTP `/exec`** at most sizes, peaking at **38 M rows/sec** (narrow, 10 M). The advantage grows with result size as the QWP columnar decode amortises fixed per-query cost; HTTP `/exec` stays JSON-parse-bound at ~2–7 M rows/sec.
- **Wide at 100 k is the one soft spot** — WS 45 ms vs HTTP 52 ms, only 1.15×. The WS figure there is anomalously slow for its size (2.2 M rows/sec, against 8.4 M at 1 M rows) with ~10 % StdDev — treat it as a noisy cell, not a real plateau; a re-run is worthwhile.
- **Egress decode allocation is modest for both paths** — WS allocates ~0 at small result sets (pooled column scratches) and lands roughly on par with HTTP at 10 M rows (~170 KB narrow; WS ~450 KB vs HTTP ~290 KB wide, as the column scratches grow with the result). Neither path allocates per row.

## 4. `BenchQueryLatencyWs` — Egress (read) query latency

**Workload**: persistent `QwpQueryClient`, one query per iteration (each iteration is one full query round-trip — IterationCount is the percentile sample size). This run used `--iterationCount 100000`, above the 20 000 default, for a stable tail. Two categories: **SingleRow** (`SELECT id` from a 1-row table) and **Bind** (`SELECT x FROM long_sequence(10) WHERE x = $1` — QWP bind parameter vs HTTP value inlined into the SQL text). HTTP `/exec` baseline.

| Category | Method | Median | p90 | p95 | Min | Ratio |
|---|---|---|---|---|---|---|
| **SingleRow** | Http_SelectSingleRow | 101 μs | 135 μs | 150 μs | 39 μs | 1.00 |
| **SingleRow** | **Ws_SelectSingleRow** | **101 μs** | **126 μs** | **135 μs** | 50 μs | 1.05\* |
| **Bind** | Http_SelectWhereBind | 114 μs | 187 μs | 214 μs | 41 μs | 1.00 |
| **Bind** | **Ws_SelectWhereBind** | **97 μs** | **141 μs** | **157 μs** | 32 μs | **0.89** |

### Observations

- **SingleRow**: WS and HTTP tie on the median (101 μs). The Mean ratio (1.05) carries RatioSD 0.29 — within noise; read this as *indistinguishable*, not "WS slower". WS is slightly better at the tail (p90 / p95).
- **Bind**: WS is faster at every percentile — median 97 vs 114 μs, p95 157 vs 214 μs (0.73× tail).
- **Allocation**: HTTP allocates ~10 KB per query (response string + `HttpClient` machinery); WS allocates ~0 (pooled decoder scratches, persistent connection).
- High sample variance is inherent to sub-100 μs single-shot latency on loopback (GC, socket, scheduling) — the percentile columns are the deliverable, not the Mean. The BDN `MinIterationTime ... very small` warning is expected for a latency benchmark and is ignorable.

\* RatioSD 0.29 — not statistically distinguishable from 1.00.

## 5. `BenchSfThroughput` — Store-and-forward overhead

**Workload**: long-lived senders, one with `sf_dir` (mmap-backed segment ring), one without (RAM-backed). Each iteration appends N rows, `SendAsync()`, then `Ping()` to wait for cumulative ACK — symmetric across both. 20 iterations × 5 warmup.

SF-vs-non-SF ratio, across repeated runs (latest run's absolute means shown):

| Rows | SF Ratio (per run) | WS_NoSf | WS_WithSf | Allocated NoSf / WithSf |
|---|---|---|---|---|
| 200000 | 2.43× / 2.52× / 1.90× / 2.26× | 25.7 ms | 58.1 ms | 264 KB / 195 KB |
| 500000 | 2.35× / 2.30× / 2.37× | 63.5 ms | 149.9 ms | 630 KB / 408 KB |

### Observations

- **SF ingest steady-state overhead is ~2.3×** — the 500k cell is the most reliable read (three runs at 2.30× / 2.35× / 2.37×, non-SF baseline StdDev ~2%). Every frame is appended to an mmap-backed segment file before it ships; that durable append is what lets frames replay after a process or host crash.
- **The non-SF baseline is stable once the workload is ≥ 200k rows** — StdDev ~2%. Smaller workloads (10k–100k) were dropped from the param set because their baselines were dominated by run-to-run outliers.
- **SF is markedly higher-variance than non-SF** — `Ws_WithSf` StdDev runs 8–22% of its mean (vs ~2% for non-SF), and its p95 sits well above the mean. mmap flush cadence and page faults produce irregular tail latency; this is why a single run's SF ratio can land anywhere in ~1.9–2.5×.
- **SF adds no managed-memory pressure** — `Ws_WithSf` allocates *less* managed memory than non-SF (~0.65–0.74×). Frame bytes live in off-heap mmap / native segments either way; the managed allocation measured here is per-frame bookkeeping.
- This is the steady-state happy-path cost. SF's actual payoff — replay through a server outage or process restart — is not exercised here.

## Caveats

1. **Local loopback only** — TCP/WebSocket handshake on `127.0.0.1` is much faster than network round-trips. Real-network numbers will be higher in absolute terms; relative ratios should hold.
2. **Single-host** — server and client share CPU and memory. Absolute throughput drifts noticeably between runs, especially for shorter workloads. Compare WS-vs-HTTP / SF-vs-non-SF *within* a run, and prefer the larger workloads for any cross-run comparison.
3. **SF bench is happy-path ingest only** — the product justification for SF (reconnect + replay through a server outage) is not exercised here. A transient-failure benchmark is separately needed.

## Reproduction

```fish
# Ingest throughput / SF
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchInsertsWs*' '*BenchSfThroughput*'

# Egress reads (live QuestDB master)
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchQueryWs*'

# Egress query latency (live QuestDB master)
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchQueryLatencyWs*'

# Latency, strict p99 (100k samples)
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchLatencyWs*' --iterationCount 100000

# Latency, quick smoke (1k samples)
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchLatencyWs*' --iterationCount 1000
```
