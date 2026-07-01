# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

.NET client library for QuestDB. Covers both **ingestion** (HTTP / TCP / WS) and **read-side egress** (QWP over WS). Three ingest transports plus a dedicated query client:

- **HTTP / HTTPS** — InfluxDB Line Protocol (ILP), recommended for most ingest workloads.
- **TCP / TCPS** — ILP over raw TCP, ECDSA P-256 auth, kept for low-overhead deployments.
- **WS / WSS (QWP) — ingest** — QuestDB's binary **columnar** wire protocol over
  WebSocket (`/write/v4`). Higher throughput than ILP for wide rows, exposes the full
  QuestDB type system (int8/int16/int32, float32, char, date,
  timestamp-nanos, uuid, varchar, geohash, decimal128, long256, double
  arrays, long arrays, Gorilla DoD timestamp compression). Always routed
  through the **cursor send engine**: every appended frame lands in a segment
  ring (RAM-backed by default, mmap-backed when `sf_dir` is set), is shipped
  asynchronously, and is replayed on transient WS reconnects. Setting
  `sf_dir` switches the segment backing to mmap files so frames survive a
  process restart.
- **WS / WSS (QWP) — egress** — read-side WebSocket (`/read/v1`) that
  streams query results as binary `RESULT_BATCH` frames. Surfaced through
  a separate `QueryClient.New(...)` factory (not `Sender.New`) returning
  `IQwpQueryClient`. Distinct connect-string parser (`QueryOptions`) with
  egress-specific keys: `target=any|primary|replica`,
  `compression=auto|raw|zstd`, `failover_*`, `initial_credit`. Decoder
  pools per-column scratches across batches; bind-parameter wire format
  is pinned by per-type byte vectors in `QwpBindValuesVectorsTests`.

NuGet package id: `net-questdb-client`. Multi-targets `net6.0;net7.0;net8.0;net9.0;net10.0`. The
`ws::`/`wss::` (QWP) sender and the egress query client both require **net7.0+** because they
depend on `ClientWebSocket.HttpResponseMessage` for header-aware handshake; HTTP
and TCP senders work on every supported target.

## Commands

```bash
# Build the whole solution.
dotnet build net-questdb-client.sln -c Release

# Full test pass excluding the three integration suites and the integration fuzz
# (which need a live QuestDB). Mirrors the CI filter applied to every leg except
# Linux + net9.0.
dotnet test src/net-questdb-client-tests/net-questdb-client-tests.csproj \
  --framework net10.0 -c Release \
  --filter "FullyQualifiedName!~QuestDbIntegrationTests&FullyQualifiedName!~QuestDbWebSocketIntegrationTests&FullyQualifiedName!~QuestDbQueryIntegrationTests"

# Run a single test or namespace via NUnit's name filter.
dotnet test src/net-questdb-client-tests/net-questdb-client-tests.csproj \
  --framework net10.0 -c Release \
  --filter "FullyQualifiedName~QwpEncoder"

# Integration tests. All three suites (HTTP/TCP, WebSocket ingest, Egress query)
# plus the integration fuzz suites (FullyQualifiedName~QuestDb & ~Fuzz) are plain
# `[TestFixture]` and lean on `QuestDbManager`, which boots QuestDB as a local
# java process — no Docker. The WS / egress endpoints (`/write/v4`, `/read/v1`)
# only exist on master, so point QuestDbManager at a built master repo via
# `QUESTDB_REPO`, or a built jar via `QUESTDB_JAR`. `java` resolves from
# `JAVA_HOME` or `PATH`. `QDB_LIVE_HTTP` / `QDB_LIVE_ILP` skip launching entirely
# when pointed at an already-running instance.
QUESTDB_REPO=/path/to/built/questdb \
  dotnet test src/net-questdb-client-tests/net-questdb-client-tests.csproj \
  --framework net10.0 -c Release \
  --filter "FullyQualifiedName~QuestDbIntegrationTests|FullyQualifiedName~QuestDbWebSocketIntegrationTests|FullyQualifiedName~QuestDbQueryIntegrationTests"

# Single-suite drilldowns:
QUESTDB_REPO=/path/to/built/questdb \
  dotnet test ... --filter "FullyQualifiedName~QuestDbWebSocketIntegrationTests"
QDB_LIVE_HTTP=127.0.0.1:9000 QDB_LIVE_ILP=127.0.0.1:9009 \
  dotnet test ... --filter "FullyQualifiedName~QuestDbQueryIntegrationTests"

# Benchmarks. Names match the BenchmarkDotNet 0.13 filter syntax
# (`Param: value` colon-space, NOT the [Param=value] display format).
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchInsertsWs*' '*BenchSfThroughput*'

# Egress throughput bench. Requires a live master endpoint.
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchQueryWs*'

# Example apps under src/example-* are compilable demos referenced by
# examples.manifest.yaml (rendered on questdb.io). Keep paths and file
# names stable when editing.
dotnet run --project src/example-qwp-ingest --framework net10.0
```

There is no Makefile. The CI definition is `azure-pipelines.yml` at the
repo root; it runs `dotnet test` against net8.0/net9.0 on Linux and
Windows runners.

## Architecture

### Senders and entry points

The public surface is the `ISender` interface in
`src/net-questdb-client/Senders/ISender.cs`. All concrete senders
(`HttpSender`, `TcpSender`, `QwpWebSocketSender`) implement it. QWP
adds a **superset** interface `IQwpWebSocketSender : ISender` with
QWP-only methods (`Ping`, `GetHighestAckedSeqTxn`,
`GetHighestDurableSeqTxn`); callers wanting those cast the returned
`ISender` to `IQwpWebSocketSender`.

Two factories are the only entry points:

- `Sender.New(string confStr)` — parses a config string via
  `SenderOptions(confStr)` (`Utils/SenderOptions.cs`). Supported schemes:
  `http`, `https`, `tcp`, `tcps`, `ws`, `wss`.
- `Sender.New(SenderOptions options)` / `options.Build()` — programmatic
  configuration. Both paths funnel through `Sender.New(SenderOptions)`,
  which calls `options.EnsureValid()` (auth, TLS, multi-addr, gzip,
  WS-only-keys-on-non-WS heuristic, auto-flush normalisation) before
  dispatching on `options.protocol`.

`HttpSender` and `TcpSender` extend `AbstractSender`; the buffer is the
ILP-row-oriented `IBuffer` (`Buffer.cs`, `BufferV1.cs`, `BufferV2.cs`)
with three protocol versions (V1 text-only, V2 adds binary `float64` and
n-dimensional `float64` arrays, V3 adds decimals). `QwpWebSocketSender`
does **not** extend `AbstractSender` — QWP is multi-table columnar and
manages its own per-table column buffers (`QwpTableBuffer`) plus a
schema cache; the `IBuffer` text-row model doesn't fit.

HTTP auto-negotiates protocol version via `/settings`; TCP requires
`protocol_version=2|3` to opt into V2/V3. When adding a new ILP column
type expect to touch `IBuffer`/`BufferV1`/`BufferV2`, all three V1/V2/V3
HttpSender variants, the TcpSender variants, the `ISender`/`ISenderV1`/
`ISenderV2` interfaces, and the sender tests under `BufferTests`,
`HttpTests`, `LineTcpSenderTests`.

### QWP (WebSocket columnar protocol)

QWP is not a version of ILP — it is a distinct binary protocol with its
own framing, codecs, and server handshake. Everything QWP lives in
`src/net-questdb-client/Qwp/`:

- `QwpConstants.cs` — magic (`"QWP1"`), header flags (Gorilla, delta
  symbol dictionary), type codes, ACK status codes. Schemas are always
  inline — there is no schema-mode byte or schema-id on the wire.
  Self-sufficient framing is a client-side mode (full symbol dict per
  frame), not a wire flag.
- `QwpVarint.cs` / `QwpBitWriter.cs` — wire primitives. Unsigned LEB128
  varints capped at 10 bytes (`MaxBytes`) with strict overflow rejection.
  `QwpBitWriter` / `QwpBitReader` are LSB-first bit-packers with upfront
  capacity validation so all-zero bitstreams can't silently advance past
  the buffer end.
- `QwpColumn.cs` — per-type columnar storage. Fixed-width types share
  `FixedData`/`FixedLen`; varchar uses `StrOffsets`/`StrData`/`StrLen`;
  symbols use `SymbolIds` (global dict ids); booleans bit-pack into
  `BoolData`. Null tracking via lazy `NullBitmap` allocated only when a
  null appears. Decimal scale and geohash precision are locked on first
  non-null write. Arrays serialise into `FixedData` as
  `[u8 nDims][i32...]shape[values]`.
- `QwpTableBuffer.cs` — per-table buffer. Owns an ordered `List<QwpColumn>`
  + designated-timestamp column slot. Per-row state machine: caller
  invokes `AppendXxx` to set values, then `At*` to commit (null-pads
  untouched columns). Failure of any single Append rolls back the
  *entire* row via `CancelCurrentRow()` (`QwpColumn.Savepoint` per
  touched column + drop columns added since `_committedColumnCount`),
  so the caller sees consistent buffer state on any error path.
- `QwpEncoder.cs` — assembles the multi-table QWP frame from a set of
  table buffers in one flush. Each table block carries its column schema
  inline (no schema-id / schema-mode byte); the encoder is stateless
  across flushes.
- `QwpSymbolDictionary.cs` — global symbol-id allocation + delta
  encoding. The ingest sender always encodes in self-sufficient mode
  (`selfSufficient: true`), re-emitting the full symbol dict on every
  frame so the cursor engine can replay un-acked frames after a
  reconnect without server-side cache state.
- `QwpGorilla.cs` — delta-of-delta timestamp compression. The encoder
  emits a 1-byte encoding flag (`0x00` uncompressed, `0x01` Gorilla)
  only when `FLAG_GORILLA` is set on the message header. Falls back to
  uncompressed when the column has < 3 non-null values or any DoD
  exceeds int32. Always-on; no opt-in flag.
- `QwpResponse.cs` — ACK / error frame parser. Strict UTF-8 (throws on
  invalid bytes) for error messages and per-table names; rejects empty
  table names, lying lengths, and trailing bytes after the last entry.
- `QwpWebSocketTransport.cs` — thin wrapper over
  `System.Net.WebSockets.ClientWebSocket`. Performs the `/write/v4`
  (ingest) or `/read/v1` (egress) upgrade — path is parameterised via
  `QwpWebSocketTransportOptions.Path`. QWP version-negotiation headers
  (`X-QWP-Max-Version`, `X-QWP-Client-Id`); optional dump stream
  records binary frames in both directions, serialised under
  `_dumpLock` because send/receive run concurrently.
- `Utils/QwpTlsAuth.cs` — shared `BuildAuthHeader` (Basic auth / raw
  header) and `BuildCertificateValidator` (TLS verify + custom CA
  PFX). Used by both the ingest sender and the egress query client so
  TLS / auth behaviour stays consistent.
- `Qwp/Query/` — egress (read-side) subsystem. Distinct entry surface
  (`QueryClient.New`) and codec from the ingest encoder; shares only
  wire primitives + `Utils/QwpTlsAuth.cs`.
  - `QueryOptions.cs` — egress connect-string parser. Disjoint from
    `SenderOptions` (egress fields like `target=`, `compression=`,
    `failover_*`, `initial_credit` don't bleed into ingest validation).
  - `QwpQueryWebSocketClient.cs` — owns the WS connection
    (`/read/v1` upgrade), I/O loop, and per-query state machine.
    Sends `QUERY_REQUEST` / `CANCEL` / `CREDIT` frames *without* the
    12-byte QWP1 header (asymmetric framing: server→client wraps the
    header, client→server is payload-only with `msg_kind` at byte 0).
    `AuthError` is immediately terminal — no failover retry.
  - `QwpResultBatchDecoder.cs` — decodes `RESULT_BATCH` payloads
    column-major. Per-column scratches (`ValueBytes`, `StringHeap`,
    `NonNullIndex`, `StringOffsets`) survive `ColumnView.Reset()` and
    grow-and-reuse across batches. ColumnView slots themselves reuse
    via `ConfigureColumn` / `TrimToColumnCount`, so a fresh batch on
    the same schema does not churn schema metadata.
  - `QwpColumnBatch.cs` / `QwpColumnBatchHandler.cs` — column-major
    view + abstract handler the user implements. Span-returning
    accessors (`GetStringSpan`) are valid only for the duration of
    `OnBatch`.
  - `QwpBindValues.cs` / `QwpBindSetter.cs` — typed bind-parameter
    builder. 18 wire types, ascending-index validation, decimal scale
    + geohash precision range checks. Wire-format byte layout pinned
    per type in `QwpBindValuesVectorsTests`.
  - `QwpEgressConnState.cs` — per-connection symbol dict + schema
    registry. State spans multiple batches so it lives outside
    `QwpColumnBatch`.
  - `QwpServerInfo.cs` / `QwpRoleMismatchException.cs` — server
    identity + the `target=` filter rejection. SERVER_INFO is emitted
    unconditionally on connect; failover walks `addr=` candidates
    filtering against role.
- `Senders/QwpWebSocketSender.cs` — owns the lifecycle. Single
  execution path through `QwpCursorSendEngine` regardless of mode — the
  engine uses pipelined double-buffering bounded by the FSN ring
  capacity rather than a numeric in-flight window:
  - **RAM mode** (default, `sf_dir` unset): the engine sits over a
    memory-backed `QwpSegmentRing` (`OpenMemoryBacked`) of
    `QwpMemorySegment`s, capped at `sf_max_total_bytes` (default
    128 MiB; 4 MiB per segment). No persistence; segments are
    `NativeMemory.Alloc`'d and freed on trim.
  - **SF mode** (`sf_dir=...` set): the engine sits over a
    file-backed `QwpSegmentRing` (`Open`) of mmap'd
    `QwpMmapSegment`s, capped at `sf_max_total_bytes` (default
    10 GiB). Frames survive process crashes and replay on next
    startup.
  - In both modes a wire failure (server close, transient I/O,
    timeout) triggers `QwpReconnectPolicy` backoff. The sender only
    becomes terminal on auth/upgrade-reject, protocol violation, or
    reconnect-budget exhaustion — `flush()` does **not** throw on
    transient disconnects.

### Cursor send engine + segment backings

Lives under `Qwp/Sf/`. The cursor engine is the universal hot path;
segments are an abstraction (`IQwpSegment`) over either
`QwpMmapSegment` (file-backed, used when `sf_dir` is set) or
`QwpMemorySegment` (RAM-backed, used when `sf_dir` is null).

- `IQwpSegment.cs` — common segment contract used by the ring.
  Implementations: `QwpMmapSegment` (file-backed) and
  `QwpMemorySegment` (RAM-backed).
- `QwpFiles.cs` — exclusive-locking file ops. `OpenExclusive` /
  `TryOpenExclusive` use `FileShare.None` as a portable advisory lock
  (held for the lifetime of the returned `FileStream`). SF is documented
  as **local filesystem only** — `FileShare.None` is unreliable on NFS
  / SMB. `LooksLikeNetworkPath` heuristic guards `QwpSlotLock.Acquire`,
  which throws when the slot path is on a UNC mount.
- `QwpSlotLock.cs` — per-sender lock file (SF mode only). `Acquire` uses
  `TryOpenExclusive`; liveness is signalled by holding the OS file lock
  for the slot's lifetime — release on process exit (graceful or crashed)
  is what makes the slot eligible for adoption by the orphan scanner.
  A best-effort `.heartbeat` mtime is refreshed by the segment manager
  every ~1s for observability; it is not consulted by the lock contention
  test.
- `QwpMmapSegment.cs` — single mmap'd segment file with envelope frames
  `[u32 crc32c][u32 frame_len][frame bytes]`. Replays on open via
  `ScanForLastGoodEnvelope`, which walks every envelope verifying its
  CRC32C and stops at the first torn tail (oversized length, declared
  length past EOF, or CRC mismatch), truncating it. `Seal()` only marks
  the segment immutable and flushes — there is no recovery sidecar, so
  the `.sfa` file stays byte-identical to the spec layout.
- `QwpMemorySegment.cs` — RAM-backed segment via
  `NativeMemory.Alloc` / `Free`. Same envelope wire format as mmap
  (preserves CRC verification on read); recovery and replay don't apply.
- `QwpSegmentRing.cs` — ring of active + sealed segments. File-backed
  rings (`Open`) use a hot-spare-path mechanism (manager pre-creates
  `.tmp` files, producer `File.Move`s them into place) so producer
  never blocks on disk allocation. Memory-backed rings
  (`OpenMemoryBacked`) skip the hot-spare path: producer allocates
  inline (cheap), and the cap is enforced directly via
  `SetMaxTotalBytes`. `IsMemoryBacked` flips many conditional paths.
- `QwpSegmentManager.cs` — manager thread: heartbeat-driven (~1s)
  plus callback-driven. File mode: provisions hot spares, trims
  acked segments by unlinking files, refreshes the slot heartbeat,
  flushes the active mmap segment to bound the host-crash data-loss
  window. Memory mode: only trims (free instead of unlink); spare
  provisioning is a no-op.
- `QwpCrc32C.cs` — software slice-by-8 CRC32C, reflected polynomial
  `0x82F63B78`. Deliberately avoids `System.IO.Hashing.Crc32C` and
  hardware intrinsics so output is bit-identical across runtime
  versions and CPU architectures, which envelope verification by
  peer clients depends on.
- `QwpCursorSendEngine.cs` — pipelined send + receive pumps over a
  reconnecting transport. State guarded by `_stateLock`; awaiters
  signalled via `_appendSignal` / `_ackSignal` (TaskCompletionSource
  with `RunContinuationsAsynchronously`). Both signals are fired via
  `Task.Run(() => prev.TrySetResult(true))` to bounce off the lock
  holder's stack — direct `TrySetResult` triggered an intermittent
  Linux + .NET 9 deadlock under `Task.WaitAsync` continuation chains
  in `EndToEnd_Sf_SingleRow_FrameReachesServerAndIsSelfSufficient`.
- `QwpReconnectPolicy.cs` — exponential backoff with saturation on
  ticks (avoids `long` overflow when `InitialBackoff` is days-scale).
- `QwpOrphanScanner.cs` / `QwpBackgroundDrainer.cs` /
  `QwpBackgroundDrainerPool.cs` — adopts crashed sibling slots (other
  `sender_id`s under the same `sf_dir`) and drains their pending
  segments. Drainer pool uses a two-phase shutdown that **leaks the
  semaphore** rather than risk `ObjectDisposedException` on late
  WaitAsync/Release from un-joined drainer tasks.
- `SfCleanup.cs` — best-effort exception swallower for cleanup paths.
  Recurses into `AggregateException` so a wrapped real failure isn't
  silently masked by the cleanup-error allowlist.

### Config string reference

`Utils/SenderOptions.cs` is the single source of truth. Notable
behaviours:

- `username` / `password`: Basic auth for HTTP **and WS**; for TCP,
  `username` is the ECDSA key id (kid) and `token` is the secret —
  **`username` + `token` together is valid for TCP** (mutually
  exclusive only for HTTP/WS). `ValidateAuthCombination` checks
  `IsTcp()` first and returns early.
- `token_x` / `token_y`: silently accepted for cross-client config-string
  interop; ignored at runtime.
- `request_durable_ack`, `sf_*`, `reconnect_*`, `initial_connect_retry`,
  `initial_connect_mode`, `close_flush_timeout_millis`, `drain_orphans`,
  `max_background_drainers`, `sender_id`, `ping_timeout`, the `on_*_error`
  policy keys, and the egress-only keys (`target`, `compression`,
  `failover_*`, `initial_credit`): WS-only. Rejected on non-WS schemes
  via `ValidateWebSocketKeys` (string-ctor path) or
  `ValidateWebSocketKeysAgainstDefaults` (programmatic-init path,
  default-comparison heuristic).
- `auto_flush=off` zeros `auto_flush_rows` / `auto_flush_bytes` /
  `auto_flush_interval` to `-1`. WS-specific defaults
  (`auto_flush_rows=1000`, `auto_flush_bytes=8 MiB`, `auto_flush_interval=100ms`)
  only apply when `auto_flush != off` — `auto_flush=off` is honoured even for ws.
  The WS byte budget is further clamped at runtime to 90% of the server's
  advertised `X-QWP-Max-Batch-Size` (when present).
- `tls_verify=unsafe_off` accepts any server cert (dev / self-signed
  only — never ship to prod).
- `tls_roots`, `tls_roots_password`: PFX path + optional password for
  pinning a custom CA bundle.
- Multiple `addr=` entries are supported on HTTP/HTTPS (failover via
  `AddressProvider`) and on WS/WSS (role-aware skipping via
  `QwpHostHealthTracker`). TCP/TCPS reject multi-addr.
- `gzip=on` rejected for ws/wss (binary protocol; the WS-only key
  check is value-based so it works for both string-ctor and
  programmatic init paths).
- `ToString()` skips WS-only keys when the protocol is non-WS so the
  output round-trips through `new SenderOptions(s.ToString())`.

### Connection pooling

A single `ISender` (or `IQwpQueryClient`) is **not** thread-safe — one per
producer thread. For multi-threaded producers there is now an in-tree pool,
`QuestDBClient` (mirrors the Java client's `QuestDB` handle), which pools
**both** ingest senders and (net7.0+) egress query clients. Files live in
`Pooling/` (public `QuestDBClient`/`IQuestDBClient`/`QuestDBClientBuilder` in
namespace `QuestDB`; internal `SenderPool`/`PooledSender`/`BorrowedSender`/`QueryClientPool`/
`PooledQueryClient`/`PoolHousekeeper`/`QuestDBClientImpl` in `QuestDB.Pooling`).
The public `Query` builder (namespace `QuestDB`, `Query.cs`) is the query-side
surface.

- **Entry points** mirror `Sender`/`ISender`: `QuestDBClient.Connect(confStr)`
  or `QuestDBClient.Builder()…Build()` returns `IQuestDBClient`. Construct
  once, share across threads. For distinct ingest/query endpoints use
  `QuestDBClient.Connect(ingestConfStr, queryConfStr)` or
  `Builder().IngestConfig(...).QueryConfig(...)` (Java `connect(ingest, query)`
  parity); a single `ws`/`wss` `FromConfig` string serves both pools.
- **Borrow / return**: `BorrowSender()` (+ `BorrowSenderAsync`) returns an
  `ISender` that is a fresh per-borrow `BorrowedSender` handle wrapping a
  reusable `PooledSender` pool entry. **Dispose does not send** — it is pure
  resource release: it discards any buffered-but-unsent rows (`Clear`),
  **returns the entry to the pool** (it does NOT close the underlying sender),
  and **never throws**. Call `Send()`/`SendAsync()` to hand rows to the
  transport, and `Flush(timeout)`/`FlushAsync(timeout)` (drain = send + await
  ACK) for delivery confirmation, **before** disposing; a sender whose buffer
  can't be cleared (terminally failed) is discarded instead of re-pooled.
  **Pooled WS `Send()` is fast flush-to-ring** (the pool sets
  `SenderOptions.SendAwaitsAck=false`): the pooled connection ships async after
  return and delivery is confirmed via the pool-wide `Flush` below — unlike a
  **standalone** WS `Send()`, which drains (`SendAwaitsAck=true` default) so
  `Send()`-before-`Dispose` delivers on its own. The
  handle is **use-after-return safe**: once disposed every ingest member throws
  `ObjectDisposedException` (so a stale reference can't alias the entry a later
  borrower now holds), and a second dispose is a tolerated no-op. There is no
  context-affine / pinned-sender API — borrow per unit of work and dispose to
  return (a single `ISender` is not thread-safe, so never share a borrowed one
  across threads).
- **Pool-wide drain**: `IQuestDBClient.Flush(timeout)` / `FlushAsync(timeout)`
  (→ `SenderPool.Flush`) fans `ISender.Flush` across every pooled sender — the
  quiescence barrier for "confirm everything landed, then mark done". Call it
  after all borrowed senders are returned; it does not synchronise against a
  concurrent borrow. Returns `true` only if every sender drained within the
  timeout. `close_flush_timeout_millis` is the default timeout for the no-arg
  `Flush()` overloads (it no longer governs Dispose, which does not drain).
- **Sizing**: elastic between `sender_pool_min` and `sender_pool_max`,
  bounded by a `SemaphoreSlim` capacity gate (counts in-use senders;
  creation happens outside the lock). `BorrowSender` blocks up to
  `acquire_timeout_ms` then throws `IngressError(ErrorCode.PoolExhausted)`.
  `PoolHousekeeper` (a `PeriodicTimer` background task) reaps idle /
  over-age senders down to `min`.
- **Config keys** (all-protocol, on `SenderOptions`, `[JsonIgnore]`d out of
  `ToString` so a plain sender round-trips byte-identically): `sender_pool_min`
  (1), `sender_pool_max` (4), `acquire_timeout_ms` (5000), `idle_timeout_ms`
  (60000), `max_lifetime_ms` (1800000), `housekeeper_interval_ms` (5000).
  Validated by `SenderOptions.ValidatePoolOptions()`.
- **Query pooling (net7.0+)**: `IQuestDBClient.NewQuery()` returns a fresh
  `Query` builder (`Sql`/`Binds`/`Handler` then `ExecuteAsync`/`Execute`);
  `ExecuteSqlAsync(sql, handler, ct)` is the convenience shortcut. There is
  **no** public borrow/release for queries — the client lease is implicit per
  `ExecuteAsync` and self-returns in a `finally` (Java `newQuery()`/
  `executeSql()` parity; Java's thread-local `query()` is **not** ported). The
  `Task` returned by `ExecuteAsync` replaces Java's `Completion`. A clean return
  re-pools the client (`GiveBack`); any throw — transport/protocol or a hard
  `CancellationToken` cancel (which is permanently terminal) — discards it
  (`MarkBroken` → `DiscardBroken`), since the egress client's terminal state is
  sticky with no reset. `Query.Cancel()` is the **cooperative** path (forwards to
  the in-flight client's `Cancel()`; the query ends normally and the client is
  re-pooled). `PooledQueryClient` also checks an internal `IPooledQueryClientInner.
  IsTerminalOrDisposed` seam (implemented by `QwpQueryWebSocketClient`) before
  re-pooling. The query pool is the **stripped sibling** of `SenderPool` — no
  SF/slot/leak-debt machinery (read side has no store-and-forward) and no
  AsyncLocal pin. Query-pool config: `query_pool_min` (1), `query_pool_max` (4)
  on `SenderOptions` (`[JsonIgnore]`, in `QwpConnectStringKeys.Shared` so both
  `SenderOptions` and `QueryOptions` accept them), validated by the separate
  `ValidateQueryPoolOptions()` (kept out of `EnsureValid` so a plain `Sender`
  stays lenient); the acquire/idle/lifetime/housekeeper knobs are **shared** with
  the sender pool, and one `PoolHousekeeper` sweeps both. The query pool is built
  only when a `ws`/`wss` query config is present (single `ws` string, explicit
  `QueryConfig`, or `Connect(ingest, query)`); an `http`/`tcp` ingest handle with
  no query config throws `IngressError(ConfigError)` on `NewQuery`. All query
  members are gated `#if NET7_0_OR_GREATER` (so `IQuestDBClient` differs by TFM);
  net6.0 keeps the sender-only surface.
- **Store-and-forward**: when pooling `ws::`+`sf_dir`, each pooled sender
  gets a distinct slot identity `sender_id = <base>-<index>` (via
  `SenderPool.ApplySlotIdentity`) so siblings never collide on a slot
  directory / flock. A discarded/reaped sender's index is freed only after
  `IPooledSlotSender.IsSlotLockReleased` confirms the lock dropped (a
  net-agnostic seam implemented by `QwpWebSocketSender`, backed by
  `QwpSlotLock.IsReleased`); otherwise it is **retired** (`leakedSlots`)
  and its capacity permit retained, shrinking effective `max`. This shrink
  is **not permanent**: when a wedged/deferred engine teardown
  (`QwpCursorSendEngine.Dispose` defers `ReleaseSharedResources` past its 5 s
  pump-join budget) leaves the lock still held at dispose time, the retired
  sender is parked on `_retired` and the housekeeper's `ReclaimRetiredSlots`
  re-tests `IsInnerSlotLockReleased` each sweep — once it flips true the index
  is freed and the withheld permit returned (cancelling pending leak debt, or
  `_capacity.Release()` if the debt was already settled). So `leakedSlots` is a
  live gauge of currently-retired slots, not a monotonic counter. Pooled
  senders pass their managed family to `QwpOrphanScanner.ClaimOrphans(...,
  managedBase, managedCount)` so orphan adoption skips live/future siblings
  but still drains true out-of-family orphans (when `drain_orphans=on`).
  In-range stranded slots recover lazily — a pooled sender replays its own
  slot's segments on open (`QwpSegmentRing.Open`) when the index is
  (re)allocated. **Known limitation:** unlike the Java pool, there is no
  proactive housekeeper-driven two-pass startup drain of in-range slots
  that are not currently allocated (e.g. a pool that permanently shrank);
  their data stays on disk until a future run reallocates the index.
- HTTP still shares `HttpClient`s under the hood per address inside
  `HttpSender`; the pool is layered above the `ISender` seam and is
  protocol-agnostic.

### Value types

- `Decimal` is the BCL 96-bit `System.Decimal`; `IBuffer.Column(name,
  decimal)` and `QwpColumn.AppendDecimal128` both take it. The QWP wire
  format is fixed-width Decimal128 (16 bytes, two's-complement signed
  scale-locked on first non-null write).
- Arrays for QWP are `ReadOnlySpan<double>` / `ReadOnlySpan<long>` plus
  a `ReadOnlySpan<int>` shape. Total element count is bounded by the
  caller; the encoder writes `[u8 nDims][i32]*nDims[values]` as a
  single packed payload into `FixedData`.

## Testing layout

- Unit tests in `src/net-questdb-client-tests/`:
  - `BufferTests.cs`, `HttpTests.cs`, `LineTcpSenderTests.cs`,
    `MultiUrlHttpTests.cs`, `SenderOptionsTests.cs` — ILP / config /
    failover, no Docker.
  - `Qwp/QwpEncoderTests.cs`, `Qwp/QwpColumnTests.cs`,
    `Qwp/QwpTableBufferTests.cs`, `Qwp/QwpVarintTests.cs`,
    `Qwp/QwpResponseTests.cs`, `Qwp/QwpSymbolDictionaryTests.cs`,
    `Qwp/QwpGorillaTests.cs`, `Qwp/QwpWebSocketTransportTests.cs`,
    `Qwp/QwpWebSocketSenderTests.cs` — QWP-side unit + component tests
    using `DummyQwpServer` (`src/dummy-http-server/DummyQwpServer.cs`)
    as a Kestrel-backed in-process WebSocket server.
  - `Qwp/Sf/Qwp*Tests.cs` — store-and-forward subsystem tests
    (file ops, segment ring/manager, drainers, cursor engine, reconnect
    policy, slot lock, orphan scanner).
  - `Qwp/Query/*Tests.cs` — egress unit + e2e tests against
    `DummyQwpServer` configured with `/read/v1` (`QueryOptionsTests`,
    `QwpResultBatchDecoderTests`, `QwpBindValuesTests`,
    `QwpRoleFilterTests`, `QwpQueryClientEndToEndTests`).
    `QwpBindValuesVectorsTests` pins the bind-payload byte layout
    per wire type.
  - `Utils/QwpTlsAuthTests.cs` — auth / TLS helper unit tests shared
    by ingest and egress.
- Integration tests — the three suites plus the integration fuzz suites
  (`QuestDb*FuzzTests`) are plain `[TestFixture]` (no `[Explicit]`),
  bootstrapped by `QuestDbManager`, which launches QuestDB as a local java
  process (`java -p questdb.jar -m io.questdb/io.questdb.ServerMain`) — no
  Docker. CI excludes the three suites by FQN on every leg except Linux +
  net9.0; the integration fuzz self-skips (`Assert.Ignore`) when no QuestDB
  build is configured. The WS (`/write/v4`) and egress (`/read/v1`) endpoints
  only exist on master, so the jar must be a master build: set `QUESTDB_REPO`
  (a built QuestDB repo — `core/target/**/questdb*-SNAPSHOT.jar`) or
  `QUESTDB_JAR` (a direct jar path); `java` resolves from `JAVA_HOME` / `PATH`.
  `QDB_LIVE_HTTP` / `QDB_LIVE_ILP` skip launching when pointed at an existing
  instance. CI clones + `mvn` builds QuestDB master (see `ci/azure-pipelines.yml`).
  - `QuestDbIntegrationTests.cs` — HTTP/TCP integration.
  - `QuestDbWebSocketIntegrationTests.cs` — ingest WS/QWP integration.
  - `QuestDbQueryIntegrationTests.cs` — egress integration. `OneTimeSetUp`
    drops + re-seeds fixture tables so runs are idempotent against a
    long-lived master instance.
  - `QuestDbWebSocketIngestFuzzTests.cs` + `QuestDbEgress{,Bind,Alter,Fragmentation}FuzzTests.cs`
    — QWP ingress/egress fuzz suites (ports of the Rust client's fuzz tests).
    Offline egress fuzz that needs no server lives in
    `Qwp/Query/QwpEgress{Bounds,Fragmentation}FuzzTests.cs`. Seeds print to the
    test output; reproduce via `QWP_FUZZ_SEED` / `QWP_WS_FUZZ_SEED` /
    `QWP_EGRESS_FUZZ_SEED`.
- `JsonSpecTestRunner.cs` — shared ILP conformance vectors
  (`Json/specs/*.json`) driven via the `RunHttp` / `RunTcp`
  `[TestCaseSource]` parameterisation.
- Benchmarks in `src/net-questdb-client-benchmarks/` (BenchmarkDotNet):
  `BenchInsertsWs`, `BenchLatencyWs`, `BenchSfThroughput`,
  `BenchSfAppend`, `BenchQueryWs`, plus the legacy ILP benches. The
  ingest QWP suite uses
  `[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory,
  BenchmarkLogicalGroupRule.ByParams)]` with
  `[BenchmarkCategory("Narrow"|"Wide"|"MultiTable")]` so each row
  shape compares against its own HTTP baseline. `BenchQueryWs` uses
  the same grouping (Narrow / Wide × 10k / 100k / 1M rows) and an
  HTTP `/exec` baseline that parses `dataset[][]` so both methods do
  equivalent extraction work; it declares its own job via
  `[Config(typeof(QueryThroughputConfig))]` (20 iter × 5 warmup).
  Senders / clients live in `[GlobalSetup]` so per-invocation cost
  is the wire path, not handshake / mmap / engine spin-up.

## Conventions

- Apache-2.0 license banner at the top of every `.cs` file. Copy from
  any existing source when adding a new file.
- Internal helpers are `internal` and tests reach them via
  `[InternalsVisibleTo("net-questdb-client-tests")]` declared in
  `net-questdb-client.csproj`. Don't make production code public to
  satisfy a test — extend the friend list instead.
- Comments default to **none**. Only add a one-liner when the *why* is
  non-obvious (a hidden constraint, a counter-intuitive ordering, a
  workaround for a runtime quirk). Don't restate what the code does;
  don't reference plan documents (e.g. "per Phase 4 §9") because those
  references rot once the plan ships.
- Errors from `IBuffer` / `ISender.Column*` ILP methods are **latched
  on the buffer** — the fluent API keeps returning the same sender,
  the error surfaces on the next `At` / `AtNow` / `Flush`. `QwpTableBuffer`
  takes a stricter line: any `AppendXxx` failure aborts the entire
  in-progress row (`CancelCurrentRow`) so the buffer never carries a
  partially-applied row.
- QWP frames are **always self-sufficient**: every frame carries the
  full schema + full symbol-dictionary delta, regardless of whether
  `sf_dir` is set. There is no reference-mode schema reuse on the
  ingest path. This is what lets the cursor engine replay un-acked
  frames after a transient WS reconnect (and lets segment files
  replay against a fresh server in SF mode) without server-side
  cache state.
- The WS sender requires net7.0+. Gate any new WS-related code behind
  `#if NET7_0_OR_GREATER` if it touches `ClientWebSocket`-specific APIs;
  HTTP and TCP code must continue to compile on net6.0.
