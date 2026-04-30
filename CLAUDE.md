# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

.NET client library for QuestDB ingestion. Three transports are supported:

- **HTTP / HTTPS** — InfluxDB Line Protocol (ILP), recommended for most workloads.
- **TCP / TCPS** — ILP over raw TCP, ECDSA P-256 auth, kept for low-overhead deployments.
- **WS / WSS (QWP)** — QuestDB's binary **columnar** wire protocol over
  WebSocket. Higher throughput than ILP for wide rows, exposes the full
  QuestDB type system (int8/int16/int32, float32, char, date,
  timestamp-nanos, uuid, varchar, geohash, decimal128, long256, double
  arrays, long arrays, Gorilla DoD timestamp compression). QWP also
  ships an opt-in **store-and-forward (SF) mode** that mmap's outgoing
  batches to disk before the wire send, enabling crash-safe replay
  through transient server outages.

NuGet package id: `net-questdb-client`. Multi-targets `net6.0;net7.0;net8.0;net9.0;net10.0`. The
`ws::`/`wss::` (QWP) sender requires **net7.0+** because it depends on
`ClientWebSocket.HttpResponseMessage` for header-aware handshake; HTTP
and TCP senders work on every supported target.

## Commands

```bash
# Build the whole solution.
dotnet build net-questdb-client.sln -c Release

# Full unit-test pass (excludes [Explicit] integration tests that need
# a live QuestDB / Docker).
dotnet test src/net-questdb-client-tests/net-questdb-client-tests.csproj \
  --framework net10.0 -c Release

# Run a single test or namespace via NUnit's name filter.
dotnet test src/net-questdb-client-tests/net-questdb-client-tests.csproj \
  --framework net10.0 -c Release \
  --filter "FullyQualifiedName~QwpEncoder"

# Integration tests (`[Explicit]`) — boot a real QuestDB via Docker,
# require Docker daemon + the master image with /write/v4 enabled.
QUESTDB_IMAGE=questdb/questdb:master \
  dotnet test src/net-questdb-client-tests/net-questdb-client-tests.csproj \
  --framework net10.0 -c Release \
  --filter "FullyQualifiedName~QuestDbWebSocketIntegrationTests"

# Benchmarks. Names match the BenchmarkDotNet 0.13 filter syntax
# (`Param: value` colon-space, NOT the [Param=value] display format).
QDB_BENCH_ENDPOINT=127.0.0.1:9000 \
  dotnet run -c Release --project src/net-questdb-client-benchmarks --framework net10.0 -- \
  --filter '*BenchInsertsWs*' '*BenchSfThroughput*'

# Example apps under src/example-* are compilable demos referenced by
# examples.manifest.yaml (rendered on questdb.io). Keep paths and file
# names stable when editing.
dotnet run --project src/example-websocket --framework net10.0
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
  symbol dictionary, self-sufficient), type codes, ACK status codes,
  schema-mode bytes (`SchemaModeFull` / `SchemaModeReference`).
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
  touched column + drop columns added since `_committedColumnCount`).
  Mirrors the Java client's `cancelCurrentRow()` semantics — any error
  aborts the in-progress row so the caller sees consistent buffer state.
- `QwpEncoder.cs` — assembles the multi-table QWP frame from a set of
  table buffers in one flush; chooses `SchemaModeFull` vs
  `SchemaModeReference` per table based on the schema cache.
- `QwpSchemaCache.cs` / `QwpSymbolDictionary.cs` — schema id allocation
  and delta symbol dictionary. Caches advance after a successful enqueue
  (async mode) or a successful ACK (sync mode). Self-sufficient mode
  (used by SF) bypasses both caches and re-emits the full schema +
  full symbol dict on every frame.
- `QwpGorilla.cs` — delta-of-delta timestamp compression. The encoder
  emits a 1-byte encoding flag (`0x00` uncompressed, `0x01` Gorilla)
  only when `FLAG_GORILLA` is set on the message header. Falls back to
  uncompressed when the column has < 2 non-null values or any DoD
  exceeds int32. Always-on for the .NET client (Java client gates
  behind a flag; we ship it in v1).
- `QwpResponse.cs` — ACK / error frame parser. Strict UTF-8 (throws on
  invalid bytes) for error messages and per-table names; rejects empty
  table names, lying lengths, and trailing bytes after the last entry.
- `QwpInFlightWindow.cs` — bounded ACK-pending tracker for non-SF async
  mode. `AwaitEmpty(timeout)` is the producer-side drain.
- `QwpWebSocketTransport.cs` — thin wrapper over
  `System.Net.WebSockets.ClientWebSocket`. Performs the `/write/v4`
  upgrade with QWP version-negotiation headers (`X-QWP-Max-Version`,
  `X-QWP-Client-Id`). Supports an optional dump stream that records
  binary frames in both directions; dump writes are serialised under
  `_dumpLock` because send/receive run concurrently.
- `Senders/QwpWebSocketSender.cs` — owns the lifecycle. Two execution
  modes (sync / `in_flight_window=1` is rejected at construction; the
  double-buffered encoder pipeline assumes window ≥ 2 — for one-batch-at-a-time
  ILP semantics use the `http::` scheme instead):
  - **Async pipelined** (default `in_flight_window=128`): bounded
    `Channel<AsyncBatch>` between producer and `SendLoop`; double-buffered
    encoders so batch N+1 encodes while batch N is in flight. Caches
    advance on enqueue — safety comes from the sender being terminal
    on I/O error (`_terminalError` poisons every subsequent call).
    `_slot` semaphore reserves channel capacity before encoding to
    prevent producer from racing the I/O thread.
  - **SF** (`sf_dir=...` set): wires through `_sfEngine`
    (`Qwp/Sf/QwpCursorSendEngine`) instead of the in-memory channel.
    Frames are appended to mmap'd segment files first; the engine's
    pumps replay them across reconnects.

### Store-and-forward (SF, opt-in)

Lives entirely under `Qwp/Sf/` and only activates when the connect
string carries `sf_dir=...`. Designed to mirror Java PR #17 (`QWiP
store-and-forward client buffer`).

- `QwpFiles.cs` — exclusive-locking file ops. `OpenExclusive` /
  `TryOpenExclusive` use `FileShare.None` as a portable advisory lock
  (held for the lifetime of the returned `FileStream`). SF is documented
  as **local filesystem only** — `FileShare.None` is unreliable on NFS
  / SMB; matches Java's `flock` limitation.
- `QwpSlotLock.cs` — per-sender lock file. `Acquire` uses
  `TryOpenExclusive` so non-collision IO errors propagate; only a real
  file-share-violation maps to "already locked".
- `QwpMmapSegment.cs` — single mmap'd segment file with envelope frames
  `[u32 crc32c][u32 frame_len][frame bytes]`. Replays on open via
  `ScanForLastGoodEnvelope` to find the last good write position;
  truncates torn tails. `TryAppend` rejects frames larger than
  `_maxFrameLength` (16 MB default) to prevent the next reopen from
  silently treating them as torn.
- `QwpSegmentRing.cs` — ring of active + sealed segments + hot-spare
  slot. Hot path is lock-free (`Volatile`/`Interlocked`); the manager
  thread provisions spares ahead of time so the producer never blocks
  on segment allocation. Recovery treats the tail as sealed if it
  carries the sealed flag (handles crashes between `Seal()` and the
  next active alloc).
- `QwpSegmentManager.cs` — manager thread: heartbeat-driven plus
  callback-driven (producer's `NeedsHotSpare` / spare-adoption-failed).
  Provisions hot spares, trims acked segments, enforces
  `sf_max_total_bytes` cap.
- `QwpCrc32C.cs` — software slice-by-8 CRC32C, reflected polynomial
  `0x82F63B78`. Deliberately avoids `System.IO.Hashing.Crc32C` and
  hardware intrinsics so behaviour is bit-identical across runtime
  versions and CPU architectures (matches Java's choice). Output is
  byte-for-byte compatible with Java client's `Crc32c.update`.
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
- `token_x` / `token_y`: silently accepted for cross-client
  interop with Java/Go config strings; ignored at runtime.
- `in_flight_window`, `close_timeout`, `max_schemas_per_connection`,
  `gorilla`, `request_durable_ack`, `sf_*`, `reconnect_*`,
  `initial_connect_retry`, `close_flush_timeout_millis`, `drain_orphans`,
  `max_background_drainers`, `sender_id`: WS-only. Rejected on
  non-WS schemes via `ValidateWebSocketKeys` (string-ctor path) or
  `ValidateWebSocketKeysAgainstDefaults` (programmatic-init path,
  default-comparison heuristic).
- `auto_flush=off` zeros `auto_flush_rows` / `auto_flush_bytes` /
  `auto_flush_interval` to `-1`. The WS auto-flush defaulting
  (`auto_flush_rows=1000`, `auto_flush_interval=100ms`) only applies
  when `auto_flush != off` — `auto_flush=off` is honoured even for ws.
- `tls_verify=unsafe_off` accepts any server cert (dev / self-signed
  only — never ship to prod).
- `tls_roots`, `tls_roots_password`: PFX path + optional password for
  pinning a custom CA bundle.
- Multiple `addr=` entries are HTTP-only (multi-endpoint failover via
  `AddressProvider`). WS rejects multi-addr.
- `gzip=on` rejected for ws/wss (binary protocol; the WS-only key
  check is value-based so it works for both string-ctor and
  programmatic init paths).
- `ToString()` skips WS-only keys when the protocol is non-WS so the
  output round-trips through `new SenderOptions(s.ToString())`.

### Connection pooling

HTTP is thread-safe at the underlying `HttpClient` level; the Sender
itself is **not** thread-safe — one Sender per producer thread, or wrap
your own pool. There is no in-tree `LineSenderPool` (Go has one for
HTTP); the .NET HTTP transport already shares `HttpClient`s under the
hood via `IHttpClientFactory` semantics in `HttpSender`. WS / SF
manage their own concurrency model (in-flight window, slot lock) and
explicitly reject pooling.

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
    `Qwp/QwpResponseTests.cs`, `Qwp/QwpSchemaCacheTests.cs`,
    `Qwp/QwpSymbolDictionaryTests.cs`, `Qwp/QwpInFlightWindowTests.cs`,
    `Qwp/QwpGorillaTests.cs`, `Qwp/QwpWebSocketTransportTests.cs`,
    `Qwp/QwpWebSocketSenderTests.cs` — QWP-side unit + component tests
    using `DummyQwpServer` (`src/dummy-http-server/DummyQwpServer.cs`)
    as a Kestrel-backed in-process WebSocket server.
  - `Qwp/Sf/Qwp*Tests.cs` — store-and-forward subsystem tests
    (file ops, segment ring/manager, drainers, cursor engine, reconnect
    policy, slot lock, orphan scanner).
- Integration tests (`[Explicit]`):
  - `QuestDbIntegrationTests.cs` — HTTP/TCP integration via
    `QuestDbManager` (Docker container provisioning).
  - `QuestDbWebSocketIntegrationTests.cs` — WS/QWP integration. Requires
    `questdb/questdb:master` image because `/write/v4` is not yet in
    a stable release; gated by `QUESTDB_IMAGE` env var and a `[Explicit]`
    NUnit attribute so the regular test pass skips them.
- `JsonSpecTestRunner.cs` — cross-language ILP conformance vectors
  (`Json/specs/*.json`) shared with Java/Go clients via the `RunHttp`
  / `RunTcp` `[TestCaseSource]` parameterisation.
- Benchmarks in `src/net-questdb-client-benchmarks/` (BenchmarkDotNet):
  `BenchInsertsWs`, `BenchLatencyWs`, `BenchSfThroughput`,
  `BenchSfAppend`, plus the legacy ILP benches. The QWP suite uses
  `[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory,
  BenchmarkLogicalGroupRule.ByParams)]` with
  `[BenchmarkCategory("Narrow"|"Wide"|"MultiTable")]` so each row
  shape compares against its own HTTP baseline. Senders are created in
  `[GlobalSetup]` and re-used across iterations to avoid per-invocation
  slot/mmap/engine spin-up dominating measurements.

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
- QWP cache advancement differs by mode:
  - **Sync mode** advances `maxSentSchemaId` / `maxSentSymbolId` only
    after the server ACKs the batch. A failed flush leaves the caches
    untouched so retries re-send the full schema and symbol delta.
  - **Async mode** advances them immediately after a successful
    enqueue. Safety comes from the sender being terminal on I/O error
    (`_terminalError` poisons every subsequent call), so stale cache
    state can never reach the wire on a live connection.
  - **SF mode** uses self-sufficient frames — every frame carries the
    full schema and full symbol dictionary; no cache advancement, no
    reference mode. This makes each segment file independently
    replayable against fresh server state.
- The WS sender requires net7.0+. Gate any new WS-related code behind
  `#if NET7_0_OR_GREATER` if it touches `ClientWebSocket`-specific APIs;
  HTTP and TCP code must continue to compile on net6.0.
