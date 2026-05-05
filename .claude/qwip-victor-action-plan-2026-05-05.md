# qwip_victor — actionable plan

Compacted from `qwip-victor-review-2026-05-05.md` (37 review passes,
~140 findings). This is the prioritised work-list; the full review
file remains the rationale source.

**Out of scope for this PR** (tracked separately):
- Thread-safe session pattern → `qwip-victor-session-pattern-2026-05-05.md`
- Profile findings + benchmark setup → `qwip-victor-profile-2026-05-05.md`

---

## Must-fix before ship (10 HIGH)

Numbered roughly by ease-of-fix × severity. Each is a small, contained
change.

| # | File:line | Issue | Fix |
|---|---|---|---|
| 1 | `Senders/QwpWebSocketSender.cs:1338–1402` | **Secrets leak**: `ToString()` emits `password`/`token`/`tls_roots_password` despite docstring claiming "minus secrets" | Skip-list of credential property names; emit `***` redaction; add unit test asserting plaintext absent |
| 2 | `Senders/QwpWebSocketSender.cs:646–653` | **Half-row `Send()` silently drops data**: filter excludes `RowCount=0` even when `HasPendingRow=true`; touched flags not cleared → next row mixes orphaned data | Throw `InvalidApiCall("row in progress — call At*() to commit or CancelRow() to abandon")` if any table has `HasPendingRow=true` |
| 3 | `Senders/QwpWebSocketSender.cs:1428–1441` | **PEM file reloaded from disk per TLS validation call** + duplicate `CustomTrustStore.Add` per call → disk I/O × N per handshake, memory pressure within handshake | Hoist `X509Certificate2.CreateFromPemFile(...)` out of the closure (one-time at sender construction); guard `CustomTrustStore.Add` with `Count == 0` check |
| 4 | `Qwp/QwpColumn.cs:510` | `BigInteger.ToByteArray(unsigned, LE)` allocates fresh `byte[]` per Long256 row | Use `TryWriteBytes(Span<byte>, ...)` overload writing directly into `FixedData.AsSpan(FixedLen, 32)` + zero high bytes |
| 5 | `Senders/QwpWebSocketSender.cs:500–508` | `Column(name, Array)` multi-dim path allocates `new double[]`/`new long[]` + `Buffer.BlockCopy` per row | Use `MemoryMarshal.CreateSpan` over the pinned source array; the typed `AppendArrayDispatch<T>` path (line 519) already does this correctly — match its pattern |
| 6 | `Qwp/Sf/QwpMmapSegment.cs:259` | `WritePosition` plain auto-property; reader (cross-thread send pump) can observe new value before envelope bytes are visible on ARM → CRC catches but throws spurious `InvalidDataException` | Convert to backing field with `Volatile.Read`/`Volatile.Write`; also fix paired finding at `:278–279` (`_offsetTable[count]` plain write before `_offsetTableCount` volatile fence) |
| 7 | `Senders/QwpWebSocketSender.cs:785` | `CancellationTokenSource.CreateLinkedTokenSource(_ioCts.Token, ct)` per `EnqueueAsyncCore` call → ~150 B/flush | Skip the link when `ct == default`; pass `_ioCts.Token` directly. Auto-flush path always passes `default` |
| 8 | `Qwp/QwpInFlightWindow.cs:144,179,196,324–329` | Fresh `TaskCompletionSource<bool>` allocated per `Add`/`Acknowledge`/`FailAll` even with no awaiter → ~160 B/flush of pure waste | Lazy-allocate (only when `AwaitEmptyAsync` is actually awaited), OR migrate to `IValueTaskSource<bool>` for zero-alloc reset |
| 9 | `src/net-questdb-client-benchmarks/BenchLatencyWs.cs:94`, `BenchInsertsWs.cs:63`, `BenchSfThroughput.cs:49` | **Benchmarks ship broken cells**: `in_flight_window=1` rejected by `QwpWebSocketSender.cs:105`, but bench params include 1 → `BenchLatencyWs` non-functional, others have failing sweep cells; documented numbers in `docs/qwp-benchmarks.md` cannot have come from these benches as shipped | `BenchLatencyWs.cs:94` → `in_flight_window=2`; drop `1` from `[Params]` arrays in the other two; re-run against real server; refresh doc numbers |
| 10 | `net-questdb-client.csproj:21` | **`PackageVersion=3.2.0` not bumped** for a release shipping QWP + proposed binary-breaking changes (Task→ValueTask) | Bump to 4.0.0 if breaking changes land in the same release; 3.3.0 for additive QWP only |

### Estimated effort for HIGH list

Each item is < 30 LOC. Total: ~1-2 days of focused coding + tests + benchmark re-runs. The bench re-runs are the longest tail because they need a real QuestDB instance.

---

## Should-fix (MED) — grouped by theme

### Performance / allocations (10)

- [ ] `Senders/QwpWebSocketSender.cs:878` — `.AsTask()` per sync flush; use `vt.GetAwaiter().GetResult()` directly. Drops ~96 B/flush.
- [ ] `Senders/QwpWebSocketSender.cs:783` — `async ValueTask EnqueueAsyncCore` boxes state machine when awaits go async. Add `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]` (one-line, .NET 6+).
- [ ] `Qwp/QwpEncoder.cs:380–391` — `WriteString` double-passes UTF-8 (`GetByteCount` then `GetBytes`). Cold for WS, **hot for SF** (every self-sufficient frame). Single-pass with back-patched varint.
- [ ] `Qwp/QwpEncoder.cs:302–310` — `WriteColumnData` Varchar offsets one-at-a-time; replace with `MemoryMarshal.Cast<uint, byte>` bulk copy on LE hosts (~5–10× faster for varchar columns). 1001 writer calls → 1 memcpy.
- [ ] `Qwp/Sf/QwpCursorSendEngine.cs:802–814` — Signal-fire allocates 3× per signal (TCS + Task.Run Task + closure). Use `Task.Factory.StartNew(state => ((TCS)state).TrySetResult(true), prev)` to avoid the closure capture.
- [ ] `Qwp/QwpInFlightWindow.cs:63–120` — Property getters take full `_lock` for single-field reads; replace with `Volatile.Read` for `AckedSequence`/`HighestSentSequence`/`HasFailure` (writer's lock release provides fence). Keep lock on composite getters (`IsEmpty`, `InFlightCount`).
- [ ] `Qwp/QwpInFlightWindow.cs:260–322` — `AwaitEmptyAsync` uses `DateTime.UtcNow` for deadlines (not monotonic). Sync `AwaitEmpty` correctly uses `Stopwatch`. Switch async to `Stopwatch` too.
- [ ] `Qwp/Sf/QwpMmapSegment.cs:577` — `ViewToSpan` per-call `AcquirePointer`/`ReleasePointer`; `ScanForLastGoodEnvelope` calls in a loop. Acquire once outside loop. Cold path, microsecond win.
- [ ] `Qwp/QwpColumn.cs:399` — `Clear()` discards `NullBitmap`; nullable workloads re-allocate per flush. `Array.Clear(NullBitmap, 0, len)` instead.
- [ ] `Qwp/QwpWebSocketTransport.cs:392–397` — Reflection in `BuildDefaultClientId` per transport ctor; cache in `static readonly string`.

### Concurrency / correctness (6)

- [ ] `Senders/QwpWebSocketSender.cs:1285` — `_terminalError` plain reference read; pair the `Interlocked.CompareExchange` writer with `Volatile.Read`.
- [ ] `Qwp/QwpInFlightWindow.cs:251` — `Monitor.Wait` 100 ms poll quantum bounds cancellation latency. Either drop to ~10–20 ms, or `CT.UnsafeRegister(() => Monitor.PulseAll(_lock))`.
- [ ] `Senders/QwpWebSocketSender.cs:1872` — `using var linked = CTS.CreateLinkedTokenSource(...)` may dispose after `_ioCts.Dispose()` in race-on-Dispose; throws ObjectDisposedException. Sequence: cancel → join → dispose, never cancel → dispose → join.
- [ ] `Qwp/Sf/QwpCursorSendEngine.cs:560` — Reconnect cursor not bounds-checked against ring; clamp `_cursorFsn = max(_ackedFsn, ring.OldestFsn)`.
- [ ] `Qwp/Sf/QwpFiles.cs:78–94` — `IsSharingViolation` over-broad on POSIX; catches disk-full/permission/etc. as "lock contended". Distinguish `EAGAIN`/`EACCES` via `errno` inspection or fallback log.
- [ ] `Qwp/QwpWebSocketTransport.cs:252–256` — Server-initiated close maps to `ErrorCode.SocketError`; conflates "server told us to disconnect" with TLS/DNS failures. Add `ErrorCode.RemoteClose`.

### API consistency (4 actions)

- [ ] **Action 1** — Re-document `ISender.Length` semantics for QWP (approximate footprint, not wire size). Validate `auto_flush_bytes ≤ MaxBatchBytes / 2` for ws/wss in `SenderOptions.EnsureValid`.
- [ ] **Action 2** — Implement `Truncate()` properly on QWP. New `QwpColumn.TrimToCurrent()` shrinks per-column buffers via `Array.Resize` to current `FixedLen`/`StrLen` boundaries. ~40 LOC.
- [ ] **Action 3** — Migrate `Task` → `ValueTask` on `ISender.SendAsync`/`CommitAsync` and `IQwpWebSocketSender.PingAsync`. **Binary-breaking** — bundle with version bump (item HIGH-10). Internal impls already use ValueTask; just stop materialising via `AsTask()`.
- [ ] **Action 4** — Strip stale `ISender.SendAsync` doc references to nonexistent HTTP/TCP return values.

### SenderOptions validation (5)

- [ ] `Utils/SenderOptions.cs:187,188,189,190,199,222,224,225,227,230` — Reject negative/zero on timeout options. `auth_timeout=0` → every connect throws immediately.
- [ ] `Utils/SenderOptions.cs:224–225` — Validate `reconnect_initial_backoff ≤ reconnect_max_backoff`.
- [ ] `Utils/SenderOptions.cs:894` — Add `tls_ca` to `keySet` (silent-accept like `token_x`/`token_y`) to match README docs which list it as a parameter.
- [ ] `Utils/SenderOptions.cs:1108,1134` — Reject `port=0` for client config (`port <= 0`).
- [ ] `Senders/QwpWebSocketSender.cs:140–145` — Plumb proxy override (currently `ws.Proxy = null` hardcoded but README documents an `IWebProxy` override that doesn't exist). Add `proxy` field on `SenderOptions` + wire through `QwpWebSocketTransportOptions`.

### Behavioural & cross-transport consistency (3)

- [ ] `Qwp/QwpTableBuffer.cs:351` — `max_name_len` half-broken: column-name validation uses hardcoded `QwpConstants.MaxNameLengthBytes` instead of the constructor's `maxNameLengthBytes` parameter. Store as field, use in both validation sites. ~3 LOC.
- [ ] `Senders/QwpWebSocketSender.cs:1380–1389` — DateTime.Kind correctly handled on QWP, but **ILP silently treats `DateTime.Now` (Local) and `new DateTime(...)` (Unspecified) as UTC** at `Buffers/BufferV1.cs:114–118`. Fix ILP to switch on `Kind` like QWP. Latent timezone bug. *(ILP-side fix; coordinate with HTTP/TCP work)*
- [ ] Add `DateTime.SpecifyKind(value, DateTimeKind.Utc)` recommendation to QWP docs for users hitting the Unspecified rejection.

### CI / testing / packaging (3)

- [ ] `ci/azure-pipelines.yml:88,97` — CI tests on `net9.0` only; add at minimum `net8.0` (last LTS) + `net10.0` (latest); ideally all five TFMs. The pre-NET9 `SpanKeyedDict` workaround path is currently never exercised in CI.
- [ ] `net-questdb-client.csproj:14,17,19` — Stale package metadata: Description still says "ILP only", Tags missing `QWP`/`WebSocket`, `PackageLicenseUrl` deprecated (use `PackageLicenseExpression="Apache-2.0"`).
- [ ] Add the `BenchAllocationsWs` smoke run to CI as an allocation-regression gate (already created in `src/net-questdb-client-benchmarks/`; see `qwip-victor-profile-2026-05-05.md`).

### README accuracy (5)

- [ ] `README.md:113,119` — HTTPS examples use port `9009` (TCP ILP); fix to `9000`. Copy-paste-and-fail bug.
- [ ] `README.md:289` — `request_timeout` default cell shows `10000`; code default is `30000`.
- [ ] `README.md:311` — `reconnect_max_backoff_millis` cell shows `30000`; code default is `5000`.
- [ ] `README.md:390` — Contribute link points to `c-questdb-client`; should be `net-questdb-client`.
- [ ] `README.md:244` — Mention of `IWebProxy` override which doesn't exist; fix when item Action 5 above lands, or strip the line until it does.

---

## Nice-to-have (LOW) — by theme

### Allocation & alloc patterns

- [ ] `Qwp/QwpColumn.cs:331` — Varchar `GetMaxByteCount` over-reserves 3× for ASCII; benchmark before/after switching to `GetByteCount` upfront for typical workloads.
- [ ] `Qwp/QwpTableBuffer.cs:58,62` — `_touchedInCurrentRow` and `_rowSavepoints` start `Array.Empty<>`; first rows thrash through 1→2→4→8 resize. Initialise to size 8.
- [ ] `Qwp/QwpColumn.cs:326` — `StrOffsets = new uint[InitialSymbolCapacity]` reuses symbol-capacity constant for varchar offsets; rename or alias.
- [ ] `Qwp/QwpInFlightWindow.cs:251` — `100 ms` poll quantum, see MED list above.

### Concurrency / safety hardening

- [ ] `Qwp/Sf/QwpMmapSegment.cs:87,214,294,370,386` — `_disposed` plain `bool`; switch to `Volatile.Read`/`Volatile.Write` (matches `QwpCursorSendEngine`).
- [ ] `Qwp/QwpSchemaCache.cs` — Add class-level XML doc: "Not thread-safe; caller must serialise (enforced by encoder ping-pong semaphore in `QwpWebSocketSender`)."
- [ ] `Senders/ISender.cs:30` — Add `<remarks>` documenting "Not thread-safe; one Sender per producer thread." Optionally add DEBUG-only thread-affinity guard via `Environment.CurrentManagedThreadId` capture.

### API surface cleanup

- [ ] `Enums/QwpTypeCode.cs` — Currently `public`; should be `internal` (wire-format detail, no consumer use case).
- [ ] `Senders/IQwpWebSocketSender.cs` — `GetHighestAckedSeqTxn(string)` / `GetHighestDurableSeqTxn(string)` → `ReadOnlySpan<char>` for span-everywhere consistency. Source-compatible, binary-breaking — bundle with item Action 3.
- [ ] `Sender.cs:67` — `Sender.New(SenderOptions? options = null)` silently builds default HTTP sender on null + bypasses `EnsureValid`. Remove null default OR route through validated dispatch.
- [ ] `Utils/SenderOptions.cs:558–563` — `bind_interface` is a public throw-on-access stub marked `[Obsolete]`. Either delete (next major bump after deprecation period) or document deprecation timeline.
- [ ] `Senders/TcpSender.cs:41` — `internal class`; should be `internal sealed` for consistency with `QwpWebSocketSender` and `HttpSender`.
- [ ] `Qwp/QwpSymbolDictionary.cs:85` — `Add(empty span)` accepts empty symbol value; throw `InvalidApiCall("symbol value must not be empty")` defensively.
- [ ] `Qwp/QwpSymbolDictionary.cs:115` — `GetSymbol(id)` throws `IndexOutOfRangeException` on bad id; wrap as `IngressError(InternalError, ...)` per project convention.

### Documentation completeness

- [ ] `README.md:295–316` — WS-only parameters table missing `max_symbols_per_connection` and `ping_timeout`.
- [ ] `README.md:353–354` — `AtNow`/`AtNowAsync` listed without `[Obsolete]` marker.
- [ ] `README.md:360–363` — Examples section missing `example-websocket` and `example-websocket-auth-tls`.
- [ ] `README.md:90` — Heading "Flush every 5000 rows" with example showing `auto_flush_rows=1000`.
- [ ] `Senders/QwpWebSocketSender.cs:1003` — `Truncate()` empty-body comment misleading (claims "no buffer-tail to trim"); remove when Action 2 implements properly.
- [ ] `Qwp/QwpSymbolDictionary.cs:155` — `Reset` doc says "called on connection reset"; SF mode actually calls per-flush. Update docstring.

### Repo housekeeping

- [ ] `ci/azurre-binaries-pipeline.yml` — Filename typo (double r); rename to `azure-binaries-pipeline.yml`.
- [ ] No `CHANGELOG.md` for a release adding QWP. Add with 4.0.0 (or 3.3.0) section.
- [ ] No `CONTRIBUTING.md` referenced by README's Contribute section.

### Config string parsing

- [ ] `Utils/SenderOptions.cs:1108,1134` — Empty hostname accepted (`addr=":9000"`); add `IsNullOrWhiteSpace(host)` check.
- [ ] `Utils/SenderOptions.cs:1255` — `Split("::")` doesn't validate exactly-one separator; throw if `splits.Length != 2`.
- [ ] `Utils/SenderOptions.cs:1286–1293` — Body `protocol=...` silently overridden by `::`-prefix protocol; reject body `protocol` key.
- [ ] `Utils/SenderOptions.cs:1411` — Unknown-key error uses lowercased form (DbConnectionStringBuilder normalisation); capture original case from raw split.

### SF subsystem polish

- [ ] `Qwp/Sf/QwpFiles.cs:209` — `LooksLikeNetworkPath` is dead code. Wire it into `QwpSlotLock.Acquire` to warn on NFS, or delete.
- [ ] `Qwp/Sf/QwpFiles.cs:195`, `Qwp/Sf/SfCleanup.cs:53`, `Qwp/Sf/QwpSlotLock.cs:150` — `File.Exists + File.Delete` redundant pattern (TOCTOU). Collapse to direct `File.Delete`.
- [ ] `Qwp/Sf/QwpSlotLock.cs:25` — Unused `using System.Diagnostics`.
- [ ] `Qwp/Sf/QwpSlotLock.cs:117–129` — Validate PID liveness in `ReadHolderHint` via `Process.GetProcessById`; append "(stale)" if dead. Diagnostic improvement.
- [ ] `Qwp/Sf/QwpSlotLock.cs:142–154` — Dispose-then-Delete races; delete sidecar BEFORE the lock-file dispose.
- [ ] `Qwp/Sf/QwpBackgroundDrainerPool.cs:257` — `TryDropFailedSentinel` writes full `ex.ToString()` (KB-sized stack trace per failure); cap at 4 KB or write `Type: Message`.
- [ ] `Qwp/Sf/QwpBackgroundDrainer.cs:92` — Hardcoded `appendDeadline=30s` (drainer doesn't actually use it; cosmetic).

### Other

- [ ] `Senders/QwpWebSocketSender.cs:1357` — Auto-flush interval check uses `DateTime.UtcNow` (non-monotonic); same shape as QWP `AwaitEmptyAsync` MED finding. Affects ILP too. `Environment.TickCount64` for elapsed.
- [ ] Error code `ProtocolVersionError` overloaded across 20 sites for parse errors; consider adding `ErrorCode.ProtocolError` and migrating parse-error sites.
- [ ] `Qwp/QwpVarint.cs`, `QwpBitWriter.cs`, etc. — Internal helpers throw raw `ArgumentException`/`InvalidOperationException` instead of `IngressError(InternalError, ...)`. Library-bug paths surface unwrapped past consumer `catch (IngressError)` blocks.
- [ ] `Qwp/QwpTableBuffer.cs:109` — `Columns` returns `IReadOnlyList<T>` over backing `List<T>`; cast-to-mutable bypasses contract. Currently `internal`, no real exposure. Defensive: return `_columns.AsReadOnly()`.
- [ ] `Senders/QwpWebSocketSender.cs:158` — Sync-over-async `ConnectAsync().GetAwaiter().GetResult()` in ctor. Safe today; commit to `ConfigureAwait(false)` discipline via `CA2007` analyzer or expose `Sender.NewAsync`.
- [ ] `Utils/SenderOptions.cs:45` — `record SenderOptions` auto-generates `Equals`/`GetHashCode`/`PrintMembers` over all properties including credentials. Pair with HIGH-1 fix: also override these to exclude secret fields.
- [ ] `Senders/QwpWebSocketSender.cs:1402–1405` — Basic auth materialises `username:password` plaintext on the GC heap. Defence-in-depth: build via `Span<byte>` + `Convert.ToBase64String(span, span)`, never materialise plaintext managed string.
- [ ] `Senders/QwpWebSocketSender.cs:1408–1410` — Header-injection if `options.token` contains `\r\n`; reject control chars in auth fields in `EnsureValid`.

---

## Decisions made (no action required, recorded)

These were investigated and consciously kept-as-is. Documented to
prevent re-litigation:

| Decision | Where | Rationale |
|---|---|---|
| Senders stay structurally separate (no shared `AbstractSender` base for QWP) | `qwip-victor-review-2026-05-05.md` § Architectural drift | Buffer model differs (text vs columnar); shared validation helpers (Tier 1) cover ~90% of drift risk |
| `long.MinValue` accepted on QWP `AppendLong` | Behavioural inconsistencies | Forward-compatible with QuestDB NOT NULL feature; ILP rejection is the bug to fix later |
| Symbol-after-Column ordering not enforced on QWP | Behavioural inconsistencies | Columnar format doesn't care; document portable code should still emit symbols first |
| Error code divergence (`InvalidName` on QWP, `InvalidApiCall` on ILP) | Behavioural inconsistencies | ILP keeps existing for backward-compat; QWP uses more specific code going forward |
| `BenchInsertsWs` etc. WS-only knobs default differently from HTTP/TCP | SenderOptions normalisation | WS pipelining means different sweet spots; documented per-transport in WS-defaults table |
| `IsReplayImpossible` (drainer pool) narrower than `IsTerminalServerError` (engine) | SF drainer pool review | Intentional: orphan drainer may succeed against recovered server where live engine couldn't |

---

## Out of scope (follow-ups, separately tracked)

- **Thread-safe session pattern** for multi-producer workloads.
  Full design in `qwip-victor-session-pattern-2026-05-05.md`.
  Decision criteria spelled out — kick off when customer demand or
  contention measurement justifies.
- **Observability hooks** — `ActivitySource` + `EventSource` + optional
  `ILogger` injection. Cross-transport feature, not QWP-specific.
  Track separately when production deployments need it.
- **`SpanKeyedDict<T>` for pre-.NET 9 fallback** — eliminates the
  ~5× allocation overhead on net6/7/8 documented in
  `qwip-victor-profile-2026-05-05.md`. Decision pending: drop EOL
  TFMs, or keep + add the workaround. Either way separate from this
  PR.
- **ILP DateTime.Kind fix** in `BufferV1.cs:114–118` (latent timezone
  bug; ILP silently treats Local/Unspecified as UTC). HTTP/TCP-side
  work; bundle with the next ILP correctness PR.
- **JsonSpecTestRunner extension to QWP** — extend the existing
  cross-language ILP conformance vectors to also dispatch over
  ws/wss. Catches behavioural drift between transports as test
  failures.

---

## Suggested PR sequencing

Three-PR sequence keeps each chunk reviewable:

### PR 1 — Correctness fixes (HIGH list, no API changes)

Items 1, 2, 3, 4, 5, 6, 8 from the HIGH list, plus the four MED-list
concurrency fixes. No public API changes; no version bump beyond
patch (3.2.1).

**Effort**: ~2-3 days. Reviewable as a single coherent "ship-blocker
fixes" PR.

### PR 2 — API consistency + version bump

Items 7, 9 (CTS optimisation, benchmarks fix) + the four "API consistency
followups" actions (Length doc, Truncate impl, Task→ValueTask, doc
strip) + version bump (item 10) + record-equality + ToString secrets
override (HIGH-1's complementary surface). Public API + binary
breaking.

**Effort**: ~3-4 days. Bundled with the version bump → 4.0.0 or
3.3.0 release. Changelog entry required.

### PR 3 — Polish (MED + LOW lists)

Everything else: README accuracy, config validation, docs completeness,
SF cleanup polish, error code naming, etc. Mostly small independent
items; can be split into smaller PRs by theme if reviewer prefers.

**Effort**: ~1 week wall-clock; 30+ small commits.

### Out-of-PR work

- **Run the BenchAllocationsWs allocation gate** before each PR and
  attach numbers in the PR description.
- **Refresh `docs/qwp-benchmarks.md`** numbers after PR 1 lands
  (since the benches are now functional).
- **CI matrix expansion** lands in PR 1 or PR 3 — matrix net8/9/10
  testing is needed before any pre-.NET 9 work proceeds.

---

## Tracking format suggestion

For a tracker (Linear, Jira, GitHub Issues), each MED/LOW item maps
to one ticket. Pre-defined labels:

- `area/qwp` — QWP-specific
- `area/ilp` — HTTP or TCP transport
- `area/sf` — Store-and-forward
- `area/perf` — Allocation or CPU
- `area/concurrency` — Threading or memory ordering
- `area/api` — Public surface
- `area/docs` — README, XML doc, comment
- `area/build` — csproj, CI, packaging

Severity from the lists above maps to priority. Each item's
file:line reference + one-sentence fix is enough for an engineer
to pick it up cold.
