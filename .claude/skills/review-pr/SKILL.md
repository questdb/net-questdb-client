---
name: review-pr
description: Review a GitHub pull request against net-questdb-client (.NET) coding standards. Performs an adversarial, blocking, mission-critical code review covering correctness, concurrency/async, performance and allocations, resource management (IDisposable + native memory), test coverage, test efficacy, test-code quality, and the QWP store-and-forward / pool invariants, then verifies every finding against source before reporting.
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior engineer performing a blocking code review of **net-questdb-client**, the .NET client library for QuestDB. It is mission-critical software: bugs can cause **silent data loss**, data corruption, or crashes in customer ingestion pipelines that the customer cannot patch quickly. The store-and-forward sender exists precisely so a producer never loses data across an outage — a regression there is the worst class of bug in this repo. There is zero tolerance for correctness issues, resource/native-memory leaks, or data-loss paths. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access or interleaved calls from the producer thread and the drainer/receive pump? What if the wire drops mid-flush, the server sends a partial frame, the TLS handshake fails, or auth is rejected? What if the buffer is empty, the column is null, the symbol dict is at capacity, or a value is `Decimal.MaxValue`?
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing `#if NET7_0_OR_GREATER` gating, missing disposal, missing documentation for non-obvious behavior.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks (`src/net-questdb-client-benchmarks`) or reason about the change — does it actually improve things, or regress another case? If it says "simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use Read/Grep/Glob to inspect the surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem requires physically impossible conditions (a buffer larger than `int.MaxValue`, a value no public API can produce, a race the single-producer-thread contract forbids), it is not a real finding — drop it. Focus on bugs real workloads can trigger, not theoretical edge cases that exist only in the type system.
- **`Debug.Assert` is compiled out in Release.** It is a valid *development* guard for internal invariants, but it provides **no runtime protection in shipped builds** (Release is what customers run). Do NOT accept a `Debug.Assert` as sufficient validation for a condition that customer input or a hostile/buggy server can trigger — that path needs a real check + thrown `IngressError`/latched buffer error. Conversely, do not flag a `Debug.Assert` guarding a genuine library-internal invariant as "insufficient"; that is its correct use.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (QWP wire format / codecs, the cursor send engine or SF segment/ring/drainer, primary reconnect/failover, `SenderPool` / `QueryClientPool` / pool startup, TLS/auth, TCP ECDSA auth, or the public `ISender` / `IQwpWebSocketSender` / `IQwpQueryClient` / `IQuestDBClient` surface).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 4. Skip Step 2.5. Skip Step 3 — no agent spawn; review the diff inline in the main loop, using Read/Grep on demand to resolve ambiguities. Skip Step 3b — verify each finding inline as you write it. Single-pass review covering correctness, null handling, disposal, test coverage, and .NET/QWP standards on the diff itself. When the diff touches test code, also apply the test-efficacy and test-code-quality anti-pattern checks inline (vacuous assertions, timing-count assertions, reflection overuse, reinvented helpers, XML-doc bloat). |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d) plus Step 2.5e when test code is present. In Step 3, launch Agent 1 (correctness), Agent 5 (test coverage), Agent 6 (code quality), and — when the diff touches test code — Agent 11 (test efficacy) and Agent 12 (test-code quality) in parallel. Skip all other agents. Skip Step 3b — verify findings inline as you draft the report. |
| **2** | Full Step 2.5 (including 2.5e when test code is present), but in 2.5b restrict the callsite inventory to `public` / `protected` / `internal`-visible-to-tests symbols (skip `private`). In Step 3, launch Agents 1-7, plus Agent 8 if the diff touches async, native memory, `unsafe`, or `#if`-gated code, plus Agents 11 and 12 when the diff touches test code. Skip Agent 9 (cross-context), Agent 10 (adversarial fresh-context), and Agent 13 (regression-test efficacy verification). Step 3b uses a single batched verification agent for all findings instead of one per finding. |
| **3** | Every step below as written, all 13 agents, per-finding verification. The full mission-critical pass. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #77 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Step 1: Gather PR context

Capture the PR identifier in `$PR` (the part of `$ARGUMENTS` left after stripping the level token), then fetch metadata, diff, and review comments in a single bash call so `$PR` is in scope for all invocations:

```bash
PR='<PR number or URL from $ARGUMENTS, with any --level=N / -lN / bare-digit level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr diff "$PR" --numstat   # binary files show as `-<TAB>-<TAB><path>`
gh pr view "$PR" --comments
```

**Committed-binary gate (runs at every level).** Scan the `--numstat` output for any added/modified file git reports as binary (`-`/`-` in the added/deleted columns). This repo ships as a managed NuGet package built and published by CI; build outputs (`bin/`, `obj/`, `*.dll`, `*.nupkg`, `*.pdb`) are not committed. Any such file is a **Critical** finding regardless of review level — report it even at level 0. See the "Committed build artifacts" checklist for the rationale and the acceptable exception (genuine test-input fixtures only, e.g. a TLS `.pfx` a test reads).

## Step 2: PR title and description

Check against the repo's conventions:
- Title follows Conventional Commits: `type(scope): description` (e.g. `feat(qwp): ...`, `fix(http): ...`, `test(qwp): ...`). Common scopes: `qwp`, `http`, `tcp`, `pool`, `ws`, `sf`.
- Description speaks to end-user / API impact, not just implementation internals
- If fixing an issue, `Fixes #NNN` (or a link) is present
- Tone is level-headed and analytical, no superlatives or bold emphasis on numbers
- For public-API changes (`ISender` / `IQwpWebSocketSender` / `IQwpQueryReader` / `IQuestDBClient`, `SenderOptions` / `QueryOptions` config keys, `Sender.New` / `QueryClient.New` / `QuestDBClient` factories), the description calls out the API/behavior change explicitly
- Multi-target impact: if a change is `#if NET7_0_OR_GREATER`-gated (WS/QWP) vs applies to all TFMs (HTTP/TCP), that should be clear

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use Grep/Glob — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added method, property, constructor, interface member, struct field, enum value, or public/internal constant, write:

- **Symbol:** fully-qualified name (e.g. `QuestDB.Senders.QwpWebSocketSender.Flush`, `QuestDB.Qwp.Sf.QwpCursorSendEngine.IsFullyDrained`)
- **Before:** signature, return type, exception behavior (which `IngressError`/`ErrorCode` it throws or latches), nullability (`?` annotations), mutation (`readonly` vs mutable, `struct` copy semantics), ordering/idempotency guarantees, allocation behavior, thread-safety, async/`ConfigureAwait` behavior, disposal ownership (does it own or transfer an `IDisposable`/native buffer?)
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is `public`, `protected`, or `internal` (internal helpers are reachable from the test project via `[InternalsVisibleTo("net-questdb-client-tests")]`), run Grep across the entire repository to find every callsite, implementation, override, or reference outside the diff.

Produce a list grouped by file. Search at minimum for:
- direct callers (`grep -rn 'SymbolName' src/`)
- interface implementations and overrides — a change to `ISender` / `ISenderV2` / `IBuffer` / `IQwpSegment` / `IQwpCursorTransport` / `IPooled*` seams affects every implementor
- the pool layer: an `ISender`-contract change is visible through `BorrowedSender` / `PooledSender` / `SenderPool` even if those files aren't in the diff
- test callers in `src/net-questdb-client-tests/` (internal symbols are visible there)
- benchmark callers in `src/net-questdb-client-benchmarks/`
- example apps under `src/example-*`
- multi-target gating: is the symbol inside `#if NET7_0_OR_GREATER` (or another TFM guard)? A caller on a lower TFM that no longer compiles is a break.

A changed `public`/`protected`/`internal` symbol with zero recorded Grep calls in the trace is a skill violation. You are not allowed to assert "this is only used here" without showing the search.

### 2.5c Implicit contract list

For each changed symbol, walk this checklist and write one line per item, stating before vs after:

- Throws or latches which errors on which inputs (`IngressError` + `ErrorCode`, `LineSenderServerException`, `QwpQueryException`, `QwpRoleMismatchException`, `ObjectDisposedException`)
- Whether the error is **latched on the buffer** (ILP `IBuffer` / `Column*` model — surfaces on the next `At`/`AtNow`/`Flush`) vs **thrown immediately** vs **aborts the whole row** (`QwpTableBuffer.CancelCurrentRow`)
- Nullability: does a param/return newly accept or produce `null`? Are sentinel-null and `null` still distinguished?
- Idempotency and re-entrancy; second-`Dispose` tolerance; use-after-return safety for pooled handles
- Async: does it now block on async (`.Result`/`.Wait()`/`.GetAwaiter().GetResult()`), and is `ConfigureAwait(false)` preserved in library code?
- Lock acquisition: which lock is held on return; `_stateLock` / `_gate` ordering
- Allocation on hot (per-row / per-frame) vs setup/compile-time path
- Thread-affinity: a single `ISender` / `IQwpQueryClient` is **not** thread-safe (one per producer thread); does the change assume or violate that?
- `Span<T>`/`ReadOnlySpan<T>` lifetime — is a returned span valid only until the next read (egress `QwpColumnBatch`), and does the change leak it past that?
- Disposal / native-memory ownership: `NativeMemory.Alloc`/`Free`, mmap map/unmap, `IDisposable`/`IAsyncDisposable` — who frees, on which paths
- QWP self-sufficiency: does a frame still carry the full schema + full symbol-dict delta (required for replay after reconnect / SF replay)?

### 2.5d Cross-context exposure list

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting agents in Step 3.

Group the callsites from 2.5b by execution context: the hot ingest path (per-row `Column*`/`At`), frame encode/flush, the **cursor send engine** send/receive pumps, the **background drainer** / orphan scanner threads, primary reconnect/failover, the **pool** (borrow/return/reap/housekeeper), TLS/auth handshake, the egress pull cursor / decoder, SF segment ring / mmap / manager, and multi-TFM compilation. Every entry on this list must be reviewed in Step 3.

### 2.5e Test surface & helper inventory

Run this only when the PR adds or changes test code. It is the test-code counterpart to 2.5b and feeds Agents 11-13. Use real Grep/Glob searches — do not reason about helpers from memory.

- **Existing-infrastructure inventory:** search the changed test files' area for base fixtures, shared `[SetUp]`/`[TearDown]`, helper methods, and the in-process servers the new tests could reuse (Grep for `DummyQwpServer`, `DummyHttpServer`, `QuestDbManager`, `[TestFixture]` base classes, shared helpers, `MockHttp`, spec-vector runners like `JsonSpecTestRunner`). This list is the baseline Agent 12 uses to flag reinvented boilerplate — a "you stamped boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** if the PR changes a shared fixture/helper (e.g. `DummyQwpServer`), run the 2.5b callsite inventory for it too — a changed shared server can silently break every dependent test.
- **Exercised-symbol map:** for each new or changed test, list which production symbols from 2.5a it actually exercises, so Agents 11 and 13 can check efficacy and regression value.

## Step 3: Parallel review

Every agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)

### Anti-anchoring directive (applies to all agents)

- **Bugs at callsites outside the diff outrank bugs inside the diff.** A confirmed bug in a file the PR did not touch but that calls a changed symbol is a P0 finding.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- A single finding of the form "in `SenderPool.cs` the new behavior of `QwpWebSocketSender.IsFullyDrained` causes a reaped sender to drop un-acked frames" is worth more than five findings inside the diff.

### Agents

Launch the following agents in parallel.

**Agent 1 — Correctness & bugs:** null handling, edge cases, logic errors, off-by-one, operator precedence, wrong `ErrorCode`, error paths, latched-vs-thrown mismatches. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches the store-and-forward sender, the cursor send engine, the async drainer / receive pump, primary reconnect/failover, NACK handling, the poison detector, or pool startup (`lazy_connect` / `initial_connect_retry` / `SenderPool` / `QueryClientPool`), also verify the "Store-and-forward & pool startup invariants" checklist — a running drainer that propagates a transport error to the caller, imposes a reconnect time budget, drops a NACKed frame, or hard-fails on a transient outage is a **Critical** (data-loss) finding.

**Agent 2 — Concurrency & async:** race conditions, shared mutable state without a lock, missing `Volatile`/`Interlocked`, lock ordering / deadlock, thread-safety of data structures shared between the producer thread and the send/receive pumps or the housekeeper. Blocking on async (`.Result` / `.Wait()` / `.GetAwaiter().GetResult()`) that can deadlock or starve the thread pool; `async void`; unobserved `Task` exceptions; missing `ConfigureAwait(false)` in library code; `TaskCompletionSource` without `RunContinuationsAsynchronously` re-introducing the documented Linux/.NET continuation-on-lock-holder deadlock. Use the implicit contract list (lock order, thread-affinity) and check every callsite from 2.5b for violations. Remember the single-`ISender`-per-thread contract: flag code that shares a borrowed sender across threads.

**Agent 3 — Performance & allocations:** regressions and allocations on the per-row / per-frame hot path — LINQ, `params`/closures capturing state, boxing (value type → `object`, primitives into non-generic APIs), `string` concatenation/interpolation instead of pooled buffers / `Span` / `stackalloc` / `ArrayPool<T>`, unnecessary `ToArray`/`ToList`, async state-machine allocs in tight loops. Algorithmic complexity: for each new loop or traversal, how does it scale with row count, column count, table count, segment/ring size? Flag O(n²)-or-worse patterns. Distinguish setup/handshake-path allocations (acceptable) from per-row/per-frame data-path allocations (not). For changed symbols now reachable from new contexts (per 2.5d), check whether any of those is a hot path.

**Agent 4 — Resource management & native memory:** leaks on all code paths (especially error paths). `IDisposable`/`IAsyncDisposable` — is every owned resource disposed on every path, is `using`/`await using` used, is double-dispose tolerated, are finalizers correct? Native memory: `NativeMemory.Alloc`/`Free` (RAM segments), mmap map/unmap and file handles (`QwpMmapSegment`, `QwpFiles`, `QwpSlotLock` flocks), `SafeHandle`/`FileStream` closure. The pool contract: **Dispose does not send** (pure release) — verify a change doesn't turn Dispose into a throwing/sending path, and that a reaped/discarded ws sender with un-acked ring frames isn't torn down in a way that silently drops data. Walk every callsite from 2.5b that constructs, owns, or transfers a disposable/native buffer and verify cleanup on all paths.

**Agent 5 — Test coverage:** coverage gaps, error-path tests, null tests, boundary conditions, regression tests present. Cross-reference 2.5d: every cross-context exposure should have a test exercising the changed symbol from that context (via `DummyQwpServer` for QWP, `DummyHttpServer`/`MockHttp` for HTTP, `QuestDbManager`/Docker for integration). Missing cross-context tests is a high-priority finding. Verify the change compiles and is tested on every affected TFM (net6.0-net10.0), not just net10.0 — WS/QWP is net7.0+ only. Test *efficacy* and test-*code* quality are handled by Agents 11-13; here focus only on whether coverage exists for every new or changed path.

**Agent 6 — Code quality & standards:** code smell, member ordering, naming, modern C# usage, dead code, third-party dependencies. **Every new `.cs` file must carry the Apache-2.0 license banner** (copy from an existing file) — flag its absence. Production code must not be made `public` to satisfy a test — the correct mechanism is `internal` + the `[InternalsVisibleTo]` friend list; flag any symbol widened past `internal` purely for test access. Comments default to none — flag comments that merely restate the code; a one-line *why* for a non-obvious constraint is fine. Also scan the diff for any committed compiled binary / build artifact (see the "Committed build artifacts" checklist) — a committed binary is **Critical**.

**Agent 7 — PR metadata & conventions:** title format (Conventional Commits), description quality, `Fixes #NNN`, labels, commit messages, and whether the multi-TFM / public-API impact is called out.

**Agent 8 — Async & native-memory safety (only if the diff touches async, `unsafe`, native memory, or `#if`-gated code):** hunt for deadlock and corruption sites. Sync-over-async (`.Result`/`.Wait()`/`.GetAwaiter().GetResult()` on a path that can run under a captured `SynchronizationContext` or exhaust the pool), `async void`, fire-and-forget `Task` whose exception is unobserved, missing `ConfigureAwait(false)`, `CancellationToken` dropped so an operation can't be cancelled, `TaskCompletionSource` continuations running on the lock holder's stack. `unsafe`/pointer code and `Span`/`stackalloc` lifetime: a span or pointer escaping the buffer it borrows, `stackalloc` in a loop, reading past a declared length, a mmap view used after unmap. Integer overflow where it is actually reachable — e.g. tick/`long` math in backoff/reconnect (the reconnect policy deliberately saturates to avoid `long` overflow on days-scale backoff; a change that reintroduces overflow is a finding), byte-length/offset arithmetic in varint/frame encoding. Every such site that a real workload can hit is a finding.

**Agent 9 — Cross-context caller impact:** walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling method plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke (latched-vs-thrown, nullability, disposal ownership, span lifetime, thread-affinity)?
- Is this caller in a context (the send/receive pump, the drainer thread, an error path, a hot loop, the housekeeper, a reconnect/failover path, TLS handshake, the egress cursor) where the new behavior misbehaves even if the inputs are valid?
- For a changed interface member (`ISender` / `IBuffer` / `IQwpSegment` / `IQwpCursorTransport` / `IPooled*`): do all implementors still satisfy the new contract?
- For a changed symbol behind a TFM guard: does every target framework still compile and behave correctly?

This agent's output is structured per callsite: SAFE / BROKEN / NEEDS VERIFICATION. Every BROKEN entry is a P0 finding regardless of whether the file is in the diff. Not optional even when the diff is small — small diffs to widely-used seams (`ISender`, `SenderOptions`, the buffer) have the largest blast radius.

**Agent 10 — Fresh-context adversarial:** dispatched separately from agents 1-9 to escape checklist anchoring. Different rules:

- It receives ONLY the PR diff and the names of the changed files. NOT the change surface map, the implicit contract list, the cross-context exposure list, or the checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy.
- Free to use Read, Grep, Glob to explore however it wants.
- Findings are not pre-classified. Each states: what's wrong, why, and the code path that demonstrates it.

A finding here that none of agents 1-9 produced is high signal. Run in parallel with agents 1-9; mandatory regardless of diff size.

**Test-code agents (Agents 11-13) — run only when the diff adds or changes test code.** Launch them in the same parallel batch as agents 1-10. Each receives the diff, the change surface map, and the test surface inventory from 2.5e. Agent 11 mirrors Agent 1 (correctness), Agent 12 mirrors Agent 6 (code quality), and Agent 13 verifies regression-test efficacy. Tests are not second-class code — apply the same adversarial rigor.

**Agent 11 — Test efficacy & correctness (adversarial):** prove each test actually exercises the production change and could fail if that change regressed.
- **Vacuous assertions:** flag every assertion that cannot fail — `Assert.That(true)`, `Assert.That(x, Is.EqualTo(x))`, asserting a value the test itself just hard-coded, or a `[Test]` with no assertion and no `Assert.Throws`/`Assert.CatchAsync`.
- **Wrong exception matcher:** `Assert.ThrowsAsync<IngressError>` is exact-type; the server-terminal path throws `LineSenderServerException` (an `IngressError` subclass), so an exact-type matcher silently fails to match — the correct assertion is `Assert.CatchAsync<IngressError>`. Flag exact-type matchers used against a subclass.
- **Timing-count assertions:** CI runners are slower than dev machines. Flag any test asserting an *absolute* throughput/iteration count or an absolute duration; the correct form asserts *behavior* (count increased past a budget, elapsed ≥ a dwell, attempts kept climbing). An absolute `>= N` on a timing-dependent counter is a flake — a real one already had to be de-flaked in this repo.
- **Tests that don't reach the changed code:** the assertion passes whether or not the production change is present. Trace the data flow from the changed symbol to the assertion.
- **Happy-path-only:** no assertion on the error/null/reconnect path the production change added.
- **Concurrency-test correctness:** races in the harness itself, missing latches/`ManualResetEventSlim`, an assertion thrown on a spawned thread/`Task` where it is swallowed instead of failing the test, `Task.Delay`/`Thread.Sleep`-based synchronization that is timing-dependent.
- Each finding states the exact assertion and why it cannot fail or what it fails to cover.

**Agent 12 — Test-code quality & maintainability:**
- **Reflection overuse:** flag `BindingFlags.NonPublic`, `GetField`/`GetMethod`/`GetProperty` + `SetValue`/`Invoke`, `typeof(...).GetField(...)` when the internal is already reachable via `[InternalsVisibleTo]`, a public/internal API, or a constructor. Reflection in tests is a last resort; name the neater path.
- **No code reuse / boilerplate stamping:** before accepting repeated setup/assertion blocks, Grep for existing helpers, base `[TestFixture]` classes, and the in-process servers (`DummyQwpServer`, `DummyHttpServer`, `QuestDbManager`) using the 2.5e inventory. If a helper already exists that the new test reimplements inline, flag it and name it. Duplicated blocks across new tests that should be a shared helper or a `[TestCase]`/`[TestCaseSource]` parameterization are findings.
- **XML-doc / comment bloat:** flag multi-paragraph doc comments on `[Test]` methods, comments that merely restate the test name, and stacked/duplicated comments. Test intent belongs in a precise test name plus at most a one-line comment.
- **Residue and smells:** dead code, commented-out code, copy-paste leftovers (a `TestFoo` that actually tests bar), `Console.WriteLine` debugging, `[Ignore]`/`[Explicit]` without a referenced reason, magic numbers ≥ 5 digits without `_` separators.
- **Which standards apply:** allocation/zero-GC rules do NOT apply to test code — do not flag `List`/`LINQ`/allocations in tests. The Apache-2.0 banner, `is`/`has` boolean naming, and member ordering DO apply.

**Agent 13 — Regression-test efficacy verification:** for any PR that claims to fix a bug, verify the regression test would actually fail without the production change. Reason about reverting the production hunk and confirm the new/changed test's assertions would then fail. If the test still passes with the fix reverted, it is not a regression test — flag it. State, per test, which production line the test depends on and what its assertion would do if that line were reverted. Run only when the PR is a fix; skip for pure features/refactors.

Combine all agent findings into a single deduplicated **draft** report. Do NOT present this draft to the user yet — it goes straight into verification.

## Step 3b: Verify every finding against source code

The parallel review agents work from the diff plus the change surface map and frequently produce false positives — especially around disposal ownership, polymorphic dispatch through interfaces, async control flow, and latched-vs-thrown error semantics. Every finding MUST be verified before it is reported.

For each finding in the draft report:

1. **Read the actual source code** at the exact lines cited. Do not rely on the agent's description alone.
2. **Trace the full code path:** follow callers, interface dispatch, and runtime types. A method called on an `ISender` reference dispatches to `HttpSender` / `TcpSender` / `QwpWebSocketSender` / `BorrowedSender`; a pooled handle wraps a `PooledSender` that wraps the real sender. Verify which concrete type actually runs.
3. **For error-semantics claims:** confirm whether the error is *latched on the buffer* (ILP model — surfaces later on `At`/`Flush`) or *thrown immediately* or *aborts the row* (`QwpTableBuffer`). An agent claiming "this doesn't throw" may be wrong because the error is latched, and vice-versa.
4. **For resource-leak claims:** trace every allocation (`NativeMemory.Alloc`, mmap, `FileStream`, `IDisposable`) to its `Free`/unmap/`Dispose` on ALL paths (happy, error, `finally`, second-dispose). Check for polymorphic disposal and the pool's deferred-teardown paths. Before claiming a leak between allocation and cleanup, verify the intervening code can actually throw.
5. **For async/deadlock claims:** verify the sync-over-async or missing-`ConfigureAwait` site is actually on a path that runs under a captured context or can starve the pool. Library code generally has no `SynchronizationContext`, so not every `.Result` is a deadlock — confirm reachability.
6. **For concurrency claims:** confirm the shared state is genuinely reachable from two threads given the single-producer-per-sender contract. A "race" on state only ever touched by one producer thread is a false positive.
7. **For overflow / numeric claims:** check whether the overflow is reachable at realistic scale (buffers ≤ `int.MaxValue`, real row/column/segment counts, real backoff durations). If it requires values beyond that scale, drop it. The reconnect policy's saturating tick math is deliberate — don't flag it as overflow.
8. **For performance claims:** check whether the cost is measurable in a realistic scenario. Downgrade to a nit if the saving is negligible relative to surrounding work. Exception: an allocation on a per-row/per-frame hot path is always worth flagging, even a single one.
9. **For cross-context findings (Agent 9):** re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production paths. High-value but easiest to overstate — verify carefully, especially interface-implementor claims (check every implementor actually exists and is affected).
10. **For test-efficacy findings (Agents 11, 13):** re-read the cited assertion in full context and confirm it truly cannot fail. A "wrong exception matcher" claim is only valid if the thrown type is actually a subclass of the asserted type. A "timing-count" claim is only valid if the asserted value is genuinely timing-dependent. For "would pass without the fix" claims, trace what the assertion observes against the reverted production hunk.
11. **For test-code-quality findings (Agent 12):** confirm a flagged reflective access really has a non-reflective alternative (the internal may already be visible via `[InternalsVisibleTo]` — verify) and confirm a "reinvented helper" finding by locating the helper with Grep and checking its signature fits.
12. **Classify each finding** as:
    - **CONFIRMED in-diff** — the bug is real and inside the diff
    - **CONFIRMED at out-of-diff callsite** — the bug is in an unchanged file because the changed symbol is used there in a way that's now broken (cite the file and the contract from 2.5c that was violated)
    - **FALSE POSITIVE** — the code is actually correct (explain why)
    - **CONFIRMED with nuance** — the issue exists but is less severe than stated (explain)

**Move false positives to a separate "Downgraded" section** at the end of the report. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification agents in parallel where findings are independent. Each verification agent should read surrounding source files, not just the diff.

## Review checklists

Review the diff for:

### Correctness & bugs
- Null handling and nullable-reference correctness; sentinel-null vs `null`
- Edge cases and error paths; correct `ErrorCode`; latched-vs-thrown consistency
- Logic errors, off-by-one, incorrect bounds, wrong operator precedence
- Varint / bit-writer / frame-length arithmetic: overflow rejection, capacity validation, torn-tail handling
- QWP self-sufficiency preserved (full schema + full symbol-dict delta per frame — required for reconnect/SF replay)
- **Reachability expansion:** for each changed symbol, list the transports (HTTP/TCP/WS), async contexts, error paths, and reconnect/failover states it can now appear in but didn't before. Verify it works in each.

### Concurrency & async
- Unsynchronized shared mutable state; missing `Volatile`/`Interlocked`; unsafe publication
- Lock ordering / deadlock (`_stateLock`, `_gate`); which lock is held on return
- Sync-over-async (`.Result`/`.Wait()`/`.GetAwaiter().GetResult()`), `async void`, unobserved `Task` exceptions, missing `ConfigureAwait(false)` in library code
- `TaskCompletionSource` continuation placement (`RunContinuationsAsynchronously`; don't fire under the lock holder)
- Single-`ISender`/`IQwpQueryClient`-per-thread contract not violated
- For every changed symbol, check whether it is now called from a thread/context (per 2.5d) where the previous assumptions don't hold

### Performance
- Allocations on the per-row / per-frame data path: boxing, closures capturing state, LINQ, `string` concat/interpolation, `ToArray`/`ToList`, avoidable async state machines
- Prefer `Span<T>`/`ReadOnlySpan<T>`, `stackalloc`, `ArrayPool<T>`, and the existing pooled scratches (decoder per-column scratches, `QwpBitWriter`) over fresh allocation
- Setup/handshake-path allocations acceptable; per-row/per-frame allocations are not
- Algorithmic complexity at scale: for each new loop/traversal, complexity vs row count, column count, segment/ring size; flag O(n²) or worse

### Code quality
- Apache-2.0 license banner on every new `.cs` file
- No symbol widened past `internal` to satisfy a test — use `[InternalsVisibleTo]`
- Comments default to none; a one-line *why* only for a non-obvious constraint
- Code smell: overly complex methods, deep nesting, unclear intent, dead code
- No new third-party dependencies without justification
- `#if NET7_0_OR_GREATER` (or other TFM) gating correct — WS/QWP code must not break the net6.0 build; HTTP/TCP must compile on every TFM

### Committed build artifacts
- **A newly committed compiled binary / build output is always Critical.** This library ships as a NuGet package built and published by CI; `bin/`, `obj/`, `*.dll`, `*.exe`, `*.nupkg`, `*.snupkg`, `*.pdb` are not committed. A binary in the diff cannot be reviewed, audited, or reproduced from source, can smuggle in unaudited code, and bloats history irreversibly.
- Detect it structurally: run `git diff --numstat` / `--stat` and flag every added/modified file git reports as binary (`numstat` shows `-`/`-`; `--stat` shows a `Bin … -> … bytes` marker).
- The only acceptable binaries are genuine test-input fixtures a test reads (e.g. a TLS `.pfx` for a cert-validation test) — and even those must be justified. `.DS_Store` and editor cruft must never be committed.
- Suggested fix: drop the artifact, confirm `.gitignore` covers it, and let CI produce the package.

### .NET / repo coding standards
- Boolean names use `is` / `has` prefix
- Modern C#: `switch` expressions, pattern matching (`is`/`is not`, property patterns), target-typed `new`, `using`/`await using`, `Span`/`ReadOnlySpan`, nullable reference types
- Consistent member ordering matching the surrounding file
- Match the surrounding code's idiom, naming, and comment density

### Resource management
- Every owned `IDisposable`/`IAsyncDisposable` disposed on all paths (especially error paths); `using` / `await using` where applicable
- Native memory (`NativeMemory.Alloc`/`Free`), mmap views, file handles, and flocks released on all paths; second-dispose tolerated
- Pool contract respected: **Dispose is pure release (no send, no throw)**; a reaped/discarded ws sender with un-acked ring frames must not be torn down in a way that silently drops data

### Store-and-forward & pool startup invariants (QWP)
Apply this whenever the diff touches the SF sender, the cursor send engine, the async drainer / send/receive pump, primary reconnect/failover, `SenderPool` / `QueryClientPool` startup, `lazy_connect`, or `initial_connect_retry`. A violation here is a **Critical** finding: the whole point of store-and-forward is that a running producer never loses data and never hard-fails on a transient outage.

**Drainer (steady state — once the engine is running).**
- Once running, the send loop / drainer ships buffered SF (or in-memory ring) data to the server. It MUST NOT propagate server / transport errors back to the caller (`ISender` producer calls, `Flush()`, the pooled handle). The ONLY error class a running engine may surface is **SF / ring out of space** (backing buffer full). Flag any other failure class (connect-refused, DNS, unreachable/black-hole, TLS/cert, auth, role-reject, upgrade/protocol timeout, reset) that can escape onto a producer or borrow call.
- Primary reconnect MUST be fully contained inside the engine and MUST have **no time limit** once past initial connect — no `reconnect_max_duration_millis`-style budget on the steady-state loop, no deadline, no "give up and latch terminal after N ms". A budget that latches the sender terminal on a long outage is a Critical violation: it drops a producer that store-and-forward promised to keep alive. Only the blocking SYNC initial connect (`initial_connect_retry=on`) is budget-bounded (Invariant B). Flag any bounded reconnect loop or terminal error reachable from the running engine's mid-stream reconnect path.
- Retry uses **exponential backoff** and handles every connect-failure class gracefully, without a hard fail — keep buffering and keep retrying until the wire is back. The per-attempt backoff may be capped (max delay between attempts); the RETRY LOOP ITSELF must be unbounded. Flag a capped total retry duration or an attempt-count cap on the steady-state loop.
- **Sanctioned terminals (orphan-slot drainer only).** The orphan drainer (`QwpBackgroundDrainer` via `QwpBackgroundDrainerPool`) MAY quarantine its slot (`.failed` sentinel, human-in-the-loop) on conditions terminal by design: auth failure, a non-421 upgrade reject, a protocol/poison-frame violation, corrupt segments, or a genuine durable-ack capability gap. These are NOT violations of the no-budget rule. A transient class (role reject, transient transport error) must NEVER drop a `.failed` sentinel — it is left for re-adoption on a later sweep. A transient state that quarantines the slot IS a Critical violation.
- **Mid-stream server NACKs (no-drop policy, NACK v2).** A rejection category a transient cluster state can produce (`WRITE_ERROR`, `INTERNAL_ERROR`, `UNKNOWN`, and any future status byte) is RETRIABLE: recycle the wire and replay from `ackedFsn+1`. It must NEVER drop the batch and NEVER latch terminal / quarantine on first sight. Only rejections deterministic under byte-identical replay (`SCHEMA_MISMATCH`, `PARSE_ERROR`, `SECURITY_ERROR` on a writable node) may go TERMINAL. A client that advances the ack watermark past a NACKed frame is silently losing data — **Critical**. A frame repeatedly rejected with no ack progress must escalate through the **poison-frame detector** (bounded consecutive strikes at the same head FSN, with a min-dwell window), not through a WS close-code list — close codes carry no policy semantics and every close is reconnect-eligible. `UNKNOWN` must fail OPEN (retry), never closed (terminal).

**Pool startup — two modes; the mode decides who sees connectivity errors.**
- `lazy_connect=on`: `QuestDBClient.Builder()…Build()` MUST succeed with **no server present**. The producing side must work immediately (writes buffer via SF/ring; ws ingest connects async), and the read pool stays enabled (defaults `query_pool_min=0`) so a query connects lazily on first `NewQuery` once the server is up. Verify `Build()` does not fail-fast, the sender does not throw on the first write while the server is down, and a later query succeeds once the server is up. `lazy_connect` + an explicit blocking `initial_connect_retry` or `query_pool_min>0` is rejected at config validation — verify that stays true.
- `lazy_connect` off (default): the initial connect MUST expose connectivity problems to the caller — DNS, connect-refused/unreachable, TLS/cert, auth, and connect/upgrade timeouts surface as a thrown `IngressError` at startup, not swallowed.
- **In BOTH modes the boundary is the same:** connectivity errors are only ever the caller's problem DURING initialization. Once connected and past init, the running engine reverts to the steady-state contract above — NEVER expose transport problems, NEVER impose a reconnect time budget, NEVER hard-fail on a transient outage. Anything that undermines the store-and-forward guarantee past init is Critical.

### Config string / SenderOptions (if the diff touches config parsing)
- `Utils/SenderOptions.cs` (and `QueryOptions.cs` for egress) is the single source of truth. A new key needs: parse + default, validation, ws-only-key handling (rejected on non-ws via `ValidateWebSocketKeys` / `ValidateWebSocketKeysAgainstDefaults` if ws-only), and `ToString()` round-trip (`new SenderOptions(s.ToString())` must reproduce it; programmatic-only delegates are `[JsonIgnore]`d out).
- Auth combination rules: HTTP/WS `username`+`password` mutually exclusive with `token`; TCP `username`(kid)+`token` valid together (`IsTcp()` checked first).
- Multi-`addr` allowed on HTTP and WS, rejected on TCP; `gzip=on` rejected for ws; `transaction=on` mutually exclusive with `sf_dir`.

### Test review (NUnit)
- **Coverage gaps:** for every new/changed code path, a corresponding test exists; if not, flag "missing test for X".
- **Cross-context coverage:** for every entry in the 2.5d list, a test exercises the changed symbol from that context (`DummyQwpServer` for QWP ingest/egress, `DummyHttpServer`/`MockHttp` for HTTP, `QuestDbManager`/Docker for integration).
- **Error-path / null / boundary:** failure cases, exceptions, null columns, empty buffers, single-row, max-value inputs, zero-length strings.
- **Concurrency tests** where shared state is touched (see `SfPoolConcurrencyTests`, connect-walk concurrency guard).
- **Multi-TFM:** the change is exercised on every affected framework (the CI filter runs the unit suite per-TFM); WS/QWP tests are net7.0+.
- **Correct matcher:** server-terminal assertions use `Assert.CatchAsync<IngressError>` (not exact-type `ThrowsAsync`, since `LineSenderServerException` subclasses it).
- **Regression tests:** if the PR fixes a bug, a test reproduces it and would fail without the fix.
- Use Grep/Glob to find existing test files for the changed classes and verify they cover the new behavior.

### Test code quality
- **No vacuous assertions.** Every assertion must be able to fail.
- **No absolute timing/count assertions** on timing-dependent counters — assert behavior (crossed a budget, elapsed ≥ dwell), never an absolute number; CI runners are slower and will flake.
- **Reflection is a last resort.** Flag `BindingFlags.NonPublic` + reflection when `[InternalsVisibleTo]`, a public/internal API, or a constructor reaches the same state; name the non-reflective path.
- **Reuse before reinventing.** Search for existing fixtures/helpers/servers before accepting inline setup; duplicated blocks an existing helper or a `[TestCase]`/`[TestCaseSource]` would cover are findings.
- **No doc/comment bloat** on `[Test]` methods; prefer a precise test name and at most a one-line comment.
- **Test-appropriate standards.** Allocation/zero-GC rules do NOT apply to tests. The Apache-2.0 banner, `is`/`has` naming, and member ordering DO apply.
- **No debugging residue.** No `Console.WriteLine`, no commented-out code, no `[Ignore]`/`[Explicit]` without a referenced reason.

### Unresolved TODOs and FIXMEs
- Scan the diff for `TODO`, `FIXME`, `HACK`, `XXX`, `WORKAROUND`. For each:
  - Pre-existing (moved/reformatted) or newly introduced in this PR?
  - If newly introduced: unfinished work that should block the merge, or an acceptable known limitation? Flag deferred bugs or incomplete implementations.
  - If it references a ticket/issue, verify the reference exists.

### Commit messages
- Conventional Commits: `type(scope): description` (matching the PR title convention)
- Body explains the *why* / user-facing impact, not just the *what*
- Active voice

## Step 4: Output

Present ONLY verified findings (false positives are excluded). Structure as:

### Critical
Issues that must be fixed before merge. **A newly committed compiled binary or other build artifact (see the "Committed build artifacts" checklist) is always Critical — this library ships as a CI-built NuGet package, so a binary in the diff is never acceptable.** A store-and-forward / NACK / pool-startup invariant violation (data loss, terminal-on-transient, reconnect budget on the steady-state loop, ack watermark advanced past a NACK) is Critical. Each must include:
- Exact file path and line numbers (including out-of-diff files)
- Whether the finding is **in-diff** or **out-of-diff**
- Code path trace showing why the bug is real
- For out-of-diff findings: the contract from 2.5c that was violated and the callsite that triggers it
- Suggested fix

### Moderate
Issues worth addressing but not blocking.

### Minor
Style nits and suggestions.

### Downgraded (false positives)
Findings from the initial review that were dismissed after source verification. For each:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Summary
- One-line verdict: approve, request changes, or needs discussion
- Highlight any regressions or tradeoffs
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the in-diff vs out-of-diff split (e.g., "5 findings in-diff, 3 out-of-diff"). If the diff is non-trivial and out-of-diff is zero, the cross-context pass likely underran — re-invoke Agent 9 with a wider grep before finalizing.
