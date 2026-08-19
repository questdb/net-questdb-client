---
name: review-pr
description: Review a GitHub pull request against net-questdb-client (.NET) coding standards. Performs an adversarial, blocking, mission-critical code review covering correctness, concurrency/async, performance and allocations, resource management (IDisposable + native memory), test coverage, test efficacy, test-code quality, and the QWP store-and-forward / pool invariants, then independently falsifies every candidate finding against source and reports only admitted, evidence-backed findings.
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Bash(git *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior engineer performing a blocking code review of **net-questdb-client**, the .NET client library for QuestDB. It is mission-critical software: bugs can cause **silent data loss**, data corruption, or crashes in customer ingestion pipelines that the customer cannot patch quickly. The store-and-forward sender exists precisely so a producer never loses data across an outage — a regression there is the worst class of bug in this repo. There is zero tolerance for correctness issues, resource/native-memory leaks, or data-loss paths. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Discovery is not a finding.** Treat every concern — including one produced by several agents — as an untrusted hypothesis until it passes the Step 3b admission gate. Report every *admitted* issue at the severity its evidence earns; omit everything else. A review with zero findings is a successful outcome.
- **Falsify before you explain.** Search for the missing producer, unsupported configuration, omitted caller, retry, guard, downstream offset, and merge-base behavior before building a narrative. Failure to disprove a hypothesis is not evidence for it, and uncertainty is never promoted to severity.
- **Keep the blast radius of the PR small.** This PR should fix what it set out to fix, plus anything this change demonstrably breaks. Pre-existing bugs, residual hardening opportunities whose behavior is unchanged from base, and propositions that only support another candidate are never findings against this PR and never affect its verdict. The one exception is a pre-existing bug that this PR demonstrably moves onto a live path. Small blast radius governs what this PR must *fix*, not what the review is allowed to *know*: a pre-existing bug proved to the same evidence bar leaves as a Step 4 adjacent issue draft rather than being thrown away.
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access or interleaved calls from the producer thread and the drainer/receive pump? What if the wire drops mid-flush, the server sends a partial frame, the TLS handshake fails, or auth is rejected? What if the buffer is empty, the column is null, the symbol dict is at capacity, or a value is `Decimal.MaxValue`?
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing `#if NET7_0_OR_GREATER` gating, missing disposal, missing documentation for non-obvious behavior.
- **Untested changed behavior is a coverage risk, not proof of a defect.** Missing tests alone cannot make a finding Critical. A Critical coverage gap must identify a supported, reachable user population and a credible regression mode with material impact — see Step 2.6. A named test with a real failure link remains the strongest evidence; when none exists, assess change risk and the least fragile meaningful test rather than blocking by category. Test difficulty never reduces the severity of an actual functional, data-loss, corruption, or resource-leak defect.
- **Urgency is neither evidence nor an exemption.** It may inform delivery sequencing only after user impact, regression risk, and stable-test feasibility are established. "Urgent", "simple", and "hard to test" are conclusions to prove, not reasons to skip analysis.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks (`src/net-questdb-client-benchmarks`) or reason about the change — does it actually improve things, or regress another case? If it says "simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use Read/Grep/Glob to inspect the surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem requires physically impossible conditions (a buffer larger than `int.MaxValue`, a value no public API can produce, a race the single-producer-thread contract forbids), it is not a real finding — drop it. Focus on bugs real workloads can trigger, not theoretical edge cases that exist only in the type system.
- **`Debug.Assert` is compiled out in Release.** It is a valid *development* guard for internal invariants, but it provides **no runtime protection in shipped builds** (Release is what customers run). Do NOT accept a `Debug.Assert` as sufficient validation for a condition that customer input or a hostile/buggy server can trigger — that path needs a real check + thrown `IngressError`/latched buffer error. Conversely, do not flag a `Debug.Assert` guarding a genuine library-internal invariant as "insufficient"; that is its correct use.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (QWP wire format / codecs, the cursor send engine or SF segment/ring/drainer, primary reconnect/failover, `SenderPool` / `QueryClientPool` / pool startup, TLS/auth, TCP ECDSA auth, or the public `ISender` / `IQwpWebSocketSender` / `IQwpQueryClient` / `IQuestDBClient` surface).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 2.6, 4. Skip Step 2.5 and agent fanout. Review the diff inline in the main loop, using Read/Grep on demand, covering correctness, null handling, disposal, test coverage, and .NET/QWP standards. Build the Step 2.6 coverage disposition inline. When the diff touches test code, also apply the test-efficacy and test-code-quality anti-pattern checks inline (vacuous assertions, timing-count assertions, reflection overuse, reinvented helpers, XML-doc bloat). Every candidate still passes the Step 3b admission gate inline from a blank evidence form; do not draft severity, a fix, or report prose first. |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d) plus Step 2.5e when test code is present. Run Agent 1 plus at most **two** applicable roles chosen from Agents 3, 5, 6, 11, and 12. Run an independent falsification task for each surviving atomic candidate. |
| **2** | Full Step 2.5 (including 2.5e when test code is present), but in 2.5b restrict the callsite inventory to `public` / `protected` / `internal`-visible-to-tests symbols (skip `private`). Run Agent 1 plus at most **four** change-relevant roles from Agents 2-8, 11, and 12. Run an independent falsification task for each surviving atomic candidate. |
| **3** | Full Step 2.5 and the complete admission protocol. Select at most **six** applicable discovery roles from Agents 1-13: Agent 1 always; Agent 9 for changed symbols with out-of-diff callers; Agents 2-8 only when their domain is touched; Agents 11-13 only for changed tests or a fix claim; Agent 10 only when a distinct adversarial pass is warranted. Depth comes from producer/reachability evidence and independent falsification, not agent count. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #77 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Step 1: Gather PR context

Every review must end this step with **`$BASE`** set — the commit the change is measured against. `$BASE` is required by every behavioral finding's same-trigger base check (Step 3b); a review that never established it cannot attribute anything. Base observations in this repo are static: `git show "$BASE:<path>"` (after a `git fetch` if the objects are missing locally) or `gh api` file reads at `$BASE`.

Capture the PR identifier in `$PR` (the part of `$ARGUMENTS` left after stripping the level token), then fetch metadata, diff, and review comments in a single bash call so `$PR` is in scope for all invocations:

```bash
PR='<PR number or URL from $ARGUMENTS, with any --level=N / -lN / bare-digit level token removed>'
gh pr view "$PR" --json number,title,body,labels,state,baseRefName,baseRefOid,headRefOid
gh pr diff "$PR"
gh pr diff "$PR" --numstat   # binary files show as `-<TAB>-<TAB><path>`
gh pr view "$PR" --comments
BASE=$(gh pr view "$PR" --json baseRefOid --jq .baseRefOid)
```

**Committed-binary gate (runs at every level).** Scan the `--numstat` output for any added/modified file git reports as binary (`-`/`-` in the added/deleted columns). This repo ships as a managed NuGet package built and published by CI; build outputs (`bin/`, `obj/`, `*.dll`, `*.nupkg`, `*.pdb`) are not committed. Any such file is a **Critical** finding regardless of review level — report it even at level 0. This is a static finding fully proved by the `--numstat` output itself. See the "Committed build artifacts" checklist for the rationale and the acceptable exception (genuine test-input fixtures only, e.g. a TLS `.pfx` a test reads).

## Step 2: PR title and description

Check against the repo's conventions:
- Title follows Conventional Commits: `type(scope): description` (e.g. `feat(qwp): ...`, `fix(http): ...`, `test(qwp): ...`). Common scopes: `qwp`, `http`, `tcp`, `pool`, `ws`, `sf`.
- Description speaks to end-user / API impact, not just implementation internals
- If fixing an issue, `Fixes #NNN` (or a link) is present
- Tone is level-headed and analytical, no superlatives or bold emphasis on numbers
- For public-API changes (`ISender` / `IQwpWebSocketSender` / `IQwpQueryReader` / `IQuestDBClient`, `SenderOptions` / `QueryOptions` config keys, `Sender.New` / `QueryClient.New` / `QuestDBClient` factories), the description calls out the API/behavior change explicitly
- Multi-target impact: if a change is `#if NET7_0_OR_GREATER`-gated (WS/QWP) vs applies to all TFMs (HTTP/TCP), that should be clear
- Bundled related fixes are allowed; do not demand a split

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

## Step 2.6: Coverage disposition

This step runs at EVERY review level, for EVERY PR that touches production code — including PRs that contain no test changes at all.

For every changed behavior, locate the test that exercises it via real Grep/Glob searches across the test tree (search for the symbol name, the config key, the error message text). Citing a test without a recorded search is a skill violation, same as 2.5b; "existing tests probably cover it" is banned. A cited test needs a **failure link**: one line stating what it asserts and why the assertion fails if this specific behavior regresses — "the test calls the method" is not a failure link.

Rows with no effective test are marked **UNTESTED**, then classified by evidence rather than category:

- **Critical gap (blocking):** only when the changed path and affected population are supported and reachable, a credible regression would cause a material Critical consequence (data loss/corruption, a crash or hang, a security failure, a compatibility break, unbounded resource growth), existing controls do not contain that risk, and the row passes Step 3b admission. The label "bug fix", "public API", "user-visible", "concurrency", or "security" never makes a gap Critical by itself.
- **Moderate gap:** meaningful but bounded regression exposure, including most bug fixes without a regression test, internal/error paths with a distinct but non-Critical consequence, or material uncertainty that does not meet the Critical burden.
- **Accepted gap:** low-risk, localized or mechanical behavior where recorded analysis shows that the least invasive meaningful test is disproportionate or more fragile than the code under test and existing safeguards make residual user risk small. Keep the rationale private unless it is material to the verdict.
- **Exempt:** verified no-behavioral-change rows (pure rename, dead-code removal, comment/doc/CI-only). "Refactor" claimed by the PR description is not an exemption — only a verified no-behavioral-change delta is.

A bug-fix label or zero test changes triggers this analysis; neither predetermines severity or verdict. Urgency cannot waive an actual defect or an admitted Critical gap. The disposition is required internal evidence for Agent 5, Agents 11-13, and the Step 4 test gate. Publish only admitted gaps; keep COVERED, ACCEPTED, EXEMPT, and omitted rows private unless the user asks.

## Step 3: Change-specific candidate discovery

Run this step with the Agent tool using fresh-context agents. Select only roles whose domain is materially touched, obey the level's discovery cap, and launch those roles as fresh-context, read-only tasks. Agent count is never evidence and unused roles are skipped. The parent session owns role selection, the private ledger, admission, severity, and output; children return candidates only.

Every selected agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)
3. The coverage disposition from Step 2.6

The diff plus surface map can be large — write them to a shared file under a temp directory and point each task at that path rather than pasting the whole payload into every task. Agent 10 is a deliberate reduced-context exception and receives only the diff and changed-file names (see its entry).

### Candidate-discovery directive (applies to all agents)

- You are a **hypothesis generator**, not an authority to publish a finding. Output atomic propositions for independent falsification. Do not assign severity, propose fixes, write persuasive titles, or use "verified", "proved", or "confirmed". Any role text below that mentions a finding or severity describes what to inspect, not what you may conclude.
- For each candidate, cite the exact changed hunk or unchanged callsite contract allegedly broken. Out-of-diff impact is valuable only after the PR-caused contract delta is established.
- Name the **supported-state producer**: the exact public-API call sequence, config string, server frame/response, wire event, or code path that creates every required trigger condition. If you cannot locate it, write `producer: unknown`; do not invent a deployment or state.
- Give the reachability chain, head observation, same-trigger merge-base observation, user-visible symptom, and raw evidence paths/commands. Mark anything not actually checked as `unknown`.
- Actively seek disproof: an unsupported or config-rejected combination, an absent producer of the alleged state, an omitted caller, retry, guard, lock, validation, downstream recovery, or unchanged/better base behavior. Record the strongest counterevidence.
- Claims containing **never**, **only**, **exactly one**, **no retry**, or equivalent universal negatives require an exhaustive caller/event-source inventory, not one traced path.
- A proposition with no independent consequence is evidence for its parent candidate, not a standalone candidate. If the parent falls, its dependent propositions fall with it.
- Pre-existing bugs and residual hardening whose same-trigger behavior is unchanged or better than base are outside this PR's findings and verdict. Never file them as findings and never propose them as changes to this PR. A fully proved one leaves as a Step 4 adjacent issue draft; an unproved one stays in the private ledger.
- Two agents repeating the same reasoning are one hypothesis, not corroboration. Corroboration requires independent evidence types and still does not bypass Step 3b.
- Returning no candidate is valid and preferred to returning a speculative one.

### Agents

Use the following as a role catalog. Select only the roles allowed by the chosen level and change surface; do not launch the whole catalog.

**Agent 1 — Correctness & bugs:** null handling, edge cases, logic errors, off-by-one, operator precedence, wrong `ErrorCode`, error paths, latched-vs-thrown mismatches. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches the store-and-forward sender, the cursor send engine, the async drainer / receive pump, primary reconnect/failover, NACK handling, the poison detector, or pool startup (`lazy_connect` / `initial_connect_retry` / `SenderPool` / `QueryClientPool`), also verify the "Store-and-forward & pool startup invariants" checklist — a running drainer that propagates a transport error to the caller, imposes a reconnect time budget, drops a NACKed frame, or hard-fails on a transient outage is a **Critical** (data-loss) finding.

**Agent 2 — Concurrency & async:** race conditions, shared mutable state without a lock, missing `Volatile`/`Interlocked`, lock ordering / deadlock, thread-safety of data structures shared between the producer thread and the send/receive pumps or the housekeeper. Blocking on async (`.Result` / `.Wait()` / `.GetAwaiter().GetResult()`) that can deadlock or starve the thread pool; `async void`; unobserved `Task` exceptions; missing `ConfigureAwait(false)` in library code; `TaskCompletionSource` without `RunContinuationsAsynchronously` re-introducing the documented Linux/.NET continuation-on-lock-holder deadlock. Use the implicit contract list (lock order, thread-affinity) and check every callsite from 2.5b for violations. Remember the single-`ISender`-per-thread contract: flag code that shares a borrowed sender across threads.

**Agent 3 — Performance & allocations:** regressions and allocations on the per-row / per-frame hot path — LINQ, `params`/closures capturing state, boxing (value type → `object`, primitives into non-generic APIs), `string` concatenation/interpolation instead of pooled buffers / `Span` / `stackalloc` / `ArrayPool<T>`, unnecessary `ToArray`/`ToList`, async state-machine allocs in tight loops. Algorithmic complexity: for each new loop or traversal, how does it scale with row count, column count, table count, segment/ring size? Flag O(n²)-or-worse patterns. Distinguish setup/handshake-path allocations (acceptable) from per-row/per-frame data-path allocations (not). For changed symbols now reachable from new contexts (per 2.5d), check whether any of those is a hot path.

**Agent 4 — Resource management & native memory:** leaks on all code paths (especially error paths). `IDisposable`/`IAsyncDisposable` — is every owned resource disposed on every path, is `using`/`await using` used, is double-dispose tolerated, are finalizers correct? Native memory: `NativeMemory.Alloc`/`Free` (RAM segments), mmap map/unmap and file handles (`QwpMmapSegment`, `QwpFiles`, `QwpSlotLock` flocks), `SafeHandle`/`FileStream` closure. The pool contract: **Dispose does not send** (pure release) — verify a change doesn't turn Dispose into a throwing/sending path, and that a reaped/discarded ws sender with un-acked ring frames isn't torn down in a way that silently drops data. Walk every callsite from 2.5b that constructs, owns, or transfers a disposable/native buffer and verify cleanup on all paths.

**Agent 5 — Test coverage:** coverage gaps, error-path tests, null tests, boundary conditions, regression tests present. Consume the Step 2.6 coverage disposition: re-verify every claimed test and failure link (read the assertion, don't trust the row) and hunt for behavioral changes the disposition missed. Cross-reference 2.5d: every cross-context exposure should have a test exercising the changed symbol from that context (via `DummyQwpServer` for QWP, `DummyHttpServer`/`MockHttp` for HTTP, `QuestDbManager`/Docker for integration). For each missing cross-context test, record an UNTESTED Step 2.6 row; do not predetermine its severity or publication. Verify the change compiles and is tested on every affected TFM (net6.0-net10.0), not just net10.0 — WS/QWP is net7.0+ only. Test *efficacy* and test-*code* quality are handled by Agents 11-13; here focus only on whether coverage exists for every new or changed path.

**Agent 6 — Code quality & standards:** code smell, member ordering, naming, modern C# usage, dead code, third-party dependencies. **Every new `.cs` file must carry the Apache-2.0 license banner** (copy from an existing file) — flag its absence. Production code must not be made `public` to satisfy a test — the correct mechanism is `internal` + the `[InternalsVisibleTo]` friend list; flag any symbol widened past `internal` purely for test access. Comments default to none — flag comments that merely restate the code; a one-line *why* for a non-obvious constraint is fine. Also scan the diff for any committed compiled binary / build artifact (see the "Committed build artifacts" checklist) — a committed binary is **Critical**.

**Agent 7 — PR metadata & conventions:** title format (Conventional Commits), description quality, `Fixes #NNN`, labels, commit messages, and whether the multi-TFM / public-API impact is called out.

**Agent 8 — Async & native-memory safety (only if the diff touches async, `unsafe`, native memory, or `#if`-gated code):** hunt for deadlock and corruption sites. Sync-over-async (`.Result`/`.Wait()`/`.GetAwaiter().GetResult()` on a path that can run under a captured `SynchronizationContext` or exhaust the pool), `async void`, fire-and-forget `Task` whose exception is unobserved, missing `ConfigureAwait(false)`, `CancellationToken` dropped so an operation can't be cancelled, `TaskCompletionSource` continuations running on the lock holder's stack. `unsafe`/pointer code and `Span`/`stackalloc` lifetime: a span or pointer escaping the buffer it borrows, `stackalloc` in a loop, reading past a declared length, a mmap view used after unmap. Integer overflow where it is actually reachable — e.g. tick/`long` math in backoff/reconnect (the reconnect policy deliberately saturates to avoid `long` overflow on days-scale backoff; a change that reintroduces overflow is a finding), byte-length/offset arithmetic in varint/frame encoding. Every such site that a real workload can hit is a candidate.

**Agent 9 — Cross-context caller impact:** walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling method plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke (latched-vs-thrown, nullability, disposal ownership, span lifetime, thread-affinity)?
- Is this caller in a context (the send/receive pump, the drainer thread, an error path, a hot loop, the housekeeper, a reconnect/failover path, TLS handshake, the egress cursor) where the new behavior misbehaves even if the inputs are valid?
- For a changed interface member (`ISender` / `IBuffer` / `IQwpSegment` / `IQwpCursorTransport` / `IPooled*`): do all implementors still satisfy the new contract?
- For a changed symbol behind a TFM guard: does every target framework still compile and behave correctly?

This agent's output is structured per callsite: SAFE / CANDIDATE / INSUFFICIENT_EVIDENCE. A CANDIDATE is only an atomic hypothesis for Step 3b; it has no severity yet. Select this role whenever changed symbols have meaningful out-of-diff callers — small diffs to widely-used seams (`ISender`, `SenderOptions`, the buffer) have the largest blast radius. It counts toward the level's discovery cap.

**Agent 10 — Fresh-context adversarial:** dispatched separately from agents 1-9 to escape checklist anchoring. Different rules:

- It receives ONLY the PR diff and the names of the changed files. NOT the change surface map, the implicit contract list, the cross-context exposure list, the coverage disposition, or the checklists below.
- Its sole instruction: "generate a small set of falsifiable ways this code could be wrong, and try to disprove each before returning it". No category list, no failure-mode taxonomy.
- Free to use Read, Grep, Glob to explore however it wants.
- Each surviving output follows the candidate contract: atomic proposition, changed attribution, producer, reachability, head/base observations, symptom, counterevidence, and missing evidence. No severity or fix.

The point is to escape the structured frame, not to create privileged findings. A unique hypothesis is not high signal by itself, and overlap is not corroboration unless it supplies an independent evidence type. Select this role only when a distinct adversarial pass is warranted; it counts toward the level's discovery cap.

**Test-code agents (Agents 11-13) — eligible only when the diff adds or changes test code or claims a bug fix.** A production change with no test code is still handled by the Step 2.6 coverage disposition. Select only the applicable test roles within the level's discovery cap. Each receives the diff, the change surface map, and the test surface inventory from 2.5e. Agent 11 mirrors Agent 1 (correctness), Agent 12 mirrors Agent 6 (code quality), and Agent 13 verifies regression-test efficacy. Tests are not second-class code — apply the same adversarial rigor.

**Agent 11 — Test efficacy & correctness (adversarial):** prove each test actually exercises the production change and could fail if that change regressed.
- **Vacuous assertions:** flag every assertion that cannot fail — `Assert.That(true)`, `Assert.That(x, Is.EqualTo(x))`, asserting a value the test itself just hard-coded, or a `[Test]` with no assertion and no `Assert.Throws`/`Assert.CatchAsync`.
- **Wrong exception matcher:** `Assert.ThrowsAsync<IngressError>` is exact-type; the server-terminal path throws `LineSenderServerException` (an `IngressError` subclass), so an exact-type matcher silently fails to match — the correct assertion is `Assert.CatchAsync<IngressError>`. Flag exact-type matchers used against a subclass.
- **Timing-count assertions:** CI runners are slower than dev machines. Flag any test asserting an *absolute* throughput/iteration count or an absolute duration; the correct form asserts *behavior* (count increased past a budget, elapsed ≥ a dwell, attempts kept climbing). An absolute `>= N` on a timing-dependent counter is a flake — a real one already had to be de-flaked in this repo.
- **Tests that don't reach the changed code:** the assertion passes whether or not the production change is present. Trace the data flow from the changed symbol to the assertion.
- **Happy-path-only:** no assertion on the error/null/reconnect path the production change added.
- **Concurrency-test correctness:** races in the harness itself, missing latches/`ManualResetEventSlim`, an assertion thrown on a spawned thread/`Task` where it is swallowed instead of failing the test, `Task.Delay`/`Thread.Sleep`-based synchronization that is timing-dependent.
- Each candidate states the exact assertion and why it cannot fail or what it fails to cover.

**Agent 12 — Test-code quality & maintainability:**
- **Reflection overuse:** flag `BindingFlags.NonPublic`, `GetField`/`GetMethod`/`GetProperty` + `SetValue`/`Invoke`, `typeof(...).GetField(...)` when the internal is already reachable via `[InternalsVisibleTo]`, a public/internal API, or a constructor. Reflection in tests is a last resort; name the neater path.
- **No code reuse / boilerplate stamping:** before accepting repeated setup/assertion blocks, Grep for existing helpers, base `[TestFixture]` classes, and the in-process servers (`DummyQwpServer`, `DummyHttpServer`, `QuestDbManager`) using the 2.5e inventory. If a helper already exists that the new test reimplements inline, flag it and name it. Duplicated blocks across new tests that should be a shared helper or a `[TestCase]`/`[TestCaseSource]` parameterization are findings.
- **XML-doc / comment bloat:** flag multi-paragraph doc comments on `[Test]` methods, comments that merely restate the test name, and stacked/duplicated comments. Test intent belongs in a precise test name plus at most a one-line comment.
- **Residue and smells:** dead code, commented-out code, copy-paste leftovers (a `TestFoo` that actually tests bar), `Console.WriteLine` debugging, `[Ignore]`/`[Explicit]` without a referenced reason, magic numbers ≥ 5 digits without `_` separators.
- **Which standards apply:** allocation/zero-GC rules do NOT apply to test code — do not flag `List`/`LINQ`/allocations in tests. The Apache-2.0 banner, `is`/`has` boolean naming, and member ordering DO apply.

**Agent 13 — Regression-test efficacy verification:** for any PR that claims to fix a bug, verify the regression test would actually fail without the production change. Reason about reverting the production hunk and confirm the new/changed test's assertions would then fail. If the test still passes with the fix reverted, it is not a regression test — flag it. State, per test, which production line the test depends on and what its assertion would do if that line were reverted. Run only when the PR is a fix; skip for pure features/refactors.

Combine agent outputs into a private **candidate ledger**. Split compound narratives into atomic propositions, deduplicate by proposition plus evidence, and record dependencies. Do not draft report prose, severity, or a suggested fix. A candidate is not a finding.

## Step 3b: Independently falsify, prove, and admit candidates

The discovery agents work from the diff plus the change surface map and frequently produce false positives — especially around disposal ownership, polymorphic dispatch through interfaces, async control flow, and latched-vs-thrown error semantics. Use this state machine with no shortcuts:

`HYPOTHESIS → FALSIFYING → PROVEN → ADMITTED`

Any missing proof, unresolved contradiction, unsupported producer, or dependence on an omitted premise ends at `OMITTED`. There is no `DOWNGRADED` state for an unproven behavioral claim, and "could not disprove" never means `PROVEN`.

At levels 1-3, launch one fresh-context falsifier per atomic candidate. The falsifier receives only (a) the neutral proposition, (b) the base/head revision identities (`$BASE` and the PR head SHA) and relevant file names, and (c) raw evidence paths. **Do not send** the discovery narrative, proposed severity, suggested fix, author identity, other agents' votes, or statements that the claim was verified. At level 0, the parent applies the same protocol inline from a blank evidence form before writing any report prose.

The falsifier's first task is to construct the strongest disproof: find a missing state producer, a config combination the parser rejects, an omitted caller or event source, a retry, guard, lock, validation, downstream offset, or identical/better base behavior. Only if the candidate survives does it assemble affirmative proof.

A behavioral candidate is admitted only when every field below is backed by cited evidence:

- **Attribution:** exact changed hunk, or exact unchanged callsite plus the contract this PR changed.
- **Supported-state producer:** the exact public-API call sequence, config string, server frame/response, wire event, or code path that creates every trigger condition. A reachable consumer branch is not proof that any producer can create its input.
- **Reachability:** complete producer-to-symptom path, including callers, event sources, retries, guards, locks, and offsets.
- **Head observation:** the code path at the reviewed revision, traced line by line to the claimed output/state.
- **Base observation:** the identical trigger traced at `$BASE`, or `N/A — genuinely new surface` with proof that base cannot express the trigger.
- **User symptom:** independently observable consequence for a producer application or operator — lost or wrong rows, a crash or hang, a leak, a misleading error. A statement that merely justifies another candidate is not a finding.
- **Counterevidence search:** strongest attempted disproof and why it does not apply.
- **Evidence:** the exact code chain (file:line per step) or artifact, plus the revision identity it was read at.

For static findings fully proved by source — compile errors, a missing license banner, a committed binary, direct standards violations — mark producer/head/base fields `N/A — static` and cite the complete source proof. For a coverage gap, recorded searches may statically prove only that an effective test is absent; they never make the supported-state producer, reachability, affected population, credible regression consequence, or user impact `N/A`. A Critical coverage gap must prove those fields under Step 2.6. `N/A` is forbidden whenever a load-bearing premise concerns runtime shape, reachability, or impact.

Special burdens:

- A format/version/state compatibility claim (an SF segment shape an older client wrote, a server frame shaped like X, a config combination) must identify an actual producer that creates the alleged state in a supported combination. A constant comparison or reader guard is not a producer.
- A claim containing **never**, **only**, **exactly one**, **no retry**, or an equivalent universal negative requires an exhaustive recorded caller/event-source inventory, not one traced path.
- A concurrency or ordering claim must prove the interleaving reachable from source: cite both threads' entry/spawn points, the shared state, and the absent synchronization. Timing prose is not evidence.
- A regression-test claim must trace the assertion line by line against the reverted production hunk.
- If a parent premise is omitted, omit every candidate that depends on it; do not preserve its supporting propositions as Moderate findings.

This review is static — it does not build or run tests (the maintainer runs them). When a load-bearing step could only be decided by execution, record the validation limitation and the exact repro command in the private ledger and omit the candidate from the public findings. Never fall back from unavailable execution to confident prose.

After a candidate satisfies this admission schema, apply the domain-specific checks below:

1. **Read the actual source code** at the exact lines cited. Do not rely on the agent's description alone.
2. **Trace the full code path:** follow callers, interface dispatch, and runtime types. A method called on an `ISender` reference dispatches to `HttpSender` / `TcpSender` / `QwpWebSocketSender` / `BorrowedSender`; a pooled handle wraps a `PooledSender` that wraps the real sender. Verify which concrete type actually runs.
3. **For error-semantics claims:** confirm whether the error is *latched on the buffer* (ILP model — surfaces later on `At`/`Flush`) or *thrown immediately* or *aborts the row* (`QwpTableBuffer`). An agent claiming "this doesn't throw" may be wrong because the error is latched, and vice-versa.
4. **For resource-leak claims:** trace every allocation (`NativeMemory.Alloc`, mmap, `FileStream`, `IDisposable`) to its `Free`/unmap/`Dispose` on ALL paths (happy, error, `finally`, second-dispose). Check for polymorphic disposal and the pool's deferred-teardown paths. Before claiming a leak between allocation and cleanup, verify the intervening code can actually throw.
5. **For async/deadlock claims:** verify the sync-over-async or missing-`ConfigureAwait` site is actually on a path that runs under a captured context or can starve the pool. Library code generally has no `SynchronizationContext`, so not every `.Result` is a deadlock — confirm reachability.
6. **For concurrency claims:** confirm the shared state is genuinely reachable from two threads given the single-producer-per-sender contract, and that the interleaving chain meets the special burden above. A "race" on state only ever touched by one producer thread is disproved.
7. **For overflow / numeric claims:** check whether the overflow is reachable at realistic scale (buffers ≤ `int.MaxValue`, real row/column/segment counts, real backoff durations). If it requires values beyond that scale, omit it. The reconnect policy's saturating tick math is deliberate — don't flag it as overflow.
8. **For performance claims:** check whether the cost is measurable in a realistic scenario. Downgrade to a nit if the saving is negligible relative to surrounding work. Exception: an allocation on a per-row/per-frame hot path is always worth flagging, even a single one.
9. **For cross-context candidates (Agent 9):** re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production paths. High-value but easiest to overstate — verify carefully, especially interface-implementor claims (check every implementor actually exists and is affected).
10. **For test-efficacy candidates (Agents 11, 13):** re-read the cited assertion in full context and confirm it truly cannot fail. A "wrong exception matcher" claim is only valid if the thrown type is actually a subclass of the asserted type. A "timing-count" claim is only valid if the asserted value is genuinely timing-dependent. For "would pass without the fix" claims, trace what the assertion observes against the reverted production hunk.
11. **For test-code-quality candidates (Agent 12):** confirm a flagged reflective access really has a non-reflective alternative (the internal may already be visible via `[InternalsVisibleTo]` — verify) and confirm a "reinvented helper" finding by locating the helper with Grep and checking its signature fits.
12. **For coverage-gap candidates (UNTESTED rows from 2.6):** verify the recorded test search and failure-link analysis, then try to falsify the risk with existing indirect assertions, guards, type/compile guarantees, constrained inputs, downstream validation, or the CI multi-TFM matrix. Establish supported reachability, an affected population, a credible regression mode, and its material consequence before assigning Critical. Evaluate the least fragile meaningful test and concrete alternatives. Reject bare "simple", "urgent", "hard to test", or "covered indirectly" claims; test-feasibility evidence counts only when it names the proposed observation seam, why it is invasive/unstable, and why cheaper stable alternatives do not work. A Critical gap may be counterfactual about whether the code is currently wrong, but never about reachability or impact. Test difficulty does not downgrade an independently admitted functional defect.
13. **Verify the conjunction, not just the links.** A multi-step candidate ("A publishes early → B can throw → C swallows → D reads stale → data loss") is only as true as its weakest step. Identify the single **load-bearing step** — usually "this supported state can actually occur" — and try to falsify it first. Per-line support for each isolated link does not prove their conjunction. Reading code is not verification when the load-bearing step is a runtime-shape claim ("this branch is taken", "the guard does not fire", "the pump observes the flag late"); such a step is admitted only with a complete static chain that forces the shape, or it is omitted under the execution rule above. Votes do not count as corroboration; even independent evidence types must still satisfy every admission field.
14. **Derive a fix only after admission, then verify it compiles and closes the window.** A plausible fix is never evidence that the finding is real. Once admitted, check that every referenced variable is in scope and non-`null`, that ownership transfers do not create a double-dispose or leak, and that the fix closes every admitted path.
15. **Determine net user impact, then classify.** Step 4 assigns severity only after this determination; a behavioral candidate missing it is `OMITTED` and never reaches Step 4.

    **(a) Net user impact — answer all five, in order:**
    - **Population** — who reaches it: every user, every user of a named transport/feature (ws + `sf_dir`, pooled senders, egress readers, TCP auth), a specific API/config shape, or an operator-only path. "Any user in principle" is not a population. If no supported population can execute the producer, omit the behavioral candidate; do not preserve it as Moderate.
    - **Delta vs base** — what that population observes differently from `$BASE` for the identical trigger.
    - **Magnitude and frequency** — per row, per frame, per flush, per reconnect, once ever.
    - **Offsets** — what recovers this downstream before the user sees anything: a retry, an SF replay, the poison detector, a validation, a caller that discards the value. Name the offset, or write "none found, searched <where>".
    - **Net** — exactly one of: **net-negative** (the population is measurably worse off than base — only these can be admitted), **net-neutral** (omit from PR findings), **net-positive** (omit from PR findings).

    A coverage-gap row is counterfactual only about whether an unobserved regression currently exists; its producer, reachable path, population, credible regression consequence, magnitude, offsets, change risk, and stable-test feasibility must be evidenced under Step 2.6. Coverage absence affects confidence; it does not manufacture impact. Static code-quality findings are assessed directly from changed lines.

    **(b) Classify ledger entries** as:
    - **ADMITTED in-diff** — every applicable admission field is proved and the defect is inside the diff
    - **ADMITTED out-of-diff-breakage** — every applicable field is proved, and an unchanged callsite is broken by a contract this PR changed (cite the callsite and the contract from 2.5c)
    - **OMITTED pre-existing/not-attributed** — base has the same or worse behavior and this PR does not expose a new path
    - **OMITTED false** — counterevidence disproves the proposition
    - **OMITTED unverified** — any required producer, reachability, observation, or dependency is missing

**Enumerated candidates are admitted per item.** A candidate that lists N instances of one pattern ("these five classes miss override X") is N candidates sharing a mechanism, not one candidate with N bullets. Never sample N instances and publish the unverified remainder: every rendered item needs its own producer/trigger and evidence; otherwise omit that item.

Keep omitted candidates and their disproofs in the private ledger. Do not publish a Downgraded, retracted, rejected, or "possible issue" section, and do not report candidate counts. **OMITTED pre-existing/not-attributed** is the one exception: an entry whose producer, reachability, and observation are all proved leaves the ledger as a Step 4 adjacent issue draft. **OMITTED false** and **OMITTED unverified** entries never do.

Fresh falsifiers may run in parallel, but each receives only its neutral proposition and raw evidence contract. The parent independently checks every returned admission form before writing Step 4.

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
- **Coverage gaps are impact- and proportionality-assessed:** consume the Step 2.6 coverage disposition. Missing tests alone are not blocking. For every uncovered path, establish user impact, change risk, existing safeguards, and the least fragile meaningful test before choosing Critical gap, Moderate gap, accepted, or exempt. Do not accept unsupported "simple" or "hard to test" claims, and do not demand a brittle/invasive test whose demonstrated cost and fragility outweigh a small residual user risk. Add every discovered path to the private disposition; publish only admitted gaps.
- **Cross-context coverage:** for every entry in the 2.5d list, a test exercises the changed symbol from that context (`DummyQwpServer` for QWP ingest/egress, `DummyHttpServer`/`MockHttp` for HTTP, `QuestDbManager`/Docker for integration). Record each missing cross-context test as an UNTESTED Step 2.6 row; classify and publish it only through the proportionality and admission gates.
- **Error-path / null / boundary:** failure cases, exceptions, null columns, empty buffers, single-row, max-value inputs, zero-length strings.
- **Concurrency tests** where shared state is touched (see `SfPoolConcurrencyTests`, connect-walk concurrency guard).
- **Multi-TFM:** the change is exercised on every affected framework (the CI filter runs the unit suite per-TFM); WS/QWP tests are net7.0+.
- **Correct matcher:** server-terminal assertions use `Assert.CatchAsync<IngressError>` (not exact-type `ThrowsAsync`, since `LineSenderServerException` subclasses it).
- **Regression tests:** if the PR fixes a bug, a test should reproduce it and fail without the fix. A fix with no regression test is classified through Step 2.6 — usually a Moderate gap, never automatically Critical.
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

Present only **ADMITTED** findings. Omitted candidates, disproofs, retractions, agent counts, candidate counts, and the private ledger never appear in the public review. Do not publish a hypothesis and retract it later; finish falsification first. It is valid to report no findings. The single exception is the **Adjacent findings** section below, which carries proved pre-existing bugs as issue drafts — not findings against this PR, and weightless in every gate.

**Proportionality.** Keep the report actionable in one sitting, ordered worst user impact first. If a normal-sized PR yields more than about seven total findings, re-run the admission gate on every item and remove dependent, duplicate, not-attributed, and low-value prose. Removing a not-attributed item means moving it to Adjacent findings, not discarding it. Review depth is demonstrated by evidence, not report length.

**Every finding — at every severity — opens with three one-line summaries, before any prose:**

- **Problem:** what is wrong. ≤ 12 words. No mechanism or fix.
- **Net impact:** supported population and magnitude. ≤ 12 words. A behavioral item with no net regression is omitted.
- **Evidence:** the decisive code chain or artifact, including the reviewed revision identity.

Write these lines last from the completed admission form, never first from a hunch. Then give only the minimal producer → path → symptom trace, base comparison, and suggested fix.

```
Problem: Reaped idle sender frees ring holding un-acked frames.
Net impact: Silent data loss for pooled ws producers on slow servers.
Evidence: ReapIdle → Dispose chain at <head-sha>; no IsFullyDrained gate; base gated.

Problem: Regression test passes with the fix reverted.
Net impact: The fixed NACK-replay path can silently regress.
Evidence: assertion traced against reverted hunk at <head-sha>; observes only the ACK count.
```

### Critical
Blocking issues introduced or exposed by this PR, ordered worst user impact first. **A newly committed compiled binary or other build artifact (see the "Committed build artifacts" checklist) is always Critical — this library ships as a CI-built NuGet package, so a binary in the diff is never acceptable.** An admitted store-and-forward / NACK / pool-startup invariant violation (data loss, terminal-on-transient, reconnect budget on the steady-state loop, ack watermark advanced past a NACK) is Critical. Each must include:
- The three summary lines (**Problem** / **Net impact** / **Evidence**) before anything else
- The net determination from 3b.15(a): population, delta vs base, magnitude/frequency, offsets, and a net of **net-negative** — a Critical that is net-neutral or net-positive is mis-filed by definition
- Exact file path and line numbers (including out-of-diff files)
- The **symptom sentence** with its supported trigger: "user does X → sees Y". For a coverage-gap Critical: "user does X; if this changed path regressed as Y, the user would see Z". "Could theoretically lose data" is not evidence
- For a coverage-gap Critical: the credible mutation/recurrence, existing safeguards and offsets, the change-risk assessment, and the least fragile stable test considered, with concrete evidence that cheaper alternatives are inadequate
- Whether the finding is **in-diff** or **out-of-diff-breakage** (an unchanged callsite this PR breaks) — both are this PR's responsibility
- Code path trace showing why the bug is real and reachable
- **Base behavior for the identical trigger** (required, cited from `$BASE`): if base shows the same or worse user-visible outcome, omit the candidate as not attributed to this PR — a fully proved one moves to Adjacent findings. For a genuinely new surface, write `N/A — new surface` and prove that base cannot express the trigger. Base rejection (a thrown config/validation error) is the absence of a defect, not a worse defect outcome.
- For out-of-diff-breakage: the callsite that triggers it, plus the violated contract from 2.5c
- Suggested fix, written to be applied in THIS PR

Pre-existing/not-attributed observations are never Critical; a fully proved one belongs under Adjacent findings instead.

### Moderate
Non-blocking admitted issues worth fixing: a concrete changed-line standards violation, a proved weak test, missing internal-path coverage (a Moderate gap from Step 2.6), a documentation defect, or a bounded off-hot-path cost. Every item still includes the three summary lines and its decisive evidence. An unreachable runtime theory, an unchanged residual hardening opportunity, or a proposition that only supports another candidate is not Moderate; omit it.

### Minor
Concrete cosmetics on changed lines: member ordering, naming, formatting, comment wording. Non-blocking, optional.

### Adjacent findings (not blocking — file as GitHub issues)

Bugs that already exist on the merge base, found in code this review visited (changed files, callers from the callsite inventory, cross-context exposures), which this PR does not introduce, break, or worsen. They are **not findings against this PR**: they never appear under Critical/Moderate/Minor, never influence the verdict, and are never proposed as changes to this PR. Discarding them instead is pure waste — the investigation is already paid for, and nobody re-finds them later.

They are held to the same evidence bar as a published finding. An adjacent draft comes only from a candidate that reached **OMITTED pre-existing/not-attributed** with its producer, reachability, and observation proved. A candidate that ended **OMITTED false** or **OMITTED unverified** stays in the private ledger; this section is not a home for speculation that failed falsification.

Report each as a ready-to-file issue draft, so it can move to GitHub without re-investigation:

- **Problem:** ≤ 12 words — doubles as the issue title
- **Net impact:** ≤ 12 words — population and magnitude, or "None — <reason>"
- **Location:** file path + line numbers
- **Evidence:** the code path that proves it, with `$BASE` shown to exhibit the same behavior
- **Suggested fix:** one or two lines
- **Severity if filed standalone:** Critical / Moderate / Minor per the rubric above

Offer to file them; do not file anything without being asked. Their count and severity sit outside the proportionality budget and outside every gate in the Summary. If one is severe enough that shipping this PR without it is genuinely unsafe — because this PR moves code onto a path where the pre-existing bug now fires — then it is not adjacent: it is out-of-diff-breakage, it belongs under Critical, and you state that argument explicitly.

### Summary
- **Verdict**, exactly one of:
  - **approve** — no admitted Critical findings and the test gate passes.
  - **approve with comments** — both gates pass; you want specific Moderate items addressed but will not block on them. Name which ones.
  - **request changes** — at least one admitted Critical is open, or the test gate fails.
  - **needs discussion** — the change requires a product, API, or compatibility decision a reviewer cannot make alone.
- **Correctness gate (hard rule):** the verdict cannot be "approve" while any **ADMITTED** Critical finding remains open, including an admitted Critical coverage gap. Omitted hypotheses never affect the verdict. Before finalizing, rerun the admission audit from evidence fields rather than from report prose:
  - **falsification:** state the strongest attempted disproof for each rendered behavioral finding;
  - **producer:** confirm a supported call sequence / config / wire event actually creates every trigger state;
  - **independence:** confirm the admitting falsifier did not receive the discovery narrative, severity, fix, or votes;
  - **static completeness:** confirm every runtime-shape claim (race, ordering, reconnect timing, replay state) carries a complete static proof chain and its same-trigger base citation, or was omitted under the execution rule;
  - **dependency:** remove every item whose parent premise was omitted;
  - **severity:** classify only after admission. Never promote missing evidence or uncertainty to Critical.

  If any field fails, omit the candidate and rerun the verdict. If the admitted Critical list is empty and the test gate passes, approve plainly; zero findings is expected for correct changes.
- **Test gate (hard rule):** the gate fails only while an **ADMITTED Critical coverage gap** remains open. Zero test changes, a bug-fix label, or missing regression coverage triggers the Step 2.6 analysis but never automatically forces "request changes". Moderate gaps may accompany "approve with comments"; accepted gaps do not affect the verdict. Any independently admitted functional Critical still fails the correctness gate regardless of test effort or urgency.
- State the test-gate result and the admitted coverage-gap count. Do not publish total UNTESTED or omitted-candidate counts from the private ledger.
- Highlight any regressions or tradeoffs.
- Never make the verdict conditional on splitting the PR. Pre-existing and not-attributed observations never affect the verdict, whether they were omitted or delivered as adjacent issue drafts.
- Do **not** state agent counts, candidate counts, rejected/false-positive counts, or retraction history.
- State the admitted split: in-diff / out-of-diff-breakage. At levels 0-1 the callsite inventory (2.5b) is not built, so out-of-diff-breakage covers only callers the inline review actually opened — report a zero there as "callsite analysis not run at this level", never as a clean bill of health. At level 2+, if the diff is non-trivial and out-of-diff-breakage is zero, either the change is genuinely well-contained — say so — or the cross-context pass underran: re-check the 2.5d exposure list (and Agent 9's output at level 3) before finalizing.
- State the severity distribution. If the report is long or severity-heavy, re-run admission; do not compensate by preserving weak items at a lower severity.
