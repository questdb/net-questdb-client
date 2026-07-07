# Plan: Sender connection-pool API for the C# QuestDB client (Java parity)

Port the Java client's `QuestDB` sender-pool to C#, **including store-and-forward (SF)
slot management**. Egress/query pooling is explicitly **out of scope** for this effort
(see §8).

Java sources analysed (`/Users/alpel/src/questdb-enterprise3/questdb/java-questdb-client`):
`QuestDB.java`, `QuestDBBuilder.java`, `impl/SenderPool.java` (1168 LOC),
`impl/PooledSender.java` (419), `impl/PoolHousekeeper.java` (137).

---

## 1. Target public API (C#)

Mirrors Java but adapts to repo idiom: the repo already pairs a static factory `Sender`
with interface `ISender`. We do the same. **The handle type cannot be named `QuestDB`** —
that's the root namespace. Use `QuestDBClient` (static factory) + `IQuestDBClient`.

```csharp
namespace QuestDB;                       // reuse root ns, consistent with Sender/ISender

public interface IQuestDBClient : IDisposable, IAsyncDisposable
{
    ISender BorrowSender();                                       // blocks ≤ acquire_timeout, throws on timeout/closed
    ValueTask<ISender> BorrowSenderAsync(CancellationToken ct);   // C# value-add (see §6)
    ISender Sender();                                             // thread-pinned borrow (ThreadLocal)
    void ReleaseSender();                                         // unpin current thread
    void Close();                                                 // == Dispose(); idempotent
}

public static class QuestDBClient                  // factory, mirrors Java QuestDB.connect/builder
{
    public static IQuestDBClient Connect(string confStr);
    public static QuestDBClientBuilder Builder();
}

public sealed class QuestDBClientBuilder           // mirrors QuestDBBuilder
{
    public QuestDBClientBuilder FromConfig(string confStr);   // == IngestConfig for now
    public QuestDBClientBuilder IngestConfig(string confStr);
    public QuestDBClientBuilder SenderPoolMin(int n);
    public QuestDBClientBuilder SenderPoolMax(int n);
    public QuestDBClientBuilder SenderPoolSize(int n);         // sets min==max
    public QuestDBClientBuilder AcquireTimeout(TimeSpan t);
    public QuestDBClientBuilder IdleTimeout(TimeSpan t);
    public QuestDBClientBuilder MaxLifetime(TimeSpan t);
    public QuestDBClientBuilder HousekeeperInterval(TimeSpan t);
    public IQuestDBClient Build();
}
```

Usage (Java parity — `close()` returns to pool, does NOT disconnect):
```csharp
using var client = QuestDBClient.Connect("http::addr=localhost:9000;sender_pool_max=8;");
using (var s = client.BorrowSender())     // ISender; Dispose() flushes + returns to pool
{
    s.Table("trades").Column("px", 101.5).At(DateTime.UtcNow);
}                                          // <- back in pool, real socket stays open
```

**Behavioral contract (matches Java `PooledSender`):**
- The borrowed `ISender` is a `PooledSender` decorator. Its `Dispose()`/`DisposeAsync()`
  **flushes pending rows then returns the decorator to the pool** — it does *not* close the
  underlying sender. The real sender closes only at `IQuestDBClient.Dispose()`.
- Idempotent: a second `Dispose()` after return is a no-op.
- If the return-flush throws, the sender is **discarded** (real delegate disposed, slot freed),
  never returned.

---

## 2. Config keys (added to `SenderOptions`)

All-protocol keys (HTTP/TCP/WS), defaults from Java `QuestDBBuilder`:

| Key | Type | Default | Notes |
|---|---|---|---|
| `sender_pool_min` | int | 1 | warm minimum, ≥ 0 |
| `sender_pool_max` | int | 4 | hard cap, ≥ 1 and ≥ min |
| `acquire_timeout_ms` | ms→TimeSpan | 5000 | borrow blocking budget |
| `idle_timeout_ms` | ms→TimeSpan | 60000 | reap idle (never below min) |
| `max_lifetime_ms` | ms→TimeSpan | 1800000 | reap over-age (30 min) |
| `housekeeper_interval_ms` | ms→TimeSpan | 5000 | reaper sweep, ≥ 100 |

Wiring (verified seams in `Utils/SenderOptions.cs`):
- **Register** all six in `Qwp/QwpConnectStringKeys.cs` `Shared[]` (line ~42) — *not* in
  `WebSocketOnlyKeys`; they are protocol-agnostic. `KnownConnectStringKeys` picks them up
  automatically, so they pass `RejectUnknownConnectStringKeys` (SenderOptions.cs:341).
- **Parse** in the string ctor (after pool_timeout, ~line 230): `ParseIntWithDefault(...)` for
  the two ints, `ParseMillisecondsWithDefault(...)` for the four ms keys. Add matching
  `_*UserSet` flags (set from `ReadOptionFromBuilder(name) is not null`).
- **Fields/props**: backing field + get/set with `_*UserSet = true` in setter (repo idiom,
  SenderOptions.cs:901). Field initializers carry the defaults.
- **Validate** — new `ValidatePoolOptions()` called from `EnsureValid()` (SenderOptions.cs:553):
  `min >= 0`, `max >= 1`, `min <= max`, all timeouts `> 0`, `housekeeper >= 100ms`. Throw
  `IngressError(ErrorCode.ConfigError, ...)` like the existing validators.
- **ToString round-trip**: reflection-based `ToString()` (SenderOptions.cs:1890) auto-emits
  public props; TimeSpans already serialise as long-millis. Pool keys are not WS-only, so they
  are not skipped → round-trips through `new SenderOptions(s.ToString())`. Add a
  `SenderOptionsTests` round-trip case.

Note: pool keys live in the **same** connect string as the sender keys (Java does the same —
`ConfigView.getInt` reads pool keys off the ingest view; there's no separate strip step).
`SenderOptions` already ignores pool keys for sender behavior; the pool reads them off the
parsed `SenderOptions`.

---

## 3. New types (the pool itself)

Under `src/net-questdb-client/Pooling/` (new folder), Apache banner on each file:

| File | Role | Java analogue |
|---|---|---|
| `IQuestDBClient.cs` | public handle interface | `QuestDB` |
| `QuestDBClient.cs` | static `Connect`/`Builder` + internal `QuestDBClientImpl` | `QuestDB`/`QuestDBImpl` |
| `QuestDBClientBuilder.cs` | builder | `QuestDBBuilder` |
| `SenderPool.cs` | elastic pool core | `SenderPool` |
| `PooledSender.cs` | `ISender` decorator, return-on-Dispose | `PooledSender` |
| `PoolHousekeeper.cs` | background reaper task | `PoolHousekeeper` |

`SenderPool` builds underlying senders via the existing `Sender.New(SenderOptions)` factory
(per-slot options cloned and `sender_id` overridden — see §5).

---

## 4. Threading model (recommended C# adaptation)

Java uses `ReentrantLock` + `Condition` + create-outside-lock. The idiomatic C# equivalent
that **also yields async acquire for free** is a `SemaphoreSlim` capacity gate + a short
`lock` for the free-list/counters:

- `SemaphoreSlim _capacity = new(maxSize, maxSize)` — counts **in-use** senders. A permit is
  taken on borrow, released on return. Because we only *create* a sender when `_available` is
  empty *and* a permit is held, total alive senders never exceeds `max` (proof: when creating,
  every other alive sender is in-use holding one of the remaining permits).
- `BorrowSender`: `_capacity.Wait(acquireTimeout)` (sync) / `await _capacity.WaitAsync(...)`
  (async) → on success, under a short `lock(_gate)` pop an idle `PooledSender` or reserve a
  creation; **create the sender outside the lock** (TLS/DNS won't block other borrowers);
  add to `_all` under lock. On timeout → throw `IngressError` ("timed out waiting for a Sender
  from the pool after Nms").
- `giveBack`: under `lock(_gate)` push to `_available` (or discard if pool closed), then
  `_capacity.Release()`.
- `_available` = `Stack<PooledSender>` (LIFO keeps hot connections hot); `_all` = `List<>`.
- Counters/flags: `_closed` (`volatile bool`, checked before gate), in-flight creations folded
  into the permit accounting.

This diverges from the literal Java lock structure but is behaviorally equivalent and is the
clean way to support both `BorrowSender()` and `BorrowSenderAsync()`. (Alternative if strict
structural parity is wanted: `Monitor.Wait/Pulse` — but then async acquire needs a second
path. Recommend SemaphoreSlim.)

**Decorator (`PooledSender`)** delegates the full `ISender` surface (~60 members: all `Column`
overloads, `Symbol`, `Table`, `At*`, `Send*`, `Transaction`, `Length`, etc.) to `_delegate`.
Only `Dispose`/`DisposeAsync` differ (flush+return, idempotent via `Interlocked` flag). This
is mechanical (~250 LOC, matches Java's 419-line explicit decorator). Returns `ISender` only —
QWP-only methods (`Ping`, ack getters) are not surfaced through the pool wrapper in v1, exactly
as Java exposes only `Sender`.

---

## 5. Store-and-forward parity (the crux) — §4 chosen scope

Each pooled WS+SF sender must get a **distinct, stable** slot identity so their mmap segment
dirs + flocks never collide. Java: `sender_id = <base>-<index>`, index ∈ `[0, maxSize)`,
lowest-free allocation. C# verification:
- `<sf_dir>/<sender_id>/` isolation is **automatic** (`QwpWebSocketSender` builds the slot path
  via `Path.Combine(sf_dir, sender_id)`; `QwpSlotLock.Acquire` takes an exclusive `FileShare.None`
  lock). So `sender_id = <base>-<i>` cleanly isolates segments + locks. ✅ reuse as-is.
- Reusable as-is: `QwpSlotLock.Acquire/TryAcquire`, `QwpOrphanScanner.ClaimOrphans`,
  `QwpBackgroundDrainer` (already forces `InitialConnectMode.off`), `QwpBackgroundDrainerPool`,
  `IQwpWebSocketSender.GetHighestAckedSeqTxn/Ping/AwaitAckedFsnAsync` for return-flush. ✅

### Two production-code gaps (MUST build before SF pool works)
These are in the **client library**, not the pool, and Java's pool depends on both:

1. **Slot-lock-released probe — ABSENT in C#.**
   Java reclaims a slot index only after `QwpWebSocketSender.isSlotLockReleased()` confirms the
   OS flock actually dropped (the I/O thread may still hold it briefly after `close()`).
   - Add `bool IsSlotLockReleased { get; }` to `QwpWebSocketSender` (and `IQwpWebSocketSender`),
     backed by a released-flag on `QwpSlotLock` (set when the held `FileStream` is actually
     released in `Dispose`). Non-WS senders report `true` (no flock).
   - Without this, the pool cannot safely reuse a slot index after discard/reap → risk of
     "slot already in use" on recreate.

2. **Exclude-managed-slots from orphan draining — ABSENT in C#.**
   `QwpOrphanScanner.ClaimOrphans(sfRoot, ourSenderId)` only skips a *single* id. A pooled
   sender at `base-2` would otherwise see siblings `base-0/base-1/...` as "orphans" and fight
   the pool for them. Java solves this with `orphanDrainExcludeManagedSlots(base, maxSize)`.
   - Add an exclude-predicate/overload to `QwpOrphanScanner.ClaimOrphans` (skip any dir matching
     `<base>-<0..maxSize-1>`), plumbed via a new internal option (e.g.
     `orphan_exclude_managed_base`/`...count`) set by the pool on each slot's `SenderOptions`.

### Pool-side SF logic (port from `SenderPool.java`)
- `slotInUse[]` bitmap, `allocateSlotIndex()` (lowest-free), `freeSlotIndex(i)`.
- `closingSlots` / `leakedSlots` counters folded into capacity accounting (a slot whose flock
  hasn't released yet must not be re-created → hold its permit until `IsSlotLockReleased`, else
  retire the index permanently and log a warning, matching Java `reclaimSlot`).
- **Two-pass startup recovery** driven on the housekeeper thread (keeps `Connect()` non-blocking):
  - Pass 1 — in-range indices `[0, maxSize)`: probe each `<base>-<i>` slot dir; if it has
    stranded `sf-*.sfa` data, reserve it (counts against capacity), drain via a recovery sender
    built with `initial_connect_mode=off` + `drain_orphans=off`, bounded per-drain (~1s).
  - Pass 2 — out-of-range orphans `[maxSize, ...)` left by a previously larger pool: lazily
    enumerate, drain, retire.
  - Reuse `QwpOrphanScanner` + `QwpBackgroundDrainer` rather than re-implementing the drain.
- `senderFactory` test seam (`Func<int, ISender>`) like Java's `IntFunction<Sender>`.

---

## 6. Async semantics (C# value-add)

- `BorrowSenderAsync(ct)` over `_capacity.WaitAsync`. The decorator implements true
  `DisposeAsync` doing **async** return-flush (WS flush is naturally async). Sync `Dispose`
  blocks on it (matches existing `QwpWebSocketSender` dispose behavior).
- ⚠️ `Sender()`/`ReleaseSender()` thread-affinity uses `ThreadLocal<PooledSender>`. Document the
  Java-aligned caveat: **dedicated producer threads only**; do not hold a pinned sender across
  `await` (continuations can resume on a different pool thread). v1 ports it verbatim with this
  doc warning; an `AsyncLocal` variant can come later if needed.

---

## 7. Phasing & deliverables

**Phase 0 — Config plumbing.** Pool keys in `SenderOptions` + `QwpConnectStringKeys.Shared` +
`ValidatePoolOptions` + ToString round-trip. Tests: `SenderOptionsTests` (parse, defaults,
min>max rejected, round-trip). *No behavior yet.* Low risk.

**Phase 1 — Core elastic pool (HTTP/TCP/WS-RAM, no SF).** `IQuestDBClient`, `QuestDBClient`,
`QuestDBClientBuilder`, `SenderPool`, `PooledSender`. Borrow/giveBack, min/max, acquire-timeout,
create-outside-lock, pool close (disposes all delegates), discard-broken, introspection
(`AvailableSize`/`TotalSize`). Sync + async borrow. Tests: port `SenderPoolTest` (recycle-same-
decorator, broken-not-returned, close-idempotent, close-rejects-borrow, exhaustion-timeout,
builds-min, grows-to-max). **This delivers ~80% of the value** (HTTP/TCP/WS-RAM all poolable).

**Phase 2 — Housekeeper.** `PoolHousekeeper` as a background `Task` + `PeriodicTimer`
(net6.0+, available on all TFMs). Idle reap + max-lifetime reap, never below min; swallow
`Exception` (C# analogue of Java's `Throwable`) so a delegate-close fault can't kill the loop.
Tests: reap-shrinks-to-min, respects-min, survives-delegate-close-error.

**Phase 3 — Thread-affine.** `Sender()`/`ReleaseSender()` via `ThreadLocal`, pin/unpin,
clear-pin-on-return, invalidate-on-close. Tests: per-thread distinct, pin-after-close rejected,
release-after-close safe.

**Phase 4 — SF slot management (largest, riskiest).**
- Production: `IsSlotLockReleased` (+ `QwpSlotLock` released-flag); orphan exclude-managed-slots.
- Pool: per-slot `sender_id=<base>-<i>`, slot bitmap, closing/leaked accounting, flock-release
  reclaim/retire, two-pass startup recovery on housekeeper.
- Tests: port `SenderPoolSfTest` (slot collision, recovery, concurrency), `SenderPoolSfTest`'s
  retire-on-leak. Integration: a `QuestDbWebSocketIntegrationTests`-style pool case against a
  live master (slots survive recycle; recovery replays stranded segments).

**Phase 5 — Error-safety + polish.** Port `SenderPoolErrorSafetyTest`; close-during-borrow
release-with-error parity; XML docs; an `example-*` demo; CLAUDE.md update (replace the
"explicitly reject pooling" note with the new pool, keep the single-Sender-not-thread-safe
guidance for the un-pooled path).

---

## 8. Out of scope (flag to user)

- **Egress/query pool** (`query()`, `executeSql`, `newQuery`, Java `QueryClientPool`,
  `query_pool_*` keys). Java's `QuestDB` owns it; the user asked specifically about *sender*
  pooling. Can be a follow-up (`IQuestDBClient` could later grow `Query()`), and `connect`
  could later require ws/wss like Java. v1: ingest-only handle, all ingest protocols.

## 9. Open questions / risks

- **net6.0 gating.** WS sender is net7.0+. The pool core is TFM-agnostic, but SF/WS-specific
  paths (Phase 4) must sit behind `#if NET7_0_OR_GREATER` (HTTP/TCP pooling must still compile
  on net6.0). The two production gaps in §5 touch `QwpWebSocketSender` → already net7.0+.
- **SF capacity accounting vs. SemaphoreSlim.** A closing-but-not-yet-released slot must not be
  recreated; reconcile the permit model with `closingSlots`/`leakedSlots` (hold permit until
  `IsSlotLockReleased`, retire index on leak). Detail to nail in Phase 4 design.
- **`InternalsVisibleTo`.** Pool tests need internals; `net-questdb-client-tests` is already a
  friend assembly — keep new pool internals `internal`, expose only `IQuestDBClient` +
  `QuestDBClient` + `QuestDBClientBuilder` publicly.
- **Decorator drift.** When `ISender` gains a method, `PooledSender` must too. Add an analyzer
  test (reflection over `ISender` members ⊆ `PooledSender`) to catch drift.
