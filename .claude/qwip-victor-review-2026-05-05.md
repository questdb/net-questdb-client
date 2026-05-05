# Branch review: qwip_victor (2026-05-05)

Scope: ~20K LOC of QWP additions on top of `main`. Focus on hot-path
allocations, async correctness, and concurrency. Test files, comment
style, and architecture nits intentionally skipped.

Verification status: every finding below was confirmed by reading the
referenced lines. A list of plausible-but-rejected findings appears at
the end so the same false positives don't get re-raised.

---

## HIGH — worth fixing before ship

### Hot-path allocations

**`src/net-questdb-client/Qwp/QwpColumn.cs:510` — `BigInteger.ToByteArray` per `Long256` row**

```csharp
var magnitude = value.ToByteArray(isUnsigned: true, isBigEndian: false);
```

Allocates a fresh `byte[]` on every row. Long256 is a per-row column
type. Replace with the `TryWriteBytes(Span<byte>, ...)` overload writing
directly into `FixedData.AsSpan(FixedLen, 32)`, then zero any unwritten
high bytes — no allocation, one fewer copy.

**`src/net-questdb-client/Senders/QwpWebSocketSender.cs:500–508` — `new double[]` / `new long[]` + `Buffer.BlockCopy` per multi-dim `Array` column**

```csharp
var flat = new double[value.Length];
Buffer.BlockCopy(value, 0, flat, 0, value.Length * sizeof(double));
EnsureCurrentTable().AppendDoubleArray(name, flat, shape);
```

Per-row allocation in the public `Column(string, Array)` API. The
underlying `AppendDoubleArray` already takes `ReadOnlySpan<double>`. For
rank-1, cast directly. For multi-dim, use `MemoryMarshal.CreateSpan`
over the pinned array to avoid the temporary. The strongly-typed
`AppendArrayDispatch<T>` path (line 519) already does this correctly —
the weakly-typed `Array` path is the outlier.

### Concurrency

**`src/net-questdb-client/Qwp/Sf/QwpMmapSegment.cs:259` — `WritePosition` is a plain auto-property, not volatile**

```csharp
WritePosition += totalSize;       // line 259, plain write
AppendOffset(envelopeStart);      // line 260, Volatile.Write inside
...
if (offset < 0 || offset >= WritePosition) return -1;   // line 299, plain read
```

The producer (under `_stateLock`) writes bytes → bumps `WritePosition`
→ publishes via `Volatile.Write` to offset table. The reader (send
pump, on a different thread, *not* holding `_stateLock`) reads
`WritePosition` directly. On weak memory architectures (ARM) the reader
can observe the new `WritePosition` before the envelope bytes are
visible — CRC catches it but throws `InvalidDataException` → terminal
failure.

Fix: convert to `Volatile.Read` / `Volatile.Write` against a backing
field, or always read `WritePosition` after the offset table to chain
through the existing volatile fence.

### Per-flush allocations (10K flushes per 10M-row workload)

Profile pass 2 (see `qwip-victor-profile-2026-05-05.md`) flagged
allocations on the per-flush path. Each verified by re-reading the
referenced lines. Individually small, collectively ~556 B / flush of
pure overhead.

**`src/net-questdb-client/Senders/QwpWebSocketSender.cs:785` — fresh `CancellationTokenSource` per flush**
```csharp
using var linked = CancellationTokenSource.CreateLinkedTokenSource(_ioCts!.Token, ct);
```
~150 B per `EnqueueAsyncCore` call. Skip the link when `ct == default`
(the auto-flush path always passes `default`) — pass `_ioCts!.Token`
directly. The cancellation semantics are unchanged.

**`src/net-questdb-client/Qwp/QwpInFlightWindow.cs:144,179,196,324–329` — fresh TCS allocated per signal even with no awaiter**
```csharp
private TaskCompletionSource<bool> ReplaceChangeSignalLocked() {
    var prev = _changeSignal;
    _changeSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    return prev;
}
```
Every `Add` (per send), `AcknowledgeUpTo` (per ack), and `FailAll`
allocates a fresh TCS (~80 B) and discards the old one — even though
the only consumer is `AwaitEmptyAsync`, called from `Ping` and
`DisposeAsync`, **never on the steady-state ingestion path**.
160 B per flush of pure waste. Fix: only allocate a new TCS when there's
an awaiter (lazy field, replace on demand inside `AwaitEmptyAsync`),
or migrate to a single `IValueTaskSource<bool>` instance that supports
reset (no per-signal alloc). The lazy field is the smaller change.

### Thread-safety documentation (pass 37)

**LOW — `ISender` thread-safety documented only in README; absent from XML docs**

`README.md:32`:
> `Sender` is single-threaded, and uses a single connection to the
> database. If you want to send in parallel, you can use multiple
> senders and standard async tasking.

This is a critical usage constraint — concurrent access to a
single `ISender` corrupts internal state (column buffers, in-flight
window, encoder buffers).

But: `ISender.cs:30` (the interface XML doc) does **not** mention
threading. IntelliSense / docs.questdb.io rendering / consumer
docfx output won't surface the constraint. A user who reads the
API docs without the README sees no warning.

Concrete failure modes if violated:
- Two threads call `Column(...)` concurrently → race on
  `_currentTable`'s column buffers → corrupt or NRE.
- Two threads call `Send()` concurrently → multiple flushes
  enqueue overlapping AsyncBatch → in-flight window non-sequential
  add → `InvalidOperationException` at
  `QwpInFlightWindow.cs:139`.

The failure mode is "obscure exception eventually" rather than
"corrupted data accepted". So the user isn't silently wrong, but
the diagnostic is poor.

Fix:
1. **Add XML doc on `ISender`**:
   ```csharp
   /// <remarks>
   ///     <para>Not thread-safe.</para> A Sender owns transient state
   ///     (column buffers, in-flight window, encoder buffers).
   ///     Concurrent access from multiple threads will corrupt this
   ///     state and surface as obscure runtime exceptions.
   ///     <para />
   ///     For parallel ingestion: use one Sender per producer thread.
   /// </remarks>
   ```
2. **Optional runtime guard in DEBUG builds**: capture
   `Environment.CurrentManagedThreadId` in the constructor; check
   on each public method call; throw a clear "concurrent access
   detected" exception if mismatched. ~10 LOC, only active in DEBUG
   so production isn't slowed.

Severity: LOW — important constraint, weakly documented at the
API surface. Documentation completeness, not a bug.

### Record-equality on `SenderOptions` includes secrets (pass 36)

**LOW — `SenderOptions` is `public record`; auto-generated Equals/GetHashCode includes credential properties**

`SenderOptions.cs:45`: `public record SenderOptions`. C# records
auto-generate `Equals(SenderOptions)`, `GetHashCode()`, and a
deconstructor based on all public properties.

Public properties include `username`, `password`, `token`,
`tls_roots_password` (`:627, :642, :656, :782`). The auto-generated
implementations:
- `Equals` compares all fields including credentials — two
  SenderOptions with the same password compare equal (semantically
  correct).
- `GetHashCode` mixes credential values into the hash — using
  SenderOptions as a `Dictionary<SenderOptions, T>` key includes
  the password in the hash computation.

**Impact** is minor in practice — the credentials don't leak via
hash codes, just contribute to hash distribution. No security
issue. But consider:
- A consumer using `SenderOptions` as a dict key (e.g., a sender
  cache) silently keys by the password value. Changing the
  password creates a new entry.
- Debugger displays of records often include all members. A
  watch-window inspection reveals secrets in plaintext.

The pass-32 `ToString()` override is the bigger surface for the
same threat. The record's auto-generated `Equals`/`GetHashCode`/
`PrintMembers` are smaller-impact relatives.

Fix options:
- Convert from `public record` to `public class` — loses record
  ergonomics (with-expressions, value equality) but gains explicit
  control. The class still has `WithClientCert` which uses `with`
  — that'd need rewriting.
- Override `Equals`/`GetHashCode`/`PrintMembers` to exclude
  secret fields. Combine with the pass-32 `ToString` redaction
  for a uniform "secrets aren't in any auto-generated output"
  contract.

Recommend the second — write three `[ExcludeFromMembers]`-style
overrides that filter password/token/tls_roots_password.
Severity: LOW — defence-in-depth; pass-32 `ToString` leak is the
real-impact one to fix first.

### Defensive read-only collections (pass 35)

**LOW — `QwpTableBuffer.Columns` exposes the backing List via `IReadOnlyList<T>`; cast-to-mutable bypasses the read-only contract**

`QwpTableBuffer.cs:109`:
```csharp
public IReadOnlyList<QwpColumn> Columns => _columns;
```

`_columns` (`:56`) is a `List<QwpColumn>`. The property exposes
the live reference typed as `IReadOnlyList<T>`. But `IReadOnlyList<T>`
isn't enforcement — consumers can cast:

```csharp
var t = sender.GetCurrentTable();   // hypothetical accessor
((IList<QwpColumn>)t.Columns).Clear();   // mutates the buffer
((List<QwpColumn>)t.Columns).RemoveAt(0);
```

`QwpTableBuffer` is `internal sealed`, so external code can't reach
this. Internal callers in `QwpEncoder` only iterate, no cast. So
the leak is theoretical right now — but if the type ever became
public, a nominal "read-only" contract would silently allow
mutation.

Fix: `public IReadOnlyList<QwpColumn> Columns => _columns.AsReadOnly();`
returns a `ReadOnlyCollection<T>` wrapper. Cast attempts fail.

Caveat: `AsReadOnly()` allocates the wrapper once per call; cache
the result if `Columns` is read in a hot path. (Currently called
from encoder per flush — a single allocation per flush is fine.)

Severity: LOW — internal type, no current exposure. Defensive
hardening for if/when the type goes public.

### MMap pointer acquire-per-call (pass 34)

**LOW — `ScanForLastGoodEnvelope` calls `ViewToSpan` (per-call pointer acquire/release) in a loop**

`QwpMmapSegment.cs:577–591`:
```csharp
private static unsafe void ViewToSpan(MemoryMappedViewAccessor view, long offset, Span<byte> dest) {
    var handle = view.SafeMemoryMappedViewHandle;
    byte* ptr = null;
    handle.AcquirePointer(ref ptr);
    try {
        var src = new ReadOnlySpan<byte>(ptr + view.PointerOffset + offset, dest.Length);
        src.CopyTo(dest);
    }
    finally {
        handle.ReleasePointer();
    }
}
```

Called per envelope in the segment-open replay scan
(`ScanForLastGoodEnvelope` at `:429, :453`). For a segment with N
envelopes, that's `2N` `AcquirePointer`/`ReleasePointer` pairs.

`SafeMemoryMappedViewHandle.AcquirePointer` is a ref-counted op
(atomic increment + bounds check). Cheap, but adds up on long
segments at startup.

Fix: refactor `ScanForLastGoodEnvelope` to acquire the pointer
once at the start, pass the raw `byte*` (or a thin wrapper) to a
non-acquiring helper inside the loop, release once at the end.
~30 LOC.

Severity: LOW — cold path (segment open at sender startup or SF
recovery), not hot. Perf win is a few microseconds per segment
opened.

### Encoder hot-path inefficiency (pass 33)

**MED — `WriteColumnData` Varchar encodes offsets one-at-a-time instead of a single bulk copy**

`QwpEncoder.cs:302–310`:
```csharp
case QwpTypeCode.Varchar:
    // (n + 1) uint32 LE offsets, then concatenated UTF-8 bytes.
    for (var i = 0; i <= n; i++) {
        buf.WriteUInt32LittleEndian(col.StrOffsets![i]);
    }
    buf.WriteBytes(col.StrData!.AsSpan(0, col.StrLen));
    break;
```

Each `WriteUInt32LittleEndian` call:
1. Allocates 4 bytes from the `FrameBuilder._buf` (capacity check + position bump),
2. Writes 4 bytes via `BinaryPrimitives.WriteUInt32LittleEndian`.

For `n+1` offsets at 1000 rows/flush, that's 1001 individual writer
calls. The data is contiguous on the source side (`StrOffsets` is
a `uint[]`), so a single bulk copy via `MemoryMarshal.Cast` would
work:

```csharp
var srcBytes = MemoryMarshal.Cast<uint, byte>(col.StrOffsets!.AsSpan(0, n + 1));
buf.WriteBytes(srcBytes);
```

One memcpy instead of N writer calls. ~5–10× faster on varchar
encoding for typical batches.

Caveat: only correct on **little-endian hosts**. Big-endian needs
the per-element write (`BinaryPrimitives.WriteUInt32LittleEndian`)
to byte-swap. The encoder already has a `BitConverter.IsLittleEndian`
branch in `WriteTimestampColumnGorilla` at `:349`; the same pattern
applies here. On the BE branch, fall back to the current loop.

For the wide bench (2 varchar columns at 1000 rows/flush × 10K
flushes × 1001 writes/varchar = ~20M writer calls), this is a
real per-flush hot path. Severity: MED — varchar-column encoding
overhead, easily ~2–5% wall-time win.

### Secrets leak via `SenderOptions.ToString()` (pass 32)

**HIGH — `SenderOptions.ToString()` doc claims "minus secrets" but emits `password`, `token`, `tls_roots_password` verbatim**

`SenderOptions.cs:1338–1402`:
```csharp
/// <summary>
///     Serialises the SenderOptions object into a config string, minus secrets.
/// </summary>
public override string ToString() {
    var builder = new DbConnectionStringBuilder();
    foreach (var prop in GetType().GetProperties(...).OrderBy(x => x.Name)) {
        // WS-only keys, compiler-generated, JsonIgnore — all skipped
        ...
        if (value != null) {
            ...
            builder.Add(prop.Name, value);
        }
    }
    return $"{protocol.ToString()}::{connectionString};";
}
```

The enumeration iterates **all public instance properties**.
There is **no exclusion** for credential-bearing properties:
- `username` (`:627`)
- `password` (`:642`)
- `token` (`:656`)
- `tls_roots_password` (`:782`)

These get emitted into the returned string verbatim:
```
http::addr=...;password=quest;token=secret;tls_roots_password=secret;...;
```

**Concrete risk**: any user who trusts the docstring and logs
`sender.Options.ToString()` (or includes it in an error message,
metrics tag, exception payload, telemetry, debug dump) leaks
credentials in plaintext.

For a client library shipped via NuGet, the doc-vs-code mismatch
is particularly dangerous because:
- The doc explicitly *promises* secrets are filtered.
- A defensively-coded user reads the doc, decides logging is
  safe, ships to production.
- Credentials end up in log aggregators, error tracking systems,
  CI artifacts.

Fix:
```csharp
private static readonly HashSet<string> SecretProperties = new(StringComparer.Ordinal)
{
    nameof(password),
    nameof(token),
    nameof(tls_roots_password),
};

// Inside the foreach:
if (SecretProperties.Contains(prop.Name)) {
    if (value != null) builder.Add(prop.Name, "***");  // redaction token preserves "was set"
    continue;
}
```

Or skip them entirely (no redaction, just absence). Redaction
preserves "the option was configured" diagnostic info — useful
for debugging — without leaking the value.

Recommend redaction. **Verify the change with a unit test that
asserts `ToString()` on a fully-configured SenderOptions does not
contain the literal password/token strings.**

Severity: **HIGH** — doc explicitly promises a security property
the code does not implement. Caller-trust violation; production
log exposure of credentials.

### Obsolete throw-on-access stub (pass 31)

**LOW — `SenderOptions.bind_interface` is a public throw-on-access stub**

`SenderOptions.cs:558–563`:
```csharp
/// <summary>
///     Not in use.
/// </summary>
[Obsolete]
public string bind_interface =>
    throw new IngressError(ErrorCode.ConfigError, "Not supported!", new NotImplementedException());
```

A public property:
- Doc says *"Not in use"*
- Marked `[Obsolete]` (compiler warns on use)
- Throws `IngressError` on get (not even a no-op or default value)
- No setter

Why does it exist? Three possible reasons:
1. **Binary compatibility** with a previous version that supported
   `bind_interface` — removing would binary-break consumers that
   reference it. Keeping the stub maintains the assembly surface.
2. **Connect-string forward-compat** — but `bind_interface` isn't
   in `keySet`, so connect-string consumers passing it would hit
   `Invalid property` first; the property itself isn't reached
   from connect-string parsing.
3. **Stale code** — the property was deprecated mid-development
   and not deleted yet.

If reason 1: keep the stub but document the version when
`bind_interface` was deprecated and when it'll be removed
(typically next major).
If reason 2: irrelevant — the connect-string parser catches it
elsewhere.
If reason 3: delete the property; `[Obsolete]` is one major
release ahead of removal in conventional .NET semver.

Severity: LOW — confusing API surface; removing it is the next
step in the deprecation lifecycle. Coordinate with the
package-version bump (pass 12).

### Internal exception types (pass 30)

**LOW — Internal helpers throw raw `ArgumentException` / `InvalidOperationException` instead of `IngressError`**

`QwpBitWriter.cs`, `QwpVarint.cs`, `QwpSymbolDictionary.cs`,
`QwpMmapSegment.cs` collectively throw 14 raw exceptions
(`ArgumentException`, `ArgumentNullException`,
`ArgumentOutOfRangeException`, `InvalidOperationException`).
Examples:
- `QwpBitWriter.cs:83`: `throw new InvalidOperationException("bit writer exhausted");`
- `QwpVarint.cs:59`: `throw new ArgumentException("destination span too small for varint", nameof(dest));`
- `QwpSymbolDictionary.cs:143`: `throw new ArgumentOutOfRangeException(nameof(targetCount), "cannot roll back below the committed watermark");`

The user-facing exception convention is `IngressError`. Public
methods catch and re-throw `IngressError`, but if these internal
guards fire (they shouldn't under correct usage — they detect
library bugs), they bubble up as raw .NET exceptions that consumer
`catch (IngressError)` blocks miss.

The triggers are all "internal bug detected" rather than "user
input bad", so the impact is "library bug surfaces unexpectedly"
rather than "user gets confusing error". Still: a uniform exception
contract on the public API would catch even the internal-bug case.

Fix: either wrap these throws in `IngressError(InternalError, ...)`,
or document on the public API that "library-bug" errors may surface
as raw .NET exceptions and consumers should catch `Exception` for
robustness.

Severity: LOW — convention, indicates code-paths that shouldn't
trigger but might under unforeseen edge cases.

### Error code naming (pass 29)

**LOW — `ErrorCode.ProtocolVersionError` overloaded across 20 sites for "malformed frame" (not version mismatch)**

Histogram of `IngressError(ErrorCode.X, ...)` throws in QWP:

| ErrorCode | Throws |
|---|---|
| `InvalidApiCall` | 27 |
| `ProtocolVersionError` | **20** |
| `InvalidName` | 4 |
| `ConfigError` | 3 |
| `SocketError` | 2 |
| `InvalidUtf8` | 2 |
| `InvalidArrayShapeError` | 1 |
| `AuthError` | 1 |

The name `ProtocolVersionError` reads as "client and server speak
different protocol versions". But the 20 uses include:

- `QwpVarint.cs:94`: "varint truncated"
- `QwpVarint.cs:101`: "varint out of range"
- `QwpVarint.cs:115`: "varint exceeds 10 bytes"
- `QwpResponse.cs:116`: "QWP response frame is empty"
- `QwpResponse.cs:135`: "QWP response carries unknown status code"
- `QwpResponse.cs:154`: "QWP OK response has invalid size"
- ... (most QwpResponse parse-error sites)

These are **malformed-frame / parse errors**, not version mismatches.
A consumer who catches `IngressError(code: ProtocolVersionError)`
expecting to handle "version downgrade" gets false positives every
time the wire is corrupted.

Fix: add a new code (e.g., `ErrorCode.ProtocolError` or
`ErrorCode.MalformedFrame`) and migrate the parse-error sites.
Reserve `ProtocolVersionError` for the literal version-mismatch
case at handshake time (which probably surfaces from
`X-QWP-Version` header negotiation in `QwpWebSocketTransport`).

Severity: LOW — style + consumer-side error handling. Not a
correctness bug. Public API change (adds an enum value), so coordinate
with consumers that pattern-match on `ErrorCode`.

### Disposed-flag fencing inconsistency (pass 28)

**LOW — `QwpMmapSegment._disposed` is a plain `bool`, not `Volatile`-fenced**

`QwpMmapSegment.cs:87`: `private bool _disposed;`

Read at `:214` (TryAppend), `:294` (TryReadFrame), `:370`
(TruncateBack), `:386` (Dispose). All are plain reads. The
`_disposed = true;` write at `:391` is also plain.

Compare to `QwpCursorSendEngine.cs`:
- Write `:416`: `Volatile.Write(ref _disposed, true);`
- Read `:829`: `if (Volatile.Read(ref _disposed)) throw new ObjectDisposedException(...)`

So one SF type uses Volatile fencing on its `_disposed` flag, the
other doesn't. Inconsistent.

Concrete risk for `QwpMmapSegment`:
- Producer thread calls `TryAppend`; reads `_disposed=false`,
  proceeds.
- Disposer thread calls `Dispose`; sets `_disposed=true`, releases
  pointer, disposes view/mmap.
- Producer's `TryAppend` operates on now-disposed view → `ObjectDisposedException`
  or worse.

The Volatile fence wouldn't *prevent* the race — only narrow the
window. Real protection is the SF state lock or a CompareExchange
gate. But matching the engine's pattern (Volatile on the
`_disposed` flag) is at least defensive.

Fix: change reads/writes to `Volatile.Read/Write`. Or migrate to
`int _disposed` + `Interlocked.CompareExchange` for an atomic
"first-disposer-wins" gate (matches `QwpWebSocketSender.cs:91`'s
`int _disposed` pattern, which also uses `Volatile.Read` for
checks at `:1280`).

Severity: LOW — narrow race window; the exceptional path
(disposed-while-in-use) already escapes via deeper exceptions.
But the inconsistency across SF types is a maintenance smell.

### CI test coverage (pass 27)

**MED — CI tests on `net9.0` only; the other four target frameworks are built but never tested**

`ci/azure-pipelines.yml:88, :97`:
```yaml
arguments: '--configuration $(buildConfiguration) --framework net9.0 --no-build ...'
```

The package multi-targets `net6.0;net7.0;net8.0;net9.0;net10.0`
(per `net-questdb-client.csproj:23`). The CI build step at `:80`
compiles all five frameworks, but the test step only runs on
**net9.0**. So:

- net6.0, net7.0, net8.0, net10.0: built but not exercised. Bugs
  that only manifest on those runtimes ship undetected.
- net9.0 specifically uses the `Dictionary.AlternateLookup` fast
  path (per the `#if NET9_0_OR_GREATER` branches across the
  span-keyed dicts). That means **the pre-.NET 9 fallback path is
  never tested in CI** — exactly the path the dominant pass-1
  finding (5× allocation overhead) lives in. If `SpanKeyedDict`
  lands as the fix, the code change won't be exercised by CI either.
- net10.0's behavioural differences (DATAS GC absorbing transient
  allocations, etc.) aren't validated.

Fix: extend the test step into a matrix:
```yaml
- task: DotNetCoreCLI@2
  displayName: 'Run tests on $(osName) net$(framework)'
  inputs:
    ...
    arguments: '--framework net$(framework) ...'
  strategy:
    matrix:
      net6:    { framework: '6.0' }
      net7:    { framework: '7.0' }
      net8:    { framework: '8.0' }
      net9:    { framework: '9.0' }
      net10:   { framework: '10.0' }
```

(or equivalent — Azure Pipelines syntax varies). Wall-clock cost is
~5× the current test step; can be parallelised across agents.

If keeping all five frameworks is too expensive, **at minimum** add
net8.0 (last LTS) and net10.0 (latest) to the test matrix.
net9.0-only is the worst single TFM to test on for this codebase
because it's the only one that exercises the BCL `AlternateLookup`
fast path uniformly with both the pre-NET9 fallback (when
`SpanKeyedDict` lands) and the post-NET9 fast path.

Severity: MED — significant test coverage gap in the multi-target
release.

### Repo housekeeping (pass 26)

**LOW — Typo in CI pipeline filename: `azurre-binaries-pipeline.yml`**

`ci/azurre-binaries-pipeline.yml` — "azurre" with double r. The
sibling file in the same directory is correctly spelled
`azure-pipelines.yml`.

Risk: search/grep for "azure-binaries" or "azure binaries
pipeline" misses this file. Any documentation referencing the
filename is silently broken. Plus the typo makes the project look
unmaintained at a glance.

Fix: rename to `azure-binaries-pipeline.yml`. If the filename is
referenced by any pipeline orchestration (Azure Devops by ID
rather than path, or templates), update the reference too.

Severity: LOW — cosmetic, but visible to anyone navigating the
`ci/` directory.

**LOW — No `CHANGELOG.md` for a release that ships major new functionality**

The branch adds QWP — an entirely new transport with a new public
API surface (`IQwpWebSocketSender`, `QwpException`, etc.) and
proposed binary-breaking changes (Task→ValueTask, span params on
seqTxn methods). There is no `CHANGELOG.md`, `RELEASES.md`, or
release-notes file at the repo root.

Users upgrading from `3.2.0` to whatever this branch ships have
no document explaining what changed, what's new, what's breaking.
The git log shows commits like "code review" and "race condition"
which aren't a useful release narrative.

Recommended: add `CHANGELOG.md` with a `## 4.0.0 - 2026-MM-DD`
section listing:
- **Added**: ws/wss transport, IQwpWebSocketSender, SF mode,
  Gorilla compression, durable ACK watermarks, etc.
- **Changed (breaking)**: Async methods on ISender now return
  ValueTask (if that change lands).
- **Fixed**: any of the bug-fix items from this review that ship
  in the same release.

Severity: LOW — standard OSS hygiene; not a code defect, but
absence is visible.

**LOW — Missing `CONTRIBUTING.md` referenced by README**

`README.md:387–390` has a "Contribute" section with PR-process
notes inline, but no link to a `CONTRIBUTING.md`. GitHub
auto-displays such a file on the PR creation page. Standard
expectation for an open-source project; absent.

Defer if a separate file isn't intended; otherwise add one with
the existing inline content + dev-setup instructions (build,
test, run benchmarks).

Severity: LOW — convention.

### README example accuracy (pass 25)

**MED — HTTPS examples use port 9009 (TCP ILP port), not 9000 (HTTP/HTTPS port)**

`README.md:113`:
```csharp
using var sender = Sender.New("https::addr=localhost:9009;tls_verify=unsafe_off;username=admin;password=quest;");
```

`:119`:
```csharp
using var sender = Sender.New("https::addr=localhost:9009;tls_verify=unsafe_off;username=admin;token=<bearer token>");
```

Port `9009` is QuestDB's **TCP ILP** port. The HTTP/HTTPS REST + ILP
endpoint listens on port `9000`. A user copying these examples
verbatim and pointing them at a default-config QuestDB gets a
connection failure (TCP ILP doesn't speak HTTPS).

The TCP example below at `:125` uses `tcps::addr=localhost:9009`
correctly — port 9009 IS the TCPS port. The HTTPS examples were
likely transcribed from the TCPS example without changing the port.

Fix: change both HTTPS examples to `https::addr=localhost:9000;...`.

Severity: MED — copy-paste-and-fail. First-time HTTPS user follows
the example, gets opaque error, debugs for an hour.

**LOW — Heading "Flush every 5000 rows" doesn't match the example's `auto_flush_rows=1000`**

`README.md:90–94`:
```
#### Flush every 5000 rows

using var sender = Sender.New("http::addr=localhost:9000;auto_flush=on;auto_flush_rows=1000;auto_flush_interval=off;");
```

Heading says 5000, code shows 1000. Either change the heading to
"Flush every 1000 rows" (matches code) or change the example to
`auto_flush_rows=5000` (matches heading). Severity: LOW —
inconsistency, not user-blocking.

### README structural accuracy (pass 24)

**MED — Contribute section links to the wrong repo**

`README.md:389–390`:
```
- Prior to opening a pull request, please create an issue
  to [discuss the scope of your proposal](https://github.com/questdb/c-questdb-client/issues).
```

This is the **net**-questdb-client repo. The link points to the
**C** client's issue tracker (`c-questdb-client`). A user wanting
to contribute to the .NET client opens an issue in the wrong repo;
maintainers either ignore or close-and-redirect.

Fix: change to `https://github.com/questdb/net-questdb-client/issues`.

Severity: MED — silently breaks the contribution flow. First-time
contributors get bounced.

**LOW — Examples section missing the WS examples**

`README.md:360–363`:
```
## Examples
* [Basic](src/example-basic/Program.cs)
* [Auth + TLS](src/example-auth-tls/Program.cs)
```

The codebase ships `src/example-websocket/Program.cs` and
`src/example-websocket-auth-tls/Program.cs` (per the QWP work in
this branch), but the README's Examples list doesn't reference
them. Users browsing examples don't see the WS examples unless
they explore the source tree.

Fix: append two rows:
```
* [WebSocket / QWP](src/example-websocket/Program.cs)
* [WebSocket auth + TLS](src/example-websocket-auth-tls/Program.cs)
```

Severity: LOW — discoverability gap.

### README WS-table accuracy (pass 23)

**MED — `reconnect_max_backoff_millis` README default contradicts code default**

`README.md:311`: `| reconnect_max_backoff_millis | 30000 | Cap on per-attempt backoff. |`

`SenderOptions.cs:225`: `ParseMillisecondsWithDefault(nameof(reconnect_max_backoff_millis), "5000", out _reconnectMaxBackoff);`

Code default is **5000 ms (5 seconds)**, README says 30000.
Six-fold discrepancy. Operational impact: a user reading the
README expects backoff to grow up to 30s; observed behaviour
caps at 5s, so reconnects retry six times more frequently than
expected.

Fix: README cell `5000`. (Or change code default if 30s is the
intended value — 5s seems aggressive for max backoff on a long
outage; verify with the SF requirements.)

**LOW — `max_symbols_per_connection` missing from WS-only parameters table**

`README.md:295–316`. The WS-only table lists `in_flight_window`,
`close_timeout`, `max_schemas_per_connection`, etc., but not
`max_symbols_per_connection` (default 1_000_000). The option
exists in code (`SenderOptions.cs:201`) and is in `keySet` and
`WebSocketOnlyKeys`. User reading just the README has no way to
discover this knob — relevant for high-cardinality workloads
where the default cap is hit (terminal failure, per pass-8
finding).

Fix: add the row.

**LOW — `ping_timeout` missing from WS-only parameters table**

Same gap. Code: `SenderOptions.cs:230`, default 5000 ms. Not in
README. User trying to tune `Ping` latency has no documented
knob.

Fix: add the row.

**LOW — `AtNow` / `AtNowAsync` listed in properties table without obsolete marker**

`README.md:353–354`:
```
| AtNow(...)        | void      | Finishes line, leaving the QuestDB server to set the timestamp |
| AtNowAsync(...)   | ValueTask | Finishes line, leaving the QuestDB server to set the timestamp |
```

Both methods are marked `[Obsolete("Not compatible with deduplication. Please use `AtAsync(DateTime.UtcNow)` instead.")]`
in `ISender.cs:224, :248`. Compiler warns at use; user reading
README sees a regular method.

Fix: append "(deprecated — see `AtAsync(DateTime.UtcNow)`)" to the
description, or remove the rows entirely from the table since
they're discouraged.

### README config-table accuracy (pass 22)

**MED — `request_timeout` README default contradicts code default**

`README.md:289`: `| request_timeout | 10000 | Base timeout for HTTP requests... |`

`SenderOptions.cs:188`: `ParseMillisecondsWithDefault(nameof(request_timeout), "30000", out _requestTimeout);`
`SenderOptions.cs:86`: `private TimeSpan _requestTimeout = TimeSpan.FromMilliseconds(30000);`

Code default is **30000 ms (30 seconds)**, README says 10000.
A user reading the README budgets 10s of timeout, runs into a
30s-timeout sender, and is confused about why their request didn't
fail-fast as expected.

Fix: README cell `30000`. (Or change the code default — but 30s
is more practical for HTTP requests under load, so prefer fixing
the doc.)

Severity: MED — documented behaviour doesn't match.

**MED — `tls_ca` documented in README config table but rejected by parser as "Invalid property"**

`README.md:285`: `| tls_ca | | Un-used. |`

The README explicitly lists `tls_ca` as a config option (with the
description "Un-used"). But `SenderOptions.cs:52–66` `keySet` does
**not** include `tls_ca`. A user setting `tls_ca=path/to/ca.pem`
in their connect string gets:

```
ConfigError: Invalid property: `tls_ca`
```

at `Sender.New(...)`. The "Un-used" wording in the README implies
"silently ignored", but the parser actually rejects it.

Two fixes:
1. **Remove from README** — if `tls_ca` was never wired up, remove
   the row from the docs entirely.
2. **Add to `keySet`** — accept it silently (matching the documented
   "Un-used" wording) for cross-client interop. Same pattern as
   `token_x` / `token_y` which are accepted but ignored.

Recommend (2) — matches the existing pattern for cross-client
config strings (Java/Go clients may pass `tls_ca`). The connect
string shouldn't error on an option this client chose not to
support.

Severity: MED — documented config knob throws on use; user must
read source to discover.

### README documentation accuracy (pass 21)

**MED — README documents `IWebProxy` override that doesn't exist**

`README.md:244`:
> The transport disables system HTTP proxies by default; long-lived
> WebSocket connections rarely survive HTTP proxies. **Pass an
> explicit `IWebProxy` to override if you have a WebSocket-aware
> proxy.**

`QwpWebSocketTransport.cs:84` hardcodes `ws.Proxy = null;`. There is
**no API to override this**:
- `SenderOptions` has no `proxy`-related property.
- `QwpWebSocketTransportOptions` (the internal config record) has no
  `Proxy` field.
- The connect string has no documented `proxy=` key.

So the README promises a capability the code doesn't expose. A user
trying to follow the documentation would search for `proxy` in
`SenderOptions`, find nothing, and have no path forward.

Two fixes:
1. **Implement the override.** Add a `proxy` knob to `SenderOptions`
   (or expose `IWebProxy` programmatically via the options record),
   wire it through `QwpWebSocketTransportOptions.Proxy`, set
   `ws.Proxy` only when not null. Match what the README already
   promises.
2. **Fix the README.** If proxy support isn't planned, change the
   line to: *"The transport disables system HTTP proxies; if your
   network requires a WebSocket-aware proxy, route at the OS layer
   instead."*

Recommend (1) — the use case (corporate networks with WebSocket-
aware proxies) is real, and the override is ~10 LOC.

Severity: MED — documented capability gap. Discoverable by users
running into proxy issues; the wrong kind of "your library is
broken" complaint.

### Config string parsing (pass 20)

**LOW — `protocol=` inside the config body is silently overridden by the `::`-prefix protocol**

`SenderOptions.cs:1286–1293`:
```csharp
_connectionStringBuilder = new DbConnectionStringBuilder {
    ConnectionString = paramString,
};
VerifyCorrectKeysInConfigString();
_connectionStringBuilder.Add("protocol", splits[0]);
```

If the user writes `"http::addr=foo;protocol=tcp;"`, the body
contains `protocol=tcp`. The builder absorbs it. Then line 1293
calls `Add("protocol", "http")` which **overwrites** silently
(per `DbConnectionStringBuilder` semantics). The user's `protocol=tcp`
is lost without a warning; the actual transport is the prefix's
`http`.

Fix: in `VerifyCorrectKeysInConfigString`, check if `protocol`
appeared in the body and throw — `protocol` should only be set
via the `::`-prefix. Or: accept body `protocol` and verify it
matches the prefix.

Severity: LOW — silent override of a config field; user's intent
is ignored. Edge case but contributes to "the config didn't do
what I asked" surprise.

**LOW — `confStr.Split("::")` doesn't validate exactly-one `::`**

`SenderOptions.cs:1255`:
```csharp
var splits = confStr.Split("::");
var paramString = splits[1];
```

For `"http::addr=foo::bar"` (two `::`), `splits` has 3 elements;
the parser uses `splits[1]` as the params and silently drops
`splits[2]`. Two `::` is malformed but not rejected.

Fix: throw `ConfigError("multiple `::` separators")` if
`splits.Length != 2`. Severity: LOW — defensive.

**LOW — Unknown-key error message uses normalized (lowercased) key**

`DbConnectionStringBuilder` lowercases keys on insert. The
"Invalid property" error message at `:1411` reports the lowercased
form, so a typo like `"FooBar=1"` shows as `"Invalid property:
\`foobar\`"`. The user typed `FooBar` and gets an error about
`foobar` — slightly disorienting.

Fix: capture the original key from the parsed splits *before* the
builder normalizes, and use that in the error. Severity: LOW —
error message clarity.

### Address & port parsing (pass 19)

**LOW — Port `0` accepted for client config**

`SenderOptions.cs:1108, 1134`:
```csharp
if (!int.TryParse(portStr, out port) || port < 0 || port > 65535)
```

Port 0 is OS-meaningful (means "kernel-assigned random port" on
bind), but for a **client** connecting to a server, it's useless —
servers don't listen on 0. The validation accepts it, so a typo
like `addr=localhost:0` yields a "connection refused" at runtime
rather than a clear config error.

Fix: change to `port <= 0 || port > 65535`. Severity: LOW —
typo-protection.

**LOW — Empty hostname accepted**

`SenderOptions.cs:1121`:
```csharp
if (firstColon < 0) {
    host = addr;
    port = -1;
    return;
}
```

If `addr` is empty (after trim) or contains only a port (`":9000"`),
the parser stores `host = ""`. The error surfaces later at DNS
resolution as a confusing message; cleaner to reject upfront with
`ConfigError("address must include a hostname")`.

Edge case `addr=":9000"`:
- `firstColon = 0`
- `host = addr.Substring(0, 0)` → empty
- `port = 9000`
- DNS resolution of empty string fails opaquely.

Fix: add `if (string.IsNullOrWhiteSpace(host)) throw ConfigError(...)`
after each `host = ...` assignment in `ParseHostPort`. Severity:
LOW — clearer error message at config time.

### SF drainer pool & cleanup helpers (pass 18)

**LOW — `TryDropFailedSentinel` writes full stack trace to sentinel file**

`QwpBackgroundDrainerPool.cs:257`:
```csharp
File.WriteAllText(Path.Combine(slotDirectory, FailedSentinel), ex.ToString());
```

`Exception.ToString()` includes the full type name, message, AND
the full stack trace (with all frames, recursing through inner
exceptions). For a deeply-nested QwpException with multiple
wrappings, the sentinel file can be many KB.

Sentinel files persist until the slot is manually cleaned. In a
high-failure environment, the slot directory accumulates large
sentinel files. They're not consumed by anything except the
`File.Exists(...)` check in `QwpOrphanScanner` — the content is
purely diagnostic.

Fix: write a tighter representation:
```csharp
var diagnostic = $"{ex.GetType().FullName}: {ex.Message}\n{DateTime.UtcNow:o}";
File.WriteAllText(Path.Combine(slotDirectory, FailedSentinel), diagnostic);
```

Or write `ex.ToString()` but with a size cap (truncate after, say,
4 KB). Severity: LOW — disk-space hygiene, not a bug.

**INFO — `IsReplayImpossible` (pool) vs `IsTerminalServerError` (engine) handle QwpException differently**

`QwpBackgroundDrainerPool.cs:237` (pool, narrower):
```csharp
private static bool IsReplayImpossible(Exception ex) {
    if (ex is QwpException q) {
        return q.Status switch {
            QwpStatusCode.SchemaMismatch => true,
            QwpStatusCode.SecurityError => true,
            QwpStatusCode.ParseError => true,
            _ => false,
        };
    }
    return false;
}
```

`QwpCursorSendEngine.cs:847` (engine, wider — any QwpException is terminal):
```csharp
return ex is QwpException
    || (ex is IngressError ie && ie.code is ErrorCode.AuthError or ErrorCode.ProtocolVersionError);
```

Asymmetric: the live engine treats *any* QwpException as terminal
(connection unrecoverable), the orphan-drainer pool considers only
specific statuses as "drop sentinel and stop retrying". For
`InternalError` and `WriteError` (server transient errors), the
pool will retry on the next sweep — but the engine that just
encountered those would have already gone terminal and disposed
itself.

The asymmetry is **intentional** — orphan slots have stale data
that the original engine couldn't deliver; "transient server
error" *might* succeed on a future drainer attempt against a
recovered server. So narrower is correct for the pool.

No fix needed. Recording as **INFO** so future audits don't try to
"unify" them. The doc comment on either method should call out the
divergence and the rationale.

**LOW — `SfCleanup.DeleteFile` redundantly checks `Exists`**

`SfCleanup.cs:53`. Same `File.Exists + File.Delete` pattern flagged
in passes 15 and 16. Cluster fix.

### Signal-fire allocations + drainer hardcoding (pass 17)

**MED — `QwpCursorSendEngine` signal-fire allocates 3× per signal**

`QwpCursorSendEngine.cs:802–814`:
```csharp
private void FireAppendSignalLocked() {
    var prev = _appendSignal;
    _appendSignal = NewSignal();           // alloc 1: new TCS<bool> ~80 B
    _ = Task.Run(() => prev.TrySetResult(true));  // alloc 2: Task ~100 B + alloc 3: closure ~40 B
}

private void FireAckSignalLocked() {
    var prev = _ackSignal;
    _ackSignal = NewSignal();
    _ = Task.Run(() => prev.TrySetResult(true));
}
```

Same shape as the previously-documented `QwpInFlightWindow.ReplaceChangeSignalLocked`,
plus the **`Task.Run` closure allocates ~40 B for `prev`** because
the lambda captures a local variable. So per signal: ~220 B (TCS +
Task + closure).

The Task.Run dispatch is intentional (CLAUDE.md documents an
intermittent deadlock fix on Linux + .NET 9). The TCS replacement
itself is the same fix-shape as the InFlightWindow case: pool the
TCS or migrate to `IValueTaskSource<bool>`.

In SF mode at 10K appends/sec: ~2.2 MB/sec of allocation purely
for signal plumbing. Adds GC pressure on the producer thread.

Fix:
- Avoid the closure: cache `prev` in an instance field, change
  the lambda to `() => _stashedPrev.TrySetResult(true)` — but this
  introduces a race (multiple stashes overwrite). Better: use an
  `Action<TaskCompletionSource<bool>>`-typed static lambda + state
  parameter via `Task.Factory.StartNew(state => ((TCS)state).TrySetResult(true), prev)`.
- Pool the TCS: keeps a single `TaskCompletionSource<bool>` per
  signal, resets via Reset method. TCS doesn't support Reset →
  use `IValueTaskSource<bool>` instead.

Bundle with the existing InFlightWindow TCS-replacement fix —
both sites use the same primitive.
Severity: MED — additive on top of the existing HIGH-severity
finding in InFlightWindow.

**LOW — `QwpBackgroundDrainer` hardcodes `appendDeadline=30s`, ignoring config**

`QwpBackgroundDrainer.cs:92`: `appendDeadline: TimeSpan.FromSeconds(30)`.

The constructor of `QwpCursorSendEngine` requires a positive
`appendDeadline` even though the drainer's code path never calls
`AppendBlocking` (the drainer only flushes existing segments,
never accepts new appends). So the hardcoded value is **unused**
in practice — but it diverges from the user's
`sf_append_deadline_millis` config and is cosmetically suspect.

Fix: pass the user's configured `sf_append_deadline_millis` from
`QwpWebSocketSender.BuildSfStack`, even though the drainer won't
exercise it. Or mark the engine's appendDeadline parameter
nullable and accept null on the drainer path.

Severity: LOW — cosmetic / future-proofing, no current bug since
the value isn't read on the drainer path.

### SF slot lock & file cleanup (pass 16)

Audit of `Qwp/Sf/QwpSlotLock.cs`. Several minor issues clustered.

**LOW — Unused `using System.Diagnostics`**

`QwpSlotLock.cs:25`. The directive is dead — the file uses
`Environment.ProcessId` (in `System` namespace), not anything from
`System.Diagnostics`. Likely leftover from a prior implementation
using `Process.GetCurrentProcess().Id`.

Fix: delete the using. Severity: LOW — code-cleanliness only.

**LOW — Stale PID sidecar file misleads contention diagnostics**

`QwpSlotLock.cs:106–115` writes the holding process's PID to
`<slot>/.lock.pid`. `ReadHolderHint` (`:117–129`) reads it back
when reporting a contention error: *"slot X is already locked by
pid 12345"*.

But the PID file isn't validated. If the holding process **crashed**:
- The OS releases the FileShare.None lock automatically (so the
  next acquirer succeeds).
- The PID sidecar from the dead process **persists** (Dispose
  didn't run).
- The next acquirer overwrites the sidecar in `WritePidSidecar`.

Net: in the success case, the stale file gets overwritten cleanly.
But in a *brief* contention window between the new acquirer's
`TryOpenExclusive` succeeding and `WritePidSidecar` running, a
*third* sender hitting the lock sees the dead pid as the supposed
holder. Confusing.

Mitigation: in `ReadHolderHint`, attempt `Process.GetProcessById(pid)`
— if it throws (no such process), append " (stale)" to the hint
or omit the pid entirely. ~10 LOC. Severity: LOW — diagnostic
only, no functional bug.

**LOW — Dispose-then-Delete races on PID sidecar**

`:142–154`:
```csharp
_file.Dispose();   // releases the lock
...
if (File.Exists(_pidSidecarPath)) File.Delete(_pidSidecarPath);
```

After `_file.Dispose()`, the FileShare.None lock is released. A
parallel `Acquire` call can now succeed. If our `File.Delete` runs
**after** the new acquirer's `WritePidSidecar`, we delete the new
acquirer's sidecar.

Subsequent error messages from any *third* sender hitting the lock
would say "locked but no pid hint" instead of pointing at the
correct holder.

Fix: read the sidecar's content first, only delete if it matches
our own `Environment.ProcessId`. Or: delete the sidecar **before**
disposing the lock file (so the new acquirer can write a fresh
one without us racing). The latter is simpler. Severity: LOW —
diagnostic-info loss, not corruption.

**LOW — `File.Exists + File.Delete` in Dispose redundantly**

`:150` — same pattern as `QwpFiles.Delete` from the previous pass.
`File.Delete` is a no-op for missing files; the Exists check adds
a syscall and a TOCTOU race. Collapse to `File.Delete(_pidSidecarPath)`
inside the existing try/catch.

### SF file primitives (pass 15)

Audit of `Qwp/Sf/QwpFiles.cs`. Three findings.

**MED — `IsSharingViolation` heuristic catches non-sharing IOException as "locked"**

`QwpFiles.cs:78–94`:
```csharp
private static bool IsSharingViolation(IOException ex) {
    if (ex is FileNotFoundException || ex is DirectoryNotFoundException || ex is PathTooLongException)
        return false;
    const int sharingViolationHResult = unchecked((int)0x80070020);
    if (ex.HResult == sharingViolationHResult) return true;
    // POSIX surfaces FileShare.None without a recognisable HResult; the type check above
    // already excludes specific subclasses, so plain IOException is the residual signal.
    return ex.GetType() == typeof(IOException);
}
```

The "plain IOException = sharing violation" residual on POSIX
catches:
- Disk-full errors (`ENOSPC`)
- Some permission errors (`EACCES` paths that surface as
  IOException rather than UnauthorizedAccessException)
- Filesystem read-only mount errors (`EROFS`)
- Quota-exceeded (`EDQUOT`)
- I/O errors (`EIO`)

`TryOpenExclusive` then returns `null` for all of these, signalling
"someone else holds the lock". The SF slot-acquisition logic
proceeds as if the slot is just contended (try another sender_id,
fall through to drainer adoption), masking the actual problem.

User experience: full disk presents as "no slot acquired" rather
than `IOException("disk full")`. Diagnosis painful.

Fix: distinguish `EAGAIN`/`EWOULDBLOCK`/`EACCES`-on-FileShare.None
from other errno values via P/Invoke `errno` inspection on POSIX
(or accept that POSIX `FileShare.None` returns plain IOException
and check against a known-message-shape heuristic). Pragmatic
alternative: log the swallowed exception via the (yet-to-be-added)
ILogger so unexpected IOExceptions surface in user logs even when
the lock-acquisition flow proceeds.

Severity: MED — silent diagnostic loss on real disk/permission
errors during SF slot-lock acquisition.

**LOW — `LooksLikeNetworkPath` is dead code**

`QwpFiles.cs:209`. The function checks for UNC paths on Windows
(returns true for `\\server\share\...`) and returns false on POSIX.
**Never called anywhere in the codebase.** The doc comment claims
"this method exists so callers can emit a warning when they spot
an obvious mistake (e.g. an NFS mount)", but no caller exists.

Either:
- Wire it up: in `QwpSlotLock.Acquire` (or wherever `sf_dir` is
  validated), call `LooksLikeNetworkPath(sf_dir)` and emit a
  warning (logger or doc-string) if true.
- Or delete the function — dead code is misleading future
  maintainers.

Severity: LOW — dead code; potential feature lurking unused.

**LOW — `Delete` redundantly checks `Exists` (TOCTOU)**

`QwpFiles.cs:195–201`:
```csharp
public static void Delete(string path) {
    if (File.Exists(path)) {
        File.Delete(path);
    }
}
```

`File.Delete` is **already a no-op** if the file doesn't exist
(per .NET docs — only throws `DirectoryNotFoundException` for
missing parent dir). The `Exists` check is redundant **and**
introduces a TOCTOU race: between the check and Delete, another
process could create the file (no-op result) or delete it (no
change). Neither outcome is harmful, but the check adds a
syscall and a race for nothing.

Fix: collapse to `File.Delete(path);` directly. If callers want
to swallow `DirectoryNotFoundException`, wrap in try/catch.

Severity: LOW — code smell, no functional bug.

### Time handling (pass 14)

**MED — `QwpInFlightWindow.AwaitEmptyAsync` uses `DateTime.UtcNow` for deadlines (non-monotonic)**

`QwpInFlightWindow.cs:263`:
```csharp
public async Task AwaitEmptyAsync(TimeSpan timeout, CancellationToken ct = default) {
    var hasDeadline = timeout >= TimeSpan.Zero;
    var deadline = hasDeadline ? DateTime.UtcNow + timeout : DateTime.MaxValue;
    ...
    var remaining = deadline - DateTime.UtcNow;
```

Compare against the sync path 50 lines earlier (`:214`) which gets
it right:
```csharp
var sw = hasDeadline ? Stopwatch.StartNew() : null;
...
var remaining = totalMs - (int)sw!.ElapsedMilliseconds;
```

`DateTime.UtcNow` is **not monotonic** — NTP sync, manual clock
adjustment, leap-second smearing, container clock skew all cause
forward or backward jumps. Under skew:
- **Backward jump** during `AwaitEmptyAsync`: `deadline - UtcNow`
  stays positive longer than expected → unbounded wait (until the
  clock catches up).
- **Forward jump**: deadline reached early → spurious
  `TimeoutException`.

Same class, two paths, divergent semantics. The async path is the
more commonly used one (called from `Ping`, dispose drain).

Fix: change `AwaitEmptyAsync` to use `Stopwatch` like the sync path:
```csharp
var sw = hasDeadline ? Stopwatch.StartNew() : null;
...
var remaining = sw is null ? Timeout.InfiniteTimeSpan : timeout - sw.Elapsed;
```

Severity: MED — real correctness issue under clock skew, but skew
is uncommon in practice; produces the wrong type of error
(TimeoutException vs. completion) when it does occur.

**LOW — Auto-flush interval check uses `DateTime.UtcNow`**

`QwpWebSocketSender.cs:1357` (and `AbstractSender.cs:325,353` for ILP):
```csharp
var timeTrigger = Options.auto_flush_interval > TimeSpan.Zero
                  && DateTime.UtcNow - LastFlush >= Options.auto_flush_interval;
```

Same monotonicity issue — clock skew either suppresses time-based
flushes (backward jump) or fires them early (forward jump). The
practical impact is mild because `auto_flush_rows` and
`auto_flush_bytes` triggers are unaffected.

Fix: store `LastFlushTicks` as `Environment.TickCount64`
(monotonic, ~16 ms resolution which is plenty for auto-flush) for
the elapsed comparison. Keep the public `LastFlush` property
(`DateTime`-typed) computed from a separate `DateTime.UtcNow`
sample so consumer-visible timestamps stay wall-clock-aligned.
~10 LOC each in QwpWebSocketSender and AbstractSender.

Severity: LOW — cross-transport bug (not QWP-specific), only
manifests under clock skew, time-based flush is one of three
triggers.

### Observability gap (pass 13)

**MED — No logging, tracing, or metrics hooks across the entire client**

QWP and ILP both ship without any diagnostics integration. Survey:

- No `ILogger` / `Microsoft.Extensions.Logging` integration anywhere
  in `src/net-questdb-client/`. Connection state changes, terminal
  failures, SF reconnect attempts, ACK mismatches — none of these
  are observable except via thrown exceptions.
- No `System.Diagnostics.ActivitySource` for distributed tracing.
  Modern .NET libraries (`HttpClient`, AWS SDK, Microsoft.Data.SqlClient,
  etc.) expose spans via `ActivitySource`; OpenTelemetry collectors
  pick them up automatically. The QWP sender exposes nothing.
- No `EventSource` for event counters. Throughput, error rate,
  in-flight queue depth, reconnect attempts — none are
  programmatically observable.
- The only `System.Diagnostics` usage in the QWP code is
  `Stopwatch` (for backoff timing in `QwpInFlightWindow`) and
  `StackTrace` (for assertions in `QwpSlotLock`). No telemetry.

The user-visible state is exposed via three public properties:
`LastFlush`, `RowCount`, `Length`. For a production-grade client
ingesting at 100K+ rows/sec, this is the bare minimum.

The gap **isn't QWP-specific** — HTTP/TCP have the same shape.
That makes it a long-standing project posture rather than a QWP
regression. But the new QWP transport doubles the surface area
that production deployments will want to monitor (in-flight depth,
ACK latency, terminal-error reasons, SF segment provisioning, SF
drainer state) — the gap is more visible now.

Recommended approach for a follow-up PR (not blocking qwip_victor):

1. **`ActivitySource`** named `"QuestDB.Client"` with spans for
   `Sender.Connect`, `Sender.Flush`, `Sender.Close`, plus tags for
   transport type, row count, frame size, ACK latency.
2. **`EventSource`** named `"QuestDB-Client"` exposing counters:
   `rows-sent`, `frames-sent`, `bytes-sent`, `acks-received`,
   `flush-failures`, `terminal-errors`, plus QWP-specific counters
   for `in-flight-count`, `reconnect-attempts`, `sf-segments-active`.
3. **Optional `ILogger` injection** via constructor / factory option
   for state-transition events (connect, terminal failure, SF
   reconnect, etc.). Default no-op logger when not provided.

Severity: MED — feature gap, not a bug. Observability is
industry-standard for production data clients; users currently have
to instrument the wrapping code rather than the client itself.

### Package & build metadata (pass 12)

Audit of `net-questdb-client.csproj` and surrounding build config.
Several stale or wrong fields.

**HIGH — Package version not bumped for the QWP release**

`net-questdb-client.csproj:21`: `<PackageVersion>3.2.0</PackageVersion>`.

This branch adds QWP — a brand-new transport, ~20K LOC, new public
API surface (`IQwpWebSocketSender`, `QwpException`, etc.).
Semantic versioning calls for **at least** a minor bump (3.3.0)
for the additive feature set, and a **major bump (4.0.0)** if any
of the binary-breaking changes from the API consistency followups
land (Task→ValueTask, span params on IQwpWebSocketSender).

Risk: a user on 3.2.0 and a user on the QWP-shipping 3.2.0 see
the same version number with different behaviour. NuGet
reproducibility broken.

Fix: decide on the breaking-change bundle (Task→ValueTask, etc.)
before bumping. If they land in the same release: bump to 4.0.0.
Otherwise: 3.3.0 for the additive QWP transport.
Severity: HIGH — release-management correctness.

**MED — Package metadata stale**

`net-questdb-client.csproj:14`: `<Description>Simple QuestDB ILP protocol client</Description>`.

Inaccurate after this branch — the package now ships QWP (a binary
columnar protocol, not ILP). Suggested rewrite: *"QuestDB client.
Supports ILP (HTTP/HTTPS, TCP/TCPS) and QWP (WebSocket binary
columnar protocol, .NET 7+) for time-series data ingestion."*

`:19`: `<PackageTags>QuestDB, ILP</PackageTags>`.
Add: `QWP`, `WebSocket`, `time-series`, `ingest`.

`:17`: `<PackageLicenseUrl>Apache 2.0</PackageLicenseUrl>`.
Two issues: (1) `PackageLicenseUrl` is **deprecated** by NuGet
(replaced by `PackageLicenseExpression` or `PackageLicenseFile`),
and (2) the value `"Apache 2.0"` is not a URL; the field expects
`https://...` format. NuGet may surface a warning at pack time.

Fix: replace with `<PackageLicenseExpression>Apache-2.0</PackageLicenseExpression>`
(SPDX identifier).

Severity: MED — visible package-page metadata; affects how the
package is discovered and trusted on NuGet.org.

**LOW — Targeting EOL frameworks (`net6.0`, `net7.0`)**

`:23`: `<TargetFrameworks>net6.0;net7.0;net8.0;net9.0;net10.0</TargetFrameworks>`.

Per the .NET support lifecycle, both `net6.0` and `net7.0` are
out of support and produce `NETSDK1138` warnings on build (visible
in our build output earlier). Continuing to target them costs:
- The `#if NET9_0_OR_GREATER` complexity for span-keyed dicts
  (whose `#else` branch is the dominant pre-NET9 perf hit
  documented in the profile findings).
- Maintenance burden when BCL features ship that older runtimes
  lack.

Decision: explicit choice between (a) dropping `net6.0`/`net7.0`
and simplifying the codebase, or (b) keeping them and accepting
the perf gap + complexity. Either way, document the choice in
the README / CONTRIBUTING.

Severity: LOW — strategic decision, not a current bug. The
multi-target setup works.

**LOW — `IsAotCompatible=true` may be over-claimed**

`:3`: `<IsAotCompatible>true</IsAotCompatible>`.

The package declares full AOT (NativeAOT) compatibility. Sites
that may not be AOT-safe:

- `BuildCertificateValidator` uses `X509Certificate2.CreateFromPemFile`,
  which loads cert types via reflection paths. Recent versions of
  `System.Security.Cryptography.X509Certificates` are AOT-friendly
  but the entire chain (PEM parsing, cert chain build) hasn't been
  formally verified.
- `QwpWebSocketTransport.BuildDefaultClientId` uses
  `Assembly.GetName().Version?.ToString()` — this is fine for AOT
  (metadata-only).
- `SenderOptions.ToString()` uses `GetType().GetProperties(...)`
  reflection — may trigger `RequiresUnreferencedCodeAttribute`
  warnings in AOT analyzer, depending on whether the relevant
  attributes are wired.

Fix: run `dotnet publish -c Release -r linux-x64 --self-contained -p:PublishAot=true`
in CI on a smoke project that exercises HTTP, TCP, and WS transports.
If any of the three above produce trim warnings, either fix them
or remove the `IsAotCompatible=true` claim. Severity: LOW — claim
verification, not necessarily a current bug. False positive on the
declaration is more dangerous than not declaring it.

### Minor pass (pass 11)

Smaller findings collected from API surface and remaining unaudited
files. Bundling them since each is too small for its own subsection.

**LOW — `QwpSymbolDictionary.Add` accepts empty span as a symbol value**

`QwpSymbolDictionary.cs:85`. No guard against `value.IsEmpty`.
Empty symbol gets `_values.Add("")`, `_ids[""] = id`, encoded
on the wire as a length-prefixed zero-byte sequence. Server may
or may not accept; client doesn't preempt either way. ILP-side
behaviour for `sender.Symbol("region", "")` likely differs (text
ILP would write `,region=` with no value, server probably rejects
as a parse error).

Fix: in `QwpWebSocketSender.Symbol`, throw
`IngressError(InvalidApiCall, "symbol value must not be empty")`
when `value.IsEmpty`. Aligns with intent (symbols are always
non-empty discriminators) and surfaces the error at the user's
call site rather than as a server-side parse error per flush.
Severity: LOW — defensive validation.

**LOW — `QwpSymbolDictionary.Reset` doc misleading**

XML doc at `:155`: *"Clears all state. Called on connection reset."*
Accurate for non-SF mode, but in SF mode `OnFlushSucceeded` calls
`_symbolDictionary.Reset()` after **every flush** (so each
self-sufficient frame re-emits the full dict). Future maintainers
reading the doc would assume Reset is a once-per-connection event.

Fix: extend the XML doc: *"Clears all state. Called on connection
reset, OR per-flush in SF mode where every frame is
self-sufficient."* Severity: LOW — doc accuracy.

**LOW — `QwpSymbolDictionary.GetSymbol(id)` no bounds check**

`:115`: `return _values[id];`. An out-of-range `id` throws
`ArgumentOutOfRangeException` from `List<T>` indexer instead of
the project's `IngressError` convention. Consumer error-handling
catches `IngressError` (per the public surface); a raw
`ArgumentOutOfRangeException` slips past.

`GetSymbol` is internal and only called by the encoder over a
range it just populated, so a bad id indicates an internal bug
rather than user error — but converting to `IngressError(InternalError, ...)`
is consistent with the rest of the codebase's error convention.

Severity: LOW — defensive hardening, not a current bug.

**LOW — `Sender.New(SenderOptions? options = null)` silently builds default HTTP sender on null**

`Sender.cs:67–72`:
```csharp
public static ISender New(SenderOptions? options = null) {
    if (options is null) {
        return new HttpSender("http::addr=localhost:9000;");
    }
    options.EnsureValid();
    ...
}
```

Three concerns:
1. Optional parameter with `null` default + null-check returning a
   default is unusual. Most factory APIs throw `ArgumentNullException`
   on null (or omit the optional default entirely).
2. The null path bypasses `EnsureValid()` — only the non-null path
   validates. If `HttpSender(string)` doesn't validate equivalently,
   the two paths produce inconsistently-validated senders.
3. The hardcoded config string `"http::addr=localhost:9000;"`
   duplicates what `new SenderOptions()` would default to —
   future drift risk if defaults change in only one place.

Fix: remove the null-as-default behaviour. Either require a
non-null `options`, or change the null path to `return New(new SenderOptions(""))`
which goes through the validated dispatch.

Severity: LOW — current behaviour works, but the API shape and
validation symmetry are weak.

### Documentation drift (pass 10)

**HIGH — Multiple benchmarks ship broken cells with `in_flight_window=1`**

`QwpWebSocketSender.cs:105` rejects `in_flight_window < 2` with
`ConfigError`. Three benchmark classes still configure `1`:

| Bench | Site | How |
|---|---|---|
| `BenchLatencyWs` | `:94` | hardcoded `in_flight_window=1` in `[GlobalSetup]` |
| `BenchInsertsWs` | `:63` | `[Params(1, 8, 32, 128, 512)]` — sweep includes `1` |
| `BenchSfThroughput` | `:49` | `[Params(1, 8, 32, 128)]` — sweep includes `1` |

For `BenchLatencyWs`, **every cell** throws at `[GlobalSetup]` →
the entire bench class is non-functional. For `BenchInsertsWs` and
`BenchSfThroughput`, the sweep cells where `InFlightWindow=1` throw;
the other cells run.

This is also worth checking against:
- `QwpWebSocketSenderTests.cs:271` — test asserts the rejection
  works: `Assert.Throws<IngressError>(() => Sender.New("...;in_flight_window=1;..."))`.
  So the validation is *intentional* and the benches are stale,
  not vice versa.

Implication for `docs/qwp-benchmarks.md`: the latency-section
numbers (notably "Round-trip latency, sync mode (`in_flight_window=1`)")
cannot have been captured from `BenchLatencyWs` as shipped. They
were either run before the validation landed, from a manually-
patched config, or fabricated.

Fix:
- `BenchLatencyWs.cs:94` — change `in_flight_window=1` → `in_flight_window=2`.
- `BenchInsertsWs.cs:63` — drop `1` from `[Params]`, leave
  `[Params(2, 8, 32, 128, 512)]` (or `[Params(8, 32, 128, 512)]`
  if `2` isn't an interesting data point).
- `BenchSfThroughput.cs:49` — same.
- Re-run all three against a real server; refresh
  `docs/qwp-benchmarks.md` numbers.

Severity: HIGH — broken benchmarks in shipped test infrastructure;
the documentation that references them is unreproducible.

**LOW — Example and benchmark doc reference `in_flight_window=1` as supported**

`README.md:166` and `:299` are correct: *"valid range is 2..N.
The WebSocket transport is async-only — `in_flight_window=1` is
rejected."* `QwpWebSocketSender.cs:105` enforces this with a
`ConfigError` throw at construction.

But two other doc artefacts contradict it:

- `src/example-websocket/Program.cs:12`:
  ```
  // in_flight_window     pipelined batches in flight (default 128; set to 1 for sync send-and-wait)
  ```
- `docs/qwp-benchmarks.md:46`:
  ```
  ## 2. `BenchLatencyWs` — Round-trip latency, sync mode (`in_flight_window=1`)
  ```

A user copying the example's hint and constructing a sender with
`in_flight_window=1` gets `ConfigError("WebSocket transport requires
in_flight_window > 1, got 1")` at construction — confusing because
the example seems to recommend it.

Fix:
- Example: rewrite the comment as *"pipelined batches in flight
  (default 128; minimum 2 — WebSocket is async-only)"*.
- Benchmark doc: rewrite the section heading to drop the parenthetical
  `(in_flight_window=1)`. Verify `BenchLatencyWs` doesn't actually
  attempt `in_flight_window=1` (it would error at sender construction
  in `[GlobalSetup]`).

Severity: LOW — runtime error is clear, but copy-paste from the
example is the obvious next step for new users and they'll hit the
throw.

### SenderOptions validation gaps (pass 9)

Audit of bounds-checking on numeric and time-valued options. Many
have no upper or lower bound — a misconfigured value parses
silently and fails far from the call site.

**MED — No lower bound on timeout options**

`SenderOptions.cs:187,188,189,190,199,222,224,225,227,230` —
ten timeout options parsed via `ParseMillisecondsWithDefault`:
`auth_timeout`, `request_timeout`, `retry_timeout`, `pool_timeout`,
`close_timeout`, `sf_append_deadline_millis`,
`reconnect_initial_backoff_millis`, `reconnect_max_backoff_millis`,
`close_flush_timeout_millis`, `ping_timeout`. None of them validate
that the parsed value is positive.

`auth_timeout=0` → `CancellationTokenSource(TimeSpan.Zero)` fires
immediately → every connect throws "auth_timeout exceeded" with no
opportunity to actually authenticate.
`ping_timeout=0` → every Ping returns instantly via the timed-out
path.
`reconnect_initial_backoff_millis=-1` → arithmetic mayhem in the
backoff schedule.

Fix: in `ParseMillisecondsWithDefault` (or in `EnsureValid`), reject
negative values with `IngressError(ConfigError, ...)`. Choose
per-option whether zero is allowed (most: no; `auto_flush_interval=0`
is legitimate as "no time-based trigger" but that's already handled
via the special `off` keyword). Severity: MED — silent
mis-configuration → confusing runtime failures.

**MED — No relational validation between `reconnect_initial_backoff` and `reconnect_max_backoff`**

`SenderOptions.cs:224–225`. If a user sets
`reconnect_initial_backoff_millis=10000` and
`reconnect_max_backoff_millis=5000`, the initial exceeds the cap.
`QwpReconnectPolicy.ComputeBackoff` clamps to `_maxBackoff` on
every retry, so the policy degenerates to "always wait
`max_backoff`" — the doubling never has effect. Probably not the
user's intent.

Fix: in `EnsureValid`, throw `ConfigError` if
`_reconnectInitialBackoff > _reconnectMaxBackoff`. Same pattern for
any other initial/max pairings (none currently exist; defensive
guard for future). Severity: MED — silently breaks the backoff
strategy.

**LOW — No bound on `max_buf_size` vs `init_buf_size`**

`SenderOptions.cs:168–169`. Defaults are 64 KiB and 100 MiB
respectively, so the default config is fine. But user can pass
`init_buf_size=200000000;max_buf_size=100000000` — initial
allocation exceeds the cap. Buffer init either succeeds (allocating
200 MB) and immediately rejects all writes as "buffer over cap", or
fails on first growth. Either way, surprising.

Fix: `EnsureValid` throw if `_initBufSize > _maxBufSize`. One-line
check. Severity: LOW — edge case, but defensive validation is
near-free.

**LOW — `in_flight_window` has lower bound but no upper bound**

`QwpWebSocketSender.cs:105–109` rejects `in_flight_window < 2`.
But `in_flight_window=int.MaxValue` allocates a `SemaphoreSlim` with
~2 billion slots — silently large memory commitment, plus a
correspondingly-sized bounded `Channel<AsyncBatch>` (each entry is
~32 B, so 64 GB of channel buffer slots reserved upfront).

Fix: cap at a sensible upper bound (e.g. 65535 — matches `MaxTablesPerMessage`
header field's varint encoding range). Throw `ConfigError` for
values above the cap. Severity: LOW — a misconfigured value but
the user opted into the large allocation; cap protects against
typo'd values.

**LOW — HTTP-only options (`pool_timeout`, `request_min_throughput`) parsed for ws/wss**

`SenderOptions.cs:186,190`. `pool_timeout` and
`request_min_throughput` are HTTP-specific (HTTP client pool
timeout, HTTP minimum-throughput-for-request-timeout calculation).
They're parsed unconditionally even for ws/wss/tcp configs.

The values are silently ignored on non-HTTP transports, but the
config-string round-trip (`ToString()` → `new SenderOptions(...)`
serialisation) preserves them, which is misleading. A user who
sets `pool_timeout=60000` on ws thinks it has effect.

Fix: add `pool_timeout` and `request_min_throughput` to
`HttpOnlyKeys` (mirror of `WebSocketOnlyKeys`); reject explicit use
on non-HTTP transports. Or: silently skip in `ToString()` when
non-HTTP. Recommend the former — explicit rejection is the
established pattern. Severity: LOW — silent ignore of misplaced
config.

### Idiom & contract audit (pass 8)

Style/contract review focused on locking patterns, thread-safety
documentation, and failure-mode communication.

**MED — `QwpInFlightWindow` property getters take a full lock for single-field reads**

`QwpInFlightWindow.cs:63–120` — five property getters
(`AckedSequence`, `HighestSentSequence`, `IsEmpty`, `InFlightCount`,
`HasFailure`) all take `_lock` to read a single field:

```csharp
public long AckedSequence {
    get { lock (_lock) { return _ackedSequence; } }
}
```

For the **single-field** getters (`AckedSequence`,
`HighestSentSequence`, `HasFailure`), the lock is overkill —
`Volatile.Read` of a `long` is atomic on 64-bit hosts, and the lock
release on the writer side already provides the publish fence.
~20-50 ns per call → noticeable when callers poll these from a
tight loop (e.g. observability code reading `AckedSequence` on every
flush).

For the **composite** getters (`IsEmpty`, `InFlightCount`), the
lock IS required to get a consistent snapshot — without it, a
reader can see new `_ackedSequence` and stale `_highestSentSequence`
(or vice versa) and compute a negative `InFlightCount` or a
spurious `IsEmpty=true` during a Add↔Acknowledge interleave.

Fix:
- `AckedSequence`, `HighestSentSequence` → `Volatile.Read(ref _x)`
  (the writer's lock provides the fence, no need for the reader
  to hold it).
- `HasFailure` → `Volatile.Read(ref _failure) is not null` (writer
  publishes via lock; reader picks up the fence).
- `IsEmpty`, `InFlightCount` → keep the lock, OR document the
  "stale-snapshot is acceptable" semantic and use unlocked reads
  with explicit `Math.Max(0, diff)` clamp. Recommend keeping the
  lock here — they're called less frequently than the single-field
  getters.

Severity: MED — easy perf win on the hot polling path; getters
called from observability, dispose, and `AwaitEmpty` poll loops.

**LOW — `QwpSchemaCache` thread-safety not documented**

`QwpSchemaCache.cs:55` is `internal sealed class` with mutable
fields (`_nextSchemaId`, `_maxSentSchemaId`) and no synchronization.
Safe today because it's only accessed from the producer thread
during encode (which is single-threaded — guarded by
`_encoderReady` semaphores), but a future maintainer adding a new
caller from a different thread (e.g. a metrics poller wanting to
read `AllocatedCount`) would silently break it.

Fix: add a class-level XML doc note: *"Not thread-safe — caller
must serialize access. In QwpWebSocketSender this is enforced by
the encoder ping-pong semaphore."* And consider making `Reset`
explicit-private if it's only called from controlled paths.

Severity: LOW — documentation hygiene; no current bug.

**LOW — `max_symbols_per_connection` exhaustion poisons the sender**

`QwpSymbolDictionary.cs:101–104`:
```csharp
if (_values.Count >= _maxSymbols)
    throw new IngressError(ErrorCode.ConfigError,
        $"symbol dictionary cardinality {_maxSymbols} exceeded; raise `max_symbols_per_connection`");
```

Default cap is 1,000,000. For typical workloads, never hit. For
high-cardinality (e.g. one symbol per user-id, multi-million users)
the sender hits this mid-flow → terminal error (the throw bubbles
through Symbol → CancelCurrentRow → eventually FailTerminal). User
must dispose+recreate to recover, losing whatever was buffered.

Per QwpSchemaCache pattern, this *could* be handled by force-flush
+ symbol dict reset (matches SF self-sufficient frame mode), but
that requires server-side cooperation (server must accept dict
reset on the same connection). Defer that as a server-coordinated
feature.

In the meantime: prominently document in `SenderOptions.max_symbols_per_connection`
XML doc that exceeding the cap is a terminal failure, and recommend
sizing the cap conservatively for the workload's expected
cardinality. Severity: LOW — known design constraint, just under-
documented.

**LOW — Out-of-QWP-scope: `TcpSender` is `internal class`, not sealed**

`Senders/TcpSender.cs:41`. `HttpSender` is sealed (presumably);
`QwpWebSocketSender` is sealed. `TcpSender` is `internal class` —
allows derivation, no obvious purpose. One-keyword fix to seal.
Out of qwip_victor scope but worth a one-line tidy-up PR.

### Connection lifecycle audit (pass 7)

Targeted scan of handshake/open, close, abort, reconnect, dispose
paths. Several real findings, several agent claims rejected.

**MED — Server-initiated close maps to `ErrorCode.SocketError`**

`QwpWebSocketTransport.cs:252–256`:
```csharp
if (result.MessageType == WebSocketMessageType.Close) {
    throw new IngressError(
        ErrorCode.SocketError,
        $"server closed the WebSocket: {_client.CloseStatus} {_client.CloseStatusDescription}");
}
```

A graceful server-initiated close (e.g. server restart, planned
shutdown) is conflated with low-level socket failures (TLS, DNS,
connection refused) under `SocketError`. Operators reading logs
can't distinguish "server told us to disconnect" from "network
fault". Both terminate ingestion identically (sender goes terminal),
but the diagnostic intent is different.

Fix: add `ErrorCode.RemoteClose` (or `ConnectionClosed`) to the enum,
use it for the server-close path. Severity: MED — operator
debuggability of production incidents. The error code is a
public-API decision so coordinate with consumers' `catch` patterns.

**MED — Linked CancellationTokenSource may outlive `_ioCts` on dispose race**

`QwpWebSocketSender.cs:785`:
```csharp
using var linked = CancellationTokenSource.CreateLinkedTokenSource(_ioCts!.Token, ct);
```

The linked CTS captures `_ioCts.Token` and the caller's `ct`. On
`Dispose`, `_ioCts` is cancelled and eventually disposed
(`FinalizeWsTeardown`). If `EnqueueAsyncCore` is concurrently mid-
`await` and `_ioCts.Dispose()` runs before the linked CTS exits its
`using` scope, the linked CTS's internal `Dispose` tries to unhook
from a disposed source — `ObjectDisposedException` from
`CancellationTokenSource.Dispose()`'s internal cleanup.

Window is narrow (race between `Dispose`'s `_ioCts.Dispose()` and
the producer's `using var` exit), and the impact is "Dispose throws
ObjectDisposedException" rather than corruption. But it pollutes
the dispose path with unexpected exceptions.

Fix: have `Dispose` *cancel* `_ioCts` but **not dispose it** until
all producers/consumers have observed the cancellation and exited.
The pattern is "cancel + join + dispose", not "cancel + dispose +
join". Or: catch `ObjectDisposedException` in `FinalizeWsTeardown`'s
cleanup path. Severity: MED — narrow race, observable as noisy
dispose exceptions.

**MED — SF reconnect cursor not bounds-checked against ring**

`QwpCursorSendEngine.cs:560` (post-reconnect): `_cursorFsn = _ackedFsn`
rewinds the send pump to the first un-acked frame. If the segment
ring was trimmed concurrently (segment manager hit a size cap and
recycled segments), `_ackedFsn` may reference a segment that no
longer exists. The next `TryReadFrame(_cursorFsn, ...)` throws
"offset out of range".

Fix: after reconnect, clamp `_cursorFsn` to `max(_ackedFsn,
ring.OldestFsn)`. If clamping moves the cursor past `_ackedFsn`,
the engine has lost frames that were never acked — terminal failure
(unrecoverable for SF semantics). Verify the ring's trim policy
already prevents trimming past `_ackedFsn`; if so, this is a
defensive guard against a logic bug rather than an active race.
Severity: MED — only triggers if ring trims past acked watermark,
which shouldn't happen under correct trim policy.

**LOW — Partially-constructed sender leaks two `SemaphoreSlim` instances**

`QwpWebSocketSender.cs:96–137`:
```csharp
public QwpWebSocketSender(SenderOptions options) {
    _schemaCache = new QwpSchemaCache(...);
    _symbolDictionary = new QwpSymbolDictionary(...);
    _encoderBuffers = new[] { new FrameBuilder(...), new FrameBuilder(...) };
    _encoderReady = new[] { new SemaphoreSlim(1,1), new SemaphoreSlim(1,1) };
    if (_sfMode) {
        (_sfEngine, _sfDrainerPool) = BuildSfStack(options);  // ← can throw
        ...
    }
    ...
}
```

If `BuildSfStack` throws, the constructor exits with the
`_encoderReady` semaphores already allocated but `_sfEngine` /
`_sfDrainerPool` null. Caller never gets the reference, so
`Dispose` never runs explicitly — the GC reclaims. `SemaphoreSlim`
has a finalizer that disposes its internals, so the OS handle is
eventually released, but until the next Gen2 finalization there's a
small managed leak.

Severity: LOW — recoverable via GC, no resource exhaustion under
normal failure rates. Fix: wrap the post-line-126 allocations in a
try/catch that disposes the semaphores on failure, OR move the
SemaphoreSlim allocation after the SF stack so they're only
allocated if the rest of construction succeeded.

**LOW — Producer/Dispose race relies on terminal-error catch path**

If thread A is mid-`Send()` and thread B calls `Dispose()`, the
flow is:
1. Dispose cancels `_ioCts` → SendLoop exits, ReceiveLoop exits.
2. Dispose drains channel via `_sendChannel.Writer.TryComplete()`,
   waits up to `close_flush_timeout_millis` on
   `Task.WhenAll(_sendLoopTask!, _receiveLoopTask!)`.
3. Producer's `EnqueueAsyncCore` is awaiting on
   `_encoderReady[idx].WaitAsync(linkedCt)` — `linkedCt` fires from
   the cancel, exception bubbles, terminal error gets set.

The race window: between step 1 and the producer observing the
cancellation, the producer may be mid-`SendBinaryAsync` or
mid-channel-write. The terminal-error pattern mostly handles this
cleanly (produced on cancellation, observed on next public call),
but the producer's CURRENT `Send()` call may not observe terminal
error promptly — it might throw OperationCanceledException
unwrapped instead of `IngressError`.

Severity: LOW — current behaviour is "Send throws something", not
"hang or corruption"; the something is just sometimes the wrong
exception type. Fix: ensure `EnqueueAsyncCore`'s outer catch maps
`OperationCanceledException` to either `_terminalError` (if set) or
re-throw as-is — already partly there, verify all paths.

**Rejected agent claims**

- *Auth-timeout exception mapping race* — claimed non-OCE
  exceptions during ConnectAsync bypass the timeout-handler. They
  do, but **correctly**: the underlying SocketException is the
  meaningful error, not the timeout wrapper. No bug.
- *AwaitEmpty drain-before-failure* — already documented as
  intentional (line 220 comment). Re-rejected.
- *Semaphore leak on wedge throws ObjectDisposedException* — the
  wedge path explicitly documents that semaphores are NOT disposed
  precisely so Release calls don't crash. Agent confused
  "intentional leak" with "missing dispose".
- *FailTerminal vs Dispose ordering* — cosmetic state-machine
  clarity, already covered in pass 5.

### TLS, auth, and DateTime audit (pass 6)

Targeted scan of paths that hadn't been reviewed yet — TLS
certificate validation, auth header construction, DateTime
handling. Several real findings.

**HIGH — `BuildCertificateValidator` reloads the PEM file from disk on every TLS validation call**

`QwpWebSocketSender.cs:1428–1441`:
```csharp
var rootsPath = options.tls_roots!;
var rootsPassword = options.tls_roots_password;
return (_, certificate, chain, errors) => {
    if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0) return false;
    chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
    chain.ChainPolicy.CustomTrustStore.Add(
        X509Certificate2.CreateFromPemFile(rootsPath, rootsPassword));   // ← disk I/O per call
    return chain.Build(new X509Certificate2(certificate!));
};
```

The closure captures `rootsPath` / `rootsPassword` (file path + password
strings) but not the loaded cert. Every TLS validation invocation
re-reads the PEM file from disk and constructs a new `X509Certificate2`.

Impact:
- One handshake may invoke the callback **N times** (once per cert in
  the server chain).
- For SF-mode reconnects, every transport reconnect = a new handshake
  = N more disk reads + cert constructions.
- The trust-store's `CustomTrustStore.Add` line *also* accumulates
  duplicates each call: same cert added repeatedly within a single
  handshake. Memory pressure on the chain object until the handshake
  completes.

Fix: hoist the cert load out of the closure:
```csharp
var rootCert = X509Certificate2.CreateFromPemFile(rootsPath, rootsPassword);
return (_, certificate, chain, errors) => {
    if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0) return false;
    chain!.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
    if (chain.ChainPolicy.CustomTrustStore.Count == 0) chain.ChainPolicy.CustomTrustStore.Add(rootCert);
    return chain.Build(new X509Certificate2(certificate!));
};
```

Cert load is one-time at sender construction. `CustomTrustStore.Add`
is guarded against repeat-add. `CryptographicException` from the file
load surfaces at `Sender.New` instead of being swallowed by the SSL
layer mid-handshake. Severity: HIGH — disk I/O + repeat allocation
on every handshake / reconnect, plus the silent dedup bug.

**MED — DateTime.Kind handling diverges between QWP and ILP**

`Senders/QwpWebSocketSender.cs:1380–1389` (QWP):
```csharp
private static long DateTimeToMicros(DateTime value) {
    var utc = value.Kind switch {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => throw new IngressError(InvalidApiCall, "DateTime.Kind must be Utc or Local; got Unspecified"),
    };
    return (utc - DateTime.UnixEpoch).Ticks / TicksPerMicrosecond;
}
```

`Buffers/BufferV1.cs:114–118` (ILP):
```csharp
public void At(DateTime timestamp) {
    var epoch = timestamp.Ticks - EpochTicks;
    PutAscii(' ').Put(epoch * 100);
    FinishLine();
}
```

ILP uses `timestamp.Ticks` directly **without inspecting `Kind`**. The
behavioural matrix:

| Kind | ILP | QWP |
|---|---|---|
| `Utc` | correct | correct |
| `Local` | **silently wrong** — Local ticks treated as UTC, time off by local UTC offset | correct (ToUniversalTime) |
| `Unspecified` (default for `new DateTime(...)`) | **silently treated as UTC** | throws |

QWP's behaviour is correct. ILP's is a latent bug — sending
`DateTime.Now` (Local kind) gets stored at the wrong instant on
ILP. The user-visible divergence:

```csharp
sender.At(DateTime.Now);             // ILP: silent timezone offset, QWP: correct
sender.At(new DateTime(2024,1,1));   // ILP: silent UTC, QWP: throws (Unspecified)
```

The Unspecified-throw on QWP is *helpful* (catches user error) but
common code patterns produce Unspecified DateTimes. Recommendations:

1. Fix the ILP latent bug — `BufferV1.At(DateTime)` should switch on
   `Kind` like QWP does. Track separately from this branch (it's an
   ILP-side correctness fix).
2. On QWP, consider relaxing Unspecified to "interpret as UTC" with
   a doc warning, OR keep the throw as a safety net but document
   that callers should pass `DateTime.SpecifyKind(value,
   DateTimeKind.Utc)` for ambiguous cases.

Severity: MED — silent timezone bug on ILP, diverging strictness on
QWP. ILP fix coordinates with this work but isn't blocking.

**LOW — Basic auth: plaintext credentials live on the GC heap**

`QwpWebSocketSender.cs:1402–1405`:
```csharp
if (!string.IsNullOrEmpty(options.username) && !string.IsNullOrEmpty(options.password)) {
    var pair = $"{options.username}:{options.password}";
    return "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(pair));
}
```

The interpolated `pair` string contains the plaintext password and
sits on the GC heap until the next Gen0 collection — typically
seconds, possibly minutes on a low-allocation app. A memory dump
captured during that window contains the password.

For credential-conscious deployments, this matters. Defence:
construct the `username:password:` byte sequence directly in a
`Span<byte>` (stackalloc for typical-length creds, ArrayPool for
larger), Base64-encode in place, never materialise the plaintext as
a managed `string`. `options.password` itself is already a managed
string from the connect-string parse — that's a separate plaintext
exposure to address upstream (`SecureString` or similar) but the
sender-side concatenation is the one we control.

Severity: LOW — defence-in-depth, not a vulnerability per se.
Mention in security docs.

**LOW — Bearer token / Basic auth: header-injection check missing**

`:1410`:
```csharp
return "Bearer " + options.token;
```

If `options.token` contains `\r\n`, the resulting header smuggles
additional headers into the HTTP upgrade request. The connect-string
parser doesn't reject newlines in token / username / password.
Concrete attack requires the user to feed an attacker-controlled
token, which is rare — but defensive validation in `SenderOptions.EnsureValid`
is cheap: reject if any auth field contains a control char.

Severity: LOW — relies on caller passing untrusted input as
credentials, which they shouldn't, but a defensive check is
near-free.

**LOW — `QwpTypeCode` enum is `public`; should be `internal`**

`Enums/QwpTypeCode.cs:35`. The enum exposes wire-format type codes
(0x01 = Bool, 0x02 = Int, etc.). User code never needs to match
against these — the public API takes typed `Column<T>` overloads,
not "tag this column with type code N". Same-pattern enums in the
BCL (`HttpStatusCode`-equivalents for protocol internals) are
typically internal.

`QwpStatusCode` (also public) is borderline — `QwpException`
exposes the status code so `try/catch` blocks can inspect it. That
justifies public. But `QwpTypeCode` is purely internal plumbing.

Fix: change `public enum QwpTypeCode` → `internal enum QwpTypeCode`.
If anything outside `net-questdb-client` references it, that
reference is wrong. Severity: LOW — API-cleanliness, no behavioural
impact.

### Concurrency & lifecycle audit (pass 5)

Two parallel agent sweeps + spot verification. Most agent claims
turned out wrong on read-back; the survivors are below. Three real
findings, two confirmed-as-correct cases worth recording, and three
rejected claims so they don't get re-raised.

**HIGH — `Send()` silently drops a half-built row**

`QwpWebSocketSender.cs:646–653` populates `_flushBatch` by filtering
on `t.RowCount > 0`:
```csharp
foreach (var t in _tables.Values) {
    if (t.RowCount > 0) _flushBatch.Add(t);
}
```

A "half row" — `Table("t").Column("a", 1)` followed directly by
`Send()` without an `At*` call — leaves the table with
`HasPendingRow = true` but `RowCount = 0`. The filter excludes it.
The encoder sees no rows, the half-row data persists in column
buffers, and the user gets no signal that their write was discarded.

Worse, the half-row's FixedLen / NonNullCount stays advanced, but
table.RowCount stays 0. On the *next* row, `SnapshotOnFirstTouch`
sees the column already touched (the touched flag wasn't cleared by
either `FinaliseRow` or `CancelCurrentRow`), so no fresh savepoint is
taken. The next `At` finalises a row that includes the *previously
discarded* half-row's data plus the new value — silent data shift
across rows.

Fix: in `Send` / `SendAsync` / auto-flush, check `_currentTable?.HasPendingRow`
and either (a) throw `IngressError(InvalidApiCall, "row in progress
— call At*() to commit or CancelRow() to abandon")`, or (b) call
`CancelCurrentRow()` automatically. Recommend (a) — silent drop /
silent cancel both surprise; an explicit error is debuggable.
Severity: HIGH — silent data corruption path.

**MED — `_terminalError` read at `:1285` is not `Volatile.Read`**

```csharp
if (_terminalError is not null) { ... }
```

`FailTerminal` writes via `Interlocked.CompareExchange` (full barrier).
The reader at `ThrowIfTerminal:1285` is a plain reference read. On
weak memory architectures (ARM), the producer thread can race past
this check before the CAS write is visible.

Fix: `if (Volatile.Read(ref _terminalError) is not null)`. The CAS
provides the write-side fence; the read needs to pair it. Severity:
MED — narrow race window, only triggers on weak memory architectures
under pathological timing, but the cost of getting it wrong is
"silent ingestion past a terminal failure". One-line fix.

**MED — `_offsetTable[count]` plain write before `_offsetTableCount` `Volatile.Write`**

`QwpMmapSegment.cs:278–279`:
```csharp
table[count] = offset;                              // plain write
Volatile.Write(ref _offsetTableCount, count + 1);   // volatile fence
```

Same shape as the `WritePosition` finding (already documented HIGH).
On ARM, the offset entry write may be reordered past the volatile
count increment — a reader observing the new count could index into
a stale slot. Cross-thread reader is the SF send pump's
`OffsetToFsn(offset)` which uses `_offsetTable`.

Fix: bundle with the `WritePosition` fix — wrap both in
`Volatile.Write` (or guarantee the plain store is fenced via a
preceding `Interlocked` op). Severity: MED — same memory-ordering
class as the existing HIGH finding; CRC catches torn reads.

**LOW — Symbol dict orphaned entries on row cancel after column throw**

`QwpWebSocketSender.cs:350–369`:
```csharp
public ISender Symbol(...) {
    var preCount = _symbolDictionary.Count;
    var globalId = _symbolDictionary.Add(value);   // adds new entry, returns id
    try {
        EnsureCurrentTable().AppendSymbol(name, globalId);  // throws → caught, dict rolled back
    } catch {
        if (_symbolDictionary.Count > preCount) _symbolDictionary.RollbackTo(preCount);
        throw;
    }
}
```

The Symbol method correctly rolls back the dict if AppendSymbol
throws. But if a *later* `Column(...)` call in the same row throws,
the row is cancelled via `CancelCurrentRow` (rolling back column
data), and the symbol dict entry from the earlier successful Symbol
call is **not** rolled back. It stays in the dict and gets emitted
on the next flush as part of the symbol delta.

Severity: LOW — wasteful (extra dict entries → extra wire bytes for
the unused symbol value), not corrupting (the dict is still
consistent; orphaned entries are accepted by the server). For
high-cardinality workloads with frequent partial-row failures this
adds up; for normal workloads it's negligible. Fix would require
threading a "symbol dict checkpoint" through `CancelCurrentRow`,
which crosses the QwpTableBuffer / QwpSymbolDictionary boundary.
Defer unless a real workload hits this.

**Confirmed-correct cases worth recording**

These looked suspicious during the agent sweep but turned out to be
correctly handled. Documenting so future audits don't waste time:

- **`AssertOrSetType` before `EnsureFixedCapacity`** in every typed
  Append (e.g., `QwpColumn.cs:187–189`): the type lock fires before
  the capacity check. If `EnsureFixedCapacity` throws, the type
  *is* locked — but the row will be cancelled via the
  `QwpTableBuffer.AppendXxx` try/catch, and `Savepoint` (line 344)
  captures `IsTyped` / `TypeCode` / `DecimalScaleSet` /
  `GeohashPrecisionSet`. `Restore` resets all of these correctly.
  Verified: the type lock is rolled back via savepoint.
- **`CancelRow` not decrementing `_runningRowCount`**: counter is
  incremented inside `At*` *after* the row commits. If `CancelRow`
  is called *before* `At` (correct usage), the counter wasn't
  incremented for this row → no decrement needed. If `CancelRow`
  is called *after* `At`, the row is committed (no in-progress row
  to cancel) — `CancelCurrentRow`'s touched-flag iteration is a
  no-op (`FinaliseRow` cleared the flags), and the counter
  correctly reflects the committed row. Verified: counter is
  consistent in both orderings.
- **`_encoderReady[idx].Release()` ordering**: released in the
  `SendLoop`'s `finally` block at `:922` — *after* `SendBinaryAsync`
  returns. `ClientWebSocket.SendAsync(Memory<byte>)` masks the
  buffer in place; releasing the encoder slot earlier would let the
  producer overwrite mid-mask. Verified: the buffer is held until
  the send completes.

**Rejected agent claims**

Documented so they don't re-surface:
- `ownsSlot`/`ownsReady` flag-flip race in `EnqueueAsyncCore:834–835`
  — the flags only guard the catch-block's release path; the actual
  buffer ownership is held by `_encoderReady[idx]` until SendLoop
  releases it (above).
- `LastFlush` torn write on 32-bit hosts — `DateTime` is 8 bytes;
  on the supported runtimes (.NET 6+) which are 64-bit-only on
  Linux/macOS and 64-bit-default on Windows, struct writes are
  atomic. Theoretical concern, no real exposure.
- `_tableEntryHandler` Volatile.Write/Read pairing claim
  (QwpCursorSendEngine:168) — current code is correct; agent
  flagged it as "fragile" without identifying a real race.

### Behavioural inconsistencies between QWP and ILP

Cross-transport audit of validation, ordering, and error semantics
on the same logical operation. Each finding verified by reading both
implementations.

**`QwpColumn.AppendLong` accepts `long.MinValue` — forward-compatible with NOT NULL support**

ILP rejects `long.MinValue` (`BufferV1.cs:429`) because in legacy
QuestDB it's the `NULL_LONG` sentinel — writing it as a non-null
value silently round-trips to NULL on query.

QWP's `AppendLong` (`QwpColumn.cs:209`) writes the value directly,
no check. **This is the correct forward-compatible behaviour.**
QuestDB is adding NOT NULL column support, which removes the
sentinel collision: a NOT NULL `long` column can store
`long.MinValue` as a real value. The QWP path is already aligned
with that future; the ILP rejection will need to be relaxed in
a coordinated server+client release.

Action: do not add the rejection to QWP. Document in the QWP
column doc / changelog that BIGINT columns accept the full int64
range when used with NOT NULL (or with any QuestDB version that
removes the sentinel semantics). When the ILP-side guard is
relaxed, that should also be a documented release note.

The legacy ILP behaviour is the bug; QWP starting clean is the
fix. Marking as **FORWARD-COMPAT NOTE**, not a finding.

**`max_name_len` config option half-broken on QWP**

`SenderOptions.max_name_len` (default 127) is plumbed into
`QwpTableBuffer`'s constructor at `QwpWebSocketSender.cs:334,341`.
The constructor uses it for **table-name** validation
(`QwpTableBuffer.cs:78`):
```csharp
if (nameByteCount > maxNameLengthBytes)
    throw new IngressError(ErrorCode.InvalidName, ...);
```

But `GetOrCreateColumn` at `:351` uses the hardcoded constant for
**column-name** validation:
```csharp
if (nameByteCount > QwpConstants.MaxNameLengthBytes)
    throw new IngressError(...);
```

So a user setting `max_name_len=200` gets:
- 200-byte table name: accepted ✓
- 200-byte column name: throws "exceeds 127 UTF-8 bytes" ✗

Fix: store `_maxNameLengthBytes` as a field on `QwpTableBuffer`,
use it in both places. ~3 LOC. Severity: HIGH — user-configurable
option is silently half-applied.

**Symbol-after-Column ordering not enforced on QWP — accepted divergence**

ILP throws `"Cannot write symbols after fields"`
(`Buffers/BufferV1.cs:293`) — required by the ILP wire format which
encodes symbols and fields with different syntax (positional prefix
vs suffix).

QWP doesn't care about order; the columnar format encodes each
column independently. Decision: keep this divergence — QWP allows
any append order, ILP enforces its format constraint. Users
writing transport-portable code know symbols-first is the
lowest-common-denominator pattern.

Document in the QWP `Symbol` XML doc that order is unconstrained
on this transport but **portable code should still emit symbols
before fields** for ILP compatibility. No code change.

**QWP frame caps — wire-format hard vs implementation policy**

The audit asked whether the QWP caps could be configurable. Auditing
the wire encoding (`QwpEncoder.cs:148–222`) splits them into two
categories:

| Constant | Wire encoding | Hard cap source | Configurable? |
|---|---|---|---|
| `MaxTablesPerMessage = 0xFFFF` | `WriteUInt16LittleEndian` at frame header | uint16 wire field | **No** — bumping requires a wire-format break |
| `MaxRowsPerTable = 1_000_000` | `WriteVarint` at `:181` | implementation policy | **Yes** — varint can hold up to ulong.MaxValue |
| `MaxColumnsPerTable = 2048` | `WriteVarint` at `:182` | implementation policy | **Yes** — same |
| `MaxBatchBytes = 16 MB` | explicit pre-send check at `:140` | implementation policy | **Yes** — sanity cap, no wire encoding |
| `MaxNameLengthBytes = 127` | name written via `WriteString` (varint length prefix) | implementation policy | **Already** half-configurable via `max_name_len`; finding above |
| `MaxArrayDimensions = 32` | `u8` ndims at array data | wire field allows up to 255; 32 is policy | **Yes** within u8 cap |
| `MaxErrorMessageBytes = 1024` | `WriteUInt16` at error response | wire field allows 65535 | **Yes** within u16 cap |
| `MaxSchemasPerConnection = 65535` | `WriteVarint` (schema id) | matches uint16 server cap | **Yes** within server cap |

So most of the caps are policy, not wire-format. They could become
config options coordinated with the server's matching limits.

**Recommended split:**

1. **`MaxBatchBytes` should become `max_batch_bytes` on
   `SenderOptions`.** Most user-visible: the server may raise its
   accepted frame size (or operator may configure it lower); clients
   should track. Default 16 MB. Validate against
   `auto_flush_bytes` (action 1 above) — `auto_flush_bytes ≤
   max_batch_bytes / 2`.
2. **`MaxRowsPerTable` and `MaxColumnsPerTable`**: leave as
   constants but bump if the server bumps. They're already loose
   — 1 M rows × 2048 cols would produce a 16+ GB raw frame, well
   beyond `MaxBatchBytes`. The byte cap will fire first in
   practice. Adding config options here is paperwork without
   meaningful new flexibility.
3. **`MaxTablesPerMessage = 0xFFFF`** is wire-format hard; no
   config change possible without a v2 frame header.
4. **`MaxNameLengthBytes`**: already plumbed as `max_name_len`;
   action 2 in the API consistency followups fixes the half-applied
   bug.

So the action is: expose `max_batch_bytes`, document the others
as wire-policy constants. The cross-transport-surprise concern
(user batching 2M rows works on HTTP, throws on QWP at 1M) is
real but rare given default `auto_flush_rows=1000` — the
auto-flush trigger fires 1000× before hitting the cap. Document
in the `auto_flush=off` doc that QWP enforces a per-frame row
cap.

Severity: MED — not a current bug, just a missing config knob
plus documentation.

**Error code divergence on equivalent failures — accepted divergence**

| Condition | ILP code | QWP code |
|---|---|---|
| Empty / oversized name | `InvalidApiCall` | `InvalidName` |
| Required-call-order violation | `InvalidApiCall` | `InvalidApiCall` ✓ |
| Unsupported feature | (n/a) | `InvalidApiCall` |

Decision: **leave ILP on `InvalidApiCall`, QWP uses `InvalidName`
going forward.** ILP's existing error-code surface is a stable
contract for HTTP/TCP consumers; backporting `InvalidName` to ILP
risks breaking error-handling code that pattern-matches on the
existing code. QWP starts clean with the more specific code.

Document the per-transport difference in the `ErrorCode.InvalidName`
XML doc: *"Used by QWP for name-validation errors; ILP uses
`InvalidApiCall` for equivalent conditions."* Future-direction note:
new senders or new validation rules should use the more specific
code where one exists.

**Architectural drift analysis — keeping the senders separate**

Decision context: the senders **stay structurally separate** —
the columnar buffer (QWP) and text buffer (ILP) don't share enough
to make a unified base profitable. The duplication is real; we want
to manage it without merging.

### Where duplication actually exists

`QwpWebSocketSender` re-implements ~30 methods that `AbstractSender`
also implements. Cataloguing by drift risk:

**High-risk (validation rules — same logical contract, different
storage):**
- `Table(span)` — name validation (length, format, empty check)
- 14 `Column<T>(span, T)` overloads — name validation +
  type-specific value validation
- `Symbol(span, span)` — name and value validation
- `ColumnNanos(span, long)` — name + nanos range validation
- 6 `At*`/`AtAsync*` overloads — timestamp validation
- `AtNow`/`AtNowAsync` — trivial dispatch

This is where the `max_name_len` half-broken bug came from. Each
overload in QwpWebSocketSender duplicates the validation that
AbstractSender's matching overload does. A new validation rule
added to one without mirroring drifts immediately.

**Medium-risk (auto-flush / row-count book-keeping):**
- `_runningRowCount++` after each `At*` (QWP) vs
  `Buffer.RowCount` (ILP) — two different counters
- `FlushIfNecessary` / `ShouldAutoFlush` — both senders have
  parallel implementations of the same trigger logic
  (`auto_flush_rows`, `auto_flush_bytes`, `auto_flush_interval`)

**Low-risk (transport-specific, no shared contract):**
- `Send`/`SendAsync` — entirely different I/O model
- `Transaction`/`Commit`/`Rollback` — QWP throws, ILP implements
- `Length` — different semantics (already documented)
- `Truncate`/`CancelRow`/`Clear` — different storage primitives
- `Dispose`/`DisposeAsync` — different lifecycle

### Mitigations, ranked

**Tier 1 — Static validation helpers (recommended)**

Extract pure validation into a `static class SenderValidation`:

```csharp
internal static class SenderValidation
{
    public static void ValidateName(ReadOnlySpan<char> name, int maxByteLen, string what)
    {
        if (name.IsEmpty)
            throw new IngressError(ErrorCode.InvalidName, $"{what} name must not be empty");
        var bytes = Encoding.UTF8.GetByteCount(name);
        if (bytes > maxByteLen)
            throw new IngressError(ErrorCode.InvalidName,
                $"{what} name exceeds {maxByteLen} UTF-8 bytes (got {bytes})");
        // Reserved chars, dot/comma, surrogate handling — extracted once.
    }

    public static void ValidateNanosRange(long nanos) { ... }
    public static void ValidateGeohashPrecision(int bits) { ... }
    public static void ValidateArrayShape(ReadOnlySpan<int> shape, int valueCount) { ... }
    // ... per-rule helper, called from both AbstractSender and QwpWebSocketSender
}
```

Both senders call the helpers from their respective `Table`,
`Column*`, `At*` entry points. **A new validation rule lands once,
applies to both transports.** Catches the most common drift class
(the `max_name_len` and `long.MinValue`-style bugs).

Effort: ~150 LOC of helpers + ~50 call-site edits. One PR. Low risk
because it's pure extraction with no behaviour change.

**Tier 2 — Trivial dispatch overloads as default interface methods**

Several `At`/`AtAsync` overloads are pure forwarding:
- `At(DateTimeOffset) => At(value.UtcDateTime)`
- `AtAsync(DateTimeOffset) => AtAsync(value.UtcDateTime)`
- `AtNow() => At(DateTime.UtcNow)`
- `AtNowAsync() => AtAsync(DateTime.UtcNow)`

These can become default interface methods on `ISender`,
removing two duplicated implementations entirely.

Effort: ~20 LOC. Mechanical.

**Tier 3 — Auto-flush trigger consolidation (optional)**

Move the `ShouldAutoFlush` logic to a shared `static class
AutoFlushPolicy` or extract into a struct that both senders embed.
Currently both senders re-implement the (rows | bytes | interval)
OR-trigger. A change to the policy (e.g. add a new trigger axis)
requires both implementations.

Effort: ~80 LOC. Requires both senders to expose `Length`/`RowCount`/
`LastFlush` to the shared logic — already true.

**Tier 4 — `IBuffer` abstraction across senders (NOT recommended)**

Make `AbstractSender` buffer-agnostic via the existing `IBuffer`
interface (V1/V2/V3 already implement it); add a
`QwpBuffer : IBuffer` shim that wraps the columnar machinery.

Effort: ~600+ LOC of refactor. Risk: high — the IBuffer surface
was designed for text; bending it to columnar may distort either
side. Not recommended unless future shared work (e.g. a fourth
transport) makes the unification worthwhile.

### Recommended action

Land **Tier 1 + Tier 2** as follow-up PRs, in that order. Together
they catch ~90% of predictable drift (validation rules) at low
cost. Defer Tier 3 unless auto-flush gets a new axis. Skip Tier 4
unless a third buffer model appears.

**Test parity** is the complementary safety net: extend
`JsonSpecTestRunner` (`src/net-questdb-client-tests/JsonSpecTestRunner.cs`)
to dispatch its conformance vectors against `Sender.New("ws::...")`
in addition to HTTP/TCP. Behavioural divergence shows up as test
failures, not silent runtime drift.

Severity: MED — not a current bug after the `max_name_len` and
`long.MinValue` decisions above are settled, but the duplication
will keep generating drift-class bugs without the helpers.

### API consistency followups (from the cross-transport audit)

Four concrete actions distilled from the API consistency review.
Listed at HIGH because the first three are correctness/contract
issues, not just style.

**Action 1 — Re-define `auto_flush_bytes` semantics on QWP and add a guardrail**

ILP `auto_flush_bytes` is a *wire-size* budget — the buffer is the
wire payload. QWP is pipelined and columnar: the buffer is in-memory
column data, the wire is an encoded frame with schema headers,
varint length prefixes, Gorilla-compressed timestamps, and an
envelope. **Wire size is always larger than `Length`**, sometimes
significantly for narrow rows with many short symbol values.

Real risk: a user who sets `auto_flush_bytes = 0.9 × MaxBatchBytes`
(thinking they're under the wire ceiling) can have `Length` stay
below threshold while the next row's encoded frame exceeds
`MaxBatchBytes` mid-encode, throwing
`payload size N bytes exceeds the M-byte limit; flush more often`
from `QwpEncoder.EncodeInto:140`. The throw is correct but the
config promise was supposed to prevent it.

Fix:
- Update the `ISender.Length` interface doc to say:
  *"Pending data size. On HTTP/TCP this is exact UTF-8 wire bytes;
  on QWP this is in-memory column-buffer footprint and does not
  include schema/varint/header overhead. Used by `auto_flush_bytes`
  to bound buffered rows; not a wire-size estimate."*
- In `SenderOptions.EnsureValid` for ws/wss schemes, validate
  `auto_flush_bytes ≤ QwpConstants.MaxBatchBytes / 2` and throw
  `IngressError(ErrorCode.ConfigError, ...)` if violated. Document
  the headroom rationale (encoder can roughly double for narrow
  symbol-heavy rows). Reuse the existing config-validation pattern
  from `ValidateAutoFlush` and friends.

**Action 2 — Implement `ISender.Truncate()` on QWP**

Currently a documented no-op (`QwpWebSocketSender.cs:1003`) with a
comment claiming "no buffer-tail to trim like the ILP text path".
Wrong: per-column `FixedData` / `StrData` / `StrOffsets` / `BoolData`
/ `SymbolIds` grow by doubling and *do* have unused tails.
ILP `TrimExcessBuffers` releases unused buffer chunks; QWP can
symmetrically `Array.Resize` the column buffers to the
`FixedLen` / `StrLen` / `NonNullCount` boundaries.

```csharp
// QwpColumn.cs — new
public void TrimToCurrent() {
    if (FixedData is not null && FixedData.Length > FixedLen)
        Array.Resize(ref FixedData, FixedLen);
    if (StrData is not null && StrData.Length > StrLen)
        Array.Resize(ref StrData, StrLen);
    if (StrOffsets is not null && StrOffsets.Length > NonNullCount + 1)
        Array.Resize(ref StrOffsets, NonNullCount + 1);
    if (BoolData is not null) {
        var needed = (NonNullCount + 7) / 8;
        if (BoolData.Length > needed) Array.Resize(ref BoolData, needed);
    }
    if (SymbolIds is not null && SymbolIds.Length > NonNullCount)
        Array.Resize(ref SymbolIds, NonNullCount);
}

// QwpWebSocketSender.cs — replace empty body
public void Truncate() {
    ThrowIfTerminal();
    foreach (var t in _tables.Values) {
        foreach (var col in t.Columns) col.TrimToCurrent();
        t.DesignatedTimestampColumn?.TrimToCurrent();
    }
}
```

Caller's mental model — "Truncate releases extra buffer memory" —
works across all transports. ~40 LOC.

**Action 3 — Task → ValueTask on `SendAsync` / `CommitAsync` / `PingAsync`**

Three async methods on the public surface still return `Task`:

| Method | Sync-completable | Allocation today |
|---|---|---|
| `ISender.SendAsync` | yes (zero rows pending → no-op) | Task per call |
| `ISender.CommitAsync` | yes (empty transaction body) | Task per call |
| `IQwpWebSocketSender.PingAsync` | yes (idle in-flight window) | Task per call |

All three have meaningful synchronous-completion paths and should
return `ValueTask`. The internal implementations already use
ValueTask (`EnqueueAsyncCore`, `PingAsyncCore`); the public methods
unwrap to Task via `.AsTask()` — which is the per-flush ~96 B
allocation already flagged in the per-flush section. Fixing this
*is* fixing that.

**Breaking change**: source-breaking for callers that store the
result as `Task`; binary-breaking. Acceptable to bundle into the
qwip_victor release alongside the new QWP transport. Changelog
note: *"Async methods on `ISender` now return `ValueTask` /
`ValueTask<T>`. Source-compatible for `await sender.X()`; callers
storing the result as `Task` should add `.AsTask()`."*

Implementation:
- Change `Task SendAsync(...)` → `ValueTask SendAsync(...)` on
  `ISender`.
- Same for `CommitAsync`.
- Change `Task PingAsync(...)` → `ValueTask PingAsync(...)` on
  `IQwpWebSocketSender`.
- HTTP/TCP `Send` / `Commit` impls in `AbstractSender` /
  `HttpSender` already wrap an internal Task — return as
  `new ValueTask(internalTask)` (zero-cost wrap) or refactor the
  internal to return ValueTask.
- QWP `SendAsync` / `CommitAsync` currently call
  `EnqueueAsyncCore(...).AsTask()` — change to return the
  `ValueTask` directly (drops the `.AsTask` allocation).

**Action 4 — Fix `ISender.SendAsync` doc comment**

Strip the stale sentences:

```csharp
/// If the SenderOptions.protocol is HTTP, this will return request and response information.
/// If the SenderOptions.protocol is TCP, this will return nulls.
```

The method returns `Task` (about to become `ValueTask`), with no
result value. The doc was likely true of an earlier API shape.
One-line edit.

### API consistency with the ILP transports

`QwpWebSocketSender` implements `ISender` (the contract shared with
`HttpSender` / `TcpSender` via `AbstractSender`). Most of the surface
matches cleanly — `Table` / `Symbol` / `Column` / `At` / `AtAsync`
overloads, `Send` / `SendAsync`, naming convention (sync method +
async-suffix variant), `WithinTransaction = false` (matches TCP),
disposal pattern (sync `Dispose` + truly-async `DisposeAsync`,
documented in `ISender` remarks), `[Obsolete]` markers on
`AtNow*` propagated via interface inheritance.

A few real inconsistencies:

**`ISender.Length` — semantic mismatch between ILP and QWP**

ILP (`AbstractSender.cs:40`): `Length => Buffer.Length` — exact UTF-8
byte count of the pending wire data, suitable for `auto_flush_bytes`
trigger comparisons.

QWP (`QwpWebSocketSender.cs:279–291` + `EstimateTableSize` at
`:1361–1378`): sums `col.FixedLen + col.StrLen` across all columns
of all tables. Excludes the schema-block bytes, the varint length
prefixes for symbols, the Gorilla compressed timestamp footprint,
and the QWP frame header. The actual wire size will be *larger* than
`Length` reports.

Caller-visible impact: `auto_flush_bytes` triggers on *content* size,
not actual wire size. Probably fine for the typical use case (callers
set this to a soft cap, not an exact byte budget), but the interface
docstring says "current length of the buffer in UTF-8 bytes" — which
is wrong for QWP (the buffer isn't UTF-8) and inaccurate as an
estimate. Two options:

1. Update the interface doc to "approximate buffer footprint in bytes
   for `auto_flush_bytes` accounting; not the exact wire size on
   binary protocols."
2. Compute exact size on QWP — would require a dry-run encode pass on
   every `Length` access, which is O(rows × cols). Don't.

Recommend (1) — document the approximation.

**`ISender.Truncate()` — silently a no-op on QWP**

ILP (`AbstractSender.cs:243–246`): `Buffer.TrimExcessBuffers()` removes
unused buffer chunks past the active one. Real memory recovery for
HTTP/TCP after a large flush.

QWP (`QwpWebSocketSender.cs:1003–1007`):
```csharp
public void Truncate() {
    ThrowIfTerminal();
    // QWP column buffers are sized by row count; no buffer-tail to trim like the ILP text path.
}
```

Silent no-op. The interface doc says "Removes unused extra buffer
space" — for QWP that's misleading. The column buffers (`FixedData`,
`StrData`, `StrOffsets`, `BoolData`, `SymbolIds`) ARE potentially
oversized after a flush due to doubling-growth; `Array.Resize` down to
`FixedLen` / `StrLen` would actually reclaim memory the same way ILP
does.

Two options:
1. Implement: shrink the per-column buffers to exact `FixedLen` /
   `StrLen` boundaries on `Truncate()`. Symmetric with ILP behaviour.
2. Document: update the interface doc to acknowledge it may be a
   no-op for some transports.

Recommend (1) — the implementation is small and gives the property
real meaning across transports. Comment says "no buffer-tail to trim
like the ILP text path" but column buffers literally have a tail
(`FixedData[FixedLen..]`).

**`IQwpWebSocketSender.GetHighestAckedSeqTxn(string)` / `GetHighestDurableSeqTxn(string)` — string-typed parameters**

The only `string`-typed parameters left in `IQwpWebSocketSender`,
inconsistent with the rest of the QWP/ILP surface (which is
`ReadOnlySpan<char>` everywhere). Already in scope as part of the
`SpanKeyedDict` work; flagged here too because it's an API design
inconsistency, not just a perf one. Change signature to
`ReadOnlySpan<char> tableName`. Non-breaking for source — string
callers pass through implicit conversion.

**`ISender.SendAsync` doc references nonexistent return value**

```csharp
/// If the SenderOptions.protocol is HTTP, this will return request and response information.
/// If the SenderOptions.protocol is TCP, this will return nulls.
public Task SendAsync(CancellationToken ct = default);
```

The method returns `Task`, not `Task<T>`. There's no return value to
contain "request/response info" or "nulls". Doc is stale relative to
the current API. Strip the misleading sentences; or, if the intent
was for HTTP to return `HttpResponseMessage`, add a
HTTP-only-overload on `IHttpSender` that does. Recommend strip.

**Error code for "operation not supported on this transport"**

QWP's `Transaction` / `Rollback` / `Commit*` throw
`IngressError(ErrorCode.InvalidApiCall, "transactions are not supported on the WebSocket transport")`.
`InvalidApiCall` is the same code used for "Table() must be called
before adding columns or symbols" — a usage error, not a
capability gap. A dedicated `ErrorCode.NotSupportedOnTransport` (or
similar) would let calling code distinguish "I called this wrong" from
"this transport can't do this". Style-only — current behaviour is
correct, just under-distinguished. Defer if not bundled with other
error-code work.

### Maintainability

**`src/net-questdb-client/Senders/QwpWebSocketSender.cs` — 1445 lines mixing AsyncMode + SF mode**

Two transport models (in-memory channel vs. cursor engine) interleaved
across one class with `_sfMode` branching at every entry. Not a bug,
but it's the file most likely to grow new bugs during maintenance.
Splitting into base + `AsyncSender` + `SfSender` is mechanical (the
public surface is already `ISender` / `IQwpWebSocketSender`). Defer if
desired, but do not let it grow further.

---

## MED — worth a fix or follow-up ticket

**`src/net-questdb-client/Qwp/QwpInFlightWindow.cs:251` — 100 ms `Monitor.Wait` poll quantum**

State-change wakeups go via `Monitor.PulseAll`, so steady-state latency
is fine. But cancellation only fires on the next poll boundary because
`Monitor.Wait` does not accept a `CancellationToken` — worst-case
100 ms cancel latency. Either drop `CancellationPollMs` to ~10–20 ms,
or register a `CT.UnsafeRegister(() => { lock (_lock)
Monitor.PulseAll(_lock); })` so cancel pulses through immediately.

**`src/net-questdb-client/Qwp/QwpColumn.cs:331` — `GetMaxByteCount` over-reserves 3× for ASCII varchars**

```csharp
var maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
EnsureStringCapacity(StrLen + maxBytes);
```

For ASCII varchar (the common case) this triples buffer growth
pressure. Trade-off vs. `GetByteCount` (which scans). Acceptable for
now — log a follow-up to benchmark `GetByteCount` upfront vs. capped
`value.Length * 1.5` retry path on long ASCII workloads.

**`src/net-questdb-client/Qwp/QwpWebSocketTransport.cs:392–397` — reflection in `BuildDefaultClientId`**

Called per transport construction (= per sender). Cache the result in a
`static readonly string`.

**`src/net-questdb-client/Qwp/Sf/QwpSegmentManager.cs:~224` — disk-block reservation writes one byte per page**

The reserve loop does `position += pageSize; stream.WriteByte(0);` over
the whole segment. Linear in pages. Use `posix_fallocate` via P/Invoke
on Linux, `SetEndOfFile` on Windows, or batch into 64 KiB zero writes.
Cold path so MED, but `sf_max_bytes=large` makes startup slow.

**`src/net-questdb-client/Senders/QwpWebSocketSender.cs:158` — `transport.ConnectAsync(...).GetAwaiter().GetResult()` in ctor**

Sync-over-async in the constructor. Safe today because every transport
await uses `ConfigureAwait(false)` (verified) — no sync-context
deadlock. But: a future internal await without `ConfigureAwait(false)`
on a UI-thread caller would deadlock. Commit to "all internal awaits
must be `.ConfigureAwait(false)`" via an analyzer (`CA2007`) and/or
expose `Sender.NewAsync`.

**`src/net-questdb-client/Qwp/Sf/QwpReconnectPolicy.cs:~136` — `elapsedSinceOutage > MaxOutageDuration` boundary**

At equality the policy still grants a backoff. Confirm intent — Java
client typically uses `>=`. Tiny but visible in the give-up window.

**`src/net-questdb-client/Senders/QwpWebSocketSender.cs:878` — `.AsTask()` allocates a `Task` wrapper on the sync flush path**
```csharp
private void EnqueueSync(CancellationToken ct, bool awaitDrain) {
    EnqueueAsyncCore(ct, awaitDrain).AsTask().GetAwaiter().GetResult();
}
```
`ValueTask.AsTask()` always materialises a `Task` (~96 B), even when
the underlying ValueTask completed synchronously. Per flush, ten
thousand times in the standard ingestion bench. Fix: spin-wait on the
ValueTask directly via `if (vt.IsCompleted) vt.Result; else
vt.AsTask().GetAwaiter().GetResult();` — pays the alloc only when the
fast path fails — or `vt.GetAwaiter().GetResult()` (which spins
internally without materialising a Task on the completed path).

**`src/net-questdb-client/Senders/QwpWebSocketSender.cs:783` — `async ValueTask EnqueueAsyncCore` heap-boxes its state machine when an await goes async**

`EnqueueAsyncCore` awaits two `SemaphoreSlim.WaitAsync` calls. With
`in_flight_window=32` and a fast-acking server most hits are
synchronous, but under contention the `async ValueTask` state machine
is heap-promoted (~150 B). One-line fix:
`[AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder))]`
on the method — pools the state machine box. First-class runtime
support since .NET 6.

**`src/net-questdb-client/Qwp/QwpEncoder.cs:380–391` — `WriteString` double-passes UTF-8**
```csharp
var byteCount = Encoding.UTF8.GetByteCount(value);   // pass 1
buf.WriteVarint((ulong)byteCount);
var dest = buf.Allocate(byteCount);
Encoding.UTF8.GetBytes(value, dest);                 // pass 2
```
Two scans of the same string. Cold path in WS mode (only the first
flush per table emits the full schema; subsequent flushes use schema
reference and skip names entirely). **Hot in SF mode** — every
self-sufficient frame re-emits the full schema + symbol dict, so
every flush double-passes every name. Fix: write into the buffer with
`GetMaxByteCount` upper bound, capture the actual count from the
`GetBytes(value, dest)` return, then back-patch the leading varint.
For names ≤ 127 UTF-8 bytes (~all real names) the varint is a single
byte and back-patching is a single store.

---

## LOW — style / nice-to-have

- **`src/net-questdb-client/Senders/QwpWebSocketSender.cs:545,560,574,584,600,616`** — `_runningRowCount++` repeated in every `At*` method. Move to a single helper called by all `At*` paths to eliminate the "did I forget one" risk on next type addition.
- **`src/net-questdb-client/Qwp/QwpTableBuffer.cs:58, 62`** — `_touchedInCurrentRow` and `_rowSavepoints` start `Array.Empty<>`. First few rows thrash through 1→2→4→8 resizes. Initialise to size 8 (matches `EnsureTouchedCapacity`).
- **`src/net-questdb-client/Qwp/QwpColumn.cs:326`** — `StrOffsets = new uint[InitialSymbolCapacity]` reuses the symbol-capacity constant for varchar offsets. Misleading name; rename or alias the constant.
- **`src/net-questdb-client/Qwp/Sf/QwpSegmentRing.cs:OldestFsn` getter** — takes the lock *and* `Volatile.Read`s inside. The lock provides the fence; the Volatile is redundant.
- **`src/net-questdb-client/Utils/SenderOptions.cs`** — `IsHttp()` / `IsTcp()` / `IsWebSocket()` each duplicate the protocol switch. Single helper or `ProtocolType.IsXxx()` extension methods.
- **`src/net-questdb-client/Qwp/Sf/QwpCursorSendEngine.cs:286`** — `(int)Math.Min(remainingMs, 200)` is safe because of the upper bound; cast reads defensively but isn't strictly needed. Style only.

---

## Rejected — confirmed non-issues, do not re-raise

These looked plausible during the scan but turned out wrong on closer
inspection. Captured here to save future review time.

- **`QwpInFlightWindow.cs:143–146`** — `wakeup.TrySetResult(true)` *outside* the lock is the correct, recommended pattern (avoids running TCS continuations under your lock). The TCS is captured under lock, then signalled outside. Not a race.
- **`QwpSymbolDictionary.cs:147–152`** — `RemoveAt(_values.Count - 1)` is O(1) (last element, no shift); the rollback loop is overall O(n), not O(n²).
- **`QwpEncoder.cs:88`** — the `new byte[len]` is in the explicitly-documented test-only `Encode()` overload; production paths use `EncodeInto` which reuses the `FrameBuilder` buffer.
- **`QwpMmapSegment.cs:AppendOffset` (line 264)** — claimed two-producer race is impossible: callers serialise via `_stateLock` (`QwpCursorSendEngine.AppendBlocking:203`).
- **`QwpMmapSegment.cs:TryReadFrame` (line 291) torn-data race** — real on weak memory if `WritePosition` isn't volatile (covered as a separate HIGH above), but the CRC check at 322–330 is the safety net by design; the read path itself is not the bug.
- **`QwpWebSocketSender.cs:826`** — `_inFlightWindow.Add(seq)` *before* `TryWrite` is intentional and commented; `TryWrite` failure after slot reservation is treated as a terminal-error invariant violation.
- **`QwpWebSocketSender.cs:1142`** — `Task.WhenAll(...).Wait(timeout)` in synchronous `Dispose` is correct; the async path (`DisposeWsStackAsync`) properly awaits.
- **`QwpWebSocketTransport.cs:246`** — receive-buffer doubling with `Array.Resize` is idiomatic and bounded by `maxBytes`.
- **`QwpCursorSendEngine.cs:188`** — agent claimed missing `ConfigureAwait` on `Task.Run(...)`; meaningless because `Task.Run` schedules onto the threadpool, where there is no captured `SynchronizationContext` anyway.
