# QWP test conventions

Tests under this folder mirror the Java QWP tests in `core/src/test/java/io/questdb/client/test/cutlass/qwp/`. Each test belongs to one of three states:

- **Pass** — assertion is implemented and runs against the .NET production code we have today. No annotation.
- **Pending** — assertion depends on production code that hasn't been ported yet. Body uses `Assert.Inconclusive("…")` with a short note describing what's missing. NUnit reports these as skipped.
- **Divergent** — the Java test relies on behaviour the .NET wrapper deliberately does not replicate (e.g. the Java fluent builder's "already configured" double-set check, which `DbConnectionStringBuilder` resolves as last-writer-wins). Annotated with `[Ignore("…")]` and a short reason citing the divergence + Java line reference where relevant.

When a Pending test gets its production code, flip the body to a real assertion in the same PR. Divergent tests stay `[Ignore]`'d until the divergence itself changes.
