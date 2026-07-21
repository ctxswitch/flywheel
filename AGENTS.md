## Commits

Use the [Conventional Commits](https://www.conventionalcommits.org/) format for commit
messages: `type(optional scope): summary`, e.g. `feat(cache): add disk reservation ledger`
or `fix(proxy): stream passthrough under disk pressure`. Common types: `feat`, `fix`,
`refactor`, `perf`, `docs`, `test`, `build`, `chore`. Keep the summary imperative and
lowercase; add a body for context and a `BREAKING CHANGE:` footer when applicable.

When asked to "commit everything," make a **single** commit covering the whole working
tree — do not split it into per-change commits. Local history is squashed before pushing.

## Agent skills

### Issue tracker

Issues and PRDs live in GitHub Issues. See `docs/agents/issue-tracker.md`.

### Triage labels

Triage uses the canonical five-label vocabulary. See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repository. See `docs/agents/domain.md`.

## Testing

### Unit tests

Unit tests live in `*_test.rs` files co-located with their source modules.
Each test file is declared in the parent `mod.rs` with `#[cfg(test)] mod <name>_test;`.
Imports use explicit `use super::{Type1, Type2}` rather than `use super::*`.

Example:

```
src/
  cache/
    mod.rs          # declares #[cfg(test)] mod space_test;
    space.rs        # production code only
    space_test.rs   # tests for space.rs
```

### Integration tests

Integration tests live in `tests/integration/`. Cargo does not auto-discover
files in that subdirectory, so each one is declared as a `[[test]]` target in
`Cargo.toml`; a new file needs a new entry there. They exercise full Flywheel
instances with HTTP routers, real TCP, and tempfile-backed storage. Helpers
shared between them live in `tests/integration/common/mod.rs`, pulled in with
`#[path = "common/mod.rs"] mod common;` rather than declared as a target.

### What to test

- **Unit tests**: single-module behavior, pure functions, edge cases,
  error paths, and protocol invariants (e.g. frozen hash vectors).
- **Integration tests**: cross-module workflows, HTTP semantics,
  concurrency, disk pressure, and failure recovery.

### What not to test

- Don't write tests that only verify trivially true properties
  (e.g. `assert_eq!(0, 0)`). Every test should fail if the code is broken.
- Prefer testing observable behavior over internal implementation details.
