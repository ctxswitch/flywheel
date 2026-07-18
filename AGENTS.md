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
