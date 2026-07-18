# Contributing to Flywheel

Flywheel accepts bug fixes, documentation improvements, performance work, and focused new
capabilities. Contributions should preserve the service's role as a disposable cache: clients
must remain able to recover from misses, disk loss, and shard replacement.

## Before making a change

- Search existing issues and pull requests before opening a duplicate.
- Open an issue before starting a large feature, public API change, storage-format change, or
  architectural refactor. Small fixes can go directly to a pull request.
- Report vulnerabilities through the private process in [SECURITY.md](SECURITY.md), not a public
  issue.
- Read [CONTEXT.md](CONTEXT.md) before changing channel behavior and
  [docs/architecture.md](docs/architecture.md) before changing storage, publication, routing, or
  recovery semantics.

## Development workflow

1. Fork the repository and create a branch from `main`.
2. Follow [BUILDING.md](BUILDING.md) to install prerequisites and build the project.
3. Add tests at the narrowest seam that proves the public behavior.
4. Run `make ci` before submitting the pull request.
5. Update user, operator, chart, and architecture documentation affected by the change.

Keep changes focused. Do not mix unrelated refactoring into a behavioral fix, and do not commit
generated build output, local data directories, credentials, or captured package tokens.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
type(optional scope): imperative summary
```

Examples:

```text
fix(proxy): preserve hashes in python file links
feat(cache): add disk reservation accounting
docs(operations): document sidecar shutdown
```

Use `feat!:` or a `BREAKING CHANGE:` footer when a change requires clients or operators to take
action.

## Testing expectations

| Change | Minimum verification |
| --- | --- |
| Rust behavior | Focused unit or integration test and `make ci` |
| HTTP contract | Integration coverage for status, headers, and body behavior |
| Storage or recovery | Restart, cleanup, and failure-path coverage |
| Package proxy | Upstream fixture plus cache-hit and route-form coverage |
| Helm chart | `helm lint charts/flywheel --strict` and review of affected manifests |
| Documentation only | Link and command review; run formatting checks when code snippets change |

Tests must not depend on public package registries or other mutable network services. Use local
fixtures and ephemeral directories.

## Pull requests

Complete the pull-request template with the behavior change, verification commands, and
operational impact. Reviewers evaluate correctness, failure behavior, compatibility, tests,
documentation, and consistency with the domain language and architecture decisions.

Maintainers may ask for a change to be split when independent behavior would be easier to review,
revert, or release separately. A pull request is merged only after required checks and review are
complete.

## License

By contributing, you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE).
