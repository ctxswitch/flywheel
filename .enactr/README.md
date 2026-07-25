# Enactr flows

Flywheel's Enactr CI and release automation lives in `flows/`. These definitions
coexist with the GitHub Actions workflows; switching off the GitHub workflows is
a separate cutover decision.

## Git worktrees

Every command action receives its own repository worktree. Actions without a
`git` block use Enactr's automatic checkout: the connected Flywheel repository
is cloned into `/workspace/src`, the run's exact `ENACTR_COMMIT` is checked out
detached, and nothing is published afterward. The `ci`, `main`, release-check,
image-build, manifest, and GitHub-release actions all use this read-only mode.

Worktrees are isolated by action, including actions in the same group. A file
created by one action is not visible to another unless it is transferred
through Storage, a pushed Git ref, or another external artifact service.

Only actions that need branch behavior or publication declare `git`:

| Action | Worktree | After a successful command |
| --- | --- | --- |
| Release `helm` | `gh-pages` branch | Commit the chart archive and index, then push |

The Helm action can own only its `gh-pages` worktree, so it downloads the source
archive addressed by `ENACTR_COMMIT` before packaging the chart. Native Git
credentials come from the connected repository's short-lived installation
token and are redacted by the runtime. `GH_TOKEN` remains separate because
GitHub release creation is a provider API operation outside the native Git
module.

The flows expect these tenant secrets:

| Secret | Used for |
| --- | --- |
| `CODECOV_TOKEN` | Uploading Rust coverage |
| `DOCKERHUB_USERNAME` | Authenticating to Docker Hub |
| `DOCKERHUB_TOKEN` | Pushing images and manifests to Docker Hub |
| `GH_TOKEN` | Creating GitHub releases |

The `GH_TOKEN` credential needs write access to repository contents so GitHub
can create the release tag. Native Git actions use the connected repository's
short-lived installation token instead. Docker image builds run natively and
concurrently on the hosted `build-amd64` and `build-arm64` queues. Each build
pushes an architecture tag; the dependent manifest action combines those
images into the version tag and `latest`.

Version bumps remain owned by `.github/workflows/bump.yaml`. Native Git
configuration is literal, so an Enactr bump flow cannot yet create a fresh
version-named branch safely; reusing a branch can start a later bump from stale
or abandoned release state.

The Enactr `release` flow is manual-only while the GitHub and Enactr
implementations coexist. Before doing any release work, it verifies that an
existing tag points to the run commit and exits without publication when the
GitHub release already exists. The push trigger should be enabled only in the
cutover that disables the GitHub release publisher.

The release publishes the Helm index from a native `gh-pages` worktree; the
action packages the exact run commit from a GitHub source archive so the
worktree remains single-purpose.

The original CI topology is represented by separate `ci` and `main` flows.
Format and Clippy fan out first; Test and the release build then run in
parallel, while Coverage waits specifically for Test.

Two GitHub Actions behaviors do not yet have direct Enactr equivalents in these
flows:

- CI does not cancel an older run for the same pull-request ref; Enactr
  concurrency is flow-wide rather than grouped by ref.
- The Rust build cache is not persisted because no durable Enactr storage
  backend has been selected for Cargo state.
