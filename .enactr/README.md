# Enactr flows

Flywheel's Enactr CI and release automation lives in `flows/`. These definitions
coexist with the GitHub Actions workflows; switching off the GitHub workflows is
a separate cutover decision.

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

The `bump-version` flow checks out the reusable `enactr/release-bump` branch,
commits the selected version increment with Enactr's native Git module, pushes
it, and reconciles one pull request to protected `main`. Merging that pull
request triggers the `release` flow. The release publishes the Helm index from
a native `gh-pages` worktree; the action packages the exact run commit from a
GitHub source archive so the worktree remains single-purpose.

The original CI topology is represented by separate `ci` and `main` flows.
Format and Clippy fan out first; Test and the release build then run in
parallel, while Coverage waits specifically for Test.

Two GitHub Actions behaviors do not yet have direct Enactr equivalents in these
flows:

- CI does not cancel an older run for the same pull-request ref; Enactr
  concurrency is flow-wide rather than grouped by ref.
- The Rust build cache is not persisted because no durable Enactr storage
  backend has been selected for Cargo state.
