# Flywheel

[![Main](https://github.com/ctxswitch/flywheel/actions/workflows/main.yaml/badge.svg)](https://github.com/ctxswitch/flywheel/actions/workflows/main.yaml)

Flywheel is a disk-backed build cache and package proxy for CI workloads. It stores build
outputs and downloaded dependencies on local disk using an embedded RocksDB metadata store.
No external database or object store is required.

## Capabilities

| Workload | Interface |
| --- | --- |
| Go build outputs | `GOCACHEPROG` through `flywheel cacheprog` |
| Bazel | HTTP remote action cache and CAS |
| Go modules | `GOPROXY` |
| Python packages | Simple Repository API with HTML and JSON negotiation |
| JavaScript packages | npm install metadata and tarballs |
| Rust packages | Cargo sparse registry mirror |
| Other build tools | Generic HTTP key/value cache |
| Artifact pipelines | SHA-256 artifacts and mutable references |

## Runtime model

- Each replica owns one local data directory.
- Cached data is not replicated and has no remote storage tier.
- Disk loss, shard replacement, and shard membership changes produce cache misses.
- Raw artifacts and Bazel CAS writes are crash-durable on the accepting disk.
- Generic build-cache, Bazel action-cache, and package-proxy entries are best effort.
- Channels provide cache isolation, access policy, retention, and deletion lifecycle.
- The optional routing agent distributes Default Channel traffic across independent shards.

Flywheel is a cache, not a system of record. Every client must be able to regenerate or
refetch missing data.

## Local execution

### Requirements

- Stable Rust toolchain with Rust 2024 edition support
- Clang and libclang
- CMake
- C++ compiler

### Start a replica

```text
make build
mkdir -p /tmp/flywheel-data
./target/debug/flywheel serve --data-dir /tmp/flywheel-data
```

The default listener is `127.0.0.1:8080`. `SIGINT` performs a graceful shutdown. Reusing
the data directory preserves cached data between runs.

### Verify the cache

```text
curl --fail --silent --show-error http://127.0.0.1:8080/health/ready

printf 'hello from flywheel\n' >/tmp/flywheel-demo
curl --fail --silent --show-error \
  --request PUT \
  --data-binary @/tmp/flywheel-demo \
  http://127.0.0.1:8080/build-cache/http/demo
curl --fail --silent --show-error \
  http://127.0.0.1:8080/build-cache/http/demo
```

## Client configuration

The following examples use the open Default Channel on a local replica.

### Go build cache

```text
export GOCACHEPROG='flywheel cacheprog --url http://127.0.0.1:8080/build-cache/http/'
go build ./...
```

`cacheprog` maintains a verified local object cache and supports session-based prefetching:
it discovers the session manifest through `GET /status?session=` and warms its local cache
with bounded parallel cache GETs. Against replicated shards, prefetch requires a pod-local
`flywheel agent` sidecar, which merges the manifest across shards and routes each download.
Use `--ephemeral-cache` to prevent reuse by later processes. Persistent local directories
and Kubernetes `hostPath` mounts are documented in the
[cacheprog operations guide](docs/operations.md#go-build-cache-with-cacheprog).

### Bazel

```text
build --remote_cache=http://127.0.0.1:8080/build-cache/bazel
```

This setting belongs in `.bazelrc`. Bazel CAS bodies use durable publication. Action-cache
entries use best-effort publication.

### Package managers

```text
export GOPROXY=http://127.0.0.1:8080/proxy/go
export PIP_INDEX_URL=http://127.0.0.1:8080/proxy/python/simple/
export NPM_CONFIG_REGISTRY=http://127.0.0.1:8080/proxy/npm/
```

Cargo source replacement in `.cargo/config.toml`:

```toml
[source.crates-io]
replace-with = "flywheel"

[source.flywheel]
registry = "sparse+http://127.0.0.1:8080/proxy/cargo/index/"
```

Package proxies are read-only. Missing packages are fetched from the configured upstream,
cached locally when capacity permits, and returned through the same channel.

Authentication, fallback behavior, custom channels, and protocol-specific configuration
are covered in the [client operations guide](docs/operations.md#client-configuration-overview).

## Channels

A channel is the isolation and lifecycle boundary for cached data.

| Channel type | Access | Intended use |
| --- | --- | --- |
| Default | Open | Shared cache protected by the deployment network boundary |
| Open custom | Open | Separate retention and cleanup without application credentials |
| Protected | Token | Separate retention and cleanup with application authentication |

Access type is fixed at channel creation. Retention can be changed later. Deletion fences
new writes, removes cached data, and resumes after restart if interrupted. Protected
channel tokens are returned once and cannot be recovered or rotated.

Flywheel serves plain HTTP. TLS termination and registry-level access control belong at the
deployment boundary. Detailed procedures are in [channel administration](docs/operations.md#channel-administration).

## Kubernetes

The Helm chart deploys persistent shards, a headless discovery Service, and an optional
shared routing-agent tier.

```text
helm upgrade --install flywheel charts/flywheel \
  --namespace cache \
  --create-namespace
```

Requirements and defaults:

- Kubernetes 1.32 or newer
- One `ReadWriteOncePod` volume per shard
- Retained claims on scale-down and chart deletion
- Non-root containers with read-only root filesystems
- Startup, readiness, and liveness probes
- PodDisruptionBudgets and topology spreading

Production deployments must pin the image digest, select a StorageClass, and size cache
watermarks against each shard volume.

| Topology | Routing |
| --- | --- |
| Shared agent | Agent Deployment and ClusterIP Service managed by the chart |
| Pod-local agent | Agent sidecar in each build Pod; chart manages shards and discovery |

The routing agent currently serves the Default Channel only. Custom channels remain on a
single-replica boundary until a shared channel control plane exists.

Deployment values and rollout constraints are documented in the [Helm chart guide](charts/flywheel/README.md).
The complete sidecar manifest is in the [sidecar runbook](docs/operations.md#pod-local-agent-sidecar).

## Development

```text
make ci
make release
```

`make ci` checks formatting, runs Clippy with warnings denied, and executes the test suite.
Pull requests and pushes to `main` run the same gates in GitHub Actions.

See [BUILDING.md](BUILDING.md) for platform prerequisites and troubleshooting. Contributions are
welcome under the process in [CONTRIBUTING.md](CONTRIBUTING.md).

## Documentation

| Document | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | Publication, storage, channels, eviction, package rewriting, and scaling boundaries |
| [Operations](docs/operations.md) | Configuration, clients, capacity, metrics, recovery, Helm, and sidecars |
| [Helm chart](charts/flywheel/README.md) | Storage, scheduling, security, networking, monitoring, and values |
| [Domain language](CONTEXT.md) | Channel terminology and invariants |
| [Recency-aware eviction ADR](docs/adr/0001-keep-recency-aware-eviction.md) | Retention and eviction decision record |

## Community and license

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). Use
[SUPPORT.md](SUPPORT.md) for support and issue-reporting guidance and [SECURITY.md](SECURITY.md)
for private vulnerability reporting.

Flywheel is licensed under the [Apache License 2.0](LICENSE).
