# Flywheel architecture

Flywheel is a local-first artifact cache and package proxy. Each replica stores artifact
bodies on a local filesystem and keeps channel, reference, and eviction metadata in an
embedded RocksDB database. There is no remote storage tier or cross-replica replication.

This document defines the service boundaries, persisted model, request paths, and
correctness rules. See [operations.md](operations.md) for configuration, channel
administration, monitoring, and recovery procedures. The domain vocabulary is defined in
[`CONTEXT.md`](../CONTEXT.md).

## System contract

Flywheel exposes four cache interfaces over one storage engine:

| Interface | Identity | Purpose |
| --- | --- | --- |
| Raw artifacts | SHA-256 digest | Durable storage and retrieval of exact bytes |
| References | Caller-supplied name | Mutable, non-pinning alias to an artifact digest |
| Build caches | HTTP key or Bazel hash | Regenerable compiler and action output |
| Package proxies | Protocol and upstream URL | Locally cached Go, PyPI, npm, and Cargo traffic |

The following rules apply across all four interfaces:

- A channel is the only isolation, authentication, retention, lifecycle, and deletion
  boundary.
- Artifact identity is the SHA-256 digest of the logical, uncompressed bytes.
- A published artifact is complete. Partial bodies are never visible to readers.
- References do not pin their targets. A reference may become a miss after eviction.
- Reads have no remote fallback. Package proxy misses are the exception because the proxy
  fetches its configured upstream as part of the request.
- Local disk loss loses the cache. Durability levels describe crash behavior on the same
  disk, not replication or backup.

## Component boundaries

```text
client
  |
  v
HTTP transport -----> ChannelService / ChannelGates
  |                              |
  +-----> CacheService <---------+
  |           |       |
  |           |       +---------> SpaceLedger
  |           |
  |           +-----------------> RocksMetadata
  |           +-----------------> ArtifactFiles
  |
  +-----> ProxyService -----> package upstreams
              |
              +-------------> CacheService
```

| Component | Responsibility |
| --- | --- |
| HTTP transport | Routing, channel resolution, authentication, input validation, range handling, response status, and foreground admission |
| `ChannelService` | Channel registration, authentication, policy updates, deletion, and startup recovery |
| `ChannelGates` | Per-channel fence shared by cache publication and channel deletion |
| `CacheService` | Artifact staging, publication, lookup, references, recency, eviction, and reconciliation |
| `ProxyService` | Upstream policy, redirects, revalidation, request coalescing, response rewriting, and cache passthrough |
| `SpaceLedger` | Capacity reservation and the normal/reclaiming state machine |
| `RocksMetadata` | Atomic metadata transitions and ordered eviction queues |
| `ArtifactFiles` | Temporary files, content-addressed final paths, fsync, rename, streaming reads, and channel directory deletion |

HTTP handlers do not manipulate RocksDB or artifact files directly. Cache mutations pass
through `CacheService`, and metadata transitions are expressed as complete RocksDB
operations rather than general key/value access.

## Request and channel model

### Route forms

The data route tree is mounted in two forms:

```text
/<data-route>
/channels/{channel}/<data-route>
```

Bare routes resolve to the Default Channel. The explicit form resolves and authorizes the
channel named in the path. These two requests therefore address the same object:

```text
/artifacts/sha256/{digest}
/channels/00000000000000000000000000/artifacts/sha256/{digest}
```

The alias applies to artifacts, references, build-cache keys, recency, eviction state, and
package-proxy request flights. It is not a copy or fallback between two stores. Both route
forms produce the same `ChannelId` before reaching the cache.

`ChannelContext` also records which route form arrived. PyPI, npm, and Cargo responses can
contain links back through Flywheel, so generated links retain the incoming form. A bare
request receives bare links; a channel-prefixed request receives links with the same channel
prefix.

### Channel record

Every persisted channel record contains:

```text
id:              canonical uppercase ULID
access:          Open | Token(SHA-256 token digest)
expiry_seconds:  positive integer
state:           active | deleting
created_at:      Unix timestamp in seconds
```

Channel access is fixed when the channel is registered:

- An open channel has no credential. Its data and management routes are available to any
  caller that can reach the service.
- A protected channel returns a random token once at registration. Only its SHA-256 digest
  is persisted, and token comparison is constant-time.

Protected requests accept the token as an HTTP Bearer credential or as the password in
HTTP Basic authentication. Cargo registry requests may use Cargo's native raw
`Authorization` token form. Data access, inspection, expiry updates, and deletion all use
the same channel credential. `POST /channels` is intentionally unauthenticated.

### Default Channel

The nil ULID is reserved as the Default Channel:

```text
00000000000000000000000000
```

Its invariants are stricter than those of an ordinary channel:

- It is always open and active.
- It cannot be deleted.
- It exists durably before the service begins listening.
- `default_expiry_seconds` seeds it only when it is first created.
- Its expiry can be patched and is not reset during a later startup.

Ordinary channel generation explicitly rejects this reserved ID.

Startup validates an existing Default Channel rather than repairing it. A persisted record
with protected access or a non-active lifecycle state prevents startup.

Bare route resolution can use the known open Default Channel without reading the channel
registry. This is an authentication optimization only. Every final mutation still checks
the persisted lifecycle state under the channel fence.

### Lifecycle and deletion

Ordinary channels have one forward lifecycle:

```text
absent --register--> active --delete--> deleting --cleanup--> absent
```

Deletion uses a persisted `deleting` state so it can resume after a crash:

1. Authenticate the caller and acquire a shared channel lease.
2. Persist `active -> deleting` synchronously.
3. Release the lease and acquire the channel gate exclusively.
4. Delete the channel's artifact, reference, and eviction key ranges.
5. Delete `artifacts/<channel-id>/`.
6. Delete the channel registry record and discard the in-memory gate.

New requests see a deleting channel as not found. Uploads may stage without holding the
gate, but the final write takes the gate shared and verifies that the channel is still
active. If publication wins the gate race, deletion subsequently removes the new data. If
deletion wins, publication fails without recreating the channel.

The Default Channel deletion check lives in `ChannelService`, so it applies independently
of the HTTP transport.

## Startup and readiness

`Flywheel::open` completes the following sequence before it returns an application router:

1. Validate configuration and create the data directory.
2. Open the channel-only RocksDB metadata store.
3. Open the artifact filesystem root.
4. Construct the shared channel gate registry and channel service.
5. Atomically create the Default Channel if absent, or validate its invariants if present.
6. Resume every ordinary channel left in `deleting`.
7. Construct capacity accounting and cache services.
8. Remove abandoned temporary files and perform bounded orphan reconciliation.
9. Construct the package proxy and foreground admission semaphore.

The process binds its listener only after this sequence succeeds. Runtime readiness checks
RocksDB health, the artifact root, and free-space observation. A failed free-space reading
makes the replica not ready and closes write admission until observation recovers.

## Persisted storage

### Store format

The metadata format marker is:

```text
flywheel-channel-cache-v1
```

Every serialized record starts with schema version byte `1`, followed by a Postcard
payload. An unknown format marker or record version is rejected. There is no migration,
legacy layout detection, or compatibility read path.

RocksDB uses these column families and keys:

| Column family | Key bytes | Value |
| --- | --- | --- |
| `meta` | `store-format` | Store format marker |
| `channels` | 26-byte channel ID | Versioned channel record |
| `artifacts` | Channel ID + 32-byte digest | Versioned artifact metadata |
| `references` | Channel ID + UTF-8 reference | Versioned artifact binding |
| `eviction` | Channel ID + 8-byte big-endian eligibility time + digest | Stored byte count |

The canonical 26-byte channel ID is the first component of every channel-scoped key.
Range deletion and ordered maintenance rely on that property.

### Artifact files

The filesystem layout is:

```text
<data-dir>/
  metadata/                       RocksDB files
  artifacts/
    <channel-id>/
      tmp/
        <upload-ulid>.part
      sha256/
        <digest[0..2]>/
          <digest[2..4]>/
            <full-digest>
```

There is no unscoped artifact directory. Reconciliation parses each top-level artifact
directory it encounters as a canonical channel ID; an invalid directory name is an error
rather than data assigned to an implicit scope.

## Artifact publication

### Staging and admission

Each admitted upload stages its own body. Concurrent writes of identical bytes may perform
duplicate staging work, but content addressing deduplicates their final file.

Publication proceeds in this order:

1. Resolve and authorize the channel.
2. Acquire a foreground permit.
3. Reserve disk capacity before consuming the body. Known lengths reserve up front;
   unknown lengths grow the reservation in configured extents.
4. Stream to a channel-local temporary file while hashing the logical bytes and enforcing
   `max_upload_bytes`.
5. Finish the selected on-disk encoding and verify the computed digest when the route
   supplied one.
6. Acquire the channel gate shared and recheck that the channel is active.
7. Acquire the transition stripe for the channel and artifact identity.
8. Atomically rename the complete temporary file to its content-addressed path.
9. Commit the artifact record, eviction row, and optional reference in one RocksDB batch.
10. Convert the reservation to committed usage and return success.

The file rename happens before the metadata record becomes visible. A crash can leave a
temporary file or a final file without metadata, but it cannot expose a partial body as a
cache hit.

Transition stripes are a fixed set of mutexes selected by channel and artifact identity.
They serialize only the rename-and-metadata transition. Hashing, compression, downloads,
and file deletion remain outside the stripe.

### Encoding and durability

| Route class | Stored encoding | Commit contract |
| --- | --- | --- |
| Raw artifact | Identity | Durable |
| Bazel content-addressable store | Identity | Durable |
| Generic HTTP build cache | Zstandard | Best effort |
| Bazel action cache | Zstandard | Best effort |
| Package proxy | Identity | Best effort |

A durable publication fsyncs the body, atomically renames it, fsyncs the containing
directory, and synchronously commits the RocksDB batch before acknowledging success.
Best-effort publications skip those sync operations because clients can regenerate or
refetch the data.

The digest and logical content length always describe the uncompressed bytes. Metadata
also records the stored length so reservations, response framing, and eviction use the
actual on-disk size.

## Reads and representations

A lookup reads artifact metadata, opens the final path, and returns the open file. The
response holds no cache lock while streaming. If the body is missing, the lookup deletes
the stale metadata and reports a miss. If eviction unlinks a file after it has been opened,
the existing descriptor remains readable until the response releases it.

Identity bodies support one HTTP byte range and advertise `Accept-Ranges: bytes`.
Syntactically invalid, multipart, overflowing, or unsupported ranges are ignored and
receive a full response. A valid range that cannot overlap the body returns `416`.

Zstandard build-cache bodies have two representations:

- Clients that advertise `Accept-Encoding: zstd` receive the stored frame directly.
- Other clients receive the complete logical body decompressed while streaming.

Ranges are ignored for compressed bodies. `HEAD` always describes the negotiated full
representation and ignores `Range`.

## References and build-cache keys

References map a channel-local name to an artifact identity. Binding a reference does not
require the artifact to exist locally, and the binding does not prevent eviction. This
allows references to survive independent artifact placement in a scaled deployment, at
the cost of ordinary misses when their target is absent.

Generic HTTP keys and Bazel action-cache hashes are implemented as internal references to
content-addressed bodies. Two logical keys that produce identical bytes share one file.
Bazel CAS and raw artifact routes address the digest directly and therefore share the same
body when used in the same channel.

`POST /build-cache/prefetch` accepts a bounded list of digests and returns a framed stream.
Each entry is either a miss header or a header followed by its stored bytes. A busy or
missing entry is reported as a miss so normal cache lookup can continue. A truncated body
terminates the stream because subsequent frame boundaries would be ambiguous.

The `cacheprog` client uses this endpoint as an optimization for Go's `GOCACHEPROG`
protocol. Prefetched content is verified before it enters the helper's local cache;
correctness does not depend on the prediction or manifest.

## Package proxy

Each package request is scoped by channel and normalized to an internal proxy reference.
A fresh reference resolves to a local artifact. A miss or stale reference enters a
single-flight section keyed by channel and reference, rechecks the cache, and then fetches
upstream. Concurrent requests for the same channel and package coordinate one upstream
fetch; requests in different channels do not.

Upstream redirects are followed only to configured or protocol-approved origins. Incoming
client credentials are not forwarded upstream. Successful bodies are published through
`CacheService`; if capacity admission fails, the untouched upstream stream passes directly
to the client without a second fetch.

Python Simple API and npm metadata contain client-facing download locations. Flywheel
stores route-neutral upstream content and applies channel-aware rewriting for each
response, which preserves bare and channel-prefixed route forms without duplicating the
cached package body.

Python HTML and JSON representations are negotiated and cached independently. HTML is
parsed before anchor and provenance attributes are changed; JSON rewriting changes only
distribution and provenance URL fields. Distribution hash fragments remain visible to the
client. Core metadata and signature suffixes are translated back to their upstream
companion files when the client follows a rewritten link.

npm full metadata and abbreviated install metadata are also separate cache variants.
Tarball fields are rewritten recursively, including scoped packages. Cargo's generated
index configuration points crate downloads back through Flywheel and advertises whether
the selected channel requires authentication. Go module responses and Cargo sparse index
entries require no payload transformation.

The proxy boundary is package acquisition: Go module reads, the Python Simple API, npm
install metadata and tarballs, and Cargo sparse reads. Publishing, account management,
registry search, npm audit, Cargo web APIs, direct VCS access, and checksum databases are
not proxied.

## Capacity and maintenance

`SpaceLedger` tracks one coherent capacity snapshot:

```text
admissible bytes = observed free space
                 - in-flight reservations
                 - bytes committed since observation
                 - emergency headroom
```

The snapshot is protected by one mutex so a free-space refresh cannot lose a concurrent
commit. A reservation is released only after a temporary file is known to be absent.
Cancellation and cleanup failures are accounted as committed until a later filesystem
observation corrects the conservative estimate.

The controller starts reclaiming when free space reaches the low watermark and remains in
that state until free space reaches the high watermark. Under pressure:

- Raw artifact and Bazel CAS writes return `507 Insufficient Storage`.
- Generic and Bazel action-cache writes report protocol-compatible success without storing.
- Package responses stream from upstream without being cached.

Every artifact receives a soft eviction deadline based on its channel's persisted
`expiry_seconds`. Maintenance visits all active channels, including the Default Channel,
using ordered eviction keys and a shared candidate and byte budget. The starting channel
rotates between passes to avoid starvation.

In normal operation, an eligible artifact seen in the recent-use Bloom filters receives a
new soft deadline. Under disk pressure, maintenance treats the oldest queue heads as
immediately eligible regardless of recency or deadline, reclaiming them toward the high
watermark. References do not affect either decision. This contract is recorded in
[ADR 0001](adr/0001-keep-recency-aware-eviction.md).

## Recovery and failure model

Startup removes temporary files and inspects up to `orphan_scan_limit` final files. A final
file without metadata is deleted. Metadata without a file is repaired lazily on the next
read or eviction. Channels persisted as `deleting` complete deletion before startup
finishes.

| Failure point | Observable result |
| --- | --- |
| Before final rename | No hit; startup removes the temporary file |
| After rename, before metadata commit | No hit; bounded reconciliation removes the orphan file if encountered |
| Metadata exists but body is missing | Next read or eviction removes stale metadata and reports a miss |
| Process crash after durable acknowledgement | Raw or CAS body and metadata remain on the same disk |
| Process crash after best-effort acknowledgement | The next request may hit or may self-heal to a miss |
| Disk loss | All data on that replica is lost |
| Crash during ordinary channel deletion | The persisted `deleting` state resumes on startup |

## Scaled deployment boundary

The optional `flywheel agent` places bare-route traffic across replicas with a consistent
hash derived from the route's semantic identity. Raw artifacts and Bazel CAS objects with
the same digest select the same owner. The agent does not replicate data, retry a failed
request on a new owner, or provide a shared channel registry.

For that reason, the agent supports only bare routes, which select the Default Channel.
Channel management and every `/channels/...` request return `501 Not Implemented` at the
agent boundary. Arbitrary channels require a shared control plane before they can be
correctly authorized and routed across replicas.

When no owner is available, build-cache reads degrade to misses and build-cache writes
degrade to successful bypasses. Raw artifacts, references, and package traffic return a
gateway or availability error. The agent's readiness endpoint means that the process can
serve this degradation contract; ring membership must be monitored separately.

## Source map

| Area | Primary source |
| --- | --- |
| Process wiring and startup | `src/lib.rs`, `src/main.rs` |
| HTTP API and channel resolution | `src/transport/http/` |
| Channel identity and lifecycle | `src/channel/` |
| Cache workflow and maintenance | `src/cache/service.rs` |
| Capacity accounting | `src/cache/space.rs` |
| Recency tracking | `src/cache/recent_use.rs` |
| Transition stripes | `src/cache/stripes.rs` |
| Artifact filesystem | `src/storage/local/artifact_files.rs` |
| RocksDB schema and transitions | `src/storage/metadata/rocksdb.rs` |
| Record encoding | `src/storage/records.rs` |
| Package proxy | `src/proxy/`, `src/transport/http/packages.rs` |
| Scaled routing agent | `src/agent/` |
| Go cache helper | `src/cacheprog/` |
