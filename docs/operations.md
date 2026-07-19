# Flywheel operations

This guide covers deployment, configuration, channel administration, capacity management,
monitoring, recovery, and the optional routing agent. See
[architecture.md](architecture.md) for storage and concurrency internals.

## Deployment contract

Each Flywheel replica owns one local data directory containing RocksDB metadata and
artifact files. Deploy it with these constraints:

- Give each replica its own data directory. Do not mount one directory into multiple
  Flywheel processes.
- Use a local or node-attached filesystem that supports atomic rename and fsync.
- Use persistent storage when cache warmth must survive process or pod replacement.
- Size the volume for cached bodies, RocksDB, temporary uploads, and emergency headroom.
- Put authentication and TLS termination in deployment infrastructure. Flywheel serves
  plain HTTP and channel tokens are bearer credentials.
- Run the process under a supervisor that restarts it after startup or runtime failure.

There is no remote storage tier, data replication, or supported online backup protocol.
Losing a replica's disk loses its cache. Raw artifact and Bazel CAS acknowledgements are
durable across a process crash on the same disk; build-cache and package-proxy entries are
best effort.

Flywheel has no operational mode setting. Channel management and both bare and
channel-prefixed data routes are always present.

## Starting a replica

Run a replica with a persistent data directory:

```text
RUST_LOG=info flywheel serve \
  --listen 0.0.0.0:8080 \
  --data-dir /var/lib/flywheel
```

The server and agent write structured JSON logs to stdout at `info` level by default.
`RUST_LOG` overrides the filter. The `cacheprog` subcommand writes diagnostics to stderr
because stdout carries its protocol with the Go tool. Prefetch status logs include the
session, configured download concurrency, manifest size, and lookup duration. Per-object
transfer details are available at `debug`.

At `debug`, every server and agent request emits one completion record with these stable
fields:

| Field | Meaning |
| --- | --- |
| `component` | `server` or `agent` |
| `request_id` | Caller-provided `x-request-id`, or a generated ULID |
| `method` | HTTP method |
| `operation` | Cache operation or control endpoint |
| `key` | Actual cache key, reference, artifact digest, or package-proxy key |
| `status` | Response status on the completion record |
| `latency` | Time to response headers |

The request ID is returned in the response. An agent preserves it while forwarding to the
selected ring member, so both processes can be queried with the same value. Access logs do not
include authorization headers, query strings, or response bodies. Server failures emit an
additional `error`-level record. The default `info` level
retains startup, shutdown, maintenance reclamation, ring changes, status fan-out summaries,
and cacheprog's aggregate prefetch result without emitting records for every cache object.

Pass `--debug` to enable Flywheel debug events and per-request completion logs. For finer
filtering, use `RUST_LOG=warn` to retain only failures or a directive such as
`RUST_LOG=flywheel=debug,tower_http=debug`. Collect stdout through the process supervisor or
container runtime. Collect cacheprog's stderr separately when troubleshooting the helper.
Flywheel does not rotate or retain log files.

The listener is bound only after the metadata store and artifact root open, the Default
Channel exists durably, interrupted channel deletions finish, and startup reconciliation
completes. A startup error leaves the process unavailable rather than accepting traffic in
a partially initialized state.

## Replica configuration

Every `flywheel serve` flag has the environment variable shown below. Byte values are
integer byte counts, and time values are integer seconds.

### Listener, storage, and admission

| Flag | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--listen` | `FLYWHEEL_LISTEN` | `127.0.0.1:8080` | HTTP listen address |
| `--data-dir` | `FLYWHEEL_DATA_DIR` | `./flywheel-data` | RocksDB and artifact root |
| `--max-upload-bytes` | `FLYWHEEL_MAX_UPLOAD_BYTES` | 10 GiB | Maximum logical body size |
| `--default-expiry-seconds` | `FLYWHEEL_DEFAULT_EXPIRY_SECONDS` | 604800 (7 days) | Default for new channels and initial Default Channel creation |
| `--foreground-concurrency` | `FLYWHEEL_FOREGROUND_CONCURRENCY` | 256 | Concurrent data operations and active response streams |
| `--reservation-extent-bytes` | `FLYWHEEL_RESERVATION_EXTENT_BYTES` | 64 MiB | Capacity increment for uploads without a known length |

`default_expiry_seconds` does not overwrite a channel's persisted setting. This includes a
Default Channel expiry that was patched before restart.

### Capacity and maintenance

| Flag | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--low-watermark-bytes` | `FLYWHEEL_LOW_WATERMARK_BYTES` | 2 GiB | Observed free space below which reclamation starts |
| `--high-watermark-bytes` | `FLYWHEEL_HIGH_WATERMARK_BYTES` | 8 GiB | Observed free space at which reclamation stops |
| `--emergency-headroom-bytes` | `FLYWHEEL_EMERGENCY_HEADROOM_BYTES` | 1 GiB | Free space withheld from artifact reservations |
| `--bloom-bits` | `FLYWHEEL_BLOOM_BITS` | 1,048,576 | Size of each approximate recent-use filter |
| `--reclaim-candidate-limit` | `FLYWHEEL_RECLAIM_CANDIDATE_LIMIT` | 256 | Maximum queue candidates examined per pass |
| `--reclaim-byte-limit` | `FLYWHEEL_RECLAIM_BYTE_LIMIT` | 8 GiB | Maximum stored bytes reclaimed per pass |
| `--orphan-scan-limit` | `FLYWHEEL_ORPHAN_SCAN_LIMIT` | 4096 | Maximum final files checked during startup reconciliation |

The high watermark must be at least the low watermark. The emergency headroom is always
subtracted from capacity available to new bodies, so it must leave enough space for the
largest expected upload reservation as well as RocksDB growth.

### Package proxy

| Flag | Environment variable | Default |
| --- | --- | --- |
| `--go-upstream` | `FLYWHEEL_GO_UPSTREAM` | `https://proxy.golang.org/` |
| `--python-upstream` | `FLYWHEEL_PYTHON_UPSTREAM` | `https://pypi.org/simple/` |
| `--npm-upstream` | `FLYWHEEL_NPM_UPSTREAM` | `https://registry.npmjs.org/` |
| `--cargo-index-upstream` | `FLYWHEEL_CARGO_INDEX_UPSTREAM` | `https://index.crates.io/` |
| `--cargo-crate-upstream` | `FLYWHEEL_CARGO_CRATE_UPSTREAM` | `https://static.crates.io/crates/` |
| `--proxy-revalidation-seconds` | `FLYWHEEL_PROXY_REVALIDATION_SECONDS` | 300 |
| `--proxy-concurrency` | `FLYWHEEL_PROXY_CONCURRENCY` | 64 |
| `--upstream-timeout-seconds` | `FLYWHEEL_UPSTREAM_TIMEOUT_SECONDS` | 30 |
| `--proxy-allowed-origin` | `FLYWHEEL_PROXY_ALLOWED_ORIGINS` | None |

`--proxy-allowed-origin` is repeatable. The environment variable accepts a comma-separated
list of `http://` or `https://` origins. Add an origin when a configured registry redirects
downloads to a host outside that protocol's built-in public origins. A path in this setting
does not broaden trust beyond its scheme, host, and port.

Flywheel does not forward a client's `Authorization` header to package upstreams. The
configured upstreams must therefore be reachable without the caller's channel credential.

## Health and process lifecycle

| Endpoint | Success | Meaning |
| --- | --- | --- |
| `GET /health/live` | `200` | The HTTP process can answer requests |
| `GET /health/ready` | `200` | RocksDB and artifact storage are healthy and free-space observation is available |
| `GET /metrics` | `200` | Prometheus text metrics |

Readiness returns `503` if RocksDB health, artifact-root health, or free-space observation
fails. When free space cannot be observed, new reservations fail closed until a maintenance
refresh succeeds.

The server performs graceful shutdown on `SIGINT`: Axum stops accepting work, the
maintenance worker stops, and one final maintenance pass runs. The current binary does not
install a separate `SIGTERM` handler; termination by another signal relies on the normal
crash-recovery guarantees at the next startup.

## HTTP route forms

Every data route is available in both forms:

```text
http://host:8080/<data-route>
http://host:8080/channels/<channel-id>/<data-route>
```

Bare routes select the open Default Channel:

```text
00000000000000000000000000
```

An explicitly Default-prefixed route is an alias for the same data, not a separate cache.
Custom channels are isolated from the Default Channel and from each other.

The route families are:

| Route | Methods | Use |
| --- | --- | --- |
| `/artifacts/{algorithm}/{digest}` | `GET`, `HEAD`, `PUT` | Raw content-addressed artifacts |
| `/references/{reference}` | `GET`, `PUT`, `DELETE` | Mutable artifact bindings |
| `/build-cache/http/{key}` | `GET`, `HEAD`, `PUT` | Generic HTTP build cache |
| `/build-cache/bazel/ac/{hash}` | `GET`, `HEAD`, `PUT` | Bazel action cache |
| `/build-cache/bazel/cas/{hash}` | `GET`, `HEAD`, `PUT` | Bazel content-addressable store |
| `/proxy/go/{path}` | `GET` | Go module proxy |
| `/proxy/python/simple/` and `/proxy/python/simple/{project}/` | `GET` | Python Simple Repository API |
| `/proxy/python/files/{target}` | `GET` | Python distribution, metadata, signature, or provenance download |
| `/proxy/npm/{package}` | `GET` | npm package metadata and rewritten tarball download |
| `/proxy/cargo/index/config.json` | `GET` | Cargo registry configuration |
| `/proxy/cargo/index/{path}` | `GET` | Sparse Cargo index |
| `/proxy/cargo/crates/{crate}/{version}/download` | `GET` | Cargo crate download |

Only canonical lowercase SHA-256 digests are supported for artifact identities. Reference
names and build-cache keys must be URL-safe.

One root route exists outside the channel tree: `GET /status?session=<label>` returns the
`cacheprog` session manifest a replica holds locally in the Default Channel, always as
`200` JSON, degrading to an empty manifest when the session is unknown or the stored body
is unreadable. A missing `session` parameter is a `400`. The routing agent serves the same
route by fanning the request out to every ring member and merging the answers; prefetch
discovery uses it and it carries no member or completeness information.

### Common response statuses

| Status | Meaning |
| --- | --- |
| `400 Bad Request` | Invalid channel ID, artifact identity, reference, channel patch, or missing status session parameter |
| `401 Unauthorized` | Missing or incorrect protected-channel credential |
| `403 Forbidden` | Package redirect or encoded download targets a disallowed origin |
| `404 Not Found` | Cache miss, absent channel, or channel in `deleting` |
| `409 Conflict` | Raw body digest mismatch or attempt to delete the Default Channel |
| `413 Payload Too Large` | Upload exceeded `max_upload_bytes` |
| `429 Too Many Requests` | Foreground or package-upstream concurrency is exhausted |
| `500 Internal Server Error` | Local storage or metadata operation failed |
| `502 Bad Gateway` | Package upstream failed, or a routing-agent backend send failed |
| `503 Service Unavailable` | Readiness failed, or the routing agent has no owner |
| `507 Insufficient Storage` | Raw artifact or Bazel CAS capacity admission failed |

Errors from channel management use a JSON body with `code` and `message` fields. Data and
protocol routes may return an empty body when their client protocol does not use that error
envelope.

## Channel administration

### Default Channel

Inspect the Default Channel explicitly:

```text
curl -sS http://127.0.0.1:8080/channels/00000000000000000000000000
```

It is always open and active. Its expiry can be changed:

```text
curl -sS -X PATCH \
  -H 'Content-Type: application/json' \
  -d '{"expiry_seconds":1209600}' \
  http://127.0.0.1:8080/channels/00000000000000000000000000
```

Deleting it returns `409 Conflict`. Startup also refuses a persisted Default Channel that
is protected or not active.

### Register an open channel

`POST /channels` is unauthenticated. Register an open channel with:

```text
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"access_control":false,"expiry_seconds":86400}' \
  http://127.0.0.1:8080/channels
```

The response is `201 Created` and omits `token`:

```json
{
  "channel": "01J2Y8J4F6H8K90M1N2P3Q4R5S",
  "access_control": false,
  "expiry_seconds": 86400,
  "state": "active"
}
```

An open ordinary channel has no administrative protection. Any caller that can reach it
can inspect, modify its expiry, write data, or delete it.

### Register a protected channel

```text
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"access_control":true,"expiry_seconds":86400}' \
  http://127.0.0.1:8080/channels
```

The response includes the token once:

```json
{
  "channel": "01J2Y8J4F6H8K90M1N2P3Q4R5S",
  "token": "flywheel_<secret>",
  "access_control": true,
  "expiry_seconds": 86400,
  "state": "active"
}
```

Store the token immediately in a secret manager. Flywheel persists only its digest and
cannot display or recover it later. There is no token rotation or access-mode change; create
a replacement channel when either is required.

Use the token on every data and management request:

```text
curl -sS \
  -H 'Authorization: Bearer flywheel_<secret>' \
  http://127.0.0.1:8080/channels/01J2Y8J4F6H8K90M1N2P3Q4R5S/build-cache/http/example
```

HTTP Basic authentication is also accepted, with the channel token as the password. This
is useful for clients that can send Basic credentials but cannot set a Bearer header.

### Inspect, update, and delete

| Operation | Request | Authentication |
| --- | --- | --- |
| Inspect | `GET /channels/{id}` | Required for a protected channel |
| Change retention | `PATCH /channels/{id}` with `{"expiry_seconds": N}` | Required for a protected channel |
| Delete | `DELETE /channels/{id}` | Required for a protected channel |

`expiry_seconds` must be positive. Unknown patch fields, including `access_control`, return
`400 Bad Request`.

Deletion persists the channel as `deleting`, fences final writes, removes all channel data,
and removes the registry record. The request returns `202 Accepted` after that cleanup has
completed. If the process stops mid-delete, startup resumes the same sequence. A deleting
channel is presented to clients as `404 Not Found`.

There is no channel-list endpoint. Keep channel IDs and protected tokens in the system that
provisions them.

### Security checklist

- Restrict `POST /channels` at the network or reverse-proxy boundary if arbitrary callers
  must not allocate storage. Flywheel has no registry-wide administrator credential.
- Do not use open ordinary channels for data that requires write or deletion protection.
- Terminate TLS before traffic crosses an untrusted network; channel tokens grant full
  access to one channel.
- Keep protected tokens in secret storage and send them in an authorization header, not a
  query parameter.
- Restrict access to `/metrics` and health endpoints if deployment metadata must remain
  private; these endpoints have no application authentication.
- Grant the Flywheel process exclusive filesystem permissions on its data directory.
- Keep `proxy_allowed_origins` narrow. Each entry authorizes package downloads from the
  entire origin and is part of the proxy's server-side request policy.

## Client configuration overview

Flywheel serves several independent cache protocols. Go's `cacheprog` integration is one
client, not the deployment model for the service as a whole.

| Workload | Flywheel interface | Client configuration |
| --- | --- | --- |
| Arbitrary content-addressed data | Raw artifacts and references | HTTP `GET`, `HEAD`, `PUT`, and `DELETE` |
| Any tool with an HTTP key/value cache | Generic build cache | Base URL ending in `/build-cache/http/` |
| Bazel | HTTP remote cache | `--remote_cache=.../build-cache/bazel` |
| Go build outputs | Generic build cache through `flywheel cacheprog` | `GOCACHEPROG` |
| Go modules | Go module proxy | `GOPROXY` |
| Python packages | HTML and JSON Simple Repository API | `PIP_INDEX_URL` or pip's `index-url` |
| npm packages | Install metadata and tarballs | `NPM_CONFIG_REGISTRY` or `.npmrc` |
| crates.io packages | Cargo sparse registry mirror | Cargo source replacement |

These clients retain their own local caches. Flywheel adds a shared cache in front of the
configured package upstreams or behind a build tool; it does not replace a package
manager's lockfile, checksum verification, or local cache.

The examples below use the open Default Channel. To select an ordinary channel, insert
`/channels/{channel-id}` before the data route. Protected channels additionally require a
client that can send the token as a Bearer token or as the password in HTTP Basic
authentication; Cargo may send its registry token without a scheme. The scaled routing
agent supports only bare Default Channel routes.

## Raw artifacts and references

Upload a raw body whose URL contains its SHA-256 digest:

```text
curl -sS -X PUT \
  --data-binary @artifact.tar \
  http://127.0.0.1:8080/artifacts/sha256/<digest>
```

A new body returns `201 Created`; an already present body returns `204 No Content`. A body
that does not match the URL digest returns `409 Conflict`.

Bind a reference without pinning the artifact:

```text
curl -sS -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"algorithm":"sha256","digest":"<digest>"}' \
  http://127.0.0.1:8080/references/release-latest
```

Reference lookup returns the same JSON identity. It does not stream the target body. A
binding can outlive or point to an absent local body, so consumers must treat it as a
cache-level hint rather than permanent object ownership.

Identity bodies support a single HTTP byte range. Zstandard build-cache bodies do not;
clients that send `Accept-Encoding: zstd` receive the stored frame, and other clients
receive decompressed logical bytes.

## Generic HTTP build cache

Tools without a dedicated adapter can use an opaque, URL-safe key under the generic HTTP
cache:

```text
PUT  http://127.0.0.1:8080/build-cache/http/<key>
HEAD http://127.0.0.1:8080/build-cache/http/<key>
GET  http://127.0.0.1:8080/build-cache/http/<key>
```

`PUT` accepts the logical body and returns success even when disk pressure makes the write
bypass storage. A later `GET` may therefore miss. Clients must treat `404` as a cache miss,
must be able to recompute an absent value, and must not use this route as artifact storage.
The response `ETag` identifies the logical body with SHA-256.

## Bazel remote cache

Flywheel implements Bazel's HTTP action-cache and CAS layout. Configure the base path in a
workspace or CI `.bazelrc`:

```text
build --remote_cache=http://127.0.0.1:8080/build-cache/bazel
```

Bazel appends `/ac/{hash}` for action results and `/cas/{hash}` for content. To allow cache
reads without publishing local results, add:

```text
build --remote_upload_local_results=false
```

For a protected channel, Bazel can use HTTP Basic authentication in an HTTPS URL. Flywheel
ignores the Basic username and treats the password as the channel token:

```text
build --remote_cache=https://flywheel:flywheel_<secret>@cache.example.com/channels/<channel-id>/build-cache/bazel
```

Inject that setting from CI secret storage rather than committing it to `.bazelrc` or
printing it in logs. Bazel CAS writes use Flywheel's durable publication contract and fail
under pressure; action-cache writes are best effort and may be accepted without storage.
See Bazel's [remote caching documentation](https://bazel.build/remote/caching) for the
client-side HTTP cache contract and additional build flags.

## Package managers

Use the appropriate bare Default Channel base URL:

| Client | Flywheel endpoint |
| --- | --- |
| Go modules | `http://host:8080/proxy/go` |
| pip / Python index | `http://host:8080/proxy/python/simple/` |
| npm registry | `http://host:8080/proxy/npm/` |
| Cargo sparse registry | `sparse+http://host:8080/proxy/cargo/index/` |

Configure each client rather than rewriting individual dependency URLs.

### Package protocol contract

Flywheel implements the package-acquisition surface used by each configured client. It is
not a publishing service or a general-purpose registry API.

| Ecosystem | Supported through Flywheel | Outside the service boundary |
| --- | --- | --- |
| Go | Module version lists, version information, `go.mod` files, module archives, and the optional latest-version endpoint | Checksum database and direct VCS access |
| Python | Simple API v1 HTML and JSON responses, distribution files, core metadata sidecars, signatures, and provenance | Upload and project-management APIs |
| npm | Full and abbreviated install metadata, scoped packages, and tarballs | Publish, account, search, and audit APIs |
| Cargo | Sparse index configuration, crate index entries, and crate downloads | Git-index protocol, publishing, and registry web APIs |

Disable npm's registry audit call for jobs that use Flywheel as their registry and run the
security audit as a separate pipeline step against an advisory service:

```ini
audit=false
```

### Go modules

Point `GOPROXY` at Flywheel. The absence of a fallback entry ensures module paths are not
sent to another proxy when Flywheel misses or fails:

```text
GOPROXY=http://127.0.0.1:8080/proxy/go
```

If direct fallback is intentional, append it using the Go tool's normal comma or pipe
semantics. This module proxy is independent of `GOCACHEPROG`: the former caches downloaded
dependencies, while the latter shares compiled build outputs. The Go
[module reference](https://go.dev/ref/mod#environment-variables) defines `GOPROXY` fallback
and privacy behavior.

### pip

Set the simple-index base URL through the environment:

```text
PIP_INDEX_URL=http://127.0.0.1:8080/proxy/python/simple/
```

The equivalent pip configuration file is:

```ini
[global]
index-url = http://127.0.0.1:8080/proxy/python/simple/
```

Flywheel negotiates `application/vnd.pypi.simple.v1+json`,
`application/vnd.pypi.simple.v1+html`, and legacy `text/html` responses. HTML and JSON are
cached separately. Project-list links remain under the Simple API route; distribution
links are rewritten through the file route so wheels and source archives are cached.
Rewriting retains the distribution hash and also routes core metadata, signatures, and
provenance through the same channel.

Do not add the public index as `extra-index-url` unless dependency-confusion behavior
across two indexes is explicitly acceptable. The [Simple Repository API
specification](https://packaging.python.org/en/latest/specifications/simple-repository-api/)
defines content negotiation and companion-file behavior. pip documents environment and
file-based settings in its [configuration
guide](https://pip.pypa.io/en/stable/topics/configuration/).

### npm

Set the registry for one process or Pod:

```text
NPM_CONFIG_REGISTRY=http://127.0.0.1:8080/proxy/npm/
```

For a project-level `.npmrc`, use:

```ini
registry=http://127.0.0.1:8080/proxy/npm/
```

Flywheel honors npm's `Accept` negotiation for full metadata and the abbreviated
`application/vnd.npm.install-v1+json` representation. Each representation has a separate
cache entry. Scoped package names are supported, and tarball URLs are rewritten so
downloads retain the incoming bare or channel-prefixed route form.

The adapter is read-only and limited to package acquisition. Publishing, registry
management, search, and audit endpoints are not supported. See npm's
[registry](https://docs.npmjs.com/using-npm/registry.html/) and
[`.npmrc`](https://docs.npmjs.com/cli/v11/configuring-npm/npmrc/) documentation for
configuration precedence and credential scoping.

### Cargo

Use source replacement because Flywheel mirrors crates.io rather than hosting an unrelated
private registry. In `.cargo/config.toml`:

```toml
[source.crates-io]
replace-with = "flywheel"

[source.flywheel]
registry = "sparse+http://127.0.0.1:8080/proxy/cargo/index/"
```

The trailing slash is required by Cargo's sparse registry URL format. Flywheel's generated
`config.json` directs crate downloads back through `/proxy/cargo/crates`. Cargo documents
this mirror pattern under
[source replacement](https://doc.rust-lang.org/cargo/reference/source-replacement.html).

For a protected channel, configure a Cargo credential provider to return the channel token
for the Flywheel registry. Cargo sends that token as the complete `Authorization` header;
Flywheel accepts this native Cargo form in addition to Bearer and Basic credentials. The
same token is then sent for the sparse index and crate downloads because the generated
configuration sets `auth-required`.

For a custom channel, insert `/channels/{channel-id}` before `/proxy`. Package index and
metadata rewrites retain that prefix. Protected channels require package-manager-specific
credential configuration. Validate both index or metadata requests and rewritten
downloads before enabling a protected package channel.

Package misses use one upstream request per channel and package reference. After
`proxy_revalidation_seconds`, Flywheel revalidates cached metadata. Redirects to unapproved
origins return `403 Forbidden`. Upstream concurrency exhaustion returns `429 Too Many
Requests` with `Retry-After: 1`.

## Go build cache with `cacheprog`

`flywheel cacheprog` adapts Go's `GOCACHEPROG` protocol to the generic HTTP cache. Configure
the Default Channel with:

```text
GOCACHEPROG='flywheel cacheprog --url http://127.0.0.1:8080/build-cache/http/'
```

For a protected channel, use its prefixed URL and token:

```text
GOCACHEPROG='flywheel cacheprog \
  --url http://127.0.0.1:8080/channels/01J2Y8J4F6H8K90M1N2P3Q4R5S/build-cache/http/ \
  --token flywheel_<secret>'
```

| Flag | Environment variable | Behavior |
| --- | --- | --- |
| `--url` | `FLYWHEEL_CACHEPROG_URL` | Generic HTTP build-cache base URL |
| `--token` | `FLYWHEEL_CACHEPROG_TOKEN` | Protected channel credential |
| `--cache-dir` | `FLYWHEEL_CACHEPROG_DIR` | Parent of the persistent `flywheel-cacheprog/` directory |
| `--ephemeral-cache` | `FLYWHEEL_CACHEPROG_EPHEMERAL_CACHE` | Use a fresh local cache for this process and delete it on exit |
| `--session` | `FLYWHEEL_SESSION` | Stable label for the prefetch manifest |
| `--prune-days` | `FLYWHEEL_CACHEPROG_PRUNE_DAYS` | Local object age limit; default 14, `0` disables pruning |
| `--prefetch-concurrency` | `FLYWHEEL_CACHEPROG_PREFETCH_CONCURRENCY` | Bound on parallel prefetch downloads; default 8, `0` disables prefetch |

Without `--cache-dir`, the helper uses the system temporary directory. A reusable CI
volume substantially improves warm builds. The helper verifies digests before publishing a
local object. Prefetch and its manifest are optimizations; misses fall back to ordinary Go
cache behavior.

Use `--ephemeral-cache` to prevent reuse between cacheprog processes. The helper creates a
fresh child under `--cache-dir` (or the system temporary directory), uses it normally for
the process, and deletes it after close.

At startup the helper asks `GET /status?session=<label>` at its configured cache origin
for the session manifest, then warms its object directory with parallel ordinary cache
GETs bounded by `--prefetch-concurrency`. A foreground Go request for an object the pool
is currently downloading waits for that download instead of fetching the same bytes
twice; if the download fails, the request falls back to its ordinary fetch. Against a single replica, the replica answers
its own status route; in a replicated deployment, point `--url` at a pod-local agent so
the agent merges the manifest across shards and routes each download to its owner.
`--prefetch-concurrency 0` is the rollback switch: it disables all prefetch traffic while
leaving foreground caching and the manifest update at close untouched. The helper logs one
summary line per session with advertised, locally present, attempted, downloaded, and
missed objects, bytes, duration, and peak concurrency.

The default session label is the Go module path plus `GOOS` and `GOARCH`, falling back to
the working directory. Set an explicit session when checkout paths change or when jobs for
the same module should maintain separate predictions.

### Persisting the local cache on a Kubernetes node

`--cache-dir` names a parent directory. The helper creates a
`flywheel-cacheprog/` child containing verified, content-named objects written by atomic
rename. The session manifest is stored remotely under the selected build-cache URL;
`--session` does not create a second local directory.

The sidecar example later in this guide uses `emptyDir`, which survives container restarts
but disappears with the Pod. A trusted, node-local CI worker can instead mount a dedicated
host directory into the build container. Prepare the parent on every eligible node using
the build container's UID and GID; this example uses `1000:1000`:

```text
sudo install -d -o 1000 -g 1000 -m 0750 /var/lib/flywheel-cacheprog
```

Mount only that directory and pass the mounted parent to `cacheprog`:

```yaml
spec:
  containers:
    - name: build
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
      env:
        - name: GOCACHEPROG
          value: >-
            flywheel cacheprog
            --url http://127.0.0.1:9080/build-cache/http/
            --cache-dir /var/cache/flywheel
            --session ci-linux-amd64
            --prune-days 14
      volumeMounts:
        - name: cacheprog-local
          mountPath: /var/cache/flywheel
  volumes:
    - name: cacheprog-local
      hostPath:
        path: /var/lib/flywheel-cacheprog
        type: Directory
```

The resulting object directory on the node is
`/var/lib/flywheel-cacheprog/flywheel-cacheprog/`. Use `Directory`, not
`DirectoryOrCreate`: kubelet-created directories are owned according to kubelet defaults
and are commonly unwritable by a non-root build UID. Do not work around that by making the
build container privileged or mounting a broad host path.

A `hostPath` cache is local to one node. Provision the directory on every CI node and
expect a cold local cache when a Pod moves; use a local PersistentVolume or another
operator-managed volume when direct `hostPath` access is prohibited. Kubernetes does not
account `hostPath` usage as Pod ephemeral storage, so monitor and alert on the node
filesystem separately. Kubernetes' [`hostPath` documentation](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath)
also describes its security and ownership risks.

The directory may expose compiled objects between workloads that land on the same node.
Use a dedicated node pool and directory per trust boundary. If concurrent Pods share one
directory, disable helper-driven object pruning with `--prune-days 0` and perform bounded
node maintenance while builds are drained; otherwise independent helpers can prune files
another build is about to consume. Abandoned temporary files are still removed by the
helper.

## Capacity, pressure, and retention

The capacity controller reserves against:

```text
observed free - in-flight reservations - newly committed bytes - emergency headroom
```

This prevents concurrent uploads from spending the same free bytes. Bodies with no known
content length reserve in `reservation_extent_bytes` increments. Foreground permits remain
held while response bodies stream, so slow clients count against
`foreground_concurrency`.

When free space drops below the low watermark, maintenance enters reclaiming mode. It runs
bounded passes back to back until the high watermark is restored. If a pass cannot reclaim
anything, it waits one second before trying again. Outside pressure, maintenance runs every
30 seconds.

| Condition | Client behavior |
| --- | --- |
| Body exceeds `max_upload_bytes` | `413 Payload Too Large` |
| Foreground budget exhausted | `429 Too Many Requests`, `Retry-After: 1` |
| Raw artifact or Bazel CAS cannot reserve space | `507 Insufficient Storage`, `Retry-After: 1` |
| Generic HTTP or Bazel action-cache cannot reserve space | Successful write response, body not stored |
| Package body cannot reserve space | Upstream response streams to the client without caching |
| Free-space observation fails | Readiness `503`; all new reservations refused |

Channel expiry is a soft retention interval, not a guaranteed minimum lifetime. During
normal maintenance, an eligible artifact read recently is requeued using the channel's
current persisted expiry. Under pressure, maintenance treats the oldest queue heads as
immediately eligible and may reclaim them before their deadline. References never pin
bodies.

Patching `expiry_seconds` affects new publications and later requeues. Existing queue rows
keep their current soft deadline until maintenance examines them; lowering expiry does not
perform an immediate channel-wide reschedule.

## Metrics and alerts

Replica metrics at `/metrics` are process-wide and have no channel label.

| Metric | Type | Meaning |
| --- | --- | --- |
| `flywheel_requests_total` | Counter | All HTTP requests, including health and metrics |
| `flywheel_http_request_duration_seconds` | Histogram | Time to response headers, labeled by method, normalized route, and status |
| `flywheel_artifact_hits_total` | Counter | Successful artifact locations |
| `flywheel_artifact_misses_total` | Counter | Artifact locations that missed |
| `flywheel_bytes_read_total` | Counter | Full logical body size attributed to each hit, not wire bytes |
| `flywheel_bytes_written_total` | Counter | Logical bytes fully staged, including a later failed commit |
| `flywheel_authorization_denials_total` | Counter | Protected channel credential failures |
| `flywheel_maintenance_reclaimed_total` | Counter | Artifacts reclaimed by maintenance |
| `flywheel_maintenance_requeued_total` | Counter | Recently used artifacts given a new deadline |
| `flywheel_build_cache_bypasses_total` | Counter | Build-cache writes accepted without storage under pressure |
| `flywheel_raw_pressure_errors_total` | Counter | Raw or CAS writes rejected for capacity |
| `flywheel_foreground_concurrency_limit` | Gauge | Configured foreground admission limit |
| `flywheel_foreground_in_flight` | Gauge | Admitted operations and active response streams |
| `flywheel_foreground_rejections_total` | Counter | Requests rejected because foreground admission was exhausted |
| `flywheel_prefetch_requests_total` | Counter | Prefetch object requests received by the shard |
| `flywheel_prefetch_responses_total` | Counter | Prefetch response headers, labeled `hit`, `miss`, or `unavailable` |
| `flywheel_prefetch_in_flight` | Gauge | Prefetch response bodies currently streaming |
| `flywheel_prefetch_transfers_total` | Counter | Prefetch bodies labeled `completed` or `cancelled` |
| `flywheel_prefetch_response_bytes_total` | Counter | Prefetch body bytes actually streamed |
| `flywheel_prefetch_transfer_duration_seconds` | Histogram | Prefetch response-body lifetime |
| `flywheel_prefetch_status_requests_total` | Counter | Session manifest lookups answered by the shard |
| `flywheel_prefetch_status_manifest_entries` | Histogram | Entries returned by shard-local session manifests |
| `flywheel_prefetch_status_duration_seconds` | Histogram | Shard-local manifest lookup latency |
| `flywheel_free_observed_bytes` | Gauge | Last successful filesystem free-space reading |
| `flywheel_reserved_bytes` | Gauge | Capacity held by in-flight writes |
| `flywheel_committed_since_bytes` | Gauge | New bytes not yet included in the free-space reading |

Recommended alerts and dashboards:

- Alert when `/health/ready` remains non-200.
- Alert on sustained increases in build-cache bypasses or raw pressure errors.
- Compare observed free space with both watermarks and emergency headroom.
- Alert when reclamation rises without restoring free space, or pressure persists with no
  reclaimed artifacts.
- Track hit rate, byte throughput, and authorization-denial rate.
- Graph `flywheel_foreground_in_flight / flywheel_foreground_concurrency_limit` with
  `rate(flywheel_foreground_rejections_total[5m])`. A flat prefetch concurrency increase
  accompanied by foreground rejections identifies shard admission as the limit.
- Compare the prefetch in-flight gauge with transfer-duration histogram quantiles. A low
  in-flight value despite available foreground capacity means the client is not supplying
  parallel work; a full in-flight pool with rising latency means the transfer path is
  saturated.
- Collect service stdout logs and alert on repeated publication, maintenance, RocksDB, or
  free-space observation errors.

## Startup recovery and storage compatibility

Every startup performs these actions before listening:

1. Open or initialize metadata format `flywheel-channel-cache-v1`.
2. Create or validate the Default Channel.
3. Finish ordinary channels recorded as `deleting`.
4. Remove abandoned files below each channel's `tmp/` directory.
5. Inspect up to `orphan_scan_limit` final files and delete those without metadata.

A metadata row whose body is absent remains until a read or eviction turns it into a miss
and removes the stale row. Because orphan scanning is bounded, an unreferenced final file
may survive a startup if it falls outside that scan.

All top-level directories below `artifacts/` must be canonical uppercase channel IDs. An
invalid directory encountered by the bounded scan prevents reconciliation and startup. Do
not place operator files in the artifact tree.

This build does not migrate older or alternate layouts. An incompatible store-format
marker or record schema version fails startup. Initialize an empty data directory when a
release intentionally changes the persisted format.

## Backup and restore

Treat Flywheel as a disposable cache unless an external requirement says otherwise. There
is no supported live filesystem-copy procedure because RocksDB metadata and artifact files
must represent a coherent point in time.

For a cold copy, stop the process, copy the complete data directory, and restore both
`metadata/` and `artifacts/` together into a replica running a compatible store format.
Partial restores are safe only in the cache sense: missing bodies self-heal to misses and
orphan files may be removed by a bounded startup scan, but restored entries are not
guaranteed to remain warm.

## Kubernetes deployment with Helm

The chart at `charts/flywheel` deploys:

- a StatefulSet with one retained PVC per shard;
- a headless Service whose named port publishes ready shards through DNS SRV;
- an optional shared agent Deployment and client-facing Service;
- default resource requests, restricted security contexts, startup and health probes,
  topology spreading, and PodDisruptionBudgets;
- optional HPA, Ingress, ingress NetworkPolicies, and Prometheus ServiceMonitors.

The chart requires Kubernetes 1.32 or newer so StatefulSet PVC retention is a stable API.
It defaults to `ReadWriteOncePod`, which requires a compatible CSI driver and prevents two
Pods from mounting one RocksDB data volume concurrently. Override it with `ReadWriteOnce`
only after confirming the storage system still enforces Flywheel's single-writer
requirement.

Install a pinned production image with a site-specific values file:

```yaml
image:
  digest: sha256:<image-digest>

shards:
  replicas: 3
  storage:
    className: premium-rwo
    size: 500Gi
  resources:
    requests:
      cpu: "2"
      memory: 4Gi
    limits:
      memory: 8Gi

agent:
  replicas: 3
```

```text
helm upgrade --install flywheel charts/flywheel \
  --namespace cache \
  --create-namespace \
  --values production-values.yaml
```

Review `charts/flywheel/README.md` and the full `values.yaml` before rollout. In
particular:

- size low/high watermarks and emergency headroom against each shard PVC, not aggregate
  cluster capacity;
- confirm the StorageClass honors `fsGroup` or otherwise makes the data volume writable
  by UID/GID 65532;
- retain claims on scale-down and deletion unless a cold cache is explicitly acceptable;
- place shard replicas across failure domains and set disruption budgets to match the
  available node count;
- install the Prometheus Operator CRDs before enabling ServiceMonitors;
- terminate TLS and restrict unauthenticated management and metrics routes at the edge.

Treat `shards.config.defaultExpirySeconds` as immutable after any shard PVC is initialized.
Flywheel preserves the Default Channel's persisted expiry on restart, so a later Helm value
changes only newly created PVCs and can give joining shards a different retention policy.
There is no scaled channel control plane that can patch every shard.

Ingress and service proxies must also permit the configured maximum upload size, long-lived
streaming responses, and appropriate idle timeouts. Configure buffering and body limits in
controller-specific annotations; the chart cannot select safe defaults across ingress
implementations.

Do not combine a Flywheel version update with a shard replica-count change. Update shards
one at a time, wait for semantic readiness and DNS membership convergence, and observe
agent forward failures, ejections, bypasses, and ring membership. Adding or removing a
shard remaps keys without copying bodies, so a temporary drop in hit rate is expected.

The chart defaults to retained PVCs. A Helm rollback is safe only when the older Flywheel
binary accepts the store format written by the newer one. This release has no metadata
migration or compatibility shim; use a new data directory when the persisted format
changes.

Increasing `shards.storage.size` does not by itself resize an existing StatefulSet safely.
Confirm StorageClass expansion support, resize each retained PVC with an operator procedure,
and verify filesystem growth before raising capacity watermarks.

## Scaled routing agent

### Routing behavior

`flywheel agent` discovers replicas from DNS SRV records and consistently hashes bare data
requests to one owner:

```text
RUST_LOG=info flywheel agent \
  --listen 0.0.0.0:9080 \
  --srv _flywheel._tcp.flywheel-shards.cache.svc.cluster.local
```

| Flag | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--listen` | `FLYWHEEL_AGENT_LISTEN` | `127.0.0.1:9080` | Agent listen address |
| `--srv` | `FLYWHEEL_AGENT_SRV` | Required | SRV name publishing ready replicas |
| `--refresh-max` | `FLYWHEEL_AGENT_REFRESH_MAX` | 30 seconds | Maximum DNS refresh interval and retry backoff |
| `--failure-limit` | `FLYWHEEL_AGENT_FAILURE_LIMIT` | 1 | Consecutive send failures before owner ejection |
| `--retry-timeout` | `FLYWHEEL_AGENT_RETRY_TIMEOUT` | 30 seconds | Ejection duration before retry |
| `--connect-timeout` | `FLYWHEEL_AGENT_CONNECT_TIMEOUT` | 5 seconds | Backend connection timeout |
| `--deadline` | `FLYWHEEL_AGENT_DEADLINE` | 60 seconds | Maximum interval with no response bytes |

Operational constraints:

- Only bare routes are supported, so all agent traffic uses the Default Channel.
- `POST /channels` and every `/channels/...` path return `501 Not Implemented`.
- The agent does not replicate data or replay a failed request against a new owner.
- Build-cache reads fail open as `404` misses, and build-cache writes fail open as `200`
  bypasses when no owner can serve them.
- Other traffic returns `503` for an empty ring or `502` after an owner send failure.
- `/health/ready` means the agent can enforce these response semantics. It remains `200`
  with an empty ring.

Use `GET /status` to inspect the ring fingerprint, last DNS refresh, last discovery error,
members, failure counts, retry times, and ejection state. `GET /status?session=<label>`
instead returns the merged `cacheprog` session manifest: the agent queries every member of
one ring snapshot concurrently, merges successful answers by recency, and returns only the
manifest — failed members contribute nothing and appear in metrics and logs, never in the
response. Monitor these agent metrics:

```text
flywheel_agent_requests_total
flywheel_agent_forwarded_total
flywheel_agent_forward_failures_total
flywheel_agent_synthesized_misses_total
flywheel_agent_synthesized_write_bypasses_total
flywheel_agent_unavailable_total
flywheel_agent_ejections_total
flywheel_agent_status_fanout_requests_total
flywheel_agent_status_fanout_in_flight
flywheel_agent_status_fanout_duration_seconds
flywheel_agent_status_fanout_members_queried_total
flywheel_agent_status_fanout_members_succeeded_total
flywheel_agent_status_fanout_members_failed_total
flywheel_agent_status_fanout_manifest_entries
flywheel_agent_prefetch_requests_total
flywheel_agent_prefetch_hits_total
flywheel_agent_prefetch_misses_total
flywheel_agent_prefetch_unavailable_total
flywheel_agent_prefetch_response_bytes_total
flywheel_agent_prefetch_in_flight
flywheel_agent_prefetch_completed_total
flywheel_agent_prefetch_cancelled_total
flywheel_agent_prefetch_transfer_duration_seconds
flywheel_agent_forward_duration_seconds
flywheel_agent_ring_members
flywheel_agent_ring_ejected
```

The `status_fanout` metrics describe manifest discovery, including concurrent fan-outs,
latency distribution, members omitted by failures, and returned manifest size. The
`prefetch` metrics describe forwarded cache requests carrying the telemetry-only
`x-flywheel-request-purpose: prefetch` header: a hit is a `2xx`, a miss a `404`, and
everything else — including degraded responses with no owner — is unavailable. The header
never changes routing, admission, authorization, or responses.

Recommended dashboards and alerts for prefetch:

- Fan-out health: graph `members_failed` against `members_queried` and alert on a
  sustained failure ratio — it means shards are unreachable from this agent even if
  foreground traffic still fails open. Empty manifests are not alertable by themselves;
  a new session label legitimately starts empty.
- Prefetch effectiveness: graph the hit ratio (`prefetch_hits` over `prefetch_requests`)
  and `prefetch_response_bytes` alongside build duration. Alert on prefetch degradation
  (hit ratio collapsing, or `prefetch_unavailable` rising) rather than on volume.
- Foreground latency: compare shard request latency before and after raising
  `--prefetch-concurrency`; speculative traffic shares the same foreground budget on
  shards, so a too-high bound shows up as foreground `429`s and slower cache reads.

On a DNS lookup failure, the agent retains the last good membership and retries with
jittered backoff. An authoritative empty SRV answer replaces the ring with an empty one.
Custom channels must remain blocked at the deployment boundary until a shared channel
control plane exists.

### Shared agent Deployment

The chart enables a shared agent Deployment and ClusterIP Service by default. With release
name `flywheel` in Kubernetes namespace `cache`, clients use:

```text
http://flywheel.cache.svc.cluster.local
```

The agent Deployment discovers
`_flywheel._tcp.flywheel-shards.cache.svc.cluster.local`. It needs DNS access but does not
use the Kubernetes API, so the chart does not grant RBAC or mount a service-account token.
Multiple agents can serve the same Service because each canonicalizes the SRV targets and
builds the same ring. During DNS convergence, different agents may briefly choose different
owners; this produces safe misses or duplicate cache copies, not shared-database
corruption.

### Pod-local agent sidecar

A pod-local agent removes the extra Service hop and gives each build Pod its own connection
pools and ring view. It is still only a router: the current agent does not contain an L1
artifact cache or a channel credential. `flywheel cacheprog` keeps its verified local
objects in the build container's configured cache directory.

The pod-local agent is the required topology for `cacheprog` prefetch against replicated
shards: manifest discovery and every prefetch download go through the agent's
`/status?session=` fan-out and ring routing. Without an agent in front of the ring,
cacheprog only sees one replica's manifest and objects, and prefetch degrades to whatever
that replica holds.

Install only the shards and headless discovery Service:

```text
helm upgrade --install flywheel charts/flywheel \
  --namespace cache \
  --set agent.enabled=false \
  --set service.enabled=false
```

Then add an agent to each build Pod. The following fragment uses Kubernetes native sidecar
containers (`restartPolicy: Always` on an init container), which start before application
containers and stop after them:

```yaml
metadata:
  labels:
    flywheel.ctxswitch.dev/agent: sidecar
spec:
  automountServiceAccountToken: false
  initContainers:
    - name: flywheel-agent
      image: ctxsh/flywheel@sha256:<image-digest>
      args: ["agent"]
      restartPolicy: Always
      env:
        - name: RUST_LOG
          value: info
        - name: FLYWHEEL_AGENT_LISTEN
          value: 0.0.0.0:9080
        - name: FLYWHEEL_AGENT_SRV
          value: _flywheel._tcp.flywheel-shards.cache.svc.cluster.local
        - name: FLYWHEEL_AGENT_REFRESH_MAX
          value: "30"
        - name: FLYWHEEL_AGENT_FAILURE_LIMIT
          value: "1"
        - name: FLYWHEEL_AGENT_RETRY_TIMEOUT
          value: "30"
        - name: FLYWHEEL_AGENT_CONNECT_TIMEOUT
          value: "5"
        - name: FLYWHEEL_AGENT_DEADLINE
          value: "60"
      ports:
        - name: flywheel-agent
          containerPort: 9080
      startupProbe:
        httpGet:
          path: /health/live
          port: flywheel-agent
        periodSeconds: 2
        failureThreshold: 30
      readinessProbe:
        httpGet:
          path: /health/ready
          port: flywheel-agent
        periodSeconds: 5
      livenessProbe:
        httpGet:
          path: /health/live
          port: flywheel-agent
        periodSeconds: 10
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
        seccompProfile:
          type: RuntimeDefault
      resources:
        requests:
          cpu: 100m
          memory: 128Mi
        limits:
          memory: 512Mi
      volumeMounts:
        - name: agent-tmp
          mountPath: /tmp
  containers:
    - name: build
      # This image must contain the flywheel binary as well as the Go toolchain.
      image: example/build-image@sha256:<image-digest>
      env:
        - name: GOCACHEPROG
          value: >-
            flywheel cacheprog
            --url http://127.0.0.1:9080/build-cache/http/
            --cache-dir /var/cache
            --session ci-linux-amd64
      volumeMounts:
        - name: cacheprog
          mountPath: /var/cache
  volumes:
    - name: agent-tmp
      emptyDir:
        sizeLimit: 64Mi
    - name: cacheprog
      emptyDir:
        sizeLimit: 20Gi
```

That `emptyDir` is the disposable option. To retain verified local objects across build
Pods, replace only the `cacheprog` volume with the dedicated `hostPath` shown under
[Persisting the local cache on a Kubernetes node](#persisting-the-local-cache-on-a-kubernetes-node)
and keep the agent's `/tmp` volume ephemeral. Match `--cache-dir` to the build container's
mount path; the agent container does not need access to the local object directory.

Native sidecars are stable in Kubernetes 1.33 and enabled by default as a beta feature from
1.29. On clusters without the feature, place the same agent under `containers` and make the
build entrypoint wait for `http://127.0.0.1:9080/health/live` before starting Go. The agent
binds `0.0.0.0` in the example so kubelet HTTP probes can reach it; do not create a Service
for the sidecar, and use a workload NetworkPolicy when other Pods must not reach the port.

If the chart's ingress NetworkPolicy is enabled, its default shard rule admits only the
chart-managed shared agents. Authorize labeled sidecars explicitly:

```yaml
networkPolicy:
  enabled: true
  shards:
    additionalIngress:
      - podSelector:
          matchLabels:
            flywheel.ctxswitch.dev/agent: sidecar
```

A `podSelector` alone selects sidecars in the Flywheel Kubernetes namespace. For build Pods
in another namespace, combine a `namespaceSelector` with the `podSelector`. Egress policy
must allow the sidecar to query cluster DNS and connect to shard port `8080`.

Point every client in the Pod at `127.0.0.1:9080` and use only bare routes. The sidecar
returns `501` for channel-prefixed requests just like the shared agent. Its readiness
endpoint remains `200` with an empty ring, so inspect `/status` or
`flywheel_agent_ring_members` when a deployment requires warm shards before starting work.

## Troubleshooting

### The replica will not start

Check the service's stdout log stream for the first initialization error. Common causes are:

- the data directory is not writable;
- another process holds the RocksDB directory;
- the store-format marker or record version is incompatible;
- the persisted Default Channel violates its open and active invariants;
- a top-level artifact directory is not a canonical uppercase channel ID;
- an upstream or allowed-origin URL is invalid;
- a numeric configuration value fails validation.

Do not edit RocksDB records to repair startup. The cache is disposable; preserve the old
directory for diagnosis and start with an empty directory when recovery is not required.

### Readiness is `503`

Check filesystem access, RocksDB errors, and free-space observation in the service log. A failed
free-space sensor deliberately refuses reservations. Maintenance refreshes the observation
every 30 seconds, or continuously while reclaiming.

### Writes succeed but later miss

Generic build-cache, Bazel action-cache, and package publications are best effort. They may
also report success without storing under disk pressure. Check
`flywheel_build_cache_bypasses_total`, free-space gauges, and maintenance activity. Raw and
Bazel CAS writes use the durable contract and return an error instead of bypassing.

### A protected channel returns `401`

Verify the canonical channel ID and send the token as `Authorization: Bearer <token>` or as
the HTTP Basic password. Cargo registry clients send the token itself as the
`Authorization` value. Tokens cannot be recovered or changed. A wrong or missing token
returns `401` with `WWW-Authenticate: Bearer realm="flywheel"`.

### A channel returns `404` during deletion

This is expected once `deleting` is persisted. Wait for `DELETE` to finish or restart the
replica so startup can resume cleanup. The channel ID is not reusable after deletion.

### Channel routes return `501`

The request reached `flywheel agent`, which supports only bare Default Channel traffic.
Send custom-channel traffic directly to a replica only when that deployment can preserve
its channel ownership, or keep it disabled until a shared channel control plane is
available.
