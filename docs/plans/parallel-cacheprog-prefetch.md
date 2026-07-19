# Parallel cacheprog prefetch

Status: implemented

## Goal

Replace the serial prefetch stream with bounded parallel cache GETs through the pod-local
Flywheel agent. Prefetch remains best-effort and never fails the build.

## Design

The existing session manifest remains the only prefetch metadata. Its key, format,
lifecycle, and shutdown update are unchanged.

Replicated cacheprog deployments use a pod-local agent. Cacheprog sends all status and
cache requests to that agent and never sees ring membership or shard addresses.

### Manifest discovery

1. Cacheprog sends `GET /status?session=<session>` to its local agent.
2. The agent sends the same request to every member of one ring snapshot concurrently.
3. Each shard derives the existing manifest key and performs a local lookup.
4. The agent merges successful manifests by Go action using existing manifest recency.
5. Failed member requests contribute no entries.
6. The agent returns the merged manifest without member or completeness information.

`GET /status` without a session retains the agent's operational ring response.

### Parallel downloads

For each manifest action not already available locally, cacheprog sends:

```http
GET /build-cache/http/go-<action>
```

Requests use a bounded worker pool and target the local agent. The agent routes them
through its normal ring and pooled backend clients.

Cacheprog streams each response to a temporary file, verifies its digest and size against
the manifest, then atomically publishes it to the local object directory. Failed requests
are not retried by prefetch; later foreground Go requests use the normal cache path.

Cacheprog continues to update the session manifest through the ordinary build-cache route
at shutdown.

## Failure contract

- An empty ring or complete status fan-out failure returns an empty manifest.
- Partial fan-out failure returns entries from successful members.
- Missing or invalid manifests contribute no entries.
- Failed, missing, incomplete, or invalid downloads remain cache misses.
- Client disconnect cancels outstanding fan-out and download work.
- Prefetch failures never fail the build or expose cluster details to cacheprog.

## Observability

The agent exports status fan-out metrics for:

- requests and duration;
- members queried, succeeded, and failed; and
- manifest entries returned.

Speculative GETs carry a telemetry-only request-purpose header. The agent exports separate
prefetch request, hit, miss, unavailable, and response-byte counters. The header does not
change routing, admission, authorization, or response behavior.

The agent logs fan-out completion counts and duration, plus member identity and error for
failed subrequests. Cacheprog logs advertised, local, attempted, downloaded, and missed
objects, bytes, duration, and peak concurrency. Session values and cache keys are excluded
from metric labels and logs.

Operations documentation adds dashboards for fan-out health, prefetch effectiveness, and
foreground latency. Alerts cover sustained member failures and prefetch degradation;
empty manifests are not alertable by themselves.

## Implementation

1. Share manifest key derivation and merge behavior between cacheprog, the HTTP service,
   and the agent.
2. Add shard-local `GET /status?session=...` manifest lookup.
3. Add concurrent agent fan-out, manifest merge, cancellation, metrics, and logs.
4. Replace framed prefetch decoding with bounded ordinary GET scheduling in cacheprog.
5. Add streamed verification and atomic local publication.
6. Remove `POST /build-cache/prefetch`, the serial agent sweep, and the framed protocol.
7. Update architecture, operations, Helm configuration, chart notes, and examples for the
   required pod-local sidecar topology.

## Verification

- Shards return the requested local manifest or an empty manifest.
- The agent queries all ring members concurrently and merges successful responses.
- Member failure remains silent to cacheprog and visible in metrics and logs.
- Unfiltered agent status remains unchanged.
- Cacheprog sends status and all downloads through its local agent.
- Downloads run concurrently without exceeding the configured bound.
- Each download is verified before publication; failed downloads remain misses.
- Manifest format, lifecycle, and shutdown persistence remain unchanged.
- Ring changes between discovery and download produce ordinary misses.
- Formatting, warning-free Clippy, focused tests, the full serial suite, and Helm lint pass.

## Rollout

1. Benchmark representative builds at several bounded concurrency values.
2. Select a default that preserves foreground cache latency.
3. Canary manifest discovery and monitor fan-out metrics.
4. Enable parallel downloads and compare build duration, prefetch hit rate, agent latency,
   backend connection use, and foreground latency.
5. Roll back by disabling prefetch; foreground traffic remains on the unchanged agent
   route.
