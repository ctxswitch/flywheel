# ADR 0001: Keep recency-aware eviction

Date: 2026-07-18

## Status

Accepted

## Context

Every artifact receives a soft retention deadline when it is published. Normal
maintenance considers entries whose deadline has passed, while pressure reclamation
must free space promptly enough to restore admission.

The cache also records approximate recent reads in a rotating pair of Bloom filters.
When normal maintenance reaches an expired entry that was read recently, it requeues
the entry with a new soft deadline. This adds a small amount of configuration,
metadata, and stale-candidate handling, but avoids evicting warm artifacts merely
because their original deadline elapsed.

Strict TTL would be simpler, but the effect is not limited to disposable proxy data.
The common publication path covers raw artifacts, Bazel CAS and action-cache entries,
build-cache outputs, and package bodies. Evicting a recently used entry can therefore
cost an upstream fetch, a rebuild, or a client re-upload. Dangling proxy references
also currently rely on the body remaining available until their metadata is repaired.

## Decision

Recency-aware eviction is an intentional retention contract.

During normal maintenance, an artifact read in the current recency window receives a
new soft deadline and is requeued. Queue entries made stale by that new deadline are
ignored when encountered later. During pressure reclamation, recency is ignored and
the oldest eligible entries are removed until the cache recovers its watermark.

The recent-use module, metadata requeue operation, `bloom_bits` configuration,
stale-candidate handling, and `flywheel_maintenance_requeued_total` metric remain part
of the design.

## Consequences

Normal maintenance retains warm artifacts beyond their original soft deadline, so
retention is deliberately not strict TTL. Approximate Bloom-filter false positives can
also grant a cold artifact one extra interval, which is acceptable for normal
maintenance. Pressure reclamation remains deterministic with respect to the eviction
queue and cannot be blocked by heat.

Strict TTL may be reconsidered only after measurement shows that recency reprieves
provide negligible cache value and dangling proxy-reference recovery no longer depends
on retained bodies.
