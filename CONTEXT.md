# Flywheel cache

Flywheel stores every cache operation inside a channel. A channel is the sole identity,
isolation, authorization, retention, lifecycle, and deletion boundary.

## Language

**Channel**: An isolated cache-data scope with a stable `ChannelId`, fixed access type,
persisted expiry, lifecycle state, and creation time. The channel ID prefixes storage
keys, file paths, locks, recency, proxy flights, and maintenance work.

**Default Channel**: The well-known open channel whose ID is the nil ULID
`00000000000000000000000000`. Bare data routes select it. A bare route and the same
route below `/channels/00000000000000000000000000` address exactly the same data.

**Open Channel**: A channel that requires no credential. Registration returns no token.

**Protected Channel**: A channel whose registration returns a token once. Flywheel
persists only its digest and requires the token for data, inspection, expiry updates,
and deletion.

## Invariants

- Channel access is fixed at registration; only `expiry_seconds` is mutable.
- The Default Channel is always open and active, and cannot be deleted.
- Startup durably creates the Default Channel before readiness and never overwrites a
  persisted expiry.
- Deleting an ordinary channel fences final writes, removes all of its cache data, and
  then removes its record. Startup resumes interrupted deletions.
- Flywheel has no operational modes and no separate cache namespace abstraction.

Use “namespace” only for unrelated infrastructure concepts such as Kubernetes release
namespaces or Linux network namespaces.
