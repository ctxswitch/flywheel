# Flywheel Helm chart

This chart deploys independent Flywheel cache shards as a StatefulSet and publishes their
ready Pod identities through a headless Service. It can also deploy a shared `flywheel
agent` tier, or leave routing to agents that run beside client workloads.

## Requirements

- Kubernetes 1.32 or newer
- Helm 3
- A CSI StorageClass that supports `ReadWriteOncePod`, or an explicit access-mode override
- Cluster DNS with headless-Service SRV records
- The Prometheus Operator CRDs only when `monitoring.serviceMonitor.enabled=true`

The agent discovers shards through DNS. It does not access the Kubernetes API and receives
no RBAC permissions or service-account token by default.

## Install

Create a production values file that pins the image and selects a StorageClass:

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

Install it in the `cache` Kubernetes namespace:

```text
helm upgrade --install flywheel charts/flywheel \
  --namespace cache \
  --create-namespace \
  --values production-values.yaml
```

The default shared endpoint is `http://flywheel.cache.svc.cluster.local`. The shard SRV
name is `_flywheel._tcp.flywheel-shards.cache.svc.cluster.local`.

## Topologies

### Shared agent tier

The default chart deploys an agent Deployment and a client-facing Service. Every agent
computes the same consistent-hash ring from the ready shard SRV records. Clients use the
Service and never contact a shard directly.

This topology is appropriate for general package and build-cache traffic. The shared
Service supports only bare routes, which select the open Default Channel. The agent rejects
channel management and `/channels/...` traffic because channel state is not replicated
between shards.

### Pod-local agents

For build workloads, an agent can run in every client Pod and listen on localhost. Disable
the shared tier while retaining the StatefulSet and headless Service:

```text
helm upgrade --install flywheel charts/flywheel \
  --namespace cache \
  --set agent.enabled=false \
  --set service.enabled=false
```

Configure each sidecar with the chart's SRV name and point its workload at
`http://127.0.0.1:9080`. A pod-local agent still uses bare Default Channel routes. This is
the required topology for `flywheel cacheprog` prefetch against replicated shards: the
sidecar merges the session manifest across shards through its `/status?session=` fan-out
and routes each parallel prefetch download to the shard that owns it. See the
[operations guide](../../docs/operations.md#pod-local-agent-sidecar) for a complete Pod
fragment and rollout checks. The recommended native sidecar lifecycle is stable in
Kubernetes 1.33; Kubernetes 1.32 clusters can use the regular multi-container form with an
explicit startup wait.

## Storage and durability

Each StatefulSet ordinal owns a separate PVC and cache database. The default retention
policy keeps claims on chart deletion and scale-down:

```yaml
shards:
  storage:
    retentionPolicy:
      whenDeleted: Retain
      whenScaled: Retain
```

Set either field to `Delete` only when cold-cache recovery is acceptable. A PDB limits
voluntary concurrent disruption but does not replicate data. Node loss, claim deletion,
shard replacement with an empty claim, or ring membership changes can all reduce hit rate.

`ReadWriteOncePod` is the default access mode so Kubernetes prevents two Pods from mounting
one cache database concurrently. If the storage driver does not support it, set
`shards.storage.accessModes` to `ReadWriteOnce` and confirm the driver still provides the
single-writer behavior Flywheel requires.

Set `shards.storage.enabled=false` only for disposable development environments. That mode
uses `emptyDir` and loses every cached body when the Pod is replaced.

The configured low and high watermarks are free-space thresholds for each PVC, not a
cluster-wide quota. Leave enough capacity above the emergency headroom for RocksDB,
temporary uploads, and filesystem variance.

Treat `shards.config.defaultExpirySeconds` as immutable after the first shard PVC is
initialized. Startup preserves the Default Channel's persisted expiry, so changing this
value affects only new PVCs and can make retention differ between existing and joining
shards. A scaled deployment has no channel control plane to patch that record everywhere.

## Scheduling and updates

The chart uses soft hostname topology spread for shards and agents. Change
`topologySpread.whenUnsatisfiable` to `DoNotSchedule` only if the cluster always has enough
eligible topology domains; otherwise a zone or node shortage can block recovery.

Shard updates are ordered and wait for semantic readiness. A joining or leaving ordinal
changes the hash ring and intentionally makes some keys cold. Roll out one shard at a time,
observe agent membership and cache bypass metrics, and avoid combining a version rollout
with a replica-count change.

The default PDBs use `maxUnavailable: 1`. Adjust them alongside replica counts and any
cluster-autoscaler policy. A singleton needs `minAvailable: 1` if voluntary eviction must
be blocked.

Only the stateless shared agent Deployment supports an HPA:

```yaml
agent:
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    targetCPUUtilizationPercentage: 70
```

Do not autoscale shards from CPU or request load. Every shard-count change remaps cache
ownership and needs an intentional rollout decision.

Kubernetes commonly rejects changes to a StatefulSet's volume claim template. To expand
existing storage, verify that the StorageClass has `allowVolumeExpansion`, resize each PVC
through an operator-controlled procedure, and confirm filesystem growth on every shard.
Changing only `shards.storage.size` is not a complete expansion runbook.

## Security

The defaults run both containers as UID/GID 65532 with a read-only root filesystem,
`RuntimeDefault` seccomp, no Linux capabilities, and no privilege escalation. The shard
Pod uses `fsGroup: 65532` so the data volume must support Kubernetes ownership changes. If
the StorageClass does not, arrange the volume ownership through the storage driver rather
than running Flywheel as root.

The chart serves HTTP. Terminate authenticated TLS at a trusted ingress or service mesh
before traffic crosses an untrusted network. `POST /channels`, health, and metrics are
application-unauthenticated, and the shared agent exposes the open Default Channel.

Ingress controllers and external proxies need controller-specific settings for Flywheel's
large streaming requests. Set body-size, request/read timeout, and request/response
buffering policies to match `maxUploadBytes`; the chart does not guess annotations for a
particular controller.

`networkPolicy.enabled` adds ingress-only policies:

- shard ingress is limited to the chart-managed agent Pods plus any
  `networkPolicy.shards.additionalIngress` peers;
- shared-agent ingress is unrestricted unless `networkPolicy.agent.ingress` is set;
- egress remains unrestricted because DNS and package upstream destinations are
  deployment-specific.

Sidecar-only installations must supply `networkPolicy.shards.additionalIngress` before
enabling the policy. Label client Pods consistently and select only those labels.

## Monitoring

Enable Prometheus Operator discovery with:

```yaml
monitoring:
  serviceMonitor:
    enabled: true
    labels:
      release: kube-prometheus-stack
```

This creates separate ServiceMonitors for the shard and shared-agent Services. In a
sidecar-only topology, scrape sidecar agents through the workload's own PodMonitor or
ServiceMonitor because they are outside this chart's ownership.

Do not use the agent readiness endpoint as proof of ring health. It remains ready with an
empty ring so build-cache reads can fail open as misses and writes can bypass. Alert on
`flywheel_agent_ring_members`, ejections, forward failures, and the `/status` membership
view.

## Configuration notes

- Pin `image.digest` for reproducible production rollouts. It takes precedence over
  `image.tag`.
- Large byte and second values under `shards.config` and `agent` are strings so Helm does
  not serialize them as floating-point environment values.
- `extraEnv` and `extraEnvFrom` support secret references and settings not modeled by the
  chart. Do not duplicate chart-managed environment names.
- PVCs keep their original StorageClass and size semantics across upgrades. Expanding a
  value requires a StorageClass that permits expansion and may require operator action.
- ServiceMonitor resources require their CRD to exist before Helm installs the chart.

Review [the operations guide](../../docs/operations.md) for Flywheel's retention, pressure,
recovery, and scaled-routing contracts.
