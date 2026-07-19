//! Kubernetes DNS SRV discovery and the shared routing state.
//!
//! One background task refreshes membership from DNS before the answer's TTL expires
//! and swaps in a complete replacement continuum. The same state carries the
//! twemproxy-style ejection bookkeeping: transport failures counted by the forwarder
//! eject a member from the continuum until its retry deadline, while discovery
//! rebuilds preserve that bookkeeping for member IDs that remain discovered.

use crate::{
    agent::ring::{Ring, RingMember},
    clock::Clock,
};
use hickory_resolver::{
    TokioResolver,
    net::{DnsError, NetError},
    proto::rr::RData,
};
use serde::Serialize;
use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// One SRV answer record: the stable shard identity and its published port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SrvRecord {
    pub target: String,
    pub port: u16,
}

/// A successful SRV response. An authoritative answer with no records is a valid
/// empty snapshot, distinct from a resolver failure (which is an `Err`).
#[derive(Clone, Debug)]
pub struct SrvSnapshot {
    pub records: Vec<SrvRecord>,
    pub ttl: Duration,
}

/// DNS as the agent consumes it, so tests can supply deterministic answers.
#[async_trait::async_trait]
pub trait Resolver: Send + Sync {
    /// Resolves the SRV name to targets, ports, and the effective answer TTL.
    async fn srv(&self, name: &str) -> anyhow::Result<SrvSnapshot>;
    /// Resolves one SRV target to its A/AAAA addresses.
    async fn ips(&self, target: &str) -> anyhow::Result<Vec<IpAddr>>;
}

/// Production resolver on the system DNS configuration (cluster DNS in Kubernetes).
pub struct DnsResolver {
    resolver: TokioResolver,
}

impl DnsResolver {
    pub fn from_system() -> anyhow::Result<Self> {
        Ok(Self {
            resolver: TokioResolver::builder_tokio()?.build()?,
        })
    }
}

/// Negative-answer TTL to use when the response does not carry one; short so an
/// empty ring is re-checked quickly.
const EMPTY_ANSWER_TTL: Duration = Duration::from_secs(5);

#[async_trait::async_trait]
impl Resolver for DnsResolver {
    async fn srv(&self, name: &str) -> anyhow::Result<SrvSnapshot> {
        match self.resolver.srv_lookup(name).await {
            Ok(lookup) => {
                let ttl = lookup
                    .valid_until()
                    .saturating_duration_since(Instant::now());
                let records = lookup
                    .answers()
                    .iter()
                    .filter_map(|record| match &record.data {
                        RData::SRV(srv) => Some(SrvRecord {
                            target: srv.target.to_utf8(),
                            port: srv.port,
                        }),
                        _ => None,
                    })
                    .collect();
                Ok(SrvSnapshot { records, ttl })
            }
            // The name resolved but has no records: an authoritative empty
            // membership, not a discovery failure.
            Err(NetError::Dns(DnsError::NoRecordsFound(no_records))) => Ok(SrvSnapshot {
                records: Vec::new(),
                ttl: no_records
                    .negative_ttl
                    .map_or(EMPTY_ANSWER_TTL, |ttl| Duration::from_secs(ttl.into())),
            }),
            Err(error) => Err(error.into()),
        }
    }

    async fn ips(&self, target: &str) -> anyhow::Result<Vec<IpAddr>> {
        Ok(self.resolver.lookup_ip(target).await?.iter().collect())
    }
}

/// Canonical ring identity of an SRV target: lowercase, without the trailing dot.
pub fn canonical_member_id(target: &str) -> String {
    target.trim_end_matches('.').to_ascii_lowercase()
}

/// A discovered member plus its consecutive-failure count and retry deadline.
#[derive(Clone, Debug)]
struct MemberHealth {
    member: RingMember,
    failures: u32,
    /// Unix-seconds deadline while the member is ejected from the continuum.
    next_retry: Option<u64>,
}

struct RingInner {
    /// Full discovered membership, sorted by member ID, including ejected members.
    members: Vec<MemberHealth>,
    /// Continuum over the currently eligible members, swapped atomically.
    ring: Arc<Ring>,
    /// Soonest retry deadline among ejected members, checked on the request path.
    earliest_retry: Option<u64>,
    last_refresh: Option<u64>,
    last_error: Option<String>,
}

/// Shared routing state: the active continuum plus per-member ejection bookkeeping.
pub struct RingState {
    inner: RwLock<RingInner>,
    clock: Arc<dyn Clock>,
    failure_limit: u32,
    retry_timeout_seconds: u64,
}

impl RingState {
    pub fn new(clock: Arc<dyn Clock>, failure_limit: u32, retry_timeout_seconds: u64) -> Self {
        Self {
            inner: RwLock::new(RingInner {
                members: Vec::new(),
                ring: Arc::new(Ring::new(Vec::new())),
                earliest_retry: None,
                last_refresh: None,
                last_error: None,
            }),
            clock,
            failure_limit: failure_limit.max(1),
            retry_timeout_seconds,
        }
    }

    /// The continuum to route against. Re-admits ejected members whose retry
    /// deadline has passed before returning, so retry eligibility needs no timer.
    pub fn ring(&self) -> Arc<Ring> {
        let now = self.clock.now();
        {
            let inner = self.inner.read().expect("ring state lock");
            if inner.earliest_retry.is_none_or(|deadline| now < deadline) {
                return Arc::clone(&inner.ring);
            }
        }
        let mut inner = self.inner.write().expect("ring state lock");
        rebuild(&mut inner, now);
        Arc::clone(&inner.ring)
    }

    /// Resets the failure count when a send receives backend response headers.
    pub fn record_success(&self, member_id: &str) {
        {
            let inner = self.inner.read().expect("ring state lock");
            let healthy = member(&inner.members, member_id).is_none_or(|found| found.failures == 0);
            if healthy {
                return;
            }
        }
        let mut inner = self.inner.write().expect("ring state lock");
        if let Some(found) = member_mut(&mut inner.members, member_id) {
            found.failures = 0;
        }
    }

    /// Counts a transport failure. Reaching the failure limit sets the member's
    /// retry deadline and rebuilds the continuum without it. Returns whether this
    /// call ejected the member.
    pub fn record_failure(&self, member_id: &str) -> bool {
        let now = self.clock.now();
        let mut inner = self.inner.write().expect("ring state lock");
        let Some(found) = member_mut(&mut inner.members, member_id) else {
            // Already removed by a discovery refresh; nothing to eject.
            return false;
        };
        found.failures = found.failures.saturating_add(1);
        if found.next_retry.is_some() || found.failures < self.failure_limit {
            return false;
        }
        found.next_retry = Some(now + self.retry_timeout_seconds);
        rebuild(&mut inner, now);
        true
    }

    /// Replaces the discovered membership with a successful snapshot, preserving
    /// failure counts and retry deadlines for member IDs that remain discovered.
    /// An address change alone moves no ring positions (the ID is the identity).
    pub fn apply_snapshot(&self, discovered: Vec<RingMember>) {
        let now = self.clock.now();
        let mut inner = self.inner.write().expect("ring state lock");
        let mut members = Vec::with_capacity(discovered.len());
        for member in discovered {
            let previous = member_mut(&mut inner.members, &member.id);
            members.push(MemberHealth {
                failures: previous.as_ref().map_or(0, |health| health.failures),
                next_retry: previous.as_ref().and_then(|health| health.next_retry),
                member,
            });
        }
        members.sort_by(|a, b| a.member.id.cmp(&b.member.id));
        members.dedup_by(|a, b| a.member.id == b.member.id);
        inner.members = members;
        inner.last_refresh = Some(now);
        inner.last_error = None;
        rebuild(&mut inner, now);
    }

    /// Records a discovery failure without touching the last good membership.
    pub fn record_refresh_error(&self, error: String) {
        let mut inner = self.inner.write().expect("ring state lock");
        inner.last_error = Some(error);
    }

    /// Last successfully discovered address per member ID, used to keep routing to
    /// a known member whose A/AAAA lookup transiently fails.
    pub fn known_addresses(&self) -> Vec<(String, SocketAddr)> {
        let inner = self.inner.read().expect("ring state lock");
        inner
            .members
            .iter()
            .map(|health| (health.member.id.clone(), health.member.address))
            .collect()
    }

    pub fn status(&self) -> RingStatus {
        let now = self.clock.now();
        let inner = self.inner.read().expect("ring state lock");
        RingStatus {
            fingerprint: inner.ring.fingerprint().to_owned(),
            last_refresh: inner.last_refresh,
            last_error: inner.last_error.clone(),
            members: inner
                .members
                .iter()
                .map(|health| MemberStatus {
                    id: health.member.id.clone(),
                    address: health.member.address.to_string(),
                    failures: health.failures,
                    next_retry: health.next_retry,
                    ejected: health.next_retry.is_some_and(|deadline| now < deadline),
                })
                .collect(),
        }
    }
}

/// Re-admits members whose retry deadline has passed, then swaps in a new continuum
/// over every eligible member.
fn rebuild(inner: &mut RingInner, now: u64) {
    let mut earliest = None;
    let mut eligible = Vec::with_capacity(inner.members.len());
    for health in &mut inner.members {
        if let Some(deadline) = health.next_retry {
            if now >= deadline {
                // Eligible again after the retry timeout; a fresh failure re-ejects.
                health.next_retry = None;
                health.failures = 0;
            } else {
                earliest = Some(earliest.map_or(deadline, |soonest: u64| soonest.min(deadline)));
                continue;
            }
        }
        eligible.push(health.member.clone());
    }
    inner.ring = Arc::new(Ring::new(eligible));
    inner.earliest_retry = earliest;
}

fn member<'a>(members: &'a [MemberHealth], id: &str) -> Option<&'a MemberHealth> {
    members
        .binary_search_by(|health| health.member.id.as_str().cmp(id))
        .ok()
        .map(|index| &members[index])
}

fn member_mut<'a>(members: &'a mut [MemberHealth], id: &str) -> Option<&'a mut MemberHealth> {
    members
        .binary_search_by(|health| health.member.id.as_str().cmp(id))
        .ok()
        .map(|index| &mut members[index])
}

/// Operator-facing view of the routing state for `/status`.
#[derive(Debug, Serialize)]
pub struct RingStatus {
    pub fingerprint: String,
    pub last_refresh: Option<u64>,
    pub last_error: Option<String>,
    pub members: Vec<MemberStatus>,
}

#[derive(Debug, Serialize)]
pub struct MemberStatus {
    pub id: String,
    pub address: String,
    pub failures: u32,
    pub next_retry: Option<u64>,
    pub ejected: bool,
}

/// Runs one discovery pass: SRV, then per-target addresses, then an atomic swap.
/// Returns the effective TTL for scheduling. A resolver failure leaves the last
/// good membership in place; an authoritative empty answer replaces it with an
/// empty ring.
pub async fn refresh_once(
    state: &RingState,
    resolver: &dyn Resolver,
    srv_name: &str,
) -> anyhow::Result<Duration> {
    let snapshot = resolver.srv(srv_name).await?;
    let known: std::collections::HashMap<String, SocketAddr> =
        state.known_addresses().into_iter().collect();
    let mut members = Vec::with_capacity(snapshot.records.len());
    for record in &snapshot.records {
        let id = canonical_member_id(&record.target);
        match resolver.ips(&record.target).await {
            Ok(ips) if !ips.is_empty() => {
                // Multiple addresses stay connection choices for one member; pick
                // deterministically so every agent converges on the same address.
                let ip = ips.into_iter().min().expect("non-empty address list");
                members.push(RingMember {
                    id,
                    address: SocketAddr::new(ip, record.port),
                });
            }
            _ => {
                // Address lookup failed for a target the SRV answer still lists:
                // keep routing to its last known address rather than dropping the
                // member (its ring positions must not move on a transient failure).
                if let Some(previous) = known.get(&id) {
                    members.push(RingMember {
                        address: SocketAddr::new(previous.ip(), record.port),
                        id,
                    });
                }
            }
        }
    }
    state.apply_snapshot(members);
    Ok(snapshot.ttl)
}

/// Discovery loop: refresh before the effective TTL (capped by `refresh_max`) with
/// jitter, and back off exponentially with jitter on failure while keeping the last
/// good snapshot.
pub async fn run(
    state: &RingState,
    resolver: &dyn Resolver,
    srv_name: &str,
    refresh_max: Duration,
    cancellation: CancellationToken,
) {
    let minimum = Duration::from_secs(1);
    let mut backoff = minimum;
    loop {
        let delay = match refresh_once(state, resolver, srv_name).await {
            Ok(ttl) => {
                backoff = minimum;
                // 70-90% of the effective TTL, so agents neither synchronize their
                // queries nor let an answer expire before its refresh.
                jitter(ttl.clamp(minimum, refresh_max), 0.7, 0.9)
            }
            Err(error) => {
                tracing::warn!(%error, srv = srv_name, "DNS discovery refresh failed");
                state.record_refresh_error(error.to_string());
                let delay = jitter(backoff, 0.5, 1.0);
                backoff = (backoff * 2).min(refresh_max).max(minimum);
                delay
            }
        };
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

fn jitter(duration: Duration, low: f64, high: f64) -> Duration {
    duration.mul_f64(rand::Rng::random_range(&mut rand::rng(), low..high))
}

