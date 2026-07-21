use super::discovery::{
    Resolver, RingState, SrvRecord, SrvSnapshot, canonical_member_id, refresh_once,
};
use super::ring::RingMember;
use crate::clock::Clock;
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

struct TestClock(AtomicU64);

impl TestClock {
    fn advance(&self, seconds: u64) {
        self.0.fetch_add(seconds, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn member(id: &str, address: &str) -> RingMember {
    RingMember {
        id: id.to_owned(),
        address: address.parse().unwrap(),
    }
}

fn three_members() -> Vec<RingMember> {
    vec![
        member("flywheel-0", "10.0.0.1:8080"),
        member("flywheel-1", "10.0.0.2:8080"),
        member("flywheel-2", "10.0.0.3:8080"),
    ]
}

fn state_with(
    clock: &Arc<TestClock>,
    failure_limit: u32,
    retry_timeout: u64,
    members: Vec<RingMember>,
) -> RingState {
    let state = RingState::new(
        Arc::clone(clock) as Arc<dyn Clock>,
        failure_limit,
        retry_timeout,
    );
    state.apply_snapshot(members);
    state
}

#[test]
fn canonicalizes_srv_targets() {
    assert_eq!(
        canonical_member_id("Flywheel-0.Flywheel-Shards.cache.svc.cluster.local."),
        "flywheel-0.flywheel-shards.cache.svc.cluster.local"
    );
}

#[test]
fn reaching_the_failure_limit_ejects_and_rebuilds_without_the_member() {
    let clock = Arc::new(TestClock(AtomicU64::new(100)));
    let state = state_with(&clock, 2, 30, three_members());
    assert!(!state.record_failure("flywheel-1"));
    assert_eq!(state.ring().members().len(), 3);
    assert!(state.record_failure("flywheel-1"));
    let ring = state.ring();
    assert_eq!(ring.members().len(), 2);
    assert!(!ring.members().iter().any(|found| found.id == "flywheel-1"));
    let status = state.status();
    let ejected = status
        .members
        .iter()
        .find(|found| found.id == "flywheel-1")
        .unwrap();
    assert!(ejected.ejected);
    assert_eq!(ejected.next_retry, Some(130));
}

#[test]
fn ejected_member_becomes_eligible_after_the_retry_timeout() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 1, 30, three_members());
    assert!(state.record_failure("flywheel-2"));
    assert_eq!(state.ring().members().len(), 2);
    clock.advance(29);
    assert_eq!(state.ring().members().len(), 2);
    clock.advance(1);
    let ring = state.ring();
    assert_eq!(ring.members().len(), 3);
    // Re-admission clears the failure streak: the next failure re-ejects.
    assert!(state.record_failure("flywheel-2"));
    assert_eq!(state.ring().members().len(), 2);
}

/// Re-admission rebuilds the continuum once, not once per concurrent request.
/// Every caller that queued on the write lock re-checks the retry deadline, so
/// a degraded cluster does not pay one full rebuild per in-flight request while
/// the lock blocks all routing; the identical `Arc` proves a single rebuild.
#[test]
fn concurrent_readmission_rebuilds_the_continuum_once() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 1, 30, three_members());
    for _ in 0..16 {
        assert!(state.record_failure("flywheel-1"));
        clock.advance(30);
        let barrier = std::sync::Barrier::new(8);
        let rings: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let (state, barrier) = (&state, &barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        state.ring()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect()
        });
        for ring in &rings {
            assert_eq!(ring.members().len(), 3);
            assert!(
                Arc::ptr_eq(ring, &rings[0]),
                "every concurrent caller must observe the one rebuilt continuum"
            );
        }
    }
}

#[test]
fn success_resets_the_consecutive_failure_count() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 3, 30, three_members());
    assert!(!state.record_failure("flywheel-0"));
    assert!(!state.record_failure("flywheel-0"));
    state.record_success("flywheel-0");
    assert!(!state.record_failure("flywheel-0"));
    assert!(!state.record_failure("flywheel-0"));
    assert_eq!(state.ring().members().len(), 3);
}

#[test]
fn snapshots_preserve_ejection_state_for_surviving_members() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 1, 300, three_members());
    assert!(state.record_failure("flywheel-1"));
    state.apply_snapshot(three_members());
    let ring = state.ring();
    assert_eq!(ring.members().len(), 2);
    assert!(!ring.members().iter().any(|found| found.id == "flywheel-1"));
    // A member absent from a successful snapshot is removed rather than retried.
    state.apply_snapshot(vec![member("flywheel-0", "10.0.0.1:8080")]);
    assert_eq!(state.status().members.len(), 1);
}

#[test]
fn address_change_moves_no_ring_positions() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 1, 30, three_members());
    let before = state.ring();
    let mut moved = three_members();
    moved[1].address = "10.9.9.9:8080".parse().unwrap();
    state.apply_snapshot(moved);
    let after = state.ring();
    assert_eq!(before.fingerprint(), after.fingerprint());
    for sample in 0..500_u32 {
        let position = super::ring::key_position("artifact", &format!("key-{sample}")).unwrap();
        assert_eq!(
            before.owner(position).unwrap().id,
            after.owner(position).unwrap().id
        );
    }
    assert_eq!(
        after
            .members()
            .iter()
            .find(|found| found.id == "flywheel-1")
            .unwrap()
            .address,
        "10.9.9.9:8080".parse::<SocketAddr>().unwrap()
    );
}

#[test]
fn empty_snapshot_produces_an_empty_ring() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 1, 30, three_members());
    state.apply_snapshot(Vec::new());
    assert!(state.ring().is_empty());
}

struct FakeResolver {
    srv: Mutex<anyhow::Result<SrvSnapshot>>,
    ips: Mutex<std::collections::HashMap<String, Vec<IpAddr>>>,
}

#[async_trait::async_trait]
impl Resolver for FakeResolver {
    async fn srv(&self, _name: &str) -> anyhow::Result<SrvSnapshot> {
        let mut guard = self.srv.lock().unwrap();
        match &mut *guard {
            Ok(snapshot) => Ok(snapshot.clone()),
            Err(error) => Err(anyhow::anyhow!("{error}")),
        }
    }

    async fn ips(&self, target: &str) -> anyhow::Result<Vec<IpAddr>> {
        self.ips
            .lock()
            .unwrap()
            .get(target)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no addresses for {target}"))
    }
}

fn snapshot(targets: &[&str]) -> SrvSnapshot {
    SrvSnapshot {
        records: targets
            .iter()
            .map(|target| SrvRecord {
                target: (*target).to_owned(),
                port: 8080,
            })
            .collect(),
        ttl: Duration::from_secs(30),
    }
}

#[tokio::test]
async fn refresh_keeps_last_known_address_when_target_lookup_fails() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 1, 30, Vec::new());
    let resolver = FakeResolver {
        srv: Mutex::new(Ok(snapshot(&["flywheel-0.shards.svc."]))),
        ips: Mutex::new(
            [(
                "flywheel-0.shards.svc.".to_owned(),
                vec!["10.0.0.7".parse().unwrap()],
            )]
            .into(),
        ),
    };
    refresh_once(&state, &resolver, "shards").await.unwrap();
    assert_eq!(
        state.known_addresses(),
        vec![(
            "flywheel-0.shards.svc".to_owned(),
            "10.0.0.7:8080".parse().unwrap()
        )]
    );

    // The A lookup fails while SRV still lists the target: the member keeps its
    // last known address instead of vanishing from the ring.
    resolver.ips.lock().unwrap().clear();
    refresh_once(&state, &resolver, "shards").await.unwrap();
    assert_eq!(
        state.known_addresses(),
        vec![(
            "flywheel-0.shards.svc".to_owned(),
            "10.0.0.7:8080".parse().unwrap()
        )]
    );
}

#[tokio::test]
async fn refresh_failure_keeps_the_last_good_membership() {
    let clock = Arc::new(TestClock(AtomicU64::new(0)));
    let state = state_with(&clock, 1, 30, three_members());
    let resolver = FakeResolver {
        srv: Mutex::new(Err(anyhow::anyhow!("SERVFAIL"))),
        ips: Mutex::new([].into()),
    };
    assert!(refresh_once(&state, &resolver, "shards").await.is_err());
    state.record_refresh_error("SERVFAIL".to_owned());
    assert_eq!(state.ring().members().len(), 3);
    assert_eq!(state.status().last_error.as_deref(), Some("SERVFAIL"));

    // A later authoritative empty answer is a real scale-to-zero.
    *resolver.srv.lock().unwrap() = Ok(snapshot(&[]));
    refresh_once(&state, &resolver, "shards").await.unwrap();
    assert!(state.ring().is_empty());
    assert_eq!(state.status().last_error, None);
}
