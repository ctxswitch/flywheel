use super::{FreeSpace, Mode, SpaceLedger, SpacePolicy};
use crate::storage::local::Reserver;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

struct Fixed(u64);
impl FreeSpace for Fixed {
    fn free_bytes(&self) -> Option<u64> {
        Some(self.0)
    }
}

/// Every case here exercises the same watermark policy; only the free-space
/// source differs.
fn ledger_from(source: Arc<dyn FreeSpace>) -> SpaceLedger {
    SpaceLedger::new(
        source,
        SpacePolicy {
            low_watermark: 10,
            high_watermark: 20,
            emergency_headroom: 0,
        },
    )
}

fn ledger(free: u64) -> SpaceLedger {
    ledger_from(Arc::new(Fixed(free)))
}

#[test]
fn concurrent_reservations_never_double_spend_the_same_capacity() {
    let ledger = ledger(100);
    // A known-length upload and an unknown-length extent both draw from the same
    // window; the ledger admits only what fits.
    assert!(ledger.try_reserve(60));
    assert!(ledger.try_reserve(40));
    assert!(!ledger.try_reserve(1));
    assert_eq!(ledger.snapshot().reserved, 100);

    // Committing frees the reservation slot but keeps the bytes accounted until the
    // next filesystem observation.
    ledger.commit(60);
    assert_eq!(ledger.snapshot().reserved, 40);
    assert!(!ledger.try_reserve(1));
    assert_eq!(ledger.snapshot().committed_since, 60);
}

#[test]
fn mode_uses_low_high_watermark_hysteresis() {
    let source = Arc::new(AtomicU64::new(100));
    struct Dynamic(Arc<AtomicU64>);
    impl FreeSpace for Dynamic {
        fn free_bytes(&self) -> Option<u64> {
            Some(self.0.load(Ordering::SeqCst))
        }
    }
    let ledger = ledger_from(Arc::new(Dynamic(Arc::clone(&source))));
    assert_eq!(ledger.mode(), Mode::Normal);

    // Below the low watermark enters Reclaiming.
    source.store(5, Ordering::SeqCst);
    ledger.refresh();
    assert_eq!(ledger.mode(), Mode::Reclaiming);

    // Between the watermarks Reclaiming persists (hysteresis).
    source.store(15, Ordering::SeqCst);
    ledger.refresh();
    assert_eq!(ledger.mode(), Mode::Reclaiming);

    // At or above the high watermark returns to Normal.
    source.store(25, Ordering::SeqCst);
    ledger.refresh();
    assert_eq!(ledger.mode(), Mode::Normal);
}

#[test]
fn failed_observation_fails_closed_and_reports_degraded() {
    struct Failing;
    impl FreeSpace for Failing {
        fn free_bytes(&self) -> Option<u64> {
            None
        }
    }
    let ledger = ledger_from(Arc::new(Failing));
    // An unreadable filesystem starts degraded and admits nothing.
    assert!(ledger.degraded());
    assert!(!ledger.try_reserve(1));
}

#[test]
fn refresh_failure_retains_last_observation_but_stops_reservations() {
    let source = Arc::new(AtomicU64::new(100));
    struct Maybe {
        free: Arc<AtomicU64>,
        fail: Arc<std::sync::atomic::AtomicBool>,
    }
    impl FreeSpace for Maybe {
        fn free_bytes(&self) -> Option<u64> {
            if self.fail.load(Ordering::SeqCst) {
                None
            } else {
                Some(self.free.load(Ordering::SeqCst))
            }
        }
    }
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ledger = ledger_from(Arc::new(Maybe {
        free: Arc::clone(&source),
        fail: Arc::clone(&fail),
    }));
    assert!(ledger.try_reserve(10));

    // The observation now fails: the ledger keeps the last free value for metrics
    // but refuses new reservations.
    fail.store(true, Ordering::SeqCst);
    ledger.refresh();
    assert!(ledger.degraded());
    assert_eq!(ledger.snapshot().free_observed, 100);
    assert!(!ledger.try_reserve(1));

    // Recovery clears the degraded state.
    fail.store(false, Ordering::SeqCst);
    ledger.refresh();
    assert!(!ledger.degraded());
    assert!(ledger.try_reserve(1));
}
