use crate::storage::local::Reserver;
use std::{path::PathBuf, sync::Arc, sync::Mutex};

/// A source of the data filesystem's observed free-space in bytes. Injectable so
/// hermetic tests can force Normal or Reclaiming without a real filesystem, mirroring
/// the `Arc<dyn Clock>` dependency injection. `None` reports that the observation
/// failed so the ledger can fail closed instead of assuming unlimited space.
pub trait FreeSpace: Send + Sync {
    fn free_bytes(&self) -> Option<u64>;
}

/// Reads free space from the real data filesystem with `statvfs`.
pub struct StatvfsFreeSpace {
    path: PathBuf,
}

impl StatvfsFreeSpace {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl FreeSpace for StatvfsFreeSpace {
    fn free_bytes(&self) -> Option<u64> {
        // A transient statvfs failure must never be read as "unlimited space": that
        // would let staging spend into the emergency headroom. Report the failure and
        // let the ledger fail closed.
        match rustix::fs::statvfs(&self.path) {
            Ok(stats) => Some(stats.f_bavail.saturating_mul(stats.f_frsize)),
            Err(_) => None,
        }
    }
}

/// Watermarks and headroom that shape reservation and reclamation decisions.
#[derive(Clone, Copy, Debug)]
pub struct SpacePolicy {
    /// Below this many available bytes the controller enters Reclaiming.
    pub low_watermark: u64,
    /// Reclaiming continues until this many available bytes are restored.
    pub high_watermark: u64,
    /// Capacity fenced off for RocksDB and recovery; never reservable by staging.
    pub emergency_headroom: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Normal,
    Reclaiming,
}

/// The mutable ledger state guarded by a single mutex. Guarding the snapshot as one
/// unit keeps a `refresh` from erasing bytes committed after its filesystem sample and
/// keeps `degraded` observation handling consistent with reservation accounting. The
/// operations are all trivial arithmetic, so the mutex never becomes a hot-path cost.
struct State {
    /// The latest trusted free-space observation.
    free_observed: u64,
    /// Outstanding staging reservations not yet committed or released.
    reserved: u64,
    /// Bytes committed to disk since `free_observed` was sampled.
    committed_since: u64,
    /// Set when the last observation failed; new reservations are rejected until a
    /// fresh observation succeeds.
    degraded: bool,
    /// Whether the controller is currently in Reclaiming (low/high hysteresis).
    reclaiming: bool,
}

/// A small ledger tracking the latest free-space observation, outstanding staging
/// reservations, and bytes committed since that observation. Reservation success is
/// authoritative for disk safety; the Normal/Reclaiming mode is derived from the same
/// numbers with low/high watermark hysteresis.
pub struct SpaceLedger {
    source: Arc<dyn FreeSpace>,
    policy: SpacePolicy,
    state: Mutex<State>,
}

impl SpaceLedger {
    pub fn new(source: Arc<dyn FreeSpace>, policy: SpacePolicy) -> Self {
        let (free_observed, degraded) = match source.free_bytes() {
            Some(free) => (free, false),
            None => (0, true),
        };
        Self {
            source,
            policy,
            state: Mutex::new(State {
                free_observed,
                reserved: 0,
                committed_since: 0,
                degraded,
                reclaiming: false,
            }),
        }
    }

    /// Re-observes filesystem free space and resets the committed-since counter, since
    /// the fresh observation already reflects previously committed bytes. The sample
    /// and reset happen under the ledger lock so a concurrent commit is either fully
    /// reflected in the new observation or preserved in `committed_since` — never lost.
    /// A failed observation retains the last trusted value and marks the ledger
    /// degraded so no new reservations are admitted until it recovers.
    pub fn refresh(&self) {
        let observation = self.source.free_bytes();
        let mut state = self.state.lock().expect("space ledger poisoned");
        match observation {
            Some(free) => {
                state.free_observed = free;
                state.committed_since = 0;
                state.degraded = false;
            }
            None => {
                state.degraded = true;
            }
        }
    }

    fn available_locked(&self, state: &State) -> u64 {
        let spoken_for = state
            .reserved
            .saturating_add(state.committed_since)
            .saturating_add(self.policy.emergency_headroom);
        state.free_observed.saturating_sub(spoken_for)
    }

    /// Bytes available for new staging reservations after subtracting outstanding
    /// reservations, bytes committed since the last observation, and headroom.
    pub fn available(&self) -> u64 {
        let state = self.state.lock().expect("space ledger poisoned");
        self.available_locked(&state)
    }

    /// The current maintenance mode using low/high watermark hysteresis.
    pub fn mode(&self) -> Mode {
        let mut state = self.state.lock().expect("space ledger poisoned");
        let available = self.available_locked(&state);
        if state.reclaiming {
            if available >= self.policy.high_watermark {
                state.reclaiming = false;
                Mode::Normal
            } else {
                Mode::Reclaiming
            }
        } else if available < self.policy.low_watermark {
            state.reclaiming = true;
            Mode::Reclaiming
        } else {
            Mode::Normal
        }
    }

    /// Whether the ledger cannot presently trust its free-space observation. Readiness
    /// reports this so a node with a failing statvfs advertises itself as not ready.
    pub fn degraded(&self) -> bool {
        self.state.lock().expect("space ledger poisoned").degraded
    }

    pub fn free_observed(&self) -> u64 {
        self.state
            .lock()
            .expect("space ledger poisoned")
            .free_observed
    }

    pub fn reserved(&self) -> u64 {
        self.state.lock().expect("space ledger poisoned").reserved
    }

    pub fn committed_since(&self) -> u64 {
        self.state
            .lock()
            .expect("space ledger poisoned")
            .committed_since
    }
}

impl Reserver for SpaceLedger {
    fn try_reserve(&self, bytes: u64) -> bool {
        let mut state = self.state.lock().expect("space ledger poisoned");
        // Fail closed while the free-space observation cannot be trusted.
        if state.degraded {
            return false;
        }
        if self.available_locked(&state) < bytes {
            return false;
        }
        state.reserved = state.reserved.saturating_add(bytes);
        true
    }

    fn release(&self, bytes: u64) {
        let mut state = self.state.lock().expect("space ledger poisoned");
        state.reserved = state.reserved.saturating_sub(bytes);
    }

    fn commit(&self, bytes: u64) {
        let mut state = self.state.lock().expect("space ledger poisoned");
        state.reserved = state.reserved.saturating_sub(bytes);
        state.committed_since = state.committed_since.saturating_add(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixed(u64);
    impl FreeSpace for Fixed {
        fn free_bytes(&self) -> Option<u64> {
            Some(self.0)
        }
    }

    fn ledger(free: u64) -> SpaceLedger {
        SpaceLedger::new(
            Arc::new(Fixed(free)),
            SpacePolicy {
                low_watermark: 10,
                high_watermark: 20,
                emergency_headroom: 0,
            },
        )
    }

    #[test]
    fn concurrent_reservations_never_double_spend_the_same_capacity() {
        let ledger = ledger(100);
        // A known-length upload and an unknown-length extent both draw from the same
        // window; the ledger admits only what fits.
        assert!(ledger.try_reserve(60));
        assert!(ledger.try_reserve(40));
        assert!(!ledger.try_reserve(1));
        assert_eq!(ledger.reserved(), 100);

        // Committing frees the reservation slot but keeps the bytes accounted until the
        // next filesystem observation.
        ledger.commit(60);
        assert_eq!(ledger.reserved(), 40);
        assert!(!ledger.try_reserve(1));
        assert_eq!(ledger.committed_since(), 60);
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
        let ledger = SpaceLedger::new(
            Arc::new(Dynamic(Arc::clone(&source))),
            SpacePolicy {
                low_watermark: 10,
                high_watermark: 20,
                emergency_headroom: 0,
            },
        );
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
        let ledger = SpaceLedger::new(
            Arc::new(Failing),
            SpacePolicy {
                low_watermark: 10,
                high_watermark: 20,
                emergency_headroom: 0,
            },
        );
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
        let ledger = SpaceLedger::new(
            Arc::new(Maybe {
                free: Arc::clone(&source),
                fail: Arc::clone(&fail),
            }),
            SpacePolicy {
                low_watermark: 10,
                high_watermark: 20,
                emergency_headroom: 0,
            },
        );
        assert!(ledger.try_reserve(10));

        // The observation now fails: the ledger keeps the last free value for metrics
        // but refuses new reservations.
        fail.store(true, Ordering::SeqCst);
        ledger.refresh();
        assert!(ledger.degraded());
        assert_eq!(ledger.free_observed(), 100);
        assert!(!ledger.try_reserve(1));

        // Recovery clears the degraded state.
        fail.store(false, Ordering::SeqCst);
        ledger.refresh();
        assert!(!ledger.degraded());
        assert!(ledger.try_reserve(1));
    }
}
