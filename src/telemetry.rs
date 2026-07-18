use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    requests: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    authorization_denials: AtomicU64,
    maintenance_reclaimed: AtomicU64,
    maintenance_requeued: AtomicU64,
    build_cache_bypasses: AtomicU64,
    raw_pressure_errors: AtomicU64,
    free_observed_bytes: AtomicU64,
    reserved_bytes: AtomicU64,
    committed_since_bytes: AtomicU64,
}

impl Metrics {
    pub(crate) fn request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn hit(&self, bytes: u64) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn authorization_denial(&self) {
        self.authorization_denials.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn reclaimed(&self, count: u64) {
        self.maintenance_reclaimed
            .fetch_add(count, Ordering::Relaxed);
    }
    pub(crate) fn requeued(&self) {
        self.maintenance_requeued.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn build_cache_bypass(&self) {
        self.build_cache_bypasses.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn raw_pressure_error(&self) {
        self.raw_pressure_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_space(&self, free_observed: u64, reserved: u64, committed_since: u64) {
        self.free_observed_bytes
            .store(free_observed, Ordering::Relaxed);
        self.reserved_bytes.store(reserved, Ordering::Relaxed);
        self.committed_since_bytes
            .store(committed_since, Ordering::Relaxed);
    }

    pub fn render(&self) -> String {
        format!(
            concat!(
                "# TYPE flywheel_requests_total counter\nflywheel_requests_total {}\n",
                "# TYPE flywheel_artifact_hits_total counter\nflywheel_artifact_hits_total {}\n",
                "# TYPE flywheel_artifact_misses_total counter\nflywheel_artifact_misses_total {}\n",
                "# TYPE flywheel_bytes_read_total counter\nflywheel_bytes_read_total {}\n",
                "# TYPE flywheel_bytes_written_total counter\nflywheel_bytes_written_total {}\n",
                "# TYPE flywheel_authorization_denials_total counter\nflywheel_authorization_denials_total {}\n",
                "# TYPE flywheel_maintenance_reclaimed_total counter\nflywheel_maintenance_reclaimed_total {}\n",
                "# TYPE flywheel_maintenance_requeued_total counter\nflywheel_maintenance_requeued_total {}\n",
                "# TYPE flywheel_build_cache_bypasses_total counter\nflywheel_build_cache_bypasses_total {}\n",
                "# TYPE flywheel_raw_pressure_errors_total counter\nflywheel_raw_pressure_errors_total {}\n",
                "# TYPE flywheel_free_observed_bytes gauge\nflywheel_free_observed_bytes {}\n",
                "# TYPE flywheel_reserved_bytes gauge\nflywheel_reserved_bytes {}\n",
                "# TYPE flywheel_committed_since_bytes gauge\nflywheel_committed_since_bytes {}\n",
            ),
            self.requests.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.bytes_read.load(Ordering::Relaxed),
            self.bytes_written.load(Ordering::Relaxed),
            self.authorization_denials.load(Ordering::Relaxed),
            self.maintenance_reclaimed.load(Ordering::Relaxed),
            self.maintenance_requeued.load(Ordering::Relaxed),
            self.build_cache_bypasses.load(Ordering::Relaxed),
            self.raw_pressure_errors.load(Ordering::Relaxed),
            self.free_observed_bytes.load(Ordering::Relaxed),
            self.reserved_bytes.load(Ordering::Relaxed),
            self.committed_since_bytes.load(Ordering::Relaxed),
        )
    }
}
