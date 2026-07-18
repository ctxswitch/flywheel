mod artifact_files;

pub use artifact_files::LocalError;
pub(crate) use artifact_files::{ArtifactFiles, FilePublication, StageOutcome, StagedArtifact};

/// Disk-space accounting used by staging. Implemented by the cache's space ledger so
/// staging can reserve capacity before consuming it and release or commit it after.
/// A reserve request returns `false` when no safe capacity is available; the caller
/// then bypasses (build-cache) or fails (raw artifact) instead of risking exhaustion.
pub(crate) trait Reserver: Send + Sync {
    /// Attempts to reserve `bytes`. Returns `false` if capacity is unavailable.
    fn try_reserve(&self, bytes: u64) -> bool;
    /// Releases previously reserved bytes that were never committed to disk.
    fn release(&self, bytes: u64);
    /// Converts `bytes` of reservation into committed on-disk usage.
    fn commit(&self, bytes: u64);
}
