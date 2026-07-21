mod rocksdb;

pub(crate) use rocksdb::RocksMetadata;

use crate::artifact::ArtifactId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceRecord {
    pub artifact: ArtifactId,
    pub updated_at: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// Whether a metadata batch must be synchronously flushed before it is
/// acknowledged. Raw artifact and Bazel CAS publications are `Durable`; build-cache
/// and proxy publications — and the reference bindings that accompany them, including
/// the rebind on an upstream 304 — are `BestEffort` because the body is already
/// complete and any crash outcome is a hit, a self-healing miss, or an orphan file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    Durable,
    BestEffort,
}

/// A candidate row from the eviction queue: its soft deadline and artifact identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Candidate {
    pub eligible_at: u64,
    pub artifact: ArtifactId,
}

/// The outcome of an eviction attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Evicted {
    /// The artifact was removed; its body occupied `stored_len` on-disk bytes (the
    /// caller unlinks the body before this metadata delete).
    Removed { stored_len: u64 },
    /// Nothing live was removed (the artifact was already gone or the queue row was
    /// stale). Any stale queue row was deleted.
    Stale,
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("metadata store failed: {0}")]
    Store(String),
    #[error("durable store format is incompatible: {0}")]
    IncompatibleStore(String),
    #[error("channel already exists")]
    AlreadyExists,
    #[error("durable record failed: {0}")]
    Record(#[from] crate::storage::records::RecordError),
    #[error("metadata task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}
