use crate::{artifact::ArtifactId, channel::ChannelId};
use std::hash::{Hash, Hasher};
use tokio::sync::{Mutex, MutexGuard};

/// Number of stripes per lock array. Fixed and independent of artifact cardinality
/// so mutation-serialization memory is bounded.
const STRIPES: usize = 512;

/// Fixed arrays of asynchronous mutexes that serialize only the final metadata and
/// file transition for one `( channel, digest)` or `( channel, reference)`.
/// Staging, hashing, reads, and physical unlink all run outside these locks.
pub(crate) struct Stripes {
    artifacts: Box<[Mutex<()>]>,
    references: Box<[Mutex<()>]>,
}

impl Stripes {
    pub(crate) fn new() -> Self {
        Self {
            artifacts: (0..STRIPES).map(|_| Mutex::new(())).collect(),
            references: (0..STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }

    pub(crate) async fn artifact(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
    ) -> MutexGuard<'_, ()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        channel.hash(&mut hasher);
        artifact.digest().as_bytes().hash(&mut hasher);
        self.artifacts[(hasher.finish() as usize) % self.artifacts.len()]
            .lock()
            .await
    }

    pub(crate) async fn reference(
        &self,
        channel: ChannelId,
        reference: &str,
    ) -> MutexGuard<'_, ()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        channel.hash(&mut hasher);
        reference.hash(&mut hasher);
        self.references[(hasher.finish() as usize) % self.references.len()]
            .lock()
            .await
    }
}
