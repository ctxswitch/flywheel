//! The session manifest: the prefetch vocabulary shared by the cacheprog helper,
//! the shard HTTP service, and the routing agent.
//!
//! A manifest remembers which Go actions the previous build with the same session
//! label used. It is stored server-side under an ordinary build-cache key derived
//! from the label, so its lifecycle is plain cache traffic. Shards answer
//! `GET /status?session=` with whatever manifest they hold locally, and the agent
//! merges those per-shard answers into one view with [`merge_manifests`].

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;

pub const MANIFEST_VERSION: u32 = 1;
pub const MANIFEST_MAX_ENTRIES: usize = 32 * 1024;
pub const MANIFEST_MAX_AGE_SECONDS: u64 = 14 * 24 * 60 * 60;

/// Telemetry-only header naming why a request was sent. The agent counts
/// speculative traffic separately from foreground traffic; the header never
/// changes routing, admission, authorization, or the response.
pub const REQUEST_PURPOSE_HEADER: &str = "x-flywheel-request-purpose";
pub const REQUEST_PURPOSE_PREFETCH: &str = "prefetch";

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct Manifest {
    pub version: u32,
    pub entries: HashMap<String, ManifestEntry>,
}

impl Manifest {
    /// The current-version manifest with no predictions: the universal
    /// "know nothing" answer every failure path degrades to.
    pub fn empty() -> Self {
        Self {
            version: MANIFEST_VERSION,
            entries: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ManifestEntry {
    pub output: String,
    pub size: u64,
    pub last_seen: u64,
}

/// The build-cache key naming the manifest for a session label. Every party
/// derives it the same way, so the label itself never appears on the wire as a
/// cache key.
pub fn manifest_key(label: &str) -> String {
    format!(
        "go-manifest-{}",
        hex::encode(Sha256::digest(label.as_bytes()))
    )
}

/// Merges shard-local manifests into one view: union per Go action, and where
/// shards disagree the most recently seen entry wins — the same recency rule the
/// session merge uses. A version-mismatched input contributes nothing.
pub fn merge_manifests(manifests: impl IntoIterator<Item = Manifest>) -> Manifest {
    let mut merged = Manifest::empty();
    for manifest in manifests {
        if manifest.version != MANIFEST_VERSION {
            continue;
        }
        for (action, entry) in manifest.entries {
            match merged.entries.get(&action) {
                Some(existing) if existing.last_seen >= entry.last_seen => {}
                _ => {
                    merged.entries.insert(action, entry);
                }
            }
        }
    }
    merged
}
