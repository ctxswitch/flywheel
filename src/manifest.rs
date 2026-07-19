//! The session manifest: the prefetch vocabulary shared by the cacheprog helper
//! and the shard HTTP service.
//!
//! A manifest remembers which Go actions the previous build with the same session
//! label used. It is stored server-side under an ordinary build-cache key derived
//! from the label, so its whole lifecycle — discovery included — is plain cache
//! traffic: the helper GETs the key at startup, and its close merges usage back
//! with a GET and a PUT of the same key. Only the latest copy matters; a lost
//! manifest just makes the next build run cold before finalize rehydrates it.

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
pub const REQUEST_PREFETCH_CONCURRENCY_HEADER: &str = "x-flywheel-prefetch-concurrency";
/// Telemetry-only correlation label attached to prefetch object requests. The
/// value is the manifest key of the session being prefetched and never affects
/// routing or cache identity.
pub const REQUEST_SESSION_HEADER: &str = "x-flywheel-session";

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
