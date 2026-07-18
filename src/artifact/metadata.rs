use serde::{Deserialize, Serialize};

/// How an artifact body is encoded on disk. The artifact's identity (digest) and
/// `content_len` always describe the logical bytes; `Zstd` bodies are transparently
/// decompressed on serve unless the client negotiates the compressed representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StoredEncoding {
    Identity,
    Zstd,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub content_len: u64,
    pub content_type: Option<String>,
    pub created_at: u64,
    /// The soft eviction deadline and the identity of this artifact's current
    /// eviction-queue row. A queue candidate whose `eligible_at` differs from the
    /// artifact record is stale and can only be deleted.
    pub eligible_at: u64,
    /// The on-disk byte count — what eviction actually reclaims. Equal to
    /// `content_len` for `Identity` bodies.
    pub stored_len: u64,
    pub encoding: StoredEncoding,
}
