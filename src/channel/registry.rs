use super::{Access, ChannelId, Lifecycle};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChannelRecord {
    pub id: ChannelId,
    pub access: Access,
    pub expiry_seconds: u64,
    pub state: Lifecycle,
    pub created_at: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelStoreError {
    #[error("channel registry failed: {0}")]
    Store(String),
    #[error("channel already exists")]
    AlreadyExists,
    #[error("durable record failed: {0}")]
    Record(#[from] crate::storage::records::RecordError),
    #[error("channel registry task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}
