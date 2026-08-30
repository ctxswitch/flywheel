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
