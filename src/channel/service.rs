use super::{Access, ChannelId, ChannelRecord, ChannelToken, Lifecycle};
use crate::storage::{
    local::{ArtifactFiles, LocalError},
    metadata::{MetadataError, RocksMetadata},
};
use dashmap::DashMap;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// The per-channel lifecycle gates, shared between the channel service (which takes the
/// exclusive write side to delete a channel) and the cache service (which takes the
/// shared read side to fence a final mutation against deletion). Sharing one registry
/// is what lets `commit_staged` and reference writes recheck `active` under the same
/// lock a deletion must acquire.
#[derive(Default)]
pub struct ChannelGates {
    gates: DashMap<ChannelId, Arc<RwLock<()>>>,
}

impl ChannelGates {
    pub fn gate(&self, id: ChannelId) -> Arc<RwLock<()>> {
        Arc::clone(
            self.gates
                .entry(id)
                .or_insert_with(|| Arc::new(RwLock::new(())))
                .value(),
        )
    }

    /// Drops a channel's gate after its deletion has fully finished. A concurrent
    /// operation that already holds an `Arc` clone keeps its own gate alive; a later
    /// lookup simply creates a fresh, uncontended gate for the (now absent) channel.
    pub fn forget(&self, id: ChannelId) {
        self.gates.remove(&id);
    }
}

pub struct ChannelService {
    store: Arc<RocksMetadata>,
    files: Arc<ArtifactFiles>,
    gates: Arc<ChannelGates>,
}

pub struct IssuedChannel {
    pub record: ChannelRecord,
    pub token: Option<ChannelToken>,
}

pub struct ChannelLease {
    pub record: ChannelRecord,
    _guard: OwnedRwLockReadGuard<()>,
}

impl ChannelService {
    pub(crate) fn new(
        store: Arc<RocksMetadata>,
        files: Arc<ArtifactFiles>,
        gates: Arc<ChannelGates>,
    ) -> Self {
        Self {
            store,
            files,
            gates,
        }
    }

    pub async fn ensure_default(&self, expiry_seconds: u64) -> Result<(), ChannelError> {
        let record = match self.store.channel(ChannelId::DEFAULT).await? {
            Some(record) => record,
            None => {
                let record = ChannelRecord {
                    id: ChannelId::DEFAULT,
                    access: Access::Open,
                    expiry_seconds,
                    state: Lifecycle::Active,
                    created_at: unix_time(),
                };
                match self.store.create_channel(record.clone()).await {
                    Ok(()) => record,
                    Err(MetadataError::AlreadyExists) => self
                        .store
                        .channel(ChannelId::DEFAULT)
                        .await?
                        .ok_or(ChannelError::InvalidDefault)?,
                    Err(error) => return Err(error.into()),
                }
            }
        };
        if record.access != Access::Open || record.state != Lifecycle::Active {
            return Err(ChannelError::InvalidDefault);
        }
        self.gates.gate(ChannelId::DEFAULT);
        Ok(())
    }

    pub async fn register(
        &self,
        protected: bool,
        expiry_seconds: u64,
    ) -> Result<IssuedChannel, ChannelError> {
        let token = protected.then(ChannelToken::generate);
        let record = ChannelRecord {
            id: ChannelId::new(),
            access: token
                .as_ref()
                .map_or(Access::Open, |token| Access::Token(token.digest())),
            expiry_seconds,
            state: Lifecycle::Active,
            created_at: unix_time(),
        };
        self.store.create_channel(record.clone()).await?;
        self.gates.gate(record.id);
        Ok(IssuedChannel { record, token })
    }

    pub async fn authorize_with_lease(
        &self,
        id: ChannelId,
        credential: Option<&str>,
    ) -> Result<ChannelLease, ChannelError> {
        let Some(record) = self.store.channel(id).await? else {
            return Err(ChannelError::NotFound);
        };
        authorize_record(&record, credential)?;
        if record.state != Lifecycle::Active {
            return Err(ChannelError::Deleting);
        }
        let guard = self.gates.gate(id).read_owned().await;
        let Some(current) = self.store.channel(id).await? else {
            return Err(ChannelError::NotFound);
        };
        authorize_record(&current, credential)?;
        if current.state != Lifecycle::Active {
            return Err(ChannelError::Deleting);
        }
        Ok(ChannelLease {
            record: current,
            _guard: guard,
        })
    }

    /// Validates channel credentials and `active` state without holding the lifecycle
    /// gate. Ordinary data requests use this so reads remain lock-free and uploads do
    /// not block deletion while staging; final mutations acquire their own fence.
    pub async fn authorize(
        &self,
        id: ChannelId,
        credential: Option<&str>,
    ) -> Result<ChannelRecord, ChannelError> {
        let Some(record) = self.store.channel(id).await? else {
            return Err(ChannelError::NotFound);
        };
        authorize_record(&record, credential)?;
        if record.state != Lifecycle::Active {
            return Err(ChannelError::Deleting);
        }
        Ok(record)
    }

    pub async fn update_expiry(
        &self,
        lease: &mut ChannelLease,
        expiry_seconds: u64,
    ) -> Result<(), ChannelError> {
        lease.record.expiry_seconds = expiry_seconds;
        self.store.store_channel(lease.record.clone()).await?;
        Ok(())
    }

    pub async fn delete(
        &self,
        id: ChannelId,
        credential: Option<&str>,
    ) -> Result<(), ChannelError> {
        if id == ChannelId::DEFAULT {
            return Err(ChannelError::DefaultChannel);
        }
        let lease = self.authorize_with_lease(id, credential).await?;
        let mut deleting = lease.record.clone();
        deleting.state = Lifecycle::Deleting;
        self.store.store_channel(deleting).await?;
        drop(lease);

        let gate = self.gates.gate(id);
        let _exclusive = gate.write().await;
        self.finish_deletion(id).await
    }

    pub async fn resume_deletions(&self) -> Result<(), ChannelError> {
        let mut deleting = self.store.channels().await?;
        deleting.retain(|channel| channel.state == Lifecycle::Deleting);
        for channel in deleting {
            let gate = self.gates.gate(channel.id);
            let _exclusive = gate.write().await;
            self.finish_deletion(channel.id).await?;
        }
        Ok(())
    }

    async fn finish_deletion(&self, id: ChannelId) -> Result<(), ChannelError> {
        self.store.delete_channel_data(id).await?;
        self.files.remove_channel(id).await?;
        self.store.finish_channel_deletion(id).await?;
        self.gates.forget(id);
        Ok(())
    }
}

fn authorize_record(record: &ChannelRecord, credential: Option<&str>) -> Result<(), ChannelError> {
    match record.access {
        Access::Open => {}
        Access::Token(digest) if credential.is_some_and(|candidate| digest.verify(candidate)) => {}
        Access::Token(_) => return Err(ChannelError::Unauthorized),
    }
    Ok(())
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("channel does not exist")]
    NotFound,
    #[error("channel credentials are missing or incorrect")]
    Unauthorized,
    #[error("channel is being deleted")]
    Deleting,
    #[error("the default channel cannot be deleted")]
    DefaultChannel,
    #[error("the persisted default channel violates its invariants")]
    InvalidDefault,
    #[error(transparent)]
    Store(#[from] MetadataError),
    #[error(transparent)]
    Local(#[from] LocalError),
}
