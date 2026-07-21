use super::{Candidate, Durability, Evicted, MetadataError, ReferenceRecord};
use crate::{
    artifact::{ArtifactId, ArtifactMetadata, Digest},
    channel::{ChannelId, ChannelRecord},
    storage::records::{decode_record, encode_record},
};
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options, WriteBatch, WriteOptions};
use std::{path::Path, sync::Arc};

const META: &str = "meta";
const ARTIFACTS: &str = "artifacts";
const REFERENCES: &str = "references";
const EVICTION: &str = "eviction";
const CHANNELS: &str = "channels";
/// Fixed key in the `meta` column family recording the durable store format. A
/// point read on open rejects an incompatible development store without scanning it.
const STORE_FORMAT_KEY: &[u8] = b"store-format";
const STORE_FORMAT: &[u8] = b"flywheel-channel-cache-v1";

pub(crate) struct RocksMetadata {
    database: Arc<DB>,
}

impl RocksMetadata {
    pub(crate) async fn open(path: impl AsRef<Path>) -> Result<Self, MetadataError> {
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut options = Options::default();
            options.create_if_missing(true);
            options.create_missing_column_families(true);
            let families = [META, ARTIFACTS, REFERENCES, EVICTION, CHANNELS]
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
            let database = DB::open_cf_descriptors(&options, path, families)
                .map(Arc::new)
                .map_err(store_error)?;
            verify_store_format(&database)?;
            Ok(Self { database })
        })
        .await?
    }
}

/// Rejects a store whose recorded format differs from the one this build writes. A
/// fresh store records the marker; a matching store is accepted without a scan.
fn verify_store_format(database: &DB) -> Result<(), MetadataError> {
    let meta = database.cf_handle(META).ok_or_else(missing_cf)?;
    match database
        .get_cf(&meta, STORE_FORMAT_KEY)
        .map_err(store_error)?
    {
        Some(recorded) if recorded == STORE_FORMAT => Ok(()),
        Some(recorded) => Err(MetadataError::IncompatibleStore(
            String::from_utf8_lossy(&recorded).into_owned(),
        )),
        None => {
            let mut batch = WriteBatch::default();
            batch.put_cf(&meta, STORE_FORMAT_KEY, STORE_FORMAT);
            write_sync(database, batch)
        }
    }
}

impl RocksMetadata {
    pub(crate) async fn health(&self) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            database
                .property_int_value("rocksdb.estimate-num-keys")
                .map_err(store_error)?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn commit_publication(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
        metadata: ArtifactMetadata,
        reference: Option<(String, ReferenceRecord)>,
        durability: Durability,
    ) -> Result<bool, MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let artifacts = database.cf_handle(ARTIFACTS).ok_or_else(missing_cf)?;
            let eviction = database.cf_handle(EVICTION).ok_or_else(missing_cf)?;
            let references = database.cf_handle(REFERENCES).ok_or_else(missing_cf)?;
            let key = artifact_key(channel, artifact);
            let exists = database
                .get_cf(&artifacts, &key)
                .map_err(store_error)?
                .is_some();
            let mut batch = WriteBatch::default();
            if !exists {
                batch.put_cf(&artifacts, &key, encode_record(&metadata)?);
                batch.put_cf(
                    &eviction,
                    eviction_key(channel, metadata.eligible_at, artifact),
                    // The queue row is its key: deadline and artifact are both encoded
                    // there and the artifact record holds everything else.
                    b"",
                );
            }
            if let Some((name, record)) = reference {
                batch.put_cf(
                    &references,
                    reference_key(channel, &name),
                    encode_record(&record)?,
                );
            }
            write(&database, batch, durability)?;
            Ok(!exists)
        })
        .await
    }

    pub(crate) async fn artifact(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
    ) -> Result<Option<ArtifactMetadata>, MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let family = database.cf_handle(ARTIFACTS).ok_or_else(missing_cf)?;
            database
                .get_cf(&family, artifact_key(channel, artifact))
                .map_err(store_error)?
                .map(|bytes| decode_record(&bytes).map_err(MetadataError::from))
                .transpose()
        })
        .await
    }

    pub(crate) async fn remove_artifact(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
    ) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let artifacts = database.cf_handle(ARTIFACTS).ok_or_else(missing_cf)?;
            let eviction = database.cf_handle(EVICTION).ok_or_else(missing_cf)?;
            let key = artifact_key(channel, artifact);
            let mut batch = WriteBatch::default();
            if let Some(bytes) = database.get_cf(&artifacts, &key).map_err(store_error)? {
                let metadata: ArtifactMetadata = decode_record(&bytes)?;
                batch.delete_cf(
                    &eviction,
                    eviction_key(channel, metadata.eligible_at, artifact),
                );
            }
            batch.delete_cf(&artifacts, &key);
            // The same delete `evict` performs, and for the same reason it needs no
            // fsync: losing the removal leaves a metadata row whose body is already
            // gone, which the read path self-heals into a miss.
            database.write(batch).map_err(store_error)
        })
        .await
    }

    pub(crate) async fn bind_reference(
        &self,
        channel: ChannelId,
        reference: String,
        record: ReferenceRecord,
        durability: Durability,
    ) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let references = database.cf_handle(REFERENCES).ok_or_else(missing_cf)?;
            // No artifact-existence check: a reference is a non-pinning alias that
            // may dangle (its target can be evicted after binding, or live on a
            // different shard behind the routing agent). Resolving a dangling
            // reference is an ordinary cache miss.
            let mut batch = WriteBatch::default();
            batch.put_cf(
                &references,
                reference_key(channel, &reference),
                encode_record(&record)?,
            );
            write(&database, batch, durability)
        })
        .await
    }

    pub(crate) async fn reference(
        &self,
        channel: ChannelId,
        reference: &str,
    ) -> Result<Option<ReferenceRecord>, MetadataError> {
        let database = Arc::clone(&self.database);
        let key = reference_key(channel, reference);
        blocking(move || {
            let family = database.cf_handle(REFERENCES).ok_or_else(missing_cf)?;
            database
                .get_cf(&family, key)
                .map_err(store_error)?
                .map(|bytes| decode_record(&bytes).map_err(MetadataError::from))
                .transpose()
        })
        .await
    }

    pub(crate) async fn delete_reference(
        &self,
        channel: ChannelId,
        reference: &str,
    ) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        let key = reference_key(channel, reference);
        blocking(move || {
            let references = database.cf_handle(REFERENCES).ok_or_else(missing_cf)?;
            let mut batch = WriteBatch::default();
            batch.delete_cf(&references, key);
            write_sync(&database, batch)
        })
        .await
    }

    /// Reads up to `limit` ordered heads of the  channel's eviction queue.
    pub(crate) async fn reclaim_candidates(
        &self,
        channel: ChannelId,
        limit: usize,
    ) -> Result<Vec<Candidate>, MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let eviction = database.cf_handle(EVICTION).ok_or_else(missing_cf)?;
            let prefix = channel.as_key();
            let mut candidates = Vec::new();
            for item in database.iterator_cf(
                &eviction,
                IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            ) {
                let (key, _) = item.map_err(store_error)?;
                if !key.starts_with(&prefix) || candidates.len() >= limit {
                    break;
                }
                let (eligible_at, artifact) = decode_eviction_key(&key)?;
                candidates.push(Candidate {
                    eligible_at,
                    artifact,
                });
            }
            Ok(candidates)
        })
        .await
    }

    /// Atomically moves an artifact's eviction-queue row to a new soft deadline and
    /// updates its `eligible_at`. A no-op if the artifact no longer matches
    /// `old_eligible_at`.
    pub(crate) async fn requeue(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
        old_eligible_at: u64,
        new_eligible_at: u64,
    ) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let artifacts = database.cf_handle(ARTIFACTS).ok_or_else(missing_cf)?;
            let eviction = database.cf_handle(EVICTION).ok_or_else(missing_cf)?;
            let key = artifact_key(channel, artifact);
            let Some(bytes) = database.get_cf(&artifacts, &key).map_err(store_error)? else {
                return Ok(());
            };
            let mut metadata: ArtifactMetadata = decode_record(&bytes)?;
            if metadata.eligible_at != old_eligible_at {
                return Ok(());
            }
            let mut batch = WriteBatch::default();
            batch.delete_cf(&eviction, eviction_key(channel, old_eligible_at, artifact));
            metadata.eligible_at = new_eligible_at;
            batch.put_cf(
                &eviction,
                eviction_key(channel, new_eligible_at, artifact),
                b"",
            );
            batch.put_cf(&artifacts, &key, encode_record(&metadata)?);
            database.write(batch).map_err(store_error)
        })
        .await
    }

    /// Removes artifact visibility and its queue row in one non-sync batch. Detects and
    /// drops a stale queue row without removing a live artifact.
    pub(crate) async fn evict(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
        eligible_at: u64,
    ) -> Result<Evicted, MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let artifacts = database.cf_handle(ARTIFACTS).ok_or_else(missing_cf)?;
            let eviction = database.cf_handle(EVICTION).ok_or_else(missing_cf)?;
            let key = artifact_key(channel, artifact);
            let mut batch = WriteBatch::default();
            let outcome = match database.get_cf(&artifacts, &key).map_err(store_error)? {
                Some(bytes) => {
                    let metadata: ArtifactMetadata = decode_record(&bytes)?;
                    if metadata.eligible_at != eligible_at {
                        // The artifact was requeued; this queue row is stale.
                        batch.delete_cf(&eviction, eviction_key(channel, eligible_at, artifact));
                        Evicted::Stale
                    } else {
                        batch.delete_cf(&artifacts, &key);
                        batch.delete_cf(&eviction, eviction_key(channel, eligible_at, artifact));
                        Evicted::Removed {
                            stored_len: metadata.stored_len,
                        }
                    }
                }
                None => {
                    batch.delete_cf(&eviction, eviction_key(channel, eligible_at, artifact));
                    Evicted::Stale
                }
            };
            database.write(batch).map_err(store_error)?;
            Ok(outcome)
        })
        .await
    }
}

impl RocksMetadata {
    pub(crate) async fn create_channel(&self, channel: ChannelRecord) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let family = database.cf_handle(CHANNELS).ok_or_else(missing_cf)?;
            let key = channel.id.as_key();
            if database
                .get_cf(&family, key)
                .map_err(store_error)?
                .is_some()
            {
                return Err(MetadataError::AlreadyExists);
            }
            let mut batch = WriteBatch::default();
            batch.put_cf(&family, key, encode_record(&channel)?);
            write_sync(&database, batch)
        })
        .await
    }

    pub(crate) async fn channel(
        &self,
        id: ChannelId,
    ) -> Result<Option<ChannelRecord>, MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let family = database.cf_handle(CHANNELS).ok_or_else(missing_cf)?;
            database
                .get_cf(&family, id.as_key())
                .map_err(store_error)?
                .map(|bytes| decode_record(&bytes).map_err(MetadataError::from))
                .transpose()
        })
        .await
    }

    pub(crate) async fn store_channel(&self, channel: ChannelRecord) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let family = database.cf_handle(CHANNELS).ok_or_else(missing_cf)?;
            let mut batch = WriteBatch::default();
            batch.put_cf(&family, channel.id.as_key(), encode_record(&channel)?);
            write_sync(&database, batch)
        })
        .await
    }

    pub(crate) async fn channels(&self) -> Result<Vec<ChannelRecord>, MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let family = database.cf_handle(CHANNELS).ok_or_else(missing_cf)?;
            database
                .iterator_cf(&family, IteratorMode::Start)
                .map(|item| {
                    let (_, bytes) = item.map_err(store_error)?;
                    decode_record::<ChannelRecord>(&bytes).map_err(MetadataError::from)
                })
                .collect()
        })
        .await
    }

    pub(crate) async fn delete_channel_data(&self, id: ChannelId) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let prefix = id.as_key();
            let end = prefix_end(prefix);
            let mut batch = WriteBatch::default();
            for name in [ARTIFACTS, REFERENCES, EVICTION] {
                let family = database.cf_handle(name).ok_or_else(missing_cf)?;
                batch.delete_range_cf(&family, prefix, end);
            }
            write_sync(&database, batch)
        })
        .await
    }

    pub(crate) async fn finish_channel_deletion(&self, id: ChannelId) -> Result<(), MetadataError> {
        let database = Arc::clone(&self.database);
        blocking(move || {
            let channels = database.cf_handle(CHANNELS).ok_or_else(missing_cf)?;
            let mut batch = WriteBatch::default();
            batch.delete_cf(&channels, id.as_key());
            write_sync(&database, batch)
        })
        .await
    }
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, MetadataError> + Send + 'static,
) -> Result<T, MetadataError> {
    tokio::task::spawn_blocking(operation).await?
}

fn write(database: &DB, batch: WriteBatch, durability: Durability) -> Result<(), MetadataError> {
    match durability {
        Durability::Durable => write_sync(database, batch),
        Durability::BestEffort => database.write(batch).map_err(store_error),
    }
}

fn write_sync(database: &DB, batch: WriteBatch) -> Result<(), MetadataError> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    database.write_opt(batch, &options).map_err(store_error)
}

fn artifact_key(channel: ChannelId, artifact: ArtifactId) -> Vec<u8> {
    let prefix = channel.as_key();
    let digest = artifact.digest();
    let digest = digest.as_bytes();
    let mut key = Vec::with_capacity(prefix.len() + digest.len());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(digest);
    key
}

fn reference_key(channel: ChannelId, reference: &str) -> Vec<u8> {
    let prefix = channel.as_key();
    let mut key = Vec::with_capacity(prefix.len() + reference.len());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(reference.as_bytes());
    key
}

fn eviction_key(channel: ChannelId, eligible_at: u64, artifact: ArtifactId) -> Vec<u8> {
    let prefix = channel.as_key();
    let digest = artifact.digest();
    let digest = digest.as_bytes();
    let mut key = Vec::with_capacity(prefix.len() + 8 + digest.len());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(&eligible_at.to_be_bytes());
    key.extend_from_slice(digest);
    key
}

/// Splits a queue key into its deadline and artifact. The caller has already matched
/// the channel prefix, so the whole-key length is the only thing left to check.
fn decode_eviction_key(key: &[u8]) -> Result<(u64, ArtifactId), MetadataError> {
    const DEADLINE_AT: usize = ulid::ULID_LEN;
    const DIGEST_AT: usize = DEADLINE_AT + 8;
    let key: &[u8; DIGEST_AT + 32] = key
        .try_into()
        .map_err(|_| MetadataError::Store("invalid eviction key".to_owned()))?;
    let mut deadline = [0_u8; 8];
    deadline.copy_from_slice(&key[DEADLINE_AT..DIGEST_AT]);
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(&key[DIGEST_AT..]);
    Ok((
        u64::from_be_bytes(deadline),
        ArtifactId::from_digest(Digest::from_bytes(digest)),
    ))
}

fn missing_cf() -> MetadataError {
    MetadataError::Store("missing RocksDB column family".to_owned())
}

fn store_error(error: impl std::fmt::Display) -> MetadataError {
    MetadataError::Store(error.to_string())
}

/// The exclusive upper bound of a channel's key range. A channel key is canonical
/// Crockford base32 text, so its last byte is an ASCII digit or capital letter and
/// incrementing it can never carry.
fn prefix_end(mut prefix: [u8; ulid::ULID_LEN]) -> [u8; ulid::ULID_LEN] {
    prefix[ulid::ULID_LEN - 1] += 1;
    prefix
}
