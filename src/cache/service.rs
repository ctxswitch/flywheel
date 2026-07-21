use crate::{
    artifact::{ArtifactId, ArtifactMetadata, StoredEncoding},
    cache::recent_use::RecentUse,
    cache::space::{Mode, SpaceLedger},
    cache::stripes::Stripes,
    channel::{ChannelGates, ChannelId, ChannelStoreError, Lifecycle},
    clock::Clock,
    storage::{
        local::{ArtifactFiles, FilePublication, LocalError, StageOutcome, StagedArtifact},
        metadata::{Durability, Evicted, MetadataError, ReferenceRecord, RocksMetadata},
    },
    telemetry::Metrics,
};
use bytes::Bytes;
use futures_util::Stream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::OwnedRwLockReadGuard;

pub struct CacheService {
    metadata: Arc<RocksMetadata>,
    files: Arc<ArtifactFiles>,
    max_upload_bytes: u64,
    space: Arc<SpaceLedger>,
    recent_use: RecentUse,
    stripes: Stripes,
    reclaim_byte_limit: u64,
    orphan_scan_limit: usize,
    maintenance_cursor: AtomicUsize,
    channel_gates: Arc<ChannelGates>,
    clock: Arc<dyn Clock>,
    metrics: Arc<Metrics>,
}

pub(crate) struct CacheDependencies {
    pub metadata: Arc<RocksMetadata>,
    pub files: Arc<ArtifactFiles>,
    pub max_upload_bytes: u64,
    pub space: Arc<SpaceLedger>,
    pub channel_gates: Arc<ChannelGates>,
    pub clock: Arc<dyn Clock>,
    pub metrics: Arc<Metrics>,
    pub bloom_bits: usize,
    pub reclaim_byte_limit: u64,
    pub orphan_scan_limit: usize,
}

pub struct LocatedArtifact {
    pub(crate) file: tokio::fs::File,
    pub metadata: ArtifactMetadata,
}

pub enum PublicationTarget {
    ById(ArtifactId),
    ContentAddressed { reference: Option<String> },
}

pub struct PublishRequest<S> {
    pub channel: ChannelId,
    pub target: PublicationTarget,
    pub content_type: Option<String>,
    pub stream: S,
    pub content_length: Option<u64>,
    pub durability: Durability,
    pub encoding: StoredEncoding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationOutcome {
    Created,
    Existing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Publication {
    pub artifact: ArtifactId,
    pub outcome: PublicationOutcome,
}

/// The result of an admission-aware publication. `Rejected` returns the untouched body
/// stream so the caller can pass it through when the reservation is refused.
pub enum Admission<S> {
    Published(Publication),
    Rejected(S),
}

/// The metadata a staged body commits alongside its final rename.
struct Commit {
    content_type: Option<String>,
    reference: Option<(String, ReferenceRecord)>,
    durability: Durability,
}

impl CacheService {
    pub(crate) fn new(dependencies: CacheDependencies) -> Self {
        Self {
            metadata: dependencies.metadata,
            files: dependencies.files,
            max_upload_bytes: dependencies.max_upload_bytes,
            space: dependencies.space,
            recent_use: RecentUse::new(dependencies.bloom_bits),
            stripes: Stripes::new(),
            reclaim_byte_limit: dependencies.reclaim_byte_limit,
            orphan_scan_limit: dependencies.orphan_scan_limit,
            maintenance_cursor: AtomicUsize::new(0),
            channel_gates: dependencies.channel_gates,
            clock: dependencies.clock,
            metrics: dependencies.metrics,
        }
    }

    pub async fn publish<S, E>(&self, request: PublishRequest<S>) -> Result<Publication, CacheError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        match self.publish_impl(request).await? {
            Admission::Published(publication) => Ok(publication),
            Admission::Rejected(_) => Err(CacheError::Local(LocalError::OutOfSpace)),
        }
    }

    /// Content-addressed publication that hands the body back instead of failing when
    /// the up-front reservation is rejected under disk pressure. The package proxy uses
    /// this so a bypassed download streams the untouched body straight to the client —
    /// never buffered, never re-fetched.
    pub async fn publish_or_reject<S, E>(
        &self,
        request: PublishRequest<S>,
    ) -> Result<Admission<S>, CacheError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.publish_impl(request).await
    }

    async fn publish_impl<S, E>(
        &self,
        request: PublishRequest<S>,
    ) -> Result<Admission<S>, CacheError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let PublishRequest {
            channel,
            target,
            content_type,
            stream,
            content_length,
            durability,
            encoding,
        } = request;
        let staged = match self
            .files
            .stage(
                channel,
                stream,
                self.max_upload_bytes,
                content_length,
                self.space.clone(),
                durability,
                encoding,
            )
            .await?
        {
            StageOutcome::Ready(staged) => staged,
            StageOutcome::Rejected(stream) => return Ok(Admission::Rejected(stream)),
        };
        self.metrics.written(staged.len);
        let (artifact, reference) = match target {
            PublicationTarget::ById(artifact) => (artifact, None),
            PublicationTarget::ContentAddressed { reference } => {
                let artifact = ArtifactId::from_digest(staged.digest);
                let reference = reference.map(|reference| {
                    (
                        reference,
                        ReferenceRecord {
                            artifact,
                            updated_at: self.clock.now(),
                            etag: None,
                            last_modified: None,
                        },
                    )
                });
                (artifact, reference)
            }
        };
        let publication = self
            .commit_staged(
                channel,
                artifact,
                staged,
                Commit {
                    content_type,
                    reference,
                    durability,
                },
            )
            .await?;
        Ok(Admission::Published(publication))
    }

    pub async fn locate(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
    ) -> Result<Option<LocatedArtifact>, CacheError> {
        if let Some(located) = self.locate_local(channel, artifact).await? {
            self.recent_use.mark(channel, artifact);
            self.metrics.hit(located.metadata.content_len);
            return Ok(Some(located));
        }
        self.metrics.miss();
        Ok(None)
    }

    pub async fn bind_reference(
        &self,
        channel: ChannelId,
        reference: String,
        artifact: ArtifactId,
    ) -> Result<(), CacheError> {
        self.bind_reference_with_validators(
            channel,
            reference,
            artifact,
            None,
            None,
            Durability::Durable,
        )
        .await
    }

    pub(crate) async fn bind_reference_with_validators(
        &self,
        channel: ChannelId,
        reference: String,
        artifact: ArtifactId,
        etag: Option<String>,
        last_modified: Option<String>,
        durability: Durability,
    ) -> Result<(), CacheError> {
        let (_gate, _) = self.channel_fence(channel).await?;
        let _stripe = self.stripes.reference(channel, &reference).await;
        self.metadata
            .bind_reference(
                channel,
                reference,
                ReferenceRecord {
                    artifact,
                    updated_at: self.clock.now(),
                    etag,
                    last_modified,
                },
                durability,
            )
            .await?;
        Ok(())
    }

    pub async fn resolve_reference(
        &self,
        channel: ChannelId,
        reference: &str,
    ) -> Result<Option<ReferenceRecord>, CacheError> {
        Ok(self.metadata.reference(channel, reference).await?)
    }

    pub async fn delete_reference(
        &self,
        channel: ChannelId,
        reference: &str,
    ) -> Result<(), CacheError> {
        let (_gate, _) = self.channel_fence(channel).await?;
        let _stripe = self.stripes.reference(channel, reference).await;
        self.metadata.delete_reference(channel, reference).await?;
        Ok(())
    }

    /// Bounded startup recovery. It never walks the artifact catalog: it clears
    /// abandoned staging, then scans a bounded number of final files and
    /// removes any lacking a metadata record. Dangling metadata (a record whose body is
    /// missing) self-heals lazily on GET or eviction instead.
    pub async fn reconcile(&self) -> Result<(), CacheError> {
        self.files.remove_temporary_files().await?;
        for (channel, artifact) in self.files.orphan_scan(self.orphan_scan_limit).await? {
            if self.metadata.artifact(channel, artifact).await?.is_some() {
                continue;
            }
            let _stripe = self.stripes.artifact(channel, artifact).await;
            // Re-check under the stripe so we never race a concurrent publication of the
            // same identity.
            if self.metadata.artifact(channel, artifact).await?.is_none() {
                self.files.remove(channel, artifact).await?;
            }
        }
        Ok(())
    }

    pub async fn ready(&self) -> Result<(), CacheError> {
        self.metadata.health().await?;
        self.files.health().await?;
        // A node that cannot observe free space cannot admit writes safely; report it
        // as not ready so traffic drains to healthier peers until the sensor recovers.
        if self.space.degraded() {
            return Err(CacheError::SpaceUnavailable);
        }
        Ok(())
    }

    /// Bounded, scan-free reclamation over the ordered eviction-queue heads.
    ///
    /// The space ledger's Normal/Reclaiming mode (low/high watermark hysteresis) drives
    /// the pass: in Normal only past-deadline artifacts are candidates and a recently
    /// used one is requeued instead of evicted; in Reclaiming the recency filter is
    /// ignored and queue heads are evicted toward the high watermark. A single pass
    /// spends one *global* candidate and byte budget across all channels (not one
    /// budget each), so total work never scales with channel cardinality. The starting
    /// channel rotates each pass so an early-exhausted budget does not starve the tail.
    pub async fn run_maintenance_once(&self, limit: usize) -> Result<usize, CacheError> {
        self.space.refresh();
        self.metrics.record_space(self.space.snapshot());
        let mode = self.space.mode();
        let now = self.clock.now();
        let mut channels = self.channels().await?;
        if !channels.is_empty() {
            let start = self.maintenance_cursor.fetch_add(1, Ordering::Relaxed) % channels.len();
            channels.rotate_left(start);
        }
        let mut remaining_candidates = limit;
        let mut remaining_bytes = self.reclaim_byte_limit;
        let mut reclaimed = 0;
        'channels: for channel in channels {
            if remaining_candidates == 0 || remaining_bytes == 0 {
                break;
            }
            let expiry_seconds = self.expiry_seconds(channel).await?;
            let candidates = self
                .metadata
                .reclaim_candidates(channel, remaining_candidates)
                .await?;
            for candidate in candidates {
                if remaining_candidates == 0 || remaining_bytes == 0 {
                    break 'channels;
                }
                let due = candidate.eligible_at <= now;
                if mode == Mode::Normal && !due {
                    // The queue is ordered by deadline; nothing beyond here is due.
                    break;
                }
                remaining_candidates -= 1;
                let _stripe = self.stripes.artifact(channel, candidate.artifact).await;
                if mode == Mode::Normal && self.recent_use.seen(channel, candidate.artifact) {
                    // Recently used: extend the soft deadline instead of evicting.
                    let new_deadline = now.saturating_add(expiry_seconds);
                    self.metadata
                        .requeue(
                            channel,
                            candidate.artifact,
                            candidate.eligible_at,
                            new_deadline,
                        )
                        .await?;
                    self.metrics.requeued();
                    continue;
                }
                // Unlink the body BEFORE deleting metadata, so a crash between the two
                // leaves stale metadata — which the read path and the next pass both
                // self-heal — instead of an invisible final-path orphan only the
                // bounded startup scan could ever reclaim. The pre-read confirms the
                // candidate row is not stale before anything is unlinked; under the
                // stripe nothing can requeue or republish in between (`commit_staged`
                // takes the same stripe), so `evict` then removes the row it just saw.
                match self.metadata.artifact(channel, candidate.artifact).await? {
                    Some(metadata) if metadata.eligible_at == candidate.eligible_at => {
                        self.files.remove(channel, candidate.artifact).await?;
                    }
                    // Requeued or already gone: let `evict` drop the stale queue row.
                    _ => {}
                }
                match self
                    .metadata
                    .evict(channel, candidate.artifact, candidate.eligible_at)
                    .await?
                {
                    Evicted::Removed { stored_len } => {
                        remaining_bytes = remaining_bytes.saturating_sub(stored_len);
                        reclaimed += 1;
                    }
                    Evicted::Stale => {}
                }
            }
        }
        self.metrics.reclaimed(reclaimed as u64);
        Ok(reclaimed)
    }

    /// Whether the space controller is currently reclaiming. The maintenance worker
    /// polls this to run continuous bounded passes under pressure instead of sleeping.
    pub fn is_reclaiming(&self) -> bool {
        matches!(self.space.mode(), Mode::Reclaiming)
    }

    /// Rotates the approximate-recency filters. Called from the maintenance tick so a
    /// GET's heat mark survives exactly one rotation window.
    pub fn rotate_recency(&self) {
        self.recent_use.rotate();
    }

    async fn locate_local(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
    ) -> Result<Option<LocatedArtifact>, CacheError> {
        let Some(metadata) = self.metadata.artifact(channel, artifact).await? else {
            return Ok(None);
        };
        // Open the published file directly rather than stat-ing first: a cache hit is
        // the hottest path, and the open both proves existence and yields the handle the
        // response streams, so we spend one filesystem syscall instead of two. A missing
        // file self-heals the stale metadata row exactly as the previous `exists` check did.
        let path = self.files.path(channel, artifact);
        let file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let _stripe = self.stripes.artifact(channel, artifact).await;
                self.metadata.remove_artifact(channel, artifact).await?;
                return Ok(None);
            }
            Err(error) => return Err(LocalError::Io(error).into()),
        };
        Ok(Some(LocatedArtifact { file, metadata }))
    }

    async fn commit_staged(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
        staged: StagedArtifact,
        commit: Commit,
    ) -> Result<Publication, CacheError> {
        // Fence the final mutation against channel deletion: acquire the shared channel
        // gate and recheck `active` so a body staged before deletion cannot be published
        // back into a  channel whose ranges are being (or have been) removed.
        let (_gate, expiry_seconds) = match self.channel_fence(channel).await {
            Ok(fence) => fence,
            Err(error) => {
                staged.discard().await;
                return Err(error);
            }
        };
        let len = staged.len;
        let stored_len = staged.stored_len;
        let encoding = staged.encoding;
        // Staging and hashing are already complete; take the transition stripe only for
        // the final rename-and-commit so a slow publish blocks only this one identity.
        let _stripe = self.stripes.artifact(channel, artifact).await;
        let file_publication = self
            .files
            .publish(channel, artifact, staged, commit.durability)
            .await?;
        let (reservation, committed_len) = match file_publication {
            FilePublication::Created(reservation) => (reservation, stored_len),
            FilePublication::Existing(reservation) => (reservation, 0),
        };
        let now = self.clock.now();
        let created = match self
            .metadata
            .commit_publication(
                channel,
                artifact,
                ArtifactMetadata {
                    content_len: len,
                    content_type: commit.content_type,
                    created_at: now,
                    eligible_at: now.saturating_add(expiry_seconds),
                    stored_len,
                    encoding,
                },
                commit.reference,
                commit.durability,
            )
            .await
        {
            Ok(created) => created,
            Err(error) => {
                reservation.discard().await;
                return Err(error.into());
            }
        };
        // Commit only blocks installed by this publication. A duplicate used staging
        // capacity but allocated no new final file, so committing zero releases it all.
        reservation.commit(committed_len);
        let outcome = if created {
            PublicationOutcome::Created
        } else {
            PublicationOutcome::Existing
        };
        Ok(Publication { artifact, outcome })
    }

    async fn expiry_seconds(&self, channel: ChannelId) -> Result<u64, CacheError> {
        self.metadata
            .channel(channel)
            .await?
            .filter(|record| record.state == Lifecycle::Active)
            .map(|record| record.expiry_seconds)
            .ok_or(CacheError::MissingChannel)
    }

    async fn channels(&self) -> Result<Vec<ChannelId>, CacheError> {
        Ok(self
            .metadata
            .channels()
            .await?
            .into_iter()
            .filter(|record| record.state == Lifecycle::Active)
            .map(|record| record.id)
            .collect())
    }

    /// Acquires the shared channel lifecycle gate (read side) and rechecks that the
    /// channel is still active, returning the guard to hold across a final mutation
    /// along with the expiry carried by the very record the recheck just read.
    /// A deletion takes the write side, so holding this read guard blocks deletion from
    /// wiping the channel mid-commit, and the active recheck rejects a mutation into a
    /// channel whose deletion has already begun to tear it down.
    async fn channel_fence(
        &self,
        channel: ChannelId,
    ) -> Result<(OwnedRwLockReadGuard<()>, u64), CacheError> {
        let guard = self.channel_gates.gate(channel).read_owned().await;
        match self.metadata.channel(channel).await? {
            Some(record) if record.state == Lifecycle::Active => Ok((guard, record.expiry_seconds)),
            Some(_) => Err(CacheError::ChannelDeleting),
            None => Err(CacheError::MissingChannel),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error(transparent)]
    Local(#[from] LocalError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    ChannelStore(#[from] ChannelStoreError),
    #[error("channel does not exist")]
    MissingChannel,
    #[error("channel is being deleted")]
    ChannelDeleting,
    #[error("free-space observation is unavailable")]
    SpaceUnavailable,
}
