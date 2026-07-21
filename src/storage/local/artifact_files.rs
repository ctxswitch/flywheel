use super::Reserver;
use crate::{
    artifact::{ArtifactId, Digest, StoredEncoding},
    channel::ChannelId,
    storage::metadata::Durability,
};
use async_compression::tokio::write::ZstdEncoder;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[cfg(test)]
mod tests;

pub struct ArtifactFiles {
    root: PathBuf,
    reservation_extent: u64,
}

/// An outstanding reservation of disk capacity held across staging and publication.
/// Committing converts part of it to durable usage; ordinary failures explicitly await
/// cleanup, while cancellation schedules best-effort asynchronous cleanup from `Drop`.
pub struct Reservation {
    reserver: Arc<dyn Reserver>,
    outstanding: u64,
    state: ReservationState,
}

/// Filesystem state associated with an outstanding reservation. Keeping the states
/// explicit prevents a cleared cleanup path from ambiguously meaning either "no file
/// was created" or "the body was published".
enum ReservationState {
    Vacant,
    Temporary(PathBuf),
    Published,
}

/// Accounting captured by asynchronous cancellation cleanup. Its conservative default
/// is commit: if the task is cancelled before confirming deletion, capacity cannot be
/// handed out twice. A later free-space refresh reconciles the temporary overcount.
struct PendingSettlement {
    reserver: Arc<dyn Reserver>,
    outstanding: u64,
}

impl PendingSettlement {
    fn release(mut self) {
        self.reserver.release(self.outstanding);
        self.outstanding = 0;
    }

    fn commit(mut self) {
        self.reserver.commit(self.outstanding);
        self.outstanding = 0;
    }
}

impl Drop for PendingSettlement {
    fn drop(&mut self) {
        if self.outstanding > 0 {
            self.reserver.commit(self.outstanding);
        }
    }
}

fn settle_removal(result: std::io::Result<()>, settlement: PendingSettlement) {
    match result {
        Ok(()) => settlement.release(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => settlement.release(),
        Err(_) => settlement.commit(),
    }
}

fn schedule_removal(path: PathBuf, settlement: PendingSettlement) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        settlement.commit();
        return;
    };
    runtime.spawn(async move {
        let result = tokio::fs::remove_file(path).await;
        // `PendingSettlement::drop` commits if shutdown cancels this task before the
        // removal result can settle the accounting.
        settle_removal(result, settlement);
    });
}

/// Makes an explicitly awaited discard cancellation-safe. If its caller is dropped
/// while the async unlink is pending, this guard schedules one best-effort retry using
/// the same conservative accounting fallback as `Reservation::drop`.
struct AwaitedRemoval {
    path: Option<PathBuf>,
    settlement: Option<PendingSettlement>,
}

impl AwaitedRemoval {
    async fn run(mut self) {
        let result = tokio::fs::remove_file(self.path.as_ref().expect("path is present")).await;
        self.path.take();
        let settlement = self.settlement.take().expect("settlement is present");
        settle_removal(result, settlement);
    }
}

impl Drop for AwaitedRemoval {
    fn drop(&mut self) {
        if let (Some(path), Some(settlement)) = (self.path.take(), self.settlement.take()) {
            schedule_removal(path, settlement);
        }
    }
}

impl Reservation {
    fn new(reserver: Arc<dyn Reserver>) -> Self {
        Self {
            reserver,
            outstanding: 0,
            state: ReservationState::Vacant,
        }
    }

    fn reserve(&mut self, bytes: u64) -> bool {
        if bytes == 0 || self.reserver.try_reserve(bytes) {
            self.outstanding = self.outstanding.saturating_add(bytes);
            true
        } else {
            false
        }
    }

    /// Marks `bytes` of the reservation as committed on-disk usage and releases any
    /// over-reserved remainder (staging rounds unknown lengths up to whole extents).
    /// Setting `outstanding` to zero disarms the failure-cleanup path in `drop`.
    pub fn commit(mut self, bytes: u64) {
        let committed = bytes.min(self.outstanding);
        self.reserver.commit(committed);
        let slack = self.outstanding - committed;
        if slack > 0 {
            self.reserver.release(slack);
        }
        self.outstanding = 0;
    }

    fn settlement(&mut self) -> PendingSettlement {
        let outstanding = std::mem::take(&mut self.outstanding);
        PendingSettlement {
            reserver: Arc::clone(&self.reserver),
            outstanding,
        }
    }

    fn mark_temporary(&mut self, path: PathBuf) {
        self.state = ReservationState::Temporary(path);
    }

    fn mark_vacant(&mut self) {
        self.state = ReservationState::Vacant;
    }

    fn mark_published(&mut self) {
        self.state = ReservationState::Published;
    }

    /// Settles a failed operation before returning to its caller. Temporary files are
    /// removed asynchronously; successful deletion and `NotFound` release the capacity,
    /// while deletion failure or a published file commits it until refresh.
    pub(crate) async fn discard(mut self) {
        let state = std::mem::replace(&mut self.state, ReservationState::Vacant);
        let settlement = self.settlement();
        match state {
            ReservationState::Vacant => settlement.release(),
            ReservationState::Temporary(path) => {
                AwaitedRemoval {
                    path: Some(path),
                    settlement: Some(settlement),
                }
                .run()
                .await;
            }
            ReservationState::Published => settlement.commit(),
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // A zero-length body reserves nothing yet still stages a file, so the
        // shortcut has to clear the filesystem state as well as the accounting.
        if self.outstanding == 0 && !matches!(self.state, ReservationState::Temporary(_)) {
            return;
        }
        let state = std::mem::replace(&mut self.state, ReservationState::Vacant);
        let settlement = self.settlement();
        match state {
            ReservationState::Vacant => settlement.release(),
            ReservationState::Published => settlement.commit(),
            ReservationState::Temporary(path) => schedule_removal(path, settlement),
        }
    }
}

pub struct StagedArtifact {
    pub(crate) path: PathBuf,
    pub digest: Digest,
    /// Logical (identity) byte count — the length of the bytes the digest covers.
    pub len: u64,
    /// On-disk byte count of the staged file; smaller than `len` for `Zstd`.
    pub stored_len: u64,
    pub encoding: StoredEncoding,
    reservation: Reservation,
}

impl StagedArtifact {
    pub(crate) async fn discard(self) {
        self.reservation.discard().await;
    }
}

/// Whether staging installed a new body or found the content-addressed body already
/// present. The reservation is released for `Existing` and committed for `Created`.
pub enum FilePublication {
    Created(Reservation),
    Existing(Reservation),
}

/// The staging writer for the requested on-disk encoding. Raw chunks are hashed
/// before they reach this writer, so the artifact's identity is always over the
/// logical bytes regardless of encoding.
enum StageWriter {
    Identity(tokio::fs::File),
    Zstd(ZstdEncoder<tokio::fs::File>),
}

enum PublishedBody {
    Created,
    Existing,
}

impl StageWriter {
    fn new(file: tokio::fs::File, encoding: StoredEncoding) -> Self {
        match encoding {
            StoredEncoding::Identity => Self::Identity(file),
            StoredEncoding::Zstd => Self::Zstd(ZstdEncoder::new(file)),
        }
    }

    async fn write_all(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Identity(file) => file.write_all(chunk).await,
            Self::Zstd(encoder) => encoder.write_all(chunk).await,
        }
    }

    /// Flushes buffered data through to the file and returns it. The flush matters
    /// even for identity bodies: `tokio::fs::File` completes writes on a background
    /// task, and the caller publishes this file as soon as it returns.
    async fn finish(self) -> std::io::Result<tokio::fs::File> {
        match self {
            Self::Identity(mut file) => {
                file.flush().await?;
                Ok(file)
            }
            Self::Zstd(mut encoder) => {
                encoder.shutdown().await?;
                Ok(encoder.into_inner())
            }
        }
    }
}

/// The result of staging a body. `Rejected` returns the still-untouched stream when the
/// up-front reservation fails, so the caller can pass the body through elsewhere (the
/// package proxy streams it straight to the client) instead of buffering or re-fetching.
pub enum StageOutcome<S> {
    Ready(StagedArtifact),
    Rejected(S),
}

impl ArtifactFiles {
    pub async fn open(root: impl AsRef<Path>, reservation_extent: u64) -> Result<Self, LocalError> {
        tokio::fs::create_dir_all(root.as_ref()).await?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
            reservation_extent: reservation_extent.max(1),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn stage<S, E>(
        &self,
        channel: ChannelId,
        mut stream: S,
        max_bytes: u64,
        content_length: Option<u64>,
        reserver: Arc<dyn Reserver>,
        durability: Durability,
        encoding: StoredEncoding,
    ) -> Result<StageOutcome<S>, LocalError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut reservation = Reservation::new(reserver);
        // Reserve a known length up front so an immediate bypass decision is possible;
        // unknown lengths reserve one extent and extend as the body streams in.
        let initial = content_length
            .map(|length| length.min(max_bytes))
            .unwrap_or(self.reservation_extent);
        if !reservation.reserve(initial) {
            // The stream has not been polled yet, so hand it back untouched: the caller
            // can stream the body onward (the proxy passes it straight to the client)
            // rather than buffering it or fetching it again.
            return Ok(StageOutcome::Rejected(stream));
        }
        let (path, digest, len, stored_len) = match self
            .stage_reserved(
                channel,
                &mut stream,
                max_bytes,
                &mut reservation,
                durability,
                encoding,
            )
            .await
        {
            Ok(staged) => staged,
            Err(error) => {
                reservation.discard().await;
                return Err(error);
            }
        };
        Ok(StageOutcome::Ready(StagedArtifact {
            path,
            digest,
            len,
            stored_len,
            encoding,
            reservation,
        }))
    }

    /// Performs all fallible work after admission. Returning errors to one outer
    /// discard site keeps ordinary failure cleanup awaited without duplicating it at
    /// every filesystem and stream operation.
    async fn stage_reserved<S, E>(
        &self,
        channel: ChannelId,
        stream: &mut S,
        max_bytes: u64,
        reservation: &mut Reservation,
        durability: Durability,
        encoding: StoredEncoding,
    ) -> Result<(PathBuf, Digest, u64, u64), LocalError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let directory = self.channel_dir(channel).join("tmp");
        tokio::fs::create_dir_all(&directory).await?;
        let path = directory.join(format!("{}.part", ulid::Ulid::new()));
        // Arm cleanup before opening: cancellation while the open is in flight treats
        // `NotFound` as a confirmed-vacant temporary path and releases capacity.
        reservation.mark_temporary(path.clone());
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;
        // Track logical bytes while streaming, then reconcile against the encoded size
        // below: a zstd frame can be slightly larger than a small or incompressible body.
        let mut writer = StageWriter::new(file, encoding);
        let mut hasher = Sha256::new();
        let mut len = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| LocalError::Stream(error.to_string()))?;
            len = len
                .checked_add(chunk.len() as u64)
                .ok_or(LocalError::TooLarge)?;
            if len > max_bytes {
                return Err(LocalError::TooLarge);
            }
            while len > reservation.outstanding {
                if !reservation.reserve(self.reservation_extent) {
                    return Err(LocalError::OutOfSpace);
                }
            }
            hasher.update(&chunk);
            writer.write_all(&chunk).await?;
        }
        let file = writer.finish().await?;
        // An identity body was written verbatim into a freshly created file, so its
        // stored length is the byte count already counted above. Only a zstd frame has
        // an encoded size that has to be read back from the filesystem.
        let stored_len = match encoding {
            StoredEncoding::Identity => len,
            StoredEncoding::Zstd => file.metadata().await?.len(),
        };
        if stored_len > reservation.outstanding
            && !reservation.reserve(stored_len - reservation.outstanding)
        {
            return Err(LocalError::OutOfSpace);
        }
        // Only the durable (raw artifact / Bazel CAS) contract pays for a body fsync;
        // best-effort build-cache and proxy bodies skip it since any crash outcome is a
        // hit, a self-healing miss, or an orphan file.
        if matches!(durability, Durability::Durable) {
            file.sync_all().await?;
        }
        drop(file);
        Ok((
            path,
            Digest::from_bytes(hasher.finalize().into()),
            len,
            stored_len,
        ))
    }

    /// Renames the completed body into its final digest path and returns the still-held
    /// reservation so the caller can commit it once metadata is written.
    pub async fn publish(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
        staged: StagedArtifact,
        durability: Durability,
    ) -> Result<FilePublication, LocalError> {
        let StagedArtifact {
            path: staged_path,
            digest,
            mut reservation,
            ..
        } = staged;
        let published = match self
            .publish_reserved(
                channel,
                artifact,
                &staged_path,
                digest,
                &mut reservation,
                durability,
            )
            .await
        {
            Ok(published) => published,
            Err(error) => {
                reservation.discard().await;
                return Err(error);
            }
        };
        Ok(match published {
            PublishedBody::Created => FilePublication::Created(reservation),
            PublishedBody::Existing => FilePublication::Existing(reservation),
        })
    }

    /// Performs the fallible pre-publication and rename work, leaving one awaited
    /// discard site in `publish` for every ordinary failure.
    async fn publish_reserved(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
        staged_path: &Path,
        digest: Digest,
        reservation: &mut Reservation,
        durability: Durability,
    ) -> Result<PublishedBody, LocalError> {
        if digest != artifact.digest() {
            return Err(LocalError::DigestMismatch);
        }
        let final_path = self.path(channel, artifact);
        let parent = final_path.parent().expect("artifact path has a parent");
        tokio::fs::create_dir_all(parent).await?;
        if tokio::fs::try_exists(&final_path).await? {
            tokio::fs::remove_file(staged_path).await?;
            reservation.mark_vacant();
            return Ok(PublishedBody::Existing);
        }
        match tokio::fs::rename(staged_path, &final_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                tokio::fs::remove_file(staged_path).await?;
                reservation.mark_vacant();
                return Ok(PublishedBody::Existing);
            }
            Err(error) => return Err(error.into()),
        }
        // The body now occupies its final path: mark it published before any further
        // fallible work so a later failure keeps the bytes committed instead of
        // releasing capacity that a real file still consumes.
        reservation.mark_published();
        // Only the durable contract flushes the parent directory so the rename survives
        // a crash; best-effort routes skip this second filesystem durability cost.
        if matches!(durability, Durability::Durable) {
            let parent = parent.to_path_buf();
            let synced =
                tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
                    .await
                    .map_err(LocalError::from)
                    .and_then(|result| result.map_err(LocalError::from));
            synced?;
        }
        Ok(PublishedBody::Created)
    }

    pub fn path(&self, channel: ChannelId, artifact: ArtifactId) -> PathBuf {
        let digest = artifact.digest().to_string();
        // Two-level sharding keeps any single directory manageable at high cardinality.
        self.channel_dir(channel)
            .join("sha256")
            .join(&digest[0..2])
            .join(&digest[2..4])
            .join(digest)
    }

    pub async fn health(&self) -> Result<(), LocalError> {
        let metadata = tokio::fs::metadata(&self.root).await?;
        if !metadata.is_dir() {
            return Err(LocalError::Io(std::io::Error::other(
                "artifact root is not a directory",
            )));
        }
        Ok(())
    }

    pub async fn remove(&self, channel: ChannelId, artifact: ArtifactId) -> Result<(), LocalError> {
        match tokio::fs::remove_file(self.path(channel, artifact)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn remove_temporary_files(&self) -> Result<(), LocalError> {
        let mut channels = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        while let Some(channel) = channels.next_entry().await? {
            let directory = channel.path().join("tmp");
            let mut files = match tokio::fs::read_dir(&directory).await {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            while let Some(file) = files.next_entry().await? {
                if file.file_type().await?.is_file() {
                    tokio::fs::remove_file(file.path()).await?;
                }
            }
        }
        Ok(())
    }

    /// Walks up to `limit` final artifact files across all channels, returning their
    /// identities so recovery can point-read each against metadata. Bounded so it never
    /// enumerates the whole catalog; callers self-heal what they find.
    pub async fn orphan_scan(
        &self,
        limit: usize,
    ) -> Result<Vec<(ChannelId, ArtifactId)>, LocalError> {
        let mut found = Vec::new();
        let mut channels = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(found),
            Err(error) => return Err(error.into()),
        };
        while let Some(entry) = channels.next_entry().await? {
            if found.len() >= limit {
                break;
            }
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let component = entry.file_name();
            let component = component.to_string_lossy();
            let channel = ChannelId::from_str(&component)
                .map_err(|_| LocalError::InvalidChannelDirectory(component.into_owned()))?;
            self.scan_channel_files(channel, entry.path().join("sha256"), limit, &mut found)
                .await?;
        }
        Ok(found)
    }

    async fn scan_channel_files(
        &self,
        channel: ChannelId,
        sha256_dir: PathBuf,
        limit: usize,
        found: &mut Vec<(ChannelId, ArtifactId)>,
    ) -> Result<(), LocalError> {
        let mut prefixes = match tokio::fs::read_dir(&sha256_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        while let Some(prefix) = prefixes.next_entry().await? {
            if found.len() >= limit {
                return Ok(());
            }
            if !prefix.file_type().await?.is_dir() {
                continue;
            }
            let mut subprefixes = tokio::fs::read_dir(prefix.path()).await?;
            while let Some(subprefix) = subprefixes.next_entry().await? {
                if found.len() >= limit {
                    return Ok(());
                }
                if !subprefix.file_type().await?.is_dir() {
                    continue;
                }
                let mut files = tokio::fs::read_dir(subprefix.path()).await?;
                while let Some(file) = files.next_entry().await? {
                    if found.len() >= limit {
                        return Ok(());
                    }
                    if !file.file_type().await?.is_file() {
                        continue;
                    }
                    let digest = file.file_name();
                    let digest = digest.to_string_lossy();
                    if let Ok(artifact) = ArtifactId::parse("sha256", &digest) {
                        found.push((channel, artifact));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn remove_channel(&self, channel: ChannelId) -> Result<(), LocalError> {
        match tokio::fs::remove_dir_all(self.root.join(channel.to_string())).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn channel_dir(&self, channel: ChannelId) -> PathBuf {
        self.root.join(channel.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LocalError {
    #[error("local artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("request body failed: {0}")]
    Stream(String),
    #[error("artifact exceeds the configured upload limit")]
    TooLarge,
    #[error("insufficient reserved disk capacity for this artifact")]
    OutOfSpace,
    #[error("artifact body does not match its sha256 digest")]
    DigestMismatch,
    #[error("artifact directory is not a canonical channel ID: {0}")]
    InvalidChannelDirectory(String),
    #[error("blocking local I/O task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}
