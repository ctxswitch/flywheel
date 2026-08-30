use super::{ArtifactFiles, FilePublication, StageOutcome};
use crate::{
    artifact::{ArtifactId, Digest, StoredEncoding},
    channel::ChannelId,
    storage::{local::Reserver, metadata::Durability},
};
use bytes::Bytes;
use futures_util::{Stream, stream};
use sha2::{Digest as _, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tempfile::TempDir;
use tokio::sync::Notify;

struct Accounting {
    capacity: u64,
    reserved: AtomicU64,
    committed: AtomicU64,
    changed: Notify,
}

impl Accounting {
    fn new(capacity: u64) -> Self {
        Self {
            capacity,
            reserved: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }

    fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::SeqCst)
    }

    fn committed(&self) -> u64 {
        self.committed.load(Ordering::SeqCst)
    }

    async fn wait_for(&self, predicate: impl Fn() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let changed = self.changed.notified();
                if predicate() {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("reservation accounting should settle");
    }
}

impl Reserver for Accounting {
    fn try_reserve(&self, bytes: u64) -> bool {
        let mut reserved = self.reserved.load(Ordering::SeqCst);
        loop {
            let committed = self.committed.load(Ordering::SeqCst);
            if self.capacity.saturating_sub(reserved + committed) < bytes {
                return false;
            }
            match self.reserved.compare_exchange(
                reserved,
                reserved + bytes,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.changed.notify_waiters();
                    return true;
                }
                Err(actual) => reserved = actual,
            }
        }
    }

    fn release(&self, bytes: u64) {
        self.reserved.fetch_sub(bytes, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    fn commit(&self, bytes: u64) {
        self.reserved.fetch_sub(bytes, Ordering::SeqCst);
        self.committed.fetch_add(bytes, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

fn body(
    chunks: impl IntoIterator<Item = Result<&'static [u8], &'static str>>,
) -> impl Stream<Item = Result<Bytes, &'static str>> + Unpin {
    stream::iter(
        chunks
            .into_iter()
            .map(|chunk| chunk.map(Bytes::from_static)),
    )
}

fn artifact(body: &[u8]) -> ArtifactId {
    ArtifactId::from_digest(Digest::from_bytes(Sha256::digest(body).into()))
}

fn temporary_file_count(root: &std::path::Path) -> usize {
    std::fs::read_dir(root.join("00000000000000000000000000/tmp"))
        .map(|entries| entries.filter_map(Result::ok).count())
        .unwrap_or(0)
}

#[tokio::test]
async fn ordinary_staging_failure_awaits_cleanup_and_reuses_capacity() {
    let directory = TempDir::new().unwrap();
    let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
    let accounting = Arc::new(Accounting::new(4));
    let failed = files
        .stage(
            ChannelId::DEFAULT,
            body([Ok(b"ab".as_slice()), Err("broken stream")]),
            8,
            None,
            accounting.clone(),
            Durability::BestEffort,
            StoredEncoding::Identity,
        )
        .await;
    assert!(failed.is_err());
    assert_eq!(accounting.reserved(), 0);
    assert_eq!(accounting.committed(), 0);
    assert_eq!(temporary_file_count(directory.path()), 0);

    let retry = files
        .stage(
            ChannelId::DEFAULT,
            body([Ok(b"data".as_slice())]),
            4,
            None,
            accounting.clone(),
            Durability::BestEffort,
            StoredEncoding::Identity,
        )
        .await
        .unwrap();
    assert!(matches!(retry, StageOutcome::Ready(_)));
}

#[tokio::test]
async fn cancelling_staging_schedules_cleanup_and_reuses_capacity() {
    let directory = TempDir::new().unwrap();
    let files = Arc::new(ArtifactFiles::open(directory.path(), 4).await.unwrap());
    let accounting = Arc::new(Accounting::new(4));
    let body_polled = Arc::new(Notify::new());
    let task = tokio::spawn({
        let files = Arc::clone(&files);
        let accounting = Arc::clone(&accounting);
        let body_polled = Arc::clone(&body_polled);
        async move {
            files
                .stage(
                    ChannelId::DEFAULT,
                    Box::pin(stream::once(async move {
                        body_polled.notify_one();
                        std::future::pending::<Result<Bytes, &'static str>>().await
                    })),
                    8,
                    None,
                    accounting,
                    Durability::BestEffort,
                    StoredEncoding::Identity,
                )
                .await
        }
    });
    body_polled.notified().await;
    assert_eq!(accounting.reserved(), 4);
    assert_eq!(temporary_file_count(directory.path()), 1);

    task.abort();
    match task.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("staging task should be cancelled"),
    }
    accounting
        .wait_for(|| accounting.reserved() == 0 && temporary_file_count(directory.path()) == 0)
        .await;
    assert_eq!(accounting.committed(), 0);

    let retry = files
        .stage(
            ChannelId::DEFAULT,
            body([Ok(b"data".as_slice())]),
            4,
            None,
            accounting,
            Durability::BestEffort,
            StoredEncoding::Identity,
        )
        .await
        .unwrap();
    assert!(matches!(retry, StageOutcome::Ready(_)));
}

/// A zero-length body reserves nothing, but staging still creates its `.part`
/// file. Dropping the stage has to remove that file: cleanup keys off the
/// filesystem state, not off the reserved byte count.
#[tokio::test]
async fn dropping_a_zero_length_stage_removes_its_temporary_file() {
    let directory = TempDir::new().unwrap();
    let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
    let accounting = Arc::new(Accounting::new(4));
    let outcome = files
        .stage(
            ChannelId::DEFAULT,
            body([]),
            4,
            Some(0),
            accounting.clone(),
            Durability::BestEffort,
            StoredEncoding::Identity,
        )
        .await
        .unwrap();
    let StageOutcome::Ready(staged) = outcome else {
        panic!("an empty body always fits");
    };
    assert_eq!(staged.len, 0);
    assert_eq!(temporary_file_count(directory.path()), 1);

    drop(staged);

    accounting
        .wait_for(|| temporary_file_count(directory.path()) == 0)
        .await;
    assert_eq!(accounting.reserved(), 0);
    assert_eq!(accounting.committed(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn failed_deletion_conservatively_commits_capacity() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TempDir::new().unwrap();
    let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
    let accounting = Arc::new(Accounting::new(4));
    let StageOutcome::Ready(staged) = files
        .stage(
            ChannelId::DEFAULT,
            body([Ok(b"data".as_slice())]),
            4,
            None,
            accounting.clone(),
            Durability::BestEffort,
            StoredEncoding::Identity,
        )
        .await
        .unwrap()
    else {
        panic!("staging should fit");
    };
    let temporary = directory.path().join("00000000000000000000000000/tmp");
    tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o500))
        .await
        .unwrap();

    let wrong_artifact = artifact(b"different");
    let failed = files
        .publish(
            ChannelId::DEFAULT,
            wrong_artifact,
            staged,
            Durability::BestEffort,
        )
        .await;

    tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700))
        .await
        .unwrap();
    assert!(failed.is_err());
    assert_eq!(accounting.reserved(), 0);
    assert_eq!(accounting.committed(), 4);
    assert_eq!(temporary_file_count(directory.path()), 1);
}

#[tokio::test]
async fn dropping_a_published_reservation_commits_its_stored_size() {
    let directory = TempDir::new().unwrap();
    let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
    let accounting = Arc::new(Accounting::new(4));
    let StageOutcome::Ready(staged) = files
        .stage(
            ChannelId::DEFAULT,
            body([Ok(b"data".as_slice())]),
            4,
            None,
            accounting.clone(),
            Durability::BestEffort,
            StoredEncoding::Identity,
        )
        .await
        .unwrap()
    else {
        panic!("staging should fit");
    };
    let publication = files
        .publish(
            ChannelId::DEFAULT,
            artifact(b"data"),
            staged,
            Durability::BestEffort,
        )
        .await
        .unwrap();
    let FilePublication::Created(reservation) = publication else {
        panic!("the first publication should create the body");
    };

    drop(reservation);

    assert_eq!(accounting.reserved(), 0);
    assert_eq!(accounting.committed(), 4);
}

#[tokio::test]
async fn unknown_length_uploads_reserve_every_extent() {
    let directory = TempDir::new().unwrap();
    let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
    let accounting = Arc::new(Accounting::new(12));
    let outcome = files
        .stage(
            ChannelId::DEFAULT,
            body([
                Ok(b"abc".as_slice()),
                Ok(b"defg".as_slice()),
                Ok(b"hijkl".as_slice()),
            ]),
            12,
            None,
            accounting.clone(),
            Durability::BestEffort,
            StoredEncoding::Identity,
        )
        .await
        .unwrap();
    let StageOutcome::Ready(staged) = outcome else {
        panic!("every extent should fit");
    };
    assert_eq!(staged.len, 12);
    assert_eq!(accounting.reserved(), 12);

    drop(staged);
    accounting.wait_for(|| accounting.reserved() == 0).await;
    assert_eq!(accounting.committed(), 0);
}

#[tokio::test]
async fn zstd_expansion_extends_the_stored_size_reservation() {
    let directory = TempDir::new().unwrap();
    let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
    let accounting = Arc::new(Accounting::new(128));
    let outcome = files
        .stage(
            ChannelId::DEFAULT,
            body([Ok(b"x".as_slice())]),
            1,
            Some(1),
            accounting.clone(),
            Durability::BestEffort,
            StoredEncoding::Zstd,
        )
        .await
        .unwrap();
    let StageOutcome::Ready(staged) = outcome else {
        panic!("the encoded extent should fit");
    };
    assert!(staged.stored_len > staged.len);
    assert_eq!(accounting.reserved(), staged.stored_len);

    drop(staged);
    accounting.wait_for(|| accounting.reserved() == 0).await;
    assert_eq!(accounting.committed(), 0);
}

#[tokio::test]
async fn duplicate_publication_releases_the_second_reservation() {
    let directory = TempDir::new().unwrap();
    let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
    let accounting = Arc::new(Accounting::new(16));
    let artifact = artifact(b"data");

    for expected_created in [true, false] {
        let StageOutcome::Ready(staged) = files
            .stage(
                ChannelId::DEFAULT,
                body([Ok(b"data".as_slice())]),
                4,
                None,
                accounting.clone(),
                Durability::BestEffort,
                StoredEncoding::Identity,
            )
            .await
            .unwrap()
        else {
            panic!("staging should fit");
        };
        let publication = files
            .publish(ChannelId::DEFAULT, artifact, staged, Durability::BestEffort)
            .await
            .unwrap();
        match publication {
            FilePublication::Created(reservation) if expected_created => reservation.commit(4),
            FilePublication::Existing(reservation) if !expected_created => reservation.commit(0),
            _ => panic!("unexpected publication result"),
        }
    }

    assert_eq!(accounting.reserved(), 0);
    assert_eq!(accounting.committed(), 4);
}

#[test]
fn dropping_a_temporary_reservation_without_a_runtime_commits_capacity() {
    let directory = TempDir::new().unwrap();
    let accounting = Arc::new(Accounting::new(4));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let staged = runtime.block_on(async {
        let files = ArtifactFiles::open(directory.path(), 4).await.unwrap();
        let outcome = files
            .stage(
                ChannelId::DEFAULT,
                stream::iter([Ok::<_, &'static str>(Bytes::from_static(b"data"))]),
                4,
                None,
                accounting.clone(),
                Durability::BestEffort,
                StoredEncoding::Identity,
            )
            .await
            .unwrap();
        let StageOutcome::Ready(staged) = outcome else {
            panic!("staging should fit");
        };
        staged
    });
    assert_eq!(accounting.reserved.load(Ordering::SeqCst), 4);

    drop(runtime);
    drop(staged);

    assert_eq!(accounting.reserved.load(Ordering::SeqCst), 0);
    assert_eq!(accounting.committed.load(Ordering::SeqCst), 4);
}
