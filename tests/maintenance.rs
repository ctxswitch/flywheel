use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use flywheel::{Flywheel, cache::FreeSpace, clock::Clock, config::Config};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tempfile::TempDir;
use tower::ServiceExt;

struct ManualClock(AtomicU64);

impl ManualClock {
    fn new(now: u64) -> Self {
        Self(AtomicU64::new(now))
    }
    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A settable free-space source so tests can force the Normal/Reclaiming controller
/// deterministically without touching the real filesystem.
struct ManualFreeSpace(AtomicU64);

impl ManualFreeSpace {
    fn new(bytes: u64) -> Self {
        Self(AtomicU64::new(bytes))
    }
    fn set(&self, bytes: u64) {
        self.0.store(bytes, Ordering::SeqCst);
    }
}

impl FreeSpace for ManualFreeSpace {
    fn free_bytes(&self) -> Option<u64> {
        Some(self.0.load(Ordering::SeqCst))
    }
}

fn artifact(body: &[u8]) -> (String, String) {
    let digest = hex::encode(Sha256::digest(body));
    (digest.clone(), format!("/artifacts/sha256/{digest}"))
}

async fn status(app: axum::Router, request: Request<Body>) -> StatusCode {
    app.oneshot(request).await.unwrap().status()
}

// A large free-space configuration that keeps the controller firmly in Normal mode so
// only the soft deadline and recency filter decide reclamation.
fn spacious(directory: &TempDir, expiry_seconds: u64) -> Config {
    let mut config = Config::new(directory.path());
    config.default_expiry_seconds = expiry_seconds;
    config.low_watermark_bytes = 1024;
    config.high_watermark_bytes = 2048;
    config.emergency_headroom_bytes = 0;
    config
}

// The bounded reclaimer evicts an artifact once its soft deadline has passed, provided
// it has not been marked recently used. A pass before the deadline reclaims nothing.
#[tokio::test]
async fn deadline_reclaims_cold_artifact_after_soft_deadline() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let space = Arc::new(ManualFreeSpace::new(1 << 40));
    let flywheel = Flywheel::open_with_space(spacious(&directory, 10), clock.clone(), space)
        .await
        .unwrap();
    let app = flywheel.router();
    let body = b"deadline bound";
    let (_, path) = artifact(body);
    assert_eq!(
        status(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await,
        StatusCode::CREATED
    );

    // Before the deadline nothing is due.
    clock.set(1_005);
    assert_eq!(flywheel.run_maintenance_once().await.unwrap(), 0);

    // After the deadline the cold artifact is reclaimed.
    clock.set(1_011);
    assert_eq!(flywheel.run_maintenance_once().await.unwrap(), 1);
    assert_eq!(
        status(app, Request::get(&path).body(Body::empty()).unwrap()).await,
        StatusCode::NOT_FOUND
    );
    // Eviction unlinks the body directly — nothing lingers on disk in a trash
    // directory waiting for a later sweep.
    let digest = hex::encode(Sha256::digest(body));
    let stored = directory
        .path()
        .join("artifacts/00000000000000000000000000/sha256")
        .join(&digest[0..2])
        .join(&digest[2..4])
        .join(&digest);
    assert!(!stored.exists());
    assert!(
        !directory
            .path()
            .join("artifacts/00000000000000000000000000/trash")
            .exists()
    );
}

// A GET marks approximate recency; a Normal-mode pass before rotation requeues the
// expired-but-hot artifact to a fresh deadline instead of evicting it.
#[tokio::test]
async fn recent_use_requeues_hot_artifact() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let space = Arc::new(ManualFreeSpace::new(1 << 40));
    let flywheel = Flywheel::open_with_space(spacious(&directory, 10), clock.clone(), space)
        .await
        .unwrap();
    let app = flywheel.router();
    let body = b"hot artifact";
    let (_, path) = artifact(body);
    assert_eq!(
        status(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await,
        StatusCode::CREATED
    );

    // A read marks the recency filter.
    clock.set(1_005);
    assert_eq!(
        status(
            app.clone(),
            Request::get(&path).body(Body::empty()).unwrap()
        )
        .await,
        StatusCode::OK
    );

    // The deadline has passed but the artifact is hot, so the pass requeues it.
    clock.set(1_011);
    assert_eq!(flywheel.run_maintenance_once().await.unwrap(), 0);
    assert_eq!(
        status(app, Request::get(&path).body(Body::empty()).unwrap()).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn maintenance_uses_each_active_channels_persisted_expiry() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let space = Arc::new(ManualFreeSpace::new(1 << 40));
    let flywheel = Flywheel::open_with_space(spacious(&directory, 1_000), clock.clone(), space)
        .await
        .unwrap();
    let app = flywheel.router();
    let registered = app
        .clone()
        .oneshot(
            Request::post("/channels")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"access_control": false, "expiry_seconds": 10}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let registered: serde_json::Value =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let channel = registered["channel"].as_str().unwrap();
    let default_body = b"default lasts longer";
    let (default_digest, default_path) = artifact(default_body);
    let custom_body = b"custom expires first";
    let (custom_digest, _) = artifact(custom_body);
    let custom_path = format!("/channels/{channel}/artifacts/sha256/{custom_digest}");
    assert_eq!(
        status(
            app.clone(),
            Request::put(&default_path)
                .body(Body::from(default_body.as_slice()))
                .unwrap(),
        )
        .await,
        StatusCode::CREATED,
    );
    assert_eq!(
        status(
            app.clone(),
            Request::put(&custom_path)
                .body(Body::from(custom_body.as_slice()))
                .unwrap(),
        )
        .await,
        StatusCode::CREATED,
    );

    clock.set(1_011);
    assert_eq!(flywheel.run_maintenance_once().await.unwrap(), 1);
    assert_eq!(
        status(
            app.clone(),
            Request::get(&custom_path).body(Body::empty()).unwrap(),
        )
        .await,
        StatusCode::NOT_FOUND,
    );
    assert_eq!(
        status(
            app,
            Request::get(format!("/artifacts/sha256/{default_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await,
        StatusCode::OK,
    );
}

// References are non-pinning aliases: a referenced artifact is still evictable once its
// soft deadline passes. The reference row survives and still resolves as an alias.
#[tokio::test]
async fn references_do_not_pin_expired_artifacts() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(1_000));
    let space = Arc::new(ManualFreeSpace::new(1 << 40));
    let flywheel = Flywheel::open_with_space(spacious(&directory, 10), clock.clone(), space)
        .await
        .unwrap();
    let app = flywheel.router();
    let body = b"still referenced";
    let (digest, path) = artifact(body);
    assert_eq!(
        status(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await,
        StatusCode::CREATED
    );
    let binding = json!({"algorithm":"sha256", "digest":digest}).to_string();
    assert_eq!(
        status(
            app.clone(),
            Request::put("/references/live")
                .header("content-type", "application/json")
                .body(Body::from(binding))
                .unwrap()
        )
        .await,
        StatusCode::NO_CONTENT
    );

    clock.set(1_011);
    assert_eq!(flywheel.run_maintenance_once().await.unwrap(), 1);
    assert_eq!(
        status(
            app.clone(),
            Request::get(&path).body(Body::empty()).unwrap()
        )
        .await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(
            app,
            Request::get("/references/live")
                .body(Body::empty())
                .unwrap()
        )
        .await,
        StatusCode::OK
    );
}

// Injected disk pressure forces Reclaiming, which ignores both the soft deadline and
// the recency filter and evicts the oldest queue heads first toward the high watermark.
#[tokio::test]
async fn disk_pressure_evicts_oldest_first_ignoring_heat() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(2_000));
    // Start with ample space so both publications reserve and commit successfully.
    let space = Arc::new(ManualFreeSpace::new(1 << 40));
    let mut config = Config::new(directory.path());
    config.default_expiry_seconds = 1_000_000; // never deadline-expire during the test
    config.low_watermark_bytes = 4096;
    config.high_watermark_bytes = 8192;
    config.emergency_headroom_bytes = 0;
    config.reservation_extent_bytes = 16;
    config.reclaim_byte_limit = 8; // only enough headroom to evict one 8-byte body
    let flywheel = Flywheel::open_with_space(config, clock.clone(), space.clone())
        .await
        .unwrap();
    let app = flywheel.router();
    let (_, old) = artifact(b"12345678");
    let (_, new) = artifact(b"abcdefgh");
    assert_eq!(
        status(
            app.clone(),
            Request::put(&old).body(Body::from("12345678")).unwrap()
        )
        .await,
        StatusCode::CREATED
    );
    clock.set(2_001);
    assert_eq!(
        status(
            app.clone(),
            Request::put(&new).body(Body::from("abcdefgh")).unwrap()
        )
        .await,
        StatusCode::CREATED
    );

    // Mark the oldest as recently used; Reclaiming must ignore the heat.
    assert_eq!(
        status(app.clone(), Request::get(&old).body(Body::empty()).unwrap()).await,
        StatusCode::OK
    );

    // Drop free space below the low watermark to force Reclaiming.
    space.set(0);
    assert_eq!(flywheel.run_maintenance_once().await.unwrap(), 1);
    assert_eq!(
        status(app.clone(), Request::get(&old).body(Body::empty()).unwrap()).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(app, Request::get(&new).body(Body::empty()).unwrap()).await,
        StatusCode::OK
    );
}

// A build-cache PUT under disk pressure bypasses the store and returns protocol-
// compatible success without persisting anything, while a raw PUT reports pressure.
#[tokio::test]
async fn write_pressure_bypasses_build_cache_and_rejects_raw() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(3_000));
    // No free space at all: every reservation fails immediately.
    let space = Arc::new(ManualFreeSpace::new(0));
    let mut config = Config::new(directory.path());
    config.emergency_headroom_bytes = 0;
    let flywheel = Flywheel::open_with_space(config, clock, space)
        .await
        .unwrap();
    let app = flywheel.router();

    // Build-cache PUT bypasses with a success status and stores nothing.
    assert_eq!(
        status(
            app.clone(),
            Request::put("/build-cache/http/pressured")
                .body(Body::from("value"))
                .unwrap()
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        status(
            app.clone(),
            Request::get("/build-cache/http/pressured")
                .body(Body::empty())
                .unwrap()
        )
        .await,
        StatusCode::NOT_FOUND
    );

    // Raw artifact PUT reports insufficient storage.
    let (_, path) = artifact(b"raw under pressure");
    assert_eq!(
        status(
            app,
            Request::put(&path)
                .body(Body::from(b"raw under pressure".as_slice()))
                .unwrap()
        )
        .await,
        StatusCode::INSUFFICIENT_STORAGE
    );
}

#[tokio::test]
async fn compressed_expansion_never_exceeds_reserved_capacity() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(4_000));
    // The logical body fits in one byte, but even the smallest zstd frame does not.
    let space = Arc::new(ManualFreeSpace::new(1));
    let mut config = Config::new(directory.path());
    config.emergency_headroom_bytes = 0;
    config.low_watermark_bytes = 0;
    config.high_watermark_bytes = 0;
    config.reservation_extent_bytes = 1;
    let flywheel = Flywheel::open_with_space(config, clock, space)
        .await
        .unwrap();
    let app = flywheel.router();
    let path = "/build-cache/http/compression-expands";

    // Build-cache pressure remains protocol-compatible success, but the expanded
    // encoded body must not be published using capacity that was never reserved.
    assert_eq!(
        status(
            app.clone(),
            Request::put(path).body(Body::from("x")).unwrap()
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        status(app, Request::get(path).body(Body::empty()).unwrap()).await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn duplicate_publication_does_not_spend_capacity_twice() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(ManualClock::new(5_000));
    let space = Arc::new(ManualFreeSpace::new(10));
    let mut config = Config::new(directory.path());
    config.emergency_headroom_bytes = 0;
    config.low_watermark_bytes = 0;
    config.high_watermark_bytes = 0;
    config.reservation_extent_bytes = 5;
    let flywheel = Flywheel::open_with_space(config, clock, space)
        .await
        .unwrap();
    let app = flywheel.router();
    let body = b"12345";
    let (_, path) = artifact(body);

    assert_eq!(
        status(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await,
        StatusCode::CREATED
    );
    assert_eq!(
        status(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await,
        StatusCode::NO_CONTENT
    );

    // The duplicate used temporary staging capacity but allocated no new blocks; that
    // capacity must be available to a subsequent publication.
    let (_, next) = artifact(b"z");
    assert_eq!(
        status(app, Request::put(&next).body(Body::from("z")).unwrap()).await,
        StatusCode::CREATED
    );
}
