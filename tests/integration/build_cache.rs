use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use flywheel::{
    Flywheel,
    config::Config,
    manifest::{MANIFEST_VERSION, Manifest, ManifestEntry, manifest_key},
};
use futures_util::stream;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::sync::Notify;
use tower::ServiceExt;

async fn call(app: axum::Router, request: Request<Body>) -> axum::response::Response {
    app.oneshot(request).await.unwrap()
}

#[tokio::test]
async fn generic_http_cache_replaces_opaque_keys_with_immutable_content() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    for body in ["first", "second"] {
        assert_eq!(
            call(
                app.clone(),
                Request::put("/build-cache/http/tool-key_1")
                    .body(Body::from(body))
                    .unwrap()
            )
            .await
            .status(),
            StatusCode::OK
        );
    }
    let response = call(
        app,
        Request::get("/build-cache/http/tool-key_1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "second"
    );
}

/// Two clients writing the same key concurrently stage independently: the
/// duplicate's 200 is its own commit, so the entry is served immediately — even
/// while the first upload is still blocked mid-body.
#[tokio::test]
async fn concurrent_http_cache_put_commits_independently_before_the_leader() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let path = "/build-cache/http/shared-monorepo-key";
    let body = bytes::Bytes::from_static(b"one expensive build output");
    let leader_body_polled = Arc::new(Notify::new());
    let release_leader_body = Arc::new(Notify::new());
    let leader_stream = stream::once({
        let body = body.clone();
        let leader_body_polled = Arc::clone(&leader_body_polled);
        let release_leader_body = Arc::clone(&release_leader_body);
        async move {
            leader_body_polled.notify_one();
            release_leader_body.notified().await;
            Ok::<_, Infallible>(body)
        }
    });
    let leader = tokio::spawn(call(
        app.clone(),
        Request::put(path)
            .body(Body::from_stream(leader_stream))
            .unwrap(),
    ));
    leader_body_polled.notified().await;

    let follower = tokio::time::timeout(
        Duration::from_secs(1),
        call(
            app.clone(),
            Request::put(path).body(Body::from(body.clone())).unwrap(),
        ),
    )
    .await
    .expect("a duplicate cache PUT must not wait for the in-flight leader");
    assert_eq!(follower.status(), StatusCode::OK);

    // The follower's 200 is its own commit: the entry serves while the leader is
    // still mid-body.
    let stored = call(app.clone(), Request::get(path).body(Body::empty()).unwrap()).await;
    assert_eq!(stored.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(stored.into_body(), usize::MAX).await.unwrap(),
        body
    );

    release_leader_body.notify_one();
    assert_eq!(leader.await.unwrap().status(), StatusCode::OK);
    let stored = call(app, Request::get(path).body(Body::empty()).unwrap()).await;
    assert_eq!(stored.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(stored.into_body(), usize::MAX).await.unwrap(),
        body
    );
    // Identical logical bytes share a digest, so two stagings still land one body.
    assert_eq!(stored_artifact_files(directory.path()).len(), 1);
}

fn stored_artifact_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![
        root.join("artifacts")
            .join("00000000000000000000000000")
            .join("sha256"),
    ];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

#[tokio::test]
async fn http_cache_bodies_are_stored_compressed_and_served_by_negotiation() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = "a highly compressible build output line\n".repeat(200);
    assert_eq!(
        call(
            app.clone(),
            Request::put("/build-cache/http/compressed-key")
                .body(Body::from(body.clone()))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::OK
    );

    // At rest: one artifact file, zstd-framed and much smaller than the logical bytes.
    let stored = stored_artifact_files(directory.path());
    assert_eq!(stored.len(), 1);
    let on_disk = std::fs::read(&stored[0]).unwrap();
    assert!(on_disk.len() < body.len() / 2);
    assert_eq!(&on_disk[..4], &[0x28, 0xb5, 0x2f, 0xfd]);

    // A plain client gets the identity bytes transparently decompressed.
    let plain = call(
        app.clone(),
        Request::get("/build-cache/http/compressed-key")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(plain.status(), StatusCode::OK);
    assert_eq!(
        plain
            .headers()
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap(),
        body.len().to_string()
    );
    assert!(plain.headers().get(header::CONTENT_ENCODING).is_none());
    assert_eq!(
        to_bytes(plain.into_body(), usize::MAX).await.unwrap(),
        body.as_bytes()
    );

    // A zstd-capable client gets the stored frame passed through untouched.
    let negotiated = call(
        app.clone(),
        Request::get("/build-cache/http/compressed-key")
            .header(header::ACCEPT_ENCODING, "gzip, zstd")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(negotiated.status(), StatusCode::OK);
    assert_eq!(
        negotiated
            .headers()
            .get(header::CONTENT_ENCODING)
            .unwrap()
            .to_str()
            .unwrap(),
        "zstd"
    );
    let compressed = to_bytes(negotiated.into_body(), usize::MAX).await.unwrap();
    assert_eq!(compressed.as_ref(), on_disk.as_slice());
    let mut decoder = async_compression::tokio::bufread::ZstdDecoder::new(compressed.as_ref());
    let mut decompressed = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut decoder, &mut decompressed)
        .await
        .unwrap();
    assert_eq!(decompressed, body.as_bytes());

    // Compressed representations ignore Range and serve the complete negotiated body.
    let ranged = call(
        app.clone(),
        Request::get("/build-cache/http/compressed-key")
            .header(header::RANGE, "bytes=2-15")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(ranged.status(), StatusCode::OK);
    assert!(ranged.headers().get(header::ACCEPT_RANGES).is_none());
    assert!(ranged.headers().get(header::CONTENT_RANGE).is_none());
    assert_eq!(
        to_bytes(ranged.into_body(), usize::MAX).await.unwrap(),
        body.as_bytes()
    );

    let negotiated_range = call(
        app.clone(),
        Request::get("/build-cache/http/compressed-key")
            .header(header::RANGE, "bytes=2-15")
            .header(header::ACCEPT_ENCODING, "zstd")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(negotiated_range.status(), StatusCode::OK);
    assert_eq!(negotiated_range.headers()[header::CONTENT_ENCODING], "zstd");
    assert!(
        negotiated_range
            .headers()
            .get(header::ACCEPT_RANGES)
            .is_none()
    );
    assert!(
        negotiated_range
            .headers()
            .get(header::CONTENT_RANGE)
            .is_none()
    );
    assert_eq!(
        to_bytes(negotiated_range.into_body(), usize::MAX)
            .await
            .unwrap(),
        on_disk
    );

    let head = call(
        app,
        Request::head("/build-cache/http/compressed-key")
            .header(header::RANGE, "bytes=2-15")
            .header(header::ACCEPT_ENCODING, "zstd")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()[header::CONTENT_ENCODING], "zstd");
    assert_eq!(
        head.headers()[header::CONTENT_LENGTH],
        on_disk.len().to_string()
    );
    assert!(head.headers().get(header::ACCEPT_RANGES).is_none());
    assert!(head.headers().get(header::CONTENT_RANGE).is_none());
    assert!(
        to_bytes(head.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn empty_compressed_bodies_ignore_ranges_with_ordinary_negotiation() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let path = "/build-cache/http/empty-compressed";
    assert_eq!(
        call(app.clone(), Request::put(path).body(Body::empty()).unwrap())
            .await
            .status(),
        StatusCode::OK
    );
    let stored = stored_artifact_files(directory.path());
    assert_eq!(stored.len(), 1);
    let on_disk = std::fs::read(&stored[0]).unwrap();
    assert!(!on_disk.is_empty());

    let plain = call(
        app.clone(),
        Request::get(path)
            .header(header::RANGE, "bytes=0-0")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(plain.status(), StatusCode::OK);
    assert_eq!(plain.headers()[header::CONTENT_LENGTH], "0");
    assert!(plain.headers().get(header::ACCEPT_RANGES).is_none());
    assert!(plain.headers().get(header::CONTENT_RANGE).is_none());
    assert!(
        to_bytes(plain.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let negotiated = call(
        app,
        Request::get(path)
            .header(header::RANGE, "bytes=0-0")
            .header(header::ACCEPT_ENCODING, "zstd")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(negotiated.status(), StatusCode::OK);
    assert_eq!(negotiated.headers()[header::CONTENT_ENCODING], "zstd");
    assert_eq!(
        negotiated.headers()[header::CONTENT_LENGTH],
        on_disk.len().to_string()
    );
    assert_eq!(
        to_bytes(negotiated.into_body(), usize::MAX).await.unwrap(),
        on_disk
    );
}

#[tokio::test]
async fn status_returns_the_locally_stored_manifest_for_a_session() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        entries: std::collections::HashMap::from([(
            "aa".repeat(32),
            ManifestEntry {
                output: "bb".repeat(32),
                size: 42,
                last_seen: 7,
            },
        )]),
    };
    // The manifest is stored through the ordinary build-cache route under the
    // shared key derivation, exactly as cacheprog's finalize writes it.
    assert_eq!(
        call(
            app.clone(),
            Request::put(format!(
                "/build-cache/http/{}",
                manifest_key("ci example.com/widget linux/amd64")
            ))
            .body(Body::from(serde_json::to_vec(&manifest).unwrap()))
            .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK
    );

    let response = call(
        app,
        Request::get("/status?session=ci%20example.com%2Fwidget%20linux%2Famd64")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let returned: Manifest =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(returned, manifest);
}

#[tokio::test]
async fn status_degrades_unknown_sessions_to_an_empty_manifest() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();

    let response = call(
        app,
        Request::get("/status?session=never-built")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let returned: Manifest =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(returned, Manifest::empty());
}

#[tokio::test]
async fn status_requires_the_session_parameter() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();

    let response = call(app, Request::get("/status").body(Body::empty()).unwrap()).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bazel_cas_bodies_stay_identity_on_disk() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = b"raw bazel cas bytes".as_slice();
    let digest = hex::encode(Sha256::digest(body));
    assert_eq!(
        call(
            app,
            Request::put(format!("/build-cache/bazel/cas/{digest}"))
                .body(Body::from(body))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::OK
    );
    let stored = stored_artifact_files(directory.path());
    assert_eq!(stored.len(), 1);
    assert_eq!(std::fs::read(&stored[0]).unwrap(), body);
}

#[tokio::test]
async fn bazel_cas_validates_hash_while_action_cache_remains_opaque() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = b"bazel bytes";
    let digest = hex::encode(Sha256::digest(body));
    assert_eq!(
        call(
            app.clone(),
            Request::put(format!("/build-cache/bazel/cas/{digest}"))
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            Request::put(format!("/build-cache/bazel/cas/{}", "0".repeat(64)))
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            app.clone(),
            Request::put(format!("/build-cache/bazel/ac/{}", "0".repeat(64)))
                .body(Body::from("opaque action result"))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn protected_channel_auth_applies_to_build_cache_routes() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = call(
        app.clone(),
        Request::post("/channels")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json!({"access_control":true}).to_string()))
            .unwrap(),
    )
    .await;
    let registered: Value =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let channel = registered["channel"].as_str().unwrap();
    let token = registered["token"].as_str().unwrap();
    let path = format!("/channels/{channel}/build-cache/http/key");
    assert_eq!(
        call(
            app.clone(),
            Request::get(&path).body(Body::empty()).unwrap()
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app,
            Request::put(&path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from("value"))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::OK
    );
}
