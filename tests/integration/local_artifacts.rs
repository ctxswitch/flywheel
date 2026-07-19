use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use flywheel::{Flywheel, config::Config};
use futures_util::stream;
use sha2::{Digest as _, Sha256};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::sync::Notify;
use tower::ServiceExt;

fn digest(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

async fn request(app: axum::Router, request: Request<Body>) -> axum::response::Response {
    app.oneshot(request).await.unwrap()
}

#[tokio::test]
async fn responses_propagate_or_create_request_ids() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();

    let generated = request(
        app.clone(),
        Request::get("/health/ready").body(Body::empty()).unwrap(),
    )
    .await;
    let generated = generated.headers()["x-request-id"].to_str().unwrap();
    assert!(ulid::Ulid::from_string(generated).is_ok());

    let provided = request(
        app,
        Request::get("/health/ready")
            .header("x-request-id", "caller-request")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(provided.headers()["x-request-id"], "caller-request");
}

#[tokio::test]
async fn publishes_streams_ranges_and_recovers_local_artifacts() {
    let directory = TempDir::new().unwrap();
    let body = b"hello durable flywheel";
    let digest = digest(body);
    let path = format!("/artifacts/sha256/{digest}");

    {
        let flywheel = Flywheel::open(Config::new(directory.path())).await.unwrap();
        let app = flywheel.router();

        let response = request(
            app.clone(),
            Request::put(&path)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(body.as_slice()))
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let duplicate = request(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap(),
        )
        .await;
        assert_eq!(duplicate.status(), StatusCode::NO_CONTENT);

        let corrupt = request(
            app.clone(),
            Request::put(&path).body(Body::from("different")).unwrap(),
        )
        .await;
        assert_eq!(corrupt.status(), StatusCode::CONFLICT);

        let response = request(
            app.clone(),
            Request::get(&path).body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            body.len().to_string()
        );
        assert_eq!(
            response.headers()[header::ETAG],
            format!("\"sha256:{digest}\"")
        );
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            body.as_slice()
        );

        let range = request(
            app.clone(),
            Request::get(&path)
                .header(header::RANGE, "bytes=6-12")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            range.headers()[header::CONTENT_RANGE],
            format!("bytes 6-12/{}", body.len())
        );
        assert_eq!(
            to_bytes(range.into_body(), usize::MAX).await.unwrap(),
            &body[6..=12]
        );

        let head = request(app, Request::head(&path).body(Body::empty()).unwrap()).await;
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(
            head.headers()[header::CONTENT_LENGTH],
            body.len().to_string()
        );
        assert!(
            to_bytes(head.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }

    let recovered = Flywheel::open(Config::new(directory.path())).await.unwrap();
    let response = request(
        recovered.router(),
        Request::get(&path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        body.as_slice()
    );
}

#[tokio::test]
async fn identity_ranges_ignore_invalid_syntax_and_reject_only_non_overlapping_ranges() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = b"0123456789";
    let path = format!("/artifacts/sha256/{}", digest(body));
    assert_eq!(
        request(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::CREATED
    );

    let range = request(
        app.clone(),
        Request::get(&path)
            .header(header::RANGE, "bytes=2-5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.headers()[header::ACCEPT_RANGES], "bytes");
    assert_eq!(range.headers()[header::CONTENT_RANGE], "bytes 2-5/10");
    assert_eq!(
        to_bytes(range.into_body(), usize::MAX).await.unwrap(),
        &body[2..=5]
    );

    for invalid in [
        "bytes=broken",
        "items=0-1",
        "bytes=18446744073709551616-",
        "bytes=0-18446744073709551616",
        "bytes=0-1,4-5",
        "bytes=5-4",
    ] {
        let response = request(
            app.clone(),
            Request::get(&path)
                .header(header::RANGE, invalid)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{invalid}");
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert!(response.headers().get(header::CONTENT_RANGE).is_none());
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            body.as_slice(),
            "{invalid}"
        );
    }

    for unsatisfiable in ["bytes=20-30", "bytes=-0"] {
        let response = request(
            app.clone(),
            Request::get(&path)
                .header(header::RANGE, unsatisfiable)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "{unsatisfiable}"
        );
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */10");
    }

    let head = request(
        app.clone(),
        Request::head(&path)
            .header(header::RANGE, "bytes=2-5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()[header::CONTENT_LENGTH], "10");
    assert_eq!(head.headers()[header::ACCEPT_RANGES], "bytes");
    assert!(head.headers().get(header::CONTENT_RANGE).is_none());
    assert!(
        to_bytes(head.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    // GET-only build-cache routes are also invoked for HEAD by axum; they must pass
    // that method through so the shared artifact responder ignores Range.
    let cas_head = request(
        app,
        Request::head(format!("/build-cache/bazel/cas/{}", digest(body)))
            .header(header::RANGE, "bytes=2-5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(cas_head.status(), StatusCode::OK);
    assert_eq!(cas_head.headers()[header::CONTENT_LENGTH], "10");
    assert_eq!(cas_head.headers()[header::ACCEPT_RANGES], "bytes");
    assert!(cas_head.headers().get(header::CONTENT_RANGE).is_none());
}

#[tokio::test]
async fn empty_identity_representation_has_only_unsatisfiable_valid_ranges() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let path = format!("/artifacts/sha256/{}", digest(b""));
    assert_eq!(
        request(
            app.clone(),
            Request::put(&path).body(Body::empty()).unwrap()
        )
        .await
        .status(),
        StatusCode::CREATED
    );

    let unsatisfiable = request(
        app.clone(),
        Request::get(&path)
            .header(header::RANGE, "bytes=0-0")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(unsatisfiable.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(unsatisfiable.headers()[header::CONTENT_RANGE], "bytes */0");

    let malformed = request(
        app,
        Request::get(&path)
            .header(header::RANGE, "bytes=garbage")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(malformed.status(), StatusCode::OK);
    assert_eq!(malformed.headers()[header::CONTENT_LENGTH], "0");
    assert!(malformed.headers().get(header::CONTENT_RANGE).is_none());
    assert!(
        to_bytes(malformed.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Two clients uploading the same digest stage independently: on this durable
/// surface a 2xx must mean the responder's own commit happened, so the duplicate is
/// readable immediately — even while the first upload is still blocked mid-body.
/// (The old coalescing path acknowledged the duplicate before anything was
/// committed, which this test now forbids.)
#[tokio::test]
async fn concurrent_duplicate_put_commits_independently_before_the_leader() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = bytes::Bytes::from_static(b"one expensive build output");
    let path = format!("/artifacts/sha256/{}", digest(&body));
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
    let leader = tokio::spawn(request(
        app.clone(),
        Request::put(&path)
            .body(Body::from_stream(leader_stream))
            .unwrap(),
    ));
    leader_body_polled.notified().await;

    // The duplicate does not wait for the leader: it stages its own body and is the
    // first to commit.
    let follower = tokio::time::timeout(
        Duration::from_secs(1),
        request(
            app.clone(),
            Request::put(&path).body(Body::from(body.clone())).unwrap(),
        ),
    )
    .await
    .expect("a duplicate PUT must not wait for the in-flight leader");
    assert_eq!(follower.status(), StatusCode::CREATED);

    let after_follower = request(
        app.clone(),
        Request::get(&path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(after_follower.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(after_follower.into_body(), usize::MAX)
            .await
            .unwrap(),
        body
    );

    // The leader finishes second, finds the committed row, and reports the duplicate.
    release_leader_body.notify_one();
    assert_eq!(leader.await.unwrap().status(), StatusCode::NO_CONTENT);
    let after_leader = request(
        app.clone(),
        Request::get(&path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(after_leader.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(after_leader.into_body(), usize::MAX)
            .await
            .unwrap(),
        body
    );
    // Content addressing still dedups: two stagings, one final body on disk.
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

/// A failed upload leaves nothing behind — no staged residue, no poisoned identity —
/// and a later PUT of the same digest succeeds normally.
#[tokio::test]
async fn failed_publication_does_not_block_a_later_put() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = bytes::Bytes::from_static(b"retryable build output");
    let path = format!("/artifacts/sha256/{}", digest(&body));
    let failing_stream = stream::once(async {
        Err::<bytes::Bytes, _>(std::io::Error::other("injected upload failure"))
    });
    let failed = request(
        app.clone(),
        Request::put(&path)
            .body(Body::from_stream(failing_stream))
            .unwrap(),
    )
    .await;
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let retry = request(
        app.clone(),
        Request::put(&path).body(Body::from(body.clone())).unwrap(),
    )
    .await;
    assert_eq!(retry.status(), StatusCode::CREATED);
    let stored = request(app, Request::get(&path).body(Body::empty()).unwrap()).await;
    assert_eq!(stored.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(stored.into_body(), usize::MAX).await.unwrap(),
        body
    );
}

#[tokio::test]
async fn logical_references_are_atomic_and_retryable() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = b"referenced";
    let digest = digest(body);
    let artifact = format!("/artifacts/sha256/{digest}");
    assert_eq!(
        request(
            app.clone(),
            Request::put(&artifact)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::CREATED
    );

    let binding = serde_json::json!({"algorithm": "sha256", "digest": digest});
    for _ in 0..2 {
        let response = request(
            app.clone(),
            Request::put("/references/toolchain")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(binding.to_string()))
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response = request(
        app.clone(),
        Request::get("/references/toolchain")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap()
        )
        .unwrap(),
        binding
    );

    for _ in 0..2 {
        assert_eq!(
            request(
                app.clone(),
                Request::delete("/references/toolchain")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .status(),
            StatusCode::NO_CONTENT
        );
    }
}

// A metadata record whose body has been reclaimed underneath it self-heals on GET:
// the request misses and the dangling record is removed, so a later GET also misses.
#[tokio::test]
async fn get_self_heals_metadata_pointing_at_a_missing_body() {
    let directory = TempDir::new().unwrap();
    let flywheel = Flywheel::open(Config::new(directory.path())).await.unwrap();
    let app = flywheel.router();
    let body = b"self-healing body";
    let digest = digest(body);
    let path = format!("/artifacts/sha256/{digest}");
    assert_eq!(
        request(
            app.clone(),
            Request::put(&path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::CREATED
    );

    // Remove the underlying body directly, leaving only the metadata record.
    let file = directory
        .path()
        .join("artifacts/00000000000000000000000000/sha256")
        .join(&digest[0..2])
        .join(&digest[2..4])
        .join(&digest);
    tokio::fs::remove_file(&file).await.unwrap();

    for _ in 0..2 {
        assert_eq!(
            request(
                app.clone(),
                Request::get(&path).body(Body::empty()).unwrap()
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
    }
}

#[tokio::test]
async fn startup_reconciliation_removes_abandoned_and_orphan_files() {
    let directory = TempDir::new().unwrap();
    let orphan_body = b"orphan";
    let digest = digest(orphan_body);
    let orphan = directory
        .path()
        .join("artifacts/00000000000000000000000000/sha256")
        .join(&digest[0..2])
        .join(&digest[2..4])
        .join(&digest);
    let temporary = directory
        .path()
        .join("artifacts/00000000000000000000000000/tmp/abandoned.part");
    tokio::fs::create_dir_all(orphan.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::create_dir_all(temporary.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&orphan, orphan_body).await.unwrap();
    tokio::fs::write(&temporary, b"partial").await.unwrap();

    let _flywheel = Flywheel::open(Config::new(directory.path())).await.unwrap();

    // A bounded orphan scan removes the final file that has no metadata, and staging
    // cleanup removes the abandoned temporary.
    assert!(!tokio::fs::try_exists(orphan).await.unwrap());
    assert!(!tokio::fs::try_exists(temporary).await.unwrap());
}

/// The foreground budget lives at the transport seam and is held by a streaming
/// response until its body is dropped: while a GET's body sits unconsumed, the next
/// request sheds with 429, and dropping the response frees the slot.
#[tokio::test]
async fn streaming_get_holds_the_transport_budget_and_sheds_with_429() {
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.foreground_concurrency = 1;
    let app = Flywheel::open(config).await.unwrap().router();

    let body = b"budgeted body";
    let path = format!("/artifacts/sha256/{}", digest(body));
    let put = request(
        app.clone(),
        Request::put(&path)
            .body(Body::from(body.as_slice()))
            .unwrap(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::CREATED);

    // The permit is moved into the body stream when the response is built, so
    // holding the unconsumed response deterministically holds the budget's only slot.
    let held = request(
        app.clone(),
        Request::get(&path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(held.status(), StatusCode::OK);

    let shed = request(
        app.clone(),
        Request::get(&path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(shed.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(shed.headers()[header::RETRY_AFTER], "1");

    drop(held);
    let served = request(app, Request::get(&path).body(Body::empty()).unwrap()).await;
    assert_eq!(served.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(served.into_body(), usize::MAX).await.unwrap(),
        body.as_slice()
    );
}
