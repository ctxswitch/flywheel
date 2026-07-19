use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use flywheel::{
    Flywheel, cacheprog::run_with_io, cli::CacheprogArgs, config::Config, manifest::manifest_key,
};
use futures_util::stream;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Notify,
};
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

/// Two builds of one session against a real server over TCP: the first
/// populates the cache and finalizes the manifest, the second bootstraps from
/// one plain manifest-key GET, synthesizes the empty object locally, and
/// downloads each distinct nonzero output at most once — never once per action.
#[tokio::test]
async fn warm_build_costs_one_manifest_get_plus_one_get_per_distinct_output() {
    fn put_line(id: u32, action: &[u8], body: &[u8]) -> String {
        let mut line = format!(
            "{{\"ID\":{id},\"Command\":\"put\",\"ActionID\":\"{}\",\"OutputID\":\"{}\",\"BodySize\":{}}}\n",
            STANDARD.encode(action),
            STANDARD.encode(Sha256::digest(body)),
            body.len(),
        );
        if !body.is_empty() {
            line.push_str(&format!("\"{}\"\n", STANDARD.encode(body)));
        }
        line
    }

    let directory = TempDir::new().unwrap();
    let flywheel = Flywheel::open(Config::new(directory.path())).await.unwrap();
    let requests: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let router = flywheel.router().layer(axum::middleware::from_fn({
        let requests = Arc::clone(&requests);
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let requests = Arc::clone(&requests);
            async move {
                requests.lock().unwrap().push((
                    request.method().to_string(),
                    request.uri().path().to_owned(),
                ));
                next.run(request).await
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let session = "e2e integration/widget linux/amd64";
    let args = |cache_dir: &std::path::Path| CacheprogArgs {
        url: format!("http://{address}/build-cache/http/"),
        token: None,
        cache_dir: Some(cache_dir.into()),
        ephemeral_cache: false,
        session: Some(session.into()),
        prune_days: 14,
        prefetch_concurrency: 4,
    };

    // One shared empty output under several actions, plus three distinct
    // nonzero bodies, one of them produced by two different actions.
    let zero_actions: Vec<Vec<u8>> = (1u8..=3).map(|n| vec![0, n]).collect();
    let sized: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![1, 1], b"first shared body".to_vec()),
        (vec![1, 2], b"first shared body".to_vec()),
        (vec![2, 1], b"second body".to_vec()),
        (vec![3, 1], b"third body".to_vec()),
    ];

    // Build 1 populates: a put per action, close writes the manifest.
    let mut input = String::new();
    let mut id = 1u32;
    for action in &zero_actions {
        input.push_str(&put_line(id, action, b""));
        id += 1;
    }
    for (action, body) in &sized {
        input.push_str(&put_line(id, action, body));
        id += 1;
    }
    input.push_str(&format!("{{\"ID\":{id},\"Command\":\"close\"}}\n"));
    let build_one_dir = TempDir::new().unwrap();
    let mut output = Vec::new();
    run_with_io(
        args(build_one_dir.path()),
        BufReader::new(input.as_bytes()),
        &mut output,
    )
    .await
    .unwrap();
    for line in String::from_utf8(output).unwrap().lines() {
        let response: Value = serde_json::from_str(line).unwrap();
        assert!(response.get("Err").is_none(), "build 1 failed: {response}");
    }

    // Build 2 starts locally cold: everything it knows must come from the
    // manifest bootstrap.
    let populate_requests = requests.lock().unwrap().len();
    let build_two_dir = TempDir::new().unwrap();
    let (mut input, cache_input) = tokio::io::duplex(64 * 1024);
    let (cache_output, output) = tokio::io::duplex(64 * 1024);
    let cache = tokio::spawn(run_with_io(
        args(build_two_dir.path()),
        BufReader::new(cache_input),
        cache_output,
    ));

    // The bootstrap runs unprompted: one manifest GET plus one GET per distinct
    // nonzero output, with the empty object elided.
    let manifest_path = format!("/build-cache/http/{}", manifest_key(session));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let warm = requests.lock().unwrap()[populate_requests..].to_vec();
        if warm.len() >= 4 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "prefetch stalled: {warm:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Foreground gets for every action — zero and nonzero — answer locally.
    let all_actions: Vec<&[u8]> = zero_actions
        .iter()
        .map(Vec::as_slice)
        .chain(sized.iter().map(|(action, _)| action.as_slice()))
        .collect();
    let mut warm_input = String::new();
    for (index, action) in all_actions.iter().enumerate() {
        warm_input.push_str(&format!(
            "{{\"ID\":{},\"Command\":\"get\",\"ActionID\":\"{}\"}}\n",
            100 + index,
            STANDARD.encode(action)
        ));
    }
    warm_input.push_str("{\"ID\":999,\"Command\":\"close\"}\n");
    input.write_all(warm_input.as_bytes()).await.unwrap();
    let mut output = BufReader::new(output);
    let mut responses = std::collections::HashMap::new();
    let mut line = String::new();
    loop {
        line.clear();
        output.read_line(&mut line).await.unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        let id = response["ID"].as_u64().unwrap();
        responses.insert(id, response);
        if id == 999 {
            break;
        }
    }
    drop(input);
    cache.await.unwrap().unwrap();
    server.abort();

    let empty_output = STANDARD.encode(Sha256::digest(b""));
    for (index, action) in all_actions.iter().enumerate() {
        let response = &responses[&(100 + index as u64)];
        assert!(
            response.get("Miss").is_none(),
            "warm get missed: {response}"
        );
        let expected = if index < zero_actions.len() {
            empty_output.clone()
        } else {
            let (_, body) = &sized[index - zero_actions.len()];
            STANDARD.encode(Sha256::digest(body))
        };
        assert_eq!(
            response["OutputID"].as_str().unwrap(),
            expected,
            "{action:?}"
        );
    }

    // The complete warm-build request set: one manifest GET, no request for any
    // zero-size action, one GET per distinct nonzero output, nothing repeated.
    let warm = requests.lock().unwrap()[populate_requests..].to_vec();
    assert_eq!(warm.len(), 4, "exactly the bootstrap traffic: {warm:?}");
    assert_eq!(
        warm.iter()
            .filter(|(method, path)| method == "GET" && path == &manifest_path)
            .count(),
        1,
        "warm requests: {warm:?}"
    );
    for action in &zero_actions {
        let path = format!("/build-cache/http/go-{}", hex::encode(action));
        assert!(
            !warm.iter().any(|(_, requested)| requested == &path),
            "zero-size action fetched: {warm:?}"
        );
    }
    let mut object_paths: Vec<&String> = warm
        .iter()
        .filter(|(_, path)| path != &manifest_path)
        .map(|(_, path)| path)
        .collect();
    object_paths.sort();
    object_paths.dedup();
    assert_eq!(
        object_paths.len(),
        3,
        "one GET per distinct nonzero output: {warm:?}"
    );
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
