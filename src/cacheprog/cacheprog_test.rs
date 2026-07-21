use super::{Request, Response, read_protocol_line, run_with_io, run_with_shutdown, session};
use crate::{
    cli::CacheprogArgs,
    manifest::{MANIFEST_MAX_AGE_SECONDS, MANIFEST_VERSION, Manifest, ManifestEntry, manifest_key},
};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get as route_get,
};
use sha2::Digest as _;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Notify, oneshot},
};

/// Serves `app` on an ephemeral loopback port, returning the bound address and
/// the server task the caller aborts when the test is done.
async fn serve(app: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (address, task)
}

fn args(url: String, cache_dir: &std::path::Path) -> CacheprogArgs {
    CacheprogArgs {
        url,
        token: None,
        cache_dir: Some(cache_dir.into()),
        ephemeral_cache: false,
        session: Some("cacheprog-test".into()),
        prune_days: 14,
        // Concurrency 0 keeps object traffic out of the protocol tests; the one
        // bootstrap manifest GET simply misses. Prefetch behavior has its own
        // tests below with explicit concurrency.
        prefetch_concurrency: 0,
    }
}

#[tokio::test]
async fn ephemeral_cache_is_removed_after_close() {
    let app = Router::new().route(
        "/{*key}",
        route_get(|| async { StatusCode::NOT_FOUND }).put(|| async { StatusCode::OK }),
    );
    let (address, server) = serve(app).await;
    let parent = tempfile::tempdir().unwrap();
    let mut options = args(format!("http://{address}/"), parent.path());
    options.ephemeral_cache = true;
    let (mut input, cache_input) = tokio::io::duplex(4096);
    let (cache_output, output) = tokio::io::duplex(4096);
    let cache = tokio::spawn(run_with_io(
        options,
        BufReader::new(cache_input),
        cache_output,
    ));
    let mut output = BufReader::new(output);
    let mut line = String::new();

    output.read_line(&mut line).await.unwrap();
    input
        .write_all(
            concat!(
                "{\"ID\":1,\"Command\":\"put\",\"ActionID\":\"Ag==\",",
                "\"OutputID\":\"LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=\",",
                "\"BodySize\":5}\n\n\"aGVsbG8=\"\n",
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    line.clear();
    output.read_line(&mut line).await.unwrap();
    let put = serde_json::from_str::<serde_json::Value>(&line).unwrap();
    let disk_path = put["DiskPath"].as_str().unwrap().to_owned();
    assert!(std::path::Path::new(&disk_path).is_file());

    input
        .write_all(b"{\"ID\":2,\"Command\":\"close\"}\n\n")
        .await
        .unwrap();
    line.clear();
    output.read_line(&mut line).await.unwrap();
    drop(input);
    cache.await.unwrap().unwrap();
    server.abort();

    let disk_path = std::path::Path::new(&disk_path);
    assert!(
        !disk_path.exists(),
        "the per-process cache must be deleted after close"
    );
    assert!(
        !parent.path().join("flywheel-cacheprog").exists(),
        "ephemeral caching must not create the reusable cache directory"
    );
}

#[tokio::test]
async fn shutdown_cancels_in_flight_requests_and_removes_ephemeral_cache() {
    let get_started = Arc::new(Notify::new());
    let release_get = Arc::new(Notify::new());
    let app = Router::new().route(
        "/{*key}",
        route_get({
            let get_started = Arc::clone(&get_started);
            let release_get = Arc::clone(&release_get);
            move |Path(key): Path<String>| {
                let get_started = Arc::clone(&get_started);
                let release_get = Arc::clone(&release_get);
                async move {
                    if key == manifest_key("cacheprog-test") {
                        return StatusCode::NOT_FOUND;
                    }
                    get_started.notify_one();
                    release_get.notified().await;
                    StatusCode::NOT_FOUND
                }
            }
        }),
    );
    let (address, server) = serve(app).await;
    let parent = tempfile::tempdir().unwrap();
    let mut options = args(format!("http://{address}/"), parent.path());
    options.ephemeral_cache = true;
    let (mut input, cache_input) = tokio::io::duplex(4096);
    let (cache_output, output) = tokio::io::duplex(4096);
    let (shutdown, shutdown_requested) = oneshot::channel();
    let (shutdown_selected, selected) = oneshot::channel();
    let cache = tokio::spawn(run_with_shutdown(
        options,
        BufReader::new(cache_input),
        cache_output,
        async move {
            let _ = shutdown_requested.await;
            let _ = shutdown_selected.send(());
        },
    ));
    let mut output = BufReader::new(output);
    let mut ready = String::new();

    output.read_line(&mut ready).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&ready).unwrap()["ID"],
        0
    );
    let mut entries = tokio::fs::read_dir(parent.path()).await.unwrap();
    let directory = entries
        .next_entry()
        .await
        .unwrap()
        .expect("cacheprog created its ephemeral cache")
        .path();
    assert!(directory.is_dir());
    assert!(entries.next_entry().await.unwrap().is_none());

    input
        .write_all(b"{\"ID\":1,\"Command\":\"get\",\"ActionID\":\"AQ==\"}\n")
        .await
        .unwrap();
    get_started.notified().await;
    drop(output);
    shutdown.send(()).unwrap();
    selected.await.unwrap();
    release_get.notify_one();
    let result = cache.await.unwrap();
    server.abort();

    result.unwrap();

    assert!(
        !directory.exists(),
        "shutdown must remove the ephemeral cache"
    );
    assert!(
        tokio::fs::read_dir(parent.path())
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none(),
        "shutdown must leave the cache parent empty"
    );
}

#[test]
fn uses_go_initialism_field_names_on_the_wire() {
    let request: Request = serde_json::from_value(serde_json::json!({
        "ID": 17,
        "Command": "put",
        "ActionID": "AQI=",
        "OutputID": "AwQ=",
        "BodySize": 2
    }))
    .unwrap();

    assert_eq!(request.id, 17);
    assert_eq!(request.action_id, [1, 2]);
    assert_eq!(request.output_id, [3, 4]);

    let response = serde_json::to_value(Response {
        id: request.id,
        output_id: request.output_id,
        ..Response::default()
    })
    .unwrap();

    assert_eq!(response["ID"], 17);
    assert_eq!(response["OutputID"], "AwQ=");
    assert!(response.get("Id").is_none());
    assert!(response.get("OutputId").is_none());
}

#[tokio::test]
async fn ignores_blank_lines_between_go_protocol_messages() {
    let input = b"\n{\"ID\":1,\"Command\":\"close\"}\n\n";
    let reader = BufReader::new(&input[..]);
    let mut output = Vec::new();
    let cache_dir = tempfile::tempdir().unwrap();

    run_with_io(
        args(
            "http://127.0.0.1:9/build-cache/http/".into(),
            cache_dir.path(),
        ),
        reader,
        &mut output,
    )
    .await
    .unwrap();

    let responses = String::from_utf8(output).unwrap();
    let responses = responses.lines().collect::<Vec<_>>();
    assert_eq!(responses.len(), 2);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(responses[0]).unwrap()["ID"],
        0
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(responses[1]).unwrap()["ID"],
        1
    );
}

#[tokio::test]
async fn skips_blank_line_before_put_body() {
    let input = b"\n\"AQI=\"\n";
    let mut reader = BufReader::new(&input[..]);
    let mut line = Vec::new();

    assert!(read_protocol_line(&mut reader, &mut line).await.unwrap());
    assert_eq!(String::from_utf8(line).unwrap().trim_end(), "\"AQI=\"");
}

#[tokio::test]
async fn completes_independent_requests_concurrently() {
    let app = Router::new().route(
        "/{*key}",
        route_get(|Path(key): Path<String>| async move {
            if key.ends_with("go-01") {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            StatusCode::NOT_FOUND
        }),
    );
    let (address, server) = serve(app).await;
    let input = concat!(
        "{\"ID\":1,\"Command\":\"get\",\"ActionID\":\"AQ==\"}\n\n",
        "{\"ID\":2,\"Command\":\"get\",\"ActionID\":\"Ag==\"}\n\n",
        "{\"ID\":3,\"Command\":\"close\"}\n\n",
    );
    let mut output = Vec::new();
    let cache_dir = tempfile::tempdir().unwrap();

    run_with_io(
        args(format!("http://{address}/"), cache_dir.path()),
        BufReader::new(input.as_bytes()),
        &mut output,
    )
    .await
    .unwrap();
    server.abort();

    let ids = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["ID"].clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, [0, 2, 1, 3]);
}

#[tokio::test]
async fn pruning_removes_only_expired_objects_and_abandoned_temp_files() {
    let directory = tempfile::tempdir().unwrap();
    let now = std::time::SystemTime::now();
    let age = |path: &std::path::Path, seconds: u64| {
        let file = std::fs::File::options().write(true).open(path).unwrap();
        file.set_modified(now - Duration::from_secs(seconds))
            .unwrap();
    };
    let object_max_age = Duration::from_secs(14 * 24 * 60 * 60);
    let expired = directory.path().join("aa".repeat(32));
    let live = directory.path().join("bb".repeat(32));
    let abandoned = directory.path().join(".old.tmp");
    let in_flight = directory.path().join(".new.tmp");
    for path in [&expired, &live, &abandoned, &in_flight] {
        std::fs::write(path, b"x").unwrap();
    }
    age(&expired, object_max_age.as_secs() + 60);
    age(&live, object_max_age.as_secs() - 60);
    age(&abandoned, super::TEMPORARY_MAX_AGE.as_secs() + 60);

    // Disabled object pruning keeps every object but still clears temp trash.
    super::prune_stale_files(directory.path(), now, None).await;
    assert!(expired.exists());
    assert!(live.exists());
    assert!(!abandoned.exists());
    assert!(in_flight.exists());

    super::prune_stale_files(directory.path(), now, Some(object_max_age)).await;
    assert!(!expired.exists());
    assert!(live.exists());
    assert!(in_flight.exists());
}

#[tokio::test]
async fn manifest_known_gets_answer_locally_without_any_request() {
    // The base URL is unreachable on purpose: a local answer must never touch
    // the network, so any request here fails the test.
    let base = reqwest::Url::parse("http://127.0.0.1:9/build-cache/http/").unwrap();
    let client = reqwest::Client::new();
    let cache_dir = tempfile::tempdir().unwrap();
    let body = b"prefetched object body";
    let output = hex::encode(sha2::Sha256::digest(body));
    tokio::fs::write(cache_dir.path().join(&output), body)
        .await
        .unwrap();
    let action = [7u8; 32];
    let session = session::SessionState::default();
    session
        .manifest
        .set(Manifest {
            version: MANIFEST_VERSION,
            entries: HashMap::from([(
                hex::encode(action),
                ManifestEntry {
                    output: output.clone(),
                    size: body.len() as u64,
                    last_seen: 0,
                },
            )]),
        })
        .unwrap();

    let response = super::get(
        &client,
        &base,
        None,
        cache_dir.path(),
        &session,
        Request {
            id: 9,
            command: "get".into(),
            action_id: action.to_vec(),
            output_id: Vec::new(),
            body_size: 0,
        },
    )
    .await
    .unwrap();

    assert!(!response.miss);
    assert_eq!(hex::encode(&response.output_id), output);
    assert_eq!(response.size, body.len() as u64);
    assert!(std::path::Path::new(&response.disk_path).is_file());
}

/// A get answered out of the local object directory is still usage. The merged
/// manifest has to carry it forward with a fresh `last_seen`, or the actions
/// prefetch predicts perfectly are exactly the ones that age out and get
/// evicted first — the manifest would decay to only what it mispredicted.
#[tokio::test]
async fn locally_answered_gets_refresh_their_manifest_entry() {
    // Unreachable on purpose: a local answer must never touch the network.
    let base = reqwest::Url::parse("http://127.0.0.1:9/build-cache/http/").unwrap();
    let client = reqwest::Client::new();
    let cache_dir = tempfile::tempdir().unwrap();
    let body = b"perfectly predicted object";
    let output = hex::encode(sha2::Sha256::digest(body));
    tokio::fs::write(cache_dir.path().join(&output), body)
        .await
        .unwrap();
    let action = hex::encode([9u8; 32]);
    let stored = Manifest {
        version: MANIFEST_VERSION,
        entries: HashMap::from([(
            action.clone(),
            ManifestEntry {
                output: output.clone(),
                size: body.len() as u64,
                last_seen: 100,
            },
        )]),
    };
    let session = session::SessionState::default();
    session.manifest.set(stored.clone()).unwrap();

    let response = super::get(
        &client,
        &base,
        None,
        cache_dir.path(),
        &session,
        Request {
            id: 3,
            command: "get".into(),
            action_id: hex::decode(&action).unwrap(),
            output_id: Vec::new(),
            body_size: 0,
        },
    )
    .await
    .unwrap();
    assert!(!response.miss);

    // Finalize past the retention window: only a recorded use keeps the entry.
    let now = 100 + MANIFEST_MAX_AGE_SECONDS + 1;
    let used = session.used.lock().unwrap().clone();
    let merged = session::merge_manifest(Some(stored), &used, now);
    assert_eq!(
        merged.entries.get(&action).map(|entry| entry.last_seen),
        Some(now),
        "a locally answered get must refresh its manifest entry"
    );
}

#[tokio::test]
async fn preserves_a_put_body_while_an_earlier_request_completes() {
    let get_started = Arc::new(Notify::new());
    let release_get = Arc::new(Notify::new());
    let app = Router::new().route(
        "/{*key}",
        route_get({
            let get_started = Arc::clone(&get_started);
            let release_get = Arc::clone(&release_get);
            move |Path(key): Path<String>| {
                let get_started = Arc::clone(&get_started);
                let release_get = Arc::clone(&release_get);
                async move {
                    // The bootstrap manifest GET misses immediately; only the
                    // protocol get participates in the interleaving under test.
                    if key == manifest_key("cacheprog-test") {
                        return StatusCode::NOT_FOUND;
                    }
                    get_started.notify_one();
                    release_get.notified().await;
                    StatusCode::NOT_FOUND
                }
            }
        })
        .put(|| async { StatusCode::OK }),
    );
    let (address, server) = serve(app).await;
    let cache_dir = tempfile::tempdir().unwrap();
    let (mut input, cache_input) = tokio::io::duplex(1);
    let (cache_output, output) = tokio::io::duplex(4096);
    let cache = tokio::spawn(run_with_io(
        args(format!("http://{address}/"), cache_dir.path()),
        BufReader::new(cache_input),
        cache_output,
    ));
    let mut output = BufReader::new(output);
    let mut line = String::new();

    output.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["ID"],
        0
    );
    input
        .write_all(b"{\"ID\":1,\"Command\":\"get\",\"ActionID\":\"AQ==\"}\n\n")
        .await
        .unwrap();
    get_started.notified().await;

    // The one-byte pipe makes the protocol reader consume this partial body and
    // block waiting for its newline. Completing request 1 then interrupts that
    // wait; the partial body must still be present when reading resumes.
    input
        .write_all(
            concat!(
                "{\"ID\":2,\"Command\":\"put\",\"ActionID\":\"Ag==\",",
                "\"OutputID\":\"LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=\",",
                "\"BodySize\":5}\n\n\"aGV"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    release_get.notify_one();
    line.clear();
    output.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["ID"],
        1
    );

    input
        .write_all(b"sbG8=\"\n{\"ID\":3,\"Command\":\"close\"}\n\n")
        .await
        .unwrap();
    drop(input);

    line.clear();
    output.read_line(&mut line).await.unwrap();
    let put = serde_json::from_str::<serde_json::Value>(&line).unwrap();
    assert_eq!(put["ID"], 2);
    assert!(put.get("Err").is_none(), "put failed: {put}");
    line.clear();
    output.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["ID"],
        3
    );

    cache.await.unwrap().unwrap();
    server.abort();

    // The local object cache persists across sessions: the verified body the
    // put wrote is still on disk after close.
    let object = cache_dir
        .path()
        .join("flywheel-cacheprog")
        .join(hex::encode(sha2::Sha256::digest(b"hello")));
    assert!(object.is_file());
}

/// A fake shard for prefetch tests: one build-cache route serving the manifest
/// as JSON under its derived key and object bodies under theirs, recording every
/// requested key and the observed download concurrency.
struct PrefetchBackend {
    manifest_key: String,
    manifest: Manifest,
    bodies: HashMap<String, Vec<u8>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    object_requests: AtomicUsize,
    requested_keys: Mutex<Vec<String>>,
}

impl PrefetchBackend {
    fn new(label: &str, manifest: Manifest, bodies: HashMap<String, Vec<u8>>) -> Self {
        Self {
            manifest_key: manifest_key(label),
            manifest,
            bodies,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            object_requests: AtomicUsize::new(0),
            requested_keys: Mutex::new(Vec::new()),
        }
    }

    fn requested(&self) -> Vec<String> {
        self.requested_keys.lock().unwrap().clone()
    }

    fn requests_for(&self, key: &str) -> usize {
        self.requested()
            .iter()
            .filter(|requested| *requested == key)
            .count()
    }
}

async fn spawn_prefetch_backend(
    backend: Arc<PrefetchBackend>,
) -> (reqwest::Url, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route(
            "/build-cache/http/{key}",
            route_get(
                |State(backend): State<Arc<PrefetchBackend>>, Path(key): Path<String>| async move {
                    backend.requested_keys.lock().unwrap().push(key.clone());
                    if key == backend.manifest_key {
                        return Json(backend.manifest.clone()).into_response();
                    }
                    backend.object_requests.fetch_add(1, Ordering::Relaxed);
                    let running = backend.active.fetch_add(1, Ordering::Relaxed) + 1;
                    backend.peak.fetch_max(running, Ordering::Relaxed);
                    // The pause keeps overlapping downloads simultaneously active
                    // so the peak observes real concurrency.
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    backend.active.fetch_sub(1, Ordering::Relaxed);
                    match backend.bodies.get(&key) {
                        Some(body) => (StatusCode::OK, body.clone()).into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                },
            ),
        )
        .with_state(backend);
    let (address, task) = serve(app).await;
    (
        reqwest::Url::parse(&format!("http://{address}/build-cache/http/")).unwrap(),
        task,
    )
}

fn manifest_entry_for(body: &[u8]) -> ManifestEntry {
    ManifestEntry {
        output: hex::encode(sha2::Sha256::digest(body)),
        size: body.len() as u64,
        last_seen: 1,
    }
}

#[tokio::test]
async fn prefetch_downloads_in_parallel_within_the_bound() {
    let mut manifest = Manifest::empty();
    let mut bodies = HashMap::new();
    for index in 0..8u32 {
        let body = format!("prefetched object {index}").into_bytes();
        let action = format!("{index:064x}");
        manifest
            .entries
            .insert(action.clone(), manifest_entry_for(&body));
        bodies.insert(format!("go-{action}"), body);
    }
    let backend = Arc::new(PrefetchBackend::new("bound-test", manifest, bodies.clone()));
    let (base, server) = spawn_prefetch_backend(Arc::clone(&backend)).await;
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(session::SessionState::default());

    session::run_prefetch(
        reqwest::Client::new(),
        base,
        None,
        directory.path().to_path_buf(),
        manifest_key("bound-test"),
        2,
        Arc::clone(&state),
    )
    .await;
    server.abort();

    // Discovery is exactly one plain GET of the derived manifest key.
    assert_eq!(backend.requests_for(&manifest_key("bound-test")), 1);
    assert_eq!(backend.object_requests.load(Ordering::Relaxed), 8);
    let peak = backend.peak.load(Ordering::Relaxed);
    assert_eq!(peak, 2, "downloads must fill but never exceed the bound");
    for body in bodies.values() {
        let path = directory
            .path()
            .join(hex::encode(sha2::Sha256::digest(body)));
        assert_eq!(&tokio::fs::read(&path).await.unwrap(), body);
    }
    assert!(state.manifest.get().is_some());
}

#[tokio::test]
async fn prefetch_publishes_only_verified_objects_and_tolerates_misses() {
    let good = b"good object body".to_vec();
    let advertised = b"advertised bytes".to_vec();
    let tampered = b"tampered bytes!!".to_vec(); // same length: only the digest differs
    let absent = b"never stored".to_vec();
    let mut manifest = Manifest::empty();
    let mut bodies = HashMap::new();
    let good_action = format!("{:064x}", 1);
    manifest
        .entries
        .insert(good_action.clone(), manifest_entry_for(&good));
    bodies.insert(format!("go-{good_action}"), good.clone());
    let corrupt_action = format!("{:064x}", 2);
    manifest
        .entries
        .insert(corrupt_action.clone(), manifest_entry_for(&advertised));
    bodies.insert(format!("go-{corrupt_action}"), tampered);
    let absent_action = format!("{:064x}", 3);
    manifest
        .entries
        .insert(absent_action.clone(), manifest_entry_for(&absent));
    let backend = Arc::new(PrefetchBackend::new("verify-test", manifest, bodies));
    let (base, server) = spawn_prefetch_backend(Arc::clone(&backend)).await;
    let directory = tempfile::tempdir().unwrap();

    session::run_prefetch(
        reqwest::Client::new(),
        base,
        None,
        directory.path().to_path_buf(),
        manifest_key("verify-test"),
        4,
        Arc::new(session::SessionState::default()),
    )
    .await;
    server.abort();

    // Only the verified body was published; the corrupted download and the miss
    // stay future foreground misses, and no temporary file is left behind.
    let mut published: Vec<String> = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    published.sort();
    assert_eq!(
        published,
        [hex::encode(sha2::Sha256::digest(&good))],
        "exactly the verified object must be published"
    );
}

/// Concurrency 0 disables the download pool, not the bootstrap: the session
/// issues exactly one request — the manifest GET — and nothing else.
#[tokio::test]
async fn prefetch_concurrency_zero_fetches_only_the_manifest() {
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let manifest_requested = Arc::new(Notify::new());
    let app = Router::new().fallback({
        let requests = Arc::clone(&requests);
        let manifest_requested = Arc::clone(&manifest_requested);
        move |request: axum::extract::Request| {
            let requests = Arc::clone(&requests);
            let manifest_requested = Arc::clone(&manifest_requested);
            async move {
                requests
                    .lock()
                    .unwrap()
                    .push(request.uri().path().to_owned());
                manifest_requested.notify_one();
                StatusCode::NOT_FOUND
            }
        }
    });
    let (address, server) = serve(app).await;
    let cache_dir = tempfile::tempdir().unwrap();
    let (mut input, cache_input) = tokio::io::duplex(4096);
    let (cache_output, output) = tokio::io::duplex(4096);
    let cache = tokio::spawn(run_with_io(
        args(
            format!("http://{address}/build-cache/http/"),
            cache_dir.path(),
        ),
        BufReader::new(cache_input),
        cache_output,
    ));
    // Close only after the bootstrap GET has landed, so the assertion below
    // observes the session's complete request set deterministically.
    manifest_requested.notified().await;
    input
        .write_all(b"{\"ID\":1,\"Command\":\"close\"}\n")
        .await
        .unwrap();
    let mut output = BufReader::new(output);
    let mut line = String::new();
    output.read_line(&mut line).await.unwrap();
    line.clear();
    output.read_line(&mut line).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["ID"],
        1
    );
    drop(input);
    cache.await.unwrap().unwrap();
    server.abort();

    assert_eq!(
        requests.lock().unwrap().as_slice(),
        [format!(
            "/build-cache/http/{}",
            manifest_key("cacheprog-test")
        )]
    );
}

/// A foreground get for an output whose download is still queued behind the
/// saturated pool must also wait: the in-flight marker is registered when the
/// download is enqueued, not when it reaches the semaphore.
#[tokio::test]
async fn foreground_get_waits_for_a_queued_prefetch_download() {
    let bodies_by_action: HashMap<String, Vec<u8>> = HashMap::from([
        (format!("{:064x}", 1), b"first contended body".to_vec()),
        (format!("{:064x}", 2), b"second contended body".to_vec()),
    ]);
    let mut manifest = Manifest::empty();
    for (action, body) in &bodies_by_action {
        manifest
            .entries
            .insert(action.clone(), manifest_entry_for(body));
    }
    let requested = Arc::new(Mutex::new(Vec::<String>::new()));
    let download_started = Arc::new(Notify::new());
    let release_download = Arc::new(Notify::new());
    let object_requests = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/build-cache/http/{key}",
        route_get({
            let manifest = manifest.clone();
            let bodies_by_action = bodies_by_action.clone();
            let requested = Arc::clone(&requested);
            let download_started = Arc::clone(&download_started);
            let release_download = Arc::clone(&release_download);
            let object_requests = Arc::clone(&object_requests);
            move |Path(key): Path<String>| {
                let manifest = manifest.clone();
                let bodies_by_action = bodies_by_action.clone();
                let requested = Arc::clone(&requested);
                let download_started = Arc::clone(&download_started);
                let release_download = Arc::clone(&release_download);
                let object_requests = Arc::clone(&object_requests);
                async move {
                    if key == manifest_key("queued-test") {
                        return Json(manifest).into_response();
                    }
                    requested.lock().unwrap().push(key.clone());
                    // Only the first object request blocks; with concurrency 1 it
                    // pins the pool while the other download waits in the queue.
                    if object_requests.fetch_add(1, Ordering::Relaxed) == 0 {
                        download_started.notify_one();
                        release_download.notified().await;
                    }
                    let action = key.strip_prefix("go-").unwrap_or(&key);
                    match bodies_by_action.get(action) {
                        Some(body) => body.clone().into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
            }
        }),
    );
    let (address, server) = serve(app).await;
    let base = reqwest::Url::parse(&format!("http://{address}/build-cache/http/")).unwrap();
    let client = reqwest::Client::new();
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(session::SessionState::default());

    let prefetch = tokio::spawn(session::run_prefetch(
        client.clone(),
        base.clone(),
        None,
        directory.path().to_path_buf(),
        manifest_key("queued-test"),
        1,
        Arc::clone(&state),
    ));
    // One download is blocked inside the server; the other sits queued behind
    // the exhausted semaphore, so its output exists only as an enqueue-time
    // marker.
    download_started.notified().await;
    let blocked_key = requested.lock().unwrap()[0].clone();
    let queued_action = bodies_by_action
        .keys()
        .find(|action| format!("go-{action}") != blocked_key)
        .unwrap()
        .clone();
    let queued_body = bodies_by_action[&queued_action].clone();

    let released = Arc::new(AtomicBool::new(false));
    let releaser = tokio::spawn({
        let release_download = Arc::clone(&release_download);
        let released = Arc::clone(&released);
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            released.store(true, Ordering::SeqCst);
            release_download.notify_one();
        }
    });
    let response = super::get(
        &client,
        &base,
        None,
        directory.path(),
        &state,
        Request {
            id: 5,
            command: "get".into(),
            action_id: hex::decode(&queued_action).unwrap(),
            output_id: Vec::new(),
            body_size: 0,
        },
    )
    .await
    .unwrap();
    assert!(
        released.load(Ordering::SeqCst),
        "the get must wait on the queued download's marker, not fetch on its own"
    );
    prefetch.await.unwrap();
    releaser.await.unwrap();
    server.abort();

    assert!(!response.miss);
    assert_eq!(
        hex::encode(&response.output_id),
        hex::encode(sha2::Sha256::digest(&queued_body))
    );
    assert_eq!(
        requested
            .lock()
            .unwrap()
            .iter()
            .filter(|key| **key == format!("go-{queued_action}"))
            .count(),
        1,
        "the queued output must cross the network exactly once"
    );
}

/// Zero-size actions all resolve to the one empty output: prefetch synthesizes
/// the empty file locally instead of downloading it, and foreground gets for
/// those actions answer locally. A zero-size entry under any other digest can
/// never verify and is dropped from the work list.
#[tokio::test]
async fn zero_size_actions_are_elided_and_answered_locally() {
    let empty_output = hex::encode(sha2::Sha256::digest([]));
    let nonzero = b"nonzero body".to_vec();
    let mut manifest = Manifest::empty();
    let zero_actions = [
        format!("{:064x}", 1),
        format!("{:064x}", 2),
        format!("{:064x}", 3),
    ];
    for action in &zero_actions {
        manifest.entries.insert(
            action.clone(),
            ManifestEntry {
                output: empty_output.clone(),
                size: 0,
                last_seen: 1,
            },
        );
    }
    let nonzero_action = format!("{:064x}", 7);
    manifest
        .entries
        .insert(nonzero_action.clone(), manifest_entry_for(&nonzero));
    let liar_action = format!("{:064x}", 8);
    let liar_output = "ff".repeat(32);
    manifest.entries.insert(
        liar_action.clone(),
        ManifestEntry {
            output: liar_output.clone(),
            size: 0,
            last_seen: 1,
        },
    );
    let bodies = HashMap::from([(format!("go-{nonzero_action}"), nonzero.clone())]);
    let backend = Arc::new(PrefetchBackend::new("zero-test", manifest, bodies));
    let (base, server) = spawn_prefetch_backend(Arc::clone(&backend)).await;
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(session::SessionState::default());
    let client = reqwest::Client::new();

    session::run_prefetch(
        client.clone(),
        base.clone(),
        None,
        directory.path().to_path_buf(),
        manifest_key("zero-test"),
        2,
        Arc::clone(&state),
    )
    .await;

    // Only the manifest and the one nonzero output crossed the network.
    assert_eq!(backend.requests_for(&manifest_key("zero-test")), 1);
    assert_eq!(backend.object_requests.load(Ordering::Relaxed), 1);
    assert_eq!(backend.requests_for(&format!("go-{nonzero_action}")), 1);
    let empty_path = directory.path().join(&empty_output);
    assert_eq!(
        tokio::fs::read(&empty_path).await.unwrap(),
        Vec::<u8>::new()
    );
    assert!(!directory.path().join(&liar_output).exists());

    // A foreground get for a zero-size action answers locally with no request.
    let requests_before = backend.requested().len();
    let response = super::get(
        &client,
        &base,
        None,
        directory.path(),
        &state,
        Request {
            id: 11,
            command: "get".into(),
            action_id: hex::decode(&zero_actions[0]).unwrap(),
            output_id: Vec::new(),
            body_size: 0,
        },
    )
    .await
    .unwrap();
    server.abort();

    assert!(!response.miss);
    assert_eq!(hex::encode(&response.output_id), empty_output);
    assert_eq!(response.size, 0);
    assert_eq!(backend.requested().len(), requests_before);
}

/// Many actions mapping to few distinct outputs cost one manifest GET plus one
/// object GET per distinct nonzero output — never one request per action.
#[tokio::test]
async fn prefetch_requests_are_bounded_by_distinct_outputs() {
    let outputs = [
        b"body one".to_vec(),
        b"body two".to_vec(),
        b"body three".to_vec(),
    ];
    let empty_output = hex::encode(sha2::Sha256::digest([]));
    let mut manifest = Manifest::empty();
    let mut bodies = HashMap::new();
    for index in 0..24u32 {
        let action = format!("{index:064x}");
        let body = &outputs[(index % 3) as usize];
        manifest
            .entries
            .insert(action.clone(), manifest_entry_for(body));
        bodies.insert(format!("go-{action}"), body.clone());
    }
    for index in 24..32u32 {
        let action = format!("{index:064x}");
        manifest.entries.insert(
            action,
            ManifestEntry {
                output: empty_output.clone(),
                size: 0,
                last_seen: 1,
            },
        );
    }
    let backend = Arc::new(PrefetchBackend::new("ceiling-test", manifest, bodies));
    let (base, server) = spawn_prefetch_backend(Arc::clone(&backend)).await;
    let directory = tempfile::tempdir().unwrap();

    session::run_prefetch(
        reqwest::Client::new(),
        base,
        None,
        directory.path().to_path_buf(),
        manifest_key("ceiling-test"),
        4,
        Arc::new(session::SessionState::default()),
    )
    .await;
    server.abort();

    let requested = backend.requested();
    assert_eq!(
        requested.len(),
        1 + outputs.len(),
        "one manifest GET plus one GET per distinct nonzero output: {requested:?}"
    );
    for key in &requested {
        assert_eq!(
            backend.requests_for(key),
            1,
            "no key is ever requested twice: {requested:?}"
        );
    }
    for body in &outputs {
        let path = directory
            .path()
            .join(hex::encode(sha2::Sha256::digest(body)));
        assert_eq!(&tokio::fs::read(&path).await.unwrap(), body);
    }
}
