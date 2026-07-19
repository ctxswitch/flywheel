use super::{Request, Response, read_protocol_line, run_with_io, session};
use crate::{
    cli::CacheprogArgs,
    manifest::{
        MANIFEST_VERSION, Manifest, ManifestEntry, REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH,
    },
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get as route_get,
};
use sha2::Digest as _;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::Notify,
};

fn args(url: String, cache_dir: &std::path::Path) -> CacheprogArgs {
    CacheprogArgs {
        url,
        token: None,
        cache_dir: Some(cache_dir.into()),
        session: Some("cacheprog-test".into()),
        prune_days: 14,
        // The protocol tests run hermetically; prefetch behavior has its own
        // tests below with explicit concurrency.
        prefetch_concurrency: 0,
    }
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
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

#[tokio::test]
async fn preserves_a_put_body_while_an_earlier_request_completes() {
    let get_started = Arc::new(Notify::new());
    let release_get = Arc::new(Notify::new());
    let app = Router::new().route(
        "/{*key}",
        route_get({
            let get_started = Arc::clone(&get_started);
            let release_get = Arc::clone(&release_get);
            move |Path(_key): Path<String>| {
                let get_started = Arc::clone(&get_started);
                let release_get = Arc::clone(&release_get);
                async move {
                    get_started.notify_one();
                    release_get.notified().await;
                    StatusCode::NOT_FOUND
                }
            }
        })
        .put(|| async { StatusCode::OK }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
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

/// A fake agent for prefetch tests: serves the manifest on `/status` and objects
/// on the build-cache route while recording concurrency and header discipline.
struct PrefetchBackend {
    manifest: Manifest,
    bodies: HashMap<String, Vec<u8>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    object_requests: AtomicUsize,
    unmarked_requests: AtomicUsize,
    sessions: Mutex<Vec<String>>,
}

impl PrefetchBackend {
    fn new(manifest: Manifest, bodies: HashMap<String, Vec<u8>>) -> Self {
        Self {
            manifest,
            bodies,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            object_requests: AtomicUsize::new(0),
            unmarked_requests: AtomicUsize::new(0),
            sessions: Mutex::new(Vec::new()),
        }
    }
}

async fn spawn_prefetch_backend(
    backend: Arc<PrefetchBackend>,
) -> (reqwest::Url, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route(
            "/status",
            route_get(
                |State(backend): State<Arc<PrefetchBackend>>,
                 Query(query): Query<HashMap<String, String>>| async move {
                    if let Some(session) = query.get("session") {
                        backend.sessions.lock().unwrap().push(session.clone());
                    }
                    Json(backend.manifest.clone())
                },
            ),
        )
        .route(
            "/build-cache/http/{key}",
            route_get(
                |State(backend): State<Arc<PrefetchBackend>>,
                 Path(key): Path<String>,
                 headers: HeaderMap| async move {
                    backend.object_requests.fetch_add(1, Ordering::Relaxed);
                    let marked = headers.get(REQUEST_PURPOSE_HEADER).is_some_and(|value| {
                        value.as_bytes() == REQUEST_PURPOSE_PREFETCH.as_bytes()
                    });
                    if !marked {
                        backend.unmarked_requests.fetch_add(1, Ordering::Relaxed);
                    }
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
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
    let backend = Arc::new(PrefetchBackend::new(manifest, bodies.clone()));
    let (base, server) = spawn_prefetch_backend(Arc::clone(&backend)).await;
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(session::SessionState::default());

    session::run_prefetch(
        reqwest::Client::new(),
        base,
        None,
        directory.path().to_path_buf(),
        "bound-test".into(),
        2,
        Arc::clone(&state),
    )
    .await;
    server.abort();

    // Discovery went through the status route with the raw session label, and
    // every object GET carried the telemetry purpose header.
    assert_eq!(backend.sessions.lock().unwrap().as_slice(), ["bound-test"]);
    assert_eq!(backend.object_requests.load(Ordering::Relaxed), 8);
    assert_eq!(backend.unmarked_requests.load(Ordering::Relaxed), 0);
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
    let backend = Arc::new(PrefetchBackend::new(manifest, bodies));
    let (base, server) = spawn_prefetch_backend(Arc::clone(&backend)).await;
    let directory = tempfile::tempdir().unwrap();

    session::run_prefetch(
        reqwest::Client::new(),
        base,
        None,
        directory.path().to_path_buf(),
        "verify-test".into(),
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

#[tokio::test]
async fn prefetch_concurrency_zero_sends_nothing() {
    let requests = Arc::new(AtomicUsize::new(0));
    let app = Router::new().fallback({
        let requests = Arc::clone(&requests);
        move || {
            let requests = Arc::clone(&requests);
            async move {
                requests.fetch_add(1, Ordering::Relaxed);
                StatusCode::NOT_FOUND
            }
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cache_dir = tempfile::tempdir().unwrap();
    let input = b"{\"ID\":1,\"Command\":\"close\"}\n";
    let mut output = Vec::new();

    run_with_io(
        args(
            format!("http://{address}/build-cache/http/"),
            cache_dir.path(),
        ),
        BufReader::new(&input[..]),
        &mut output,
    )
    .await
    .unwrap();
    server.abort();

    assert_eq!(requests.load(Ordering::Relaxed), 0);
}

/// A foreground get for an object the prefetch pool is mid-download must wait
/// for that download and answer locally — one fetch total, not two.
#[tokio::test]
async fn foreground_get_waits_for_the_inflight_prefetch_download() {
    let body = b"contended object body".to_vec();
    let action = format!("{:064x}", 9);
    let mut manifest = Manifest::empty();
    manifest
        .entries
        .insert(action.clone(), manifest_entry_for(&body));
    let object_requests = Arc::new(AtomicUsize::new(0));
    let download_started = Arc::new(Notify::new());
    let release_download = Arc::new(Notify::new());
    let app = Router::new()
        .route(
            "/status",
            route_get({
                let manifest = manifest.clone();
                move || {
                    let manifest = manifest.clone();
                    async move { Json(manifest) }
                }
            }),
        )
        .route(
            "/build-cache/http/{key}",
            route_get({
                let object_requests = Arc::clone(&object_requests);
                let download_started = Arc::clone(&download_started);
                let release_download = Arc::clone(&release_download);
                let body = body.clone();
                move |Path(_key): Path<String>| {
                    let object_requests = Arc::clone(&object_requests);
                    let download_started = Arc::clone(&download_started);
                    let release_download = Arc::clone(&release_download);
                    let body = body.clone();
                    async move {
                        object_requests.fetch_add(1, Ordering::Relaxed);
                        download_started.notify_one();
                        release_download.notified().await;
                        body
                    }
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = reqwest::Url::parse(&format!("http://{address}/build-cache/http/")).unwrap();
    let client = reqwest::Client::new();
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(session::SessionState::default());

    let prefetch = tokio::spawn(session::run_prefetch(
        client.clone(),
        base.clone(),
        None,
        directory.path().to_path_buf(),
        "wait-test".into(),
        1,
        Arc::clone(&state),
    ));
    // The prefetch download is now in flight, blocked inside the server handler.
    download_started.notified().await;

    let releaser = tokio::spawn({
        let release_download = Arc::clone(&release_download);
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
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
            id: 4,
            command: "get".into(),
            action_id: hex::decode(&action).unwrap(),
            output_id: Vec::new(),
            body_size: 0,
        },
    )
    .await
    .unwrap();
    prefetch.await.unwrap();
    releaser.await.unwrap();
    server.abort();

    assert!(!response.miss);
    assert_eq!(
        hex::encode(&response.output_id),
        hex::encode(sha2::Sha256::digest(&body))
    );
    assert_eq!(
        object_requests.load(Ordering::Relaxed),
        1,
        "the waiting get must reuse the prefetch download, not repeat it"
    );
}
