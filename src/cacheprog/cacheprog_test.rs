use super::{Request, Response, read_protocol_line, run_with_io};
use crate::cli::CacheprogArgs;
use axum::{Router, extract::Path, http::StatusCode, routing::get as route_get};
use sha2::Digest as _;
use std::{sync::Arc, time::Duration};
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
    let session = crate::cacheprog::session::SessionState::default();
    session
        .manifest
        .set(crate::cacheprog::session::Manifest {
            version: crate::cacheprog::session::MANIFEST_VERSION,
            entries: std::collections::HashMap::from([(
                hex::encode(action),
                crate::cacheprog::session::ManifestEntry {
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
            move |Path(key): Path<String>| {
                let get_started = Arc::clone(&get_started);
                let release_get = Arc::clone(&release_get);
                async move {
                    // The session manifest lookup must answer immediately so it
                    // never consumes the object-get rendezvous below.
                    if key.contains("go-manifest-") {
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
