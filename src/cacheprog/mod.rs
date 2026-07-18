use crate::cli::CacheprogArgs;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use ulid::Ulid;

pub mod session;

use session::SessionState;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Request {
    #[serde(rename = "ID")]
    id: i64,
    command: String,
    #[serde(rename = "ActionID", default, deserialize_with = "deserialize_bytes")]
    action_id: Vec<u8>,
    #[serde(rename = "OutputID", default, deserialize_with = "deserialize_bytes")]
    output_id: Vec<u8>,
    #[serde(default)]
    body_size: u64,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Response {
    #[serde(rename = "ID")]
    id: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    err: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    known_commands: Vec<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    miss: bool,
    #[serde(
        rename = "OutputID",
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_bytes"
    )]
    output_id: Vec<u8>,
    #[serde(skip_serializing_if = "is_zero")]
    size: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    disk_path: String,
}

enum ProtocolMessage {
    Request {
        request: Request,
        encoded_body: Vec<u8>,
    },
    Invalid(String),
}

struct ProtocolReader<R> {
    reader: R,
    line: Vec<u8>,
    pending_put: Option<Request>,
}

impl<R> ProtocolReader<R>
where
    R: AsyncBufRead + Unpin,
{
    fn new(reader: R) -> Self {
        Self {
            reader,
            line: Vec::new(),
            pending_put: None,
        }
    }

    async fn next_message(&mut self) -> anyhow::Result<Option<ProtocolMessage>> {
        loop {
            if self.pending_put.is_some() {
                anyhow::ensure!(
                    read_protocol_line(&mut self.reader, &mut self.line).await?,
                    "cacheprog put body is missing"
                );
                let request = self.pending_put.take().expect("pending put was checked");
                return Ok(Some(ProtocolMessage::Request {
                    request,
                    encoded_body: std::mem::take(&mut self.line),
                }));
            }

            if !read_protocol_line(&mut self.reader, &mut self.line).await? {
                return Ok(None);
            }
            let request: Request = match serde_json::from_slice(&self.line) {
                Ok(request) => request,
                Err(error) => {
                    self.line.clear();
                    return Ok(Some(ProtocolMessage::Invalid(error.to_string())));
                }
            };
            self.line.clear();
            if request.command == "put" && request.body_size > 0 {
                self.pending_put = Some(request);
                continue;
            }
            return Ok(Some(ProtocolMessage::Request {
                request,
                encoded_body: Vec::new(),
            }));
        }
    }
}

pub async fn run(args: CacheprogArgs) -> anyhow::Result<()> {
    let reader = BufReader::new(tokio::io::stdin());
    let writer = tokio::io::stdout();
    run_with_io(args, reader, writer).await
}

async fn run_with_io<R, W>(args: CacheprogArgs, reader: R, mut writer: W) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let base = reqwest::Url::parse(&args.url)?;
    anyhow::ensure!(
        base.path().ends_with('/'),
        "cacheprog URL must end with '/'"
    );
    let directory = cache_directory(args.cache_dir.as_deref()).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    let session_state = Arc::new(SessionState::default());
    let label = session::session_label(
        args.session.as_deref(),
        &std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    );
    let manifest_key = session::manifest_key(&label);
    let mut prefetch_task = Some(tokio::spawn(session::run_prefetch(
        client.clone(),
        base.clone(),
        args.token.clone(),
        directory.clone(),
        manifest_key.clone(),
        Arc::clone(&session_state),
    )));

    write_response(
        &mut writer,
        &Response {
            known_commands: vec!["get".into(), "put".into(), "close".into()],
            ..Response::default()
        },
    )
    .await?;

    enum Event {
        Input(Option<ProtocolMessage>),
        Response(Response),
    }

    let mut reader = ProtocolReader::new(reader);
    let mut in_flight = FuturesUnordered::new();
    let mut close_id = None;
    loop {
        if close_id.is_some() && in_flight.is_empty() {
            finish_session(
                &mut prefetch_task,
                &client,
                &base,
                args.token.as_deref(),
                &manifest_key,
                &session_state,
            )
            .await;
            write_response(
                &mut writer,
                &Response {
                    id: close_id.expect("close ID was checked"),
                    ..Response::default()
                },
            )
            .await?;
            break;
        }

        let event = if close_id.is_some() {
            Event::Response(
                in_flight
                    .next()
                    .await
                    .expect("in-flight request was checked"),
            )
        } else if in_flight.is_empty() {
            Event::Input(reader.next_message().await?)
        } else {
            tokio::select! {
                message = reader.next_message() => Event::Input(message?),
                response = in_flight.next() => {
                    Event::Response(response.expect("in-flight request was checked"))
                }
            }
        };

        match event {
            Event::Response(response) => write_response(&mut writer, &response).await?,
            Event::Input(None) => {
                while let Some(response) = in_flight.next().await {
                    write_response(&mut writer, &response).await?;
                }
                break;
            }
            Event::Input(Some(ProtocolMessage::Invalid(error))) => {
                write_response(
                    &mut writer,
                    &Response {
                        err: error,
                        ..Response::default()
                    },
                )
                .await?;
            }
            Event::Input(Some(ProtocolMessage::Request {
                request,
                encoded_body,
            })) => {
                if request.command == "close" {
                    close_id = Some(request.id);
                } else {
                    in_flight.push(execute_request(
                        &client,
                        &base,
                        args.token.as_deref(),
                        &directory,
                        &session_state,
                        request,
                        encoded_body,
                    ));
                }
            }
        }
    }
    finish_session(
        &mut prefetch_task,
        &client,
        &base,
        args.token.as_deref(),
        &manifest_key,
        &session_state,
    )
    .await;
    let object_max_age =
        (args.prune_days > 0).then(|| Duration::from_secs(args.prune_days * 24 * 60 * 60));
    prune_stale_files(&directory, std::time::SystemTime::now(), object_max_age).await;
    Ok(())
}

/// Ends the session exactly once: stop predicting, then persist what this build
/// used so the next one can prefetch it.
async fn finish_session(
    task: &mut Option<tokio::task::JoinHandle<()>>,
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    manifest_key: &str,
    state: &SessionState,
) {
    let Some(task) = task.take() else { return };
    task.abort();
    let _ = task.await;
    session::finalize(client, base, token, manifest_key, state).await;
}

async fn read_protocol_line(
    reader: &mut (impl AsyncBufRead + Unpin),
    line: &mut Vec<u8>,
) -> anyhow::Result<bool> {
    loop {
        // `run_with_io` may cancel this future when an in-flight response completes.
        // `read_until` preserves bytes appended before cancellation, so resume by
        // appending to `line` rather than clearing the partial protocol message.
        let existing = line.len();
        let read = reader.read_until(b'\n', line).await?;
        if read == 0 && existing == 0 {
            return Ok(false);
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            return Ok(true);
        }
        line.clear();
    }
}

async fn execute_request(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    directory: &Path,
    session: &SessionState,
    request: Request,
    encoded_body: Vec<u8>,
) -> Response {
    let id = request.id;
    let response = match request.command.as_str() {
        "put" => {
            put(
                client,
                base,
                token,
                directory,
                session,
                request,
                &encoded_body,
            )
            .await
        }
        "get" => get(client, base, token, directory, session, request).await,
        command => Err(anyhow::anyhow!("unsupported command {command}")),
    };
    response.unwrap_or_else(|error| Response {
        id,
        err: error.to_string(),
        ..Response::default()
    })
}

async fn put(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    directory: &Path,
    session: &SessionState,
    request: Request,
    encoded_body: &[u8],
) -> anyhow::Result<Response> {
    let body: String = if request.body_size == 0 {
        String::new()
    } else {
        serde_json::from_slice(encoded_body)?
    };
    let body = STANDARD.decode(body)?;
    anyhow::ensure!(
        body.len() as u64 == request.body_size,
        "cacheprog body size mismatch"
    );
    anyhow::ensure!(
        Sha256::digest(&body).as_slice() == request.output_id,
        "cacheprog output digest mismatch"
    );
    let output = hex::encode(&request.output_id);
    let disk_path = write_disk_file(directory, &output, &body).await?;
    let action = hex::encode(&request.action_id);
    // One upload per object: the body lives directly under the per-action key, and
    // server-side content addressing preserves dedup across actions.
    send_put(client, base, token, &format!("go-{action}"), body).await?;
    session.record_used(action, output, request.body_size);
    Ok(Response {
        id: request.id,
        disk_path,
        ..Response::default()
    })
}

async fn get(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    directory: &Path,
    session: &SessionState,
    request: Request,
) -> anyhow::Result<Response> {
    let action = hex::encode(&request.action_id);
    // A manifest-known action whose verified body already sits in the local cache
    // answers with zero requests. The file only exists if something verified it.
    if let Some(entry) = session.manifest_entry(&action) {
        let path = directory.join(&entry.output);
        if file_has_size(&path, entry.size).await {
            touch(&path);
            return Ok(Response {
                id: request.id,
                output_id: hex::decode(&entry.output)?,
                size: entry.size,
                disk_path: canonical_path(&path).await?,
                ..Response::default()
            });
        }
    }
    let Some(response) = send_get(client, base, token, &format!("go-{action}")).await? else {
        return Ok(Response {
            id: request.id,
            miss: true,
            ..Response::default()
        });
    };
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.bytes().await?.to_vec();
    // The put-side check guarantees the stored body's hash IS the output ID.
    let output = hex::encode(Sha256::digest(&body));
    if let Some(expected) = etag
        .as_deref()
        .and_then(|etag| etag.trim_matches('"').strip_prefix("sha256:"))
        && expected != output
    {
        // Corruption tripwire: the server's ETag carries the content digest.
        return Ok(Response {
            id: request.id,
            miss: true,
            ..Response::default()
        });
    }
    let disk_path = write_disk_file(directory, &output, &body).await?;
    let size = body.len() as u64;
    session.record_used(action, output.clone(), size);
    Ok(Response {
        id: request.id,
        output_id: hex::decode(output)?,
        size,
        disk_path,
        ..Response::default()
    })
}

async fn send_put(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    key: &str,
    body: Vec<u8>,
) -> anyhow::Result<()> {
    let mut request = client.put(base.join(key)?).body(body);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    anyhow::ensure!(
        response.status().is_success(),
        "cache PUT returned {}",
        response.status()
    );
    Ok(())
}

async fn send_get(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    key: &str,
) -> anyhow::Result<Option<reqwest::Response>> {
    let mut request = client.get(base.join(key)?);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    anyhow::ensure!(
        response.status().is_success(),
        "cache GET returned {}",
        response.status()
    );
    Ok(Some(response))
}

async fn write_disk_file(directory: &Path, name: &str, body: &[u8]) -> anyhow::Result<String> {
    let path = directory.join(name);
    if file_has_size(&path, body.len() as u64).await {
        touch(&path);
        return canonical_path(&path).await;
    }
    write_atomic(&path, body).await?;
    canonical_path(&path).await
}

async fn write_atomic(path: &Path, body: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cache path has no parent"))?;
    let temporary = parent.join(format!(".{}.{}.tmp", Ulid::new(), std::process::id()));
    let mut file = tokio::fs::File::create(&temporary).await?;
    if let Err(error) = file.write_all(body).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    drop(file);
    if let Err(error) = tokio::fs::rename(&temporary, path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        if !tokio::fs::try_exists(path).await? {
            return Err(error.into());
        }
    }
    Ok(())
}

async fn file_has_size(path: &Path, size: u64) -> bool {
    tokio::fs::metadata(path)
        .await
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() == size)
}

async fn canonical_path(path: &Path) -> anyhow::Result<String> {
    Ok(tokio::fs::canonicalize(path)
        .await?
        .to_string_lossy()
        .into_owned())
}

/// The local object cache is a stable directory that outlives the session: point
/// `--cache-dir` at a reusable pod volume or a host directory and consecutive
/// builds answer straight from disk. Every file in it is content-named and only
/// ever appears through a verified atomic rename, so reuse needs no re-hashing.
async fn cache_directory(parent: Option<&Path>) -> anyhow::Result<PathBuf> {
    let parent = parent
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let directory = parent.join("flywheel-cacheprog");
    tokio::fs::create_dir_all(&directory).await?;
    Ok(directory)
}

const TEMPORARY_MAX_AGE: Duration = Duration::from_secs(60 * 60);

/// Best-effort cleanup at close. `object_max_age` is operator policy (`--prune-days`;
/// `None` keeps objects forever for deployments whose volume lifecycle already
/// bounds growth). Abandoned temp files from crashed sessions are always removed —
/// they are garbage in every deployment.
async fn prune_stale_files(
    directory: &Path,
    now: std::time::SystemTime,
    object_max_age: Option<Duration>,
) {
    let Ok(mut entries) = tokio::fs::read_dir(directory).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let max_age = if name.starts_with('.') && name.ends_with(".tmp") {
            Some(TEMPORARY_MAX_AGE)
        } else {
            object_max_age
        };
        let Some(max_age) = max_age else {
            continue;
        };
        let stale = entry.metadata().await.is_ok_and(|metadata| {
            metadata.is_file()
                && metadata
                    .modified()
                    .ok()
                    .and_then(|modified| now.duration_since(modified).ok())
                    .is_some_and(|age| age > max_age)
        });
        if stale {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

/// Marks a reused object file as live so pruning keeps it.
fn touch(path: &Path) {
    if let Ok(file) = std::fs::File::options().write(true).open(path) {
        let _ = file.set_modified(std::time::SystemTime::now());
    }
}

async fn write_response(
    writer: &mut (impl AsyncWrite + Unpin),
    response: &Response,
) -> anyhow::Result<()> {
    writer.write_all(&serde_json::to_vec(response)?).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

fn deserialize_bytes<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<u8>, D::Error> {
    let value = String::deserialize(deserializer)?;
    STANDARD.decode(value).map_err(serde::de::Error::custom)
}

fn serialize_bytes<S: serde::Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&STANDARD.encode(bytes))
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
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
}
