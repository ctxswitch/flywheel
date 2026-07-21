#[cfg(test)]
mod cacheprog_test;

use crate::cli::CacheprogArgs;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    future::Future,
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
    run_with_shutdown(args, reader, writer, async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::debug!(%error, "shutdown signal failed");
        }
        tracing::debug!(component = "cacheprog", "shutdown requested");
    })
    .await
}

/// The protocol loop over arbitrary IO: `run` binds it to stdio; integration
/// tests drive it through in-memory pipes.
pub async fn run_with_io<R, W>(args: CacheprogArgs, reader: R, mut writer: W) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    run_with_shutdown(args, reader, &mut writer, std::future::pending()).await
}

async fn run_with_shutdown<R, W, S>(
    args: CacheprogArgs,
    reader: R,
    mut writer: W,
    shutdown: S,
) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    S: Future<Output = ()>,
{
    let base = reqwest::Url::parse(&args.url)?;
    anyhow::ensure!(
        base.path().ends_with('/'),
        "cacheprog URL must end with '/'"
    );
    let directory = cache_directory(args.cache_dir.as_deref(), args.ephemeral_cache).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    let session_state = Arc::new(SessionState::default());
    let label = session::session_label(
        args.session.as_deref(),
        &std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    );
    let manifest_key = crate::manifest::manifest_key(&label);
    // Always fetch the manifest — one GET that powers zero-elision and local
    // answering. Concurrency 0 disables only the parallel object downloads.
    let mut prefetch_task = Some(tokio::spawn(session::run_prefetch(
        client.clone(),
        base.clone(),
        args.token.clone(),
        directory.clone(),
        manifest_key.clone(),
        args.prefetch_concurrency,
        Arc::clone(&session_state),
    )));
    let mut session_finished = false;

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
        Shutdown,
    }

    let mut reader = ProtocolReader::new(reader);
    let mut in_flight = FuturesUnordered::new();
    let mut close_id = None;
    tokio::pin!(shutdown);
    loop {
        if let Some(id) = close_id
            && in_flight.is_empty()
        {
            finish_session(
                &mut prefetch_task,
                &mut session_finished,
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
                    id,
                    ..Response::default()
                },
            )
            .await?;
            break;
        }

        let event = if close_id.is_some() {
            tokio::select! {
                biased;
                () = &mut shutdown => Event::Shutdown,
                response = in_flight.next() => Event::Response(
                    response.expect("in-flight request was checked")
                ),
            }
        } else if in_flight.is_empty() {
            tokio::select! {
                biased;
                () = &mut shutdown => Event::Shutdown,
                message = reader.next_message() => Event::Input(message?),
            }
        } else {
            tokio::select! {
                biased;
                () = &mut shutdown => Event::Shutdown,
                message = reader.next_message() => Event::Input(message?),
                response = in_flight.next() => {
                    Event::Response(response.expect("in-flight request was checked"))
                },
            }
        };

        match event {
            Event::Response(response) => write_response(&mut writer, &response).await?,
            Event::Input(None) | Event::Shutdown => {
                in_flight.clear();
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
        &mut session_finished,
        &client,
        &base,
        args.token.as_deref(),
        &manifest_key,
        &session_state,
    )
    .await;
    if args.ephemeral_cache {
        tokio::fs::remove_dir_all(&directory).await?;
    } else {
        let object_max_age =
            (args.prune_days > 0).then(|| Duration::from_secs(args.prune_days * 24 * 60 * 60));
        prune_stale_files(&directory, std::time::SystemTime::now(), object_max_age).await;
    }
    Ok(())
}

/// Ends the session exactly once: stop predicting, then persist what this build
/// used so the next one can prefetch it.
async fn finish_session(
    task: &mut Option<tokio::task::JoinHandle<()>>,
    finished: &mut bool,
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    manifest_key: &str,
    state: &SessionState,
) {
    if std::mem::replace(finished, true) {
        return;
    }
    if let Some(task) = task.take() {
        task.abort();
        let _ = task.await;
    }
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
        // If the prefetch pool is pulling this exact object, wait for it instead
        // of downloading the same bytes again: Go's own build parallelism bounds
        // the waiters, and the signal fires however the download ends, so a
        // failed one simply falls through to the ordinary request below.
        if !file_has_size(&path, entry.size).await
            && let Some(mut pending) = session.download_in_progress(&entry.output)
        {
            let _ = pending.changed().await;
        }
        if file_has_size(&path, entry.size).await {
            touch(&path);
            // A local answer is still a use: without this the manifest retains the
            // entry against its stale `last_seen`, so the best-predicted actions
            // are the first to age out and the first evicted at the entry cap.
            session.record_used(action, entry.output.clone(), entry.size);
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

/// A fresh temporary path in `parent`, named so `prune_stale_files` recognizes and
/// eventually removes abandoned ones from crashed sessions.
fn temporary_path(parent: &Path) -> PathBuf {
    parent.join(format!(".{}.{}.tmp", Ulid::new(), std::process::id()))
}

async fn write_atomic(path: &Path, body: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cache path has no parent"))?;
    let temporary = temporary_path(parent);
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

async fn cache_directory(parent: Option<&Path>, ephemeral: bool) -> anyhow::Result<PathBuf> {
    let parent = parent
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let directory = if ephemeral {
        parent.join(format!("flywheel-cacheprog-{}", Ulid::new()))
    } else {
        parent.join("flywheel-cacheprog")
    };
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
