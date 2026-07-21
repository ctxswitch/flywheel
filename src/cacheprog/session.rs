//! Manifest-driven prefetch for the cacheprog helper.
//!
//! A session remembers which objects the previous build with the same label used
//! (the manifest, stored server-side under an ordinary build-cache key). At startup
//! the helper GETs that manifest key — plain cache traffic that any route owner
//! answers — and warms the local object directory with bounded parallel cache GETs
//! — exactly the requests a foreground miss would issue — while the build runs.
//! Prefetch is pure prediction: every body is verified against the manifest before
//! it lands in the scratch directory, and anything missing falls back to a normal
//! get, so correctness never depends on it.

use crate::manifest::{
    MANIFEST_MAX_AGE_SECONDS, MANIFEST_MAX_ENTRIES, MANIFEST_VERSION, Manifest, ManifestEntry,
    REQUEST_PREFETCH_CONCURRENCY_HEADER, REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH,
    REQUEST_SESSION_HEADER,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{io::AsyncWriteExt, sync::watch};

const FINALIZE_TIMEOUT: Duration = Duration::from_secs(15);

/// What this build actually touched: action hex → (output hex, logical size).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsedEntry {
    pub output: String,
    pub size: u64,
}

/// The only state shared between the protocol loop and the prefetch task.
#[derive(Default)]
pub struct SessionState {
    pub manifest: OnceLock<Manifest>,
    pub used: Mutex<HashMap<String, UsedEntry>>,
    /// Outputs the prefetch pool is downloading right now. A foreground get for
    /// one of these waits for the in-flight download instead of fetching the
    /// same bytes a second time.
    inflight: Mutex<HashMap<String, watch::Receiver<()>>>,
}

impl SessionState {
    pub fn record_used(&self, action: String, output: String, size: u64) {
        self.used
            .lock()
            .expect("used map lock")
            .insert(action, UsedEntry { output, size });
    }

    /// Answers a get locally when the manifest knows the action and a verified
    /// scratch file of the right size already exists. The file only exists if the
    /// prefetch task (or an earlier get) verified its digest before writing it.
    pub fn manifest_entry(&self, action: &str) -> Option<ManifestEntry> {
        self.manifest.get()?.entries.get(action).cloned()
    }

    /// Marks `output` as downloading for as long as the returned marker lives.
    /// Success, failure, and cancellation all end in the same drop, so waiters
    /// need no completion bookkeeping beyond its scope.
    fn begin_download(&self, output: &str) -> InflightDownload<'_> {
        let (sender, receiver) = watch::channel(());
        self.inflight
            .lock()
            .expect("inflight map lock")
            .insert(output.to_owned(), receiver);
        InflightDownload {
            session: self,
            output: output.to_owned(),
            _sender: sender,
        }
    }

    /// The completion signal of an in-flight download of `output`, if any. The
    /// signal resolves when the download ends, however it ends; the caller
    /// rechecks the disk to learn whether it succeeded.
    pub fn download_in_progress(&self, output: &str) -> Option<watch::Receiver<()>> {
        self.inflight
            .lock()
            .expect("inflight map lock")
            .get(output)
            .cloned()
    }
}

/// One download the pool currently has in flight: dropping it removes the map
/// entry and then drops the sender, which wakes every waiter.
struct InflightDownload<'a> {
    session: &'a SessionState,
    output: String,
    _sender: watch::Sender<()>,
}

impl Drop for InflightDownload<'_> {
    fn drop(&mut self) {
        self.session
            .inflight
            .lock()
            .expect("inflight map lock")
            .remove(&self.output);
    }
}

/// Merges the stored manifest with this build's usage: union so CI shards and
/// multiple targets sharing a label accumulate instead of thrashing, drop entries
/// not seen for two weeks, and cap the size by evicting the oldest.
pub fn merge_manifest(
    stored: Option<Manifest>,
    used: &HashMap<String, UsedEntry>,
    now: u64,
) -> Manifest {
    let mut entries = stored.map(|manifest| manifest.entries).unwrap_or_default();
    entries.retain(|_, entry| now.saturating_sub(entry.last_seen) <= MANIFEST_MAX_AGE_SECONDS);
    for (action, entry) in used {
        entries.insert(
            action.clone(),
            ManifestEntry {
                output: entry.output.clone(),
                size: entry.size,
                last_seen: now,
            },
        );
    }
    if entries.len() > MANIFEST_MAX_ENTRIES {
        let mut by_age: Vec<(u64, String)> = entries
            .iter()
            .map(|(action, entry)| (entry.last_seen, action.clone()))
            .collect();
        by_age.sort();
        for (_, action) in by_age
            .into_iter()
            .take(entries.len() - MANIFEST_MAX_ENTRIES)
        {
            entries.remove(&action);
        }
    }
    Manifest {
        version: MANIFEST_VERSION,
        entries,
    }
}

/// The label names the manifest so consecutive builds of the same thing find each
/// other: an explicit `--session` wins, then the Go module being built plus its
/// target platform, then the working directory.
pub fn session_label(explicit: Option<&str>, cwd: &Path) -> String {
    if let Some(label) = explicit {
        return label.to_owned();
    }
    compose_label(
        find_module_path(cwd).as_deref(),
        &go_env("GOOS", host_goos()),
        &go_env("GOARCH", host_goarch()),
        cwd,
    )
}

pub fn compose_label(module: Option<&str>, goos: &str, goarch: &str, cwd: &Path) -> String {
    match module {
        Some(module) => format!("{module} {goos}/{goarch}"),
        None => cwd.to_string_lossy().into_owned(),
    }
}

/// Extracts the module path from `go.mod` contents.
pub fn parse_module_path(go_mod: &str) -> Option<String> {
    for line in go_mod.lines() {
        let line = line.split("//").next().unwrap_or("").trim();
        if let Some(path) = line.strip_prefix("module") {
            let path = path.trim().trim_matches('"');
            if !path.is_empty() {
                return Some(path.to_owned());
            }
        }
    }
    None
}

fn find_module_path(cwd: &Path) -> Option<String> {
    for directory in cwd.ancestors() {
        if let Ok(contents) = std::fs::read_to_string(directory.join("go.mod")) {
            return parse_module_path(&contents);
        }
    }
    None
}

fn go_env(name: &str, fallback: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => fallback.to_owned(),
    }
}

fn host_goos() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

fn host_goarch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        other => other,
    }
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The hex digest naming the empty body: every zero-size action resolves to this
/// one output, so a single synthesized empty file answers all of them locally.
const EMPTY_OUTPUT: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The startup task: fetch the stored manifest by its cache key — the same plain
/// GET `finalize` issues — then warm the object directory with bounded parallel
/// cache GETs. Any failure — a dropped connection, a body that fails verification
/// — degrades to fewer warm objects; everything simply falls back to normal gets.
pub async fn run_prefetch(
    client: reqwest::Client,
    base: reqwest::Url,
    token: Option<String>,
    directory: PathBuf,
    manifest_key: String,
    concurrency: usize,
    state: Arc<SessionState>,
) {
    if let Err(error) = prefetch(
        &client,
        &base,
        token.as_deref(),
        &directory,
        &manifest_key,
        concurrency,
        &state,
    )
    .await
    {
        tracing::debug!(%error, "build-cache prefetch stopped; falling back to normal gets");
    }
}

async fn prefetch(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    directory: &Path,
    manifest_key: &str,
    concurrency: usize,
    state: &SessionState,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let manifest = match super::send_get(client, base, token, manifest_key).await? {
        Some(response) => serde_json::from_slice::<Manifest>(&response.bytes().await?)
            .ok()
            .filter(|manifest| manifest.version == MANIFEST_VERSION)
            .unwrap_or_else(Manifest::empty),
        None => Manifest::empty(),
    };
    let manifest = state.manifest.get_or_init(|| manifest);

    // One download per distinct output: several actions can share one body.
    let mut seen = HashSet::new();
    let mut wanted = Vec::new();
    let mut local = 0usize;
    for (action, entry) in &manifest.entries {
        if !seen.insert(entry.output.as_str()) {
            continue;
        }
        if entry.size == 0 {
            // Every zero-size action shares the one empty output; synthesize the
            // file locally instead of downloading nothing over the network. A
            // zero-size entry under any other digest can never verify, so it is
            // dropped from the work list entirely.
            if entry.output == EMPTY_OUTPUT {
                super::write_disk_file(directory, &entry.output, &[]).await?;
                local += 1;
            }
            continue;
        }
        let path = directory.join(&entry.output);
        if super::file_has_size(&path, entry.size).await {
            // Still predicted, so keep it inside the pruning window even if this
            // build never asks for it.
            super::touch(&path);
            local += 1;
            continue;
        }
        wanted.push((action.as_str(), entry));
    }
    let advertised = seen.len();

    if concurrency == 0 {
        // Downloads are disabled, but the installed manifest still answers
        // manifest-known gets locally and the elided empty file is in place.
        tracing::debug!(
            advertised,
            local,
            duration_ms = started.elapsed().as_millis() as u64,
            "build-cache prefetch downloads disabled; manifest installed"
        );
        return Ok(());
    }

    let limit = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut downloads: FuturesUnordered<_> = wanted
        .iter()
        .map(|&(action, entry)| {
            let limit = Arc::clone(&limit);
            // Register before the download queues behind the concurrency bound so
            // a foreground get for any wanted output — queued or actively
            // downloading — waits for the pool instead of duplicating the fetch.
            let inflight = state.begin_download(&entry.output);
            async move {
                let _inflight = inflight;
                let _permit = limit
                    .acquire_owned()
                    .await
                    .expect("prefetch semaphore is never closed");
                match download_object(
                    client,
                    base,
                    token,
                    directory,
                    manifest_key,
                    concurrency,
                    (action, entry),
                )
                .await
                {
                    Ok(published) => published,
                    Err(error) => {
                        // Every per-object failure is just a future foreground miss.
                        tracing::debug!(%error, "prefetch download skipped");
                        None
                    }
                }
            }
        })
        .collect();
    let attempted = downloads.len();
    let mut downloaded = 0usize;
    let mut bytes = 0u64;
    while let Some(published) = downloads.next().await {
        if let Some(size) = published {
            downloaded += 1;
            bytes += size;
        }
    }
    drop(downloads);
    tracing::debug!(
        advertised,
        local,
        attempted,
        downloaded,
        missed = attempted - downloaded,
        bytes,
        duration_ms = started.elapsed().as_millis() as u64,
        "build-cache prefetch complete"
    );
    Ok(())
}

/// Downloads one predicted `(action, entry)` through the ordinary cache route — the
/// exact GET a foreground miss would issue, plus the telemetry purpose header —
/// verifying size and digest against the manifest entry before publishing it into
/// the local object directory. Returns the object's size when this call published
/// it, and `None` when the server does not hold it or a foreground get won the race.
async fn download_object(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    directory: &Path,
    manifest_key: &str,
    concurrency: usize,
    (action, entry): (&str, &ManifestEntry),
) -> anyhow::Result<Option<u64>> {
    let path = directory.join(&entry.output);
    // The work list was computed at startup; a foreground get may have published
    // this object while the download waited behind the concurrency bound.
    if super::file_has_size(&path, entry.size).await {
        return Ok(None);
    }
    let mut request = client
        .get(base.join(&format!("go-{action}"))?)
        .header(REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH)
        .header(REQUEST_PREFETCH_CONCURRENCY_HEADER, concurrency)
        .header(REQUEST_SESSION_HEADER, manifest_key);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    anyhow::ensure!(
        response.status().is_success(),
        "prefetch GET returned {}",
        response.status()
    );
    let temporary = super::temporary_path(directory);
    if let Err(error) = spool_verified(response, &temporary, entry).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error);
    }
    super::publish_temporary(&temporary, &path).await?;
    Ok(Some(entry.size))
}

/// Streams the response body into the temporary file while hashing incrementally,
/// then verifies size and digest against the manifest entry. The caller removes
/// the temporary file on any error.
async fn spool_verified(
    response: reqwest::Response,
    temporary: &Path,
    entry: &ManifestEntry,
) -> anyhow::Result<()> {
    let mut file = tokio::fs::File::create(temporary).await?;
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        size += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    anyhow::ensure!(size == entry.size, "prefetched object size mismatch");
    anyhow::ensure!(
        hex::encode(hasher.finalize()) == entry.output,
        "prefetched object failed digest verification"
    );
    Ok(())
}

/// Writes the merged manifest back, best-effort under a short timeout: GET the
/// stored manifest fresh (a concurrent shard may have written since startup), merge
/// with what this build used, PUT the union.
pub async fn finalize(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    manifest_key: &str,
    state: &SessionState,
) {
    let used = std::mem::take(&mut *state.used.lock().expect("used map lock"));
    if used.is_empty() {
        return;
    }
    let update = async {
        let stored = match super::send_get(client, base, token, manifest_key).await? {
            Some(response) => serde_json::from_slice::<Manifest>(&response.bytes().await?)
                .ok()
                .filter(|manifest| manifest.version == MANIFEST_VERSION),
            None => None,
        };
        let merged = merge_manifest(stored, &used, unix_now());
        super::send_put(
            client,
            base,
            token,
            manifest_key,
            serde_json::to_vec(&merged)?,
        )
        .await
    };
    match tokio::time::timeout(FINALIZE_TIMEOUT, update).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "session manifest update failed"),
        Err(_) => tracing::debug!("session manifest update timed out"),
    }
}
