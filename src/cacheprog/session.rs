//! Manifest-driven prefetch for the cacheprog helper.
//!
//! A session remembers which objects the previous build with the same label used
//! (the manifest, stored server-side under an ordinary build-cache key) and streams
//! all of them down in one request while the build runs. Prefetch is pure
//! prediction: every body is sha256-verified before it lands in the scratch
//! directory, and anything missing falls back to a normal get, so correctness
//! never depends on it.

use crate::prefetch::{FrameDecoder, FrameEncoding, PrefetchRequest};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

pub const MANIFEST_VERSION: u32 = 1;
pub const MANIFEST_MAX_ENTRIES: usize = 32 * 1024;
pub const MANIFEST_MAX_AGE_SECONDS: u64 = 14 * 24 * 60 * 60;
const PREFETCH_TIMEOUT: Duration = Duration::from_secs(600);
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct Manifest {
    pub version: u32,
    pub entries: HashMap<String, ManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ManifestEntry {
    pub output: String,
    pub size: u64,
    pub last_seen: u64,
}

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

pub fn manifest_key(label: &str) -> String {
    format!(
        "go-manifest-{}",
        hex::encode(Sha256::digest(label.as_bytes()))
    )
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The startup task: fetch the manifest, then stream every predicted object down in
/// one request. Any failure — endpoint missing on an older server, stream drop, bad
/// frame — logs and stops; everything simply falls back to normal gets.
pub async fn run_prefetch(
    client: reqwest::Client,
    base: reqwest::Url,
    token: Option<String>,
    directory: PathBuf,
    manifest_key: String,
    state: Arc<SessionState>,
) {
    if let Err(error) = prefetch(
        &client,
        &base,
        token.as_deref(),
        &directory,
        &manifest_key,
        &state,
    )
    .await
    {
        tracing::warn!(%error, "build-cache prefetch stopped; falling back to normal gets");
    }
}

async fn prefetch(
    client: &reqwest::Client,
    base: &reqwest::Url,
    token: Option<&str>,
    directory: &Path,
    manifest_key: &str,
    state: &SessionState,
) -> anyhow::Result<()> {
    let Some(response) = super::send_get(client, base, token, manifest_key).await? else {
        return Ok(());
    };
    let manifest: Manifest = serde_json::from_slice(&response.bytes().await?)?;
    anyhow::ensure!(
        manifest.version == MANIFEST_VERSION,
        "unknown manifest version {}",
        manifest.version
    );
    let manifest = state.manifest.get_or_init(|| manifest);

    let mut seen = HashSet::new();
    let mut digests = Vec::new();
    for entry in manifest.entries.values() {
        if !seen.insert(entry.output.as_str()) {
            continue;
        }
        let path = directory.join(&entry.output);
        if super::file_has_size(&path, entry.size).await {
            // Still predicted, so keep it inside the pruning window even if this
            // build never asks for it.
            super::touch(&path);
            continue;
        }
        digests.push(entry.output.clone());
    }
    if digests.is_empty() {
        return Ok(());
    }

    let mut request = client
        .post(base.join("../prefetch")?)
        .timeout(PREFETCH_TIMEOUT)
        .json(&PrefetchRequest { digests });
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    anyhow::ensure!(
        response.status().is_success(),
        "prefetch returned {}",
        response.status()
    );
    let stream =
        futures_util::TryStreamExt::map_err(response.bytes_stream(), std::io::Error::other);
    let mut decoder = FrameDecoder::new(StreamReader::new(stream));
    while let Some((header, body)) = decoder.next_frame().await? {
        if header.miss {
            continue;
        }
        let content = match header.encoding {
            FrameEncoding::Zstd => {
                let mut decoder =
                    async_compression::tokio::bufread::ZstdDecoder::new(body.as_slice());
                let mut content = Vec::new();
                decoder.read_to_end(&mut content).await?;
                content
            }
            FrameEncoding::Identity => body,
        };
        anyhow::ensure!(
            content.len() as u64 == header.content_len,
            "prefetched object size mismatch"
        );
        anyhow::ensure!(
            hex::encode(Sha256::digest(&content)) == header.digest,
            "prefetched object failed digest verification"
        );
        super::write_atomic(&directory.join(&header.digest), &content).await?;
    }
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
        Ok(Err(error)) => tracing::warn!(%error, "session manifest update failed"),
        Err(_) => tracing::warn!("session manifest update timed out"),
    }
}
