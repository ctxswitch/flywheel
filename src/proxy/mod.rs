use crate::{
    artifact::ArtifactId,
    cache::{
        Admission, CacheError, CacheService, Durability, PublicationTarget, PublishRequest,
        StoredEncoding,
    },
    channel::ChannelId,
    clock::Clock,
    config::Config,
    storage::metadata::ReferenceRecord,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{Stream, stream};
use html_escape::decode_html_entities;
use http::{HeaderMap, HeaderValue, StatusCode, header};
use lol_html::{RewriteStrSettings, element, rewrite_str};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{collections::HashSet, convert::Infallible, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, Semaphore},
};
use url::{Origin, Url};

/// Redirect follows permitted per fetch, matching the limit the reqwest policy
/// enforced before hops were validated manually.
const REDIRECT_HOP_LIMIT: usize = 5;

/// Public download origins that trusted indexes point at even when the index
/// itself lives elsewhere. PyPI's simple index links files.pythonhosted.org.
const PYTHON_DOWNLOAD_ORIGINS: &[&str] = &["https://files.pythonhosted.org"];

/// A borrowed-through upstream body: the response stream handed straight to the client
/// when a download bypasses the cache under disk pressure.
pub type PassthroughStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

#[derive(Clone, Copy, Debug)]
pub enum Protocol {
    Go,
    Python,
    Npm,
    CargoIndex,
    CargoCrate,
}

#[derive(Clone, Debug)]
pub enum Transform {
    None,
    PythonHtml {
        route_prefix: String,
        is_project_page: bool,
    },
    PythonJson {
        route_prefix: String,
        is_project_page: bool,
    },
    NpmMetadata {
        route_prefix: String,
        abbreviated: bool,
    },
}

impl Transform {
    pub fn python_simple(route_prefix: String, path: &str, accept: Option<&str>) -> Option<Self> {
        let is_project_page = !path.trim_matches('/').is_empty();
        match negotiate_python(accept)? {
            PythonRepresentation::Html => Some(Self::PythonHtml {
                route_prefix,
                is_project_page,
            }),
            PythonRepresentation::Json => Some(Self::PythonJson {
                route_prefix,
                is_project_page,
            }),
        }
    }

    pub fn npm_metadata(route_prefix: String, accept: Option<&str>) -> Option<Self> {
        negotiate_npm(accept).map(|abbreviated| Self::NpmMetadata {
            route_prefix,
            abbreviated,
        })
    }

    fn upstream_accept(&self) -> Option<&'static str> {
        match self {
            Self::PythonHtml { .. } => Some("application/vnd.pypi.simple.v1+html"),
            Self::PythonJson { .. } => Some("application/vnd.pypi.simple.v1+json"),
            Self::NpmMetadata {
                abbreviated: true, ..
            } => Some("application/vnd.npm.install-v1+json"),
            Self::NpmMetadata {
                abbreviated: false, ..
            } => Some("application/json"),
            Self::None => None,
        }
    }

    fn cache_variant(&self) -> &'static str {
        match self {
            Self::PythonHtml { .. } => "html",
            Self::PythonJson { .. } => "json",
            Self::NpmMetadata {
                abbreviated: true, ..
            } => "install-v1",
            Self::NpmMetadata {
                abbreviated: false, ..
            } => "full",
            Self::None => "",
        }
    }
}

#[derive(Clone, Copy)]
enum PythonRepresentation {
    Html,
    Json,
}

fn negotiate_python(accept: Option<&str>) -> Option<PythonRepresentation> {
    let Some(accept) = accept.filter(|value| !value.trim().is_empty()) else {
        return Some(PythonRepresentation::Html);
    };
    let mut html = None;
    let mut json = None;
    for range in accept.split(',') {
        let mut fields = range.split(';');
        let media_type = fields
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let quality = fields
            .filter_map(|field| field.trim().split_once('='))
            .find_map(|(name, value)| {
                name.trim()
                    .eq_ignore_ascii_case("q")
                    .then(|| value.trim().parse::<f32>().ok())
                    .flatten()
            })
            .unwrap_or(1.0)
            .clamp(0.0, 1.0);
        let update = |candidate: &mut Option<(u8, f32)>, specificity| {
            if candidate.is_none_or(|current| {
                specificity > current.0 || (specificity == current.0 && quality > current.1)
            }) {
                *candidate = Some((specificity, quality));
            }
        };
        match media_type.as_str() {
            "application/vnd.pypi.simple.v1+json" | "application/vnd.pypi.simple.latest+json" => {
                update(&mut json, 2)
            }
            "application/vnd.pypi.simple.v1+html"
            | "application/vnd.pypi.simple.latest+html"
            | "text/html" => update(&mut html, 2),
            "application/*" => {
                update(&mut html, 1);
                update(&mut json, 1);
            }
            "text/*" => update(&mut html, 1),
            "*/*" => {
                update(&mut html, 0);
                update(&mut json, 0);
            }
            _ => {}
        }
    }
    let html = html.filter(|(_, quality)| *quality > 0.0);
    let json = json.filter(|(_, quality)| *quality > 0.0);
    match (html, json) {
        (None, None) => None,
        (Some(_), None) => Some(PythonRepresentation::Html),
        (None, Some(_)) => Some(PythonRepresentation::Json),
        (Some((html_specificity, html_quality)), Some((json_specificity, json_quality))) => {
            if json_quality > html_quality
                || (json_quality == html_quality
                    && json_specificity > 0
                    && json_specificity >= html_specificity)
            {
                Some(PythonRepresentation::Json)
            } else {
                Some(PythonRepresentation::Html)
            }
        }
    }
}

fn negotiate_npm(accept: Option<&str>) -> Option<bool> {
    let Some(accept) = accept.filter(|value| !value.trim().is_empty()) else {
        return Some(false);
    };
    let mut abbreviated = None;
    let mut full = None;
    for range in accept.split(',') {
        let mut fields = range.split(';');
        let media_type = fields
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        let quality = fields
            .filter_map(|field| field.trim().split_once('='))
            .find_map(|(name, value)| {
                name.trim()
                    .eq_ignore_ascii_case("q")
                    .then(|| value.trim().parse::<f32>().ok())
                    .flatten()
            })
            .unwrap_or(1.0)
            .clamp(0.0, 1.0);
        let update = |candidate: &mut Option<(u8, f32)>, specificity| {
            if candidate.is_none_or(|current| {
                specificity > current.0 || (specificity == current.0 && quality > current.1)
            }) {
                *candidate = Some((specificity, quality));
            }
        };
        match media_type.as_str() {
            "application/vnd.npm.install-v1+json" => update(&mut abbreviated, 2),
            "application/json" => update(&mut full, 2),
            "application/*" => {
                update(&mut abbreviated, 1);
                update(&mut full, 1);
            }
            "*/*" => {
                update(&mut abbreviated, 0);
                update(&mut full, 0);
            }
            _ => {}
        }
    }
    let abbreviated = abbreviated.filter(|(_, quality)| *quality > 0.0);
    let full = full.filter(|(_, quality)| *quality > 0.0);
    match (abbreviated, full) {
        (None, None) => None,
        (Some(_), None) => Some(true),
        (None, Some(_)) => Some(false),
        (Some((specificity, quality)), Some((_, full_quality))) => {
            Some(quality > full_quality || (quality == full_quality && specificity > 0))
        }
    }
}

pub enum ProxyOutcome {
    Artifact(ArtifactId),
    CachedMetadata {
        body: Bytes,
        content_type: Option<String>,
    },
    Upstream {
        status: StatusCode,
        body: Bytes,
        content_type: Option<String>,
    },
    /// A cache-bypassed download streamed straight from the upstream response, used when
    /// admission is refused under disk pressure so large bodies are never buffered.
    UpstreamStream {
        status: StatusCode,
        body: PassthroughStream,
        content_type: Option<String>,
    },
}

pub struct ProxyService {
    cache: Arc<CacheService>,
    clock: Arc<dyn Clock>,
    client: reqwest::Client,
    bases: UpstreamBases,
    allowed: AllowedOrigins,
    ttl: u64,
    concurrency: Arc<Semaphore>,
    requests: DashMap<String, Arc<Mutex<()>>>,
}

struct UpstreamBases {
    go: Url,
    python: Url,
    npm: Url,
    cargo_index: Url,
    cargo_crate: Url,
}

struct MetadataPublication {
    reference: String,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    raw: Bytes,
    rewritten: Bytes,
}

/// The origins each protocol may fetch from: the configured upstream's own origin,
/// any protocol-specific public download origins, and every operator-supplied entry
/// from `proxy_allowed_origins`. Origins derived from configuration are trusted even
/// when they resolve to private addresses (internal mirrors are a supported
/// deployment); every other origin is denied.
struct AllowedOrigins {
    go: HashSet<Origin>,
    python: HashSet<Origin>,
    npm: HashSet<Origin>,
    cargo_index: HashSet<Origin>,
    cargo_crate: HashSet<Origin>,
}

impl AllowedOrigins {
    fn for_protocol(&self, protocol: Protocol) -> &HashSet<Origin> {
        match protocol {
            Protocol::Go => &self.go,
            Protocol::Python => &self.python,
            Protocol::Npm => &self.npm,
            Protocol::CargoIndex => &self.cargo_index,
            Protocol::CargoCrate => &self.cargo_crate,
        }
    }
}

fn origin_set(base: &Url, shared: &[Origin], defaults: &[&str]) -> anyhow::Result<HashSet<Origin>> {
    let mut origins = HashSet::new();
    origins.insert(base.origin());
    origins.extend(shared.iter().cloned());
    for entry in defaults {
        origins.insert(Url::parse(entry)?.origin());
    }
    Ok(origins)
}

impl ProxyService {
    pub fn new(
        cache: Arc<CacheService>,
        clock: Arc<dyn Clock>,
        config: &Config,
    ) -> anyhow::Result<Self> {
        // Redirects are never followed by the client itself: every hop must pass the
        // same origin validation as the initial URL, so `send_validated` follows them
        // manually.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.upstream_timeout_seconds))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("flywheel/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let bases = UpstreamBases {
            go: Url::parse(&config.go_upstream)?,
            python: Url::parse(&config.python_upstream)?,
            npm: Url::parse(&config.npm_upstream)?,
            cargo_index: Url::parse(&config.cargo_index_upstream)?,
            cargo_crate: Url::parse(&config.cargo_crate_upstream)?,
        };
        let shared = config
            .proxy_allowed_origins
            .iter()
            .map(|entry| Ok(Url::parse(entry)?.origin()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let allowed = AllowedOrigins {
            go: origin_set(&bases.go, &shared, &[])?,
            python: origin_set(&bases.python, &shared, PYTHON_DOWNLOAD_ORIGINS)?,
            npm: origin_set(&bases.npm, &shared, &[])?,
            cargo_index: origin_set(&bases.cargo_index, &shared, &[])?,
            cargo_crate: origin_set(&bases.cargo_crate, &shared, &[])?,
        };
        Ok(Self {
            cache,
            clock,
            client,
            bases,
            allowed,
            ttl: config.proxy_revalidation_seconds,
            concurrency: Arc::new(Semaphore::new(config.proxy_concurrency)),
            requests: DashMap::new(),
        })
    }

    /// Refuses any URL whose origin is not allowlisted for the protocol. Non-http(s)
    /// schemes produce opaque origins that can never match an allowlisted tuple, so
    /// they are rejected by the same check.
    fn check_origin(&self, protocol: Protocol, url: &Url) -> Result<(), ProxyError> {
        if self.allowed.for_protocol(protocol).contains(&url.origin()) {
            Ok(())
        } else {
            Err(ProxyError::DisallowedOrigin)
        }
    }

    pub fn url(&self, protocol: Protocol, path: &str) -> Result<Url, ProxyError> {
        let base = match protocol {
            Protocol::Go => &self.bases.go,
            Protocol::Python => &self.bases.python,
            Protocol::Npm => &self.bases.npm,
            Protocol::CargoIndex => &self.bases.cargo_index,
            Protocol::CargoCrate => &self.bases.cargo_crate,
        };
        // `join` replaces the base entirely when the path is itself an absolute URL,
        // so the joined result must pass the same origin check as any other target.
        let url = base.join(path).map_err(ProxyError::Url)?;
        self.check_origin(protocol, &url)?;
        Ok(url)
    }

    pub async fn fetch(
        &self,
        channel: ChannelId,
        protocol: Protocol,
        path: &str,
        transform: Transform,
    ) -> Result<ProxyOutcome, ProxyError> {
        let url = self.url(protocol, path)?;
        self.fetch_url(channel, protocol, url, transform).await
    }

    pub async fn fetch_encoded_url(
        &self,
        channel: ChannelId,
        protocol: Protocol,
        encoded: &str,
    ) -> Result<ProxyOutcome, ProxyError> {
        self.fetch_encoded_url_with_suffix(channel, protocol, encoded, "")
            .await
    }

    pub async fn fetch_encoded_url_with_suffix(
        &self,
        channel: ChannelId,
        protocol: Protocol,
        encoded: &str,
        suffix: &str,
    ) -> Result<ProxyOutcome, ProxyError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| ProxyError::InvalidEncodedUrl)?;
        let value = std::str::from_utf8(&bytes).map_err(|_| ProxyError::InvalidEncodedUrl)?;
        let mut url = Url::parse(value).map_err(ProxyError::Url)?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(ProxyError::InvalidEncodedUrl);
        }
        if !suffix.is_empty() {
            let path = format!("{}{suffix}", url.path());
            url.set_path(&path);
        }
        self.check_origin(protocol, &url)?;
        self.fetch_url(channel, protocol, url, Transform::None)
            .await
    }

    async fn fetch_url(
        &self,
        channel: ChannelId,
        protocol: Protocol,
        url: Url,
        transform: Transform,
    ) -> Result<ProxyOutcome, ProxyError> {
        let reference = proxy_reference(protocol, &url, transform.cache_variant());
        if let Some(record) = self.cache.resolve_reference(channel, &reference).await?
            && self.clock.now().saturating_sub(record.updated_at) < self.ttl
            && let Some(outcome) = self
                .cached_outcome(channel, record.artifact, &url, &transform)
                .await?
        {
            return Ok(outcome);
        }

        let key = format!("{channel}:{reference}");
        let lock = Arc::clone(
            self.requests
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .value(),
        );
        let _flight = RequestFlight {
            requests: &self.requests,
            key,
        };
        let _coalesced = lock.lock().await;
        let current = self.cache.resolve_reference(channel, &reference).await?;
        if let Some(record) = &current
            && self.clock.now().saturating_sub(record.updated_at) < self.ttl
            && let Some(outcome) = self
                .cached_outcome(channel, record.artifact, &url, &transform)
                .await?
        {
            return Ok(outcome);
        }
        let _permit = self
            .concurrency
            .clone()
            .try_acquire_owned()
            .map_err(|_| ProxyError::Busy)?;

        let response = self
            .send_validated(protocol, url.clone(), &current, transform.upstream_accept())
            .await?;
        if response.status() == StatusCode::NOT_MODIFIED {
            let record = current.ok_or_else(|| {
                ProxyError::Unavailable("upstream returned 304 without a cached value".to_owned())
            })?;
            self.cache
                .bind_reference_with_validators(
                    channel,
                    reference,
                    record.artifact,
                    record.etag,
                    record.last_modified,
                )
                .await?;
            return self
                .cached_outcome(channel, record.artifact, &url, &transform)
                .await?
                .ok_or_else(|| {
                    ProxyError::Unavailable(
                        "upstream returned 304 for a missing cached body".to_owned(),
                    )
                });
        }

        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let etag = header_string(response.headers().get(header::ETAG));
        let last_modified = header_string(response.headers().get(header::LAST_MODIFIED));
        if !status.is_success() {
            return Ok(ProxyOutcome::Upstream {
                status,
                body: response.bytes().await?,
                content_type,
            });
        }

        match transform {
            // The raw-file path streams the body straight into the cache so large
            // packages are never buffered. If admission is refused up-front (the common
            // near-full steady state), the body is handed back untouched and streamed
            // straight to the client — no buffering and no second upstream fetch.
            Transform::None => {
                let upstream_length = header_length(response.headers());
                let bypass_content_type = content_type.clone();
                match self
                    .cache
                    .publish_or_reject(PublishRequest {
                        channel,
                        target: PublicationTarget::ContentAddressed { reference: None },
                        content_type,
                        stream: response.bytes_stream(),
                        content_length: upstream_length,
                        durability: Durability::BestEffort,
                        encoding: StoredEncoding::Identity,
                    })
                    .await?
                {
                    Admission::Published(publication) => {
                        self.cache
                            .bind_reference_with_validators(
                                channel,
                                reference,
                                publication.artifact,
                                etag,
                                last_modified,
                            )
                            .await?;
                        Ok(ProxyOutcome::Artifact(publication.artifact))
                    }
                    Admission::Rejected(stream) => {
                        tracing::warn!(%url, "package cache bypassed under disk pressure");
                        Ok(ProxyOutcome::UpstreamStream {
                            status,
                            content_type: bypass_content_type,
                            body: Box::pin(stream),
                        })
                    }
                }
            }
            // Metadata documents must be materialized to rewrite their links in place, so
            // they are read into memory regardless of caching. On admission failure the
            // already-built bytes are served directly — no allocation beyond the rewrite.
            transform @ (Transform::PythonHtml { .. }
            | Transform::PythonJson { .. }
            | Transform::NpmMetadata { .. }) => {
                let raw = response.bytes().await?;
                let rewritten = self.rewrite_metadata(&url, &transform, &raw)?;
                self.serve_metadata(
                    channel,
                    MetadataPublication {
                        reference,
                        content_type,
                        etag,
                        last_modified,
                        raw,
                        rewritten,
                    },
                )
                .await
            }
        }
    }

    async fn cached_outcome(
        &self,
        channel: ChannelId,
        artifact: ArtifactId,
        upstream: &Url,
        transform: &Transform,
    ) -> Result<Option<ProxyOutcome>, ProxyError> {
        if matches!(transform, Transform::None) {
            return Ok(Some(ProxyOutcome::Artifact(artifact)));
        }
        let Some(mut located) = self.cache.locate(channel, artifact).await? else {
            return Ok(None);
        };
        if located.metadata.encoding != StoredEncoding::Identity {
            return Err(ProxyError::Malformed(
                "cached package metadata has an unsupported encoding".to_owned(),
            ));
        }
        let mut raw = Vec::with_capacity(located.metadata.stored_len as usize);
        located
            .file
            .read_to_end(&mut raw)
            .await
            .map_err(|error| ProxyError::Unavailable(error.to_string()))?;
        let body = self.rewrite_metadata(upstream, transform, &raw)?;
        Ok(Some(ProxyOutcome::CachedMetadata {
            body,
            content_type: located.metadata.content_type,
        }))
    }

    fn rewrite_metadata(
        &self,
        upstream: &Url,
        transform: &Transform,
        bytes: &[u8],
    ) -> Result<Bytes, ProxyError> {
        match transform {
            Transform::None => Ok(Bytes::copy_from_slice(bytes)),
            Transform::PythonHtml {
                route_prefix,
                is_project_page,
            } => self.rewrite_python(upstream, route_prefix, *is_project_page, bytes),
            Transform::PythonJson {
                route_prefix,
                is_project_page,
            } => self.rewrite_python_json(upstream, route_prefix, *is_project_page, bytes),
            Transform::NpmMetadata { route_prefix, .. } => {
                rewrite_npm(upstream, route_prefix, bytes, &self.allowed.npm)
            }
        }
    }

    /// Sends the request, following at most [`REDIRECT_HOP_LIMIT`] redirects manually
    /// so every hop passes [`Self::check_origin`]. Each hop gets its own 3-attempt
    /// retry budget for 5xx / connect / timeout failures, and carries the conditional
    /// validators so a revalidation can complete at any hop.
    async fn send_validated(
        &self,
        protocol: Protocol,
        url: Url,
        current: &Option<ReferenceRecord>,
        accept: Option<&'static str>,
    ) -> Result<reqwest::Response, ProxyError> {
        let mut target = url;
        // One initial request plus up to REDIRECT_HOP_LIMIT follows.
        for _ in 0..=REDIRECT_HOP_LIMIT {
            let mut response = None;
            for attempt in 0..3 {
                let mut request = self.client.get(target.clone());
                if let Some(accept) = accept {
                    request = request.header(header::ACCEPT, accept);
                }
                if let Some(record) = current {
                    if let Some(etag) = &record.etag {
                        request = request.header(header::IF_NONE_MATCH, etag);
                    }
                    if let Some(last_modified) = &record.last_modified {
                        request = request.header(header::IF_MODIFIED_SINCE, last_modified);
                    }
                }
                match request.send().await {
                    Ok(candidate) if candidate.status().is_server_error() && attempt < 2 => {
                        continue;
                    }
                    Ok(candidate) => {
                        response = Some(candidate);
                        break;
                    }
                    Err(error) if attempt < 2 && (error.is_connect() || error.is_timeout()) => {
                        continue;
                    }
                    Err(error) => return Err(ProxyError::Http(error)),
                }
            }
            let response = response.ok_or_else(|| {
                ProxyError::Unavailable("upstream retry budget exhausted".to_owned())
            })?;
            if !is_redirect(response.status()) {
                return Ok(response);
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    ProxyError::Unavailable("redirect without a Location header".to_owned())
                })?;
            let next = target.join(location).map_err(ProxyError::Url)?;
            self.check_origin(protocol, &next)?;
            target = next;
        }
        Err(ProxyError::Unavailable(
            "upstream redirect limit exceeded".to_owned(),
        ))
    }

    async fn serve_metadata(
        &self,
        channel: ChannelId,
        publication: MetadataPublication,
    ) -> Result<ProxyOutcome, ProxyError> {
        let MetadataPublication {
            reference,
            content_type,
            etag,
            last_modified,
            raw,
            rewritten,
        } = publication;
        let length = raw.len() as u64;
        let body = stream::iter([Ok::<_, Infallible>(raw)]);
        match self
            .cache
            .publish(PublishRequest {
                channel,
                target: PublicationTarget::ContentAddressed { reference: None },
                content_type: content_type.clone(),
                stream: body,
                content_length: Some(length),
                durability: Durability::BestEffort,
                encoding: StoredEncoding::Identity,
            })
            .await
        {
            Ok(publication) => {
                self.cache
                    .bind_reference_with_validators(
                        channel,
                        reference,
                        publication.artifact,
                        etag,
                        last_modified,
                    )
                    .await?;
                Ok(ProxyOutcome::CachedMetadata {
                    body: rewritten,
                    content_type,
                })
            }
            Err(CacheError::Local(crate::storage::local::LocalError::OutOfSpace)) => {
                Ok(ProxyOutcome::Upstream {
                    status: StatusCode::OK,
                    body: rewritten,
                    content_type,
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn rewrite_python(
        &self,
        upstream: &Url,
        route_prefix: &str,
        is_project_page: bool,
        bytes: &[u8],
    ) -> Result<Bytes, ProxyError> {
        let html = std::str::from_utf8(bytes)
            .map_err(|_| ProxyError::Malformed("Python simple response is not UTF-8".to_owned()))?;
        let settings = RewriteStrSettings::new().append_element_content_handler(element!(
            "a[href]",
            |element| {
                if let Some(href) = element.get_attribute("href")
                    && let Ok(url) = upstream.join(&decode_html_entities(&href))
                {
                    let routed = if is_project_page {
                        self.check_origin(Protocol::Python, &url)
                            .ok()
                            .map(|()| routed_download_url(route_prefix, "/proxy/python/files", url))
                    } else {
                        upstream
                            .make_relative(&url)
                            .filter(|relative| !relative.starts_with("../"))
                            .map(|relative| {
                                format!(
                                    "{route_prefix}/proxy/python/simple/{}",
                                    relative.trim_start_matches("./")
                                )
                            })
                    };
                    if let Some(routed) = routed {
                        element.set_attribute("href", &routed)?;
                    }
                }
                if is_project_page
                    && let Some(provenance) = element.get_attribute("data-provenance")
                    && let Ok(url) = upstream.join(&decode_html_entities(&provenance))
                    && self.check_origin(Protocol::Python, &url).is_ok()
                {
                    let routed = routed_download_url(route_prefix, "/proxy/python/files", url);
                    element.set_attribute("data-provenance", &routed)?;
                }
                Ok(())
            }
        ));
        rewrite_str(html, settings)
            .map(Bytes::from)
            .map_err(|error| ProxyError::Malformed(format!("invalid Python HTML: {error}")))
    }

    fn rewrite_python_json(
        &self,
        upstream: &Url,
        route_prefix: &str,
        is_project_page: bool,
        bytes: &[u8],
    ) -> Result<Bytes, ProxyError> {
        let mut metadata: Value = serde_json::from_slice(bytes)
            .map_err(|error| ProxyError::Malformed(format!("invalid Python metadata: {error}")))?;
        if is_project_page
            && let Some(files) = metadata.get_mut("files").and_then(Value::as_array_mut)
        {
            for file in files {
                for field in ["url", "provenance"] {
                    let Some(url) = file
                        .get(field)
                        .and_then(Value::as_str)
                        .and_then(|candidate| upstream.join(candidate).ok())
                        .filter(|candidate| self.check_origin(Protocol::Python, candidate).is_ok())
                    else {
                        continue;
                    };
                    file[field] = Value::String(routed_download_url(
                        route_prefix,
                        "/proxy/python/files",
                        url,
                    ));
                }
            }
        }
        serde_json::to_vec(&metadata)
            .map(Bytes::from)
            .map_err(|error| ProxyError::Malformed(error.to_string()))
    }
}

/// The 3xx statuses that carry a `Location` to follow. Deliberately not
/// `StatusCode::is_redirection()`, which also matches 304 NOT_MODIFIED — a
/// revalidation hit the caller must see.
fn is_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

/// Publishes proxied bytes best-effort, returning the cached artifact id — or `None`
/// when the reservation failed under disk pressure so the caller can bypass the cache
/// and serve the upstream response directly instead of surfacing an error.
/// Parses an upstream `Content-Length` so the cache can size its reservation up front
/// and make an immediate admit/bypass decision before the body is streamed.
fn header_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

fn rewrite_npm(
    upstream: &Url,
    route_prefix: &str,
    bytes: &[u8],
    allowed: &HashSet<Origin>,
) -> Result<Bytes, ProxyError> {
    let mut metadata: Value = serde_json::from_slice(bytes)
        .map_err(|error| ProxyError::Malformed(format!("invalid npm metadata: {error}")))?;
    rewrite_tarballs(&mut metadata, upstream, route_prefix, allowed);
    serde_json::to_vec(&metadata)
        .map(Bytes::from)
        .map_err(|error| ProxyError::Malformed(error.to_string()))
}

fn rewrite_tarballs(
    value: &mut Value,
    upstream: &Url,
    route_prefix: &str,
    allowed: &HashSet<Origin>,
) {
    match value {
        Value::Object(fields) => {
            for (name, value) in fields {
                if name == "tarball" {
                    if let Some(candidate) = value
                        .as_str()
                        .and_then(|candidate| upstream.join(candidate).ok())
                        // A tarball the proxy would refuse to fetch keeps its real
                        // URL so the client goes direct instead of getting a 403.
                        .filter(|candidate| allowed.contains(&candidate.origin()))
                    {
                        let encoded = URL_SAFE_NO_PAD.encode(candidate.as_str());
                        *value =
                            Value::String(format!("{route_prefix}/proxy/npm/-/tarball/{encoded}"));
                    }
                } else {
                    rewrite_tarballs(value, upstream, route_prefix, allowed);
                }
            }
        }
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| rewrite_tarballs(value, upstream, route_prefix, allowed)),
        _ => {}
    }
}

fn routed_download_url(route_prefix: &str, route: &str, mut upstream: Url) -> String {
    let fragment = upstream.fragment().map(str::to_owned);
    upstream.set_fragment(None);
    let encoded = URL_SAFE_NO_PAD.encode(upstream.as_str());
    let mut routed = format!("{route_prefix}{route}/{encoded}");
    if let Some(fragment) = fragment {
        routed.push('#');
        routed.push_str(&fragment);
    }
    routed
}

fn proxy_reference(protocol: Protocol, url: &Url, variant: &str) -> String {
    let protocol = match protocol {
        Protocol::Go => "go",
        Protocol::Python => "python",
        Protocol::Npm => "npm",
        Protocol::CargoIndex => "cargo-index",
        Protocol::CargoCrate => "cargo-crate",
    };
    let url = url.as_str().as_bytes();
    let mut digest = Sha256::new();
    digest.update((url.len() as u64).to_be_bytes());
    digest.update(url);
    digest.update(variant.as_bytes());
    format!("proxy:{protocol}:{}", hex::encode(digest.finalize()))
}

fn header_string(value: Option<&HeaderValue>) -> Option<String> {
    value
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Removes a proxy coalescing entry once this is the last request referencing it, keeping
/// the `requests` map bounded across the process lifetime.
///
/// Invariant: every request-owned `Arc<Mutex<()>>` clone is paired with one
/// `RequestFlight`. At the call site the clone is declared first, this flight second,
/// and the mutex guard last, so reverse drop order is mutex guard → flight → request
/// clone. A count of two therefore means only the map and this request still own the
/// lock; another paired waiter keeps the count higher and its later flight retries
/// removal. An unpaired clone can leave the entry behind: it may keep the count above
/// two when the last flight drops, and dropping that clone later performs no retry.
struct RequestFlight<'a> {
    requests: &'a DashMap<String, Arc<Mutex<()>>>,
    key: String,
}

impl Drop for RequestFlight<'_> {
    fn drop(&mut self) {
        self.requests
            .remove_if(&self.key, |_, lock| Arc::strong_count(lock) <= 2);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("upstream resources are busy")]
    Busy,
    #[error("invalid encoded upstream URL")]
    InvalidEncodedUrl,
    #[error("upstream origin is not allowed")]
    DisallowedOrigin,
    #[error("invalid upstream URL: {0}")]
    Url(url::ParseError),
    #[error("upstream HTTP failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("upstream unavailable: {0}")]
    Unavailable(String),
    #[error("malformed upstream response: {0}")]
    Malformed(String),
    #[error(transparent)]
    Cache(#[from] CacheError),
}

#[cfg(test)]
mod tests {
    use super::RequestFlight;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn last_paired_request_removes_the_flight_entry() {
        let requests = DashMap::new();
        let key = "00000000000000000000000000:proxy:test".to_owned();
        requests.insert(key.clone(), Arc::new(Mutex::new(())));

        let lock = Arc::clone(requests.get(&key).unwrap().value());
        let flight = RequestFlight {
            requests: &requests,
            key: key.clone(),
        };
        let guard = lock.lock().await;

        drop(guard);
        drop(flight);

        assert!(!requests.contains_key(&key));
        drop(lock);
    }

    #[tokio::test]
    async fn flight_entry_stays_while_another_paired_waiter_exists() {
        let requests = DashMap::new();
        let key = "00000000000000000000000000:proxy:test".to_owned();
        requests.insert(key.clone(), Arc::new(Mutex::new(())));

        let first_lock = Arc::clone(requests.get(&key).unwrap().value());
        let first_flight = RequestFlight {
            requests: &requests,
            key: key.clone(),
        };
        let first_guard = first_lock.lock().await;

        // The second request owns both parts of the invariant while it waits: an Arc
        // clone and a flight that will retry removal when this request finishes.
        let second_lock = Arc::clone(requests.get(&key).unwrap().value());
        let second_flight = RequestFlight {
            requests: &requests,
            key: key.clone(),
        };

        drop(first_guard);
        drop(first_flight);
        assert!(requests.contains_key(&key));
        drop(first_lock);

        let second_guard = second_lock.lock().await;
        drop(second_guard);
        drop(second_flight);
        assert!(!requests.contains_key(&key));
        drop(second_lock);
    }
}
