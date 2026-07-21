use crate::{
    artifact::ArtifactId,
    cache::{
        CacheError, CacheService, Durability, PublicationOutcome, PublicationTarget,
        PublishRequest, StoredEncoding,
    },
    channel::{ChannelError, ChannelId, ChannelLease, ChannelRecord, ChannelService, Lifecycle},
    config::Config,
    manifest::{
        REQUEST_PREFETCH_CONCURRENCY_HEADER, REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH,
        REQUEST_SESSION_HEADER,
    },
    proxy::ProxyService,
    reference::Reference,
    telemetry::{
        MakeRequestUlid, Metrics, PrefetchObservation, REQUEST_ID_HEADER, observe_prefetch_body,
    },
    transport::content_length,
};
use async_compression::tokio::bufread::ZstdDecoder;
use axum::{
    Json, Router,
    body::Body,
    extract::{FromRequestParts, MatchedPath, Path, Request, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, io::SeekFrom, sync::Arc, time::Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use tower::ServiceBuilder;
use tower_http::{
    LatencyUnit,
    request_id::{PropagateRequestIdLayer, SetRequestIdLayer},
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
};

mod packages;

pub struct AppState {
    pub config: Config,
    pub cache: Arc<CacheService>,
    pub channels: Arc<ChannelService>,
    pub proxy: Arc<ProxyService>,
    pub metrics: Arc<Metrics>,
    /// The transport-wide foreground budget. Handlers acquire a permit before
    /// touching the cache and, for streamed responses, attach it to the body so
    /// the slot stays held until the client finishes downloading.
    pub foreground: Arc<tokio::sync::Semaphore>,
}

/// One slot of the foreground budget, or `None` when the server is saturated and
/// the request should shed with a 429.
struct ForegroundPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
}

impl Drop for ForegroundPermit {
    fn drop(&mut self) {
        self.metrics.foreground_released();
    }
}

fn acquire_foreground(state: &AppState) -> Option<ForegroundPermit> {
    match Arc::clone(&state.foreground).try_acquire_owned() {
        Ok(permit) => {
            state.metrics.foreground_acquired();
            Some(ForegroundPermit {
                _permit: permit,
                metrics: Arc::clone(&state.metrics),
            })
        }
        Err(_) => {
            state.metrics.foreground_rejected();
            None
        }
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    let router = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/channels", post(register_channel))
        .route(
            "/channels/{channel}",
            get(get_channel).patch(patch_channel).delete(delete_channel),
        )
        .nest("/channels/{channel}", data_router())
        .merge(data_router());
    let request_metrics = Arc::clone(&state.metrics);
    router
        .layer(middleware::from_fn_with_state(
            request_metrics,
            count_request,
        ))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::new(
                    REQUEST_ID_HEADER.clone(),
                    MakeRequestUlid,
                ))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(server_request_span)
                        .on_request(())
                        .on_response(
                            DefaultOnResponse::new()
                                .level(tracing::Level::DEBUG)
                                .latency_unit(LatencyUnit::Millis),
                        )
                        .on_failure(
                            DefaultOnFailure::new()
                                .level(tracing::Level::ERROR)
                                .latency_unit(LatencyUnit::Millis),
                        ),
                )
                .layer(PropagateRequestIdLayer::new(REQUEST_ID_HEADER.clone())),
        )
        .with_state(state)
}

fn server_request_span(request: &Request) -> tracing::Span {
    let (operation, key) = server_request_identity(request);
    let request_id = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("invalid");
    tracing::info_span!(
        "http_request",
        component = "server",
        %request_id,
        method = %request.method(),
        %operation,
        %key,
    )
}

/// The span's operation and key for one request. Both borrow out of the request
/// wherever the path already holds the text; only the multi-segment proxy key is
/// assembled.
fn server_request_identity(request: &Request) -> (&str, Cow<'_, str>) {
    let path = request.uri().path();
    let raw_segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let segments = match raw_segments.as_slice() {
        ["channels", _channel, rest @ ..] => rest,
        rest => rest,
    };
    match segments {
        ["artifacts", _algorithm, digest] => ("artifact", Cow::Borrowed(*digest)),
        ["build-cache", "bazel", "cas", hash] => ("artifact", Cow::Borrowed(*hash)),
        ["build-cache", "bazel", "ac", hash] => ("bazel-action", Cow::Borrowed(*hash)),
        ["build-cache", "http", key] => ("http-cache", Cow::Borrowed(*key)),
        ["references", reference] => ("reference", Cow::Borrowed(*reference)),
        ["proxy", protocol, rest @ ..] if !rest.is_empty() => {
            (*protocol, Cow::Owned(rest.join("/")))
        }
        _ => (
            request
                .extensions()
                .get::<MatchedPath>()
                .map_or("unmatched", MatchedPath::as_str),
            Cow::Borrowed(""),
        ),
    }
}

fn data_router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/artifacts/{algorithm}/{digest}",
            get(get_artifact).put(put_artifact),
        )
        .route(
            "/references/{reference}",
            get(get_reference)
                .put(put_reference)
                .delete(delete_reference),
        )
        .route(
            "/build-cache/http/{key}",
            get(get_http_cache).put(put_http_cache),
        )
        .route(
            "/build-cache/bazel/ac/{hash}",
            get(get_bazel_ac).put(put_bazel_ac),
        )
        .route(
            "/build-cache/bazel/cas/{hash}",
            get(get_bazel_cas).put(put_bazel_cas),
        )
        .route("/proxy/go/{*path}", get(packages::go))
        .route("/proxy/python/simple", get(packages::python_simple_root))
        .route("/proxy/python/simple/", get(packages::python_simple_root))
        .route("/proxy/python/simple/{*path}", get(packages::python_simple))
        .route("/proxy/python/files/{encoded}", get(packages::python_file))
        .route("/proxy/npm/{*path}", get(packages::npm))
        .route(
            "/proxy/cargo/index/config.json",
            get(packages::cargo_config),
        )
        .route("/proxy/cargo/index/{*path}", get(packages::cargo_index))
        .route(
            "/proxy/cargo/crates/{crate}/{version}/download",
            get(packages::cargo_crate),
        )
}

/// The `{channel}` capture every data route may carry. Present on the `/channels/{id}`
/// prefixed forms and absent on the bare ones, which resolve to the default channel.
#[derive(Deserialize)]
struct ChannelPath {
    channel: Option<String>,
}

#[derive(Deserialize)]
struct ArtifactPath {
    algorithm: String,
    digest: String,
}

#[derive(Deserialize)]
struct ReferencePath {
    reference: String,
}

#[derive(Deserialize)]
struct KeyPath {
    key: String,
}

#[derive(Deserialize)]
struct HashPath {
    hash: String,
}

pub(super) struct ChannelContext {
    pub channel: ChannelId,
    /// Set only when the request came in under an explicit `/channels/{id}` prefix.
    /// The bare routes resolve to the default channel and carry no prefix.
    scope: Option<ChannelId>,
    pub access_control: bool,
}

/// Resolves and authorizes the channel a data request addresses, so every data handler
/// gets it by taking a `ChannelContext` argument instead of repeating the lookup.
///
/// The `{channel}` capture is read through `Path`, which borrows the captured
/// parameters out of the request extensions rather than taking them, so a handler can
/// still extract its own `Path<T>` for the rest of the route.
impl FromRequestParts<Arc<AppState>> for ChannelContext {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let Path(ChannelPath { channel }) = Path::<ChannelPath>::from_request_parts(parts, state)
            .await
            .map_err(|_| invalid_channel())?;
        let Some(channel) = channel else {
            return Ok(Self {
                channel: ChannelId::DEFAULT,
                scope: None,
                access_control: false,
            });
        };
        let record = authorize_channel(state, &channel, &parts.headers).await?;
        Ok(Self {
            channel: record.id,
            scope: Some(record.id),
            access_control: record.access.is_protected(),
        })
    }
}

impl ChannelContext {
    /// The path prefix rewritten package URLs must carry to route back to this
    /// channel. Only the package-proxy handlers rewrite URLs, so it is built on
    /// demand rather than on every request.
    pub(super) fn route_prefix(&self) -> String {
        self.scope
            .map_or_else(String::new, |channel| format!("/channels/{channel}"))
    }
}

async fn live() -> StatusCode {
    StatusCode::OK
}
async fn ready(State(state): State<Arc<AppState>>) -> StatusCode {
    match state.cache.ready().await {
        Ok(()) => StatusCode::OK,
        Err(error) => {
            tracing::warn!(%error, "readiness check failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}
async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    match state.metrics.encode() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)
            .body(Body::from(body))
            .expect("static Prometheus response is valid"),
        Err(error) => {
            tracing::error!(%error, "Prometheus metrics encoding failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn count_request(
    State(metrics): State<Arc<Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    metrics.request();
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |path| path.as_str().to_owned());
    let prefetch = request
        .headers()
        .get(REQUEST_PURPOSE_HEADER)
        .is_some_and(|value| value.as_bytes() == REQUEST_PURPOSE_PREFETCH.as_bytes());
    // The session and concurrency headers are read only to describe a prefetch, so
    // foreground traffic never parses them.
    let observation = prefetch.then(|| {
        let session = request
            .headers()
            .get(REQUEST_SESSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let configured_concurrency = request
            .headers()
            .get(REQUEST_PREFETCH_CONCURRENCY_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok());
        PrefetchObservation::start(
            Arc::clone(&metrics),
            Some(route.clone()),
            session,
            configured_concurrency,
        )
    });
    let started = Instant::now();
    let response = next.run(request).await;
    let response_headers_duration = started.elapsed();
    metrics.observe_http_request(
        method.as_str(),
        &route,
        response.status(),
        response_headers_duration,
    );
    match observation {
        Some(observation) => observe_prefetch_body(response, observation),
        None => response,
    }
}

async fn put_artifact(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ArtifactPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Ok(artifact) = ArtifactId::parse(&path.algorithm, &path.digest) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ById(artifact),
            content_type: content_type(&headers),
            stream: body.into_data_stream(),
            content_length,
            durability: Durability::Durable,
            encoding: StoredEncoding::Identity,
        })
        .await
    {
        Ok(publication) if publication.outcome == PublicationOutcome::Created => {
            StatusCode::CREATED.into_response()
        }
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => cache_write_failure(&state, error),
    }
}

async fn get_artifact(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ArtifactPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Ok(artifact) = ArtifactId::parse(&path.algorithm, &path.digest) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    serve_artifact(
        state,
        context.channel,
        artifact,
        headers,
        method == Method::HEAD,
    )
    .await
}

async fn serve_artifact(
    state: Arc<AppState>,
    channel: ChannelId,
    artifact: ArtifactId,
    headers: HeaderMap,
    head: bool,
) -> Response {
    let Some(permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let located = match state.cache.locate(channel, artifact).await {
        Ok(Some(located)) => located,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(error = %error, "artifact lookup failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let content_len = located.metadata.content_len;
    // Only identity GETs support ranges. HEAD and zstd-stored representations ignore
    // Range and follow ordinary full-response content negotiation.
    let range = if !head && located.metadata.encoding == StoredEncoding::Identity {
        parse_range(headers.get(header::RANGE), content_len)
    } else {
        RangeSelection::Full
    };
    let (start, end, length) = match range {
        RangeSelection::Partial { start, end } => (start, end, end - start + 1),
        RangeSelection::Full => (0, content_len.saturating_sub(1), content_len),
        RangeSelection::Unsatisfiable => {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CONTENT_RANGE, format!("bytes */{content_len}"))
                .header(header::ETAG, format!("\"{}\"", artifact))
                .body(Body::empty())
                .unwrap();
        }
    };
    // A zstd-stored response is passed through untouched when the client accepts zstd;
    // otherwise it serves the complete logical bytes through the decoder.
    let passthrough = located.metadata.encoding == StoredEncoding::Zstd && accepts_zstd(&headers);
    let mut builder = Response::builder()
        .status(if matches!(range, RangeSelection::Partial { .. }) {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(
            header::CONTENT_LENGTH,
            if passthrough {
                located.metadata.stored_len
            } else {
                length
            },
        )
        .header(header::ETAG, format!("\"{}\"", artifact));
    if located.metadata.encoding == StoredEncoding::Identity {
        builder = builder.header(header::ACCEPT_RANGES, "bytes");
    }
    if located.metadata.encoding == StoredEncoding::Zstd {
        builder = builder.header(header::VARY, "accept-encoding");
    }
    if passthrough {
        builder = builder.header(header::CONTENT_ENCODING, "zstd");
    }
    if let Some(content_type) = located.metadata.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    if matches!(range, RangeSelection::Partial { .. }) {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{content_len}"),
        );
    }
    if head || (length == 0 && !passthrough) {
        return builder.body(Body::empty()).unwrap();
    }
    let mut file = located.file;
    let body = match located.metadata.encoding {
        StoredEncoding::Zstd if passthrough => body_with_permit(ReaderStream::new(file), permit),
        StoredEncoding::Zstd => {
            let decoder = ZstdDecoder::new(tokio::io::BufReader::new(file));
            body_with_permit(ReaderStream::new(decoder.take(length)), permit)
        }
        StoredEncoding::Identity => {
            if start > 0 && file.seek(SeekFrom::Start(start)).await.is_err() {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            body_with_permit(ReaderStream::new(file.take(length)), permit)
        }
    };
    builder.body(body).unwrap()
}

fn accepts_zstd(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().split(';').next().map(str::trim) == Some("zstd"))
        })
}

/// Streams a body while holding the foreground permit until the stream is dropped.
fn body_with_permit<S>(stream: S, permit: ForegroundPermit) -> Body
where
    S: futures_util::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Unpin + 'static,
{
    let stream =
        futures_util::stream::unfold((stream, permit), |(mut stream, permit)| async move {
            stream.next().await.map(|item| (item, (stream, permit)))
        });
    Body::from_stream(stream)
}

#[derive(Deserialize, Serialize)]
struct ArtifactBinding {
    algorithm: String,
    digest: String,
}

/// The one data handler that takes its `ChannelContext` as a `Result`: extractors run
/// before the body, so rejecting here would answer a syntactically broken request with
/// a channel error. Holding the rejection until the body has parsed keeps a malformed
/// payload a 400 whatever the channel's access control says.
async fn put_reference(
    State(state): State<Arc<AppState>>,
    context: Result<ChannelContext, Response>,
    Path(path): Path<ReferencePath>,
    Json(binding): Json<ArtifactBinding>,
) -> Response {
    let context = match context {
        Ok(context) => context,
        Err(response) => return response,
    };
    let Ok(reference) = Reference::parse(path.reference) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_reference",
            "reference is not URL-safe",
        );
    };
    let Ok(artifact) = ArtifactId::parse(&binding.algorithm, &binding.digest) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_artifact",
            "artifact identity is invalid",
        );
    };
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    match state
        .cache
        .bind_reference(context.channel, reference.into_string(), artifact)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(CacheError::ChannelDeleting | CacheError::MissingChannel) => api_error(
            StatusCode::NOT_FOUND,
            "channel_not_found",
            "channel does not exist",
        ),
        Err(error) => {
            tracing::error!(error = %error, "reference update failed");
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "reference update failed",
            )
        }
    }
}

async fn get_reference(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ReferencePath>,
) -> Response {
    let Ok(reference) = Reference::parse(path.reference) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_reference",
            "reference is not URL-safe",
        );
    };
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    match state
        .cache
        .resolve_reference(context.channel, reference.as_str())
        .await
    {
        Ok(Some(record)) => Json(ArtifactBinding {
            algorithm: ArtifactId::ALGORITHM.to_owned(),
            digest: record.artifact.digest().to_string(),
        })
        .into_response(),
        Ok(None) => api_error(
            StatusCode::NOT_FOUND,
            "reference_not_found",
            "reference does not exist",
        ),
        Err(error) => {
            tracing::error!(error = %error, "reference lookup failed");
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "reference lookup failed",
            )
        }
    }
}

async fn delete_reference(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ReferencePath>,
) -> Response {
    let Ok(reference) = Reference::parse(path.reference) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_reference",
            "reference is not URL-safe",
        );
    };
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    match state
        .cache
        .delete_reference(context.channel, reference.as_str())
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(CacheError::ChannelDeleting | CacheError::MissingChannel) => api_error(
            StatusCode::NOT_FOUND,
            "channel_not_found",
            "channel does not exist",
        ),
        Err(error) => {
            tracing::error!(error = %error, "reference deletion failed");
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "reference deletion failed",
            )
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RangeSelection {
    Full,
    Partial { start: u64, end: u64 },
    Unsatisfiable,
}

/// Selects one identity-byte range. Invalid syntax, unsupported units, numeric
/// overflow, and multipart requests are ignored as a normal full response; only a
/// syntactically valid range that cannot overlap the representation is unsatisfiable.
fn parse_range(value: Option<&HeaderValue>, len: u64) -> RangeSelection {
    let Some(value) = value else {
        return RangeSelection::Full;
    };
    let Ok(value) = value.to_str() else {
        return RangeSelection::Full;
    };
    let Some(range) = value.strip_prefix("bytes=") else {
        return RangeSelection::Full;
    };
    if range.contains(',') {
        return RangeSelection::Full;
    }
    let Some((start, end)) = range.split_once('-') else {
        return RangeSelection::Full;
    };
    if start.is_empty() {
        let Ok(suffix) = end.parse::<u64>() else {
            return RangeSelection::Full;
        };
        if suffix == 0 || len == 0 {
            return RangeSelection::Unsatisfiable;
        }
        let start = len.saturating_sub(suffix);
        return RangeSelection::Partial {
            start,
            end: len - 1,
        };
    }
    let Ok(start) = start.parse::<u64>() else {
        return RangeSelection::Full;
    };
    let end = if end.is_empty() {
        None
    } else {
        let Ok(end) = end.parse::<u64>() else {
            return RangeSelection::Full;
        };
        if end < start {
            return RangeSelection::Full;
        }
        Some(end)
    };
    if start >= len {
        return RangeSelection::Unsatisfiable;
    }
    RangeSelection::Partial {
        start,
        end: end.unwrap_or(len - 1).min(len - 1),
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
}

fn api_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(ErrorBody { code, message })).into_response()
}

#[derive(Deserialize)]
struct RegisterChannel {
    access_control: Option<bool>,
    expiry_seconds: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchChannel {
    expiry_seconds: Option<u64>,
}

#[derive(Serialize)]
struct ChannelView {
    channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    access_control: bool,
    expiry_seconds: u64,
    state: Lifecycle,
}

impl ChannelView {
    fn from_record(record: &ChannelRecord, token: Option<String>) -> Self {
        Self {
            channel: record.id.to_string(),
            token,
            access_control: record.access.is_protected(),
            expiry_seconds: record.expiry_seconds,
            state: record.state,
        }
    }
}

async fn register_channel(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RegisterChannel>,
) -> Response {
    let protected = request.access_control.unwrap_or(false);
    let expiry_seconds = request
        .expiry_seconds
        .unwrap_or(state.config.default_expiry_seconds);
    if expiry_seconds == 0 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_policy",
            "expiry must be positive",
        );
    }
    match state.channels.register(protected, expiry_seconds).await {
        Ok(issued) => (
            StatusCode::CREATED,
            Json(ChannelView::from_record(
                &issued.record,
                issued.token.as_ref().map(|token| token.expose().to_owned()),
            )),
        )
            .into_response(),
        Err(error) => channel_failure(error),
    }
}

async fn get_channel(
    State(state): State<Arc<AppState>>,
    Path(channel): Path<String>,
    headers: HeaderMap,
) -> Response {
    let record = match authorize_channel(&state, &channel, &headers).await {
        Ok(record) => record,
        Err(response) => return response,
    };
    Json(ChannelView::from_record(&record, None)).into_response()
}

async fn patch_channel(
    State(state): State<Arc<AppState>>,
    Path(channel): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<PatchChannel>, JsonRejection>,
) -> Response {
    let Json(patch) = match payload {
        Ok(patch) => patch,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_patch",
                "only expiry_seconds may be changed",
            );
        }
    };
    let mut lease = match channel_lease(&state, &channel, &headers).await {
        Ok(lease) => lease,
        Err(response) => return response,
    };
    let expiry_seconds = patch.expiry_seconds.unwrap_or(lease.record.expiry_seconds);
    if expiry_seconds == 0 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_policy",
            "expiry must be positive",
        );
    }
    match state
        .channels
        .update_expiry(&mut lease, expiry_seconds)
        .await
    {
        Ok(()) => Json(ChannelView::from_record(&lease.record, None)).into_response(),
        Err(error) => channel_failure(error),
    }
}

async fn delete_channel(
    State(state): State<Arc<AppState>>,
    Path(channel): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(channel) = channel.parse::<ChannelId>() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_channel",
            "channel identifier is invalid",
        );
    };
    let credential = credential(&headers);
    match state.channels.delete(channel, credential.as_deref()).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => channel_failure(error),
    }
}

async fn channel_lease(
    state: &Arc<AppState>,
    channel: &str,
    headers: &HeaderMap,
) -> Result<ChannelLease, Response> {
    let channel = channel
        .parse::<ChannelId>()
        .map_err(|_| invalid_channel())?;
    state
        .channels
        .authorize_with_lease(channel, credential(headers).as_deref())
        .await
        .map_err(|error| channel_denied(state, error))
}

/// Validates channel credentials and `active` state without taking the lifecycle gate.
/// Reads remain lock-free, while uploads acquire their deletion fence only at commit.
async fn authorize_channel(
    state: &Arc<AppState>,
    channel: &str,
    headers: &HeaderMap,
) -> Result<ChannelRecord, Response> {
    let channel = channel
        .parse::<ChannelId>()
        .map_err(|_| invalid_channel())?;
    state
        .channels
        .authorize(channel, credential(headers).as_deref())
        .await
        .map_err(|error| channel_denied(state, error))
}

/// The response for a rejected channel authorization, counting the denials that were
/// credential failures rather than missing or deleting channels.
fn channel_denied(state: &Arc<AppState>, error: ChannelError) -> Response {
    if matches!(error, ChannelError::Unauthorized) {
        state.metrics.authorization_denial();
    }
    channel_failure(error)
}

fn invalid_channel() -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        "invalid_channel",
        "channel identifier is invalid",
    )
}

fn credential(headers: &HeaderMap) -> Option<String> {
    let authorization = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    if let Some(token) = authorization.strip_prefix("Bearer ") {
        return Some(token.to_owned());
    }
    if let Some(encoded) = authorization.strip_prefix("Basic ") {
        let decoded = STANDARD.decode(encoded).ok()?;
        let decoded = std::str::from_utf8(&decoded).ok()?;
        return Some(decoded.split_once(':')?.1.to_owned());
    }
    // Cargo's registry credential-provider protocol sends the token itself as the
    // Authorization value, without an HTTP authentication scheme.
    authorization
        .starts_with("flywheel_")
        .then(|| authorization.to_owned())
}

fn channel_failure(error: ChannelError) -> Response {
    match error {
        // A channel being deleted is indistinguishable from an absent one to a client:
        // its data is already unreachable and its id will never be reused.
        ChannelError::NotFound | ChannelError::Deleting => api_error(
            StatusCode::NOT_FOUND,
            "channel_not_found",
            "channel does not exist",
        ),
        ChannelError::Unauthorized => {
            let mut response = api_error(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "channel credentials are required",
            );
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"flywheel\""),
            );
            response
        }
        ChannelError::DefaultChannel => api_error(
            StatusCode::CONFLICT,
            "default_channel",
            "the default channel cannot be deleted",
        ),
        error => {
            tracing::error!(error = %error, "channel operation failed");
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "channel operation failed",
            )
        }
    }
}

fn build_reference(kind: &str, key: &str) -> String {
    format!("build:{kind}:{key}")
}

async fn put_http_cache(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<KeyPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if !Reference::is_valid(&path.key) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ContentAddressed {
                reference: Some(build_reference("http", &path.key)),
            },
            content_type: content_type(&headers),
            stream: body.into_data_stream(),
            content_length,
            durability: Durability::BestEffort,
            encoding: StoredEncoding::Zstd,
        })
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(error) => cache_write_failure_with_bypass(&state, error),
    }
}

async fn get_http_cache(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<KeyPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    serve_reference_artifact(
        state,
        context.channel,
        build_reference("http", &path.key),
        headers,
        method == Method::HEAD,
    )
    .await
}

async fn serve_reference_artifact(
    state: Arc<AppState>,
    channel: ChannelId,
    reference: String,
    headers: HeaderMap,
    head: bool,
) -> Response {
    // Scope the resolve permit so it is released before `serve_artifact`
    // acquires its own — otherwise this path would briefly need two slots and a
    // foreground budget of one would always shed against itself.
    let resolved = {
        let Some(_permit) = acquire_foreground(&state) else {
            return busy_response();
        };
        state.cache.resolve_reference(channel, &reference).await
    };
    match resolved {
        Ok(Some(record)) => serve_artifact(state, channel, record.artifact, headers, head).await,
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(error = %error, "build-cache reference lookup failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn put_bazel_cas(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<HashPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Ok(artifact) = ArtifactId::parse("sha256", &path.hash) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ById(artifact),
            content_type: content_type(&headers),
            stream: body.into_data_stream(),
            content_length,
            durability: Durability::Durable,
            encoding: StoredEncoding::Identity,
        })
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(error) => cache_write_failure(&state, error),
    }
}

async fn get_bazel_cas(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<HashPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Ok(artifact) = ArtifactId::parse("sha256", &path.hash) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    serve_artifact(
        state,
        context.channel,
        artifact,
        headers,
        method == Method::HEAD,
    )
    .await
}

async fn put_bazel_ac(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<HashPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if ArtifactId::parse("sha256", &path.hash).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ContentAddressed {
                reference: Some(build_reference("bazel-ac", &path.hash)),
            },
            content_type: content_type(&headers),
            stream: body.into_data_stream(),
            content_length,
            durability: Durability::BestEffort,
            encoding: StoredEncoding::Zstd,
        })
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(error) => cache_write_failure_with_bypass(&state, error),
    }
}

async fn get_bazel_ac(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<HashPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    // No hex validation on the read: an unparseable hash cannot name a stored action
    // result either, so it takes the same reference lookup and misses, exactly as the
    // HTTP build-cache read does.
    serve_reference_artifact(
        state,
        context.channel,
        build_reference("bazel-ac", &path.hash),
        headers,
        method == Method::HEAD,
    )
    .await
}

/// The response for a failed upload. Disk pressure is reported to the client as 507
/// with a retry hint; the artifact and Bazel CAS routes have no protocol-level way to
/// say "stored elsewhere", so they cannot bypass the way the build-cache writes do.
fn cache_write_failure(state: &Arc<AppState>, error: CacheError) -> Response {
    match error {
        CacheError::Local(crate::storage::local::LocalError::OutOfSpace) => {
            state.metrics.raw_pressure_error();
            insufficient_storage()
        }
        CacheError::Local(crate::storage::local::LocalError::DigestMismatch) => {
            StatusCode::CONFLICT.into_response()
        }
        CacheError::Local(crate::storage::local::LocalError::TooLarge) => {
            StatusCode::PAYLOAD_TOO_LARGE.into_response()
        }
        CacheError::ChannelDeleting | CacheError::MissingChannel => {
            StatusCode::NOT_FOUND.into_response()
        }
        error => {
            tracing::error!(error = %error, "cache write failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// A build-cache write under disk pressure bypasses the store and reports
/// protocol-compatible success so the caller proceeds without caching.
fn cache_write_failure_with_bypass(state: &Arc<AppState>, error: CacheError) -> Response {
    if matches!(
        error,
        CacheError::Local(crate::storage::local::LocalError::OutOfSpace)
    ) {
        state.metrics.build_cache_bypass();
        return StatusCode::OK.into_response();
    }
    cache_write_failure(state, error)
}

fn content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn insufficient_storage() -> Response {
    Response::builder()
        .status(StatusCode::INSUFFICIENT_STORAGE)
        .header(header::RETRY_AFTER, "1")
        .body(Body::empty())
        .unwrap()
}

fn busy_response() -> Response {
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(header::RETRY_AFTER, "1")
        .body(Body::empty())
        .unwrap()
}
