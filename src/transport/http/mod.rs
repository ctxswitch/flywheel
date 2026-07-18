use crate::{
    artifact::{ArtifactId, Digest},
    cache::{
        CacheError, CacheService, Durability, PublicationOutcome, PublicationTarget,
        PublishRequest, StoredEncoding,
    },
    channel::{ChannelError, ChannelId, ChannelLease, ChannelRecord, ChannelService, Lifecycle},
    config::Config,
    prefetch::{FrameEncoding, FrameHeader, PrefetchRequest, encode_header},
    proxy::ProxyService,
    reference::Reference,
    telemetry::Metrics,
};
use async_compression::tokio::bufread::ZstdDecoder;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Request, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{io::SeekFrom, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

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
fn acquire_foreground(state: &AppState) -> Option<tokio::sync::OwnedSemaphorePermit> {
    Arc::clone(&state.foreground).try_acquire_owned().ok()
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
        .with_state(state)
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
        .route("/build-cache/prefetch", post(prefetch_build_cache))
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

#[derive(Deserialize)]
struct ChannelPath {
    channel: Option<String>,
}

#[derive(Deserialize)]
struct ArtifactPath {
    channel: Option<String>,
    algorithm: String,
    digest: String,
}

#[derive(Deserialize)]
struct ReferencePath {
    channel: Option<String>,
    reference: String,
}

#[derive(Deserialize)]
struct KeyPath {
    channel: Option<String>,
    key: String,
}

#[derive(Deserialize)]
struct HashPath {
    channel: Option<String>,
    hash: String,
}

pub(super) struct ChannelContext {
    pub channel: ChannelId,
    pub route_prefix: String,
    pub access_control: bool,
}

impl ChannelContext {
    async fn resolve(
        state: &Arc<AppState>,
        channel: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<Self, Response> {
        let Some(channel) = channel else {
            return Ok(Self {
                channel: ChannelId::DEFAULT,
                route_prefix: String::new(),
                access_control: false,
            });
        };
        let record = authorize_channel(state, channel, headers).await?;
        Ok(Self {
            channel: record.id,
            route_prefix: format!("/channels/{}", record.id),
            access_control: record.access.is_protected(),
        })
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
async fn metrics(State(state): State<Arc<AppState>>) -> String {
    state.metrics.render()
}

async fn count_request(
    State(metrics): State<Arc<Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    metrics.request();
    next.run(request).await
}

async fn put_artifact(
    State(state): State<Arc<AppState>>,
    Path(path): Path<ArtifactPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
    let Ok(artifact) = ArtifactId::parse(&path.algorithm, &path.digest) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ById(artifact),
            content_type,
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
        Err(CacheError::Local(crate::storage::local::LocalError::OutOfSpace)) => {
            state.metrics.raw_pressure_error();
            insufficient_storage()
        }
        Err(CacheError::Local(crate::storage::local::LocalError::DigestMismatch)) => {
            StatusCode::CONFLICT.into_response()
        }
        Err(CacheError::Local(crate::storage::local::LocalError::TooLarge)) => {
            StatusCode::PAYLOAD_TOO_LARGE.into_response()
        }
        Err(CacheError::ChannelDeleting | CacheError::MissingChannel) => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(error) => {
            tracing::error!(error = %error, "artifact publication failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn get_artifact(
    State(state): State<Arc<AppState>>,
    Path(path): Path<ArtifactPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
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
    if range == RangeSelection::Unsatisfiable {
        return Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::CONTENT_RANGE, format!("bytes */{content_len}"))
            .header(header::ETAG, format!("\"{}\"", artifact))
            .body(Body::empty())
            .unwrap();
    }
    let (start, end, length) = match range {
        RangeSelection::Partial { start, end } => (start, end, end - start + 1),
        RangeSelection::Full => (0, content_len.saturating_sub(1), content_len),
        RangeSelection::Unsatisfiable => unreachable!("handled above"),
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
fn body_with_permit<S>(stream: S, permit: tokio::sync::OwnedSemaphorePermit) -> Body
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

async fn put_reference(
    State(state): State<Arc<AppState>>,
    Path(path): Path<ReferencePath>,
    headers: HeaderMap,
    Json(binding): Json<ArtifactBinding>,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
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
        .bind_reference(context.channel, reference.to_string(), artifact)
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
    Path(path): Path<ReferencePath>,
    headers: HeaderMap,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
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
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    match state
        .cache
        .resolve_reference(context.channel, reference.as_str())
        .await
    {
        Ok(Some(record)) => Json(ArtifactBinding {
            algorithm: record.artifact.algorithm().to_owned(),
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
    Path(path): Path<ReferencePath>,
    headers: HeaderMap,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    let credential = credential(headers);
    match state
        .channels
        .authorize_with_lease(channel, credential.as_deref())
        .await
    {
        Ok(lease) => Ok(lease),
        Err(error) => {
            if matches!(error, ChannelError::Unauthorized) {
                state.metrics.authorization_denial();
            }
            Err(channel_failure(error))
        }
    }
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
    let credential = credential(headers);
    match state
        .channels
        .authorize(channel, credential.as_deref())
        .await
    {
        Ok(record) => Ok(record),
        Err(error) => {
            if matches!(error, ChannelError::Unauthorized) {
                state.metrics.authorization_denial();
            }
            Err(channel_failure(error))
        }
    }
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
        ChannelError::NotFound => api_error(
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
        ChannelError::Deleting => api_error(
            StatusCode::NOT_FOUND,
            "channel_not_found",
            "channel does not exist",
        ),
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
    Path(path): Path<KeyPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
    if Reference::parse(&path.key).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ContentAddressed {
                reference: Some(build_reference("http", &path.key)),
            },
            content_type,
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
    Path(path): Path<KeyPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
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

async fn prefetch_build_cache(
    State(state): State<Arc<AppState>>,
    Path(path): Path<ChannelPath>,
    headers: HeaderMap,
    Json(request): Json<PrefetchRequest>,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
    prefetch_response(state, context.channel, request)
}

/// Streams every requested digest back as one framed response. Unavailable entries
/// (evicted, never stored, or momentarily busy) become miss frames and the stream
/// keeps going — prefetch is a prediction, and the client falls back to ordinary
/// gets for anything missing. Only a truncated file aborts the stream, because
/// continuing would mis-frame every entry after it.
fn prefetch_response(
    state: Arc<AppState>,
    channel: ChannelId,
    request: PrefetchRequest,
) -> Response {
    if request.digests.len() > crate::prefetch::MAX_DIGESTS {
        return api_error(
            StatusCode::BAD_REQUEST,
            "too_many_digests",
            "prefetch request exceeds the digest limit",
        );
    }
    let mut digests = Vec::with_capacity(request.digests.len());
    for digest in &request.digests {
        match Digest::parse(digest) {
            Ok(digest) => digests.push(digest),
            Err(_) => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_digest",
                    "digests must be 64 lowercase hexadecimal characters",
                );
            }
        }
    }
    let streamer = PrefetchStreamer {
        cache: Arc::clone(&state.cache),
        foreground: Arc::clone(&state.foreground),
        channel,
        digests: digests.into_iter(),
        current: None,
    };
    let stream = futures_util::stream::unfold(streamer, |mut streamer| async move {
        streamer.next_chunk().await.map(|chunk| (chunk, streamer))
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, crate::prefetch::CONTENT_TYPE)
        .body(Body::from_stream(stream))
        .unwrap()
}

struct PrefetchStreamer {
    cache: Arc<CacheService>,
    foreground: Arc<tokio::sync::Semaphore>,
    channel: ChannelId,
    digests: std::vec::IntoIter<Digest>,
    current: Option<PrefetchEntry>,
}

/// The entry currently streaming: its open file, the stored bytes still owed to the
/// frame, and the foreground permit held until this entry finishes.
struct PrefetchEntry {
    file: tokio::fs::File,
    remaining: u64,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PrefetchStreamer {
    async fn next_chunk(&mut self) -> Option<std::io::Result<bytes::Bytes>> {
        loop {
            if let Some(current) = &mut self.current {
                if current.remaining == 0 {
                    self.current = None;
                    continue;
                }
                let take = usize::try_from(current.remaining.min(64 * 1024)).expect("chunk fits");
                let mut buffer = vec![0; take];
                return match current.file.read(&mut buffer).await {
                    Ok(0) => Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "stored artifact ended before its recorded length",
                    ))),
                    Ok(read) => {
                        current.remaining -= read as u64;
                        buffer.truncate(read);
                        Some(Ok(buffer.into()))
                    }
                    Err(error) => Some(Err(error)),
                };
            }
            let digest = self.digests.next()?;
            let artifact = ArtifactId::from_digest(digest);
            // A saturated foreground budget downgrades the entry to a miss frame —
            // prefetch is a prediction and the client falls back to ordinary gets.
            let Ok(permit) = Arc::clone(&self.foreground).try_acquire_owned() else {
                let header = FrameHeader::miss(digest.to_string());
                return Some(Ok(encode_header(&header).into()));
            };
            let header = match self.cache.locate(self.channel, artifact).await {
                Ok(Some(located)) => {
                    let header = FrameHeader {
                        digest: digest.to_string(),
                        miss: false,
                        stored_len: located.metadata.stored_len,
                        content_len: located.metadata.content_len,
                        encoding: match located.metadata.encoding {
                            StoredEncoding::Zstd => FrameEncoding::Zstd,
                            StoredEncoding::Identity => FrameEncoding::Identity,
                        },
                    };
                    self.current = Some(PrefetchEntry {
                        file: located.file,
                        remaining: header.stored_len,
                        _permit: permit,
                    });
                    header
                }
                Ok(None) => FrameHeader::miss(digest.to_string()),
                Err(error) => {
                    tracing::warn!(%error, "prefetch entry lookup failed");
                    FrameHeader::miss(digest.to_string())
                }
            };
            return Some(Ok(encode_header(&header).into()));
        }
    }
}

async fn put_bazel_cas(
    State(state): State<Arc<AppState>>,
    Path(path): Path<HashPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
    let Ok(artifact) = ArtifactId::parse("sha256", &path.hash) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ById(artifact),
            content_type,
            stream: body.into_data_stream(),
            content_length,
            durability: Durability::Durable,
            encoding: StoredEncoding::Identity,
        })
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(CacheError::Local(crate::storage::local::LocalError::OutOfSpace)) => {
            state.metrics.raw_pressure_error();
            insufficient_storage()
        }
        Err(error) => cache_write_failure(error),
    }
}

async fn get_bazel_cas(
    State(state): State<Arc<AppState>>,
    Path(path): Path<HashPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
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
    Path(path): Path<HashPath>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
    if ArtifactId::parse("sha256", &path.hash).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(_permit) = acquire_foreground(&state) else {
        return busy_response();
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_length = content_length(&headers);
    match state
        .cache
        .publish(PublishRequest {
            channel: context.channel,
            target: PublicationTarget::ContentAddressed {
                reference: Some(build_reference("bazel-ac", &path.hash)),
            },
            content_type,
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
    Path(path): Path<HashPath>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let context = match ChannelContext::resolve(&state, path.channel.as_deref(), &headers).await {
        Ok(context) => context,
        Err(response) => return response,
    };
    if ArtifactId::parse("sha256", &path.hash).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    serve_reference_artifact(
        state,
        context.channel,
        build_reference("bazel-ac", &path.hash),
        headers,
        method == Method::HEAD,
    )
    .await
}

fn cache_write_failure(error: CacheError) -> Response {
    match error {
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
            tracing::error!(error = %error, "build-cache write failed");
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
    cache_write_failure(error)
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
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
