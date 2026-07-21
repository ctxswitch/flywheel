use super::*;
use crate::proxy::{Protocol, ProxyError, ProxyOutcome, Transform};

#[derive(Deserialize)]
pub(super) struct ProxyPath {
    path: String,
}

#[derive(Deserialize)]
pub(super) struct EncodedPath {
    encoded: String,
}

#[derive(Deserialize)]
pub(super) struct CargoCratePath {
    #[serde(rename = "crate")]
    name: String,
    version: String,
}

pub(super) async fn go(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ProxyPath>,
) -> Response {
    fetch(
        &state,
        context.channel,
        Protocol::Go,
        &path.path,
        Transform::None,
    )
    .await
}

pub(super) async fn python_simple(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ProxyPath>,
    headers: HeaderMap,
) -> Response {
    fetch_python_simple(&state, &context, &path.path, &headers).await
}

pub(super) async fn python_simple_root(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    headers: HeaderMap,
) -> Response {
    fetch_python_simple(&state, &context, "", &headers).await
}

async fn fetch_python_simple(
    state: &Arc<AppState>,
    context: &ChannelContext,
    path: &str,
    headers: &HeaderMap,
) -> Response {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok());
    let Some(transform) = Transform::python_simple(context.route_prefix(), path, accept) else {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    };
    fetch(state, context.channel, Protocol::Python, path, transform).await
}

pub(super) async fn python_file(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<EncodedPath>,
) -> Response {
    let (encoded, suffix) = [".metadata", ".asc"]
        .into_iter()
        .find_map(|suffix| {
            path.encoded
                .strip_suffix(suffix)
                .map(|encoded| (encoded, suffix))
        })
        .unwrap_or((&path.encoded, ""));
    fetch_encoded(&state, context.channel, Protocol::Python, encoded, suffix).await
}

pub(super) async fn npm(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ProxyPath>,
    headers: HeaderMap,
) -> Response {
    if let Some(encoded) = path.path.strip_prefix("-/tarball/") {
        return fetch_encoded(&state, context.channel, Protocol::Npm, encoded, "").await;
    }
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok());
    let Some(transform) = Transform::npm_metadata(context.route_prefix(), accept) else {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    };
    fetch(
        &state,
        context.channel,
        Protocol::Npm,
        &path.path,
        transform,
    )
    .await
}

pub(super) async fn cargo_index(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<ProxyPath>,
) -> Response {
    fetch(
        &state,
        context.channel,
        Protocol::CargoIndex,
        &path.path,
        Transform::None,
    )
    .await
}

pub(super) async fn cargo_config(context: ChannelContext) -> Response {
    Json(serde_json::json!({
        "dl": format!("{}/proxy/cargo/crates", context.route_prefix()),
        "api": null,
        "auth-required": context.access_control,
    }))
    .into_response()
}

pub(super) async fn cargo_crate(
    State(state): State<Arc<AppState>>,
    context: ChannelContext,
    Path(path): Path<CargoCratePath>,
) -> Response {
    fetch_cargo_crate(&state, context.channel, &path.name, &path.version).await
}

async fn fetch_cargo_crate(
    state: &Arc<AppState>,
    channel: ChannelId,
    name: &str,
    version: &str,
) -> Response {
    if name.is_empty() || version.is_empty() || name.contains('/') || version.contains('/') {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let path = format!("{name}/{name}-{version}.crate");
    fetch(state, channel, Protocol::CargoCrate, &path, Transform::None).await
}

async fn fetch(
    state: &Arc<AppState>,
    channel: ChannelId,
    protocol: Protocol,
    path: &str,
    transform: Transform,
) -> Response {
    match state.proxy.fetch(channel, protocol, path, transform).await {
        Ok(outcome) => outcome_response(state, channel, outcome).await,
        Err(error) => proxy_failure(error),
    }
}

async fn fetch_encoded(
    state: &Arc<AppState>,
    channel: ChannelId,
    protocol: Protocol,
    encoded: &str,
    suffix: &str,
) -> Response {
    match state
        .proxy
        .fetch_encoded_url(channel, protocol, encoded, suffix)
        .await
    {
        Ok(outcome) => outcome_response(state, channel, outcome).await,
        Err(error) => proxy_failure(error),
    }
}

async fn outcome_response(
    state: &Arc<AppState>,
    channel: ChannelId,
    outcome: ProxyOutcome,
) -> Response {
    match outcome {
        ProxyOutcome::Artifact(artifact) => {
            serve_artifact(
                Arc::clone(state),
                channel,
                artifact,
                HeaderMap::new(),
                false,
            )
            .await
        }
        // The rewritten document is already materialized in memory, so there is no
        // cache work left to admit and no reason to hold a foreground slot for the
        // client's download.
        ProxyOutcome::CachedMetadata { body, content_type } => {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, body.len());
            if let Some(content_type) = content_type {
                builder = builder.header(header::CONTENT_TYPE, content_type);
            }
            builder.body(Body::from(body)).unwrap()
        }
        ProxyOutcome::Upstream {
            status,
            body,
            content_type,
        } => upstream_response(status, content_type, Body::from(body)),
        ProxyOutcome::UpstreamStream {
            status,
            body,
            content_type,
        } => upstream_response(status, content_type, Body::from_stream(body)),
    }
}

/// Passes an upstream response through unchanged apart from its content type, whether
/// the body was buffered or is being streamed past the cache under disk pressure.
fn upstream_response(status: StatusCode, content_type: Option<String>, body: Body) -> Response {
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder.body(body).unwrap()
}

fn proxy_failure(error: ProxyError) -> Response {
    match error {
        ProxyError::Busy => Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(header::RETRY_AFTER, "1")
            .body(Body::empty())
            .unwrap(),
        ProxyError::DisallowedOrigin => StatusCode::FORBIDDEN.into_response(),
        ProxyError::InvalidEncodedUrl | ProxyError::Url(_) | ProxyError::Malformed(_) => {
            StatusCode::BAD_GATEWAY.into_response()
        }
        error => {
            tracing::warn!(error = %error, "package upstream request failed");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}
