//! The `flywheel agent` routing sidecar: DNS SRV discovery, a consistent-hash ring,
//! and one-owner request forwarding with twemproxy-style backend ejection.
//!
//! The agent owns placement only. It derives a versioned routing key from each
//! request path, forwards the request to the one owner selected by the continuum,
//! and never replays a failed request. Build-cache routes fail open (a miss for
//! reads, protocol-compatible success for writes) so a backend failure degrades a
//! build's cache instead of the build.

pub mod discovery;
pub mod ring;

use crate::{
    clock::Clock,
    prefetch::{FrameDecoder, FrameHeader, MAX_DIGESTS, PrefetchRequest, encode_header},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use discovery::{Resolver, RingState, RingStatus};
use ring::RingMember;
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct AgentOptions {
    /// SRV name that publishes one record per ready shard.
    pub srv: String,
    /// Upper bound on the DNS refresh interval regardless of answer TTL.
    pub refresh_max: Duration,
    /// Consecutive connect failures or send timeouts that eject a member (twemproxy
    /// `server_failure_limit`).
    pub failure_limit: u32,
    /// How long an ejected member stays out of the continuum (twemproxy
    /// `server_retry_timeout`).
    pub retry_timeout: Duration,
    /// Bound on establishing a backend connection; the primary detector for
    /// dead or blackholed backends.
    pub connect_timeout: Duration,
    /// Read-inactivity deadline: a forward fails when the backend makes no
    /// progress for this long. Deliberately not a total-transfer cap — cache
    /// bodies can be arbitrarily large and slow links must not be truncated.
    pub deadline: Duration,
}

#[derive(Clone)]
pub struct Agent {
    state: Arc<AgentState>,
}

struct AgentState {
    options: AgentOptions,
    ring: RingState,
    resolver: Arc<dyn Resolver>,
    client: reqwest::Client,
    metrics: AgentMetrics,
}

impl Agent {
    pub fn new(
        options: AgentOptions,
        resolver: Arc<dyn Resolver>,
        clock: Arc<dyn Clock>,
    ) -> anyhow::Result<Self> {
        // The client must behave as a transparent hop: no redirect following and no
        // transparent decompression, so stored zstd bodies pass through untouched.
        // Timeouts bound progress, never total transfer time: connect_timeout
        // detects dead backends, read_timeout fails stalled responses.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_zstd()
            .connect_timeout(options.connect_timeout)
            .read_timeout(options.deadline)
            .user_agent(concat!("flywheel-agent/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let ring = RingState::new(
            clock,
            options.failure_limit,
            options.retry_timeout.as_secs(),
        );
        Ok(Self {
            state: Arc::new(AgentState {
                options,
                ring,
                resolver,
                client,
                metrics: AgentMetrics::default(),
            }),
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/health/live", get(|| async { StatusCode::OK }))
            // Serving is the only readiness condition: an empty ring still answers
            // build-cache traffic with misses and write bypasses.
            .route("/health/ready", get(|| async { StatusCode::OK }))
            .route("/metrics", get(metrics))
            .route("/status", get(status))
            .route("/build-cache/prefetch", post(prefetch))
            .fallback(forward)
            .with_state(Arc::clone(&self.state))
    }

    /// Runs one discovery pass; tests drive membership through this determinately.
    pub async fn refresh_once(&self) -> anyhow::Result<Duration> {
        discovery::refresh_once(
            &self.state.ring,
            self.state.resolver.as_ref(),
            &self.state.options.srv,
        )
        .await
    }

    /// The background discovery loop, cancelled on shutdown.
    pub async fn run_discovery(&self, cancellation: CancellationToken) {
        let state = Arc::clone(&self.state);
        discovery::run(
            &state.ring,
            state.resolver.as_ref(),
            &state.options.srv,
            state.options.refresh_max,
            cancellation,
        )
        .await;
    }
}

/// Entry point for the `flywheel agent` subcommand.
pub async fn run(arguments: crate::cli::AgentArgs) -> anyhow::Result<()> {
    let options = AgentOptions {
        srv: arguments.srv.clone(),
        refresh_max: Duration::from_secs(arguments.refresh_max.max(1)),
        failure_limit: arguments.failure_limit,
        retry_timeout: Duration::from_secs(arguments.retry_timeout),
        connect_timeout: Duration::from_secs(arguments.connect_timeout.max(1)),
        deadline: Duration::from_secs(arguments.deadline.max(1)),
    };
    let resolver = Arc::new(discovery::DnsResolver::from_system()?);
    let agent = Agent::new(options, resolver, Arc::new(crate::clock::SystemClock))?;
    let listener = tokio::net::TcpListener::bind(arguments.listen).await?;
    let cancellation = CancellationToken::new();
    let discovery_task = tokio::spawn({
        let agent = agent.clone();
        let cancellation = cancellation.clone();
        async move { agent.run_discovery(cancellation).await }
    });
    tracing::info!(listen = %arguments.listen, srv = arguments.srv, "Flywheel agent is ready");
    axum::serve(listener, agent.router())
        .with_graceful_shutdown({
            let cancellation = cancellation.clone();
            async move {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "shutdown signal failed");
                }
                cancellation.cancel();
            }
        })
        .await?;
    cancellation.cancel();
    let _ = discovery_task.await;
    Ok(())
}

/// How a route degrades when no owner can serve it: build-cache traffic fails
/// open, everything else surfaces an upstream error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RouteClass {
    BuildCacheRead,
    BuildCacheWrite,
    Passthrough,
}

#[derive(Debug, Eq, PartialEq)]
struct RoutedKey {
    kind: String,
    id: String,
    class: RouteClass,
}

/// Derives the routing object from a bare default-channel request path, mirroring the
/// bare routes in `transport/http`. The method never contributes to
/// the key — `GET`, `HEAD`, and `PUT` for one object select the same owner — it
/// only picks the degradation class for build-cache routes.
fn classify(method: &Method, path: &str) -> RoutedKey {
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (kind, id) = match segments.as_slice() {
        ["artifacts", _algorithm, digest] => ("artifact", (*digest).to_owned()),
        ["build-cache", "bazel", "cas", hash] => ("artifact", (*hash).to_owned()),
        ["build-cache", "bazel", "ac", hash] => ("bazel-action", (*hash).to_owned()),
        ["build-cache", "http", key] => ("http-cache", (*key).to_owned()),
        ["references", reference] => ("reference", (*reference).to_owned()),
        ["proxy", protocol, rest @ ..] if !rest.is_empty() => {
            return RoutedKey {
                kind: (*protocol).to_owned(),
                id: rest.join("/"),
                class: RouteClass::Passthrough,
            };
        }
        // No stable semantic ID: the documented fallback keys on the full path and
        // keeps the route's own failure semantics.
        _ => {
            return RoutedKey {
                kind: "path".to_owned(),
                id: path.to_owned(),
                class: RouteClass::Passthrough,
            };
        }
    };
    let class = if segments.first() == Some(&"build-cache") {
        match *method {
            Method::GET | Method::HEAD => RouteClass::BuildCacheRead,
            Method::PUT | Method::POST => RouteClass::BuildCacheWrite,
            _ => RouteClass::Passthrough,
        }
    } else {
        RouteClass::Passthrough
    };
    RoutedKey {
        kind: kind.to_owned(),
        id,
        class,
    }
}

async fn forward(State(state): State<Arc<AgentState>>, request: Request) -> Response {
    state.metrics.requests.bump();
    if request.uri().path() == "/channels" || request.uri().path().starts_with("/channels/") {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    }
    let routed = classify(request.method(), request.uri().path());
    let Some(position) = ring::key_position(&routed.kind, &routed.id) else {
        return StatusCode::URI_TOO_LONG.into_response();
    };
    let Some(owner) = state.ring.ring().owner(position).cloned() else {
        return degrade(&state, routed.class, true);
    };
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    let url = format!("http://{}{}", owner.address, path_and_query);
    let method = request.method().clone();
    let reqwest_method =
        reqwest::Method::from_bytes(method.as_str().as_bytes()).expect("method round-trips");
    let (parts, body) = request.into_parts();
    let mut upstream = state.client.request(reqwest_method, url);
    for (name, value) in &parts.headers {
        if !skip_request_header(name.as_str()) {
            upstream = upstream.header(name.as_str(), value.as_bytes());
        }
    }
    if !matches!(method, Method::GET | Method::HEAD) {
        upstream = upstream.body(reqwest::Body::wrap_stream(body.into_data_stream()));
    }
    match upstream.send().await {
        Ok(response) => {
            // Receiving response headers completes the send and proves the member is
            // reachable. Status and later body consumption do not affect ejection.
            state.ring.record_success(&owner.id);
            state.metrics.forwarded.bump();
            proxied_response(response)
        }
        Err(error) => {
            tracing::warn!(%error, owner = owner.id, "forwarded request failed");
            record_send_failure(&state, &owner, &error);
            degrade(&state, routed.class, false)
        }
    }
}

/// Counts a failure toward ejection only when it is attributable to the backend
/// (connect refused or timed out). A client aborting its own upload mid-stream
/// also fails `send()`, and that must not eject a healthy shard — a genuinely
/// dead backend produces a connect failure on the very next request anyway.
fn record_send_failure(state: &AgentState, owner: &RingMember, error: &reqwest::Error) {
    if !(error.is_connect() || error.is_timeout()) {
        return;
    }
    state.metrics.forward_failures.bump();
    if state.ring.record_failure(&owner.id) {
        tracing::warn!(owner = owner.id, "backend ejected from the continuum");
        state.metrics.ejections.bump();
    }
}

/// Synthesizes the route's degraded response when no backend exchange happened:
/// the failed request is never replayed against the rebuilt continuum.
fn degrade(state: &AgentState, class: RouteClass, empty_ring: bool) -> Response {
    match class {
        RouteClass::BuildCacheRead => {
            state.metrics.synthesized_misses.bump();
            StatusCode::NOT_FOUND.into_response()
        }
        RouteClass::BuildCacheWrite => {
            state.metrics.synthesized_bypasses.bump();
            StatusCode::OK.into_response()
        }
        RouteClass::Passthrough => {
            state.metrics.unavailable.bump();
            if empty_ring {
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            } else {
                StatusCode::BAD_GATEWAY.into_response()
            }
        }
    }
}

/// Transparently streams the backend body. Backend health was already judged when
/// `send()` returned response headers, so body errors are only client-visible errors.
fn proxied_response(upstream: reqwest::Response) -> Response {
    let mut builder = Response::builder().status(upstream.status().as_u16());
    for (name, value) in upstream.headers() {
        if !skip_response_header(name.as_str()) {
            builder = builder.header(name.as_str(), value.as_bytes());
        }
    }
    builder
        .body(Body::from_stream(upstream.bytes_stream()))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

/// Hop-by-hop request headers. `Content-Length` passes through: the inbound
/// server already enforces it on the streamed body, and shards use it to size
/// reservations up front.
fn skip_request_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "expect"
    )
}

fn skip_response_header(name: &str) -> bool {
    matches!(
        name,
        "connection" | "keep-alive" | "te" | "trailer" | "transfer-encoding" | "upgrade"
    )
}

#[derive(Serialize)]
struct AgentStatus {
    srv: String,
    #[serde(flatten)]
    ring: RingStatus,
}

async fn status(State(state): State<Arc<AgentState>>) -> Response {
    Json(AgentStatus {
        srv: state.options.srv.clone(),
        ring: state.ring.status(),
    })
    .into_response()
}

async fn metrics(State(state): State<Arc<AgentState>>) -> String {
    let status = state.ring.status();
    let ejected = status
        .members
        .iter()
        .filter(|member| member.ejected)
        .count();
    state.metrics.render(status.members.len(), ejected)
}

/// Serves a prefetch by sweeping the ring members in order: the full digest list
/// goes to the first member, only the still-unserved digests to the next, and
/// whatever no member returned becomes miss frames. The sweep makes no placement
/// assumption at all — an object is found on whichever shard its original write
/// landed on (the cacheprog flow places bodies by HTTP-cache key, not by digest),
/// and a dead or truncated member simply passes its unserved digests to the next
/// one. Only complete frames are forwarded, so one bad member cannot corrupt the
/// framing of the rest.
async fn prefetch(
    State(state): State<Arc<AgentState>>,
    Json(request): Json<PrefetchRequest>,
) -> Response {
    state.metrics.requests.bump();
    if request.digests.len() > MAX_DIGESTS {
        return api_error(
            StatusCode::BAD_REQUEST,
            "too_many_digests",
            "prefetch request exceeds the digest limit",
        );
    }
    if request
        .digests
        .iter()
        .any(|digest| crate::artifact::Digest::parse(digest).is_err())
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_digest",
            "digests must be 64 lowercase hexadecimal characters",
        );
    }
    let members = state.ring.ring().members().to_vec();
    let (sender, receiver) = tokio::sync::mpsc::channel::<PrefetchChunk>(8);
    tokio::spawn(prefetch_sweep(
        Arc::clone(&state),
        members,
        request.digests,
        sender,
    ));
    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|chunk| (chunk, receiver))
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, crate::prefetch::CONTENT_TYPE)
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

type PrefetchChunk = Result<Bytes, std::convert::Infallible>;

async fn prefetch_sweep(
    state: Arc<AgentState>,
    members: Vec<RingMember>,
    mut remaining: Vec<String>,
    output: tokio::sync::mpsc::Sender<PrefetchChunk>,
) {
    for member in members {
        if remaining.is_empty() {
            break;
        }
        let outcome = state
            .client
            .post(format!("http://{}/build-cache/prefetch", member.address))
            .json(&PrefetchRequest {
                digests: remaining.clone(),
            })
            .send()
            .await;
        let response = match outcome {
            Ok(response) => {
                // Like ordinary forwarding, response headers reset the streak even
                // when the status rejects the request or the body later truncates.
                state.ring.record_success(&member.id);
                if response.status().is_success() {
                    response
                } else {
                    tracing::warn!(
                        member = member.id,
                        status = %response.status(),
                        "prefetch sweep rejected"
                    );
                    continue;
                }
            }
            Err(error) => {
                tracing::warn!(%error, member = member.id, "prefetch sweep failed");
                record_send_failure(&state, &member, &error);
                continue;
            }
        };
        let body =
            futures_util::TryStreamExt::map_err(response.bytes_stream(), std::io::Error::other);
        let mut decoder = FrameDecoder::new(tokio_util::io::StreamReader::new(body));
        let mut hits = std::collections::HashSet::new();
        let mut missing = Vec::new();
        loop {
            match decoder.next_frame().await {
                Ok(Some((header, frame_body))) => {
                    if header.miss {
                        missing.push(header.digest);
                        continue;
                    }
                    hits.insert(header.digest.clone());
                    if output
                        .send(Ok(encode_header(&header).into()))
                        .await
                        .is_err()
                    {
                        return; // The client went away; nothing left to serve.
                    }
                    if !frame_body.is_empty() && output.send(Ok(frame_body.into())).await.is_err() {
                        return;
                    }
                }
                Ok(None) => {
                    remaining = missing;
                    break;
                }
                Err(error) => {
                    // Truncated sub-response: frames already forwarded are whole,
                    // and everything this member did not fully serve rolls over to
                    // the next one. A truncated exchange is not a success.
                    tracing::warn!(%error, member = member.id, "prefetch sub-response truncated");
                    remaining.retain(|digest| !hits.contains(digest));
                    break;
                }
            }
        }
    }
    for digest in remaining {
        let frame = Bytes::from(encode_header(&FrameHeader::miss(digest)));
        if output.send(Ok(frame)).await.is_err() {
            return;
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

fn api_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(ErrorBody { code, message })).into_response()
}

#[derive(Default)]
struct Counter(AtomicU64);

impl Counter {
    fn bump(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct AgentMetrics {
    requests: Counter,
    forwarded: Counter,
    forward_failures: Counter,
    synthesized_misses: Counter,
    synthesized_bypasses: Counter,
    unavailable: Counter,
    ejections: Counter,
}

impl AgentMetrics {
    fn render(&self, members: usize, ejected: usize) -> String {
        format!(
            concat!(
                "# TYPE flywheel_agent_requests_total counter\nflywheel_agent_requests_total {}\n",
                "# TYPE flywheel_agent_forwarded_total counter\nflywheel_agent_forwarded_total {}\n",
                "# TYPE flywheel_agent_forward_failures_total counter\nflywheel_agent_forward_failures_total {}\n",
                "# TYPE flywheel_agent_synthesized_misses_total counter\nflywheel_agent_synthesized_misses_total {}\n",
                "# TYPE flywheel_agent_synthesized_write_bypasses_total counter\nflywheel_agent_synthesized_write_bypasses_total {}\n",
                "# TYPE flywheel_agent_unavailable_total counter\nflywheel_agent_unavailable_total {}\n",
                "# TYPE flywheel_agent_ejections_total counter\nflywheel_agent_ejections_total {}\n",
                "# TYPE flywheel_agent_ring_members gauge\nflywheel_agent_ring_members {}\n",
                "# TYPE flywheel_agent_ring_ejected gauge\nflywheel_agent_ring_ejected {}\n",
            ),
            self.requests.get(),
            self.forwarded.get(),
            self.forward_failures.get(),
            self.synthesized_misses.get(),
            self.synthesized_bypasses.get(),
            self.unavailable.get(),
            self.ejections.get(),
            members,
            ejected,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{RouteClass, classify};
    use axum::http::Method;

    const DIGEST: &str = "0f7fe6f9a72d9298a0295e64c400b485b0a4118bcdcd7d100e9d9c1a1ea115c5";

    #[test]
    fn raw_artifact_and_bazel_cas_share_a_routing_object() {
        let raw = classify(&Method::GET, &format!("/artifacts/sha256/{DIGEST}"));
        let cas = classify(&Method::PUT, &format!("/build-cache/bazel/cas/{DIGEST}"));
        assert_eq!(raw.kind, "artifact");
        assert_eq!(cas.kind, "artifact");
        assert_eq!(raw.id, cas.id);
        // The raw artifact API keeps its durable error semantics while the CAS
        // route fails open as a build-cache write.
        assert_eq!(raw.class, RouteClass::Passthrough);
        assert_eq!(cas.class, RouteClass::BuildCacheWrite);
    }

    #[test]
    fn method_selects_class_but_never_the_key() {
        let read = classify(&Method::GET, "/build-cache/http/some-key");
        let head = classify(&Method::HEAD, "/build-cache/http/some-key");
        let write = classify(&Method::PUT, "/build-cache/http/some-key");
        assert_eq!(read.kind, write.kind);
        assert_eq!(read.id, write.id);
        assert_eq!(read.class, RouteClass::BuildCacheRead);
        assert_eq!(head.class, RouteClass::BuildCacheRead);
        assert_eq!(write.class, RouteClass::BuildCacheWrite);
    }

    #[test]
    fn build_cache_routes_map_to_their_kinds() {
        assert_eq!(
            classify(&Method::GET, &format!("/build-cache/bazel/ac/{DIGEST}")).kind,
            "bazel-action"
        );
        assert_eq!(
            classify(&Method::GET, "/build-cache/http/go-abc123").id,
            "go-abc123"
        );
        assert_eq!(
            classify(&Method::GET, "/references/latest").kind,
            "reference"
        );
    }

    #[test]
    fn proxy_routes_key_on_protocol_and_canonical_remainder() {
        let go = classify(&Method::GET, "/proxy/go/github.com/foo/@v/v1.0.0.zip");
        assert_eq!(go.kind, "go");
        assert_eq!(go.id, "github.com/foo/@v/v1.0.0.zip");
        assert_eq!(go.class, RouteClass::Passthrough);
        let cargo = classify(&Method::GET, "/proxy/cargo/index/config.json");
        assert_eq!(cargo.kind, "cargo");
        assert_eq!(cargo.id, "index/config.json");
    }

    #[test]
    fn unrecognized_paths_fall_back_to_the_full_path_key() {
        let routed = classify(&Method::GET, "/future/route");
        assert_eq!(routed.kind, "path");
        assert_eq!(routed.id, "/future/route");
        assert_eq!(routed.class, RouteClass::Passthrough);
        let bare_proxy = classify(&Method::GET, "/proxy/go");
        assert_eq!(bare_proxy.kind, "path");
    }
}
