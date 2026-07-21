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

#[cfg(test)]
mod agent_test;
#[cfg(test)]
mod discovery_test;
#[cfg(test)]
mod ring_test;
use crate::{
    clock::Clock,
    manifest::{
        REQUEST_PREFETCH_CONCURRENCY_HEADER, REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH,
        REQUEST_SESSION_HEADER,
    },
    telemetry::{
        MakeRequestUlid, PrefetchObservation, PrefetchRecorder, REQUEST_ID_HEADER, encode_registry,
        histogram, int_counter, int_gauge, observe_prefetch_body, saturating_i64, transfer_buckets,
    },
};
use axum::{
    Json, Router,
    body::Body,
    extract::{MatchedPath, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use discovery::{Resolver, RingState, RingStatus};
use prometheus::{Histogram, IntCounter, IntGauge, Registry};
use ring::RingMember;
use serde::Serialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::{
    LatencyUnit,
    request_id::{PropagateRequestIdLayer, SetRequestIdLayer},
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
};

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
    metrics: Arc<AgentMetrics>,
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
                metrics: Arc::new(AgentMetrics::new()?),
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
            .fallback(forward)
            .layer(
                ServiceBuilder::new()
                    .layer(SetRequestIdLayer::new(
                        REQUEST_ID_HEADER.clone(),
                        MakeRequestUlid,
                    ))
                    .layer(
                        TraceLayer::new_for_http()
                            .make_span_with(agent_request_span)
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
    let startup = Instant::now();
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
    tracing::info!(
        component = "agent",
        version = env!("CARGO_PKG_VERSION"),
        listen = %arguments.listen,
        srv = arguments.srv,
        failure_limit = arguments.failure_limit,
        retry_timeout_seconds = arguments.retry_timeout,
        connect_timeout_seconds = arguments.connect_timeout,
        read_timeout_seconds = arguments.deadline,
        startup_ms = startup.elapsed().as_millis() as u64,
        "Flywheel agent is ready"
    );
    axum::serve(listener, agent.router())
        .with_graceful_shutdown({
            let cancellation = cancellation.clone();
            async move {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "shutdown signal failed");
                }
                tracing::info!(component = "agent", "shutdown requested");
                cancellation.cancel();
            }
        })
        .await?;
    cancellation.cancel();
    let _ = discovery_task.await;
    tracing::info!(component = "agent", "shutdown complete");
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

impl RouteClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::BuildCacheRead => "build_cache_read",
            Self::BuildCacheWrite => "build_cache_write",
            Self::Passthrough => "passthrough",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct RoutedKey {
    kind: String,
    id: String,
    class: RouteClass,
}

fn agent_request_span(request: &Request) -> tracing::Span {
    let request_id = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("invalid");
    let span = tracing::info_span!(
        "http_request",
        component = "agent",
        %request_id,
        method = %request.method(),
        operation = tracing::field::Empty,
        key = tracing::field::Empty,
        class = tracing::field::Empty,
    );
    // A control route is identified by its matched path; every other request is
    // classified once by the forwarder, which records these fields itself rather
    // than making the span repeat the work.
    if let Some(path) = request.extensions().get::<MatchedPath>() {
        span.record("operation", path.as_str());
        span.record("key", "");
        span.record("class", "control");
    }
    span
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
    state.metrics.requests.inc();
    // Telemetry only: the purpose header separates speculative prefetch traffic
    // from foreground traffic in the counters and is forwarded untouched. It never
    // affects routing, admission, authorization, or the response.
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
            Arc::clone(&state.metrics),
            None,
            session,
            configured_concurrency,
        )
    });
    let started = Instant::now();
    let response = forward_request(&state, request, prefetch).await;
    state
        .metrics
        .forward_duration
        .observe(started.elapsed().as_secs_f64());
    match observation {
        Some(observation) => observe_prefetch_body(response, observation),
        None => response,
    }
}

async fn forward_request(state: &Arc<AgentState>, request: Request, prefetch: bool) -> Response {
    let routed = classify(request.method(), request.uri().path());
    let span = tracing::Span::current();
    span.record("operation", routed.kind.as_str());
    span.record("key", routed.id.as_str());
    span.record("class", routed.class.as_str());
    if request.uri().path() == "/channels" || request.uri().path().starts_with("/channels/") {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    }
    let Some(position) = ring::key_position(&routed.kind, &routed.id) else {
        return StatusCode::URI_TOO_LONG.into_response();
    };
    let ring = state.ring.ring();
    let Some(owner) = ring.owner(position) else {
        if prefetch {
            state.metrics.prefetch_unavailable.inc();
        }
        return degrade(state, routed.class, true);
    };
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    let url = format!("http://{}{}", owner.address, path_and_query);
    let (parts, body) = request.into_parts();
    // Axum and reqwest share one `http` crate, so the inbound method is the outbound
    // method — no textual round-trip.
    let bodyless = matches!(parts.method, Method::GET | Method::HEAD);
    let mut upstream = state.client.request(parts.method, url);
    for (name, value) in &parts.headers {
        if !skip_request_header(name.as_str()) {
            upstream = upstream.header(name.as_str(), value.as_bytes());
        }
    }
    if !bodyless {
        upstream = upstream.body(reqwest::Body::wrap_stream(body.into_data_stream()));
    }
    match upstream.send().await {
        Ok(response) => {
            // Receiving response headers completes the send and proves the member is
            // reachable. Status and later body consumption do not affect ejection.
            state.ring.record_success(&owner.id);
            state.metrics.forwarded.inc();
            if prefetch {
                if response.status().is_success() {
                    state.metrics.prefetch_hits.inc();
                } else if response.status() == reqwest::StatusCode::NOT_FOUND {
                    state.metrics.prefetch_misses.inc();
                } else {
                    state.metrics.prefetch_unavailable.inc();
                }
            }
            proxied_response(response)
        }
        Err(error) => {
            tracing::warn!(%error, owner = owner.id, "forwarded request failed");
            record_send_failure(state, owner, &error);
            if prefetch {
                state.metrics.prefetch_unavailable.inc();
            }
            degrade(state, routed.class, false)
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
    state.metrics.forward_failures.inc();
    if state.ring.record_failure(&owner.id) {
        tracing::warn!(owner = owner.id, "backend ejected from the continuum");
        state.metrics.ejections.inc();
    }
}

/// Synthesizes the route's degraded response when no backend exchange happened:
/// the failed request is never replayed against the rebuilt continuum.
fn degrade(state: &AgentState, class: RouteClass, empty_ring: bool) -> Response {
    match class {
        RouteClass::BuildCacheRead => {
            state.metrics.synthesized_misses.inc();
            StatusCode::NOT_FOUND.into_response()
        }
        RouteClass::BuildCacheWrite => {
            state.metrics.synthesized_bypasses.inc();
            StatusCode::OK.into_response()
        }
        RouteClass::Passthrough => {
            state.metrics.unavailable.inc();
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

/// The operational ring view: SRV name, fingerprint, and members. Never routes,
/// addresses, or manifests — the agent stays the topology boundary.
async fn status(State(state): State<Arc<AgentState>>) -> Response {
    Json(AgentStatus {
        srv: state.options.srv.clone(),
        ring: state.ring.status(),
    })
    .into_response()
}

async fn metrics(State(state): State<Arc<AgentState>>) -> Response {
    let status = state.ring.status();
    let ejected = status
        .members
        .iter()
        .filter(|member| member.ejected)
        .count();
    match state.metrics.encode(status.members.len(), ejected) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, prometheus::TEXT_FORMAT)
            .body(Body::from(body))
            .expect("static Prometheus response is valid"),
        Err(error) => {
            tracing::error!(%error, "Prometheus metrics encoding failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

struct AgentMetrics {
    registry: Registry,
    requests: IntCounter,
    forwarded: IntCounter,
    forward_duration: Histogram,
    forward_failures: IntCounter,
    synthesized_misses: IntCounter,
    synthesized_bypasses: IntCounter,
    unavailable: IntCounter,
    ejections: IntCounter,
    prefetch_requests: IntCounter,
    prefetch_hits: IntCounter,
    prefetch_misses: IntCounter,
    prefetch_unavailable: IntCounter,
    prefetch_response_bytes: IntCounter,
    prefetch_in_flight: IntGauge,
    prefetch_completed: IntCounter,
    prefetch_cancelled: IntCounter,
    prefetch_transfer_duration: Histogram,
    ring_members: IntGauge,
    ring_ejected: IntGauge,
}

impl AgentMetrics {
    fn new() -> prometheus::Result<Self> {
        let registry = Registry::new();
        Ok(Self {
            requests: int_counter(
                &registry,
                "flywheel_agent_requests_total",
                "Requests received by the routing agent.",
            )?,
            forwarded: int_counter(
                &registry,
                "flywheel_agent_forwarded_total",
                "Requests forwarded to a shard.",
            )?,
            forward_duration: histogram(
                &registry,
                "flywheel_agent_forward_duration_seconds",
                "Time from agent request receipt until response headers are ready.",
                request_buckets(),
            )?,
            forward_failures: int_counter(
                &registry,
                "flywheel_agent_forward_failures_total",
                "Shard connection failures and send timeouts.",
            )?,
            synthesized_misses: int_counter(
                &registry,
                "flywheel_agent_synthesized_misses_total",
                "Build-cache misses synthesized without a shard response.",
            )?,
            synthesized_bypasses: int_counter(
                &registry,
                "flywheel_agent_synthesized_write_bypasses_total",
                "Build-cache write successes synthesized without storing the body.",
            )?,
            unavailable: int_counter(
                &registry,
                "flywheel_agent_unavailable_total",
                "Non-build-cache requests that could not reach an owner.",
            )?,
            ejections: int_counter(
                &registry,
                "flywheel_agent_ejections_total",
                "Shard ejections from the routing ring.",
            )?,
            prefetch_requests: int_counter(
                &registry,
                "flywheel_agent_prefetch_requests_total",
                "Prefetch requests received by the routing agent.",
            )?,
            prefetch_hits: int_counter(
                &registry,
                "flywheel_agent_prefetch_hits_total",
                "Prefetch requests answered successfully by a shard.",
            )?,
            prefetch_misses: int_counter(
                &registry,
                "flywheel_agent_prefetch_misses_total",
                "Prefetch requests answered with a cache miss.",
            )?,
            prefetch_unavailable: int_counter(
                &registry,
                "flywheel_agent_prefetch_unavailable_total",
                "Prefetch requests unavailable because no shard produced a cache response.",
            )?,
            prefetch_response_bytes: int_counter(
                &registry,
                "flywheel_agent_prefetch_response_bytes_total",
                "Prefetch response bytes actually streamed through the agent.",
            )?,
            prefetch_in_flight: int_gauge(
                &registry,
                "flywheel_agent_prefetch_in_flight",
                "Prefetch response bodies currently streaming through the agent.",
            )?,
            prefetch_completed: int_counter(
                &registry,
                "flywheel_agent_prefetch_completed_total",
                "Prefetch response bodies streamed to completion.",
            )?,
            prefetch_cancelled: int_counter(
                &registry,
                "flywheel_agent_prefetch_cancelled_total",
                "Prefetch response bodies dropped before completion.",
            )?,
            prefetch_transfer_duration: histogram(
                &registry,
                "flywheel_agent_prefetch_transfer_duration_seconds",
                "Prefetch response body lifetime through the agent.",
                transfer_buckets(),
            )?,
            ring_members: int_gauge(
                &registry,
                "flywheel_agent_ring_members",
                "Members in the current routing ring.",
            )?,
            ring_ejected: int_gauge(
                &registry,
                "flywheel_agent_ring_ejected",
                "Members currently ejected from the routing ring.",
            )?,
            registry,
        })
    }

    fn encode(&self, members: usize, ejected: usize) -> prometheus::Result<Vec<u8>> {
        self.ring_members.set(saturating_i64(members as u64));
        self.ring_ejected.set(saturating_i64(ejected as u64));
        encode_registry(&self.registry)
    }
}

impl PrefetchRecorder for AgentMetrics {
    fn prefetch_started(&self) -> i64 {
        self.prefetch_requests.inc();
        self.prefetch_in_flight.inc();
        self.prefetch_in_flight.get()
    }

    /// The agent classifies a prefetch where it learns the outcome — a forwarded
    /// response, an empty ring, or a send failure — so there is nothing left to
    /// record once the body is on its way back.
    fn prefetch_response(&self, _status: u16) {}

    fn prefetch_finished(&self, duration: Duration, bytes: u64, completed: bool) {
        self.prefetch_in_flight.dec();
        self.prefetch_response_bytes.inc_by(bytes);
        self.prefetch_transfer_duration
            .observe(duration.as_secs_f64());
        if completed {
            self.prefetch_completed.inc();
        } else {
            self.prefetch_cancelled.inc();
        }
    }
}

fn request_buckets() -> Vec<f64> {
    vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    ]
}
