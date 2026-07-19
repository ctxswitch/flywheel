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
    manifest::{Manifest, REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH, merge_manifests},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use discovery::{Resolver, RingState, RingStatus};
use futures_util::{StreamExt, stream::FuturesUnordered};
use ring::RingMember;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
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
    // Telemetry only: the purpose header separates speculative prefetch traffic
    // from foreground traffic in the counters and is forwarded untouched. It never
    // affects routing, admission, authorization, or the response.
    let prefetch = request
        .headers()
        .get(REQUEST_PURPOSE_HEADER)
        .is_some_and(|value| value.as_bytes() == REQUEST_PURPOSE_PREFETCH.as_bytes());
    if prefetch {
        state.metrics.prefetch_requests.bump();
    }
    if request.uri().path() == "/channels" || request.uri().path().starts_with("/channels/") {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    }
    let routed = classify(request.method(), request.uri().path());
    let Some(position) = ring::key_position(&routed.kind, &routed.id) else {
        return StatusCode::URI_TOO_LONG.into_response();
    };
    let Some(owner) = state.ring.ring().owner(position).cloned() else {
        if prefetch {
            state.metrics.prefetch_unavailable.bump();
        }
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
            if prefetch {
                if response.status().is_success() {
                    state.metrics.prefetch_hits.bump();
                    if let Some(length) = response.content_length() {
                        state.metrics.prefetch_response_bytes.add(length);
                    }
                } else if response.status() == reqwest::StatusCode::NOT_FOUND {
                    state.metrics.prefetch_misses.bump();
                } else {
                    state.metrics.prefetch_unavailable.bump();
                }
            }
            proxied_response(response)
        }
        Err(error) => {
            tracing::warn!(%error, owner = owner.id, "forwarded request failed");
            record_send_failure(&state, &owner, &error);
            if prefetch {
                state.metrics.prefetch_unavailable.bump();
            }
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

#[derive(Deserialize)]
struct StatusQuery {
    session: Option<String>,
}

/// Without `?session=`, the operational ring response, unchanged. With a session,
/// prefetch manifest discovery: the merged manifest for that session label, and
/// never members, addresses, or completeness — the agent stays the topology
/// boundary.
async fn status(
    State(state): State<Arc<AgentState>>,
    Query(query): Query<StatusQuery>,
) -> Response {
    match query.session.as_deref() {
        Some(session) => Json(merged_session_manifest(&state, session).await).into_response(),
        None => Json(AgentStatus {
            srv: state.options.srv.clone(),
            ring: state.ring.status(),
        })
        .into_response(),
    }
}

/// Fans the session lookup out to every member of one ring snapshot concurrently
/// and merges the answers by recency. Failed members contribute nothing — an
/// empty ring or a total failure is simply an empty manifest. Runs inside the
/// handler future, so a client disconnect cancels the outstanding subrequests.
async fn merged_session_manifest(state: &Arc<AgentState>, session: &str) -> Manifest {
    let started = Instant::now();
    state.metrics.status_fanout_requests.bump();
    let members = state.ring.ring().members().to_vec();
    state
        .metrics
        .status_fanout_members_queried
        .add(members.len() as u64);
    let mut pending: FuturesUnordered<_> = members
        .iter()
        .map(|member| async move { (member, member_manifest(state, member, session).await) })
        .collect();
    let mut manifests = Vec::new();
    let mut failed = 0u64;
    while let Some((member, outcome)) = pending.next().await {
        match outcome {
            Ok(manifest) => manifests.push(manifest),
            Err(error) => {
                failed += 1;
                tracing::warn!(%error, member = member.id, "status fan-out member failed");
            }
        }
    }
    drop(pending);
    let succeeded = members.len() as u64 - failed;
    let merged = merge_manifests(manifests);
    state.metrics.status_fanout_members_succeeded.add(succeeded);
    state.metrics.status_fanout_members_failed.add(failed);
    state
        .metrics
        .status_fanout_manifest_entries
        .add(merged.entries.len() as u64);
    state
        .metrics
        .status_fanout_seconds
        .add_duration(started.elapsed());
    tracing::info!(
        members = members.len(),
        succeeded,
        failed,
        entries = merged.entries.len(),
        duration_ms = started.elapsed().as_millis() as u64,
        "session status fan-out complete"
    );
    merged
}

/// One member's local manifest. Transport failures feed the same ring health
/// accounting as ordinary forwards; a non-200 or unparseable answer counts
/// against the fan-out but not against the member's ejection streak.
async fn member_manifest(
    state: &AgentState,
    member: &RingMember,
    session: &str,
) -> anyhow::Result<Manifest> {
    let outcome = state
        .client
        .get(format!("http://{}/status", member.address))
        .query(&[("session", session)])
        .send()
        .await;
    let response = match outcome {
        Ok(response) => {
            state.ring.record_success(&member.id);
            response
        }
        Err(error) => {
            record_send_failure(state, member, &error);
            return Err(error.into());
        }
    };
    anyhow::ensure!(
        response.status().is_success(),
        "member status returned {}",
        response.status()
    );
    Ok(serde_json::from_slice(&response.bytes().await?)?)
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

#[derive(Default)]
struct Counter(AtomicU64);

impl Counter {
    fn bump(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn add(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A duration sum counter, accumulated in microseconds and rendered as seconds
/// (the repo's metrics format has no histograms).
#[derive(Default)]
struct SecondsCounter(AtomicU64);

impl SecondsCounter {
    fn add_duration(&self, duration: Duration) {
        self.0.fetch_add(
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
    fn seconds(&self) -> f64 {
        self.0.load(Ordering::Relaxed) as f64 / 1e6
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
    status_fanout_requests: Counter,
    status_fanout_seconds: SecondsCounter,
    status_fanout_members_queried: Counter,
    status_fanout_members_succeeded: Counter,
    status_fanout_members_failed: Counter,
    status_fanout_manifest_entries: Counter,
    prefetch_requests: Counter,
    prefetch_hits: Counter,
    prefetch_misses: Counter,
    prefetch_unavailable: Counter,
    prefetch_response_bytes: Counter,
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
                "# TYPE flywheel_agent_status_fanout_requests_total counter\nflywheel_agent_status_fanout_requests_total {}\n",
                "# TYPE flywheel_agent_status_fanout_seconds_total counter\nflywheel_agent_status_fanout_seconds_total {}\n",
                "# TYPE flywheel_agent_status_fanout_members_queried_total counter\nflywheel_agent_status_fanout_members_queried_total {}\n",
                "# TYPE flywheel_agent_status_fanout_members_succeeded_total counter\nflywheel_agent_status_fanout_members_succeeded_total {}\n",
                "# TYPE flywheel_agent_status_fanout_members_failed_total counter\nflywheel_agent_status_fanout_members_failed_total {}\n",
                "# TYPE flywheel_agent_status_fanout_manifest_entries_total counter\nflywheel_agent_status_fanout_manifest_entries_total {}\n",
                "# TYPE flywheel_agent_prefetch_requests_total counter\nflywheel_agent_prefetch_requests_total {}\n",
                "# TYPE flywheel_agent_prefetch_hits_total counter\nflywheel_agent_prefetch_hits_total {}\n",
                "# TYPE flywheel_agent_prefetch_misses_total counter\nflywheel_agent_prefetch_misses_total {}\n",
                "# TYPE flywheel_agent_prefetch_unavailable_total counter\nflywheel_agent_prefetch_unavailable_total {}\n",
                "# TYPE flywheel_agent_prefetch_response_bytes_total counter\nflywheel_agent_prefetch_response_bytes_total {}\n",
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
            self.status_fanout_requests.get(),
            self.status_fanout_seconds.seconds(),
            self.status_fanout_members_queried.get(),
            self.status_fanout_members_succeeded.get(),
            self.status_fanout_members_failed.get(),
            self.status_fanout_manifest_entries.get(),
            self.prefetch_requests.get(),
            self.prefetch_hits.get(),
            self.prefetch_misses.get(),
            self.prefetch_unavailable.get(),
            self.prefetch_response_bytes.get(),
            members,
            ejected,
        )
    }
}
