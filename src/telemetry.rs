use crate::cache::SpaceSnapshot;
use axum::{body::Body, response::Response};
use futures_util::StreamExt;
use http::{HeaderName, HeaderValue, Request, StatusCode};
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder, core::Collector,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tower_http::request_id::{MakeRequestId, RequestId};

pub(crate) const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone, Default)]
pub(crate) struct MakeRequestUlid;

impl MakeRequestId for MakeRequestUlid {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let value = ulid::Ulid::new()
            .to_string()
            .parse::<HeaderValue>()
            .expect("canonical ULID is a valid header value");
        Some(RequestId::new(value))
    }
}

pub struct Metrics {
    registry: Registry,
    requests: IntCounter,
    http_request_duration: HistogramVec,
    hits: IntCounter,
    misses: IntCounter,
    bytes_read: IntCounter,
    bytes_written: IntCounter,
    authorization_denials: IntCounter,
    maintenance_reclaimed: IntCounter,
    maintenance_requeued: IntCounter,
    build_cache_bypasses: IntCounter,
    raw_pressure_errors: IntCounter,
    foreground_in_flight: IntGauge,
    foreground_rejections: IntCounter,
    prefetch_requests: IntCounter,
    prefetch_responses: IntCounterVec,
    prefetch_in_flight: IntGauge,
    prefetch_transfers: IntCounterVec,
    prefetch_response_bytes: IntCounter,
    prefetch_transfer_duration: Histogram,
    free_observed_bytes: IntGauge,
    reserved_bytes: IntGauge,
    committed_since_bytes: IntGauge,
}

impl Metrics {
    pub fn new(configured_foreground_limit: usize) -> prometheus::Result<Self> {
        let registry = Registry::new();
        let requests = int_counter(
            &registry,
            "flywheel_requests_total",
            "HTTP requests received by this replica.",
        )?;
        let http_request_duration = register(
            &registry,
            HistogramVec::new(
                HistogramOpts::new(
                    "flywheel_http_request_duration_seconds",
                    "Time from request receipt until response headers are ready.",
                ),
                &["method", "route", "status"],
            )?,
        )?;
        let hits = int_counter(
            &registry,
            "flywheel_artifact_hits_total",
            "Successful artifact locations.",
        )?;
        let misses = int_counter(
            &registry,
            "flywheel_artifact_misses_total",
            "Artifact locations that missed.",
        )?;
        let bytes_read = int_counter(
            &registry,
            "flywheel_bytes_read_total",
            "Logical artifact bytes attributed to successful locations.",
        )?;
        let bytes_written = int_counter(
            &registry,
            "flywheel_bytes_written_total",
            "Logical artifact bytes fully staged.",
        )?;
        let authorization_denials = int_counter(
            &registry,
            "flywheel_authorization_denials_total",
            "Protected channel authorization denials.",
        )?;
        let maintenance_reclaimed = int_counter(
            &registry,
            "flywheel_maintenance_reclaimed_total",
            "Artifacts reclaimed by maintenance.",
        )?;
        let maintenance_requeued = int_counter(
            &registry,
            "flywheel_maintenance_requeued_total",
            "Recently used artifacts assigned a new eviction deadline.",
        )?;
        let build_cache_bypasses = int_counter(
            &registry,
            "flywheel_build_cache_bypasses_total",
            "Build-cache writes accepted without storage under disk pressure.",
        )?;
        let raw_pressure_errors = int_counter(
            &registry,
            "flywheel_raw_pressure_errors_total",
            "Raw artifact and CAS writes rejected under disk pressure.",
        )?;
        let foreground_limit = int_gauge(
            &registry,
            "flywheel_foreground_concurrency_limit",
            "Configured shard foreground concurrency limit.",
        )?;
        foreground_limit.set(saturating_i64(configured_foreground_limit as u64));
        let foreground_in_flight = int_gauge(
            &registry,
            "flywheel_foreground_in_flight",
            "Shard foreground operations and response streams currently admitted.",
        )?;
        let foreground_rejections = int_counter(
            &registry,
            "flywheel_foreground_rejections_total",
            "Requests rejected because the shard foreground limit was exhausted.",
        )?;
        let prefetch_requests = int_counter(
            &registry,
            "flywheel_prefetch_requests_total",
            "Prefetch object requests received by this shard.",
        )?;
        let prefetch_responses = int_counter_vec(
            &registry,
            "flywheel_prefetch_responses_total",
            "Prefetch responses classified at response headers.",
            &["outcome"],
        )?;
        let prefetch_in_flight = int_gauge(
            &registry,
            "flywheel_prefetch_in_flight",
            "Prefetch response bodies currently streaming from this shard.",
        )?;
        let prefetch_transfers = int_counter_vec(
            &registry,
            "flywheel_prefetch_transfers_total",
            "Prefetch response bodies by terminal outcome.",
            &["outcome"],
        )?;
        let prefetch_response_bytes = int_counter(
            &registry,
            "flywheel_prefetch_response_bytes_total",
            "Prefetch response body bytes actually streamed by this shard.",
        )?;
        let prefetch_transfer_duration = histogram(
            &registry,
            "flywheel_prefetch_transfer_duration_seconds",
            "Prefetch response body lifetime, including transfers cancelled by the client.",
            transfer_buckets(),
        )?;
        let free_observed_bytes = int_gauge(
            &registry,
            "flywheel_free_observed_bytes",
            "Last successful filesystem free-space observation.",
        )?;
        let reserved_bytes = int_gauge(
            &registry,
            "flywheel_reserved_bytes",
            "Capacity held by in-flight artifact writes.",
        )?;
        let committed_since_bytes = int_gauge(
            &registry,
            "flywheel_committed_since_bytes",
            "Committed bytes not yet included in the free-space observation.",
        )?;

        Ok(Self {
            registry,
            requests,
            http_request_duration,
            hits,
            misses,
            bytes_read,
            bytes_written,
            authorization_denials,
            maintenance_reclaimed,
            maintenance_requeued,
            build_cache_bypasses,
            raw_pressure_errors,
            foreground_in_flight,
            foreground_rejections,
            prefetch_requests,
            prefetch_responses,
            prefetch_in_flight,
            prefetch_transfers,
            prefetch_response_bytes,
            prefetch_transfer_duration,
            free_observed_bytes,
            reserved_bytes,
            committed_since_bytes,
        })
    }

    pub(crate) fn request(&self) {
        self.requests.inc();
    }

    pub(crate) fn observe_http_request(
        &self,
        method: &str,
        route: &str,
        status: StatusCode,
        duration: Duration,
    ) {
        self.http_request_duration
            .with_label_values(&[method, route, status.as_str()])
            .observe(duration.as_secs_f64());
    }

    pub(crate) fn hit(&self, bytes: u64) {
        self.hits.inc();
        self.bytes_read.inc_by(bytes);
    }

    pub(crate) fn miss(&self) {
        self.misses.inc();
    }

    pub(crate) fn written(&self, bytes: u64) {
        self.bytes_written.inc_by(bytes);
    }

    pub(crate) fn authorization_denial(&self) {
        self.authorization_denials.inc();
    }

    pub(crate) fn reclaimed(&self, count: u64) {
        self.maintenance_reclaimed.inc_by(count);
    }

    pub(crate) fn requeued(&self) {
        self.maintenance_requeued.inc();
    }

    pub(crate) fn build_cache_bypass(&self) {
        self.build_cache_bypasses.inc();
    }

    pub(crate) fn raw_pressure_error(&self) {
        self.raw_pressure_errors.inc();
    }

    pub(crate) fn foreground_acquired(&self) {
        self.foreground_in_flight.inc();
    }

    pub(crate) fn foreground_released(&self) {
        self.foreground_in_flight.dec();
    }

    pub(crate) fn foreground_rejected(&self) {
        self.foreground_rejections.inc();
    }

    pub fn record_space(&self, space: SpaceSnapshot) {
        self.free_observed_bytes
            .set(saturating_i64(space.free_observed));
        self.reserved_bytes.set(saturating_i64(space.reserved));
        self.committed_since_bytes
            .set(saturating_i64(space.committed_since));
    }

    pub fn encode(&self) -> prometheus::Result<Vec<u8>> {
        encode_registry(&self.registry)
    }
}

/// How a prefetch body is counted. The shard and the routing agent keep separate
/// registries, so an observation records against whichever handle started it.
pub(crate) trait PrefetchRecorder {
    /// Counts a started prefetch and returns the resulting in-flight count.
    fn prefetch_started(&self) -> i64;
    fn prefetch_response(&self, status: u16);
    fn prefetch_finished(&self, duration: Duration, bytes: u64, completed: bool);
}

impl PrefetchRecorder for Metrics {
    fn prefetch_started(&self) -> i64 {
        self.prefetch_requests.inc();
        self.prefetch_in_flight.inc();
        self.prefetch_in_flight.get()
    }

    fn prefetch_response(&self, status: u16) {
        let outcome = if (200..300).contains(&status) {
            "hit"
        } else if status == 404 {
            "miss"
        } else {
            "unavailable"
        };
        self.prefetch_responses.with_label_values(&[outcome]).inc();
    }

    fn prefetch_finished(&self, duration: Duration, bytes: u64, completed: bool) {
        self.prefetch_in_flight.dec();
        self.prefetch_response_bytes.inc_by(bytes);
        self.prefetch_transfer_duration
            .observe(duration.as_secs_f64());
        self.prefetch_transfers
            .with_label_values(&[if completed { "completed" } else { "cancelled" }])
            .inc();
    }
}

/// One prefetch response body, from its headers until the body is dropped. The
/// terminal counters are recorded in `Drop` so a client that abandons the transfer
/// is counted exactly like one that finishes it.
pub(crate) struct PrefetchObservation<R: PrefetchRecorder> {
    recorder: Arc<R>,
    started: Instant,
    /// The matched route, where the caller has one; the agent forwards by ring
    /// position and never matches a route.
    route: Option<String>,
    session: Option<String>,
    configured_concurrency: Option<usize>,
    status: u16,
    bytes: u64,
    completed: bool,
}

impl<R: PrefetchRecorder> PrefetchObservation<R> {
    pub(crate) fn start(
        recorder: Arc<R>,
        route: Option<String>,
        session: Option<String>,
        configured_concurrency: Option<usize>,
    ) -> Self {
        let in_flight = recorder.prefetch_started();
        tracing::debug!(
            route = route.as_deref().unwrap_or(""),
            session_id = session.as_deref().unwrap_or(""),
            configured_concurrency,
            in_flight,
            "prefetch request started"
        );
        Self {
            recorder,
            started: Instant::now(),
            route,
            session,
            configured_concurrency,
            status: 0,
            bytes: 0,
            completed: false,
        }
    }

    fn set_status(&mut self, status: u16) {
        self.status = status;
        self.recorder.prefetch_response(status);
    }

    fn add_bytes(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes as u64);
    }
}

impl<R: PrefetchRecorder> Drop for PrefetchObservation<R> {
    fn drop(&mut self) {
        let duration = self.started.elapsed();
        self.recorder
            .prefetch_finished(duration, self.bytes, self.completed);
        tracing::debug!(
            route = self.route.as_deref().unwrap_or(""),
            session_id = self.session.as_deref().unwrap_or(""),
            configured_concurrency = self.configured_concurrency,
            status = self.status,
            bytes = self.bytes,
            duration_ms = duration.as_millis() as u64,
            completed = self.completed,
            "prefetch request finished"
        );
    }
}

/// Wraps a prefetch response body so the observation follows the bytes the client
/// actually receives. The observation lives in the stream state and is dropped with
/// it, whether the stream ends or the client goes away.
pub(crate) fn observe_prefetch_body<R>(
    response: Response,
    mut observation: PrefetchObservation<R>,
) -> Response
where
    R: PrefetchRecorder + Send + Sync + 'static,
{
    observation.set_status(response.status().as_u16());
    let (parts, body) = response.into_parts();
    let stream = Box::pin(body.into_data_stream());
    let observed = futures_util::stream::unfold(
        (stream, observation),
        |(mut stream, mut observation)| async move {
            match stream.next().await {
                Some(Ok(bytes)) => {
                    observation.add_bytes(bytes.len());
                    Some((Ok(bytes), (stream, observation)))
                }
                Some(Err(error)) => Some((Err(error), (stream, observation))),
                None => {
                    observation.completed = true;
                    None
                }
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(observed))
}

pub(crate) fn encode_registry(registry: &Registry) -> prometheus::Result<Vec<u8>> {
    let mut body = Vec::new();
    TextEncoder::new().encode(&registry.gather(), &mut body)?;
    Ok(body)
}

fn register<T>(registry: &Registry, metric: T) -> prometheus::Result<T>
where
    T: Collector + Clone + 'static,
{
    registry.register(Box::new(metric.clone()))?;
    Ok(metric)
}

pub(crate) fn int_counter(
    registry: &Registry,
    name: &str,
    help: &str,
) -> prometheus::Result<IntCounter> {
    register(registry, IntCounter::with_opts(Opts::new(name, help))?)
}

fn int_counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> prometheus::Result<IntCounterVec> {
    register(registry, IntCounterVec::new(Opts::new(name, help), labels)?)
}

pub(crate) fn int_gauge(
    registry: &Registry,
    name: &str,
    help: &str,
) -> prometheus::Result<IntGauge> {
    register(registry, IntGauge::with_opts(Opts::new(name, help))?)
}

pub(crate) fn histogram(
    registry: &Registry,
    name: &str,
    help: &str,
    buckets: Vec<f64>,
) -> prometheus::Result<Histogram> {
    register(
        registry,
        Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))?,
    )
}

pub(crate) fn transfer_buckets() -> Vec<f64> {
    vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
}

pub(crate) fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
