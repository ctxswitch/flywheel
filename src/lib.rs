pub mod agent;
pub mod artifact;
pub mod cache;
pub mod cacheprog;
pub mod channel;
pub mod cli;
pub mod clock;
pub mod config;
pub mod manifest;
pub mod proxy;
pub mod reference;
mod storage;
pub mod telemetry;
pub mod transport;

use cache::{
    CacheDependencies, CacheService, FreeSpace, SpaceLedger, SpacePolicy, StatvfsFreeSpace,
};
use channel::{ChannelGates, ChannelService};
use clock::{Clock, SystemClock};
use config::Config;
use proxy::ProxyService;
use std::sync::Arc;
use storage::{local::ArtifactFiles, metadata::RocksMetadata};
use telemetry::Metrics;
use transport::http::{AppState, router};

pub struct Flywheel {
    state: Arc<AppState>,
}

impl Flywheel {
    pub async fn open(config: Config) -> anyhow::Result<Self> {
        let space = default_free_space(&config);
        Self::open_internal(config, Arc::new(SystemClock), space).await
    }

    /// Opens Flywheel with an injected free-space source so tests can force the
    /// Normal/Reclaiming controller without a real filesystem.
    pub async fn open_with_space(
        config: Config,
        clock: Arc<dyn Clock>,
        free_space: Arc<dyn FreeSpace>,
    ) -> anyhow::Result<Self> {
        Self::open_internal(config, clock, free_space).await
    }

    async fn open_internal(
        config: Config,
        clock: Arc<dyn Clock>,
        free_space: Arc<dyn FreeSpace>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let metadata = Arc::new(RocksMetadata::open(config.data_dir.join("metadata")).await?);
        let files = Arc::new(
            ArtifactFiles::open(
                config.data_dir.join("artifacts"),
                config.reservation_extent_bytes,
            )
            .await?,
        );
        let channel_gates = Arc::new(ChannelGates::default());
        let channels = Arc::new(ChannelService::new(
            Arc::clone(&metadata),
            Arc::clone(&files),
            Arc::clone(&channel_gates),
        ));
        channels
            .ensure_default(config.default_expiry_seconds)
            .await?;
        channels.resume_deletions().await?;
        let metrics = Arc::new(Metrics::default());
        let space = Arc::new(SpaceLedger::new(
            free_space,
            SpacePolicy {
                low_watermark: config.low_watermark_bytes,
                high_watermark: config.high_watermark_bytes,
                emergency_headroom: config.emergency_headroom_bytes,
            },
        ));
        let cache = Arc::new(CacheService::new(CacheDependencies {
            metadata: metadata.clone(),
            files: Arc::clone(&files),
            max_upload_bytes: config.max_upload_bytes,
            space,
            channel_gates: Arc::clone(&channel_gates),
            clock: Arc::clone(&clock),
            metrics: Arc::clone(&metrics),
            bloom_bits: config.bloom_bits,
            reclaim_byte_limit: config.reclaim_byte_limit,
            orphan_scan_limit: config.orphan_scan_limit,
        }));
        cache.reconcile().await?;
        let proxy = Arc::new(ProxyService::new(
            Arc::clone(&cache),
            Arc::clone(&clock),
            &config,
        )?);
        let foreground = Arc::new(tokio::sync::Semaphore::new(config.foreground_concurrency));
        Ok(Self {
            state: Arc::new(AppState {
                config,
                cache,
                channels,
                proxy,
                metrics,
                foreground,
            }),
        })
    }

    pub fn router(&self) -> axum::Router {
        router(Arc::clone(&self.state))
    }

    pub async fn run_maintenance_once(&self) -> Result<usize, cache::CacheError> {
        let reclaimed = self
            .state
            .cache
            .run_maintenance_once(self.state.config.reclaim_candidate_limit)
            .await?;
        self.state.cache.rotate_recency();
        Ok(reclaimed)
    }

    /// Whether the space controller is reclaiming. The maintenance worker uses this to
    /// keep running bounded passes back-to-back under disk pressure.
    pub fn is_reclaiming(&self) -> bool {
        self.state.cache.is_reclaiming()
    }
}

fn default_free_space(config: &Config) -> Arc<dyn FreeSpace> {
    Arc::new(StatvfsFreeSpace::new(config.data_dir.clone()))
}
