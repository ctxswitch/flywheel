use crate::config::Config;
use clap::{Args, Parser, Subcommand};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Parser)]
#[command(
    name = "flywheel",
    version,
    about = "Durable local-first build and package cache"
)]
pub struct Cli {
    /// Enable Flywheel debug events and per-request completion logs.
    #[arg(long, global = true)]
    pub debug: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve(Box<ServeArgs>),
    Cacheprog(CacheprogArgs),
    Agent(AgentArgs),
}

#[derive(Clone, Debug, Args)]
pub struct ServeArgs {
    #[arg(long, env = "FLYWHEEL_LISTEN", default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,
    #[arg(long, env = "FLYWHEEL_DATA_DIR", default_value = "./flywheel-data")]
    pub data_dir: PathBuf,
    #[arg(long, env = "FLYWHEEL_MAX_UPLOAD_BYTES", default_value_t = 10 * 1024 * 1024 * 1024_u64)]
    pub max_upload_bytes: u64,
    #[arg(long, env = "FLYWHEEL_DEFAULT_EXPIRY_SECONDS", default_value_t = 7 * 24 * 60 * 60_u64)]
    pub default_expiry_seconds: u64,
    #[arg(long, env = "FLYWHEEL_FOREGROUND_CONCURRENCY", default_value_t = 256)]
    pub foreground_concurrency: usize,
    #[arg(long, env = "FLYWHEEL_RESERVATION_EXTENT_BYTES", default_value_t = 64 * 1024 * 1024)]
    pub reservation_extent_bytes: u64,
    #[arg(long, env = "FLYWHEEL_LOW_WATERMARK_BYTES", default_value_t = 2 * 1024 * 1024 * 1024)]
    pub low_watermark_bytes: u64,
    #[arg(long, env = "FLYWHEEL_HIGH_WATERMARK_BYTES", default_value_t = 8 * 1024 * 1024 * 1024)]
    pub high_watermark_bytes: u64,
    #[arg(long, env = "FLYWHEEL_EMERGENCY_HEADROOM_BYTES", default_value_t = 1024 * 1024 * 1024)]
    pub emergency_headroom_bytes: u64,
    #[arg(long, env = "FLYWHEEL_BLOOM_BITS", default_value_t = 1 << 20)]
    pub bloom_bits: usize,
    #[arg(long, env = "FLYWHEEL_RECLAIM_CANDIDATE_LIMIT", default_value_t = 256)]
    pub reclaim_candidate_limit: usize,
    #[arg(long, env = "FLYWHEEL_RECLAIM_BYTE_LIMIT", default_value_t = 8 * 1024 * 1024 * 1024)]
    pub reclaim_byte_limit: u64,
    #[arg(long, env = "FLYWHEEL_ORPHAN_SCAN_LIMIT", default_value_t = 4096)]
    pub orphan_scan_limit: usize,
    #[arg(
        long,
        env = "FLYWHEEL_GO_UPSTREAM",
        default_value = "https://proxy.golang.org/"
    )]
    pub go_upstream: String,
    #[arg(
        long,
        env = "FLYWHEEL_PYTHON_UPSTREAM",
        default_value = "https://pypi.org/simple/"
    )]
    pub python_upstream: String,
    #[arg(
        long,
        env = "FLYWHEEL_NPM_UPSTREAM",
        default_value = "https://registry.npmjs.org/"
    )]
    pub npm_upstream: String,
    #[arg(
        long,
        env = "FLYWHEEL_CARGO_INDEX_UPSTREAM",
        default_value = "https://index.crates.io/"
    )]
    pub cargo_index_upstream: String,
    #[arg(
        long,
        env = "FLYWHEEL_CARGO_CRATE_UPSTREAM",
        default_value = "https://static.crates.io/crates/"
    )]
    pub cargo_crate_upstream: String,
    #[arg(
        long,
        env = "FLYWHEEL_PROXY_REVALIDATION_SECONDS",
        default_value_t = 300
    )]
    pub proxy_revalidation_seconds: u64,
    #[arg(long, env = "FLYWHEEL_PROXY_CONCURRENCY", default_value_t = 64)]
    pub proxy_concurrency: usize,
    #[arg(long, env = "FLYWHEEL_UPSTREAM_TIMEOUT_SECONDS", default_value_t = 30)]
    pub upstream_timeout_seconds: u64,
    /// Extra origin (scheme://host[:port]) every package protocol may fetch from;
    /// repeatable, or comma-separated via the environment variable.
    #[arg(
        long = "proxy-allowed-origin",
        env = "FLYWHEEL_PROXY_ALLOWED_ORIGINS",
        value_delimiter = ','
    )]
    pub proxy_allowed_origins: Vec<String>,
}

impl ServeArgs {
    /// Names every `Config` field rather than overwriting `Config::new`'s defaults, so
    /// a field added to `Config` is a compile error here instead of a silent default.
    pub fn config(self) -> Config {
        Config {
            data_dir: self.data_dir,
            max_upload_bytes: self.max_upload_bytes,
            default_expiry_seconds: self.default_expiry_seconds,
            go_upstream: self.go_upstream,
            python_upstream: self.python_upstream,
            npm_upstream: self.npm_upstream,
            cargo_index_upstream: self.cargo_index_upstream,
            cargo_crate_upstream: self.cargo_crate_upstream,
            proxy_revalidation_seconds: self.proxy_revalidation_seconds,
            proxy_concurrency: self.proxy_concurrency,
            upstream_timeout_seconds: self.upstream_timeout_seconds,
            proxy_allowed_origins: self.proxy_allowed_origins,
            foreground_concurrency: self.foreground_concurrency,
            reservation_extent_bytes: self.reservation_extent_bytes,
            low_watermark_bytes: self.low_watermark_bytes,
            high_watermark_bytes: self.high_watermark_bytes,
            emergency_headroom_bytes: self.emergency_headroom_bytes,
            bloom_bits: self.bloom_bits,
            reclaim_candidate_limit: self.reclaim_candidate_limit,
            reclaim_byte_limit: self.reclaim_byte_limit,
            orphan_scan_limit: self.orphan_scan_limit,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct AgentArgs {
    #[arg(long, env = "FLYWHEEL_AGENT_LISTEN", default_value = "127.0.0.1:9080")]
    pub listen: SocketAddr,
    /// SRV name publishing one record per ready shard, e.g.
    /// `_flywheel._tcp.flywheel-shards.cache.svc.cluster.local`.
    #[arg(long, env = "FLYWHEEL_AGENT_SRV")]
    pub srv: String,
    /// Upper bound in seconds on the DNS refresh interval regardless of answer TTL.
    #[arg(long, env = "FLYWHEEL_AGENT_REFRESH_MAX", default_value_t = 30)]
    pub refresh_max: u64,
    /// Consecutive transport failures that eject a backend from the continuum.
    #[arg(long, env = "FLYWHEEL_AGENT_FAILURE_LIMIT", default_value_t = 1)]
    pub failure_limit: u32,
    /// Seconds an ejected backend stays out of the continuum before retry.
    #[arg(long, env = "FLYWHEEL_AGENT_RETRY_TIMEOUT", default_value_t = 30)]
    pub retry_timeout: u64,
    /// Seconds to establish a backend connection before the attempt fails.
    #[arg(long, env = "FLYWHEEL_AGENT_CONNECT_TIMEOUT", default_value_t = 5)]
    pub connect_timeout: u64,
    /// Seconds a response may stall (no bytes read) before the forward fails.
    /// This bounds inactivity, not total transfer time: large bodies stream for
    /// as long as they keep making progress.
    #[arg(long, env = "FLYWHEEL_AGENT_DEADLINE", default_value_t = 60)]
    pub deadline: u64,
}

#[derive(Clone, Debug, Args)]
pub struct CacheprogArgs {
    #[arg(long, env = "FLYWHEEL_CACHEPROG_URL")]
    pub url: String,
    #[arg(long, env = "FLYWHEEL_CACHEPROG_TOKEN")]
    pub token: Option<String>,
    #[arg(long, env = "FLYWHEEL_CACHEPROG_DIR")]
    pub cache_dir: Option<PathBuf>,
    /// Use a fresh local cache for this process and delete it on exit.
    #[arg(long, env = "FLYWHEEL_CACHEPROG_EPHEMERAL_CACHE")]
    pub ephemeral_cache: bool,
    /// Label naming the prefetch manifest shared by consecutive builds; defaults to
    /// the Go module path plus GOOS/GOARCH, then the working directory.
    #[arg(long, env = "FLYWHEEL_SESSION")]
    pub session: Option<String>,
    /// Days a cached object may go untouched before close-time pruning removes it;
    /// 0 disables object pruning for deployments whose volume lifecycle already
    /// bounds growth. Defaults to the manifest retention window.
    #[arg(
        long,
        env = "FLYWHEEL_CACHEPROG_PRUNE_DAYS",
        default_value_t = crate::manifest::MANIFEST_MAX_AGE_SECONDS / (24 * 60 * 60)
    )]
    pub prune_days: u64,
    /// Bound on concurrent prefetch downloads; 0 disables the downloads while
    /// keeping the single manifest fetch that answers known actions locally.
    #[arg(
        long,
        env = "FLYWHEEL_CACHEPROG_PREFETCH_CONCURRENCY",
        default_value_t = 8
    )]
    pub prefetch_concurrency: usize,
}
