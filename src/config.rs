use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Config {
    pub data_dir: PathBuf,
    pub max_upload_bytes: u64,
    pub default_expiry_seconds: u64,
    pub go_upstream: String,
    pub python_upstream: String,
    pub npm_upstream: String,
    pub cargo_index_upstream: String,
    pub cargo_crate_upstream: String,
    pub proxy_revalidation_seconds: u64,
    pub proxy_concurrency: usize,
    pub upstream_timeout_seconds: u64,
    /// Extra origins (scheme://host[:port]) every package protocol may fetch from,
    /// beyond each protocol's configured upstream origin and its known public
    /// download origins.
    pub proxy_allowed_origins: Vec<String>,
    pub foreground_concurrency: usize,
    pub reservation_extent_bytes: u64,
    pub low_watermark_bytes: u64,
    pub high_watermark_bytes: u64,
    pub emergency_headroom_bytes: u64,
    pub bloom_bits: usize,
    pub reclaim_candidate_limit: usize,
    pub reclaim_byte_limit: u64,
    pub orphan_scan_limit: usize,
}

impl Config {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            max_upload_bytes: 10 * 1024 * 1024 * 1024,
            default_expiry_seconds: 7 * 24 * 60 * 60,
            go_upstream: "https://proxy.golang.org/".to_owned(),
            python_upstream: "https://pypi.org/simple/".to_owned(),
            npm_upstream: "https://registry.npmjs.org/".to_owned(),
            cargo_index_upstream: "https://index.crates.io/".to_owned(),
            cargo_crate_upstream: "https://static.crates.io/crates/".to_owned(),
            proxy_revalidation_seconds: 300,
            proxy_concurrency: 64,
            upstream_timeout_seconds: 30,
            proxy_allowed_origins: Vec::new(),
            foreground_concurrency: 256,
            reservation_extent_bytes: 64 * 1024 * 1024,
            low_watermark_bytes: 2 * 1024 * 1024 * 1024,
            high_watermark_bytes: 8 * 1024 * 1024 * 1024,
            emergency_headroom_bytes: 1024 * 1024 * 1024,
            bloom_bits: 1 << 20,
            reclaim_candidate_limit: 256,
            reclaim_byte_limit: 8 * 1024 * 1024 * 1024,
            orphan_scan_limit: 4096,
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_upload_bytes > 0,
            "max upload bytes must be positive"
        );
        anyhow::ensure!(
            self.default_expiry_seconds > 0,
            "default expiry seconds must be positive"
        );
        anyhow::ensure!(
            self.proxy_concurrency > 0,
            "proxy concurrency must be positive"
        );
        anyhow::ensure!(
            self.upstream_timeout_seconds > 0,
            "upstream timeout must be positive"
        );
        anyhow::ensure!(
            self.foreground_concurrency > 0,
            "foreground concurrency must be positive"
        );
        anyhow::ensure!(
            self.reservation_extent_bytes > 0,
            "reservation extent must be positive"
        );
        anyhow::ensure!(
            self.high_watermark_bytes >= self.low_watermark_bytes,
            "high watermark must be at least the low watermark"
        );
        anyhow::ensure!(self.bloom_bits > 0, "bloom filter bits must be positive");
        anyhow::ensure!(
            self.reclaim_candidate_limit > 0,
            "reclaim candidate limit must be positive"
        );
        for (name, value) in [
            ("go upstream", &self.go_upstream),
            ("python upstream", &self.python_upstream),
            ("npm upstream", &self.npm_upstream),
            ("cargo index upstream", &self.cargo_index_upstream),
            ("cargo crate upstream", &self.cargo_crate_upstream),
        ] {
            url::Url::parse(value)
                .map_err(|error| anyhow::anyhow!("{name} URL is invalid: {error}"))?;
        }
        for entry in &self.proxy_allowed_origins {
            let origin = url::Url::parse(entry)
                .map_err(|error| anyhow::anyhow!("allowed origin {entry:?} is invalid: {error}"))?;
            anyhow::ensure!(
                matches!(origin.scheme(), "http" | "https") && origin.has_host(),
                "allowed origin {entry:?} must be an http(s) URL with a host"
            );
        }
        Ok(())
    }
}
