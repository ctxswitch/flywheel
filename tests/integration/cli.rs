use clap::Parser;
use flywheel::cli::{Cli, Command};
use std::path::Path;

#[test]
fn exposes_agent_command() {
    let agent = Cli::try_parse_from([
        "flywheel",
        "agent",
        "--srv",
        "_flywheel._tcp.flywheel-shards.cache.svc.cluster.local",
    ])
    .expect("agent command parses");
    let Command::Agent(arguments) = agent.command else {
        panic!("expected the agent command");
    };
    assert_eq!(arguments.listen.to_string(), "127.0.0.1:9080");
    assert_eq!(arguments.refresh_max, 30);
    assert_eq!(arguments.failure_limit, 1);
    assert_eq!(arguments.retry_timeout, 30);
    assert_eq!(arguments.connect_timeout, 5);
    assert_eq!(arguments.deadline, 60);

    assert!(
        Cli::try_parse_from(["flywheel", "agent"]).is_err(),
        "the SRV name is required"
    );
}

#[test]
fn exposes_serve_and_cacheprog_commands() {
    let serve = Cli::try_parse_from(["flywheel", "serve", "--data-dir", "/tmp/flywheel-test"])
        .expect("serve command parses");
    assert!(matches!(serve.command, Command::Serve(_)));

    let cacheprog = Cli::try_parse_from([
        "flywheel",
        "cacheprog",
        "--url",
        "http://127.0.0.1:9999/build-cache/http/",
    ])
    .expect("cacheprog command parses");
    assert!(matches!(cacheprog.command, Command::Cacheprog(_)));
}

/// Every serve flag carries a value distinct from every other flag's, so a field wired
/// to the wrong argument — the failure mode the struct literal in `ServeArgs::config`
/// cannot catch — surfaces as a mismatch rather than a coincidence.
#[test]
fn serve_arguments_map_onto_the_configuration() {
    let serve = Cli::try_parse_from([
        "flywheel",
        "serve",
        "--data-dir",
        "/tmp/flywheel-mapping",
        "--max-upload-bytes",
        "11",
        "--default-expiry-seconds",
        "12",
        "--foreground-concurrency",
        "13",
        "--reservation-extent-bytes",
        "14",
        "--low-watermark-bytes",
        "15",
        "--high-watermark-bytes",
        "16",
        "--emergency-headroom-bytes",
        "17",
        "--bloom-bits",
        "18",
        "--reclaim-candidate-limit",
        "19",
        "--reclaim-byte-limit",
        "20",
        "--orphan-scan-limit",
        "21",
        "--proxy-revalidation-seconds",
        "22",
        "--proxy-concurrency",
        "23",
        "--upstream-timeout-seconds",
        "24",
        "--go-upstream",
        "https://go.example/",
        "--python-upstream",
        "https://python.example/",
        "--npm-upstream",
        "https://npm.example/",
        "--cargo-index-upstream",
        "https://cargo-index.example/",
        "--cargo-crate-upstream",
        "https://cargo-crate.example/",
        "--proxy-allowed-origin",
        "https://mirror.example",
    ])
    .expect("serve command parses");
    let Command::Serve(arguments) = serve.command else {
        panic!("expected the serve command");
    };
    let config = arguments.config();
    config.validate().expect("mapped configuration is valid");

    assert_eq!(config.data_dir, Path::new("/tmp/flywheel-mapping"));
    assert_eq!(config.max_upload_bytes, 11);
    assert_eq!(config.default_expiry_seconds, 12);
    assert_eq!(config.foreground_concurrency, 13);
    assert_eq!(config.reservation_extent_bytes, 14);
    assert_eq!(config.low_watermark_bytes, 15);
    assert_eq!(config.high_watermark_bytes, 16);
    assert_eq!(config.emergency_headroom_bytes, 17);
    assert_eq!(config.bloom_bits, 18);
    assert_eq!(config.reclaim_candidate_limit, 19);
    assert_eq!(config.reclaim_byte_limit, 20);
    assert_eq!(config.orphan_scan_limit, 21);
    assert_eq!(config.proxy_revalidation_seconds, 22);
    assert_eq!(config.proxy_concurrency, 23);
    assert_eq!(config.upstream_timeout_seconds, 24);
    assert_eq!(config.go_upstream, "https://go.example/");
    assert_eq!(config.python_upstream, "https://python.example/");
    assert_eq!(config.npm_upstream, "https://npm.example/");
    assert_eq!(config.cargo_index_upstream, "https://cargo-index.example/");
    assert_eq!(config.cargo_crate_upstream, "https://cargo-crate.example/");
    assert_eq!(config.proxy_allowed_origins, ["https://mirror.example"]);
}
