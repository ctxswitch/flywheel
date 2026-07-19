//! End-to-end agent tests: a deterministic resolver, real Flywheel backends on
//! ephemeral ports, and the agent forwarding between them over real TCP.

use flywheel::{
    Flywheel,
    agent::{
        Agent, AgentOptions,
        discovery::{Resolver, SrvRecord, SrvSnapshot},
        ring::{Ring, RingMember, key_position},
    },
    clock::SystemClock,
    config::Config,
    manifest::{
        MANIFEST_VERSION, Manifest, ManifestEntry, REQUEST_PURPOSE_HEADER,
        REQUEST_PURPOSE_PREFETCH, manifest_key,
    },
};
use sha2::{Digest as _, Sha256};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::net::TcpListener;

struct Backend {
    address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
    _directory: TempDir,
}

impl Backend {
    async fn spawn() -> Self {
        let directory = TempDir::new().unwrap();
        let flywheel = Flywheel::open(Config::new(directory.path())).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = flywheel.router();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            address,
            task,
            _directory: directory,
        }
    }

    /// Abrupt scale-down: the listener closes and every new connection is refused.
    async fn kill(&self) {
        self.task.abort();
        while let Ok(_connection) = tokio::net::TcpStream::connect(self.address).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Deterministic SRV source: membership is whatever the test put in it.
#[derive(Default)]
struct TestResolver {
    members: Mutex<Vec<(String, SocketAddr)>>,
}

impl TestResolver {
    fn set(&self, members: Vec<(String, SocketAddr)>) {
        *self.members.lock().unwrap() = members;
    }
}

#[async_trait::async_trait]
impl Resolver for TestResolver {
    async fn srv(&self, _name: &str) -> anyhow::Result<SrvSnapshot> {
        Ok(SrvSnapshot {
            records: self
                .members
                .lock()
                .unwrap()
                .iter()
                .map(|(id, address)| SrvRecord {
                    target: id.clone(),
                    port: address.port(),
                })
                .collect(),
            ttl: Duration::from_secs(30),
        })
    }

    async fn ips(&self, target: &str) -> anyhow::Result<Vec<IpAddr>> {
        self.members
            .lock()
            .unwrap()
            .iter()
            .find(|(id, _)| id == target)
            .map(|(_, address)| vec![address.ip()])
            .ok_or_else(|| anyhow::anyhow!("unknown target {target}"))
    }
}

struct AgentHarness {
    base: String,
    agent: Agent,
    _task: tokio::task::JoinHandle<()>,
}

async fn spawn_agent(resolver: Arc<TestResolver>) -> AgentHarness {
    spawn_agent_with_failure_limit(resolver, 1).await
}

async fn spawn_agent_with_failure_limit(
    resolver: Arc<TestResolver>,
    failure_limit: u32,
) -> AgentHarness {
    let agent = Agent::new(
        AgentOptions {
            srv: "_flywheel._tcp.test.svc.cluster.local".to_owned(),
            refresh_max: Duration::from_secs(30),
            failure_limit,
            retry_timeout: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(2),
            deadline: Duration::from_secs(10),
        },
        resolver,
        Arc::new(SystemClock),
    )
    .unwrap();
    agent.refresh_once().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = agent.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    AgentHarness {
        base: format!("http://{address}"),
        agent,
        _task: task,
    }
}

async fn member_status(client: &reqwest::Client, harness: &AgentHarness) -> serde_json::Value {
    client
        .get(format!("{}/status", harness.base))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["members"][0]
        .clone()
}

/// SRV entries with stable identities: the ordinal names the backend itself, the
/// way a StatefulSet DNS name survives membership churn, so a subset snapshot
/// must not renumber the survivors.
fn members(entries: &[(usize, &Backend)]) -> Vec<(String, SocketAddr)> {
    entries
        .iter()
        .map(|(ordinal, backend)| {
            (
                format!("flywheel-{ordinal}.shards.test.svc"),
                backend.address,
            )
        })
        .collect()
}

fn all_members(backends: &[Backend]) -> Vec<(String, SocketAddr)> {
    members(
        &backends
            .iter()
            .enumerate()
            .collect::<Vec<(usize, &Backend)>>(),
    )
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_zstd().build().unwrap()
}

fn digest_of(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

async fn backend_has_key(client: &reqwest::Client, backend: &Backend, key: &str) -> bool {
    client
        .get(format!("http://{}/build-cache/http/{key}", backend.address))
        .send()
        .await
        .unwrap()
        .status()
        .is_success()
}

#[tokio::test]
async fn same_key_reaches_the_same_backend_for_get_head_and_put() {
    let backends = [
        Backend::spawn().await,
        Backend::spawn().await,
        Backend::spawn().await,
    ];
    let resolver = Arc::new(TestResolver::default());
    resolver.set(all_members(&backends));
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();

    let body = b"one build output".to_vec();
    let digest = digest_of(&body);
    let url = format!("{}/artifacts/sha256/{digest}", harness.base);
    let put = client.put(&url).body(body.clone()).send().await.unwrap();
    assert_eq!(put.status(), reqwest::StatusCode::CREATED);

    let get = client.get(&url).send().await.unwrap();
    assert_eq!(get.status(), reqwest::StatusCode::OK);
    assert_eq!(get.bytes().await.unwrap().as_ref(), body.as_slice());
    let head = client.head(&url).send().await.unwrap();
    assert_eq!(head.status(), reqwest::StatusCode::OK);

    // Exactly one backend owns the object: placement, not replication.
    let mut holders = 0;
    for backend in &backends {
        let direct = client
            .get(format!(
                "http://{}/artifacts/sha256/{digest}",
                backend.address
            ))
            .send()
            .await
            .unwrap();
        if direct.status().is_success() {
            holders += 1;
        }
    }
    assert_eq!(holders, 1);

    // The Bazel CAS route shares the artifact's placement, so the ring sends it
    // to the one backend that stores the digest.
    let cas = client
        .get(format!("{}/build-cache/bazel/cas/{digest}", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(cas.status(), reqwest::StatusCode::OK);
    assert_eq!(cas.bytes().await.unwrap().as_ref(), body.as_slice());
}

#[tokio::test]
async fn dead_backend_fails_open_is_ejected_and_the_ring_rebuilds() {
    let backends = [
        Backend::spawn().await,
        Backend::spawn().await,
        Backend::spawn().await,
    ];
    let resolver = Arc::new(TestResolver::default());
    resolver.set(all_members(&backends));
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();

    // Spread keys through the agent until every backend owns at least one, so a
    // key owned by the victim definitely exists.
    let mut keys_by_backend: Vec<Vec<String>> = vec![Vec::new(), Vec::new(), Vec::new()];
    for sample in 0..64 {
        let key = format!("build-key-{sample}");
        let put = client
            .put(format!("{}/build-cache/http/{key}", harness.base))
            .body(format!("payload-{sample}"))
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), reqwest::StatusCode::OK);
        for (ordinal, backend) in backends.iter().enumerate() {
            if backend_has_key(&client, backend, &key).await {
                keys_by_backend[ordinal].push(key.clone());
            }
        }
        if keys_by_backend.iter().all(|keys| !keys.is_empty()) {
            break;
        }
    }
    let victim = 0;
    let victim_key = keys_by_backend[victim].first().expect("victim owns a key");
    backends[victim].kill().await;

    // First touch of the dead owner: the read fails open as a miss, is not
    // replayed, and ejects the member.
    let miss = client
        .get(format!("{}/build-cache/http/{victim_key}", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(miss.status(), reqwest::StatusCode::NOT_FOUND);
    let status: serde_json::Value = client
        .get(format!("{}/status", harness.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let reported = status["members"].as_array().unwrap();
    assert_eq!(reported.len(), 3);
    let ejected: Vec<_> = reported
        .iter()
        .filter(|member| member["ejected"].as_bool().unwrap())
        .collect();
    assert_eq!(ejected.len(), 1);
    assert_eq!(ejected[0]["id"], "flywheel-0.shards.test.svc");
    assert!(ejected[0]["next_retry"].as_u64().unwrap() > 0);

    // A write whose owner is already ejected lands on the rebuilt N-1 ring and
    // is really stored there; the following read returns it.
    let put = client
        .put(format!("{}/build-cache/http/{victim_key}", harness.base))
        .body("rewritten after scale-down")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), reqwest::StatusCode::OK);
    let get = client
        .get(format!("{}/build-cache/http/{victim_key}", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), reqwest::StatusCode::OK);
    assert_eq!(
        get.bytes().await.unwrap().as_ref(),
        b"rewritten after scale-down"
    );

    // A write bypass for a not-yet-ejected dead owner: re-admit the victim by
    // refreshing membership without it and then re-adding it, so the next touch
    // is a fresh transport failure on a write.
    let alive: Vec<&Backend> = vec![&backends[1], &backends[2]];
    resolver.set(members(&[(1, &backends[1]), (2, &backends[2])]));
    harness.agent.refresh_once().await.unwrap();
    resolver.set(all_members(&backends));
    harness.agent.refresh_once().await.unwrap();
    let bypass = client
        .put(format!("{}/build-cache/http/{victim_key}", harness.base))
        .body("never stored")
        .send()
        .await
        .unwrap();
    assert_eq!(bypass.status(), reqwest::StatusCode::OK);
    // The bypassed body was neither stored by the dead owner nor replayed to a
    // live member: whatever a live member holds is the earlier rebuilt-ring write.
    for backend in &alive {
        let direct = client
            .get(format!(
                "http://{}/build-cache/http/{victim_key}",
                backend.address
            ))
            .send()
            .await
            .unwrap();
        if direct.status().is_success() {
            assert_eq!(
                direct.bytes().await.unwrap().as_ref(),
                b"rewritten after scale-down"
            );
        }
    }
}

fn test_ring(entries: &[(String, SocketAddr)]) -> Ring {
    Ring::new(
        entries
            .iter()
            .map(|(id, address)| RingMember {
                id: id.clone(),
                address: *address,
            })
            .collect(),
    )
}

#[tokio::test]
async fn reference_binds_even_when_its_artifact_lives_on_another_shard() {
    let backends = [
        Backend::spawn().await,
        Backend::spawn().await,
        Backend::spawn().await,
    ];
    let membership = all_members(&backends);
    let resolver = Arc::new(TestResolver::default());
    resolver.set(membership.clone());
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();

    let body = b"cross-shard referenced".to_vec();
    let digest = digest_of(&body);
    let put = client
        .put(format!("{}/artifacts/sha256/{digest}", harness.base))
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), reqwest::StatusCode::CREATED);

    // Deliberately pick a reference name whose ring owner is a different shard
    // than the artifact's, so the bind cannot see the artifact locally.
    let ring = test_ring(&membership);
    let artifact_owner = ring
        .owner(key_position("artifact", &digest).unwrap())
        .unwrap()
        .id
        .clone();
    let name = (0..)
        .map(|attempt| format!("toolchain-{attempt}"))
        .find(|name| {
            ring.owner(key_position("reference", name).unwrap())
                .unwrap()
                .id
                != artifact_owner
        })
        .unwrap();

    let binding = serde_json::json!({ "algorithm": "sha256", "digest": digest });
    let bind = client
        .put(format!("{}/references/{name}", harness.base))
        .json(&binding)
        .send()
        .await
        .unwrap();
    assert_eq!(bind.status(), reqwest::StatusCode::NO_CONTENT);

    let resolved: serde_json::Value = client
        .get(format!("{}/references/{name}", harness.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resolved, binding);
    let fetched = client
        .get(format!("{}/artifacts/sha256/{digest}", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(fetched.status(), reqwest::StatusCode::OK);
    assert_eq!(fetched.bytes().await.unwrap().as_ref(), body.as_slice());
}

#[tokio::test]
async fn empty_ring_fails_open_for_build_cache_and_unavailable_for_the_rest() {
    let resolver = Arc::new(TestResolver::default());
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();

    let ready = client
        .get(format!("{}/health/ready", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), reqwest::StatusCode::OK);

    let read = client
        .get(format!("{}/build-cache/http/some-key", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), reqwest::StatusCode::NOT_FOUND);
    let write = client
        .put(format!("{}/build-cache/http/some-key", harness.base))
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(write.status(), reqwest::StatusCode::OK);

    let digest = digest_of(b"anything");
    let artifact = client
        .get(format!("{}/artifacts/sha256/{digest}", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(artifact.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let reference = client
        .get(format!("{}/references/latest", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(reference.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    let status: serde_json::Value = client
        .get(format!("{}/status", harness.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["members"].as_array().unwrap().len(), 0);
    assert_eq!(status["fingerprint"].as_str().unwrap().len(), 64);
}

#[tokio::test]
async fn agent_rejects_channel_prefixed_routes_until_shared_control_exists() {
    let resolver = Arc::new(TestResolver::default());
    let harness = spawn_agent(resolver).await;
    let response = client()
        .get(format!(
            "{}/channels/00000000000000000000000000/artifacts/sha256/{}",
            harness.base,
            digest_of(b"unsupported")
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn agents_with_divergent_membership_views_both_serve_safely() {
    let backends = [
        Backend::spawn().await,
        Backend::spawn().await,
        Backend::spawn().await,
    ];
    let full = Arc::new(TestResolver::default());
    full.set(all_members(&backends));
    let stale = Arc::new(TestResolver::default());
    stale.set(members(&[(0, &backends[0]), (1, &backends[1])]));
    let agent_full = spawn_agent(full).await;
    let agent_stale = spawn_agent(stale).await;
    let client = client();

    // Writes through the converged agent, reads through the stale one: a key may
    // land on a shard the stale view does not own, so the only acceptable
    // outcomes are a hit or a clean miss — never an error.
    for sample in 0..24 {
        let key = format!("divergent-{sample}");
        let put = client
            .put(format!("{}/build-cache/http/{key}", agent_full.base))
            .body(format!("payload-{sample}"))
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), reqwest::StatusCode::OK);
        let read = client
            .get(format!("{}/build-cache/http/{key}", agent_stale.base))
            .send()
            .await
            .unwrap();
        assert!(
            read.status() == reqwest::StatusCode::OK
                || read.status() == reqwest::StatusCode::NOT_FOUND,
            "divergent read returned {}",
            read.status()
        );
    }

    // Each agent is internally consistent: its own writes are its own hits.
    for (harness, key) in [
        (&agent_full, "own-write-full"),
        (&agent_stale, "own-write-stale"),
    ] {
        let put = client
            .put(format!("{}/build-cache/http/{key}", harness.base))
            .body("own payload")
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), reqwest::StatusCode::OK);
        let get = client
            .get(format!("{}/build-cache/http/{key}", harness.base))
            .send()
            .await
            .unwrap();
        assert_eq!(get.status(), reqwest::StatusCode::OK);
        assert_eq!(get.bytes().await.unwrap().as_ref(), b"own payload");
    }
}

fn manifest_of(entries: &[(&str, &str, u64)]) -> Manifest {
    Manifest {
        version: MANIFEST_VERSION,
        entries: entries
            .iter()
            .map(|(action, output, last_seen)| {
                (
                    (*action).to_owned(),
                    ManifestEntry {
                        output: (*output).to_owned(),
                        size: 1,
                        last_seen: *last_seen,
                    },
                )
            })
            .collect(),
    }
}

/// Stores a shard-local manifest generation the way cacheprog's finalize does:
/// directly on one member, under the shared session key.
async fn put_manifest(
    client: &reqwest::Client,
    backend: &Backend,
    session: &str,
    manifest: &Manifest,
) {
    let response = client
        .put(format!(
            "http://{}/build-cache/http/{}",
            backend.address,
            manifest_key(session)
        ))
        .body(serde_json::to_vec(manifest).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

async fn merged_manifest(
    client: &reqwest::Client,
    harness: &AgentHarness,
    session: &str,
) -> Manifest {
    let response = client
        .get(format!("{}/status", harness.base))
        .query(&[("session", session)])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.json().await.unwrap()
}

async fn agent_metrics(client: &reqwest::Client, harness: &AgentHarness) -> String {
    client
        .get(format!("{}/metrics", harness.base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

/// Ring membership drifted between builds, so different members hold different
/// manifest generations. The sessioned status fan-out must query every member of
/// the snapshot and merge per Go action by recency, while the bare status
/// response stays the operational ring view.
#[tokio::test]
async fn sessioned_status_fans_out_to_all_members_and_merges_by_recency() {
    let backends = [
        Backend::spawn().await,
        Backend::spawn().await,
        Backend::spawn().await,
    ];
    let resolver = Arc::new(TestResolver::default());
    resolver.set(all_members(&backends));
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();
    // Spaces and slashes exercise the URL-encoded raw-label contract.
    let session = "ci example.com/widget linux/amd64";

    put_manifest(
        &client,
        &backends[0],
        session,
        &manifest_of(&[("shared", "aa", 10), ("only-first", "bb", 5)]),
    )
    .await;
    put_manifest(
        &client,
        &backends[1],
        session,
        &manifest_of(&[("shared", "cc", 20), ("only-second", "dd", 7)]),
    )
    .await;

    let merged = merged_manifest(&client, &harness, session).await;
    assert_eq!(
        merged,
        manifest_of(&[
            ("shared", "cc", 20),
            ("only-first", "bb", 5),
            ("only-second", "dd", 7),
        ])
    );

    // The bare status response is the ring view, not a manifest.
    let bare: serde_json::Value = client
        .get(format!("{}/status", harness.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(bare["members"].as_array().unwrap().len(), 3);
    assert!(bare["srv"].as_str().is_some());
    assert!(bare.get("entries").is_none());

    let metrics = agent_metrics(&client, &harness).await;
    assert!(metrics.contains("flywheel_agent_status_fanout_requests_total 1\n"));
    assert!(metrics.contains("flywheel_agent_status_fanout_members_queried_total 3\n"));
    assert!(metrics.contains("flywheel_agent_status_fanout_members_succeeded_total 3\n"));
    assert!(metrics.contains("flywheel_agent_status_fanout_members_failed_total 0\n"));
    assert!(metrics.contains("flywheel_agent_status_fanout_manifest_entries_total 3\n"));
}

/// A dead member stays silent to cacheprog: the fan-out merges what the
/// survivors answered and only the metrics and ring accounting see the failure.
#[tokio::test]
async fn sessioned_status_survives_member_failure_with_partial_results() {
    let backends = [
        Backend::spawn().await,
        Backend::spawn().await,
        Backend::spawn().await,
    ];
    let resolver = Arc::new(TestResolver::default());
    resolver.set(all_members(&backends));
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();
    let session = "partial-fanout";

    put_manifest(
        &client,
        &backends[0],
        session,
        &manifest_of(&[("survives", "aa", 10)]),
    )
    .await;
    put_manifest(
        &client,
        &backends[1],
        session,
        &manifest_of(&[("lost-with-member", "bb", 20)]),
    )
    .await;
    backends[1].kill().await;

    let merged = merged_manifest(&client, &harness, session).await;
    assert_eq!(merged, manifest_of(&[("survives", "aa", 10)]));

    let metrics = agent_metrics(&client, &harness).await;
    assert!(metrics.contains("flywheel_agent_status_fanout_members_queried_total 3\n"));
    assert!(metrics.contains("flywheel_agent_status_fanout_members_succeeded_total 2\n"));
    assert!(metrics.contains("flywheel_agent_status_fanout_members_failed_total 1\n"));
}

#[tokio::test]
async fn sessioned_status_returns_an_empty_manifest_on_an_empty_ring() {
    let resolver = Arc::new(TestResolver::default());
    let harness = spawn_agent(resolver).await;
    let client = client();

    let merged = merged_manifest(&client, &harness, "anything").await;
    assert_eq!(merged, Manifest::empty());

    let metrics = agent_metrics(&client, &harness).await;
    assert!(metrics.contains("flywheel_agent_status_fanout_requests_total 1\n"));
    assert!(metrics.contains("flywheel_agent_status_fanout_members_queried_total 0\n"));
}

/// The purpose header is telemetry only: hits, misses, and bytes are counted
/// separately from foreground traffic while responses stay byte-identical.
#[tokio::test]
async fn prefetch_purpose_header_is_counted_without_changing_responses() {
    let backends = [Backend::spawn().await, Backend::spawn().await];
    let resolver = Arc::new(TestResolver::default());
    resolver.set(all_members(&backends));
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();

    let put = client
        .put(format!("{}/build-cache/http/warm-key", harness.base))
        .body("warm object")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), reqwest::StatusCode::OK);

    let hit = client
        .get(format!("{}/build-cache/http/warm-key", harness.base))
        .header(REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH)
        .send()
        .await
        .unwrap();
    assert_eq!(hit.status(), reqwest::StatusCode::OK);
    assert_eq!(hit.bytes().await.unwrap().as_ref(), b"warm object");
    let miss = client
        .get(format!("{}/build-cache/http/cold-key", harness.base))
        .header(REQUEST_PURPOSE_HEADER, REQUEST_PURPOSE_PREFETCH)
        .send()
        .await
        .unwrap();
    assert_eq!(miss.status(), reqwest::StatusCode::NOT_FOUND);

    let metrics = agent_metrics(&client, &harness).await;
    assert!(metrics.contains("flywheel_agent_prefetch_requests_total 2\n"));
    assert!(metrics.contains("flywheel_agent_prefetch_hits_total 1\n"));
    assert!(metrics.contains("flywheel_agent_prefetch_misses_total 1\n"));
    assert!(metrics.contains("flywheel_agent_prefetch_unavailable_total 0\n"));
    assert!(!metrics.contains("flywheel_agent_prefetch_response_bytes_total 0\n"));
}

/// Response headers prove that an ordinary send reached the member. The success must
/// reset a prior connect-failure streak even when the caller never consumes the body,
/// so the next connect failure starts a new streak instead of ejecting the member.
#[tokio::test]
async fn successful_headers_reset_forward_failure_streak_before_body_consumption() {
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_address = unused.local_addr().unwrap();
    drop(unused);
    let member_id = "flywheel-0.shards.test.svc".to_owned();
    let resolver = Arc::new(TestResolver::default());
    resolver.set(vec![(member_id.clone(), dead_address)]);
    let harness = spawn_agent_with_failure_limit(Arc::clone(&resolver), 2).await;
    let client = client();
    let digest = digest_of(b"health probe object");
    let url = format!("{}/artifacts/sha256/{digest}", harness.base);

    let first_failure = client.get(&url).send().await.unwrap();
    assert_eq!(first_failure.status(), reqwest::StatusCode::BAD_GATEWAY);
    let status = member_status(&client, &harness).await;
    assert_eq!(status["failures"], 1);
    assert_eq!(status["ejected"], false);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_address = listener.local_addr().unwrap();
    let backend = axum::Router::new().fallback(|| async {
        let body = axum::body::Body::from_stream(futures_util::stream::pending::<
            Result<bytes::Bytes, std::convert::Infallible>,
        >());
        axum::response::Response::builder()
            .status(200)
            .body(body)
            .unwrap()
    });
    let backend_task = tokio::spawn(async move {
        axum::serve(listener, backend).await.unwrap();
    });
    resolver.set(vec![(member_id.clone(), live_address)]);
    harness.agent.refresh_once().await.unwrap();

    let successful = client.get(&url).send().await.unwrap();
    assert_eq!(successful.status(), reqwest::StatusCode::OK);
    let status = member_status(&client, &harness).await;
    assert_eq!(status["failures"], 0);

    // Keep `successful` unconsumed while the next send proves the streak was reset at
    // headers rather than at clean body EOF.
    resolver.set(vec![(member_id, dead_address)]);
    harness.agent.refresh_once().await.unwrap();
    let second_failure = client.get(&url).send().await.unwrap();
    assert_eq!(second_failure.status(), reqwest::StatusCode::BAD_GATEWAY);
    let status = member_status(&client, &harness).await;
    assert_eq!(status["failures"], 1);
    assert_eq!(status["ejected"], false);

    drop(successful);
    backend_task.abort();
}

/// The sessioned status fan-out uses the same send-time health rule as ordinary
/// forwards: response headers from a member reset its failure streak, so a
/// successful fan-out answer heals a prior connect failure before the next one.
#[tokio::test]
async fn successful_fanout_headers_reset_failure_streak() {
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_address = unused.local_addr().unwrap();
    drop(unused);
    let member_id = "flywheel-0.shards.test.svc".to_owned();
    let resolver = Arc::new(TestResolver::default());
    resolver.set(vec![(member_id.clone(), dead_address)]);
    let harness = spawn_agent_with_failure_limit(Arc::clone(&resolver), 2).await;
    let client = client();

    merged_manifest(&client, &harness, "streak-probe").await;
    assert_eq!(member_status(&client, &harness).await["failures"], 1);

    let backend = Backend::spawn().await;
    resolver.set(vec![(member_id.clone(), backend.address)]);
    harness.agent.refresh_once().await.unwrap();
    merged_manifest(&client, &harness, "streak-probe").await;
    assert_eq!(member_status(&client, &harness).await["failures"], 0);

    resolver.set(vec![(member_id, dead_address)]);
    harness.agent.refresh_once().await.unwrap();
    merged_manifest(&client, &harness, "streak-probe").await;
    let status = member_status(&client, &harness).await;
    assert_eq!(status["failures"], 1);
    assert_eq!(status["ejected"], false);
}

/// A backend that returns 200 and then dies mid-body must surface the truncation
/// to the client, and must not be ejected for it: a single body error is not
/// proof the backend is down, and a genuinely dead backend fails its very next
/// connect anyway.
#[tokio::test]
async fn truncated_backend_body_propagates_without_false_ejection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let truncating = axum::Router::new().fallback(|| async {
        use futures_util::StreamExt as _;
        // The pause lets the 200 and first chunk flush before the abort, so the
        // client observes a truncated body rather than a failed connect.
        let body = axum::body::Body::from_stream(
            futures_util::stream::iter(vec![
                Ok(bytes::Bytes::from_static(b"partial")),
                Err(std::io::Error::other("backend died mid-body")),
            ])
            .then(|item| async {
                if item.is_err() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                item
            }),
        );
        axum::response::Response::builder()
            .status(200)
            .body(body)
            .unwrap()
    });
    tokio::spawn(async move {
        axum::serve(listener, truncating).await.unwrap();
    });

    let resolver = Arc::new(TestResolver::default());
    resolver.set(vec![("flywheel-0.shards.test.svc".to_owned(), address)]);
    let harness = spawn_agent(Arc::clone(&resolver)).await;
    let client = client();

    let digest = digest_of(b"anything");
    let response = client
        .get(format!("{}/artifacts/sha256/{digest}", harness.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(
        response.bytes().await.is_err(),
        "truncation must propagate, not silently complete"
    );

    let status: serde_json::Value = client
        .get(format!("{}/status", harness.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["members"][0]["ejected"], false);
    let metrics = client
        .get(format!("{}/metrics", harness.base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("flywheel_agent_forward_failures_total 0\n"));
}
