use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use flywheel::{Flywheel, channel::ChannelId, config::Config};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::ServiceExt;

async fn call(app: axum::Router, path: &str) -> axum::response::Response {
    app.oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn call_accepting(
    app: axum::Router,
    path: &str,
    accept: &'static str,
) -> axum::response::Response {
    app.oneshot(
        Request::get(path)
            .header(header::ACCEPT, accept)
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn call_authorized(app: axum::Router, path: &str, token: &str) -> axum::response::Response {
    app.oneshot(
        Request::get(path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn call_with_raw_authorization(
    app: axum::Router,
    path: &str,
    token: &str,
) -> axum::response::Response {
    app.oneshot(
        Request::get(path)
            .header(header::AUTHORIZATION, token)
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

/// Serves `router` on an ephemeral loopback port, returning its base URL and a
/// handle to abort it.
async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}"), server)
}

fn encoded_file_path(url: &str) -> String {
    format!("/proxy/python/files/{}", URL_SAFE_NO_PAD.encode(url))
}

/// A free-space source that reports a full disk so every reservation is refused.
struct FullDisk;
impl flywheel::cache::FreeSpace for FullDisk {
    fn free_bytes(&self) -> Option<u64> {
        Some(0)
    }
}

/// Under disk pressure a proxied package must still download: admission is refused
/// up-front and the untouched upstream body is streamed straight through to the client
/// without being cached, buffered, or re-fetched into the store.
#[tokio::test]
async fn package_download_streams_through_under_disk_pressure() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let upstream = Router::new().route(
        "/crates/demo/demo-1.0.0.crate",
        get(move || {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                "crate-bytes"
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let base = format!("http://{address}");

    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.cargo_crate_upstream = format!("{base}/crates/");
    let flywheel = Flywheel::open_with_space(
        config,
        Arc::new(flywheel::clock::SystemClock),
        Arc::new(FullDisk),
    )
    .await
    .unwrap();
    let app = flywheel.router();

    let response = call(app.clone(), "/proxy/cargo/crates/demo/1.0.0/download").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "crate-bytes"
    );

    // Nothing was cached, so a second request fetches upstream again rather than being
    // served from a (nonexistent) local copy.
    let response = call(app, "/proxy/cargo/crates/demo/1.0.0/download").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "crate-bytes"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    server.abort();
}

#[tokio::test]
async fn package_adapters_preserve_protocol_payloads_and_rewrite_downloads() {
    let npm_hits = Arc::new(AtomicUsize::new(0));
    let hits = npm_hits.clone();
    let upstream = Router::new()
        .route("/go/example.com/mod/@v/list", get(|| async { "v1.0.0\n" }))
        .route(
            "/python/demo/",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/html")],
                    "<a href=\"../files/demo.whl\">demo</a>",
                )
            }),
        )
        .route("/python/files/demo.whl", get(|| async { "wheel-bytes" }))
        .route(
            "/npm/demo",
            get(move |headers: HeaderMap| {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    if headers
                        .get(header::IF_NONE_MATCH)
                        .is_some_and(|value| value == "\"demo-v1\"")
                    {
                        return StatusCode::NOT_MODIFIED.into_response();
                    }
                    (
                        [
                            (header::CONTENT_TYPE, "application/json"),
                            (header::ETAG, "\"demo-v1\""),
                        ],
                        json!({"name":"demo","dist":{"tarball":"demo/-/demo.tgz"}}).to_string(),
                    )
                        .into_response()
                }
            }),
        )
        .route("/npm/demo/-/demo.tgz", get(|| async { "tgz-bytes" }))
        .route(
            "/cargo/de/mo/demo",
            get(|| async { "{\"name\":\"demo\",\"vers\":\"1.0.0\"}\n" }),
        )
        .route(
            "/crates/demo/demo-1.0.0.crate",
            get(|| async { "crate-bytes" }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let base = format!("http://{address}");

    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.go_upstream = format!("{base}/go/");
    config.python_upstream = format!("{base}/python/");
    config.npm_upstream = format!("{base}/npm/");
    config.cargo_index_upstream = format!("{base}/cargo/");
    config.cargo_crate_upstream = format!("{base}/crates/");
    let app = Flywheel::open(config).await.unwrap().router();

    let go = call(app.clone(), "/proxy/go/example.com/mod/@v/list").await;
    assert_eq!(go.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(go.into_body(), usize::MAX).await.unwrap(),
        "v1.0.0\n"
    );

    let python = call(app.clone(), "/proxy/python/simple/demo/").await;
    assert_eq!(python.status(), StatusCode::OK);
    let python = String::from_utf8(
        to_bytes(python.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let rewritten_file = python
        .split("href=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    assert!(rewritten_file.starts_with("/proxy/python/files/"));
    assert_eq!(
        to_bytes(
            call(app.clone(), rewritten_file).await.into_body(),
            usize::MAX
        )
        .await
        .unwrap(),
        "wheel-bytes"
    );

    let npm = call(app.clone(), "/proxy/npm/demo").await;
    assert_eq!(npm.status(), StatusCode::OK);
    let npm: Value =
        serde_json::from_slice(&to_bytes(npm.into_body(), usize::MAX).await.unwrap()).unwrap();
    let tarball = npm["dist"]["tarball"].as_str().unwrap();
    assert!(tarball.starts_with("/proxy/npm/-/tarball/"));
    // A second metadata request is served from the five-minute reference cache.
    assert_eq!(
        call(app.clone(), "/proxy/npm/demo").await.status(),
        StatusCode::OK
    );
    assert_eq!(npm_hits.load(Ordering::SeqCst), 1);

    let cargo_config: Value = serde_json::from_slice(
        &to_bytes(
            call(app.clone(), "/proxy/cargo/index/config.json")
                .await
                .into_body(),
            usize::MAX,
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(cargo_config["auth-required"], false);
    assert_eq!(
        to_bytes(
            call(app.clone(), "/proxy/cargo/index/de/mo/demo")
                .await
                .into_body(),
            usize::MAX
        )
        .await
        .unwrap(),
        "{\"name\":\"demo\",\"vers\":\"1.0.0\"}\n"
    );
    assert_eq!(
        to_bytes(
            call(app, "/proxy/cargo/crates/demo/1.0.0/download")
                .await
                .into_body(),
            usize::MAX
        )
        .await
        .unwrap(),
        "crate-bytes"
    );

    server.abort();
}

#[tokio::test]
async fn python_simple_negotiates_json_and_rewrites_distribution_urls() {
    const PYTHON_JSON: &str = "application/vnd.pypi.simple.v1+json";

    let upstream = Router::new()
        .route(
            "/python/demo/",
            get(|headers: HeaderMap| async move {
                if headers
                    .get(header::ACCEPT)
                    .is_some_and(|value| value == PYTHON_JSON)
                {
                    (
                        [(header::CONTENT_TYPE, PYTHON_JSON)],
                        json!({
                            "meta": {"api-version": "1.4"},
                            "name": "demo",
                            "versions": ["1.0.0"],
                            "files": [{
                                "filename": "demo-1.0.0-py3-none-any.whl",
                                "url": "../files/demo.whl#sha256=abc123",
                                "hashes": {"sha256": "abc123"},
                                "requires-python": ">=3.11",
                                "yanked": false
                            }]
                        })
                        .to_string(),
                    )
                        .into_response()
                } else {
                    StatusCode::NOT_ACCEPTABLE.into_response()
                }
            }),
        )
        .route("/python/files/demo.whl", get(|| async { "wheel-bytes" }));
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call_accepting(app.clone(), "/proxy/python/simple/demo/", PYTHON_JSON).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], PYTHON_JSON);
    let metadata: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(metadata["meta"]["api-version"], "1.4");
    assert_eq!(metadata["files"][0]["hashes"]["sha256"], "abc123");
    assert_eq!(metadata["files"][0]["requires-python"], ">=3.11");
    let file = metadata["files"][0]["url"].as_str().unwrap();
    assert!(file.starts_with("/proxy/python/files/"));
    assert!(file.ends_with("#sha256=abc123"));

    let download = file.split_once('#').unwrap().0;
    let response = call(app, download).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "wheel-bytes"
    );

    server.abort();
}

#[tokio::test]
async fn python_simple_honors_accept_preferences_and_caches_each_representation() {
    const PYTHON_JSON: &str = "application/vnd.pypi.simple.v1+json";
    const PYTHON_HTML: &str = "application/vnd.pypi.simple.v1+html";

    let hits = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&hits);
    let upstream = Router::new().route(
        "/python/demo/",
        get(move |headers: HeaderMap| {
            let counted = Arc::clone(&counted);
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                match headers
                    .get(header::ACCEPT)
                    .and_then(|value| value.to_str().ok())
                {
                    Some(PYTHON_JSON) => (
                        [(header::CONTENT_TYPE, PYTHON_JSON)],
                        json!({"meta":{"api-version":"1.0"},"name":"demo","files":[]}).to_string(),
                    )
                        .into_response(),
                    Some(PYTHON_HTML) => (
                        [(header::CONTENT_TYPE, PYTHON_HTML)],
                        "<!doctype html><html><body></body></html>",
                    )
                        .into_response(),
                    _ => StatusCode::NOT_ACCEPTABLE.into_response(),
                }
            }
        }),
    );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();
    let path = "/proxy/python/simple/demo/";

    let prefers_html = concat!(
        "application/vnd.pypi.simple.v1+json;q=0.1, ",
        "application/vnd.pypi.simple.v1+html;q=1"
    );
    let html = call_accepting(app.clone(), path, prefers_html).await;
    assert_eq!(html.status(), StatusCode::OK);
    assert_eq!(html.headers()[header::CONTENT_TYPE], PYTHON_HTML);

    let json = call_accepting(app.clone(), path, PYTHON_JSON).await;
    assert_eq!(json.status(), StatusCode::OK);
    assert_eq!(json.headers()[header::CONTENT_TYPE], PYTHON_JSON);

    assert_eq!(
        call_accepting(app.clone(), path, prefers_html)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        call_accepting(app.clone(), path, PYTHON_JSON)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    let unsupported = call_accepting(app, path, "application/xml").await;
    assert_eq!(unsupported.status(), StatusCode::NOT_ACCEPTABLE);
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    server.abort();
}

#[tokio::test]
async fn python_html_project_list_routes_to_simple_pages_not_distribution_downloads() {
    const PYTHON_HTML: &str = "application/vnd.pypi.simple.v1+html";

    let upstream = Router::new()
        .route(
            "/python/",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, PYTHON_HTML)],
                    "<!doctype html><html><body><a href=\"demo/\">Demo</a></body></html>",
                )
            }),
        )
        .route(
            "/python/demo/",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, PYTHON_HTML)],
                    "<!doctype html><html><body><a href=\"../files/demo.whl\">demo.whl</a></body></html>",
                )
            }),
        );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let root = call_accepting(app.clone(), "/proxy/python/simple/", PYTHON_HTML).await;
    assert_eq!(root.status(), StatusCode::OK);
    let root = String::from_utf8(
        to_bytes(root.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(root.contains("href=\"/proxy/python/simple/demo/\""));
    assert!(!root.contains("/proxy/python/files/"));

    let project = call_accepting(app, "/proxy/python/simple/demo/", PYTHON_HTML).await;
    let project = String::from_utf8(
        to_bytes(project.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(project.contains("href=\"/proxy/python/files/"));

    server.abort();
}

#[tokio::test]
async fn npm_negotiates_and_separately_caches_install_and_full_metadata() {
    const NPM_INSTALL: &str = "application/vnd.npm.install-v1+json";

    let hits = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&hits);
    let upstream = Router::new().route(
        "/npm/demo",
        get(move |headers: HeaderMap| {
            let counted = Arc::clone(&counted);
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                match headers
                    .get(header::ACCEPT)
                    .and_then(|value| value.to_str().ok())
                {
                    Some(NPM_INSTALL) => (
                        [(header::CONTENT_TYPE, NPM_INSTALL)],
                        json!({
                            "name": "demo",
                            "dist-tags": {"latest": "1.0.0"},
                            "versions": {"1.0.0": {"dist": {"tarball": "demo/-/demo.tgz"}}}
                        })
                        .to_string(),
                    )
                        .into_response(),
                    Some("application/json") => (
                        [(header::CONTENT_TYPE, "application/json")],
                        json!({
                            "name": "demo",
                            "readme": "full metadata",
                            "dist": {"tarball": "demo/-/demo.tgz"}
                        })
                        .to_string(),
                    )
                        .into_response(),
                    _ => StatusCode::NOT_ACCEPTABLE.into_response(),
                }
            }
        }),
    );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.npm_upstream = format!("{base}/npm/");
    let app = Flywheel::open(config).await.unwrap().router();
    let path = "/proxy/npm/demo";

    let install_accept = concat!(
        "application/vnd.npm.install-v1+json;q=1, ",
        "application/json;q=0.8"
    );
    let install = call_accepting(app.clone(), path, install_accept).await;
    assert_eq!(install.status(), StatusCode::OK);
    assert_eq!(install.headers()[header::CONTENT_TYPE], NPM_INSTALL);
    let install: Value =
        serde_json::from_slice(&to_bytes(install.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(
        install["versions"]["1.0.0"]["dist"]["tarball"]
            .as_str()
            .unwrap()
            .starts_with("/proxy/npm/-/tarball/")
    );

    let full = call_accepting(app.clone(), path, "application/json").await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.headers()[header::CONTENT_TYPE], "application/json");
    let full: Value =
        serde_json::from_slice(&to_bytes(full.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(full["readme"], "full metadata");

    assert_eq!(
        call_accepting(app.clone(), path, install_accept)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        call_accepting(app.clone(), path, "application/json")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(
        call_accepting(app, path, "application/xml").await.status(),
        StatusCode::NOT_ACCEPTABLE
    );
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    server.abort();
}

/// A wildcard range never outranks a representation the client named explicitly
/// at the same quality: `application/json, application/*` asks for the full
/// document and merely tolerates the rest, so it must not be answered with the
/// abbreviated one. Only an explicit q-value reorders the two.
#[tokio::test]
async fn wildcard_accept_ranges_never_outrank_a_named_representation() {
    const NPM_INSTALL: &str = "application/vnd.npm.install-v1+json";
    const NPM_FULL: &str = "application/json";
    const PYTHON_JSON: &str = "application/vnd.pypi.simple.v1+json";
    const PYTHON_HTML: &str = "application/vnd.pypi.simple.v1+html";

    let upstream = Router::new()
        .route(
            "/npm/demo",
            get(|headers: HeaderMap| async move {
                match headers
                    .get(header::ACCEPT)
                    .and_then(|value| value.to_str().ok())
                {
                    Some(NPM_INSTALL) => (
                        [(header::CONTENT_TYPE, NPM_INSTALL)],
                        json!({"name": "demo", "dist-tags": {"latest": "1.0.0"}}).to_string(),
                    )
                        .into_response(),
                    Some(NPM_FULL) => (
                        [(header::CONTENT_TYPE, NPM_FULL)],
                        json!({"name": "demo", "readme": "full metadata"}).to_string(),
                    )
                        .into_response(),
                    _ => StatusCode::NOT_ACCEPTABLE.into_response(),
                }
            }),
        )
        .route(
            "/python/demo/",
            get(|headers: HeaderMap| async move {
                match headers
                    .get(header::ACCEPT)
                    .and_then(|value| value.to_str().ok())
                {
                    Some(PYTHON_JSON) => (
                        [(header::CONTENT_TYPE, PYTHON_JSON)],
                        json!({"meta":{"api-version":"1.0"},"name":"demo","files":[]}).to_string(),
                    )
                        .into_response(),
                    Some(PYTHON_HTML) => (
                        [(header::CONTENT_TYPE, PYTHON_HTML)],
                        "<!doctype html><html><body></body></html>",
                    )
                        .into_response(),
                    _ => StatusCode::NOT_ACCEPTABLE.into_response(),
                }
            }),
        );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.npm_upstream = format!("{base}/npm/");
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    for (accept, expected) in [
        ("application/json, application/*", NPM_FULL),
        ("application/*, application/json", NPM_FULL),
        ("application/json, */*", NPM_FULL),
        (
            "application/vnd.npm.install-v1+json, application/*",
            NPM_INSTALL,
        ),
        (
            "application/*, application/vnd.npm.install-v1+json",
            NPM_INSTALL,
        ),
        // A bare wildcard names neither document, so npm's own abbreviated
        // install metadata stays the answer.
        ("application/*", NPM_INSTALL),
        // Specificity only breaks ties: an explicit q-value still decides.
        ("application/json;q=0.2, application/*", NPM_INSTALL),
    ] {
        let response = call_accepting(app.clone(), "/proxy/npm/demo", accept).await;
        assert_eq!(response.status(), StatusCode::OK, "Accept: {accept}");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            expected,
            "Accept: {accept}"
        );
    }

    for (accept, expected) in [
        (
            "application/vnd.pypi.simple.v1+json, application/*",
            PYTHON_JSON,
        ),
        (
            "application/*, application/vnd.pypi.simple.v1+json",
            PYTHON_JSON,
        ),
        (
            "application/vnd.pypi.simple.v1+html, application/*",
            PYTHON_HTML,
        ),
        ("text/html, */*", PYTHON_HTML),
        (
            "application/vnd.pypi.simple.v1+json;q=0.2, text/*",
            PYTHON_HTML,
        ),
    ] {
        let response = call_accepting(app.clone(), "/proxy/python/simple/demo/", accept).await;
        assert_eq!(response.status(), StatusCode::OK, "Accept: {accept}");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            expected,
            "Accept: {accept}"
        );
    }

    server.abort();
}

#[tokio::test]
async fn protected_cargo_registry_accepts_credential_provider_authorization() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = app
        .clone()
        .oneshot(
            Request::post("/channels")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"access_control": true}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let registered: Value =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let channel = registered["channel"].as_str().unwrap();
    let token = registered["token"].as_str().unwrap();
    let config_path = format!("/channels/{channel}/proxy/cargo/index/config.json");

    let challenge = call(app.clone(), &config_path).await;
    assert_eq!(challenge.status(), StatusCode::UNAUTHORIZED);
    assert!(challenge.headers().contains_key(header::WWW_AUTHENTICATE));

    let config = call_with_raw_authorization(app, &config_path, token).await;
    assert_eq!(config.status(), StatusCode::OK);
    let config: Value =
        serde_json::from_slice(&to_bytes(config.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(config["auth-required"], true);

    assert!(token.starts_with("flywheel_"));
}

#[tokio::test]
async fn python_rewritten_files_retain_metadata_signatures_and_provenance() {
    const PYTHON_JSON: &str = "application/vnd.pypi.simple.v1+json";

    let upstream = Router::new()
        .route(
            "/python/demo/",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, PYTHON_JSON)],
                    json!({
                        "meta": {"api-version": "1.3"},
                        "name": "demo",
                        "files": [{
                            "filename": "demo.whl",
                            "url": "../files/demo.whl",
                            "hashes": {"sha256": "abc123"},
                            "core-metadata": {"sha256": "def456"},
                            "gpg-sig": true,
                            "provenance": "../files/demo.whl.provenance",
                            "size": 11
                        }]
                    })
                    .to_string(),
                )
            }),
        )
        .route(
            "/python/files/demo.whl.metadata",
            get(|| async { "Metadata-Version: 2.4\n" }),
        )
        .route("/python/files/demo.whl.asc", get(|| async { "signature" }))
        .route(
            "/python/files/demo.whl.provenance",
            get(|| async { "provenance" }),
        );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call_accepting(app.clone(), "/proxy/python/simple/demo/", PYTHON_JSON).await;
    let metadata: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let file = metadata["files"][0]["url"].as_str().unwrap();
    let provenance = metadata["files"][0]["provenance"].as_str().unwrap();
    assert!(provenance.starts_with("/proxy/python/files/"));

    let core_metadata = call(app.clone(), &format!("{file}.metadata")).await;
    assert_eq!(core_metadata.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(core_metadata.into_body(), usize::MAX)
            .await
            .unwrap(),
        "Metadata-Version: 2.4\n"
    );
    let signature = call(app.clone(), &format!("{file}.asc")).await;
    assert_eq!(signature.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(signature.into_body(), usize::MAX).await.unwrap(),
        "signature"
    );
    let provenance = call(app, provenance).await;
    assert_eq!(provenance.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(provenance.into_body(), usize::MAX).await.unwrap(),
        "provenance"
    );

    server.abort();
}

#[tokio::test]
async fn go_proxy_serves_the_complete_read_protocol() {
    let upstream = Router::new()
        .route(
            "/go/example.com/!demo/@v/list",
            get(|| async { "v1.0.0\n" }),
        )
        .route(
            "/go/example.com/!demo/@latest",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    r#"{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}"#,
                )
            }),
        )
        .route(
            "/go/example.com/!demo/@v/v1.0.0.info",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    r#"{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}"#,
                )
            }),
        )
        .route(
            "/go/example.com/!demo/@v/v1.0.0.mod",
            get(|| async { "module example.com/Demo\n\ngo 1.26\n" }),
        )
        .route(
            "/go/example.com/!demo/@v/v1.0.0.zip",
            get(|| async { "zip-bytes" }),
        );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.go_upstream = format!("{base}/go/");
    let app = Flywheel::open(config).await.unwrap().router();
    let module = "/proxy/go/example.com/!demo";

    for (suffix, expected) in [
        ("/@v/list", "v1.0.0\n"),
        (
            "/@latest",
            r#"{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}"#,
        ),
        (
            "/@v/v1.0.0.info",
            r#"{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}"#,
        ),
        ("/@v/v1.0.0.mod", "module example.com/Demo\n\ngo 1.26\n"),
        ("/@v/v1.0.0.zip", "zip-bytes"),
    ] {
        let response = call(app.clone(), &format!("{module}{suffix}")).await;
        assert_eq!(response.status(), StatusCode::OK, "{suffix}");
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            expected,
            "{suffix}"
        );
    }

    server.abort();
}

#[tokio::test]
async fn npm_proxy_rewrites_scoped_package_tarballs() {
    let upstream = Router::new()
        .route(
            "/npm/@scope/pkg",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    json!({
                        "name": "@scope/pkg",
                        "dist-tags": {"latest": "1.0.0"},
                        "versions": {
                            "1.0.0": {
                                "name": "@scope/pkg",
                                "version": "1.0.0",
                                "dist": {"tarball": "/npm/@scope/pkg/-/pkg-1.0.0.tgz"}
                            }
                        }
                    })
                    .to_string(),
                )
            }),
        )
        .route(
            "/npm/@scope/pkg/-/pkg-1.0.0.tgz",
            get(|| async { "tgz-bytes" }),
        );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.npm_upstream = format!("{base}/npm/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call_accepting(app.clone(), "/proxy/npm/@scope%2Fpkg", "application/json").await;
    assert_eq!(response.status(), StatusCode::OK);
    let metadata: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let tarball = metadata["versions"]["1.0.0"]["dist"]["tarball"]
        .as_str()
        .unwrap();
    assert!(tarball.starts_with("/proxy/npm/-/tarball/"));
    let tarball = call(app, tarball).await;
    assert_eq!(tarball.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(tarball.into_body(), usize::MAX).await.unwrap(),
        "tgz-bytes"
    );

    server.abort();
}

#[tokio::test]
async fn python_html_rewriting_uses_parsed_anchor_attributes() {
    const PYTHON_HTML: &str = "application/vnd.pypi.simple.v1+html";

    let upstream = Router::new().route(
        "/python/demo/",
        get(|| async {
            (
                [(header::CONTENT_TYPE, PYTHON_HTML)],
                concat!(
                    "<!doctype html><html><body>",
                    "<!-- <a href=\"../files/not-a-link.whl\">ignored</a> -->",
                    "<a href='../files/demo.whl?one=1&amp;two=2#sha256=abc123' ",
                    "data-provenance='../files/demo.whl.provenance?one=1&amp;two=2'>",
                    "demo.whl</a></body></html>"
                ),
            )
        }),
    );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call_accepting(app, "/proxy/python/simple/demo/", PYTHON_HTML).await;
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("<!-- <a href=\"../files/not-a-link.whl\">ignored</a> -->"));
    assert_eq!(html.matches("/proxy/python/files/").count(), 2);
    assert!(html.contains("#sha256=abc123"));

    let encoded = html
        .split("/proxy/python/files/")
        .nth(1)
        .unwrap()
        .split('#')
        .next()
        .unwrap();
    let upstream_url = String::from_utf8(URL_SAFE_NO_PAD.decode(encoded).unwrap()).unwrap();
    assert!(
        upstream_url.ends_with("/python/files/demo.whl?one=1&two=2"),
        "{upstream_url}"
    );

    server.abort();
}

#[tokio::test]
async fn package_cache_keys_separate_metadata_variants_from_download_urls() {
    const PYTHON_HTML: &str = "application/vnd.pypi.simple.v1+html";

    let upstream = Router::new()
        .route(
            "/python/demo/",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, PYTHON_HTML)],
                    "<!doctype html><html><body><a href=\"html\">demo.whl</a></body></html>",
                )
            }),
        )
        .route("/python/demo/html", get(|| async { "wheel-bytes" }));
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call_accepting(app.clone(), "/proxy/python/simple/demo/", PYTHON_HTML).await;
    let html = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let download = html
        .split("href=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();

    let download = call(app, download).await;
    assert_eq!(download.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(download.into_body(), usize::MAX).await.unwrap(),
        "wheel-bytes"
    );

    server.abort();
}

#[tokio::test]
async fn default_channel_proxy_cache_preserves_each_requests_route_form() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&hits);
    let upstream = Router::new().route(
        "/python/{project}/",
        get(move || {
            let counted = Arc::clone(&counted);
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                (
                    [(header::CONTENT_TYPE, "text/html")],
                    "<a href=\"../files/demo.whl\">demo</a>",
                )
            }
        }),
    );
    let (base, server) = serve(upstream).await;
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();
    let default_prefix = format!("/channels/{}", ChannelId::DEFAULT);

    let bare = call(app.clone(), "/proxy/python/simple/demo/").await;
    assert_eq!(bare.status(), StatusCode::OK);
    let bare = String::from_utf8(
        to_bytes(bare.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(bare.contains("href=\"/proxy/python/files/"));
    assert!(!bare.contains(&default_prefix));

    let prefixed_path = format!("{default_prefix}/proxy/python/simple/demo/");
    let prefixed = call(app.clone(), &prefixed_path).await;
    assert_eq!(prefixed.status(), StatusCode::OK);
    let prefixed = String::from_utf8(
        to_bytes(prefixed.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(prefixed.contains(&format!("href=\"{default_prefix}/proxy/python/files/")));
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    let first_prefixed = call(
        app.clone(),
        &format!("{default_prefix}/proxy/python/simple/reverse/"),
    )
    .await;
    let first_prefixed = String::from_utf8(
        to_bytes(first_prefixed.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(first_prefixed.contains(&format!("href=\"{default_prefix}/proxy/python/files/")));

    let then_bare = call(app, "/proxy/python/simple/reverse/").await;
    let then_bare = String::from_utf8(
        to_bytes(then_bare.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(then_bare.contains("href=\"/proxy/python/files/"));
    assert!(!then_bare.contains(&default_prefix));
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    server.abort();
}

#[tokio::test]
async fn channel_package_routes_use_canonical_prefixes_and_cargo_auth_metadata() {
    let upstream = Router::new()
        .route("/go/example.com/mod/@v/list", get(|| async { "v1.0.0\n" }))
        .route(
            "/python/demo/",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/html")],
                    "<a href=\"../files/demo.whl\">demo</a>",
                )
            }),
        )
        .route("/python/files/demo.whl", get(|| async { "wheel-bytes" }))
        .route(
            "/npm/demo",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    json!({"name":"demo","dist":{"tarball":"demo/-/demo.tgz"}}).to_string(),
                )
            }),
        )
        .route("/npm/demo/-/demo.tgz", get(|| async { "tgz-bytes" }))
        .route(
            "/cargo/de/mo/demo",
            get(|| async { "{\"name\":\"demo\",\"vers\":\"1.0.0\"}\n" }),
        )
        .route(
            "/crates/demo/demo-1.0.0.crate",
            get(|| async { "crate-bytes" }),
        );
    let (base, server) = serve(upstream).await;

    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.go_upstream = format!("{base}/go/");
    config.python_upstream = format!("{base}/python/");
    config.npm_upstream = format!("{base}/npm/");
    config.cargo_index_upstream = format!("{base}/cargo/");
    config.cargo_crate_upstream = format!("{base}/crates/");
    let app = Flywheel::open(config).await.unwrap().router();
    let registered = app
        .clone()
        .oneshot(
            Request::post("/channels")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"access_control": true}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(registered.status(), StatusCode::CREATED);
    let registered: Value =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let channel = registered["channel"].as_str().unwrap();
    let token = registered["token"].as_str().unwrap();
    let prefix = format!("/channels/{channel}");

    let go = call_authorized(
        app.clone(),
        &format!("{prefix}/proxy/go/example.com/mod/@v/list"),
        token,
    )
    .await;
    assert_eq!(go.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(go.into_body(), usize::MAX).await.unwrap(),
        "v1.0.0\n"
    );

    let python = call_authorized(
        app.clone(),
        &format!("{prefix}/proxy/python/simple/demo/"),
        token,
    )
    .await;
    assert_eq!(python.status(), StatusCode::OK);
    let python = String::from_utf8(
        to_bytes(python.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let wheel = python
        .split("href=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    assert!(wheel.starts_with(&format!("{prefix}/proxy/python/files/")));
    assert_eq!(
        to_bytes(
            call_authorized(app.clone(), wheel, token).await.into_body(),
            usize::MAX,
        )
        .await
        .unwrap(),
        "wheel-bytes",
    );

    let npm = call_authorized(app.clone(), &format!("{prefix}/proxy/npm/demo"), token).await;
    assert_eq!(npm.status(), StatusCode::OK);
    let npm: Value =
        serde_json::from_slice(&to_bytes(npm.into_body(), usize::MAX).await.unwrap()).unwrap();
    let tarball = npm["dist"]["tarball"].as_str().unwrap();
    assert!(tarball.starts_with(&format!("{prefix}/proxy/npm/-/tarball/")));
    assert_eq!(
        to_bytes(
            call_authorized(app.clone(), tarball, token)
                .await
                .into_body(),
            usize::MAX,
        )
        .await
        .unwrap(),
        "tgz-bytes",
    );

    let cargo_config: Value = serde_json::from_slice(
        &to_bytes(
            call_authorized(
                app.clone(),
                &format!("{prefix}/proxy/cargo/index/config.json"),
                token,
            )
            .await
            .into_body(),
            usize::MAX,
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(cargo_config["auth-required"], true);
    assert_eq!(cargo_config["dl"], format!("{prefix}/proxy/cargo/crates"));
    assert_eq!(
        to_bytes(
            call_authorized(
                app.clone(),
                &format!("{prefix}/proxy/cargo/index/de/mo/demo"),
                token,
            )
            .await
            .into_body(),
            usize::MAX,
        )
        .await
        .unwrap(),
        "{\"name\":\"demo\",\"vers\":\"1.0.0\"}\n",
    );
    assert_eq!(
        to_bytes(
            call_authorized(
                app,
                &format!("{prefix}/proxy/cargo/crates/demo/1.0.0/download"),
                token,
            )
            .await
            .into_body(),
            usize::MAX,
        )
        .await
        .unwrap(),
        "crate-bytes",
    );

    server.abort();
}

/// An encoded download URL pointing at an origin outside the protocol's allowlist is
/// refused before any connection is made — same host, different port is a different
/// origin.
#[tokio::test]
async fn encoded_download_urls_outside_allowed_origins_are_refused() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let attacker = Router::new().route(
        "/secret",
        get(move || {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                "attacker-bytes"
            }
        }),
    );
    let (attacker_base, server) = serve(attacker).await;

    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();

    let response = call(app, &encoded_file_path(&format!("{attacker_base}/secret"))).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    server.abort();
}

/// A redirect from an allowed upstream to a disallowed origin is refused at the hop —
/// the target is never contacted.
#[tokio::test]
async fn redirects_to_disallowed_origins_are_refused() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let attacker = Router::new().route(
        "/steal",
        get(move || {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                "attacker-bytes"
            }
        }),
    );
    let (attacker_base, attacker_server) = serve(attacker).await;

    let steal = format!("{attacker_base}/steal");
    let upstream = Router::new().route(
        "/python/files/escape",
        get(move || {
            let steal = steal.clone();
            async move { (StatusCode::FOUND, [(header::LOCATION, steal)]) }
        }),
    );
    let (base, upstream_server) = serve(upstream).await;

    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call(
        app,
        &encoded_file_path(&format!("{base}/python/files/escape")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    attacker_server.abort();
    upstream_server.abort();
}

/// Same-origin redirect chains — including relative Locations — are followed and the
/// final body is served and cached.
#[tokio::test]
async fn allowed_redirect_chains_are_followed() {
    let upstream = Router::new()
        .route(
            "/python/files/hop1",
            get(|| async { (StatusCode::FOUND, [(header::LOCATION, "hop2")]) }),
        )
        .route("/python/files/hop2", get(|| async { "wheel-bytes" }));
    let (base, server) = serve(upstream).await;

    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call(
        app,
        &encoded_file_path(&format!("{base}/python/files/hop1")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "wheel-bytes"
    );

    server.abort();
}

/// Credentialed download URLs stay refused as malformed input (502), before any
/// origin consideration.
#[tokio::test]
async fn credentialed_encoded_urls_are_refused() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();

    let response = call(app, &encoded_file_path("http://user:pw@127.0.0.1:9/x")).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

/// Origins added through `proxy_allowed_origins` become fetchable for every protocol.
#[tokio::test]
async fn operator_extended_origins_are_fetchable() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let mirror = Router::new().route(
        "/mirror/demo.whl",
        get(move || {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                "mirror-bytes"
            }
        }),
    );
    let (mirror_base, server) = serve(mirror).await;

    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.proxy_allowed_origins = vec![mirror_base.clone()];
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call(
        app,
        &encoded_file_path(&format!("{mirror_base}/mirror/demo.whl")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "mirror-bytes"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    server.abort();
}

/// A redirect chain longer than the hop limit fails as unavailable rather than
/// looping.
#[tokio::test]
async fn redirect_chains_beyond_the_hop_limit_fail() {
    let upstream = Router::new().route(
        "/python/files/loop",
        get(|| async { (StatusCode::FOUND, [(header::LOCATION, "loop")]) }),
    );
    let (base, server) = serve(upstream).await;

    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.python_upstream = format!("{base}/python/");
    let app = Flywheel::open(config).await.unwrap().router();

    let response = call(
        app,
        &encoded_file_path(&format!("{base}/python/files/loop")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    server.abort();
}
