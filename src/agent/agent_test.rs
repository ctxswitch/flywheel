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
