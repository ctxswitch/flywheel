use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use flywheel::{
    Flywheel,
    channel::{Access, ChannelId, ChannelRecord, ChannelToken, Lifecycle},
    config::Config,
};
use rocksdb::{DB, IteratorMode, Options};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;

#[path = "common/mod.rs"]
mod common;
use common::call;

async fn register(app: axum::Router, protected: bool) -> Value {
    let response = call(
        app,
        Request::post("/channels")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json!({"access_control": protected}).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

fn open_metadata(directory: &TempDir) -> DB {
    DB::open_cf(
        &Options::default(),
        directory.path().join("metadata"),
        ["meta", "artifacts", "references", "eviction", "channels"],
    )
    .unwrap()
}

fn encode_channel(record: &ChannelRecord) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend(postcard::to_stdvec(record).unwrap());
    bytes
}

#[tokio::test]
async fn data_route_tree_is_visible_bare_and_below_channels() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = register(app.clone(), false).await;
    let channel = registered["channel"].as_str().unwrap();
    let digest = "0".repeat(64);
    let data_paths = [
        format!("/artifacts/sha256/{digest}"),
        "/references/example".to_owned(),
        "/build-cache/http/example".to_owned(),
        format!("/build-cache/bazel/ac/{digest}"),
        format!("/build-cache/bazel/cas/{digest}"),
        "/proxy/go/example.com/mod/@v/list".to_owned(),
        "/proxy/python/simple/example/".to_owned(),
        "/proxy/python/files/encoded".to_owned(),
        "/proxy/npm/example".to_owned(),
        "/proxy/cargo/index/config.json".to_owned(),
        "/proxy/cargo/index/ex/am/example".to_owned(),
        "/proxy/cargo/crates/example/1.0.0/download".to_owned(),
    ];

    for bare in data_paths {
        let prefixed = format!("/channels/{channel}{bare}");
        for path in [bare.as_str(), prefixed.as_str()] {
            assert_eq!(
                call(
                    app.clone(),
                    Request::builder()
                        .method(Method::OPTIONS)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "unexpected route visibility for {path}",
            );
        }
    }
}

#[tokio::test]
async fn default_channel_aliases_bare_data_and_preserves_patched_expiry() {
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.default_expiry_seconds = 120;
    let app = Flywheel::open(config).await.unwrap().router();
    let default = ChannelId::DEFAULT.to_string();
    let channel_path = format!("/channels/{default}");

    let view = call(
        app.clone(),
        Request::get(&channel_path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(view.status(), StatusCode::OK);
    let view: Value =
        serde_json::from_slice(&to_bytes(view.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(view["channel"], default);
    assert_eq!(view["access_control"], false);
    assert_eq!(view["expiry_seconds"], 120);
    assert!(view.get("token").is_none());

    let first = b"written bare";
    let first_digest = hex::encode(Sha256::digest(first));
    let first_bare = format!("/artifacts/sha256/{first_digest}");
    let first_prefixed = format!("/channels/{default}{first_bare}");
    assert_eq!(
        call(
            app.clone(),
            Request::put(&first_bare)
                .body(Body::from(first.as_slice()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::CREATED,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(&first_prefixed).body(Body::empty()).unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );

    let second = b"written prefixed";
    let second_digest = hex::encode(Sha256::digest(second));
    let second_bare = format!("/artifacts/sha256/{second_digest}");
    let second_prefixed = format!("/channels/{default}{second_bare}");
    assert_eq!(
        call(
            app.clone(),
            Request::put(&second_prefixed)
                .body(Body::from(second.as_slice()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::CREATED,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(&second_bare).body(Body::empty()).unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );

    let binding = json!({"algorithm": "sha256", "digest": first_digest}).to_string();
    assert_eq!(
        call(
            app.clone(),
            Request::put("/references/default-alias")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(binding))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NO_CONTENT,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(format!("/channels/{default}/references/default-alias"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );
    let reverse_binding = json!({"algorithm": "sha256", "digest": second_digest}).to_string();
    assert_eq!(
        call(
            app.clone(),
            Request::put(format!(
                "/channels/{default}/references/reverse-default-alias"
            ))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(reverse_binding))
            .unwrap(),
        )
        .await
        .status(),
        StatusCode::NO_CONTENT,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get("/references/reverse-default-alias")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );

    assert_eq!(
        call(
            app.clone(),
            Request::put(format!(
                "/channels/{default}/build-cache/http/default-alias"
            ))
            .body(Body::from("build output"))
            .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::put("/build-cache/http/reverse-default-alias")
                .body(Body::from("reverse build output"))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(format!(
                "/channels/{default}/build-cache/http/reverse-default-alias"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get("/build-cache/http/default-alias")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );

    assert_eq!(
        call(
            app.clone(),
            Request::patch(&channel_path)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"expiry_seconds": 900}).to_string()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::patch(&channel_path)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"access_control": true}).to_string()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::delete(&channel_path).body(Body::empty()).unwrap(),
        )
        .await
        .status(),
        StatusCode::CONFLICT,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(&first_bare).body(Body::empty()).unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );

    drop(app);
    let mut reopened_config = Config::new(directory.path());
    reopened_config.default_expiry_seconds = 1;
    let reopened = Flywheel::open(reopened_config).await.unwrap().router();
    let view = call(
        reopened,
        Request::get(&channel_path).body(Body::empty()).unwrap(),
    )
    .await;
    let view: Value =
        serde_json::from_slice(&to_bytes(view.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(view["expiry_seconds"], 900);
}

#[tokio::test]
async fn registration_fixes_channel_authentication_for_its_lifetime() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();

    let open = register(app.clone(), false).await;
    assert_eq!(open["access_control"], false);
    assert!(open.get("token").is_none());
    let open_path = format!("/channels/{}", open["channel"].as_str().unwrap());
    assert_eq!(
        call(
            app.clone(),
            Request::patch(&open_path)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"access_control": true}).to_string()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(&open_path).body(Body::empty()).unwrap(),
        )
        .await
        .status(),
        StatusCode::OK,
    );

    let protected = register(app.clone(), true).await;
    let token = protected["token"].as_str().unwrap();
    let protected_path = format!("/channels/{}", protected["channel"].as_str().unwrap());
    assert_eq!(
        call(
            app.clone(),
            Request::patch(&protected_path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"access_control": false}).to_string()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST,
    );
    let view = call(
        app,
        Request::get(&protected_path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let view: Value =
        serde_json::from_slice(&to_bytes(view.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(view["access_control"], true);
    assert!(view.get("token").is_none());
}

#[tokio::test]
async fn fresh_store_uses_only_channel_keys_and_record_schema_one() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let body = b"channel layout";
    let digest = hex::encode(Sha256::digest(body));
    assert_eq!(
        call(
            app.clone(),
            Request::put(format!("/artifacts/sha256/{digest}"))
                .body(Body::from(body.as_slice()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::CREATED,
    );
    assert_eq!(
        call(
            app.clone(),
            Request::put("/references/layout")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"algorithm": "sha256", "digest": digest}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NO_CONTENT,
    );
    drop(app);

    let mut families = DB::list_cf(&Options::default(), directory.path().join("metadata")).unwrap();
    families.sort();
    assert_eq!(
        families,
        [
            "artifacts",
            "channels",
            "default",
            "eviction",
            "meta",
            "references"
        ],
    );
    let database = open_metadata(&directory);
    let meta = database.cf_handle("meta").unwrap();
    assert_eq!(
        database.get_cf(&meta, b"store-format").unwrap().unwrap(),
        b"flywheel-channel-cache-v1",
    );
    let prefix = ChannelId::DEFAULT.as_key();
    for (family_name, key_len) in [
        ("artifacts", 26 + 32),
        ("references", 26 + "layout".len()),
        ("eviction", 26 + 8 + 32),
        ("channels", 26),
    ] {
        let family = database.cf_handle(family_name).unwrap();
        let rows: Vec<_> = database
            .iterator_cf(&family, IteratorMode::Start)
            .map(Result::unwrap)
            .collect();
        assert!(!rows.is_empty(), "{family_name:?} should contain a row");
        for (key, record) in rows {
            assert!(key.starts_with(&prefix));
            assert_eq!(key.len(), key_len);
            if family_name != "eviction" {
                assert_eq!(record[0], 1);
            }
        }
    }
    assert!(
        directory
            .path()
            .join(format!("artifacts/{}/sha256", ChannelId::DEFAULT))
            .is_dir(),
    );
    assert!(!directory.path().join("artifacts/single").exists());
}

#[tokio::test]
async fn startup_rejects_invalid_persisted_default_channel_invariants() {
    for record in [
        ChannelRecord {
            id: ChannelId::DEFAULT,
            access: Access::Token(ChannelToken::generate().digest()),
            expiry_seconds: 60,
            state: Lifecycle::Active,
            created_at: 1,
        },
        ChannelRecord {
            id: ChannelId::DEFAULT,
            access: Access::Open,
            expiry_seconds: 60,
            state: Lifecycle::Deleting,
            created_at: 1,
        },
    ] {
        let directory = TempDir::new().unwrap();
        let app = Flywheel::open(Config::new(directory.path()))
            .await
            .unwrap()
            .router();
        drop(app);
        let database = open_metadata(&directory);
        let channels = database.cf_handle("channels").unwrap();
        database
            .put_cf(
                &channels,
                ChannelId::DEFAULT.as_key(),
                encode_channel(&record),
            )
            .unwrap();
        drop(database);

        let error = match Flywheel::open(Config::new(directory.path())).await {
            Ok(_) => panic!("invalid default channel should fail startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("persisted default channel"));
    }
}

#[tokio::test]
async fn startup_resumes_an_interrupted_custom_channel_deletion() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = register(app.clone(), false).await;
    let channel: ChannelId = registered["channel"].as_str().unwrap().parse().unwrap();
    let body = b"deleted after restart";
    let digest = hex::encode(Sha256::digest(body));
    let artifact_path = format!("/channels/{channel}/artifacts/sha256/{digest}");
    assert_eq!(
        call(
            app.clone(),
            Request::put(&artifact_path)
                .body(Body::from(body.as_slice()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::CREATED,
    );
    drop(app);

    let database = open_metadata(&directory);
    let channels = database.cf_handle("channels").unwrap();
    let bytes = database
        .get_cf(&channels, channel.as_key())
        .unwrap()
        .unwrap();
    let mut record: ChannelRecord = postcard::from_bytes(&bytes[1..]).unwrap();
    record.state = Lifecycle::Deleting;
    database
        .put_cf(&channels, channel.as_key(), encode_channel(&record))
        .unwrap();
    drop(database);

    let reopened = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    assert_eq!(
        call(
            reopened,
            Request::get(format!("/channels/{channel}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND,
    );
    assert!(
        !directory
            .path()
            .join(format!("artifacts/{channel}"))
            .exists()
    );
}

#[tokio::test]
async fn protected_channel_authorizes_every_data_route_before_foreground_admission() {
    let directory = TempDir::new().unwrap();
    let mut config = Config::new(directory.path());
    config.foreground_concurrency = 1;
    let app = Flywheel::open(config).await.unwrap().router();
    let registered = register(app.clone(), true).await;
    let channel = registered["channel"].as_str().unwrap();
    let token = registered["token"].as_str().unwrap();
    let held_body = b"hold the only foreground permit";
    let held_digest = hex::encode(Sha256::digest(held_body));
    let held_path = format!("/channels/{channel}/artifacts/sha256/{held_digest}");
    assert_eq!(
        call(
            app.clone(),
            Request::put(&held_path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(held_body.as_slice()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::CREATED,
    );
    let _held_response = call(
        app.clone(),
        Request::get(&held_path)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let digest = "0".repeat(64);
    let binding = json!({"algorithm": "sha256", "digest": digest}).to_string();
    let requests = [
        (Method::GET, format!("/artifacts/sha256/{digest}"), None),
        (
            Method::PUT,
            format!("/artifacts/sha256/{digest}"),
            Some(String::new()),
        ),
        (Method::GET, "/references/example".to_owned(), None),
        (Method::PUT, "/references/example".to_owned(), Some(binding)),
        (Method::DELETE, "/references/example".to_owned(), None),
        (Method::GET, "/build-cache/http/example".to_owned(), None),
        (
            Method::PUT,
            "/build-cache/http/example".to_owned(),
            Some(String::new()),
        ),
        (Method::GET, format!("/build-cache/bazel/ac/{digest}"), None),
        (
            Method::PUT,
            format!("/build-cache/bazel/ac/{digest}"),
            Some(String::new()),
        ),
        (
            Method::GET,
            format!("/build-cache/bazel/cas/{digest}"),
            None,
        ),
        (
            Method::PUT,
            format!("/build-cache/bazel/cas/{digest}"),
            Some(String::new()),
        ),
        (
            Method::GET,
            "/proxy/go/example.com/mod/@v/list".to_owned(),
            None,
        ),
        (
            Method::GET,
            "/proxy/python/simple/example/".to_owned(),
            None,
        ),
        (Method::GET, "/proxy/python/files/encoded".to_owned(), None),
        (Method::GET, "/proxy/npm/example".to_owned(), None),
        (
            Method::GET,
            "/proxy/cargo/index/config.json".to_owned(),
            None,
        ),
        (
            Method::GET,
            "/proxy/cargo/index/ex/am/example".to_owned(),
            None,
        ),
        (
            Method::GET,
            "/proxy/cargo/crates/example/1.0.0/download".to_owned(),
            None,
        ),
    ];

    for (method, suffix, body) in requests {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("/channels/{channel}{suffix}"));
        if suffix == "/references/example" && body.is_some() {
            request = request.header(header::CONTENT_TYPE, "application/json");
        }
        let response = call(
            app.clone(),
            request
                .body(body.map_or_else(Body::empty, Body::from))
                .unwrap(),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "protected route did not authorize {suffix}",
        );
    }
}

#[tokio::test]
async fn malformed_json_still_precedes_protected_channel_authorization() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = register(app.clone(), true).await;
    let channel = registered["channel"].as_str().unwrap();

    assert_eq!(
        call(
            app.clone(),
            Request::put(format!("/channels/{channel}/references/example"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST,
    );
}

#[tokio::test]
async fn channel_data_heads_match_get_headers_without_bodies() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = register(app.clone(), false).await;
    let channel = registered["channel"].as_str().unwrap();
    let body = b"channel head behavior";
    let digest = hex::encode(Sha256::digest(body));
    let paths = [
        format!("/channels/{channel}/artifacts/sha256/{digest}"),
        format!("/channels/{channel}/build-cache/bazel/cas/{digest}"),
    ];

    for path in paths {
        assert!(
            call(
                app.clone(),
                Request::put(&path)
                    .body(Body::from(body.as_slice()))
                    .unwrap(),
            )
            .await
            .status()
            .is_success(),
        );
        let head = call(
            app.clone(),
            Request::head(&path)
                .header(header::RANGE, "bytes=1-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(
            head.headers()[header::CONTENT_LENGTH],
            body.len().to_string()
        );
        assert!(head.headers().get(header::CONTENT_RANGE).is_none());
        assert!(
            to_bytes(head.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn custom_channels_are_isolated_from_default_and_each_other() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let first = register(app.clone(), false).await;
    let second = register(app.clone(), false).await;
    let body = b"isolated";
    let digest = hex::encode(Sha256::digest(body));
    let first_path = format!(
        "/channels/{}/artifacts/sha256/{digest}",
        first["channel"].as_str().unwrap()
    );
    let second_path = format!(
        "/channels/{}/artifacts/sha256/{digest}",
        second["channel"].as_str().unwrap()
    );

    assert_eq!(
        call(
            app.clone(),
            Request::put(&first_path)
                .body(Body::from(body.as_slice()))
                .unwrap()
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(&first_path).body(Body::empty()).unwrap()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            Request::get(&second_path).body(Body::empty()).unwrap()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            app,
            Request::get(format!("/artifacts/sha256/{digest}"))
                .body(Body::empty())
                .unwrap()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn protected_channels_accept_bearer_or_basic_for_every_operation() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = register(app.clone(), true).await;
    let channel = registered["channel"].as_str().unwrap();
    let token = registered["token"].as_str().unwrap();
    let channel_path = format!("/channels/{channel}");

    let denied = call(
        app.clone(),
        Request::get(&channel_path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert!(denied.headers().contains_key(header::WWW_AUTHENTICATE));

    assert_eq!(
        call(
            app.clone(),
            Request::get(&channel_path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK
    );

    let basic = STANDARD.encode(format!("ignored:{token}"));
    assert_eq!(
        call(
            app,
            Request::patch(&channel_path)
                .header(header::AUTHORIZATION, format!("Basic {basic}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"expiry_seconds": 60}).to_string()))
                .unwrap(),
        )
        .await
        .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn deletion_removes_channel_data_and_leaves_no_servable_registry_entry() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = register(app.clone(), false).await;
    let channel = registered["channel"].as_str().unwrap();
    let channel_path = format!("/channels/{channel}");

    let response = call(
        app.clone(),
        Request::delete(&channel_path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        call(
            app.clone(),
            Request::get(&channel_path).body(Body::empty()).unwrap()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    drop(response);
    drop(app);
    let reopened = Flywheel::open(Config::new(directory.path())).await.unwrap();
    assert_eq!(
        call(
            reopened.router(),
            Request::get(&channel_path).body(Body::empty()).unwrap()
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

/// A registered channel's channel-only record round-trips through a real store reopen.
#[tokio::test]
async fn registered_channel_survives_reopen_without_legacy_policy_fields() {
    let directory = TempDir::new().unwrap();
    let app = Flywheel::open(Config::new(directory.path()))
        .await
        .unwrap()
        .router();
    let registered = call(
        app.clone(),
        Request::post("/channels")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"access_control": false, "expiry_seconds": 1234}).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(registered.status(), StatusCode::CREATED);
    let registered: Value =
        serde_json::from_slice(&to_bytes(registered.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(registered["expiry_seconds"], 1234);
    assert!(registered.get("max_bytes").is_none());
    let channel_path = format!("/channels/{}", registered["channel"].as_str().unwrap());

    drop(app);
    let reopened = Flywheel::open(Config::new(directory.path())).await.unwrap();
    let view = call(
        reopened.router(),
        Request::get(&channel_path).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(view.status(), StatusCode::OK);
    let view: Value =
        serde_json::from_slice(&to_bytes(view.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(view["expiry_seconds"], 1234);
    assert_eq!(view["access_control"], false);
    assert_eq!(view["state"], "active");
    assert!(view.get("max_bytes").is_none());
}
