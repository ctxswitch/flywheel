//! Helpers shared by the integration test targets. This file is not a test
//! target of its own: each test that needs it pulls it in with
//! `#[path = "common/mod.rs"] mod common;`.

use axum::{body::Body, http::Request, response::Response};
use tower::ServiceExt as _;

/// Drives one request through a router without binding a port.
pub async fn call(app: axum::Router, request: Request<Body>) -> Response {
    app.oneshot(request).await.unwrap()
}
