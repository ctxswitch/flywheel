pub mod http;

use ::http::{HeaderMap, header};

/// Parses a `Content-Length` so a body can be sized before it is read: uploads size
/// their reservation from the request header, proxied downloads from the upstream
/// response header.
pub(crate) fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}
