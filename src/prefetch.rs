//! Wire format for the build-cache prefetch stream, shared by the server route and
//! the cacheprog client.
//!
//! A response is a sequence of frames, one per requested digest, in no guaranteed
//! order: clients match frames to requests by digest, which lets a routing agent
//! fan a request out across shards and concatenate the sub-responses. Each frame
//! is one JSON
//! header line terminated by `\n`, followed by exactly `stored_len` stored bytes
//! (a zstd frame verbatim for compressed entries, identity bytes otherwise). A
//! miss frame has `miss: true` and no body. The response deliberately carries no
//! `Content-Encoding`, so nothing between the two ends decompresses the framing
//! away.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// Content type of a prefetch response body.
pub const CONTENT_TYPE: &str = "application/x-flywheel-prefetch";

/// Maximum number of digests a single prefetch request may carry.
pub const MAX_DIGESTS: usize = 65_536;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrefetchRequest {
    pub digests: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FrameEncoding {
    #[default]
    Identity,
    Zstd,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct FrameHeader {
    pub digest: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub miss: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub stored_len: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub content_len: u64,
    #[serde(default, skip_serializing_if = "is_identity")]
    pub encoding: FrameEncoding,
}

impl FrameHeader {
    pub fn miss(digest: String) -> Self {
        Self {
            digest,
            miss: true,
            stored_len: 0,
            content_len: 0,
            encoding: FrameEncoding::Identity,
        }
    }
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn is_identity(encoding: &FrameEncoding) -> bool {
    *encoding == FrameEncoding::Identity
}

pub fn encode_header(header: &FrameHeader) -> Vec<u8> {
    let mut line = serde_json::to_vec(header).expect("frame header serializes");
    line.push(b'\n');
    line
}

/// Reads prefetch frames off any buffered reader. `next_frame` returns `None` at a
/// clean end of stream and an error when the stream ends mid-header or mid-body,
/// since continuing past either would mis-frame everything that follows.
pub struct FrameDecoder<R> {
    reader: R,
    line: Vec<u8>,
}

impl<R> FrameDecoder<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line: Vec::new(),
        }
    }

    pub async fn next_frame(&mut self) -> anyhow::Result<Option<(FrameHeader, Vec<u8>)>> {
        self.line.clear();
        let read = self.reader.read_until(b'\n', &mut self.line).await?;
        if read == 0 {
            return Ok(None);
        }
        anyhow::ensure!(
            self.line.last() == Some(&b'\n'),
            "prefetch stream ended mid-header"
        );
        let header: FrameHeader = serde_json::from_slice(&self.line)?;
        let mut body = vec![0; usize::try_from(header.stored_len)?];
        self.reader
            .read_exact(&mut body)
            .await
            .map_err(|_| anyhow::anyhow!("prefetch stream ended mid-body"))?;
        Ok(Some((header, body)))
    }
}
