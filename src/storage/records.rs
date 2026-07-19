use serde::{Serialize, de::DeserializeOwned};

const SCHEMA_VERSION: u8 = 1;

pub(crate) fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>, RecordError> {
    let payload = postcard::to_stdvec(value).map_err(RecordError::Encode)?;
    let mut encoded = Vec::with_capacity(payload.len() + 1);
    encoded.push(SCHEMA_VERSION);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub(crate) fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, RecordError> {
    let Some((&version, payload)) = bytes.split_first() else {
        return Err(RecordError::Empty);
    };
    if version != SCHEMA_VERSION {
        return Err(RecordError::UnsupportedVersion(version));
    }
    postcard::from_bytes(payload).map_err(RecordError::Decode)
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("durable record is empty")]
    Empty,
    #[error("unsupported durable record schema version {0}")]
    UnsupportedVersion(u8),
    #[error("could not encode durable record: {0}")]
    Encode(postcard::Error),
    #[error("could not decode durable record: {0}")]
    Decode(postcard::Error),
}

