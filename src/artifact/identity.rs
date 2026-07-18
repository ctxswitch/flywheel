use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

const SHA256_BYTES: usize = 32;
const SHA256_HEX_LEN: usize = SHA256_BYTES * 2;

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Digest([u8; SHA256_BYTES]);

impl Digest {
    pub fn parse(value: &str) -> Result<Self, IdentityError> {
        if value.len() != SHA256_HEX_LEN
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
        {
            return Err(IdentityError::InvalidDigest);
        }

        let mut bytes = [0; SHA256_BYTES];
        hex::decode_to_slice(value, &mut bytes).map_err(|_| IdentityError::InvalidDigest)?;
        Ok(Self(bytes))
    }

    pub fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; SHA256_BYTES] {
        &self.0
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Digest")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for Digest {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ArtifactId {
    digest: Digest,
}

impl ArtifactId {
    pub const ALGORITHM: &'static str = "sha256";

    pub fn parse(algorithm: &str, digest: &str) -> Result<Self, IdentityError> {
        if algorithm != Self::ALGORITHM {
            return Err(IdentityError::UnsupportedAlgorithm);
        }
        Ok(Self {
            digest: Digest::parse(digest)?,
        })
    }

    pub fn from_digest(digest: Digest) -> Self {
        Self { digest }
    }

    pub fn algorithm(&self) -> &'static str {
        Self::ALGORITHM
    }

    pub fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", Self::ALGORITHM, self.digest)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IdentityError {
    #[error("only sha256 artifact identities are supported")]
    UnsupportedAlgorithm,
    #[error("digest must be 64 lowercase hexadecimal characters")]
    InvalidDigest,
}
