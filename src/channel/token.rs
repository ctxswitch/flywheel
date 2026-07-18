use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::fmt;
use subtle::ConstantTimeEq;

pub struct ChannelToken(String);

impl ChannelToken {
    pub fn generate() -> Self {
        let mut secret = [0_u8; 32];
        rand::rng().fill_bytes(&mut secret);
        Self(format!("flywheel_{}", URL_SAFE_NO_PAD.encode(secret)))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn digest(&self) -> TokenDigest {
        TokenDigest(Sha256::digest(self.0.as_bytes()).into())
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenDigest([u8; 32]);

impl TokenDigest {
    pub fn verify(&self, candidate: &str) -> bool {
        let candidate: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
        bool::from(self.0.ct_eq(&candidate))
    }
}

impl fmt::Debug for TokenDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TokenDigest([REDACTED])")
    }
}
