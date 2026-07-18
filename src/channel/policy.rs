use super::TokenDigest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Access {
    Open,
    Token(TokenDigest),
}

impl Access {
    pub fn is_protected(self) -> bool {
        matches!(self, Self::Token(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lifecycle {
    Active,
    Deleting,
}
