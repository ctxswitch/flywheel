use serde::{Deserialize, Serialize};
use std::fmt;

const MAX_REFERENCE_LEN: usize = 512;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Reference(String);

impl Reference {
    pub fn parse(value: impl Into<String>) -> Result<Self, ReferenceError> {
        let value = value.into();
        if !Self::is_valid(&value) {
            return Err(ReferenceError);
        }
        Ok(Self(value))
    }

    /// The same rule `parse` applies, for callers that only need the verdict and
    /// would otherwise allocate an owned reference just to drop it.
    pub fn is_valid(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= MAX_REFERENCE_LEN
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
            })
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for Reference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("reference must contain 1 to 512 URL-safe characters")]
pub struct ReferenceError;
