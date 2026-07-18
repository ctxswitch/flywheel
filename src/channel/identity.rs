use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ChannelId(Ulid);

impl ChannelId {
    pub const DEFAULT: Self = Self(Ulid::nil());

    pub fn new() -> Self {
        loop {
            let id = Self(Ulid::new());
            if id != Self::DEFAULT {
                return id;
            }
        }
    }

    pub fn as_key(self) -> [u8; 26] {
        self.to_string()
            .as_bytes()
            .try_into()
            .expect("a ULID is always 26 bytes")
    }
}

impl Default for ChannelId {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ChannelId {
    type Err = ChannelIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = Ulid::from_string(value).map_err(|_| ChannelIdError)?;
        let channel = Self(parsed);
        if channel.to_string() != value {
            return Err(ChannelIdError);
        }
        Ok(channel)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("channel must be a canonical uppercase ULID")]
pub struct ChannelIdError;
