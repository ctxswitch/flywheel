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

    /// The channel's durable key prefix: the canonical ULID text, encoded straight
    /// into a stack buffer. `Display` itself calls `array_to_str`, so the bytes are
    /// identical to the string form without the intermediate allocation.
    pub fn as_key(self) -> [u8; ulid::ULID_LEN] {
        let mut key = [0_u8; ulid::ULID_LEN];
        self.0.array_to_str(&mut key);
        key
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
