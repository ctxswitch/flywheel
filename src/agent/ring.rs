//! The frozen v1 routing contract: versioned routing-key encoding and the Ketama-style
//! consistent-hash continuum (see `docs/plans/sidecar-hash-ring-scaling.md` §5.3).
//!
//! Every agent instance must derive identical placement from identical membership, so
//! the encoding, hash, virtual-node count, and sort order here are load-bearing wire
//! contract, not implementation detail. The fixed vectors in the tests below pin them.

use sha2::{Digest as _, Sha256};
use std::net::SocketAddr;

/// Domain separator and version of the routing-key encoding.
pub const ROUTING_VERSION: &str = "flywheel-routing-v1";

/// Virtual nodes placed on the continuum for every member; all members weigh equally.
pub const VIRTUAL_NODES: u16 = 160;

/// Encodes the versioned routing key: `len_u16_be(part) || part` for each of the
/// version, the object kind, and the canonical object ID. Returns `None` when a part
/// exceeds the `u16` length prefix, which no supported route produces.
pub fn routing_key(kind: &str, id: &str) -> Option<Vec<u8>> {
    let parts = [ROUTING_VERSION, kind, id];
    let mut key = Vec::with_capacity(parts.iter().map(|part| part.len() + 2).sum());
    for part in parts {
        let length = u16::try_from(part.len()).ok()?;
        key.extend_from_slice(&length.to_be_bytes());
        key.extend_from_slice(part.as_bytes());
    }
    Some(key)
}

/// Ring position of a routing key: the first 8 bytes of its SHA-256, big-endian.
pub fn key_position(kind: &str, id: &str) -> Option<u64> {
    routing_key(kind, id).map(|key| position(&key))
}

fn position(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 yields 32 bytes"))
}

/// One shard on the ring. The identity is the canonical SRV target; the address is
/// only where that identity currently lives and never contributes ring positions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RingMember {
    pub id: String,
    pub address: SocketAddr,
}

/// An immutable continuum over a membership snapshot. Rebuilt wholesale on any
/// membership or ejection change and swapped atomically behind the routing state.
#[derive(Debug)]
pub struct Ring {
    members: Vec<RingMember>,
    points: Vec<(u64, u32)>,
    fingerprint: String,
}

impl Ring {
    /// Builds the continuum. Input order is insignificant: members are sorted and
    /// deduplicated by ID first, so every agent converges on the same ring.
    pub fn new(mut members: Vec<RingMember>) -> Self {
        members.sort_by(|a, b| a.id.cmp(&b.id));
        members.dedup_by(|a, b| a.id == b.id);
        let mut points = Vec::with_capacity(members.len() * usize::from(VIRTUAL_NODES));
        for (index, member) in members.iter().enumerate() {
            for vnode in 0..VIRTUAL_NODES {
                let mut hasher = Sha256::new();
                hasher.update(member.id.as_bytes());
                hasher.update(vnode.to_be_bytes());
                let digest = hasher.finalize();
                let point = u64::from_be_bytes(digest[..8].try_into().expect("sha256 slice"));
                points.push((point, u32::try_from(index).expect("member count fits u32")));
            }
        }
        points.sort_unstable();
        let ids = members
            .iter()
            .map(|member| member.id.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let fingerprint = hex::encode(Sha256::digest(ids.as_bytes()));
        Self {
            members,
            points,
            fingerprint,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn members(&self) -> &[RingMember] {
        &self.members
    }

    /// The member owning `position`: the first continuum point at or past it,
    /// wrapping to the smallest point.
    pub fn owner(&self, position: u64) -> Option<&RingMember> {
        if self.points.is_empty() {
            return None;
        }
        let index = self
            .points
            .partition_point(|(point, _)| *point < position)
            .checked_rem(self.points.len())
            .expect("points is non-empty");
        let (_, member) = self.points[index];
        Some(&self.members[member as usize])
    }

    /// Hex SHA-256 of the sorted member IDs joined with `\n`. Diagnostic only.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}
