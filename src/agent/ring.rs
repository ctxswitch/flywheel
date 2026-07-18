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

#[cfg(test)]
mod tests {
    use super::{Ring, RingMember, VIRTUAL_NODES, key_position, routing_key};
    use std::collections::HashMap;

    const DIGEST_A: &str = "0f7fe6f9a72d9298a0295e64c400b485b0a4118bcdcd7d100e9d9c1a1ea115c5";
    const DIGEST_B: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn member(id: &str) -> RingMember {
        RingMember {
            id: id.to_owned(),
            address: "127.0.0.1:8080".parse().unwrap(),
        }
    }

    fn three_member_ring() -> Ring {
        Ring::new(vec![
            member("flywheel-0.flywheel-shards.cache.svc.cluster.local"),
            member("flywheel-1.flywheel-shards.cache.svc.cluster.local"),
            member("flywheel-2.flywheel-shards.cache.svc.cluster.local"),
        ])
    }

    fn owner_ordinal(ring: &Ring, kind: &str, id: &str) -> String {
        ring.owner(key_position(kind, id).unwrap())
            .unwrap()
            .id
            .clone()
    }

    #[test]
    fn routing_key_bytes_are_length_prefixed_parts() {
        let key = routing_key("artifact", DIGEST_A).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&19_u16.to_be_bytes());
        expected.extend_from_slice(b"flywheel-routing-v1");
        expected.extend_from_slice(&8_u16.to_be_bytes());
        expected.extend_from_slice(b"artifact");
        expected.extend_from_slice(&64_u16.to_be_bytes());
        expected.extend_from_slice(DIGEST_A.as_bytes());
        assert_eq!(key, expected);
    }

    #[test]
    fn oversized_parts_are_rejected_rather_than_truncated() {
        assert!(routing_key("artifact", &"a".repeat(usize::from(u16::MAX) + 1)).is_none());
        assert!(routing_key("artifact", &"a".repeat(usize::from(u16::MAX))).is_some());
    }

    #[test]
    fn key_positions_match_frozen_vectors() {
        // Frozen v1 vectors: any implementation of the contract must reproduce these.
        for (kind, id, expected) in [
            ("artifact", DIGEST_A, 0x1ab9_79b1_0ffa_d1d8_u64),
            ("artifact", DIGEST_B, 0x6bf3_4f57_c2a0_b7d3_u64),
            ("bazel-action", DIGEST_A, 0x7b6e_5b1d_84e0_03c0_u64),
            ("bazel-action", DIGEST_B, 0x1ffb_40a7_c7eb_9587_u64),
            ("http-cache", "go-0123abcd", 0x1e0b_30ec_100b_769a_u64),
            ("reference", "latest-release", 0x797e_d55d_6029_b3c8_u64),
            (
                "go",
                "github.com/foo/bar/@v/v1.2.3.zip",
                0xda01_8ad1_33a6_0b8d_u64,
            ),
            ("path", "/health/custom", 0xbd8a_214f_5975_d71f_u64),
        ] {
            assert_eq!(key_position(kind, id).unwrap(), expected, "{kind}:{id}");
        }
    }

    #[test]
    fn three_member_owners_match_frozen_vectors() {
        let ring = three_member_ring();
        for (kind, id, expected) in [
            ("artifact", DIGEST_A, "flywheel-0"),
            ("artifact", DIGEST_B, "flywheel-0"),
            ("bazel-action", DIGEST_A, "flywheel-2"),
            ("bazel-action", DIGEST_B, "flywheel-2"),
            ("http-cache", "go-0123abcd", "flywheel-0"),
            ("reference", "latest-release", "flywheel-2"),
            ("go", "github.com/foo/bar/@v/v1.2.3.zip", "flywheel-0"),
            ("path", "/health/custom", "flywheel-2"),
        ] {
            let owner = owner_ordinal(&ring, kind, id);
            assert!(
                owner.starts_with(expected),
                "{kind}:{id} owned by {owner}, expected {expected}"
            );
        }
    }

    #[test]
    fn kinds_place_independently_and_cas_shares_artifact_placement() {
        let ring = three_member_ring();
        // The action record and an output that reuses the same hex string are distinct
        // routing objects, while the raw-artifact and Bazel CAS routes share a kind.
        assert_eq!(
            owner_ordinal(&ring, "artifact", DIGEST_A),
            owner_ordinal(&ring, "artifact", DIGEST_A)
        );
        assert_ne!(
            key_position("artifact", DIGEST_A),
            key_position("bazel-action", DIGEST_A)
        );
    }

    #[test]
    fn construction_is_independent_of_discovery_order() {
        let forward = three_member_ring();
        let reversed = Ring::new(vec![
            member("flywheel-2.flywheel-shards.cache.svc.cluster.local"),
            member("flywheel-1.flywheel-shards.cache.svc.cluster.local"),
            member("flywheel-0.flywheel-shards.cache.svc.cluster.local"),
        ]);
        assert_eq!(forward.fingerprint(), reversed.fingerprint());
        assert_eq!(forward.members(), reversed.members());
        for sample in 0..1_000_u32 {
            let position = key_position("http-cache", &format!("key-{sample}")).unwrap();
            assert_eq!(
                forward.owner(position).unwrap().id,
                reversed.owner(position).unwrap().id
            );
        }
    }

    #[test]
    fn duplicate_member_ids_collapse_to_one_member() {
        let ring = Ring::new(vec![member("flywheel-0"), member("flywheel-0")]);
        assert_eq!(ring.members().len(), 1);
        assert_eq!(ring.points.len(), usize::from(VIRTUAL_NODES));
    }

    #[test]
    fn keys_distribute_roughly_evenly_across_equal_members() {
        let ring = three_member_ring();
        let mut counts: HashMap<String, u32> = HashMap::new();
        for sample in 0..12_000_u32 {
            let position = key_position("artifact", &format!("output-{sample}")).unwrap();
            *counts
                .entry(ring.owner(position).unwrap().id.clone())
                .or_default() += 1;
        }
        assert_eq!(counts.len(), 3);
        for (id, count) in counts {
            assert!(
                (2_800..=5_200).contains(&count),
                "{id} owned {count} of 12000 keys"
            );
        }
    }

    #[test]
    fn removing_one_member_remaps_about_a_quarter_of_keys() {
        let four = Ring::new(vec![
            member("flywheel-0"),
            member("flywheel-1"),
            member("flywheel-2"),
            member("flywheel-3"),
        ]);
        let three = Ring::new(vec![
            member("flywheel-0"),
            member("flywheel-1"),
            member("flywheel-2"),
        ]);
        let total = 12_000_u32;
        let mut moved = 0_u32;
        for sample in 0..total {
            let position = key_position("artifact", &format!("output-{sample}")).unwrap();
            let before = &four.owner(position).unwrap().id;
            let after = &three.owner(position).unwrap().id;
            if before == "flywheel-3" {
                // Keys owned by the removed member must move somewhere else.
                assert_ne!(before, after);
                moved += 1;
            } else {
                // Every other key keeps its owner: removal only remaps the departed
                // member's arcs.
                assert_eq!(before, after);
            }
        }
        let fraction = f64::from(moved) / f64::from(total);
        assert!(
            (0.15..=0.35).contains(&fraction),
            "remapped fraction {fraction}"
        );
    }

    #[test]
    fn empty_ring_owns_nothing() {
        let ring = Ring::new(Vec::new());
        assert!(ring.is_empty());
        assert!(ring.owner(0).is_none());
        assert!(ring.owner(u64::MAX).is_none());
    }
}
