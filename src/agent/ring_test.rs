use super::ring::{Ring, RingMember, key_position, routing_key};
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
fn distinct_kinds_place_the_same_id_independently() {
    // The action record and an output that reuses the same hex string are distinct
    // routing objects.
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
