use super::recent_use::RecentUse;
use crate::{
    artifact::{ArtifactId, Digest},
    channel::ChannelId,
};

fn artifact(index: u32) -> ArtifactId {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&index.to_le_bytes());
    ArtifactId::from_digest(Digest::from_bytes(bytes))
}

#[test]
fn a_mark_survives_exactly_one_rotation() {
    let filter = RecentUse::new(4096);
    let channel = ChannelId::DEFAULT;
    filter.mark(channel, artifact(1));
    assert!(filter.seen(channel, artifact(1)));
    filter.rotate();
    assert!(filter.seen(channel, artifact(1)));
    filter.rotate();
    assert!(!filter.seen(channel, artifact(1)));
}

#[test]
fn the_same_channel_and_artifact_probe_the_same_slots() {
    let filter = RecentUse::new(4096);
    filter.mark(ChannelId::DEFAULT, artifact(7));
    // The mark is keyed on both halves of the identity, so neither a different
    // artifact in the same channel nor the same artifact elsewhere reads as seen.
    assert!(!filter.seen(ChannelId::DEFAULT, artifact(8)));
    assert!(!filter.seen(ChannelId::new(), artifact(7)));
}

#[test]
fn derived_probes_keep_false_positives_rare() {
    let filter = RecentUse::new(4096);
    let channel = ChannelId::DEFAULT;
    for index in 0..100 {
        filter.mark(channel, artifact(index));
    }
    // Four probes over a 2.4%-full filter should almost never all collide. A
    // degenerate derivation (probes collapsing onto one slot) lands near 2.4%,
    // an order of magnitude above this bound.
    let false_positives = (1_000..2_000)
        .filter(|&index| filter.seen(channel, artifact(index)))
        .count();
    assert!(
        false_positives < 5,
        "{false_positives} false positives in 1000 probes"
    );
}
