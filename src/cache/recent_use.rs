use crate::{artifact::ArtifactId, channel::ChannelId};
use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// Number of hash probes per element.
const PROBES: usize = 4;

/// Two fixed-size Bloom filters approximating recent GET activity. GET marks the
/// active filter; a maintenance rotation swaps the active index and clears the older
/// filter, so a mark survives exactly one rotation. Reads racing a rotation may lose a
/// mark, which only affects cache quality. Consulted in Normal mode and ignored in
/// Reclaiming so a saturated filter can never block disk reclamation.
pub(crate) struct RecentUse {
    filters: [Box<[AtomicU64]>; 2],
    bits: usize,
    active: AtomicUsize,
}

impl RecentUse {
    pub(crate) fn new(bits: usize) -> Self {
        // Round up to whole 64-bit words. `Config::validate` rejects a zero width, so
        // the filter is never empty.
        let words = bits.div_ceil(64);
        Self {
            filters: [new_words(words), new_words(words)],
            bits: words * 64,
            active: AtomicUsize::new(0),
        }
    }

    pub(crate) fn mark(&self, channel: ChannelId, artifact: ArtifactId) {
        let filter = &self.filters[self.active.load(Ordering::Relaxed) & 1];
        for position in self.positions(channel, artifact) {
            filter[position / 64].fetch_or(1 << (position % 64), Ordering::Relaxed);
        }
    }

    pub(crate) fn seen(&self, channel: ChannelId, artifact: ArtifactId) -> bool {
        let positions = self.positions(channel, artifact);
        self.contains(0, &positions) || self.contains(1, &positions)
    }

    /// Clears the passive filter and makes it active, retaining the marks placed in the
    /// previously active filter for one more window.
    pub(crate) fn rotate(&self) {
        let next = (self.active.load(Ordering::Relaxed) & 1) ^ 1;
        for word in self.filters[next].iter() {
            word.store(0, Ordering::Relaxed);
        }
        self.active.store(next, Ordering::Relaxed);
    }

    fn contains(&self, filter: usize, positions: &[usize; PROBES]) -> bool {
        let filter = &self.filters[filter];
        positions.iter().all(|&position| {
            filter[position / 64].load(Ordering::Relaxed) & (1 << (position % 64)) != 0
        })
    }

    fn positions(&self, channel: ChannelId, artifact: ArtifactId) -> [usize; PROBES] {
        let mut base = std::collections::hash_map::DefaultHasher::new();
        channel.hash(&mut base);
        artifact.digest().as_bytes().hash(&mut base);
        let seed = base.finish();
        // Kirsch-Mitzenmacher: two independent halves of the one hash generate every
        // probe with the same false-positive behaviour as independent hashes, so a
        // mark costs one hash instead of five. Forcing the step odd keeps it non-zero,
        // so the probes never all collapse onto one slot.
        let start = seed & 0xffff_ffff;
        let step = (seed >> 32) | 1;
        let bits = self.bits as u64;
        std::array::from_fn(|probe| {
            (start.wrapping_add(step.wrapping_mul(probe as u64)) % bits) as usize
        })
    }
}

fn new_words(words: usize) -> Box<[AtomicU64]> {
    (0..words).map(|_| AtomicU64::new(0)).collect()
}
