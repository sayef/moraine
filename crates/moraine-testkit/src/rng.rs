//! A small seeded generator (SplitMix64). Deterministic on every platform, no dependencies.

use moraine::testing::splitmix64;

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// SplitMix64: fast, seedable, good enough for scheduling and fault decisions.
#[derive(Clone, Debug)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        SimRng { state: seed }
    }

    /// A generator for a sub-purpose, so adding draws in one place does not shift another.
    pub fn fork(&mut self, salt: u64) -> SimRng {
        SimRng::new(self.next_u64() ^ salt.wrapping_mul(GOLDEN))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN);
        splitmix64(self.state)
    }

    /// Uniform in `0..n` (`n > 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        // Lemire's multiply-shift; the bias is far below anything a test could notice.
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_in_range() {
        let a: Vec<u64> = {
            let mut r = SimRng::new(7);
            (0..5).map(|_| r.next_u64()).collect()
        };
        let b: Vec<u64> = {
            let mut r = SimRng::new(7);
            (0..5).map(|_| r.next_u64()).collect()
        };
        assert_eq!(a, b);
        let mut r = SimRng::new(1);
        for _ in 0..1000 {
            assert!(r.below(3) < 3);
            let x = r.range(5, 9);
            assert!((5..=9).contains(&x));
        }
        assert!(!r.chance(0.0));
        assert!(r.chance(1.0));
    }
}
