//! Time and randomness for the client side: monotonic time, sleeping, and backoff jitter.
//!
//! Everything in moraine that waits or draws a random number goes through [`Clock`], so the
//! deterministic simulation in `moraine-testkit` can replace both and replay a run exactly.
//! Wall-clock time is never used for expiry decisions; those use the object store's clock.
//!
//! Applications only meet this module to give a database a clock of their own
//! ([`DatabaseBuilder::clock`](crate::DatabaseBuilder::clock)); [`SystemClock`] is the default.

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;

/// Monotonic time, sleeping and random numbers for one client.
pub trait Clock: Send + Sync + Debug + 'static {
    /// Monotonic time since an arbitrary fixed point. Never goes backwards.
    fn now(&self) -> Duration;
    /// Waits for `d`.
    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()>;
    /// A uniformly distributed random number (backoff jitter, probe object names).
    fn random_u64(&self) -> u64;

    /// The client's wall clock. Only used for values written into the log for other tools
    /// (Delta's `commitInfo.timestamp`, `add.modificationTime`); never for expiry decisions.
    fn wall_now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    /// Waits for an event another task of the same process delivers (a group commit's
    /// result). The simulation's clock polls `fut` between scheduling steps instead, because its
    /// scheduler only lets a task wait at a store call or a timer.
    fn wait_for<'a>(&self, fut: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        fut
    }

    /// Runs `fut` on a task of its own, detached. A table handle commits its writes this way
    /// (D64), so a caller that stops waiting cannot stop a commit other callers wait for. The
    /// simulation's clock registers the task with its scheduler, which counts it as a client
    /// while it runs. Must be called inside a Tokio runtime.
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        tokio::spawn(fut);
    }
}

/// The process clock: [`Instant`] and tokio timers. Randomness from SplitMix64 seeded by the
/// process's hasher keys and the time of creation.
#[derive(Debug)]
pub struct SystemClock {
    start: Instant,
    state: AtomicU64,
}

impl SystemClock {
    /// A clock on the system's time, with random names seeded from the process's hasher.
    pub fn new() -> Self {
        use std::hash::{BuildHasher, RandomState};
        let seed = RandomState::new().hash_one(std::time::SystemTime::now());
        SystemClock {
            start: Instant::now(),
            state: AtomicU64::new(seed),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }

    fn sleep(&self, d: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(d))
    }

    fn random_u64(&self) -> u64 {
        splitmix64(
            self.state
                .fetch_add(GOLDEN, Ordering::Relaxed)
                .wrapping_add(GOLDEN),
        )
    }
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// The SplitMix64 output function over an already advanced state.
pub(crate) fn splitmix64(state: u64) -> u64 {
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A random hex name of 32 characters drawn from `clock`, for probe and scratch objects.
pub(crate) fn random_name(clock: &dyn Clock) -> String {
    format!("{:016x}{:016x}", clock.random_u64(), clock.random_u64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_is_monotonic_and_random() {
        let c = SystemClock::new();
        let a = c.now();
        let b = c.now();
        assert!(b >= a);
        let names: std::collections::BTreeSet<_> = (0..100).map(|_| random_name(&c)).collect();
        assert_eq!(names.len(), 100);
    }
}
