//! Internals the test kit, the benchmarks and the fuzz targets reach into: the commit retry loop,
//! the store stack and its fault classes, and the sidecar machinery. Not part of the stable API
//! (docs/SEMVER.md): anything here may change in any release.

pub use crate::commit::{AttemptError, ConflictKind, LoopStats, commit_with_retries};
pub use crate::index::build::BundleBuild;
pub use crate::index::cache::{HandleCache, SidecarCache, entry_name};
pub use crate::index::discover::{Discovery, Listed, sidecar_location};
pub use crate::index::key::encode::encode_row;
pub use crate::index::key::{KeyIndex, KeyKind, KeyRows, key_spec};
pub use crate::index::spec::{INDEXES_PROPERTY, canonical_json, spec_hash};
pub use crate::store::class::{Class, StoreFault, classify};
pub use crate::store::metrics::MetricsStore;
pub use crate::store::probe::{
    Capabilities, Support, probe_conditional_writes, require_conditional_writes,
};
pub use crate::store::retry::RetryStore;
pub use crate::store::stack::Store;
pub use crate::store::tags::TagStore;
pub use crate::store::time::{ServerClock, server_now};

/// The SplitMix64 output function over an already advanced state.
pub fn splitmix64(state: u64) -> u64 {
    crate::clock::splitmix64(state)
}

/// A random hex name of 32 characters drawn from `clock`.
pub fn random_name(clock: &dyn crate::clock::Clock) -> String {
    crate::clock::random_name(clock)
}
