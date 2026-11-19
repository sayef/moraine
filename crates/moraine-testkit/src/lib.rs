//! Test tooling for moraine, built before the features it checks.
//!
//! - [`SimClock`] and [`ClientClock`]: controllable server time and client time.
//! - [`SimStore`]: an in-memory object store whose timestamps come from the [`SimClock`].
//! - [`ChaosStore`]: wraps any store and injects faults from a seed and a rule list, including
//!   ambiguous outcomes (applied, then the response is lost), and counts requests and bytes.
//! - [`Scheduler`]: runs logical clients on one thread and picks, from a seed, which pending
//!   store call or timer proceeds next, so a seed replays the same interleaving.
//! - [`model`]: the reference model of a table, and [`check`]: the invariant checker.
//! - [`datagen`]: data generators; [`table_log`]: a table's history read without moraine.
//! - [`fuzz`]: deterministic byte-level and JSON-aware mutation for the corruption fuzz tests.
//! - [`sim`]: runs one seed or a range of seeds and reports the first failing seed.

pub mod blobgen;
pub mod chaos;
pub mod check;
pub mod clock;
pub mod datagen;
pub mod fuzz;
pub mod golden;
pub mod maintcheck;
pub mod model;
pub mod rng;
pub mod sched;
pub mod sim;
pub mod sim_store;
pub mod table_log;
pub mod text;
pub mod toykind;
pub mod vectors;

pub use chaos::{ChaosStore, Conditional, FaultRules};
pub use clock::{ClientClock, SimClock};
pub use rng::SimRng;
pub use sched::Scheduler;
pub use sim_store::SimStore;
