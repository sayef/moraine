//! The invariant checker.
//!
//! Clients record what they intended and what they were told ([`Recorder`]); the test then hands
//! over the true commit log read from the store, and [`History::check`] compares the two with the
//! reference model. It checks the invariants that need no tables or indexes:
//!
//! | ID | Check |
//! |---|---|
//! | I1 | An acknowledged operation is in the log at the version it was acknowledged with. |
//! | I2 | An operation that returned an error (other than "outcome unknown") is not in the log. |
//! | I3 | Log versions are gapless from 1, every read equals the model replayed to its version, and (when the test supplies them) the rows physically stored at every version equal the model too. |
//! | I4 | With keys enforced, no read and no version holds a key twice. |
//! | I5 | Each client's reads never go back, and never miss the client's own acknowledged writes. |
//! | I10 | No operation is in the log twice, and each log entry is exactly what was intended. |
//! | I11 | A caller transaction id (D32) commits at most once, an application's versions only grow along the log, an operation told "already applied" is not in the log and its id (or a later one of the same application) is committed at or before the version it was told, and (when the test supplies them) each version's `txn` actions are exactly the ids of the operations placed there. |
//!
//! I6 to I9 are added here as their features land.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::model::{ModelOp, Row, TableModel, Txn};

/// What a client was told about one of its operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Committed as this version.
    Acked { version: u64 },
    /// Returned an error that promises the write is not visible.
    Failed(String),
    /// Returned "outcome unknown".
    Unknown,
    /// A write with a transaction id was told it had been applied before (D32); `version` is
    /// the table version it was told includes that earlier commit.
    AlreadyApplied { version: u64 },
}

/// One thing a client saw, in its own order.
#[derive(Clone, Debug)]
enum Event<R: Row> {
    Read { version: u64, rows: Vec<R> },
    Outcome { op: String, outcome: Outcome },
}

/// A broken invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub invariant: &'static str,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} violated: {}", self.invariant, self.detail)
    }
}

impl std::error::Error for Violation {}

struct Inner<R: Row> {
    intended: BTreeMap<String, (usize, ModelOp<R>)>,
    /// Transaction ids of the operations that carry one.
    txns: BTreeMap<String, Txn>,
    events: BTreeMap<usize, Vec<Event<R>>>,
}

/// Shared by all clients of one run; cheap to clone.
pub struct Recorder<R: Row> {
    inner: Arc<Mutex<Inner<R>>>,
}

impl<R: Row> Clone for Recorder<R> {
    fn clone(&self) -> Self {
        Recorder {
            inner: self.inner.clone(),
        }
    }
}

impl<R: Row> Default for Recorder<R> {
    fn default() -> Self {
        Recorder {
            inner: Arc::new(Mutex::new(Inner {
                intended: BTreeMap::new(),
                txns: BTreeMap::new(),
                events: BTreeMap::new(),
            })),
        }
    }
}

impl<R: Row> Recorder<R> {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<R>> {
        self.inner.lock().expect("recorder lock")
    }

    /// Client `client` is about to run operation `op` (ids must be unique per run).
    pub fn intend(&self, client: usize, op: &str, what: ModelOp<R>) {
        let prev = self.lock().intended.insert(op.to_string(), (client, what));
        assert!(prev.is_none(), "operation id {op} used twice");
    }

    /// Like [`Recorder::intend`], for an operation that carries transaction id `txn`. Several
    /// operations (attempts of one write) may carry the same id; I11 allows one of them in the
    /// log.
    pub fn intend_txn(&self, client: usize, op: &str, what: ModelOp<R>, txn: Txn) {
        self.intend(client, op, what);
        self.lock().txns.insert(op.to_string(), txn);
    }

    /// The transaction ids of the operations that carry one, by operation id.
    pub fn txns(&self) -> BTreeMap<String, Txn> {
        self.lock().txns.clone()
    }

    /// Client `client` was told `outcome` for `op`.
    pub fn outcome(&self, client: usize, op: &str, outcome: Outcome) {
        self.lock()
            .events
            .entry(client)
            .or_default()
            .push(Event::Outcome {
                op: op.to_string(),
                outcome,
            });
    }

    /// Client `client` read the table at `version` and saw `rows`.
    pub fn read(&self, client: usize, version: u64, mut rows: Vec<R>) {
        rows.sort();
        self.lock()
            .events
            .entry(client)
            .or_default()
            .push(Event::Read { version, rows });
    }

    /// Joins the recording with the true log (`(version, op id, operation)`, in version order).
    pub fn history(&self, log: Vec<(u64, String, ModelOp<R>)>) -> History<R> {
        let g = self.lock();
        History {
            intended: g.intended.clone(),
            txns: g.txns.clone(),
            events: g.events.clone(),
            log,
            physical: BTreeMap::new(),
            txn_actions: None,
        }
    }

    /// The operations intended so far, by id.
    pub fn intended(&self) -> BTreeMap<String, ModelOp<R>> {
        self.lock()
            .intended
            .iter()
            .map(|(op, (_, w))| (op.clone(), w.clone()))
            .collect()
    }

    /// The outcome reported for each operation, by id.
    pub fn outcomes(&self) -> BTreeMap<String, Outcome> {
        let g = self.lock();
        let mut out = BTreeMap::new();
        for e in g.events.values().flatten() {
            if let Event::Outcome { op, outcome } = e {
                out.insert(op.clone(), outcome.clone());
            }
        }
        out
    }

    /// Outcomes reported so far, by kind: (acknowledged, failed, unknown). "Already applied"
    /// counts as acknowledged.
    pub fn outcome_counts(&self) -> (usize, usize, usize) {
        let g = self.lock();
        let mut c = (0, 0, 0);
        for e in g.events.values().flatten() {
            if let Event::Outcome { outcome, .. } = e {
                match outcome {
                    Outcome::Acked { .. } | Outcome::AlreadyApplied { .. } => c.0 += 1,
                    Outcome::Failed(_) => c.1 += 1,
                    Outcome::Unknown => c.2 += 1,
                }
            }
        }
        c
    }
}

/// A finished run, ready to check.
pub struct History<R: Row> {
    intended: BTreeMap<String, (usize, ModelOp<R>)>,
    txns: BTreeMap<String, Txn>,
    events: BTreeMap<usize, Vec<Event<R>>>,
    log: Vec<(u64, String, ModelOp<R>)>,
    /// Rows stored at each version, read from the store without moraine.
    physical: BTreeMap<u64, Vec<R>>,
    /// Caller transaction ids in each version's `txn` actions, read from the store.
    txn_actions: Option<BTreeMap<u64, BTreeSet<Txn>>>,
}

fn violation(invariant: &'static str, detail: String) -> Result<(), Violation> {
    Err(Violation { invariant, detail })
}

impl<R: Row> History<R> {
    /// Adds the rows stored at each version (any order), read from the store without the
    /// library under test; [`History::check`] then also compares every version with the model.
    pub fn with_physical(mut self, physical: BTreeMap<u64, Vec<R>>) -> Self {
        self.physical = physical
            .into_iter()
            .map(|(v, mut rows)| {
                rows.sort();
                (v, rows)
            })
            .collect();
        self
    }

    /// Adds the caller transaction ids each version's `txn` actions hold (versions without any
    /// may be left out), read from the store; [`History::check`] then also requires them to be
    /// exactly the ids of the operations the log places at each version (I11).
    pub fn with_txn_actions(mut self, actions: BTreeMap<u64, BTreeSet<Txn>>) -> Self {
        self.txn_actions = Some(actions);
        self
    }

    /// Checks every invariant listed in the module documentation. `enforce_keys` turns on I4.
    pub fn check(&self, enforce_keys: bool) -> Result<(), Violation> {
        // I3 (gapless versions) and I10 (each operation at most once, exactly as intended),
        // building the model at every version on the way.
        let mut model = TableModel::default();
        // The empty table of version 0, unless operations that changed nothing were
        // acknowledged with version 0; then it is pushed once they are applied.
        let mut states: Vec<Vec<R>> = Vec::new();
        if self.log.first().is_none_or(|l| l.0 != 0) {
            states.push(model.rows());
        }
        let mut seen: BTreeMap<&str, u64> = BTreeMap::new();
        // I11: committed transaction ids per application, as (log version, txn version), and
        // the ids each version holds.
        let mut committed_txns: BTreeMap<&str, Vec<(u64, i64)>> = BTreeMap::new();
        let mut placed_txns: BTreeMap<u64, BTreeSet<Txn>> = BTreeMap::new();
        for (i, (version, op, what)) in self.log.iter().enumerate() {
            let same_version = (i > 0 && self.log[i - 1].0 == *version) || *version == 0;
            let applied = if same_version {
                model.apply_more(*version, what)
            } else {
                model.apply(*version, what)
            };
            if let Err(e) = applied {
                return violation("I3", e);
            }
            if let Some(first) = seen.insert(op, *version) {
                return violation(
                    "I10",
                    format!("operation {op} committed twice, as versions {first} and {version}"),
                );
            }
            if let Some(txn) = self.txns.get(op) {
                if let Err(e) = model.commit_txn(txn) {
                    return violation("I11", format!("version {version} ({op}): {e}"));
                }
                committed_txns
                    .entry(txn.app_id.as_str())
                    .or_default()
                    .push((*version, txn.version));
                placed_txns.entry(*version).or_default().insert(txn.clone());
            }
            match self.intended.get(op) {
                None => {
                    return violation(
                        "I10",
                        format!("version {version} holds unknown operation {op}"),
                    );
                }
                Some((_, intended)) if intended != what => {
                    return violation(
                        "I10",
                        format!(
                            "version {version} holds {what:?}, operation {op} intended {intended:?}"
                        ),
                    );
                }
                Some(_) => {}
            }
            if enforce_keys {
                let dups = model.duplicate_keys();
                if !dups.is_empty() {
                    return violation(
                        "I4",
                        format!("version {version} holds keys twice: {dups:?}"),
                    );
                }
            }
            if self.log.get(i + 1).is_none_or(|n| n.0 != *version) {
                let rows = model.rows();
                if let Some(stored) = self.physical.get(version)
                    && *stored != rows
                {
                    return violation(
                        "I3",
                        format!("version {version} stores {stored:?}, the model has {rows:?}"),
                    );
                }
                states.push(rows);
            }
        }

        if let Some(actions) = &self.txn_actions {
            let versions: BTreeSet<u64> =
                actions.keys().chain(placed_txns.keys()).copied().collect();
            for v in versions {
                let logged = actions.get(&v).cloned().unwrap_or_default();
                let placed = placed_txns.get(&v).cloned().unwrap_or_default();
                if logged != placed {
                    return violation(
                        "I11",
                        format!(
                            "version {v} holds txn actions {logged:?}, its operations carry {placed:?}"
                        ),
                    );
                }
            }
        }

        for (client, events) in &self.events {
            let mut floor = 0u64; // I5: highest version this client has seen or written
            for e in events {
                match e {
                    Event::Outcome { op, outcome } => match outcome {
                        Outcome::Acked { version } => {
                            // I1
                            match seen.get(op.as_str()) {
                                Some(v) if v == version => {}
                                Some(v) => {
                                    return violation(
                                        "I1",
                                        format!(
                                            "client {client}: {op} acknowledged as version {version}, committed as {v}"
                                        ),
                                    );
                                }
                                None => {
                                    return violation(
                                        "I1",
                                        format!(
                                            "client {client}: {op} acknowledged as version {version}, not in the log"
                                        ),
                                    );
                                }
                            }
                            floor = floor.max(*version);
                        }
                        Outcome::Failed(why) => {
                            // I2
                            if let Some(v) = seen.get(op.as_str()) {
                                return violation(
                                    "I2",
                                    format!(
                                        "client {client}: {op} returned an error ({why}) but committed as version {v}"
                                    ),
                                );
                            }
                        }
                        Outcome::Unknown => {}
                        Outcome::AlreadyApplied { version } => {
                            // I11
                            if let Some(v) = seen.get(op.as_str()) {
                                return violation(
                                    "I11",
                                    format!(
                                        "client {client}: {op} was told already applied but committed as version {v}"
                                    ),
                                );
                            }
                            let Some(txn) = self.txns.get(op) else {
                                return violation(
                                    "I11",
                                    format!(
                                        "client {client}: {op} was told already applied but carries no transaction id"
                                    ),
                                );
                            };
                            let earlier =
                                committed_txns.get(txn.app_id.as_str()).is_some_and(|c| {
                                    c.iter().any(|(lv, tv)| lv <= version && *tv >= txn.version)
                                });
                            if !earlier {
                                return violation(
                                    "I11",
                                    format!(
                                        "client {client}: {op} ({txn}) was told already applied at version {version}, but {} committed nothing at or above {} by then",
                                        txn.app_id, txn.version
                                    ),
                                );
                            }
                            floor = floor.max(*version);
                        }
                    },
                    Event::Read { version, rows } => {
                        // I5
                        if *version < floor {
                            return violation(
                                "I5",
                                format!(
                                    "client {client} read version {version} after seeing or writing {floor}"
                                ),
                            );
                        }
                        floor = *version;
                        // I3
                        let Some(expected) = states.get(*version as usize) else {
                            return violation(
                                "I3",
                                format!(
                                    "client {client} read version {version}, beyond the log's {}",
                                    states.len() - 1
                                ),
                            );
                        };
                        if expected != rows {
                            return violation(
                                "I3",
                                format!(
                                    "client {client} read version {version} as {rows:?}, the model has {expected:?}"
                                ),
                            );
                        }
                        if enforce_keys {
                            let mut keys = BTreeSet::new();
                            for r in rows {
                                if let Some(k) = r.key()
                                    && !keys.insert(k.clone())
                                {
                                    return violation(
                                        "I4",
                                        format!(
                                            "client {client} read key {k:?} twice at version {version}"
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn log_len(&self) -> usize {
        self.log.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(id: &str) -> ModelOp<String> {
        ModelOp::Append(vec![id.to_string()])
    }

    #[test]
    fn catches_each_violation() {
        let ok = || {
            let r = Recorder::<String>::default();
            r.intend(0, "a", append("a"));
            r.intend(1, "b", append("b"));
            r
        };
        let log = || {
            vec![
                (1, "a".to_string(), append("a")),
                (2, "b".to_string(), append("b")),
            ]
        };

        let r = ok();
        r.outcome(0, "a", Outcome::Acked { version: 1 });
        r.read(1, 1, vec!["a".into()]);
        r.outcome(1, "b", Outcome::Acked { version: 2 });
        r.read(1, 2, vec!["b".into(), "a".into()]);
        assert_eq!(r.history(log()).check(false), Ok(()));

        let r = ok();
        r.outcome(1, "b", Outcome::Failed("conflict".into()));
        assert_eq!(r.history(log()).check(false).unwrap_err().invariant, "I2");

        let r = ok();
        r.outcome(0, "a", Outcome::Acked { version: 2 });
        assert_eq!(r.history(log()).check(false).unwrap_err().invariant, "I1");

        let r = ok();
        r.outcome(1, "b", Outcome::Acked { version: 2 });
        r.read(1, 1, vec!["a".into()]);
        assert_eq!(r.history(log()).check(false).unwrap_err().invariant, "I5");

        let r = ok();
        r.read(0, 2, vec!["a".into()]);
        assert_eq!(r.history(log()).check(false).unwrap_err().invariant, "I3");

        let r = ok();
        let mut l = log();
        l.push((3, "a".to_string(), append("a")));
        assert_eq!(r.history(l).check(false).unwrap_err().invariant, "I10");

        let r = ok();
        let l = vec![
            (1, "a".to_string(), append("a")),
            (3, "b".to_string(), append("b")),
        ];
        assert_eq!(r.history(l).check(false).unwrap_err().invariant, "I3");

        let r = ok();
        let stored = BTreeMap::from([(1, vec!["a".to_string()]), (2, vec!["a".to_string()])]);
        let h = r.history(log()).with_physical(stored);
        assert_eq!(h.check(false).unwrap_err().invariant, "I3");
    }

    #[test]
    fn catches_a_transaction_applied_twice() {
        let rec = || {
            let r = Recorder::<String>::default();
            r.intend_txn(0, "a#1", append("a"), Txn::new("w", 1));
            r.intend_txn(1, "a#2", append("a"), Txn::new("w", 1));
            r.intend_txn(0, "b#1", append("b"), Txn::new("w", 2));
            r
        };
        let line = |v: u64, op: &str, row: &str| (v, op.to_string(), append(row));
        let txns = |pairs: &[(u64, i64)]| {
            let mut m: BTreeMap<u64, BTreeSet<Txn>> = BTreeMap::new();
            for (v, t) in pairs {
                m.entry(*v).or_default().insert(Txn::new("w", *t));
            }
            m
        };

        // One attempt committed, the replay was told so, then the next id committed.
        let r = rec();
        r.outcome(0, "a#1", Outcome::Unknown);
        r.outcome(1, "a#2", Outcome::AlreadyApplied { version: 1 });
        r.outcome(0, "b#1", Outcome::Acked { version: 2 });
        let log = vec![line(1, "a#1", "a"), line(2, "b#1", "b")];
        let h = r.history(log).with_txn_actions(txns(&[(1, 1), (2, 2)]));
        assert_eq!(h.check(false), Ok(()));

        // Both attempts committed.
        let r = rec();
        let log = vec![line(1, "a#1", "a"), line(2, "a#2", "a")];
        assert_eq!(r.history(log).check(false).unwrap_err().invariant, "I11");

        // Versions went backwards.
        let r = rec();
        let log = vec![line(1, "b#1", "b"), line(2, "a#1", "a")];
        assert_eq!(r.history(log).check(false).unwrap_err().invariant, "I11");

        // Told "already applied" before anything committed.
        let r = rec();
        r.outcome(1, "a#2", Outcome::AlreadyApplied { version: 0 });
        let log = vec![line(1, "a#1", "a")];
        assert_eq!(r.history(log).check(false).unwrap_err().invariant, "I11");

        // Told "already applied" but committed.
        let r = rec();
        r.outcome(0, "a#1", Outcome::AlreadyApplied { version: 1 });
        let log = vec![line(1, "a#1", "a")];
        assert_eq!(r.history(log).check(false).unwrap_err().invariant, "I11");

        // The log's txn actions disagree with the operations placed.
        let r = rec();
        let log = vec![line(1, "a#1", "a")];
        let h = r.history(log).with_txn_actions(txns(&[(1, 1), (2, 1)]));
        assert_eq!(h.check(false).unwrap_err().invariant, "I11");
    }
}
