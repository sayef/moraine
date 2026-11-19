//! The reference model: a table as an in-memory multiset of rows, with an
//! optional key, to which committed operations are applied in version order, and the newest
//! committed version of every caller transaction id (D32).

use std::collections::BTreeMap;
use std::fmt::Debug;

/// A row the model can hold. Rows are compared whole; the key decides upserts and deletes.
pub trait Row: Clone + Ord + Debug {
    type Key: Clone + Ord + Debug;
    /// A filter on rows, for deletes and updates by predicate.
    type Pred: Clone + Eq + Debug;
    /// A change to the non-key columns of a row, for updates.
    type Patch: Clone + Eq + Debug;
    /// The primary key, or `None` for a keyless table.
    fn key(&self) -> Option<Self::Key>;
    /// Whether the row passes `pred`.
    fn matches(&self, pred: &Self::Pred) -> bool;
    /// The row with `patch` applied.
    fn patched(&self, patch: &Self::Patch) -> Self;
}

impl Row for String {
    type Key = String;
    type Pred = ();
    type Patch = ();
    fn key(&self) -> Option<String> {
        None
    }
    fn matches(&self, _: &()) -> bool {
        false
    }
    fn patched(&self, _: &()) -> String {
        self.clone()
    }
}

/// One committed operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelOp<R: Row> {
    /// Add rows as they are (a blind append may create a duplicate key, case C3).
    Append(Vec<R>),
    /// Replace every row with the same key, or insert.
    Upsert(Vec<R>),
    /// Remove every row with one of these keys.
    DeleteKeys(Vec<R::Key>),
    /// Remove every row that passes the filter.
    DeleteWhere(R::Pred),
    /// Apply the patch to every row that passes the filter.
    Update(R::Pred, R::Patch),
    /// A change of schema or options that leaves the rows as they are (`add_column`).
    Schema(String),
}

/// A caller's transaction id (D32): an application id and its version.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Txn {
    pub app_id: String,
    pub version: i64,
}

impl Txn {
    pub fn new(app_id: impl Into<String>, version: i64) -> Txn {
        Txn {
            app_id: app_id.into(),
            version,
        }
    }
}

impl std::fmt::Display for Txn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.app_id, self.version)
    }
}

/// The table at some version.
#[derive(Clone, Debug)]
pub struct TableModel<R: Row> {
    rows: BTreeMap<R, usize>,
    version: u64,
    /// The newest committed transaction version per application id.
    txns: BTreeMap<String, i64>,
}

impl<R: Row> Default for TableModel<R> {
    fn default() -> Self {
        TableModel {
            rows: BTreeMap::new(),
            version: 0,
            txns: BTreeMap::new(),
        }
    }
}

impl<R: Row> TableModel<R> {
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Records that an operation carrying `txn` committed. A transaction id applies at most
    /// once, and an application's versions only grow (I11): committing a version at or below
    /// the newest one already committed for the same application is an error.
    pub fn commit_txn(&mut self, txn: &Txn) -> Result<(), String> {
        if let Some(v) = self.txns.get(&txn.app_id)
            && *v >= txn.version
        {
            return Err(format!(
                "transaction {txn} applied after {}:{v} was already committed",
                txn.app_id
            ));
        }
        self.txns.insert(txn.app_id.clone(), txn.version);
        Ok(())
    }

    /// The newest committed transaction version of `app_id`.
    pub fn txn_version(&self, app_id: &str) -> Option<i64> {
        self.txns.get(app_id).copied()
    }

    /// Applies `op` as version `version`, which must be the next one (I3: versions are applied
    /// in order, without gaps).
    pub fn apply(&mut self, version: u64, op: &ModelOp<R>) -> Result<(), String> {
        if version != self.version + 1 {
            return Err(format!(
                "version {version} applied after {}; versions must be gapless",
                self.version
            ));
        }
        self.apply_op(op);
        self.version = version;
        Ok(())
    }

    /// Applies one more operation of the current version: a group commit puts several
    /// callers' operations into one version (D18), and a write that changes nothing (case C19)
    /// is acknowledged with the version it saw, which may be version 0.
    pub fn apply_more(&mut self, version: u64, op: &ModelOp<R>) -> Result<(), String> {
        if version != self.version {
            return Err(format!(
                "operation of version {version} applied as part of version {}",
                self.version
            ));
        }
        self.apply_op(op);
        Ok(())
    }

    fn apply_op(&mut self, op: &ModelOp<R>) {
        match op {
            ModelOp::Append(rows) => {
                for r in rows {
                    *self.rows.entry(r.clone()).or_default() += 1;
                }
            }
            ModelOp::Upsert(rows) => {
                for r in rows {
                    let k = r.key();
                    self.rows.retain(|old, _| old.key() != k || k.is_none());
                    *self.rows.entry(r.clone()).or_default() += 1;
                }
            }
            ModelOp::DeleteKeys(keys) => {
                self.rows
                    .retain(|old, _| old.key().is_none_or(|k| !keys.contains(&k)));
            }
            ModelOp::DeleteWhere(pred) => self.rows.retain(|old, _| !old.matches(pred)),
            ModelOp::Update(pred, patch) => {
                let old = std::mem::take(&mut self.rows);
                for (r, n) in old {
                    let r = if r.matches(pred) { r.patched(patch) } else { r };
                    *self.rows.entry(r).or_default() += n;
                }
            }
            ModelOp::Schema(_) => {}
        }
    }

    /// The rows, sorted, with duplicates repeated.
    pub fn rows(&self) -> Vec<R> {
        let mut out = Vec::new();
        for (r, n) in &self.rows {
            for _ in 0..*n {
                out.push(r.clone());
            }
        }
        out
    }

    /// Keys held by more than one row (I4).
    pub fn duplicate_keys(&self) -> Vec<R::Key> {
        let mut seen: BTreeMap<R::Key, usize> = BTreeMap::new();
        for (r, n) in &self.rows {
            if let Some(k) = r.key() {
                *seen.entry(k).or_default() += n;
            }
        }
        seen.into_iter()
            .filter(|(_, n)| *n > 1)
            .map(|(k, _)| k)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Kv(u32, u32);
    impl Row for Kv {
        type Key = u32;
        /// Rows whose value is this.
        type Pred = u32;
        /// The new value.
        type Patch = u32;
        fn key(&self) -> Option<u32> {
            Some(self.0)
        }
        fn matches(&self, p: &u32) -> bool {
            self.1 == *p
        }
        fn patched(&self, p: &u32) -> Kv {
            Kv(self.0, *p)
        }
    }

    #[test]
    fn upsert_replaces_and_delete_removes() {
        let mut m = TableModel::default();
        m.apply(1, &ModelOp::Append(vec![Kv(1, 1), Kv(2, 1)]))
            .unwrap();
        m.apply(2, &ModelOp::Upsert(vec![Kv(1, 2)])).unwrap();
        assert_eq!(m.rows(), vec![Kv(1, 2), Kv(2, 1)]);
        m.apply(3, &ModelOp::DeleteKeys(vec![2])).unwrap();
        assert_eq!(m.rows(), vec![Kv(1, 2)]);
        m.apply(4, &ModelOp::Append(vec![Kv(1, 3)])).unwrap();
        assert_eq!(m.duplicate_keys(), vec![1]);
        m.apply(5, &ModelOp::Update(3, 4)).unwrap();
        assert_eq!(m.rows(), vec![Kv(1, 2), Kv(1, 4)]);
        m.apply(6, &ModelOp::DeleteWhere(2)).unwrap();
        m.apply(7, &ModelOp::Schema("x".into())).unwrap();
        assert_eq!(m.rows(), vec![Kv(1, 4)]);
        assert!(m.apply(9, &ModelOp::Append(vec![])).is_err());
    }

    #[test]
    fn a_transaction_id_applies_once() {
        let mut m = TableModel::<Kv>::default();
        m.commit_txn(&Txn::new("a", 1)).unwrap();
        m.commit_txn(&Txn::new("b", 1)).unwrap();
        m.commit_txn(&Txn::new("a", 3)).unwrap();
        assert!(m.commit_txn(&Txn::new("a", 3)).is_err());
        assert!(m.commit_txn(&Txn::new("a", 2)).is_err());
        assert_eq!(m.txn_version("a"), Some(3));
        assert_eq!(m.txn_version("c"), None);
    }
}
