//! One version of a table, as one handle knows it. Immutable; a newer version
//! is a new value built by applying log entries (D20: never reloaded when the entries in hand
//! suffice).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;

use crate::error::{Error, Result};
use crate::log::{DataFile, Loaded, LogEntry, TableMeta, TxnId};
use crate::options::TableDef;

#[derive(Clone, Debug)]
pub(crate) struct Snapshot {
    pub version: u64,
    pub meta: Arc<TableMeta>,
    pub def: Arc<TableDef>,
    /// Live files in the order they were added.
    pub files: Arc<Vec<Arc<DataFile>>>,
    pub total_bytes: u64,
    /// The newest `txn` version per application id, moraine's own and callers' (D32).
    pub txns: Txns,
    /// Client monotonic time when the log was last seen to have nothing newer than `version`
    /// (or when this version was written by the handle itself).
    pub confirmed_at: Duration,
}

/// The newest `txn` version per application id at one version.
///
/// Every log entry carries a transaction id, so the map changes with every version; it is kept
/// as a shared base plus the ids of recent entries, folded into a new base once they outnumber a
/// fraction of it, so that applying an entry costs amortised constant time.
#[derive(Clone, Debug, Default)]
pub(crate) struct Txns {
    /// Version and `lastUpdated` (when the action carried one) per application id.
    base: Arc<HashMap<String, (i64, Option<i64>)>>,
    /// Newer than `base`, in log order.
    recent: Arc<Vec<(TxnId, Option<i64>)>>,
}

/// Recent ids kept unfolded, at least.
const TXN_FOLD_MIN: usize = 32;

impl Txns {
    /// The newest version committed for `app_id`, in log order (Delta's rule).
    pub fn get(&self, app_id: &str) -> Option<i64> {
        self.recent
            .iter()
            .rev()
            .find(|(t, _)| t.app_id == app_id)
            .map(|(t, _)| t.version)
            .or_else(|| self.base.get(app_id).map(|(v, _)| *v))
    }

    /// The first of `txns` that is already applied: its app id has this version or a later one.
    pub fn applied<'a>(&self, txns: &'a [TxnId]) -> Option<(&'a TxnId, i64)> {
        txns.iter().find_map(|t| {
            self.get(&t.app_id)
                .filter(|v| *v >= t.version)
                .map(|v| (t, v))
        })
    }

    /// Every application id with its newest version and `lastUpdated`, sorted by id.
    pub fn all(&self) -> Vec<(String, i64, Option<i64>)> {
        let mut m: HashMap<&str, (i64, Option<i64>)> =
            self.base.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        for (t, at) in self.recent.iter() {
            m.insert(t.app_id.as_str(), (t.version, *at));
        }
        let mut out: Vec<(String, i64, Option<i64>)> = m
            .into_iter()
            .map(|(k, (v, at))| (k.to_string(), v, at))
            .collect();
        out.sort();
        out
    }

    fn extend(&mut self, txns: &[TxnId], times: &[(String, i64)]) {
        if txns.is_empty() {
            return;
        }
        let recent = Arc::make_mut(&mut self.recent);
        for t in txns {
            let at = times
                .iter()
                .find(|(a, _)| *a == t.app_id)
                .map(|(_, at)| *at);
            recent.push((t.clone(), at));
        }
        if recent.len() > TXN_FOLD_MIN.max(self.base.len() / 4) {
            let base = Arc::make_mut(&mut self.base);
            for (t, at) in recent.drain(..) {
                base.insert(t.app_id, (t.version, at));
            }
        }
    }
}

impl Snapshot {
    pub fn schema(&self) -> &SchemaRef {
        &self.meta.schema
    }

    /// Builds the snapshot a load describes.
    pub fn from_loaded(l: Loaded, now: Duration) -> Result<Snapshot> {
        let (version, meta, files) = match l.base {
            Some((v, m, f)) => (Some(v), Some(m), f),
            None => (None, None, Vec::new()),
        };
        let mut s = match meta {
            Some(meta) => {
                let mut s = Snapshot::new(version.unwrap_or(0), meta, files, now)?;
                s.txns.extend(&l.base_txns, &l.base_txn_times);
                s
            }
            None => {
                let first = l
                    .entries
                    .first()
                    .ok_or_else(|| Error::corrupt("log", "no entries"))?;
                let meta = first
                    .meta
                    .clone()
                    .ok_or_else(|| Error::corrupt("log entry 0", "no metaData action"))?;
                let mut s = Snapshot::new(0, meta, Vec::new(), now)?;
                s = s.apply_one(first)?;
                s
            }
        };
        let skip = usize::from(version.is_none());
        for e in &l.entries[skip..] {
            s = s.apply_one(e)?;
        }
        s.confirmed_at = now;
        Ok(s)
    }

    fn new(
        version: u64,
        meta: Arc<TableMeta>,
        files: Vec<DataFile>,
        now: Duration,
    ) -> Result<Self> {
        let def = Arc::new(TableDef::from_properties(&meta.configuration)?);
        let files: Vec<Arc<DataFile>> = files.into_iter().map(Arc::new).collect();
        let total_bytes = files.iter().map(|f| f.size).sum();
        Ok(Snapshot {
            version,
            meta,
            def,
            files: Arc::new(files),
            total_bytes,
            txns: Txns::default(),
            confirmed_at: now,
        })
    }

    /// Applies `entries`, which must continue this version without a gap (case C15).
    pub fn apply(&self, entries: &[LogEntry]) -> Result<Snapshot> {
        let mut s = self.clone();
        for e in entries {
            s = s.apply_one(e)?;
        }
        Ok(s)
    }

    fn apply_one(&self, e: &LogEntry) -> Result<Snapshot> {
        let expected = if self.version == 0 && e.version == 0 {
            0
        } else {
            self.version + 1
        };
        if e.version != expected {
            return Err(Error::corrupt(
                format!("log entry {}", e.version),
                format_args!("applied to version {}", self.version),
            ));
        }
        let mut s = self.clone();
        s.version = e.version;
        s.txns.extend(&e.txns, &e.txn_times);
        if let Some(m) = &e.meta {
            s.def = Arc::new(TableDef::from_properties(&m.configuration)?);
            s.meta = m.clone();
        }
        if !e.removes.is_empty() || !e.adds.is_empty() {
            let gone: HashSet<&str> = e.removes.iter().map(String::as_str).collect();
            // A rewrite that changes no rows (compaction, D48) takes the place of the first file
            // it removes, so files keep the order they were added in and rows with equal keys
            // keep their commit order across files (D38).
            let at = if e.data_change || gone.is_empty() {
                None
            } else {
                self.files
                    .iter()
                    .position(|f| gone.contains(f.path.as_str()))
            };
            let mut files: Vec<Arc<DataFile>> = Vec::with_capacity(self.files.len() + e.adds.len());
            for (i, f) in self.files.iter().enumerate() {
                if Some(i) == at {
                    files.extend(e.adds.iter().cloned().map(Arc::new));
                }
                if !gone.contains(f.path.as_str()) {
                    files.push(f.clone());
                }
            }
            if at.is_none() {
                files.extend(e.adds.iter().cloned().map(Arc::new));
            }
            s.total_bytes = files.iter().map(|f| f.size).sum();
            s.files = Arc::new(files);
        }
        Ok(s)
    }
}
