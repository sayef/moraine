//! Table options (D12, D19) and how they are stored as table properties
//! (D1).

use std::collections::BTreeMap;
use std::time::Duration;

use crate::error::{Error, Result};

/// What a write does with a key that occurs more than once in its batches (case D2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum DuplicatePolicy {
    /// Refuse the write with [`crate::Error::DuplicateKeyInBatch`].
    #[default]
    Error,
    /// Keep the row that comes last (in batch order, then row order).
    LastWins,
}

/// A write that is in the table: what [`Table::append`](crate::Table::append) and the other
/// plain writes return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Committed {
    /// The table version that holds the write: the version it committed as, or the current
    /// version when there was nothing to write (case C19).
    pub version: u64,
}

impl Committed {
    /// A write held by `version`: for tests of code that consumes write results.
    pub fn new(version: u64) -> Committed {
        Committed { version }
    }

    /// The table version that holds the write.
    pub fn version(&self) -> u64 {
        self.version
    }
}

/// What a write with a transaction id did ([`crate::Table::txn`], D32).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
#[non_exhaustive]
pub enum WriteOutcome {
    /// The write committed, with its `txn` action.
    Committed(Committed),
    /// The application id had already committed this transaction version or a later one, so
    /// nothing was written.
    AlreadyApplied {
        /// A table version that includes the earlier commit.
        version: u64,
        /// The newest transaction version committed for the application id.
        txn_version: i64,
    },
}

impl WriteOutcome {
    /// A table version that includes the write: the one it committed as, or one that holds the
    /// earlier commit of the same transaction id.
    pub fn version(&self) -> u64 {
        match self {
            WriteOutcome::Committed(c) => c.version,
            WriteOutcome::AlreadyApplied { version, .. } => *version,
        }
    }

    /// Whether this call committed the write (rather than finding it already applied).
    pub fn is_committed(&self) -> bool {
        matches!(self, WriteOutcome::Committed(_))
    }
}

/// Options of a new table, built from [`TableOptions::default`] with the setters below.
/// Everything here is stored in the table's properties at creation, so every client reads the
/// same values (case G7).
///
/// ```
/// use std::time::Duration;
/// use moraine::TableOptions;
///
/// let options = TableOptions::default()
///     .primary_key(["run_id", "item_id"])
///     .cluster_by(["run_id"])
///     .grace_period(Duration::from_secs(2 * 3600));
/// assert_eq!(options.primary_key, ["run_id", "item_id"]);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableOptions {
    /// Primary key columns; empty for a keyless table.
    pub primary_key: Vec<String>,
    /// Refuse `append` (only `append_unchecked` and `upsert` may write) so no snapshot holds a
    /// key twice. `None` means "true when a key is declared".
    pub enforce_primary_key: Option<bool>,
    /// Columns rows are clustered by (D19). Every data file moraine writes is sorted by the
    /// cluster key and then the primary key, so a lookup by either touches few pages.
    ///
    /// The sort is stable: rows of one commit whose sort columns are equal keep the order in
    /// which they were given (batch order, then row order; in a group commit, callers in
    /// arrival order). Readers may rely on it, for example to let the newest of several equal
    /// keys in an append win.
    pub cluster_by: Vec<String>,
    /// How long removed files and old log entries are kept. At least 10 minutes.
    pub grace_period: Duration,
    /// Rows per Parquet data page at most (D12).
    pub page_row_limit: usize,
    /// Rows per Parquet row group at most.
    pub row_group_rows: usize,
    /// Size a data file written by one commit aims for; larger groups are split.
    pub target_file_bytes: u64,
    /// Blob values up to this many bytes stay inside the row; larger ones are stored as
    /// objects of their own. At most 16 MiB.
    pub blob_inline_bytes: u64,
}

impl Default for TableOptions {
    fn default() -> Self {
        TableOptions {
            primary_key: Vec::new(),
            enforce_primary_key: None,
            cluster_by: Vec::new(),
            grace_period: Duration::from_secs(3600),
            page_row_limit: 1024,
            row_group_rows: 16_384,
            target_file_bytes: 256 << 20,
            blob_inline_bytes: crate::blob::DEFAULT_INLINE_BYTES,
        }
    }
}

impl TableOptions {
    /// Declares the primary key columns. A table with a key enforces it unless
    /// [`TableOptions::enforce_primary_key`] says otherwise.
    pub fn primary_key<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.primary_key = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Whether `append` is refused so that no snapshot holds a key twice (default: true when
    /// a key is declared).
    pub fn enforce_primary_key(mut self, enforce: bool) -> Self {
        self.enforce_primary_key = Some(enforce);
        self
    }

    /// The columns rows are clustered by (D19).
    pub fn cluster_by<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.cluster_by = columns.into_iter().map(Into::into).collect();
        self
    }

    /// How long removed files and old log entries are kept (default 1 hour, at least 10
    /// minutes).
    pub fn grace_period(mut self, d: Duration) -> Self {
        self.grace_period = d;
        self
    }

    /// Rows per Parquet data page at most (default 1,024).
    pub fn page_row_limit(mut self, rows: usize) -> Self {
        self.page_row_limit = rows;
        self
    }

    /// Rows per Parquet row group at most (default 16,384).
    pub fn row_group_rows(mut self, rows: usize) -> Self {
        self.row_group_rows = rows;
        self
    }

    /// Size a data file written by one commit aims for (default 256 MiB).
    pub fn target_file_bytes(mut self, bytes: u64) -> Self {
        self.target_file_bytes = bytes;
        self
    }

    /// Blob values up to this size stay inside the row (default 256 KiB, at most 16 MiB).
    pub fn blob_inline_bytes(mut self, bytes: u64) -> Self {
        self.blob_inline_bytes = bytes;
        self
    }
}

/// The options a table actually has, as read back from its properties
/// ([`Table::definition`](crate::Table::definition)).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableDef {
    /// Primary key columns; empty for a keyless table.
    pub primary_key: Vec<String>,
    /// Whether `append` is refused so that no snapshot holds a key twice.
    pub enforce_primary_key: bool,
    /// Columns rows are clustered by.
    pub cluster_by: Vec<String>,
    /// How long removed files and old log entries are kept.
    pub grace_period: Duration,
    /// Rows per Parquet data page at most.
    pub page_row_limit: usize,
    /// Rows per Parquet row group at most.
    pub row_group_rows: usize,
    /// Size a data file written by one commit aims for.
    pub target_file_bytes: u64,
    /// Blob values up to this many bytes stay inside the row.
    pub blob_inline_bytes: u64,
}

impl TableDef {
    /// The order every data file moraine writes is sorted in: cluster key, then primary key,
    /// each column once.
    pub fn sort_columns(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for c in self.cluster_by.iter().chain(&self.primary_key) {
            if !out.contains(c) {
                out.push(c.clone());
            }
        }
        out
    }

    /// The commit deadline: a quarter of the grace period.
    pub fn commit_deadline(&self) -> Duration {
        self.grace_period / 4
    }
}

pub(crate) const MIN_GRACE: Duration = Duration::from_secs(600);
const FORMAT_VERSION: &str = "1";

pub(crate) mod keys {
    pub const FORMAT_VERSION: &str = "moraine.formatVersion";
    pub const PRIMARY_KEY: &str = "moraine.primaryKey";
    pub const ENFORCE_PRIMARY_KEY: &str = "moraine.enforcePrimaryKey";
    pub const CLUSTER_BY: &str = "moraine.clusterBy";
    pub const INDEXES: &str = "moraine.indexes";
    pub const GRACE_SECONDS: &str = "moraine.gracePeriodSeconds";
    pub const PAGE_ROW_LIMIT: &str = "moraine.pageRowLimit";
    pub const ROW_GROUP_ROWS: &str = "moraine.rowGroupRows";
    pub const TARGET_FILE_BYTES: &str = "moraine.targetFileBytes";
    pub const BLOB_INLINE_BYTES: &str = "moraine.blobInlineBytes";
}

impl TableOptions {
    /// Checks the options and returns the table properties to store (moraine's and Delta's).
    pub(crate) fn to_properties(&self) -> Result<BTreeMap<String, String>> {
        if self.grace_period < MIN_GRACE {
            return Err(Error::InvalidConfig(format!(
                "grace period {:?} is below the minimum of {MIN_GRACE:?}",
                self.grace_period
            )));
        }
        if self.page_row_limit == 0 || self.row_group_rows == 0 || self.target_file_bytes == 0 {
            return Err(Error::InvalidConfig(
                "page_row_limit, row_group_rows and target_file_bytes must be positive".into(),
            ));
        }
        if self.blob_inline_bytes > 16 << 20 {
            return Err(Error::InvalidConfig(format!(
                "blob_inline_bytes {} is above 16 MiB",
                self.blob_inline_bytes
            )));
        }
        let key = self.primary_key.clone();
        let enforce = self.enforce_primary_key.unwrap_or(!key.is_empty());
        if enforce && key.is_empty() {
            return Err(Error::PrimaryKeyRequired);
        }
        let grace = self.grace_period.as_secs();
        let interval = format!("interval {grace} seconds");
        let mut p = BTreeMap::new();
        p.insert(keys::FORMAT_VERSION.into(), FORMAT_VERSION.into());
        if !key.is_empty() {
            p.insert(keys::PRIMARY_KEY.into(), json_list(&key));
        }
        p.insert(keys::ENFORCE_PRIMARY_KEY.into(), enforce.to_string());
        p.insert(keys::CLUSTER_BY.into(), json_list(&self.cluster_by));
        p.insert(keys::INDEXES.into(), "[]".into());
        p.insert(keys::GRACE_SECONDS.into(), grace.to_string());
        p.insert(keys::PAGE_ROW_LIMIT.into(), self.page_row_limit.to_string());
        p.insert(keys::ROW_GROUP_ROWS.into(), self.row_group_rows.to_string());
        p.insert(
            keys::TARGET_FILE_BYTES.into(),
            self.target_file_bytes.to_string(),
        );
        p.insert(
            keys::BLOB_INLINE_BYTES.into(),
            self.blob_inline_bytes.to_string(),
        );
        p.insert("delta.checkpointInterval".into(), "20".into());
        p.insert("delta.logRetentionDuration".into(), interval.clone());
        p.insert(
            "delta.deletedFileRetentionDuration".into(),
            interval.clone(),
        );
        p.insert("delta.setTransactionRetentionDuration".into(), interval);
        p.insert("delta.enableDeletionVectors".into(), "false".into());
        Ok(p)
    }
}

impl TableDef {
    /// Reads the options back from table properties. A table without moraine's properties (one
    /// written only by other Delta tools) gets the defaults and no keys.
    pub(crate) fn from_properties(p: &BTreeMap<String, String>) -> Result<TableDef> {
        let d = TableOptions::default();
        let list = |k: &str| -> Result<Vec<String>> {
            match p.get(k) {
                None => Ok(Vec::new()),
                Some(v) => serde_json::from_str(v).map_err(|e| {
                    Error::corrupt(format!("table property {k}"), format_args!("{v}: {e}"))
                }),
            }
        };
        let num = |k: &str, default: u64| -> Result<u64> {
            match p.get(k) {
                None => Ok(default),
                Some(v) => v.parse().map_err(|e| {
                    Error::corrupt(format!("table property {k}"), format_args!("{v}: {e}"))
                }),
            }
        };
        if let Some(v) = p.get(keys::FORMAT_VERSION)
            && v != FORMAT_VERSION
        {
            return Err(Error::UnsupportedTableFeature(format!(
                "{}={v}",
                keys::FORMAT_VERSION
            )));
        }
        let primary_key = list(keys::PRIMARY_KEY)?;
        let enforce_primary_key = p
            .get(keys::ENFORCE_PRIMARY_KEY)
            .map(|v| v == "true")
            .unwrap_or(false);
        Ok(TableDef {
            primary_key,
            enforce_primary_key,
            cluster_by: list(keys::CLUSTER_BY)?,
            grace_period: Duration::from_secs(num(keys::GRACE_SECONDS, d.grace_period.as_secs())?),
            page_row_limit: num(keys::PAGE_ROW_LIMIT, d.page_row_limit as u64)? as usize,
            row_group_rows: num(keys::ROW_GROUP_ROWS, d.row_group_rows as u64)? as usize,
            target_file_bytes: num(keys::TARGET_FILE_BYTES, d.target_file_bytes)?,
            blob_inline_bytes: num(keys::BLOB_INLINE_BYTES, d.blob_inline_bytes)?,
        })
    }
}

fn json_list(v: &[String]) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "[]".into())
}

/// When a query looks for commits newer than the snapshot in hand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Consistency {
    /// Before every query, read the next log entry (one small GET).
    Strong,
    /// A query refreshes first when the snapshot was last confirmed more than `max_staleness`
    /// ago; otherwise it uses the snapshot in hand.
    Eventual {
        /// How old the last confirmation of the snapshot may be.
        max_staleness: Duration,
    },
    /// Only [`crate::Table::refresh`] and the handle's own writes move the snapshot.
    Manual,
    /// A background task reads new log entries every `interval` and prewarms the caches for
    /// each new version (D29); queries use the snapshot in hand and make no snapshot request.
    /// A query refreshes by itself only when the task has not confirmed the snapshot for four
    /// intervals and a second (the store unreachable, say). The task ends with the last clone
    /// of the handle, or when the mode changes.
    Background {
        /// Time between two reads of the log.
        interval: Duration,
    },
}

impl Default for Consistency {
    fn default() -> Self {
        Consistency::Eventual {
            max_staleness: Duration::from_secs(5),
        }
    }
}

impl Consistency {
    pub(crate) fn max_staleness(&self) -> Duration {
        match self {
            Consistency::Strong | Consistency::Manual => Duration::ZERO,
            Consistency::Background { interval } => *interval,
            Consistency::Eventual { max_staleness } => *max_staleness,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_round_trip() {
        let o = TableOptions::default()
            .primary_key(["run_id", "item_id"])
            .cluster_by(["run_id"]);
        let p = o.to_properties().unwrap();
        let d = TableDef::from_properties(&p).unwrap();
        assert_eq!(d.primary_key, vec!["run_id", "item_id"]);
        assert!(d.enforce_primary_key);
        assert_eq!(d.sort_columns(), vec!["run_id", "item_id"]);
        assert_eq!(d.grace_period, Duration::from_secs(3600));
        assert_eq!(
            p["delta.logRetentionDuration"],
            "interval 3600 seconds".to_string()
        );
    }

    #[test]
    fn invalid_options_g6() {
        let short = TableOptions::default().grace_period(Duration::from_secs(60));
        assert!(matches!(
            short.to_properties(),
            Err(Error::InvalidConfig(_))
        ));
        let enforce_without_key = TableOptions::default().enforce_primary_key(true);
        assert!(matches!(
            enforce_without_key.to_properties(),
            Err(Error::PrimaryKeyRequired)
        ));
    }
}
