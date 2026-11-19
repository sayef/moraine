//! The commit layer: the table log as moraine sees it.
//!
//! [`CommitLayer`] is the only interface to the log, and [`delta::DeltaLog`] its only
//! implementation; no delta-rs type crosses this module's boundary. The layer knows the log format and how to read and write one entry. The commit
//! protocol on top of it (rebase, conflict rules, transaction-id lookup, refresh by probing)
//! lives in [`crate::commit`] and [`crate::table`], so its request budget (D18) is moraine's own.

pub(crate) mod checkpoint;
pub(crate) mod delta;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;

use crate::commit::AttemptError;
use crate::error::Result;
use crate::stats::FileStats;

/// One data file as the snapshot knows it.
#[derive(Clone, Debug, PartialEq)]
pub struct DataFile {
    /// Path relative to the table's data root, as the `add` action names it (decoded).
    pub path: String,
    pub size: u64,
    pub stats: FileStats,
    /// Client wall-clock time of the write, in milliseconds (Delta's `modificationTime`).
    pub modification_time: i64,
    /// The columns the file is sorted by, when moraine wrote it (D19). Its statistics are then
    /// exact.
    pub sorted_by: Option<Vec<String>>,
    /// The version that added the file (0 when it came from a checkpoint and is unknown).
    pub added_in: u64,
    /// The file's other `add.tags`: its compaction level ([`LEVEL_TAG`], D29) and any tags a
    /// caller set ([`crate::Replace::tag`]).
    pub tags: BTreeMap<String, String>,
}

/// The `add.tags` key holding a file's compaction level (D29). Files without it are level 0.
pub const LEVEL_TAG: &str = "moraine.level";

impl DataFile {
    pub fn rows(&self) -> Option<u64> {
        self.stats.rows
    }

    /// The compaction level ([`LEVEL_TAG`]); 0 for appended files and other writers' files.
    pub fn level(&self) -> u32 {
        self.tags
            .get(LEVEL_TAG)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }
}

/// The table's metadata: schema, partitioning and properties.
#[derive(Clone, Debug, PartialEq)]
pub struct TableMeta {
    pub id: String,
    pub schema: SchemaRef,
    pub partition_columns: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    /// The `protocol` action that came with this metadata, as JSON, when it was not moraine's
    /// own (reader 1, writer 2). Checkpoints write it back unchanged.
    pub protocol: Option<String>,
}

/// A transaction id, written as a Delta `txn` action .
///
/// moraine gives every group commit one of its own (the writer handle and its group sequence
/// number) to resolve ambiguous outcomes. A caller can attach one more to a write
/// ([`crate::Table::txn`]) to make it exactly-once: a write whose `version` is at or below the
/// newest committed version for its `app_id` is not applied again (D32).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TxnId {
    /// The application id (Delta's `appId`).
    pub app_id: String,
    /// The transaction version; higher versions of one application id supersede lower ones.
    pub version: i64,
}

impl TxnId {
    /// The id `version` of application `app_id`.
    pub fn new(app_id: impl Into<String>, version: i64) -> TxnId {
        TxnId {
            app_id: app_id.into(),
            version,
        }
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.app_id, self.version)
    }
}

/// One log entry, parsed.
#[derive(Clone, Debug, Default)]
pub struct LogEntry {
    pub version: u64,
    pub meta: Option<Arc<TableMeta>>,
    pub adds: Vec<DataFile>,
    pub removes: Vec<String>,
    pub txns: Vec<TxnId>,
    /// `lastUpdated` of the `txn` actions that carry one, by application id. moraine's own
    /// group ids carry it and expire from checkpoints (D14); callers' ids and fences do not.
    pub txn_times: Vec<(String, i64)>,
    /// Whether an action of the entry changes rows (`dataChange`). Compaction does not: its
    /// added files hold exactly the rows of the files it removes.
    pub data_change: bool,
}

impl LogEntry {
    pub fn has_txn(&self, txn: &TxnId) -> bool {
        self.txns.iter().any(|t| t == txn)
    }
}

/// The state of the table at the newest checkpoint or version 0, plus everything after it.
#[derive(Debug)]
pub struct Loaded {
    /// State at `base_version` (from a checkpoint), when there is one.
    pub base: Option<(u64, Arc<TableMeta>, Vec<DataFile>)>,
    /// The `txn` actions of the checkpoint (the newest version per application id).
    pub base_txns: Vec<TxnId>,
    /// `lastUpdated` of the checkpoint's `txn` actions that carry one.
    pub base_txn_times: Vec<(String, i64)>,
    /// Entries after the base (or from version 0), contiguous and in order.
    pub entries: Vec<LogEntry>,
}

/// What a new log entry carries.
#[derive(Clone, Debug)]
pub struct NewEntry {
    /// Set for version 0 and metadata changes.
    pub meta: Option<TableMeta>,
    /// Write the protocol action (version 0).
    pub protocol: bool,
    pub adds: Vec<DataFile>,
    /// Files the entry removes (rewritten by a merge, delete or update).
    pub removes: Vec<DataFile>,
    pub txn: Option<TxnId>,
    /// Callers' transaction ids (D32), written without `lastUpdated` so that table-level
    /// transaction retention never expires them.
    pub caller_txns: Vec<TxnId>,
    /// Delta's operation name for `commitInfo` ("WRITE", "MERGE", "DELETE", "UPDATE",
    /// "ADD COLUMNS", "CREATE TABLE").
    pub operation: &'static str,
    /// Client wall-clock time in milliseconds.
    pub timestamp: i64,
    /// `dataChange` of the entry's adds and removes; false only for compaction.
    pub data_change: bool,
}

/// One object in the log directory, from a listing.
#[derive(Clone, Debug)]
pub struct LogObject {
    pub location: object_store::path::Path,
    pub version: u64,
    pub kind: LogObjectKind,
    /// Server time of the write.
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogObjectKind {
    /// `<version>.json`
    Entry,
    /// `<version>.checkpoint.parquet` (single-file)
    Checkpoint,
    /// Multi-part or V2 checkpoints, CRC files and anything else versioned.
    Other,
}

/// The state a checkpoint records.
#[derive(Debug)]
pub struct CheckpointState<'a> {
    pub version: u64,
    pub meta: &'a TableMeta,
    pub files: &'a [Arc<DataFile>],
    /// Application id, version and `lastUpdated` of every transaction to keep.
    pub txns: Vec<(String, i64, Option<i64>)>,
}

/// See the module documentation.
#[async_trait]
pub trait CommitLayer: Send + Sync + fmt::Debug {
    /// The schema as the log stores it, converted back to Arrow: the table's canonical schema.
    /// [`crate::Error::InvalidSchema`] when the log format cannot represent it.
    fn canonical_schema(&self, schema: &Schema) -> Result<SchemaRef>;

    /// Parses a body in the log-entry format that carries its own metadata (a pin manifest).
    fn parse_entry(&self, version: u64, body: &[u8]) -> Result<LogEntry>;

    /// Serialises an entry. Encoding once lets a rebased commit put the same bytes again.
    fn encode(&self, entry: &NewEntry, schema: &Schema) -> Result<Bytes>;

    /// Creates log entry `version` with put-if-absent. A taken version is
    /// `AttemptError::Conflict(VersionTaken)`.
    async fn put_entry(&self, version: u64, body: Bytes) -> Result<(), AttemptError>;

    /// Reads log entry `version`; `None` when it does not exist. `schema` is the table schema
    /// in force before the entry, for parsing statistics.
    async fn read_entry(
        &self,
        version: u64,
        schema: Option<&SchemaRef>,
    ) -> Result<Option<LogEntry>>;

    /// Loads the table from a listing: the newest checkpoint, if any, and every entry after it.
    /// [`crate::Error::TableNotFound`] when there is no log.
    async fn load(&self) -> Result<Loaded>;

    /// Loads the table as it was at `version` (an older one): the newest checkpoint at or
    /// before it and the entries up to it. [`crate::Error::VersionNotAvailable`] when log
    /// cleanup removed what that needs.
    async fn load_at(&self, version: u64) -> Result<Loaded>;

    /// Versions of the log entries and checkpoints after `version`, from a listing.
    async fn list_versions_after(&self, version: u64) -> Result<Vec<u64>>;

    /// Paths of data files are relative to this prefix of the store.
    fn data_prefix(&self) -> &object_store::path::Path;

    /// Reads log entry `version` and the server time it was written; `None` when it does not
    /// exist (never written, or cleaned up).
    async fn read_entry_at(
        &self,
        version: u64,
        schema: Option<&SchemaRef>,
    ) -> Result<Option<(LogEntry, chrono::DateTime<chrono::Utc>)>>;

    /// Writes a checkpoint with put-if-absent and points `_last_checkpoint` at it. `false`
    /// when a checkpoint of that version exists already.
    async fn write_checkpoint(&self, state: &CheckpointState<'_>) -> Result<bool>;

    /// Lists the log directory. Fails on an incomplete listing.
    async fn list_log_objects(&self) -> Result<Vec<LogObject>>;

    /// Deletes objects of the log directory; one already gone counts as deleted.
    async fn delete_log_objects(&self, locations: Vec<object_store::path::Path>) -> Result<usize>;

    /// Rewrites log entry `version` in place without the values an erasure takes out (D66):
    /// the minimum and maximum of the files in `erased` it adds, and commit information that
    /// may hold values. A conditional overwrite, so an entry log cleanup deleted meanwhile is
    /// not written back. `true` when it was rewritten; `false` when there was nothing to take
    /// out or the entry is gone.
    async fn scrub_entry(
        &self,
        version: u64,
        erased: &std::collections::HashSet<String>,
    ) -> Result<bool>;
}
