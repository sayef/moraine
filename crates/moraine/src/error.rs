//! The one error type of the public API.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::names::NotFound;
use crate::store::{Class, classify};

/// Result with [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every error moraine returns. Variants are stable; new ones may be added.
///
/// A returned error means the operation's write is **not** visible, with one exception:
/// [`Error::OutcomeUnknown`], returned only when the write may or may not have committed and the
/// lookup that would tell has itself kept failing.
///
/// Errors are cheap to clone: errors of the libraries moraine uses are held behind an `Arc`
/// ([`StorageError`], [`LibraryError`]) and reachable through
/// [`source`](std::error::Error::source), so upgrading one of those libraries does not change
/// the variants.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A schema moraine cannot store or that does not fit the table: an unsupported column
    /// type, a key column that is missing or nullable, a column added twice.
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    /// A batch that does not match the table's schema, or holds a null key. Nothing of the
    /// write was queued (case C17).
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
    /// A filter, projection, search parameter or argument that cannot be used as given.
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    /// An upsert's batches repeat a primary key under `DuplicatePolicy::Error`.
    #[error("duplicate key in batch: {0}")]
    DuplicateKeyInBatch(String),
    /// An operation that needs a primary key (`upsert`, `get`, `delete_keys`, enforcing a key)
    /// on a table without one.
    #[error("the table has no primary key, and this operation needs one")]
    PrimaryKeyRequired,
    /// `append` on a table that enforces its primary key (case C3): use `append_unchecked` for a
    /// load known not to repeat keys, or `upsert`.
    #[error(
        "the table enforces its primary key; append would allow duplicates (use upsert or append_unchecked)"
    )]
    PrimaryKeyEnforced,
    /// No table of that name; the message names the closest table and the available ones.
    #[error("table not found: {0}")]
    TableNotFound(NotFound),
    /// A column named in a projection, filter or assignment does not exist; the message names the
    /// closest column and the table's columns.
    #[error("column not found: {0}")]
    ColumnNotFound(NotFound),
    /// `create_table` found the table already created with another schema or other options
    /// (case C12); creating it again with the same definition opens it instead.
    #[error("table {0} exists with a different schema or options")]
    TableExistsWithDifferentSchema(String),
    /// No index of that name on the table; the message names the closest index and the
    /// table's indexes.
    #[error("index not found: {0}")]
    IndexNotFound(NotFound),
    /// An index of that name exists with a different spec.
    #[error("index {0} exists with a different spec")]
    IndexExists(String),
    /// An index spec, or the table's list of them, is malformed. Scans and writes still work.
    #[error("invalid index spec: {0}")]
    InvalidIndexSpec(String),
    /// The table has an index of a kind this client has not registered; the message names the
    /// registered kinds.
    #[error("index kind not registered in this client: {0}")]
    IndexKindNotRegistered(NotFound),
    /// A sidecar could not be used (it is treated as missing wherever a query meets it).
    #[error(transparent)]
    Sidecar(#[from] crate::index::bundle::BundleError),
    /// A local file operation failed (the sidecar cache, a build's temporary file).
    #[error("i/o: {0}")]
    Io(#[source] Arc<std::io::Error>),
    /// The query would score more rows on the exact route than `max_exact_scan_rows` allows
    /// (case V13, policy `ExactScanPolicy::Error`); build the index or narrow the filter.
    #[error("exact route needs {rows} rows, more than the limit of {limit}")]
    ExactScanLimit {
        /// Rows the exact route would score.
        rows: u64,
        /// The configured `max_exact_scan_rows`.
        limit: u64,
    },
    /// A full-text index names a tokenizer this client has not registered; the message names
    /// the registered ones.
    #[error("tokenizer not registered: {0}")]
    TokenizerNotRegistered(NotFound),
    /// The table uses a format moraine cannot read: a newer `moraine.formatVersion`, a Delta
    /// protocol or feature beyond reader 1 and writer 2, or a column type it does not support
    /// (docs/FORMAT.md). Nothing is read or written.
    #[error("unsupported table feature: {0}")]
    UnsupportedTableFeature(String),
    /// The store does not honour conditional writes (capability probe, case S2). Reading works.
    #[error("the object store does not honour conditional writes ({0}); open it read-only")]
    ConditionalWritesUnsupported(String),
    /// Configuration that breaks a rule checked at open (for example the grace-period
    /// inequality, case G6).
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    /// Conflicts kept invalidating the write until the attempt limit or the operation deadline.
    #[error("gave up after {attempts} attempts that conflicted with other writers")]
    TooManyConflicts {
        /// Attempts made, the first one included.
        attempts: u32,
    },
    /// The write took longer than the commit deadline before committing (cases C6, C20).
    /// Nothing was written; split the write.
    #[error("the write did not commit within the commit deadline ({elapsed:?}); split it")]
    CommitDeadlineExceeded {
        /// How long the write had run.
        elapsed: Duration,
    },
    /// The commit may or may not have happened; its transaction id could not be looked up.
    /// Write with [`Table::txn`](crate::Table::txn) to make a retry safe.
    #[error("commit outcome unknown for transaction {txn}")]
    OutcomeUnknown {
        /// The transaction id of the commit attempt.
        txn: String,
    },
    /// The write was not committed because the task that commits the table handle's writes
    /// stopped before it took them (D64): its Tokio runtime shut down. Nothing was written.
    #[error("the write was cancelled before it committed: the table's commit task stopped")]
    Cancelled,
    /// A read met a data file that vacuum has removed: the snapshot is older than the grace
    /// period. Take a new snapshot.
    #[error("the snapshot is older than the grace period and its files are gone")]
    SnapshotExpired,
    /// A blob's object does not exist (a value whose row is older than the grace period, or a
    /// reference made up by hand).
    #[error("blob not found: {0}")]
    BlobNotFound(NotFound),
    /// A blob's bytes do not match the size or CRC-32C its row records (case B10).
    #[error("blob corrupt: {0}")]
    BlobCorrupt(String),
    /// A file a replace or compaction removes is no longer in the table: another commit
    /// removed it first. Nothing was written; plan the replace again against a fresh snapshot.
    #[error("file {0} is no longer in the table")]
    FilesChanged(String),
    /// A maintenance commit carried a lease generation older than one already committed for
    /// its job (D29 fencing): another holder took the lease over. Nothing was written.
    #[error("lease {job} generation {generation} was superseded by generation {current}")]
    Fenced {
        /// The lease's job.
        job: String,
        /// The generation the commit carried.
        generation: i64,
        /// The newest generation already committed.
        current: i64,
    },
    /// Another client holds the lease of this job ([`crate::Table::purge`]); try again once it
    /// is released or its time to live has passed.
    #[error("another client holds the {job} lease")]
    LeaseHeld {
        /// The lease's job.
        job: String,
    },
    /// Queued writes were committed by another ingestor first (D51). Nothing was written.
    #[error("queued writes {0} were ingested by another client")]
    AlreadyIngested(String),
    /// No pin of that name exists; the message names the closest pin and the table's pins.
    #[error("pin not found: {0}")]
    PinNotFound(NotFound),
    /// The version is older than the log and the grace period keep, or than the newest purge:
    /// it can no longer be pinned or read.
    #[error("version {0} is no longer available")]
    VersionNotAvailable(u64),
    /// The store kept throttling (429, 503 SlowDown) until the operation deadline.
    #[error("throttled by the object store after {attempts} attempts")]
    Throttled {
        /// Attempts made, the first one included.
        attempts: u32,
    },
    /// A store request kept timing out until the operation deadline.
    #[error("object store request timed out: {0}")]
    Timeout(String),
    /// An object moraine reads to know the table's state does not parse: a log entry, a
    /// checkpoint, a table property, a pin manifest or a lease record.
    #[error("corrupt {object}: {reason}")]
    Corrupt {
        /// The object, such as `log entry 12`, `checkpoint 40` or `table property
        /// moraine.pins`.
        object: String,
        /// What is wrong with it.
        reason: String,
    },
    /// Any other error of the object store, after retries.
    #[error(transparent)]
    Storage(StorageError),
    /// An Arrow error (casting, concatenating or encoding batches).
    #[error("arrow: {0}")]
    Arrow(#[source] LibraryError),
    /// A Parquet error reading or writing a data file.
    #[error("parquet: {0}")]
    Parquet(#[source] LibraryError),
    /// A DataFusion error planning or evaluating a filter or an update expression.
    #[error("datafusion: {0}")]
    DataFusion(#[source] LibraryError),
    /// A bug in moraine. Please report it.
    #[error("internal error: {0}")]
    Internal(String),
}

/// What kind of object-store failure a [`StorageError`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StorageErrorKind {
    /// The object does not exist.
    NotFound,
    /// A create-if-absent target exists.
    AlreadyExists,
    /// A conditional request's precondition failed.
    Precondition,
    /// The credentials are missing or do not allow the request.
    PermissionDenied,
    /// The store refused the request for its rate (429, 503 SlowDown).
    Throttled,
    /// The request certainly did not reach the store (connection refused, DNS failure).
    Unavailable,
    /// The request may or may not have taken effect: a timeout, a reset connection, a 5xx
    /// response, a truncated body.
    Interrupted,
    /// Anything else: configuration, unsupported operations, bad paths.
    Other,
}

/// An object-store error. The store library's own error is its
/// [`source`](std::error::Error::source).
#[derive(Clone, Debug)]
pub struct StorageError {
    kind: StorageErrorKind,
    inner: Arc<object_store::Error>,
}

impl StorageError {
    /// What kind of failure this is.
    pub fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    /// Whether the object does not exist.
    pub fn is_not_found(&self) -> bool {
        self.kind == StorageErrorKind::NotFound
    }

    /// Whether the store refused the request for its rate.
    pub fn is_throttled(&self) -> bool {
        self.kind == StorageErrorKind::Throttled
    }

    /// The store library's error.
    pub(crate) fn inner(&self) -> &object_store::Error {
        &self.inner
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.inner.as_ref())
    }
}

impl From<object_store::Error> for StorageError {
    fn from(e: object_store::Error) -> Self {
        let kind = match &e {
            object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. } => StorageErrorKind::PermissionDenied,
            other => match classify(other) {
                Class::NotFound => StorageErrorKind::NotFound,
                Class::AlreadyExists => StorageErrorKind::AlreadyExists,
                Class::Precondition => StorageErrorKind::Precondition,
                Class::Throttled => StorageErrorKind::Throttled,
                Class::Unavailable => StorageErrorKind::Unavailable,
                Class::Ambiguous | Class::Corrupt => StorageErrorKind::Interrupted,
                Class::NotModified | Class::Permanent => StorageErrorKind::Other,
            },
        };
        StorageError {
            kind,
            inner: Arc::new(e),
        }
    }
}

/// An error of a library moraine uses (Arrow, Parquet, DataFusion). It displays as the
/// library's error; [`LibraryError::get_ref`] gives the error itself, to downcast.
#[derive(Clone, Debug)]
pub struct LibraryError(Arc<dyn std::error::Error + Send + Sync + 'static>);

impl LibraryError {
    fn new(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        LibraryError(Arc::new(e))
    }

    /// The library's error, for `downcast_ref`.
    pub fn get_ref(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.0.as_ref()
    }
}

impl fmt::Display for LibraryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for LibraryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::Storage(StorageError::from(e))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(Arc::new(e))
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(e: arrow::error::ArrowError) -> Self {
        Error::Arrow(LibraryError::new(e))
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error::Parquet(LibraryError::new(e))
    }
}

impl From<datafusion::error::DataFusionError> for Error {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Error::DataFusion(LibraryError::new(e))
    }
}

impl Error {
    /// Whether this is a read of an object that is gone: a data file vacuum removed under a
    /// snapshot older than the grace period.
    pub(crate) fn is_missing_object(&self) -> bool {
        matches!(self, Error::Storage(s) if matches!(s.inner(), object_store::Error::NotFound { .. }))
    }

    /// [`Error::Corrupt`] for `object`.
    pub(crate) fn corrupt(object: impl Into<String>, reason: impl fmt::Display) -> Error {
        Error::Corrupt {
            object: object.into(),
            reason: reason.to_string(),
        }
    }

    /// [`Error::SnapshotExpired`] for a missing object, the error itself otherwise.
    pub(crate) fn expired(self) -> Error {
        if self.is_missing_object() {
            Error::SnapshotExpired
        } else {
            self
        }
    }

    /// Whether calling again later can succeed without changing the call.
    ///
    /// [`Error::OutcomeUnknown`] is not retriable: repeating an append whose first attempt
    /// committed would store it twice.
    pub fn is_retriable(&self) -> bool {
        match self {
            Error::TooManyConflicts { .. }
            | Error::CommitDeadlineExceeded { .. }
            | Error::SnapshotExpired
            | Error::Cancelled
            | Error::Throttled { .. }
            | Error::Timeout(_) => true,
            Error::Storage(e) => matches!(
                e.kind(),
                StorageErrorKind::Throttled
                    | StorageErrorKind::Unavailable
                    | StorageErrorKind::Interrupted
            ),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoreFault;

    #[test]
    fn retriable_variants() {
        assert!(Error::TooManyConflicts { attempts: 3 }.is_retriable());
        assert!(Error::Throttled { attempts: 3 }.is_retriable());
        assert!(!Error::OutcomeUnknown { txn: "w-1".into() }.is_retriable());
        assert!(!Error::InvalidBatch("x".into()).is_retriable());
        let throttled = StoreFault::new(Class::Throttled, "503 SlowDown").into_store_error();
        assert!(Error::from(throttled).is_retriable());
        let denied = object_store::Error::PermissionDenied {
            path: "p".into(),
            source: "no".into(),
        };
        assert!(!Error::from(denied).is_retriable());
    }
}
