//! moraine: a client-only, multi-writer database on object storage.
//!
//! A [`Database`] is a prefix of an object store (S3, an S3-compatible server, a local
//! directory or memory); each [`Table`] in it is a Delta Lake table whose log is the only
//! commit point. There is no server: every process that opens the prefix is a full client, and
//! any number of them write the same tables at once. Writes go through a per-handle
//! group-commit queue and cost one PUT per data file plus one conditional PUT of the log entry.
//! Reads pin a snapshot, prune files by their key ranges and pages by the Parquet page index,
//! and keep footers and small tables in memory per version.
//!
//! # Quick start
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow::array::{Float64Array, RecordBatch, StringArray};
//! use arrow::datatypes::{DataType, Field, Schema};
//! use moraine::expr::{col, lit};
//! use moraine::{Database, TableOptions};
//!
//! # #[tokio::main]
//! # async fn main() -> moraine::Result<()> {
//! // "s3://bucket/prefix" in production; a directory or "memory://" for tests.
//! let db = Database::builder("memory://").open().await?;
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("device", DataType::Utf8, false),
//!     Field::new("celsius", DataType::Float64, false),
//! ]));
//! let options = TableOptions::default().cluster_by(["device"]);
//! let table = db.create_table("readings", schema.clone(), options).await?;
//!
//! let batch = RecordBatch::try_new(
//!     schema,
//!     vec![
//!         Arc::new(StringArray::from(vec!["boiler", "attic"])),
//!         Arc::new(Float64Array::from(vec![61.5, 14.0])),
//!     ],
//! )?;
//! let committed = table.append(vec![batch]).await?;
//! println!("committed as version {}", committed.version);
//!
//! let boiler = table
//!     .scan()
//!     .filter(col("device").eq(lit("boiler")))
//!     .collect()
//!     .await?;
//! assert_eq!(boiler.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
//! # Ok(())
//! # }
//! ```
//!
//! # Where to go next
//!
//! - **Writing.** [`Table::append`], [`Table::upsert`], [`Table::delete`] and [`Table::update`]
//!   return a [`Committed`]; [`Table::txn`] makes a write exactly-once across retries and
//!   restarts ([`WriteOutcome`]). [`Table::replace_files`] swaps files for applications that
//!   manage their own layout.
//! - **Reading.** [`Table::scan`] ([`Scan`]), [`Table::get`] by primary key, and
//!   [`Table::snapshot`] for several reads of one version ([`TableSnapshot`]).
//! - **Consistency.** [`Consistency`] decides when a handle looks for other clients' commits.
//! - **Indexes.** [`index`]: vector search (feature `vector` or `turbovec`), full-text and
//!   hybrid search (feature `fts`), and the [`IndexKind`](index::IndexKind) trait for kinds of
//!   your own.
//! - **Blobs.** [`blob`]: large binary values stored as objects of their own, referenced from
//!   rows.
//! - **Maintenance.** [`Table::maintain`] and [`Table::spawn_maintenance`] compact,
//!   checkpoint, vacuum and build sidecars ([`MaintainOptions`]); [`Table::purge`] erases
//!   deleted rows for good.
//! - **Errors.** One [`Error`] type; [`Error::is_retriable`] says whether calling again can
//!   succeed.
//!
//! # Features
//!
//! None are on by default.
//!
//! | Feature | What it adds |
//! |---|---|
//! | `vector` | The `vec-hnsw` vector kind (usearch HNSW). |
//! | `turbovec` | The `vec-tq` vector kind (TurboQuant). |
//! | `fts` | The `fts` full-text kind (tantivy) and hybrid search. |
//! | `aws-sdk` | Credentials and region from the AWS SDK's default chain for `s3://` databases. |
//! | `failpoints` | Failpoints on the write path for moraine's crash tests; not for production. |
//!
//! Design decisions, with their reasons, are in `DECISIONS.md` in the repository; the
//! versioning policy is in `docs/SEMVER.md`.

#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod assertions;
pub mod blob;
pub mod clock;
pub(crate) mod commit;
mod db;
mod error;
#[doc(hidden)]
pub mod fuzzing;
pub mod index;
mod log;
mod maintain;
mod names;
mod options;
mod read;
mod schema;
mod snapshot;
mod stats;
pub mod store;
mod table;
#[doc(hidden)]
pub mod testing;
mod write;

pub use db::{CacheUsage, Database, DatabaseBuilder};
pub use error::{Error, LibraryError, Result, StorageError, StorageErrorKind};
pub use index::IndexSpec;
pub use log::TxnId;
pub use maintain::{
    CompactionPolicy, CompactionReport, IngestReport, Lease, LogReport, MaintainOptions,
    MaintenanceMetrics, MaintenanceReport, MaintenanceTask, Purge, PurgeReport, RepairReport,
    Submitted, VacuumReport, VerifyReport,
};
pub use names::NotFound;
pub use options::{Committed, Consistency, DuplicatePolicy, TableDef, TableOptions, WriteOutcome};
pub use table::{
    RefreshMetrics, Replace, Scan, SnapshotFile, Table, TableSnapshot, Take, TxnWrite, WriteMetrics,
};

/// Filter expressions (DataFusion's) and the value type used for keys.
pub mod expr {
    pub use datafusion::common::ScalarValue;
    pub use datafusion::logical_expr::Expr;
    pub use datafusion::prelude::{col, lit};
}
