//! Compile-time checks that the handles, builders, results and futures of the public API can
//! cross threads, so a field change that makes one `!Send` or `!Sync` fails the build instead
//! of a caller's `tokio::spawn`.

use std::future::Future;

use crate::blob::{Blob, BlobColumn, BlobRef};
use crate::index::{BuildOptions, IndexSpec, Search, SearchResult};
use crate::store::{RetryPolicy, StoreMetrics};
use crate::{
    CacheUsage, Committed, CompactionPolicy, Database, DatabaseBuilder, Error, MaintainOptions,
    MaintenanceMetrics, MaintenanceReport, MaintenanceTask, Purge, Replace, Scan, Table,
    TableOptions, TableSnapshot, Take, TxnWrite, WriteOutcome,
};

const fn send_sync<T: Send + Sync>() {}
const fn send<T: Send>() {}

const _: () = {
    send_sync::<Database>();
    send_sync::<DatabaseBuilder>();
    send_sync::<Table>();
    send_sync::<TableSnapshot>();
    send_sync::<TxnWrite>();
    send_sync::<Scan>();
    send_sync::<Take>();
    send_sync::<Search>();
    #[cfg(feature = "fts")]
    send_sync::<crate::index::hybrid::HybridSearch>();
    send_sync::<SearchResult>();
    send_sync::<MaintenanceTask>();
    send_sync::<MaintenanceMetrics>();
    send_sync::<MaintenanceReport>();
    send_sync::<Purge>();
    send_sync::<Blob>();
    send_sync::<BlobRef>();
    send_sync::<BlobColumn>();
    send_sync::<StoreMetrics>();
    send_sync::<CacheUsage>();
    send_sync::<Committed>();
    send_sync::<WriteOutcome>();
    send_sync::<Error>();
    send_sync::<TableOptions>();
    send_sync::<MaintainOptions>();
    send_sync::<CompactionPolicy>();
    send_sync::<BuildOptions>();
    send_sync::<RetryPolicy>();
    send_sync::<IndexSpec>();
    // A replace may hold caller streams, which are `Send` only.
    send::<Replace>();
};

fn future_is_send<F: Future + Send>(_: F) {}

/// Never called: it only has to type-check, which proves every future below is `Send`.
#[allow(dead_code, unused_must_use)]
fn futures_are_send(db: Database, t: Table, s: TableSnapshot, b: Blob) {
    future_is_send(Database::builder("memory://").open());
    future_is_send(db.open_table("t"));
    future_is_send(db.table_names());
    future_is_send(t.refresh());
    future_is_send(t.snapshot());
    future_is_send(t.append(Vec::new()));
    future_is_send(t.upsert(Vec::new()));
    future_is_send(t.delete(crate::expr::lit(true)));
    future_is_send(t.update(crate::expr::lit(true), Vec::new()));
    future_is_send(t.get(&[]));
    future_is_send(t.txn("app", 1).append(Vec::new()));
    future_is_send(t.replace_files(Replace::new()));
    future_is_send(t.scan().collect());
    future_is_send(t.scan().count());
    future_is_send(s.get(&[]));
    future_is_send(s.take("part.parquet", &[0]).collect());
    future_is_send(t.search_vector("emb", vec![0.0]).collect());
    #[cfg(feature = "fts")]
    future_is_send(t.search_hybrid("emb", vec![0.0], "body", "q").collect());
    future_is_send(t.build_indexes(BuildOptions::default()));
    future_is_send(t.pin("p", 1));
    future_is_send(t.snapshot_at_pin("p"));
    future_is_send(t.maintain(&MaintainOptions::default()));
    future_is_send(t.compact(&CompactionPolicy::default()));
    future_is_send(t.vacuum());
    future_is_send(t.purge(&Purge::new()));
    future_is_send(t.submit(Vec::new()));
    future_is_send(b.bytes());
    future_is_send(b.stream());
}
