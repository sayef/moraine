//! A table handle: its snapshot, refresh, writes and reads.
//!
//! A handle keeps one current snapshot and only ever moves it forward (case I5). Writes go
//! through the handle's group-commit queue ([`write`]); reads pin a snapshot for their whole
//! duration ([`scan`]).

mod blobs;
mod index;
mod mutate;
mod pins;
mod refresh;
mod replace;
mod scan;
mod write;

use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;

use crate::db::DbShared;
use crate::error::{Error, Result};
use crate::log::{CommitLayer, LogEntry, TxnId};
use crate::options::{Committed, Consistency, DuplicatePolicy, TableDef, WriteOutcome};
use crate::schema::{
    check_unique_keys, column_not_found, conform, key_rows, project_schema, validate_new_schema,
};
use crate::snapshot::Snapshot;
use crate::store::classify;
use mutate::Keys;
use write::Pending;

pub(crate) use index::Change;
pub(crate) use mutate::ReplaceWork;
pub(crate) use pins::{parse_pins, purged};
pub use refresh::RefreshMetrics;
pub use replace::Replace;
pub use scan::{Scan, SnapshotFile, TableSnapshot, Take};

/// Counters of a table handle's writes ([`Table::write_metrics`]).
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct WriteMetrics {
    /// Objects (data files, their sidecars, blobs) that writes through this handle uploaded and
    /// then did not commit, deleted when the write ended (D65).
    pub uploads_deleted: std::sync::atomic::AtomicU64,
    /// Such objects left for vacuum: the write's outcome was unknown, or a delete failed.
    pub uploads_left: std::sync::atomic::AtomicU64,
}

/// A handle on one table. Cheap to clone; clones share the snapshot and the commit queue.
#[derive(Clone, Debug)]
pub struct Table {
    pub(crate) inner: Arc<TableInner>,
}

#[derive(Debug)]
pub(crate) struct TableInner {
    pub name: String,
    pub db: Arc<DbShared>,
    pub layer: Arc<dyn CommitLayer>,
    current: Mutex<Arc<Snapshot>>,
    consistency: Mutex<Consistency>,
    pub queue: Mutex<write::Queue>,
    pub small: Mutex<Option<Arc<scan::Materialised>>>,
    /// How this handle's recent log puts went, for the starvation guard (D28).
    pub contention: Mutex<mutate::Contention>,
    /// Whether the background refresh task runs.
    background: std::sync::atomic::AtomicBool,
    refresh_metrics: RefreshMetrics,
    /// Counters of this handle's writes.
    pub write_metrics: WriteMetrics,
    /// Pinned versions read through this handle, by (name, version).
    pub pinned: Mutex<std::collections::HashMap<(String, u64), Arc<Snapshot>>>,
    /// Maintenance state of this handle: leases held, removals seen (D50).
    pub maint: Mutex<crate::maintain::HandleState>,
    /// Queued-write submissions of this handle (D51), one at a time.
    pub submit_state: tokio::sync::Mutex<crate::maintain::inbox::SubmitState>,
    /// Blobs this handle uploaded ahead of a write ([`Table::upload_blob`]).
    pub staged: crate::blob::Staged,
    /// Blob keys of data files, which never change (blob collection's mark phase).
    pub blob_keys: Mutex<std::collections::HashMap<String, Arc<std::collections::HashSet<String>>>>,
}

impl Table {
    pub(crate) fn new(
        name: String,
        db: Arc<DbShared>,
        layer: Arc<dyn CommitLayer>,
        snap: Snapshot,
    ) -> Table {
        let consistency = db.opts.consistency;
        let t = Table {
            inner: Arc::new(TableInner {
                name,
                db,
                layer,
                current: Mutex::new(Arc::new(snap)),
                consistency: Mutex::new(consistency),
                queue: Mutex::new(write::Queue::default()),
                small: Mutex::new(None),
                contention: Mutex::new(mutate::Contention::default()),
                background: std::sync::atomic::AtomicBool::new(false),
                refresh_metrics: RefreshMetrics::default(),
                write_metrics: WriteMetrics::default(),
                pinned: Mutex::default(),
                maint: Mutex::default(),
                submit_state: tokio::sync::Mutex::default(),
                staged: Default::default(),
                blob_keys: Mutex::default(),
            }),
        };
        t.inner.start_background();
        t
    }

    /// The table's name in its database.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The table's id ([`TableSnapshot::table_id`]): stable for the table's life, new when a
    /// table of the same name is created again.
    pub fn id(&self) -> String {
        self.inner.current().meta.id.clone()
    }

    /// The version of the snapshot in hand.
    pub fn version(&self) -> u64 {
        self.inner.current().version
    }

    /// The table schema at the snapshot in hand.
    pub fn schema(&self) -> SchemaRef {
        self.inner.current().schema().clone()
    }

    /// The table's options as stored in its properties.
    pub fn definition(&self) -> TableDef {
        self.inner.current().def.as_ref().clone()
    }

    /// Changes when this handle looks for newer commits.
    pub fn set_consistency(&self, c: Consistency) {
        if let Ok(mut g) = self.inner.consistency.lock() {
            *g = c;
        }
        self.inner.start_background();
    }

    /// Counters of the background refresh ([`Consistency::Background`]).
    pub fn refresh_metrics(&self) -> &RefreshMetrics {
        &self.inner.refresh_metrics
    }

    /// Counters of this handle's writes.
    pub fn write_metrics(&self) -> &WriteMetrics {
        &self.inner.write_metrics
    }

    /// Moves the handle to the newest committed version. One GET when nothing is new (D18).
    pub async fn refresh(&self) -> Result<u64> {
        Ok(self.inner.refresh().await?.version)
    }

    /// A snapshot pinned for reading, refreshed first as the consistency mode asks.
    pub async fn snapshot(&self) -> Result<TableSnapshot> {
        let snap = self.inner.snapshot_for_query().await?;
        Ok(TableSnapshot::new(self.inner.clone(), snap))
    }

    /// The snapshot the handle holds, without any request, whatever the consistency mode
    /// (D39). A handle's own commits advance it: right after a write returns version `v`, this
    /// snapshot is at `v` or later, with the write's files in it. Commits of other writers
    /// appear after a refresh.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table, batch: arrow::array::RecordBatch) -> moraine::Result<()> {
    /// let committed = t.append(vec![batch]).await?;
    /// let snap = t.snapshot_in_hand();
    /// assert!(snap.version() >= committed.version);
    /// for f in snap.files() {
    ///     println!("{} ({} bytes, added in {:?})", f.path, f.size, f.added_in);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn snapshot_in_hand(&self) -> TableSnapshot {
        TableSnapshot::new(self.inner.clone(), self.inner.current())
    }

    /// Appends rows. The table must not enforce its primary key (case C3); returns the
    /// committed version, or the current one when there is nothing to write (case C19).
    pub async fn append(&self, batches: Vec<RecordBatch>) -> Result<Committed> {
        committed(self.append_rows(batches, false, None).await)
    }

    /// Appends rows without the primary-key check, for bulk loads into a table known not to
    /// hold the keys. Duplicate keys inside the call are still refused (case D2).
    pub async fn append_unchecked(&self, batches: Vec<RecordBatch>) -> Result<Committed> {
        committed(self.append_rows(batches, true, None).await)
    }

    /// Writes that carry the transaction id `(app_id, version)` (D32): each is applied at most
    /// once, across retries, ambiguous outcomes, processes and restarts.
    ///
    /// A write whose `version` is at or below the newest version committed for `app_id`
    /// returns [`WriteOutcome::AlreadyApplied`] and writes nothing. Otherwise it commits with a
    /// Delta `txn` action, even when it changes no row (so `append_unchecked(vec![])` records
    /// the id alone, on any table). Versions need not be consecutive, but must grow: a write
    /// with a lower version than one already committed is treated as applied, so an application
    /// must send its writes in version order and retry each until it is committed or already
    /// applied before sending the next.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table, batch: arrow::array::RecordBatch) -> moraine::Result<()> {
    /// use moraine::WriteOutcome;
    /// // Replaying a write-ahead log: entries already in the table are skipped.
    /// match t.txn("wal-7f3a", 42).append(vec![batch]).await? {
    ///     WriteOutcome::Committed(c) => println!("committed as version {}", c.version),
    ///     WriteOutcome::AlreadyApplied { txn_version, .. } => {
    ///         println!("already applied (newest for this id: {txn_version})")
    ///     }
    ///     other => println!("in version {}", other.version()),
    /// }
    /// assert_eq!(t.last_txn_version("wal-7f3a").await?, Some(42));
    /// # Ok(()) }
    /// ```
    pub fn txn(&self, app_id: impl Into<String>, version: i64) -> TxnWrite {
        TxnWrite {
            table: self.clone(),
            txn: TxnId::new(app_id, version),
        }
    }

    /// The newest transaction version committed for `app_id` ([`Table::txn`]), after moving the
    /// handle to the newest committed version (one GET when nothing is new). `None` when the
    /// application id has never committed.
    pub async fn last_txn_version(&self, app_id: &str) -> Result<Option<i64>> {
        Ok(self.inner.refresh().await?.txns.get(app_id))
    }

    async fn append_rows(
        &self,
        batches: Vec<RecordBatch>,
        unchecked: bool,
        txn: Option<TxnId>,
    ) -> Result<WriteOutcome> {
        let snap = self.inner.current();
        let def = &snap.def;
        if def.enforce_primary_key && !unchecked {
            return Err(Error::PrimaryKeyEnforced);
        }
        let conformed: Vec<RecordBatch> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .map(|b| conform(b, snap.schema(), def))
            .collect::<Result<_>>()?;
        if def.enforce_primary_key {
            check_unique_keys(&conformed, &def.primary_key)?;
        }
        if conformed.is_empty() {
            return self.nothing(txn).await;
        }
        self.inner.enqueue(Pending::Append(conformed), txn).await
    }

    /// A write that changes no row: without a transaction id, the current version (case C19);
    /// with one, a commit that records the id alone.
    async fn nothing(&self, txn: Option<TxnId>) -> Result<WriteOutcome> {
        match txn {
            None => Ok(WriteOutcome::Committed(Committed::new(
                self.inner.current().version,
            ))),
            Some(t) => {
                self.inner
                    .enqueue(Pending::Append(Vec::new()), Some(t))
                    .await
            }
        }
    }

    /// Inserts rows, replacing every stored row with the same primary key.
    /// A key twice in the batches is [`Error::DuplicateKeyInBatch`] (case D2). Returns the
    /// committed version. Two writers upserting the same key both succeed; the later commit's
    /// row remains (case C2).
    pub async fn upsert(&self, batches: Vec<RecordBatch>) -> Result<Committed> {
        self.upsert_with(batches, DuplicatePolicy::Error).await
    }

    /// [`Table::upsert`] with a choice of what a key twice in the batches means.
    pub async fn upsert_with(
        &self,
        batches: Vec<RecordBatch>,
        duplicates: DuplicatePolicy,
    ) -> Result<Committed> {
        committed(self.upsert_rows(batches, duplicates, None).await)
    }

    async fn upsert_rows(
        &self,
        batches: Vec<RecordBatch>,
        duplicates: DuplicatePolicy,
        txn: Option<TxnId>,
    ) -> Result<WriteOutcome> {
        let snap = self.inner.current();
        let def = &snap.def;
        if def.primary_key.is_empty() {
            return Err(Error::PrimaryKeyRequired);
        }
        let conformed: Vec<RecordBatch> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .map(|b| conform(b, snap.schema(), def))
            .collect::<Result<_>>()?;
        if conformed.is_empty() {
            return self.nothing(txn).await;
        }
        let conformed = match duplicates {
            DuplicatePolicy::Error => {
                check_unique_keys(&conformed, &def.primary_key)?;
                conformed
            }
            DuplicatePolicy::LastWins => {
                vec![last_wins(snap.schema(), &conformed, &def.primary_key)?]
            }
        };
        let keys = Keys::of(&def.primary_key, &conformed)?;
        self.inner
            .enqueue(Pending::Upsert(conformed, keys), txn)
            .await
    }

    /// Removes the rows for which `predicate` is true. Returns the committed version, or the
    /// current one when no row matches (case C19).
    pub async fn delete(&self, predicate: Expr) -> Result<Committed> {
        committed(self.delete_where(predicate, None).await)
    }

    async fn delete_where(&self, predicate: Expr, txn: Option<TxnId>) -> Result<WriteOutcome> {
        let snap = self.inner.current();
        self.inner
            .db
            .physical_expr(predicate.clone(), snap.schema())?;
        self.inner.enqueue(Pending::Delete(predicate), txn).await
    }

    /// Removes the rows with these primary keys (one value per key column each). Returns the
    /// committed version, or the current one when none is stored (case C19).
    pub async fn delete_keys(&self, keys: &[Vec<ScalarValue>]) -> Result<Committed> {
        committed(self.delete_by_keys(keys, None).await)
    }

    async fn delete_by_keys(
        &self,
        keys: &[Vec<ScalarValue>],
        txn: Option<TxnId>,
    ) -> Result<WriteOutcome> {
        let snap = self.inner.current();
        let def = &snap.def;
        if def.primary_key.is_empty() {
            return Err(Error::PrimaryKeyRequired);
        }
        if keys.is_empty() {
            return self.nothing(txn).await;
        }
        let key_schema = project_schema(snap.schema(), &def.primary_key)?;
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(def.primary_key.len());
        for (i, f) in key_schema.fields().iter().enumerate() {
            let mut values = Vec::with_capacity(keys.len());
            for k in keys {
                if k.len() != def.primary_key.len() {
                    return Err(Error::InvalidQuery(format!(
                        "the key has {} columns, the value {}",
                        def.primary_key.len(),
                        k.len()
                    )));
                }
                if k[i].is_null() {
                    return Err(Error::InvalidQuery(format!(
                        "null in key column {:?}",
                        f.name()
                    )));
                }
                values.push(k[i].clone());
            }
            let a = ScalarValue::iter_to_array(values)?;
            cols.push(if a.data_type() == f.data_type() {
                a
            } else {
                arrow::compute::cast(&a, f.data_type())?
            });
        }
        let batch = RecordBatch::try_new(key_schema, cols)?;
        let keys = Keys::of(&def.primary_key, &[batch])?;
        self.inner.enqueue(Pending::DeleteKeys(keys), txn).await
    }

    /// Sets `assignments` (column, expression over the row) in the rows for which `predicate`
    /// is true. Primary-key columns cannot be assigned (delete and upsert instead). Returns the
    /// committed version, or the current one when no row matches (case C19).
    pub async fn update(
        &self,
        predicate: Expr,
        assignments: Vec<(String, Expr)>,
    ) -> Result<Committed> {
        committed(self.update_where(predicate, assignments, None).await)
    }

    async fn update_where(
        &self,
        predicate: Expr,
        assignments: Vec<(String, Expr)>,
        txn: Option<TxnId>,
    ) -> Result<WriteOutcome> {
        let snap = self.inner.current();
        let schema = snap.schema();
        if assignments.is_empty() {
            return self.nothing(txn).await;
        }
        self.inner.db.physical_expr(predicate.clone(), schema)?;
        let mut seen = std::collections::HashSet::new();
        for (c, e) in &assignments {
            let f = schema
                .field_with_name(c)
                .map_err(|_| column_not_found(c, schema))?;
            if snap.def.primary_key.contains(c) {
                return Err(Error::InvalidQuery(format!(
                    "primary key column {c:?} cannot be updated; delete and upsert instead"
                )));
            }
            // A new blob value has to be uploaded or copied, which only the
            // paths that write new rows do.
            if crate::blob::is_blob(f.data_type()) {
                return Err(Error::InvalidQuery(format!(
                    "blob column {c:?} cannot be assigned by an update; upsert the row instead"
                )));
            }
            if !seen.insert(c.as_str()) {
                return Err(Error::InvalidQuery(format!("column {c:?} assigned twice")));
            }
            let p = self.inner.db.physical_expr(e.clone(), schema)?;
            let t = p.data_type(schema)?;
            if !arrow::compute::can_cast_types(&t, f.data_type()) {
                return Err(Error::InvalidQuery(format!(
                    "column {c:?} has type {}, the expression {t}",
                    f.data_type()
                )));
            }
        }
        self.inner
            .enqueue(Pending::Update(predicate, assignments), txn)
            .await
    }

    /// Adds a nullable column; existing rows, and appends prepared before it, read it as null
    /// (case C10, D9). Adding a column that exists with the same type changes nothing.
    pub async fn add_column(&self, field: Field) -> Result<Committed> {
        let snap = self.inner.current();
        let ty = crate::schema::delta_item_names(field.data_type());
        let field = field.with_data_type(ty);
        if !field.is_nullable() {
            return Err(Error::InvalidSchema(format!(
                "added column {:?} must be nullable",
                field.name()
            )));
        }
        if snap.schema().field_with_name(field.name()).is_err() {
            let mut fields: Vec<Field> = snap
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            fields.push(field.clone());
            let new = self
                .inner
                .layer
                .canonical_schema(&arrow::datatypes::Schema::new(fields))?;
            validate_new_schema(&new, &snap.def)?;
        }
        committed(self.inner.enqueue(Pending::AddColumn(field), None).await)
    }

    /// A scan of the table; the snapshot is taken when it runs.
    pub fn scan(&self) -> Scan {
        Scan::new(self.inner.clone(), None)
    }

    /// The row with primary key `key` (one value per key column), or `None`. Without key
    /// enforcement a key can be stored more than once; then the row committed last wins.
    pub async fn get(&self, key: &[ScalarValue]) -> Result<Option<RecordBatch>> {
        match self.snapshot().await?.get(key).await {
            // A file vacuum removed under an old snapshot: once more at the newest version.
            Err(e) if e.is_missing_object() => {
                let snap = self.inner.refresh().await?;
                TableSnapshot::new(self.inner.clone(), snap)
                    .get(key)
                    .await
                    .map_err(Error::expired)
            }
            r => r,
        }
    }
}

/// The version of a write without a transaction id, which is never "already applied".
fn committed(r: Result<WriteOutcome>) -> Result<Committed> {
    r.map(|o| Committed::new(o.version()))
}

/// Writes that carry one caller transaction id; made by [`Table::txn`]. Each method is the
/// [`Table`] method of the same name, returning a [`WriteOutcome`].
#[derive(Clone, Debug)]
pub struct TxnWrite {
    table: Table,
    txn: TxnId,
}

impl TxnWrite {
    /// The transaction id the writes carry.
    pub fn id(&self) -> &TxnId {
        &self.txn
    }

    fn checked(&self) -> Result<Option<TxnId>> {
        if self.txn.app_id.is_empty() {
            return Err(Error::InvalidQuery(
                "a transaction id needs a non-empty application id".into(),
            ));
        }
        if self.txn.app_id == self.table.inner.db.writer_id {
            return Err(Error::InvalidQuery(format!(
                "application id {:?} is this handle's own writer id",
                self.txn.app_id
            )));
        }
        Ok(Some(self.txn.clone()))
    }

    /// [`Table::append`] with the transaction id.
    pub async fn append(&self, batches: Vec<RecordBatch>) -> Result<WriteOutcome> {
        self.table
            .append_rows(batches, false, self.checked()?)
            .await
    }

    /// [`Table::append_unchecked`] with the transaction id. With no rows it records the id
    /// alone, on any table.
    pub async fn append_unchecked(&self, batches: Vec<RecordBatch>) -> Result<WriteOutcome> {
        self.table.append_rows(batches, true, self.checked()?).await
    }

    /// [`Table::upsert`] with the transaction id.
    pub async fn upsert(&self, batches: Vec<RecordBatch>) -> Result<WriteOutcome> {
        self.upsert_with(batches, DuplicatePolicy::Error).await
    }

    /// [`Table::upsert_with`] with the transaction id.
    pub async fn upsert_with(
        &self,
        batches: Vec<RecordBatch>,
        duplicates: DuplicatePolicy,
    ) -> Result<WriteOutcome> {
        self.table
            .upsert_rows(batches, duplicates, self.checked()?)
            .await
    }

    /// [`Table::delete`] with the transaction id; it commits even when no row matches.
    pub async fn delete(&self, predicate: Expr) -> Result<WriteOutcome> {
        self.table.delete_where(predicate, self.checked()?).await
    }

    /// [`Table::delete_keys`] with the transaction id; it commits even when no key is stored.
    pub async fn delete_keys(&self, keys: &[Vec<ScalarValue>]) -> Result<WriteOutcome> {
        self.table.delete_by_keys(keys, self.checked()?).await
    }

    /// [`Table::update`] with the transaction id; it commits even when no row matches.
    pub async fn update(
        &self,
        predicate: Expr,
        assignments: Vec<(String, Expr)>,
    ) -> Result<WriteOutcome> {
        self.table
            .update_where(predicate, assignments, self.checked()?)
            .await
    }
}

impl TableInner {
    pub fn current(&self) -> Arc<Snapshot> {
        self.current
            .lock()
            .map(|g| g.clone())
            .unwrap_or_else(|p| p.into_inner().clone())
    }

    /// Makes `s` current unless the handle already has a newer version (monotonic reads, I5).
    pub fn install(&self, s: Snapshot) -> Arc<Snapshot> {
        let mut g = self.current.lock().unwrap_or_else(|p| p.into_inner());
        if s.version > g.version || (s.version == g.version && s.confirmed_at > g.confirmed_at) {
            *g = Arc::new(s);
        }
        g.clone()
    }

    fn consistency(&self) -> Consistency {
        self.consistency.lock().map(|g| *g).unwrap_or_default()
    }

    pub async fn snapshot_for_query(&self) -> Result<Arc<Snapshot>> {
        let cur = self.current();
        let now = self.db.clock().now();
        let refresh = match self.consistency() {
            Consistency::Strong => true,
            Consistency::Manual => false,
            Consistency::Background { interval } => self.background_stale(interval, &cur),
            c @ Consistency::Eventual { .. } => {
                now.saturating_sub(cur.confirmed_at) > c.max_staleness()
            }
        };
        if refresh {
            self.refresh().await
        } else {
            Ok(cur)
        }
    }

    /// Reads the log entries after the snapshot in hand and applies them.
    ///
    /// Entries are read by GET until one is missing, without listing (D18). A handle whose
    /// snapshot was last confirmed more than a commit deadline ago lists the log first: by then
    /// log cleanup may have removed the entry it would probe, and a missing
    /// entry would look like "nothing new".
    pub async fn refresh(&self) -> Result<Arc<Snapshot>> {
        let clock = self.db.clock().clone();
        let mut base = self.current();
        let stale = clock.now().saturating_sub(base.confirmed_at) > base.def.commit_deadline();
        let mut listed = None;
        if stale {
            let after = self.layer.list_versions_after(base.version).await?;
            if after.first().is_some_and(|v| *v != base.version + 1) {
                return self.reload().await;
            }
            listed = after.last().copied();
        }
        let entries = self.probe(&base, base.version + 1).await?;
        // Entries the listing showed that probing could not reach were cleaned up meanwhile:
        // reload from the checkpoint rather than take "missing" for "nothing new".
        if listed.is_some_and(|l| entries.last().map_or(base.version, |e| e.version) < l) {
            return self.reload().await;
        }
        base = Arc::new(base.apply(&entries)?);
        let mut s = base.as_ref().clone();
        s.confirmed_at = clock.now();
        Ok(self.install(s))
    }

    /// Loads the newest version from a listing (and the newest checkpoint).
    async fn reload(&self) -> Result<Arc<Snapshot>> {
        let loaded = self.layer.load().await?;
        let s = Snapshot::from_loaded(loaded, self.db.clock().now())?;
        Ok(self.install(s))
    }

    /// Log entries from `from` on, read by GET until one is missing. Reads one at a time; after
    /// two found in a row, reads ahead in windows of up to the database's concurrency.
    pub async fn probe(&self, base: &Snapshot, from: u64) -> Result<Vec<LogEntry>> {
        use futures::StreamExt;
        let mut out: Vec<LogEntry> = Vec::new();
        let mut schema = base.schema().clone();
        let mut v = from;
        let mut window = 1usize;
        let mut found = 0usize;
        let max = self.db.opts.io_concurrency.max(1);
        loop {
            let wanted: Vec<u64> = (v..v + window as u64).collect();
            let layer = &self.layer;
            let sch = schema.clone();
            let got: Vec<Result<Option<LogEntry>>> = futures::stream::iter(wanted)
                .map(|x| {
                    let sch = sch.clone();
                    async move { layer.read_entry(x, Some(&sch)).await }
                })
                .buffered(window)
                .collect()
                .await;
            for r in got {
                match r? {
                    Some(e) => {
                        if let Some(m) = &e.meta {
                            schema = m.schema.clone();
                        }
                        out.push(e);
                        v += 1;
                        found += 1;
                    }
                    None => return Ok(out),
                }
            }
            if found >= 2 {
                window = (window * 2).min(max);
            }
        }
    }
}

/// Whether a store error from a read inside a commit attempt is worth another attempt.
pub(crate) fn transient(e: &Error) -> bool {
    match e {
        Error::Storage(s) => classify(s.inner()).retry_idempotent(),
        _ => false,
    }
}

/// The rows of `batches` with every key's last occurrence only, in their original order
/// (case D2, `DuplicatePolicy::LastWins`).
fn last_wins(schema: &SchemaRef, batches: &[RecordBatch], key: &[String]) -> Result<RecordBatch> {
    let all = arrow::compute::concat_batches(schema, batches)?;
    let rows = key_rows(&all, key)?;
    let mut seen = std::collections::HashSet::new();
    let mut keep: Vec<u32> = Vec::with_capacity(all.num_rows());
    for i in (0..all.num_rows()).rev() {
        if seen.insert(rows.row(i).as_ref().to_vec()) {
            keep.push(i as u32);
        }
    }
    keep.reverse();
    let idx = arrow::array::UInt32Array::from(keep);
    Ok(arrow::compute::take_record_batch(&all, &idx)?)
}
