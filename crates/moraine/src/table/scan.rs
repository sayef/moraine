//! Reads: scans with filters and projections, point lookups by primary key, and the per-version
//! materialisation of small tables (D20).

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::{col, lit};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use super::TableInner;
use crate::error::{Error, Result};
use crate::log::DataFile;
use crate::read::prune::{Bounds, may_match};
use crate::read::{FileQuery, bloom_excludes, project, read_file, to_schema};
use crate::schema::{column_not_found, project_schema};
use crate::snapshot::Snapshot;
use crate::stats::ColRange;

/// A table version held in memory as one batch, rows in commit order (D20).
pub(crate) struct Materialised {
    version: u64,
    schema: SchemaRef,
    /// Paths of the files it holds, in snapshot order.
    files: Vec<String>,
    batch: RecordBatch,
}

impl std::fmt::Debug for Materialised {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Materialised")
            .field("version", &self.version)
            .field("rows", &self.batch.num_rows())
            .finish()
    }
}

/// One live data file of a [`TableSnapshot`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SnapshotFile {
    /// Path relative to the table's data root, as the Delta `add` action names it (decoded).
    /// Data files are never rewritten, so a path names the same rows for as long as it lives.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Rows, from the file statistics (always present for files moraine wrote).
    pub rows: Option<u64>,
    /// Writer's wall-clock time of the write, in milliseconds since the Unix epoch.
    pub modification_time: i64,
    /// The version that added the file; `None` when it came from a checkpoint and is unknown.
    pub added_in: Option<u64>,
    /// Compaction level (D29): 0 for appended files, one more for each compaction that merged
    /// the rows into a new file.
    pub level: u32,
    /// Tags set with [`crate::Replace::tag`] (and moraine's own, under `moraine.`).
    pub tags: std::collections::BTreeMap<String, String>,
}

/// A table version pinned for reading (snapshot isolation per query).
#[derive(Clone, Debug)]
pub struct TableSnapshot {
    table: Arc<TableInner>,
    snap: Arc<Snapshot>,
}

impl TableSnapshot {
    pub(crate) fn new(table: Arc<TableInner>, snap: Arc<Snapshot>) -> Self {
        TableSnapshot { table, snap }
    }

    /// The table version this snapshot is.
    pub fn version(&self) -> u64 {
        self.snap.version
    }

    /// The table schema at this version.
    pub fn schema(&self) -> SchemaRef {
        self.snap.schema().clone()
    }

    /// Live data files.
    pub fn num_files(&self) -> usize {
        self.snap.files.len()
    }

    /// Rows according to the file statistics (exact for files moraine wrote).
    pub fn num_rows(&self) -> u64 {
        self.snap.files.iter().filter_map(|f| f.rows()).sum()
    }

    /// The live data files of this version, in snapshot order (D39). Costs no request: the
    /// snapshot already holds them.
    pub fn files(&self) -> Vec<SnapshotFile> {
        self.snap
            .files
            .iter()
            .map(|f| SnapshotFile {
                path: f.path.clone(),
                size: f.size,
                rows: f.rows(),
                modification_time: f.modification_time,
                added_in: (f.added_in > 0).then_some(f.added_in),
                level: f.level(),
                tags: f.tags.clone(),
            })
            .collect()
    }

    /// A scan of this version.
    pub fn scan(&self) -> Scan {
        Scan::new(self.table.clone(), Some(self.snap.clone()))
    }

    /// The table's id: Delta's `metaData.id`, set when the table was created and kept by
    /// every commit and checkpoint since. A table dropped and created again under the same
    /// name gets a new one, so the id tells caches keyed by table apart.
    pub fn table_id(&self) -> &str {
        &self.snap.meta.id
    }

    /// The rows at `offsets` (0 is a file's first row, in the file's own order: the order
    /// [`Scan::files`] reads it in) of data file `path` of this version, in the order of
    /// `offsets`, repeats included. Reads only the pages that hold them, through the footer
    /// cache. A path this version does not hold is [`Error::FilesChanged`], an offset beyond
    /// the file's rows [`Error::InvalidQuery`], and a file vacuum has collected
    /// [`Error::SnapshotExpired`].
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// let snap = t.snapshot().await?;
    /// let file = &snap.files()[0];
    /// let rows = snap.take(&file.path, &[7, 0, 3]).select(["value"]).collect().await?;
    /// assert_eq!(rows.num_rows(), 3);
    /// # Ok(()) }
    /// ```
    pub fn take(&self, path: &str, offsets: &[u64]) -> Take {
        Take {
            table: self.table.clone(),
            snap: self.snap.clone(),
            path: path.to_string(),
            offsets: offsets.to_vec(),
            columns: None,
        }
    }

    /// The newest transaction version committed for `app_id` at this version
    /// ([`crate::Table::txn`]), or `None`.
    pub fn last_txn_version(&self, app_id: &str) -> Option<i64> {
        self.snap.txns.get(app_id)
    }

    /// The row with primary key `key`, the one committed last when the key is stored twice.
    pub async fn get(&self, key: &[ScalarValue]) -> Result<Option<RecordBatch>> {
        let def = &self.snap.def;
        if def.primary_key.is_empty() {
            return Err(Error::PrimaryKeyRequired);
        }
        if key.len() != def.primary_key.len() {
            return Err(Error::InvalidQuery(format!(
                "the key has {} columns, the value {}",
                def.primary_key.len(),
                key.len()
            )));
        }
        let filter = def
            .primary_key
            .iter()
            .zip(key)
            .map(|(c, v)| col(c.as_str()).eq(lit(v.clone())))
            .reduce(Expr::and)
            .ok_or(Error::PrimaryKeyRequired)?;
        let all: Vec<String> = self
            .snap
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let plan = Plan::new(&self.table, &self.snap, Some(filter), all)?;
        if let Some(m) = self.table.small_table(&self.snap).await? {
            let rows = plan.filter_batch(&m.batch)?;
            return Ok(last_row(&rows));
        }
        // Newest files first, in windows read concurrently; the newest hit wins. Files whose
        // cached bloom filters exclude the key are dropped first, without a task or a request.
        let cache = &self.table.db.cache;
        let prefix = self.table.layer.data_prefix();
        let cands: Vec<Arc<DataFile>> = plan
            .candidates(&self.snap)
            .into_iter()
            .rev()
            .filter(|f| match &plan.filter {
                Some(e) => !bloom_excludes(cache, &prefix.clone().join(f.path.as_str()), f.size, e),
                None => true,
            })
            .collect();
        let window = self.table.db.opts.io_concurrency.max(1);
        // A key index this handle already knows rejects files without a read and narrows the
        // others to a page or a few rows (D19); without one the read below is still exact.
        let kb = crate::index::key::key_bytes(self.snap.schema(), &def.primary_key, key).ok();
        let plan = &plan;
        for chunk in cands.chunks(window) {
            let results: Vec<Vec<RecordBatch>> = futures::stream::iter(chunk.iter().cloned())
                .map(|f| {
                    let kb = kb.as_deref();
                    async move {
                        match kb {
                            Some(kb) => self.read_key(plan, f, kb).await,
                            None => plan.read(&self.table, f, false).await,
                        }
                    }
                })
                .buffered(window)
                .try_collect()
                .await?;
            for batches in results {
                if let Some(b) = batches.last()
                    && b.num_rows() > 0
                {
                    return Ok(Some(b.slice(b.num_rows() - 1, 1)));
                }
            }
        }
        Ok(None)
    }
}

impl TableSnapshot {
    /// One file's rows with the key, through its key index when one is known.
    async fn read_key(
        &self,
        plan: &Plan,
        f: Arc<DataFile>,
        key: &[u8],
    ) -> Result<Vec<RecordBatch>> {
        use crate::index::key::KeyRows;
        match self.table.key_rows(&self.snap.def, &f, key).await {
            Some(KeyRows::Absent) => Ok(Vec::new()),
            Some(KeyRows::Rows(r)) if r.iter().map(|x| x.end - x.start).sum::<u64>() <= 4096 => {
                let offsets: Vec<u64> = r.into_iter().flatten().collect();
                let path = self.table.layer.data_prefix().clone().join(f.path.as_str());
                let b = crate::read::fetch::fetch_rows(
                    &self.table.db.cache,
                    self.table.db.store.object_store(),
                    &path,
                    f.size,
                    self.snap.schema(),
                    &offsets,
                )
                .await?;
                Ok(vec![plan.filter_batch(&b)?])
            }
            _ => plan.read(&self.table, f, false).await,
        }
    }
}

/// Rows of one data file by offset ([`TableSnapshot::take`]).
#[derive(Clone, Debug)]
pub struct Take {
    table: Arc<TableInner>,
    snap: Arc<Snapshot>,
    path: String,
    offsets: Vec<u64>,
    columns: Option<Vec<String>>,
}

impl Take {
    /// The columns of the result, in this order (all by default).
    pub fn select<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Reads the rows, as one batch with one row per offset.
    pub async fn collect(self) -> Result<RecordBatch> {
        let schema = self.snap.schema();
        let names: Vec<String> = match self.columns {
            Some(c) => c,
            None => schema.fields().iter().map(|f| f.name().clone()).collect(),
        };
        let needed = project_schema(schema, &names)?;
        let f = self
            .snap
            .files
            .iter()
            .find(|f| f.path == self.path)
            .ok_or_else(|| Error::FilesChanged(self.path.clone()))?;
        if let (Some(rows), Some(o)) = (f.rows(), self.offsets.iter().max())
            && *o >= rows
        {
            return Err(Error::InvalidQuery(format!(
                "row {o} of {}: the file has {rows} rows",
                self.path
            )));
        }
        if self.offsets.is_empty() {
            return Ok(RecordBatch::new_empty(needed));
        }
        let path = self.table.layer.data_prefix().clone().join(f.path.as_str());
        crate::read::fetch::fetch_rows(
            &self.table.db.cache,
            self.table.db.store.object_store(),
            &path,
            f.size,
            &needed,
            &self.offsets,
        )
        .await
        .map_err(Error::expired)
    }
}

fn last_row(b: &RecordBatch) -> Option<RecordBatch> {
    (b.num_rows() > 0).then(|| b.slice(b.num_rows() - 1, 1))
}

/// A scan builder: `table.scan().filter(expr).select(["a", "b"]).limit(10).collect()`.
#[derive(Clone, Debug)]
pub struct Scan {
    table: Arc<TableInner>,
    snap: Option<Arc<Snapshot>>,
    filter: Option<Expr>,
    columns: Option<Vec<String>>,
    limit: Option<usize>,
    files: Option<Vec<String>>,
}

impl Scan {
    pub(crate) fn new(table: Arc<TableInner>, snap: Option<Arc<Snapshot>>) -> Self {
        Scan {
            table,
            snap,
            filter: None,
            columns: None,
            limit: None,
            files: None,
        }
    }

    /// Keeps rows for which `e` (a DataFusion expression) is true; several calls combine with AND.
    pub fn filter(mut self, e: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(f) => f.and(e),
            None => e,
        });
        self
    }

    /// The columns of the result, in this order.
    pub fn select<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Returns at most `n` rows.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Reads only these data files ([`crate::SnapshotFile::path`]), in this order; the other
    /// options apply as usual. A path the scanned version does not hold is
    /// [`Error::FilesChanged`]. A file's rows come in the file's own order, so
    /// [`TableSnapshot::take`] reads the same rows by offset.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// let snap = t.snapshot().await?;
    /// let tagged: Vec<String> = snap
    ///     .files()
    ///     .into_iter()
    ///     .filter(|f| f.tags.contains_key("stats"))
    ///     .map(|f| f.path)
    ///     .collect();
    /// let rows = snap.scan().files(tagged).collect().await?;
    /// # let _ = rows; Ok(()) }
    /// ```
    pub fn files<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.files = Some(paths.into_iter().map(Into::into).collect());
        self
    }

    /// Runs the scan. Rows come in commit order of their files; within a file in the file's
    /// sort order.
    ///
    /// A data file that vacuum removed under an old snapshot makes a scan of the table move to
    /// the newest version and run once more; a second miss, or a miss in a scan
    /// of an explicit [`TableSnapshot`], is [`Error::SnapshotExpired`].
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let snap = match &self.snap {
            Some(s) => s.clone(),
            None => self.table.snapshot_for_query().await?,
        };
        match self.clone().on(snap).try_collect().await {
            Err(e) if e.is_missing_object() && self.snap.is_none() => {
                let fresh = self.table.refresh().await?;
                self.on(fresh).try_collect().await.map_err(Error::expired)
            }
            r => r.map_err(Error::expired),
        }
    }

    /// Runs the scan as a stream of batches, in the order [`Scan::collect`] returns them,
    /// reading a few files ahead (the database's I/O concurrency) rather than the whole
    /// result into memory.
    ///
    /// A data file found missing before the first batch makes a scan of the table move to the
    /// newest version once, as [`Scan::collect`] does; after a batch has been returned it ends
    /// the stream with [`Error::SnapshotExpired`].
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// use futures::TryStreamExt;
    /// let mut rows = 0;
    /// let mut s = t.scan().select(["id"]).stream();
    /// while let Some(b) = s.try_next().await? {
    ///     rows += b.num_rows();
    /// }
    /// # let _ = rows; Ok(()) }
    /// ```
    pub fn stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        let first = {
            let scan = self.clone();
            async move {
                let snap = match &scan.snap {
                    Some(s) => s.clone(),
                    None => scan.table.snapshot_for_query().await?,
                };
                Ok::<_, Error>(scan.on(snap))
            }
        };
        let current = futures::stream::once(first).try_flatten().boxed();
        let restart = self.snap.is_none();
        Restarting {
            scan: self,
            current,
            restart,
            yielded: false,
        }
        .boxed()
    }

    /// The batches of this scan of `snap`, with the limit applied.
    fn on(self, snap: Arc<Snapshot>) -> BoxStream<'static, Result<RecordBatch>> {
        let limit = self.limit.unwrap_or(usize::MAX);
        let setup = async move {
            let output = match self.columns.clone() {
                Some(c) => c,
                None => snap
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
            };
            let plan = Arc::new(Plan::new(&self.table, &snap, self.filter.clone(), output)?);
            if limit == 0 {
                return Ok(futures::stream::empty().boxed());
            }
            let files = match &self.files {
                Some(paths) => Some(plan.chosen(&snap, paths)?),
                None => None,
            };
            if files.is_none()
                && let Some(m) = self.table.small_table(&snap).await?
            {
                let b = plan.filter_batch(&m.batch)?;
                let b = project(&b, &plan.output)?;
                let one = (b.num_rows() > 0).then_some(Ok(b));
                return Ok(futures::stream::iter(one).boxed());
            }
            let files = files.unwrap_or_else(|| plan.candidates(&snap));
            let n = self.table.db.opts.io_concurrency.max(1);
            let table = self.table.clone();
            Ok::<_, Error>(
                futures::stream::iter(files)
                    .map(move |f| plan.read(&table, f, true))
                    .buffered(n)
                    .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                    .try_flatten()
                    .boxed(),
            )
        };
        let mut left = limit;
        futures::stream::once(setup)
            .try_flatten()
            .try_filter_map(move |b| {
                let out = if left == 0 || b.num_rows() == 0 {
                    None
                } else if b.num_rows() > left {
                    let s = b.slice(0, left);
                    left = 0;
                    Some(s)
                } else {
                    left -= b.num_rows();
                    Some(b)
                };
                futures::future::ready(Ok(out))
            })
            .boxed()
    }

    /// Rows the scan returns.
    pub async fn count(self) -> Result<usize> {
        Ok(self.collect().await?.iter().map(|b| b.num_rows()).sum())
    }
}

/// [`Scan::stream`]: moves to the newest version once when a data file is missing before the
/// first batch.
struct Restarting {
    scan: Scan,
    current: BoxStream<'static, Result<RecordBatch>>,
    restart: bool,
    yielded: bool,
}

impl futures::Stream for Restarting {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        loop {
            match self.current.poll_next_unpin(cx) {
                Poll::Ready(Some(Err(e)))
                    if e.is_missing_object() && self.restart && !self.yielded =>
                {
                    self.restart = false;
                    let scan = self.scan.clone();
                    let fresh = async move {
                        let snap = scan.table.refresh().await?;
                        Ok::<_, Error>(scan.on(snap))
                    };
                    self.current = futures::stream::once(fresh).try_flatten().boxed();
                }
                Poll::Ready(Some(Err(e))) => {
                    // Nothing follows an error.
                    self.current = futures::stream::empty().boxed();
                    return Poll::Ready(Some(Err(e.expired())));
                }
                Poll::Ready(Some(Ok(b))) => {
                    self.yielded = true;
                    return Poll::Ready(Some(Ok(b)));
                }
                other => return other,
            }
        }
    }
}

/// A scan resolved against one snapshot.
struct Plan {
    schema: SchemaRef,
    output: Vec<String>,
    needed: SchemaRef,
    filter: Option<Expr>,
    physical: Option<Arc<dyn PhysicalExpr>>,
    /// The same filter over the full table schema, for materialised tables.
    physical_full: Option<Arc<dyn PhysicalExpr>>,
    query: Arc<FileQuery>,
}

impl Plan {
    fn new(
        table: &TableInner,
        snap: &Snapshot,
        filter: Option<Expr>,
        output: Vec<String>,
    ) -> Result<Plan> {
        let schema = snap.schema().clone();
        let mut needed_names = output.clone();
        if let Some(f) = &filter {
            for c in f.column_refs() {
                if schema.field_with_name(&c.name).is_err() {
                    return Err(column_not_found(&c.name, &schema));
                }
                if !needed_names.contains(&c.name) {
                    needed_names.push(c.name.clone());
                }
            }
        }
        let needed = project_schema(&schema, &needed_names)?;
        let physical = match &filter {
            Some(f) => Some(table.db.physical_expr(f.clone(), &needed)?),
            None => None,
        };
        let physical_full = match &filter {
            Some(f) => Some(table.db.physical_expr(f.clone(), &schema)?),
            None => None,
        };
        let query = Arc::new(FileQuery {
            table_schema: schema.clone(),
            output: output.clone(),
            needed: needed.clone(),
            filter: filter.clone(),
            physical: physical.clone(),
        });
        Ok(Plan {
            schema,
            output,
            needed,
            filter,
            physical,
            physical_full,
            query,
        })
    }

    /// Files whose statistics do not exclude the filter, in commit order.
    fn candidates(&self, snap: &Snapshot) -> Vec<Arc<DataFile>> {
        snap.files
            .iter()
            .filter(|f| match &self.filter {
                Some(e) => may_match(e, &self.schema, &FileBounds(f)),
                None => true,
            })
            .cloned()
            .collect()
    }

    /// The files `paths` names, in that order, less those whose statistics exclude the filter.
    /// [`Error::FilesChanged`] for a path `snap` does not hold.
    fn chosen(&self, snap: &Snapshot, paths: &[String]) -> Result<Vec<Arc<DataFile>>> {
        let live: std::collections::HashMap<&str, &Arc<DataFile>> =
            snap.files.iter().map(|f| (f.path.as_str(), f)).collect();
        let mut out = Vec::with_capacity(paths.len());
        for p in paths {
            let f = live
                .get(p.as_str())
                .ok_or_else(|| Error::FilesChanged(p.clone()))?;
            if self
                .filter
                .as_ref()
                .is_none_or(|e| may_match(e, &self.schema, &FileBounds(f)))
            {
                out.push((*f).clone());
            }
        }
        Ok(out)
    }

    /// Reads one candidate file; `spawn` runs it on its own task (scans that decode a lot).
    fn read(
        &self,
        table: &Arc<TableInner>,
        f: Arc<DataFile>,
        spawn: bool,
    ) -> futures::future::BoxFuture<'static, Result<Vec<RecordBatch>>> {
        let parallel = spawn && table.db.opts.io_concurrency > 1;
        let table = table.clone();
        let q = self.query.clone();
        let fut = async move {
            let path = table.layer.data_prefix().clone().join(f.path.as_str());
            read_file(
                &table.db.cache,
                table.db.store.object_store(),
                &path,
                f.size,
                &q,
            )
            .await
        };
        if parallel {
            // Decoding is CPU work: run each file on its own task when the runtime has threads.
            let h = tokio::spawn(fut);
            Box::pin(async move {
                h.await
                    .map_err(|e| Error::Internal(format!("read task: {e}")))?
            })
        } else {
            Box::pin(fut)
        }
    }

    /// Rows of a full-schema batch matching the filter, with every column.
    fn filter_batch(&self, b: &RecordBatch) -> Result<RecordBatch> {
        let _ = (&self.physical, &self.needed);
        match &self.physical_full {
            Some(p) => {
                let mask = p.evaluate(b)?.into_array(b.num_rows())?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::InvalidQuery("the filter is not boolean".into()))?;
                Ok(filter_record_batch(b, mask)?)
            }
            None => Ok(b.clone()),
        }
    }
}

struct FileBounds<'a>(&'a DataFile);

impl Bounds for FileBounds<'_> {
    fn bounds(&self, column: &str) -> Option<ColRange> {
        self.0.stats.column(column).cloned()
    }
}

impl TableInner {
    /// The materialised table for `snap` when its live data is below the small-table threshold
    /// (D20); extended with new files when the version moved by appends only.
    pub(crate) async fn small_table(
        self: &Arc<Self>,
        snap: &Arc<Snapshot>,
    ) -> Result<Option<Arc<Materialised>>> {
        let limit = self.db.opts.small_table_bytes;
        let rows: Option<u64> = snap.files.iter().map(|f| f.rows()).sum();
        let too_many = rows.is_none_or(|r| r > self.db.opts.small_table_rows);
        if snap.files.is_empty() || snap.total_bytes > limit || too_many {
            return Ok(None);
        }
        let cached = self.small.lock().ok().and_then(|g| g.clone());
        if let Some(m) = &cached
            && m.version == snap.version
            && Arc::ptr_eq(&m.schema, snap.schema())
        {
            return Ok(Some(m.clone()));
        }
        let paths: Vec<&str> = snap.files.iter().map(|f| f.path.as_str()).collect();
        // Reuse the cached rows when they are a prefix of this version's files.
        let (mut parts, from) = match &cached {
            Some(m)
                if m.schema == *snap.schema()
                    && m.version < snap.version
                    && m.files.len() <= paths.len()
                    && m.files.iter().zip(&paths).all(|(a, b)| a == b) =>
            {
                (vec![m.batch.clone()], m.files.len())
            }
            _ => (Vec::new(), 0),
        };
        let all: Vec<String> = snap
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let q = Arc::new(FileQuery {
            table_schema: snap.schema().clone(),
            output: all,
            needed: snap.schema().clone(),
            filter: None,
            physical: None,
        });
        let n = self.db.opts.io_concurrency.max(1);
        let new_files: Vec<Arc<DataFile>> = snap.files[from..].to_vec();
        let me = self.clone();
        let read: Vec<Vec<RecordBatch>> = futures::stream::iter(new_files)
            .map(|f| {
                let me = me.clone();
                let q = q.clone();
                async move {
                    let path = me.layer.data_prefix().clone().join(f.path.as_str());
                    read_file(&me.db.cache, me.db.store.object_store(), &path, f.size, &q).await
                }
            })
            .buffered(n)
            .try_collect()
            .await?;
        for b in read.into_iter().flatten() {
            parts.push(to_schema(&b, snap.schema())?);
        }
        let batch = concat_batches(snap.schema(), &parts)?;
        let m = Arc::new(Materialised {
            version: snap.version,
            schema: snap.schema().clone(),
            files: paths.iter().map(|p| p.to_string()).collect(),
            batch,
        });
        if let Ok(mut g) = self.small.lock()
            && g.as_ref().is_none_or(|c| c.version <= m.version)
        {
            *g = Some(m.clone());
        }
        Ok(Some(m))
    }
}
