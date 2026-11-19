//! Index queries: one pinned snapshot, files
//! pruned by statistics and by the filter, a fan-out where each file answers from its sidecar
//! or by the kind's exact route, a global top-k, a rescore of approximate candidates, and a
//! fetch of the final rows by offset.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, BooleanArray, Float32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use serde_json::Value;

use super::bundle::{BundleView, Expect};
use super::discover::{SidecarMap, data_segment};
use super::kind::{FileIndex, IndexKind, Query};
use super::spec::{IndexSpec, parse_specs, spec_hash};
use super::topk::{Candidate, TopK};
use super::{ExactScanPolicy, TableIndexState};
use crate::error::{Error, Result};
use crate::log::DataFile;
use crate::read::fetch::{fetch_rows, scan_offsets};
use crate::read::prune::{Bounds, may_match};
use crate::schema::{column_not_found, project_schema};
use crate::snapshot::Snapshot;
use crate::stats::ColRange;
use crate::table::TableInner;

/// The score column added to search results.
pub const SCORE_COLUMN: &str = "_moraine_score";

/// One hit of a search.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SearchHit {
    /// The data file's path, relative to the table's data root.
    pub file: String,
    /// The row's position in the data file, counted from 0.
    pub offset: u64,
    /// The kind's score: a distance for vector kinds (smaller is closer).
    pub score: f32,
}

/// How a search was answered.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SearchStats {
    /// Files in the snapshot.
    pub files: usize,
    /// Files skipped by statistics or by an empty filter result (case V9).
    pub skipped_files: usize,
    /// Files answered through their sidecar.
    pub sidecar_files: usize,
    /// Files answered on the exact route (no usable sidecar, or below the kind's threshold).
    pub exact_files: usize,
    /// Rows scored on the exact route.
    pub exact_rows: u64,
    /// Approximate candidates rescored from their rows.
    pub rescored: usize,
}

/// What a search returns: the rows (selected columns plus [`SCORE_COLUMN`]) best first, the
/// hits behind them, and how the files were searched.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SearchResult {
    /// The hits' rows, best first: the selected columns and [`SCORE_COLUMN`].
    pub batch: RecordBatch,
    /// One hit per row of `batch`, in the same order.
    pub hits: Vec<SearchHit>,
    /// How the files were searched.
    pub stats: SearchStats,
}

#[derive(Clone, Debug)]
pub(crate) enum Owned {
    Vector(Vec<f32>),
    Custom(Vec<u8>),
    /// A full-text query in tantivy's syntax.
    Text(String),
}

/// A search builder: `table.search_vector("emb", v).limit(10).filter(e).select(["id"])`.
#[derive(Clone, Debug)]
pub struct Search {
    pub(crate) table: Arc<TableInner>,
    pub(crate) index: String,
    pub(crate) query: Owned,
    pub(crate) k: usize,
    pub(crate) filter: Option<Expr>,
    pub(crate) select: Option<Vec<String>>,
    pub(crate) params: serde_json::Map<String, Value>,
    /// Hits only: no rows are fetched beyond what rescoring needs (hybrid search).
    pub(crate) hits_only: bool,
}

impl Search {
    pub(crate) fn vector(table: Arc<TableInner>, index: &str, v: Vec<f32>) -> Search {
        Self::new(table, index, Owned::Vector(v))
    }

    pub(crate) fn custom(table: Arc<TableInner>, index: &str, bytes: Vec<u8>) -> Search {
        Self::new(table, index, Owned::Custom(bytes))
    }

    pub(crate) fn text(table: Arc<TableInner>, index: &str, query: String) -> Search {
        Self::new(table, index, Owned::Text(query))
    }

    fn new(table: Arc<TableInner>, index: &str, query: Owned) -> Search {
        Search {
            table,
            index: index.into(),
            query,
            k: 10,
            filter: None,
            select: None,
            params: serde_json::Map::new(),
            hits_only: false,
        }
    }

    /// Hits wanted, at most (default 10).
    pub fn limit(mut self, n: usize) -> Self {
        self.k = n;
        self
    }

    /// Only rows for which `e` is true; several calls combine with AND.
    pub fn filter(mut self, e: Expr) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(f) => f.and(e),
            None => e,
        });
        self
    }

    /// The columns of the result (default: all), before the score column.
    pub fn select<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.select = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Overrides one of the index's search defaults (`expansion_search`, `rerank`, `boosts`,
    /// `conjunction`, ...).
    pub fn param(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Runs the search against a snapshot taken now.
    ///
    /// A data file that vacuum removed under an old snapshot makes the search move to the
    /// newest version and run once more; a second miss is [`Error::SnapshotExpired`].
    pub async fn collect(self) -> Result<SearchResult> {
        let snap = self.table.snapshot_for_query().await?;
        match run(self.clone(), snap).await {
            Err(e) if e.is_missing_object() => {
                let fresh = self.table.refresh().await?;
                run(self, fresh).await.map_err(Error::expired)
            }
            r => r,
        }
    }
}

/// A compiled filter: the expression, the schema of its columns and its physical form.
pub(crate) type Filter = (Expr, SchemaRef, Arc<dyn PhysicalExpr>);

/// Compiles `f` against `schema`, checking its columns.
pub(crate) fn compile_filter(
    table: &TableInner,
    schema: &SchemaRef,
    f: &Option<Expr>,
) -> Result<Option<Filter>> {
    let Some(f) = f else { return Ok(None) };
    let mut cols: Vec<String> = Vec::new();
    for c in f.column_refs() {
        if schema.field_with_name(&c.name).is_err() {
            return Err(column_not_found(&c.name, schema));
        }
        if !cols.contains(&c.name) {
            cols.push(c.name.clone());
        }
    }
    let fs = project_schema(schema, &cols)?;
    let phys = table.db.physical_expr(f.clone(), &fs)?;
    Ok(Some((f.clone(), fs, phys)))
}

/// The files of `snap` whose statistics do not exclude `filter` (case V9).
pub(crate) fn candidate_files(snap: &Snapshot, filter: &Option<Expr>) -> Vec<Arc<DataFile>> {
    let schema = snap.schema();
    snap.files
        .iter()
        .filter(|f| match filter {
            Some(e) => may_match(e, schema, &FileBounds(f)),
            None => true,
        })
        .cloned()
        .collect()
}

/// The output columns of a search and their schema, checked against `schema` along with the
/// index columns.
pub(crate) fn output_columns(
    schema: &SchemaRef,
    select: &Option<Vec<String>>,
    spec: &IndexSpec,
) -> Result<(Vec<String>, SchemaRef)> {
    let output: Vec<String> = match select {
        Some(c) => c.clone(),
        None => schema.fields().iter().map(|f| f.name().clone()).collect(),
    };
    for c in output.iter().chain(&spec.columns) {
        if schema.field_with_name(c).is_err() {
            return Err(column_not_found(c, schema));
        }
    }
    let out_schema = project_schema(schema, &output)?;
    Ok((output, out_schema))
}

/// The spec of index `name` at `snap`.
pub(crate) fn find_spec(snap: &Snapshot, name: &str) -> Result<IndexSpec> {
    let specs = parse_specs(&snap.meta.configuration)?;
    let names: Vec<String> = specs.iter().map(|x| x.name.clone()).collect();
    specs
        .into_iter()
        .find(|x| x.name == name)
        .ok_or_else(|| Error::IndexNotFound(crate::names::NotFound::among(name, names)))
}

/// The rows of `ranked` (best first), with the score column, and their hits.
#[cfg_attr(not(feature = "fts"), allow(dead_code))]
pub(crate) async fn rows_of(
    table: &Arc<TableInner>,
    snap: &Snapshot,
    output: &[String],
    out_schema: &SchemaRef,
    ranked: Vec<Candidate>,
) -> Result<(RecordBatch, Vec<SearchHit>)> {
    let schema = snap.schema();
    let fetch_schema = project_schema(schema, output)?;
    let fetched = fetch_all(table, snap, &fetch_schema, &ranked).await?;
    let idx: Vec<(usize, usize)> = ranked
        .iter()
        .map(|c| fetched.position(c))
        .collect::<Result<_>>()?;
    assemble(&fetched, output, out_schema, ranked, &idx)
}

fn assemble(
    fetched: &Fetched,
    output: &[String],
    out_schema: &SchemaRef,
    ranked: Vec<Candidate>,
    idx: &[(usize, usize)],
) -> Result<(RecordBatch, Vec<SearchHit>)> {
    let parts: Vec<RecordBatch> = fetched
        .batches
        .iter()
        .map(|(_, b)| crate::read::project(b, output))
        .collect::<Result<_>>()?;
    let refs: Vec<&RecordBatch> = parts.iter().collect();
    let batch = if idx.is_empty() {
        RecordBatch::new_empty(out_schema.clone())
    } else {
        arrow::compute::interleave_record_batch(&refs, idx)?
    };
    let batch = with_scores(batch, ranked.iter().map(|c| c.score).collect())?;
    let hits = ranked
        .into_iter()
        .map(|c| SearchHit {
            file: c.file.to_string(),
            offset: c.offset,
            score: c.score,
        })
        .collect();
    Ok((batch, hits))
}

/// Everything a per-file task needs.
struct Ctx {
    table: Arc<TableInner>,
    spec: IndexSpec,
    kind: Arc<dyn IndexKind>,
    hash: String,
    query: Owned,
    params: Value,
    want: usize,
    k: usize,
    filter: Option<Filter>,
    map: Option<Arc<SidecarMap>>,
    state: Arc<TableIndexState>,
    spec_schema: SchemaRef,
    exact_rows: AtomicU64,
    /// Sidecar searches are left to the shared pool rather than run per file (D83).
    pooled: bool,
}

impl Ctx {
    fn query(&self) -> Query<'_> {
        match &self.query {
            Owned::Vector(v) => Query::Vector {
                vector: v,
                params: &self.params,
            },
            Owned::Custom(b) => Query::Custom {
                bytes: b,
                params: &self.params,
            },
            Owned::Text(t) => Query::Custom {
                bytes: t.as_bytes(),
                params: &self.params,
            },
        }
    }
}

/// What one file contributed.
enum FileResult {
    Skipped,
    Sidecar(Vec<Candidate>),
    Exact(Vec<Candidate>, u64),
    /// An opened sidecar whose pieces are searched with every other file's in the shared pool
    /// (D83).
    Ready(
        Arc<DataFile>,
        Arc<dyn FileIndex>,
        Option<Arc<RoaringBitmap>>,
    ),
}

struct FileBounds<'a>(&'a DataFile);

impl Bounds for FileBounds<'_> {
    fn bounds(&self, column: &str) -> Option<ColRange> {
        self.0.stats.column(column).cloned()
    }
}

pub(crate) fn merge_params(defaults: &Value, overrides: serde_json::Map<String, Value>) -> Value {
    let mut m = match defaults {
        Value::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    m.extend(overrides);
    Value::Object(m)
}

/// The search parameter moraine sets to the number of files a search fans out to.
pub const FANOUT_PARAM: &str = "moraine.files";
/// Search parameter: `1` hands every file's pieces to the shared pool, `0` searches each file's
/// sidecar on its own task (D83). By default the pool is used when the search covers more
/// files than it runs tasks at once.
pub const POOL_PARAM: &str = "moraine.pool";

pub(crate) async fn run(s: Search, snap: Arc<Snapshot>) -> Result<SearchResult> {
    if matches!(s.query, Owned::Text(_)) {
        #[cfg(feature = "fts")]
        return super::fts::search::run(s, snap).await;
        #[cfg(not(feature = "fts"))]
        return Err(Error::IndexKindNotRegistered(crate::names::NotFound::bare(
            "fts (moraine was built without its `fts` feature)",
        )));
    }
    let table = s.table.clone();
    let rt = &table.db.index;
    let spec = find_spec(&snap, &s.index)?;
    let kind = rt.kind(&spec.kind)?.clone();
    let schema = snap.schema().clone();
    let (output, out_schema) = output_columns(&schema, &s.select, &spec)?;
    let output = if s.hits_only { Vec::new() } else { output };
    let params = merge_params(&spec.search, s.params);
    let mut ctx = Ctx {
        table: table.clone(),
        hash: spec_hash(kind.as_ref(), &spec),
        spec_schema: project_schema(&schema, &spec.columns)?,
        kind,
        query: s.query,
        params,
        want: 0,
        k: s.k,
        filter: None,
        map: None,
        state: rt.table(table.idx_prefix().as_ref()),
        spec,
        exact_rows: AtomicU64::new(0),
        pooled: false,
    };
    ctx.kind.validate(&ctx.spec, &ctx.query())?;
    let rerank = ctx.kind.rerank(&ctx.spec, &ctx.query());
    ctx.want = s.k.saturating_mul(1 + rerank);
    let mut stats = SearchStats {
        files: snap.files.len(),
        ..Default::default()
    };
    if s.k == 0 {
        return Ok(SearchResult {
            batch: with_scores(RecordBatch::new_empty(out_schema), Vec::new())?,
            hits: Vec::new(),
            stats,
        });
    }
    ctx.filter = compile_filter(&table, &schema, &s.filter)?;
    // Files whose statistics exclude the filter are skipped before any sidecar is opened (V9).
    let files = candidate_files(&snap, &s.filter);
    stats.skipped_files = snap.files.len() - files.len();
    // How many files the search fans out to, so a kind can tell whether the files already keep
    // the cores busy (D52).
    if let Value::Object(m) = &mut ctx.params {
        m.insert(FANOUT_PARAM.into(), Value::from(files.len()));
    }
    if !matches!(rt.cache.mode(), super::cache::CacheMode::Disabled) {
        let clock = table.db.clock();
        ctx.map = ctx
            .state
            .discovery
            .map(
                table.db.store.object_store(),
                &table.idx_prefix(),
                snap.version,
                clock.now(),
                rt.cfg.poll,
            )
            .await
            .ok();
    }
    // Exact-route budget (V13): what the files without a usable sidecar would cost at most is
    // only known per file, so it is checked as files are scanned.
    let n = table.db.opts.io_concurrency.max(1);
    let parallel = n > 1;
    // The pool pays only once the files outnumber the tasks in flight, which otherwise run in
    // waves; with fewer files the per-file tasks and D52's part threads answer sooner (D83).
    ctx.pooled = parallel
        && cfg!(feature = "vector")
        && match ctx.params.get(POOL_PARAM).and_then(Value::as_u64) {
            Some(p) => p != 0,
            None => files.len() > n,
        };
    let ctx = Arc::new(ctx);
    let results: Vec<FileResult> = futures::stream::iter(files)
        .map(|f| {
            let ctx = ctx.clone();
            let fut = async move { search_file(&ctx, f).await };
            async move {
                if parallel {
                    tokio::spawn(fut)
                        .await
                        .map_err(|e| Error::Internal(format!("search task: {e}")))?
                } else {
                    fut.await
                }
            }
        })
        .buffer_unordered(n)
        .try_collect()
        .await?;
    let results = search_pooled(&ctx, results).await?;
    let order = ctx.kind.order();
    let mut top = TopK::new(order, ctx.want);
    for r in results {
        match r {
            FileResult::Ready(..) => {
                return Err(Error::Internal("a file's search was not run".into()));
            }
            FileResult::Skipped => stats.skipped_files += 1,
            FileResult::Sidecar(c) => {
                stats.sidecar_files += 1;
                c.into_iter().for_each(|c| top.push(c));
            }
            FileResult::Exact(c, rows) => {
                stats.exact_files += 1;
                stats.exact_rows += rows;
                c.into_iter().for_each(|c| top.push(c));
            }
        }
    }
    let metrics = &rt.metrics;
    metrics
        .sidecar_searches
        .fetch_add(stats.sidecar_files as u64, Ordering::Relaxed);
    metrics
        .exact_searches
        .fetch_add(stats.exact_files as u64, Ordering::Relaxed);
    metrics
        .exact_rows
        .fetch_add(stats.exact_rows, Ordering::Relaxed);
    let candidates = top.into_sorted();

    // Rescore approximate candidates from their rows, reading the output columns in the same
    // fetch, then cut to k: rerank and fetch together.
    let needs_rescore = candidates.iter().any(|c| !c.exact);
    let mut fetch_cols = output.clone();
    if needs_rescore {
        for c in &ctx.spec.columns {
            if !fetch_cols.contains(c) {
                fetch_cols.push(c.clone());
            }
        }
    }
    let fetch_schema = project_schema(&schema, &fetch_cols)?;
    let rows_of: Vec<Candidate> = if needs_rescore {
        candidates
    } else {
        candidates.into_iter().take(ctx.k).collect()
    };
    let fetched = if s.hits_only && !needs_rescore {
        Fetched::default()
    } else {
        fetch_all(&ctx.table, &snap, &fetch_schema, &rows_of).await?
    };
    let mut ranked: Vec<(Candidate, usize, usize)> = Vec::with_capacity(rows_of.len());
    {
        let mut scored: HashMap<usize, Vec<Option<f32>>> = HashMap::new();
        if needs_rescore {
            let scorer = ctx.kind.exact(&ctx.spec, &ctx.query())?;
            for (i, (_, b)) in fetched.batches.iter().enumerate() {
                let spec_batch = crate::read::project(b, &ctx.spec.columns)?;
                scored.insert(i, scorer.score(&spec_batch)?);
            }
        }
        for c in rows_of {
            let (bi, row) = if s.hits_only && !needs_rescore {
                (0, 0)
            } else {
                fetched.position(&c)?
            };
            let mut c = c;
            if !c.exact {
                stats.rescored += 1;
                match scored.get(&bi).and_then(|s| s.get(row).copied().flatten()) {
                    Some(score) => {
                        c.score = score;
                        c.exact = true;
                    }
                    // A row the exact route would never return (V4, zero vectors) is dropped.
                    None => continue,
                }
            }
            ranked.push((c, bi, row));
        }
    }
    ranked.sort_by(|a, b| super::topk::cmp(order, &a.0, &b.0));
    ranked.truncate(ctx.k);
    if s.hits_only {
        let hits = ranked
            .into_iter()
            .map(|(c, ..)| SearchHit {
                file: c.file.to_string(),
                offset: c.offset,
                score: c.score,
            })
            .collect();
        let batch = with_scores(
            RecordBatch::new_empty(project_schema(&schema, &[])?),
            Vec::new(),
        )?;
        return Ok(SearchResult { batch, hits, stats });
    }
    let idx: Vec<(usize, usize)> = ranked.iter().map(|(_, b, r)| (*b, *r)).collect();
    let ranked: Vec<Candidate> = ranked.into_iter().map(|(c, ..)| c).collect();
    let (batch, hits) = assemble(&fetched, &output, &out_schema, ranked, &idx)?;
    Ok(SearchResult { batch, hits, stats })
}

pub(crate) fn with_scores(batch: RecordBatch, scores: Vec<f32>) -> Result<RecordBatch> {
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(SCORE_COLUMN, DataType::Float32, false)));
    let mut cols = batch.columns().to_vec();
    cols.push(Arc::new(Float32Array::from(scores)));
    let opts = arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        cols,
        &opts,
    )?)
}

/// Fetched rows, one batch per file, and where each candidate sits in them.
#[derive(Default)]
struct Fetched {
    batches: Vec<(Arc<str>, RecordBatch)>,
    at: HashMap<(Arc<str>, u64), (usize, usize)>,
}

impl Fetched {
    fn position(&self, c: &Candidate) -> Result<(usize, usize)> {
        self.at
            .get(&(c.file.clone(), c.offset))
            .copied()
            .ok_or_else(|| Error::Internal(format!("row {} of {} not fetched", c.offset, c.file)))
    }
}

async fn fetch_all(
    table: &Arc<TableInner>,
    snap: &Snapshot,
    needed: &SchemaRef,
    cands: &[Candidate],
) -> Result<Fetched> {
    let mut by_file: HashMap<Arc<str>, Vec<u64>> = HashMap::new();
    for c in cands {
        by_file.entry(c.file.clone()).or_default().push(c.offset);
    }
    let sizes: HashMap<&str, u64> = snap
        .files
        .iter()
        .map(|f| (f.path.as_str(), f.size))
        .collect();
    let n = table.db.opts.io_concurrency.max(1);
    let jobs: Vec<(Arc<str>, Vec<u64>, u64)> = by_file
        .into_iter()
        .map(|(f, mut o)| {
            o.sort_unstable();
            o.dedup();
            let size = sizes.get(&*f).copied().unwrap_or(0);
            (f, o, size)
        })
        .collect();
    let got: Vec<(Arc<str>, Vec<u64>, RecordBatch)> = futures::stream::iter(jobs)
        .map(|(f, offsets, size)| {
            let table = table.clone();
            let needed = needed.clone();
            async move {
                let path = table.layer.data_prefix().clone().join(&*f);
                let b = fetch_rows(
                    &table.db.cache,
                    table.db.store.object_store(),
                    &path,
                    size,
                    &needed,
                    &offsets,
                )
                .await?;
                Ok::<_, Error>((f, offsets, b))
            }
        })
        .buffer_unordered(n)
        .try_collect()
        .await?;
    let mut out = Fetched {
        batches: Vec::with_capacity(got.len()),
        at: HashMap::new(),
    };
    for (i, (f, offsets, b)) in got.into_iter().enumerate() {
        for (row, o) in offsets.iter().enumerate() {
            out.at.insert((f.clone(), *o), (i, row));
        }
        out.batches.push((f, b));
    }
    Ok(out)
}

/// The rows of `f` that the filter admits, cached per (file, filter).
pub(crate) async fn allow_bitmap(
    table: &TableInner,
    filter: &Option<Filter>,
    f: &DataFile,
) -> Result<Option<Arc<RoaringBitmap>>> {
    let Some((expr, schema, phys)) = filter else {
        return Ok(None);
    };
    let rt = &table.db.index;
    let key = (f.path.clone(), f.size, format!("{expr}"));
    if let Some(b) = rt.bitmap(&key) {
        return Ok(Some(b));
    }
    let path = table.layer.data_prefix().clone().join(f.path.as_str());
    let mut bm = RoaringBitmap::new();
    scan_offsets(
        &table.db.cache,
        table.db.store.object_store(),
        &path,
        f.size,
        schema,
        None,
        &mut |offsets, batch| {
            let mask = phys.evaluate(batch)?.into_array(batch.num_rows())?;
            let mask = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::InvalidQuery("the filter is not boolean".into()))?;
            for (i, o) in offsets.iter().enumerate() {
                if mask.is_valid(i) && mask.value(i) {
                    bm.insert(*o as u32);
                }
            }
            Ok(())
        },
    )
    .await?;
    let b = Arc::new(bm);
    rt.put_bitmap(key, b.clone());
    Ok(Some(b))
}

/// Runs the pieces of every [`FileResult::Ready`] file on the shared work-stealing pool, all
/// files' pieces at once, so a search over many files or a file of many parts keeps every core
/// busy until the last piece is done, whatever the layout (D83). A file whose sidecar fails to
/// search takes the exact route (V5).
async fn search_pooled(ctx: &Arc<Ctx>, results: Vec<FileResult>) -> Result<Vec<FileResult>> {
    let mut out = Vec::with_capacity(results.len());
    let mut ready = Vec::new();
    for r in results {
        match r {
            FileResult::Ready(f, index, allow) => ready.push((f, index, allow)),
            other => out.push(other),
        }
    }
    if ready.is_empty() {
        return Ok(out);
    }
    let pieces: Vec<(usize, usize)> = ready
        .iter()
        .enumerate()
        .flat_map(|(i, (_, index, _))| (0..index.pieces().max(1)).map(move |p| (i, p)))
        .collect();
    let ready = Arc::new(ready);
    let found = run_pieces(ctx.clone(), ready.clone(), pieces.clone()).await?;
    let mut per_file: Vec<Option<Vec<Candidate>>> = vec![Some(Vec::new()); ready.len()];
    for ((i, _), r) in pieces.into_iter().zip(found) {
        let file: Arc<str> = ready[i].0.path.as_str().into();
        match (r, &mut per_file[i]) {
            (Ok(hits), Some(c)) => c.extend(hits.into_iter().map(|h| Candidate {
                file: file.clone(),
                offset: h.offset,
                score: h.score,
                exact: h.exact,
            })),
            (Err(_), slot) => *slot = None,
            (Ok(_), None) => {}
        }
    }
    for ((f, _, allow), c) in ready.iter().zip(per_file) {
        match c {
            Some(c) => out.push(FileResult::Sidecar(c)),
            None => {
                ctx.table
                    .db
                    .index
                    .metrics
                    .bad_sidecars
                    .fetch_add(1, Ordering::Relaxed);
                let file: Arc<str> = f.path.as_str().into();
                out.push(exact_file(ctx, f, file, allow.as_deref()).await?);
            }
        }
    }
    Ok(out)
}

type ReadyFile = (
    Arc<DataFile>,
    Arc<dyn FileIndex>,
    Option<Arc<RoaringBitmap>>,
);

/// Searches `pieces` (file, piece) of `ready` on rayon's pool, one task per piece so idle
/// threads steal what is left; the caller's task waits without holding a runtime thread.
#[cfg(feature = "vector")]
async fn run_pieces(
    ctx: Arc<Ctx>,
    ready: Arc<Vec<ReadyFile>>,
    pieces: Vec<(usize, usize)>,
) -> Result<Vec<Result<Vec<super::kind::Hit>>>> {
    let one = move |(i, p): (usize, usize)| {
        let (_, index, allow) = &ready[i];
        index.search_piece(p, &ctx.query(), allow.as_deref(), ctx.want)
    };
    if pieces.len() == 1 {
        return Ok(pieces.into_iter().map(one).collect());
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        use rayon::prelude::*;
        let found: Vec<_> = pieces.into_par_iter().with_max_len(1).map(one).collect();
        let _ = tx.send(found);
    });
    rx.await
        .map_err(|_| Error::Internal("a piece search panicked".into()))
}

#[cfg(not(feature = "vector"))]
async fn run_pieces(
    ctx: Arc<Ctx>,
    ready: Arc<Vec<ReadyFile>>,
    pieces: Vec<(usize, usize)>,
) -> Result<Vec<Result<Vec<super::kind::Hit>>>> {
    Ok(pieces
        .into_iter()
        .map(|(i, p)| {
            let (_, index, allow) = &ready[i];
            index.search_piece(p, &ctx.query(), allow.as_deref(), ctx.want)
        })
        .collect())
}

async fn search_file(ctx: &Ctx, f: Arc<DataFile>) -> Result<FileResult> {
    let allow = allow_bitmap(&ctx.table, &ctx.filter, &f).await?;
    if allow.as_ref().is_some_and(|a| a.is_empty()) {
        return Ok(FileResult::Skipped);
    }
    let rt = &ctx.table.db.index;
    let file: Arc<str> = f.path.as_str().into();
    let rows = f.rows();
    let small_filter = allow
        .as_ref()
        .is_some_and(|a| a.len() < rt.cfg.exact_filter_threshold);
    let below_policy = rows.is_some_and(|r| !ctx.kind.wants_sidecar(&ctx.spec, r));
    if !small_filter
        && !below_policy
        && let Some(index) = sidecar(ctx, &f).await
    {
        if ctx.pooled {
            return Ok(FileResult::Ready(f, index, allow));
        }
        match index.search(&ctx.query(), allow.as_deref(), ctx.want) {
            Ok(hits) => {
                return Ok(FileResult::Sidecar(
                    hits.into_iter()
                        .map(|h| Candidate {
                            file: file.clone(),
                            offset: h.offset,
                            score: h.score,
                            exact: h.exact,
                        })
                        .collect(),
                ));
            }
            Err(_) => {
                // A sidecar that fails to search is treated as missing (V5).
                rt.metrics.bad_sidecars.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    exact_file(ctx, &f, file, allow.as_deref()).await
}

async fn exact_file(
    ctx: &Ctx,
    f: &DataFile,
    file: Arc<str>,
    allow: Option<&RoaringBitmap>,
) -> Result<FileResult> {
    let rt = &ctx.table.db.index;
    let cost = match allow {
        Some(a) => a.len(),
        None => f.rows().unwrap_or(0),
    };
    let total = ctx.exact_rows.fetch_add(cost, Ordering::Relaxed) + cost;
    let limit = rt.cfg.max_exact_scan_rows;
    if total > limit {
        match rt.cfg.exact_policy {
            ExactScanPolicy::Error => return Err(Error::ExactScanLimit { rows: total, limit }),
            // Counted once per query, by the file that crossed the limit.
            ExactScanPolicy::Allow if total - cost <= limit => {
                rt.metrics
                    .exact_limit_exceeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExactScanPolicy::Allow => {}
        }
    }
    let scorer = ctx.kind.exact(&ctx.spec, &ctx.query())?;
    let path = ctx.table.layer.data_prefix().clone().join(f.path.as_str());
    let mut top = TopK::new(ctx.kind.order(), ctx.k);
    let count = scan_offsets(
        &ctx.table.db.cache,
        ctx.table.db.store.object_store(),
        &path,
        f.size,
        &ctx.spec_schema,
        allow,
        &mut |offsets, batch| {
            for (o, s) in offsets.iter().zip(scorer.score(batch)?) {
                if let Some(score) = s {
                    top.push(Candidate {
                        file: file.clone(),
                        offset: *o,
                        score,
                        exact: true,
                    });
                }
            }
            Ok(())
        },
    )
    .await?;
    Ok(FileResult::Exact(top.into_sorted(), count.rows))
}

/// The opened sidecar of `f` for the query's index, when one is listed, usable and cached.
async fn sidecar(ctx: &Ctx, f: &DataFile) -> Option<Arc<dyn FileIndex>> {
    let map = ctx.map.as_ref()?;
    let now = ctx.table.db.clock().now();
    let listed = map
        .get(&(data_segment(&f.path), ctx.spec.name.clone()))?
        .iter()
        .find(|l| l.spec_hash == ctx.hash && !ctx.state.discovery.is_bad(&l.location, now))?
        .clone();
    open_listed(
        &ctx.table,
        ctx.kind.as_ref(),
        &ctx.spec,
        &ctx.hash,
        f,
        &listed,
        &ctx.state,
    )
    .await
}

/// Opens `listed`, the sidecar of `f` for `spec`, through the handle and disk caches. A
/// sidecar that fails to download, validate or open is counted and marked bad for 60 s (V5).
pub(crate) async fn open_listed(
    table: &TableInner,
    kind: &dyn IndexKind,
    spec: &IndexSpec,
    hash: &str,
    f: &DataFile,
    listed: &super::discover::Listed,
    state: &TableIndexState,
) -> Option<Arc<dyn FileIndex>> {
    let rt = &table.db.index;
    let key = format!(
        "{}#{}",
        listed.location,
        listed.etag.as_deref().unwrap_or("")
    );
    if let Some(h) = rt.handles.get(&key) {
        return Some(h);
    }
    let kind_name = kind.name();
    let kind_format = kind.format_version();
    let (rows, size) = (f.rows(), f.size);
    let check = move |v: &BundleView| {
        v.check(&Expect {
            kind: kind_name,
            kind_format,
            spec_hash: hash,
            source_rows: rows,
            source_size: size,
        })
    };
    let url = table.db.url.as_str();
    let opened = match rt
        .cache
        .get(table.db.store.object_store(), url, listed, &check)
        .await
    {
        Ok(Some(p)) => match kind.open(spec, p.view.clone()) {
            Ok(i) => Some((i, p)),
            Err(_) => None,
        },
        Ok(None) => return None,
        Err(_) => None,
    };
    match opened {
        Some((index, pin)) => {
            rt.handles.insert(key, index.clone(), pin);
            Some(index)
        }
        None => {
            rt.metrics.bad_sidecars.fetch_add(1, Ordering::Relaxed);
            let now = table.db.clock().now();
            state
                .discovery
                .mark_bad(&listed.location, now + std::time::Duration::from_secs(60));
            None
        }
    }
}
