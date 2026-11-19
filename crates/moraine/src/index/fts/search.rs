//! Full-text search over one snapshot (T9; cases
//! F1 to F12).
//!
//! 1. Parse the query once, with the spec's tokenizers, boosts and default operator.
//! 2. Get a searcher for **every** file of the snapshot: its sidecar when one is listed and
//!    opens, otherwise an index built in memory from the file's text columns (the same builder,
//!    so the same tokens; case F7), kept in a byte-bounded LRU (case F8).
//! 3. Sum the BM25 statistics of all of them: documents, tokens per field, and documents per
//!    query term. These are exactly the statistics one tantivy index over the whole table would
//!    have, so every file scores as that index would (case F5). A filter does not change them,
//!    as a filter does not change a merged index's statistics: it only drops rows.
//! 4. Search the files the filter can match, each for `k` hits among the rows it admits, and
//!    merge by score descending, then file path, then row offset.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use tantivy::query::{Bm25StatisticsProvider, QueryParser, QueryParserError};
use tantivy::schema::Field;
use tantivy::{Searcher, Term};

use super::{FtsIndex, FtsKind, FtsParams, KIND, TopRows, check_search};
use crate::error::{Error, Result};
use crate::index::ExactScanPolicy;
use crate::index::bundle::{BundleBytes, BundleMeta, BundleView, BundleWriter};
use crate::index::discover::{SidecarMap, data_segment};
use crate::index::kind::{FileIndex, IndexBuilder, IndexKind};
use crate::index::query::{
    Owned, Search, SearchResult, SearchStats, allow_bitmap, candidate_files, compile_filter,
    find_spec, merge_params, open_listed, output_columns, rows_of, with_scores,
};
use crate::index::spec::{IndexSpec, spec_hash};
use crate::index::topk::{Candidate, TopK};
use crate::index::{Order, TableIndexState};
use crate::log::DataFile;
use crate::read::fetch::scan_windows;
use crate::schema::project_schema;
use crate::snapshot::Snapshot;
use crate::table::TableInner;

/// Table-wide BM25 statistics: the sums over every file's searcher. Document frequencies are
/// computed for the query's terms up front and on demand for any other term a query asks for.
pub(crate) struct TableStats {
    searchers: Vec<Searcher>,
    docs: u64,
    tokens: Mutex<HashMap<Field, u64>>,
    freqs: Mutex<HashMap<Term, u64>>,
}

impl TableStats {
    pub(crate) fn new(searchers: Vec<Searcher>) -> TableStats {
        let docs = searchers.iter().map(|s| s.num_docs()).sum();
        TableStats {
            searchers,
            docs,
            tokens: Mutex::new(HashMap::new()),
            freqs: Mutex::new(HashMap::new()),
        }
    }

    fn sum_freq(&self, term: &Term) -> tantivy::Result<u64> {
        let mut n = 0;
        for s in &self.searchers {
            n += s.doc_freq(term)?;
        }
        Ok(n)
    }
}

impl Bm25StatisticsProvider for TableStats {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        if let Some(n) = self.tokens.lock().ok().and_then(|g| g.get(&field).copied()) {
            return Ok(n);
        }
        let mut n = 0;
        for s in &self.searchers {
            n += Bm25StatisticsProvider::total_num_tokens(s, field)?;
        }
        if let Ok(mut g) = self.tokens.lock() {
            g.insert(field, n);
        }
        Ok(n)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        if let Some(n) = self.freqs.lock().ok().and_then(|g| g.get(term).copied()) {
            return Ok(n);
        }
        let n = self.sum_freq(term)?;
        if let Ok(mut g) = self.freqs.lock() {
            g.insert(term.clone(), n);
        }
        Ok(n)
    }
}

/// The parsed query and what parsing it needed.
pub(crate) struct Parsed {
    pub query: Arc<dyn tantivy::query::Query>,
    pub terms: Vec<Term>,
}

/// Parses `text` for `spec` with `search` (the merged search parameters). `None` when the
/// query is empty (case F1).
pub(crate) fn parse(
    kind: &FtsKind,
    spec: &IndexSpec,
    search: &serde_json::Value,
    text: &str,
) -> Result<Option<Parsed>> {
    let params = FtsParams::of(spec)?;
    let (schema, fields, _) = params.schema();
    let manager = kind.tokenizers.manager(params.tokenizer_names())?;
    if text.trim().is_empty() {
        return Ok(None);
    }
    let mut parser = QueryParser::new(schema, fields.clone(), manager);
    if search.get("conjunction").and_then(|v| v.as_bool()) == Some(true) {
        parser.set_conjunction_by_default();
    }
    let boosts = search.get("boosts");
    for (c, f) in spec.columns.iter().zip(&fields) {
        let b = boosts
            .and_then(|b| b.get(c))
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        if b != 1.0 {
            parser.set_field_boost(*f, b as f32);
        }
    }
    let query = match parser.parse_query(text) {
        Ok(q) => q,
        // Nothing but excluded terms, or nothing left after tokenising: no rows (case F1).
        Err(QueryParserError::AllButQueryForbidden) => return Ok(None),
        Err(e) => return Err(parse_error(text, e)),
    };
    let mut terms: Vec<Term> = Vec::new();
    query.query_terms(&mut |t, _| {
        if !terms.contains(t) {
            terms.push(t.clone());
        }
    });
    Ok(Some(Parsed {
        query: Arc::from(query),
        terms,
    }))
}

fn parse_error(text: &str, e: QueryParserError) -> Error {
    match e {
        QueryParserError::FieldDoesNotHavePositionsIndexed(f) => Error::InvalidQuery(format!(
            "a phrase query on {f}, which records no positions (set record to \"positions\")"
        )),
        QueryParserError::FieldDoesNotExist(f) => Error::InvalidQuery(format!(
            "the query names {f}, which this index does not hold"
        )),
        other => Error::InvalidQuery(format!("full-text query {text:?}: {other}")),
    }
}

/// One file's searcher and how it was obtained.
struct Opened {
    file: Arc<DataFile>,
    index: Arc<dyn FileIndex>,
    sidecar: bool,
    /// Rows indexed in memory by this query (0 when cached or from a sidecar).
    built_rows: u64,
}

struct Ctx {
    table: Arc<TableInner>,
    kind: Arc<dyn IndexKind>,
    fts: FtsKind,
    spec: IndexSpec,
    hash: String,
    map: Option<Arc<SidecarMap>>,
    state: Arc<TableIndexState>,
    built_rows: AtomicU64,
}

pub(crate) async fn run(s: Search, snap: Arc<Snapshot>) -> Result<SearchResult> {
    let Owned::Text(text) = &s.query else {
        return Err(Error::Internal("not a text query".into()));
    };
    let table = s.table.clone();
    let rt = &table.db.index;
    let spec = find_spec(&snap, &s.index)?;
    if spec.kind != KIND {
        return Err(Error::InvalidQuery(format!(
            "index {} is of kind {}; search_text needs a full-text index",
            spec.name, spec.kind
        )));
    }
    let kind = rt.kind(KIND)?.clone();
    let fts = FtsKind::new(rt.cfg.tokenizers.clone());
    let search = merge_params(&spec.search, s.params.clone());
    check_search(&search, &spec.columns)?;
    let schema = snap.schema().clone();
    let (output, out_schema) = output_columns(&schema, &s.select, &spec)?;
    let mut stats = SearchStats {
        files: snap.files.len(),
        ..Default::default()
    };
    let filter = compile_filter(&table, &schema, &s.filter)?;
    let parsed = parse(&fts, &spec, &search, text)?;
    let empty = |stats: SearchStats| -> Result<SearchResult> {
        let out = if s.hits_only {
            project_schema(&schema, &[])?
        } else {
            out_schema.clone()
        };
        Ok(SearchResult {
            batch: with_scores(RecordBatch::new_empty(out), Vec::new())?,
            hits: Vec::new(),
            stats,
        })
    };
    let Some(parsed) = parsed else {
        return empty(stats);
    };
    if s.k == 0 {
        return empty(stats);
    }
    let mut map = None;
    if !matches!(rt.cache.mode(), crate::index::cache::CacheMode::Disabled) {
        let clock = table.db.clock();
        map = rt
            .table(table.idx_prefix().as_ref())
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
    let state = rt.table(table.idx_prefix().as_ref());
    state.text_searched.store(true, Ordering::Relaxed);
    let ctx = Arc::new(Ctx {
        hash: spec_hash(kind.as_ref(), &spec),
        state: rt.table(table.idx_prefix().as_ref()),
        table: table.clone(),
        kind,
        fts,
        spec,
        map,
        built_rows: AtomicU64::new(0),
    });

    // Every file's searcher: the statistics are the whole table's.
    let n = table.db.opts.io_concurrency.max(1);
    let opened: Vec<Opened> = futures::stream::iter(snap.files.iter().cloned())
        .map(|f| {
            let ctx = ctx.clone();
            async move { open_file(&ctx, f).await }
        })
        .buffered(n)
        .try_collect()
        .await?;
    let mut searchers = Vec::with_capacity(opened.len());
    for o in &opened {
        searchers.push(fts_index(o.index.as_ref())?.searcher().clone());
        if o.sidecar {
            stats.sidecar_files += 1;
        } else {
            stats.exact_files += 1;
        }
        stats.exact_rows += o.built_rows;
    }
    // The statistics depend only on the version's files, so one version's queries share
    // them, with the document frequencies summed so far (D53).
    let key = (snap.version, ctx.hash.clone());
    let cached = ctx.state.text_stats.lock().ok().and_then(|g| {
        g.iter()
            .find(|(k, _)| *k == key)
            .and_then(|(_, v)| v.clone().downcast::<TableStats>().ok())
    });
    let provider = match cached {
        Some(p) if p.searchers.len() == searchers.len() => p,
        _ => {
            let p = Arc::new(TableStats::new(searchers));
            if let Ok(mut g) = ctx.state.text_stats.lock() {
                g.retain(|(k, _)| *k != key);
                g.push((key, p.clone()));
                let excess = g.len().saturating_sub(2);
                g.drain(..excess);
            }
            p
        }
    };
    for t in &parsed.terms {
        provider.doc_freq(t).map_err(super::native)?;
    }

    // Search the files the filter can match.
    let wanted: std::collections::HashSet<String> = candidate_files(&snap, &s.filter)
        .into_iter()
        .map(|f| f.path.clone())
        .collect();
    let filter = Arc::new(filter);
    let k = s.k;
    let results: Vec<Option<Vec<Candidate>>> = futures::stream::iter(opened)
        .map(|o| {
            let (table, filter, provider) = (table.clone(), filter.clone(), provider.clone());
            let query = parsed.query.clone();
            let skip = !wanted.contains(&o.file.path);
            async move {
                if skip {
                    return Ok(None);
                }
                let allow = allow_bitmap(&table, &filter, &o.file).await?;
                if allow.as_ref().is_some_and(|a| a.is_empty()) {
                    return Ok(None);
                }
                let file: Arc<str> = o.file.path.as_str().into();
                tokio::task::spawn_blocking(move || -> Result<Option<Vec<Candidate>>> {
                    let index = fts_index(o.index.as_ref())?;
                    let top = index
                        .searcher()
                        .search_with_statistics_provider(
                            query.as_ref(),
                            &TopRows { want: k, allow },
                            provider.as_ref(),
                        )
                        .map_err(super::native)?;
                    Ok(Some(
                        top.into_iter()
                            .map(|(score, doc)| Candidate {
                                file: file.clone(),
                                offset: u64::from(doc),
                                score,
                                exact: true,
                            })
                            .collect(),
                    ))
                })
                .await
                .map_err(|e| Error::Internal(format!("search task: {e}")))?
            }
        })
        .buffer_unordered(n)
        .try_collect()
        .await?;
    let mut top = TopK::new(Order::Descending, k);
    for r in results {
        match r {
            None => stats.skipped_files += 1,
            Some(c) => c.into_iter().for_each(|c| top.push(c)),
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
    let ranked = top.into_sorted();
    if s.hits_only {
        let hits = ranked
            .into_iter()
            .map(|c| crate::index::SearchHit {
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
    let (batch, hits) = rows_of(&table, &snap, &output, &out_schema, ranked).await?;
    Ok(SearchResult { batch, hits, stats })
}

fn fts_index(i: &dyn FileIndex) -> Result<&FtsIndex> {
    i.as_any()
        .downcast_ref::<FtsIndex>()
        .ok_or_else(|| Error::Internal("fts: a sidecar of another kind".into()))
}

/// The searcher of `f`: its sidecar when listed, usable and above the policy's row count,
/// otherwise an index built in memory (cached).
async fn open_file(ctx: &Ctx, f: Arc<DataFile>) -> Result<Opened> {
    let rt = &ctx.table.db.index;
    let below_policy = f
        .rows()
        .is_some_and(|r| !ctx.kind.wants_sidecar(&ctx.spec, r));
    if !below_policy && let Some(index) = sidecar(ctx, &f).await {
        if index.as_any().downcast_ref::<FtsIndex>().is_some() {
            return Ok(Opened {
                file: f,
                index,
                sidecar: true,
                built_rows: 0,
            });
        }
        rt.metrics.bad_sidecars.fetch_add(1, Ordering::Relaxed);
    }
    let key = (f.path.clone(), f.size, ctx.hash.clone());
    if let Some(index) = rt.fallback.get(&key) {
        return Ok(Opened {
            file: f,
            index,
            sidecar: false,
            built_rows: 0,
        });
    }
    let rows = f.rows().unwrap_or(0);
    let total = ctx.built_rows.fetch_add(rows, Ordering::Relaxed) + rows;
    let limit = rt.cfg.max_exact_scan_rows;
    if total > limit {
        match rt.cfg.exact_policy {
            ExactScanPolicy::Error => return Err(Error::ExactScanLimit { rows: total, limit }),
            ExactScanPolicy::Allow if total - rows <= limit => {
                rt.metrics
                    .exact_limit_exceeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            ExactScanPolicy::Allow => {}
        }
    }
    let (index, bytes, built) = build_in_memory(ctx, &f).await?;
    rt.fallback.insert(key, index.clone(), bytes);
    Ok(Opened {
        file: f,
        index,
        sidecar: false,
        built_rows: built,
    })
}

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

/// Indexes `f`'s text columns in memory with the sidecar builder, and opens the result the way
/// a sidecar is opened. Returns the index, its bytes and the rows indexed.
async fn build_in_memory(ctx: &Ctx, f: &DataFile) -> Result<(Arc<dyn FileIndex>, usize, u64)> {
    let table = &ctx.table;
    let store = table.db.store.object_store().clone();
    let schema = project_schema(table.current().schema(), &ctx.spec.columns)?;
    let path = table.layer.data_prefix().clone().join(f.path.as_str());
    let (fts, spec) = (ctx.fts.clone(), ctx.spec.clone());
    let (tx, rx) = std::sync::mpsc::sync_channel::<(u64, RecordBatch)>(1);
    let worker = tokio::task::spawn_blocking(move || -> Result<super::FtsBuilder> {
        let mut b = fts.memory_builder(&spec)?;
        for (first, batch) in rx {
            b.add(first, &batch)?;
        }
        Ok(b)
    });
    let read = scan_windows(
        &table.db.cache,
        &store,
        &path,
        f.size,
        &schema,
        4 << 20,
        &mut |o, b| {
            let first = o.first().copied().unwrap_or(0);
            let _ = tx.send((first, b.clone()));
            Ok(())
        },
    )
    .await;
    drop(tx);
    let builder = worker
        .await
        .map_err(|e| Error::Internal(format!("build task: {e}")))??;
    let count = read?;
    let meta = BundleMeta {
        kind: KIND.into(),
        kind_format: ctx.kind.format_version(),
        spec_hash: ctx.hash.clone(),
        source_rows: count.rows,
        source_size: f.size,
    };
    let (fts, spec) = (ctx.fts.clone(), ctx.spec.clone());
    let index = tokio::task::spawn_blocking(move || -> Result<(Arc<dyn FileIndex>, usize)> {
        let mut w = BundleWriter::new(std::io::Cursor::new(Vec::new()), meta)?;
        let summary = IndexBuilder::finish(Box::new(builder), &mut w)?;
        let (c, _) = w.finish(summary.indexed_rows)?;
        let bytes = BundleBytes::copy(&c.into_inner())?;
        let len = bytes.len();
        let view = BundleView::parse(bytes)
            .map_err(|e| Error::Internal(format!("fts: in-memory bundle: {e}")))?;
        Ok((fts.open(&spec, view)?, len))
    })
    .await
    .map_err(|e| Error::Internal(format!("build task: {e}")))??;
    Ok((index.0, index.1, count.rows))
}

/// Opens or builds the searchers of `fresh` files at `snap` for `spec` (background refresh).
pub(crate) async fn prewarm(
    table: &Arc<TableInner>,
    snap: &Arc<Snapshot>,
    spec: &IndexSpec,
    fresh: &[Arc<DataFile>],
    map: Option<Arc<SidecarMap>>,
) {
    let rt = &table.db.index;
    let Ok(kind) = rt.kind(KIND) else { return };
    let kind = kind.clone();
    let _ = snap;
    let ctx = Ctx {
        hash: spec_hash(kind.as_ref(), spec),
        state: rt.table(table.idx_prefix().as_ref()),
        table: table.clone(),
        kind,
        fts: FtsKind::new(rt.cfg.tokenizers.clone()),
        spec: spec.clone(),
        map,
        built_rows: AtomicU64::new(0),
    };
    for f in fresh {
        let _ = open_file(&ctx, f.clone()).await;
    }
}
