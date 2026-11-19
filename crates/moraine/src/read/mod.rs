//! The read path for data files (D19, D20).
//!
//! - [`FileCache`]: Parquet footers with page indexes, the file's tail (which holds the bloom
//!   filters, D19) and, for small files, the whole file, cached by (path, size) in a
//!   byte-bounded LRU shared by every table of a database. Data files are immutable, so entries
//!   are evicted but never invalidated.
//! - [`read_file`]: one file's rows matching a filter, reading only the row groups whose bloom
//!   filters admit the filter's equality values and the pages whose statistics may match.

pub(crate) mod fetch;
pub(crate) mod prune;

use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, new_null_array};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use bytes::Bytes;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use futures::TryStreamExt;
use futures::future::{BoxFuture, FutureExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::bloom_filter::Sbbf;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::page_index::column_index::ColumnIndexMetaData;

use crate::error::{Error, Result};
use crate::stats::ColRange;
use prune::Bounds;

/// Files up to this size are fetched whole in one request and kept whole.
const WHOLE_FILE: u64 = 256 << 10;
/// Bytes of a larger file's tail fetched first: footer, page index and bloom filters (D19).
const TAIL: u64 = 128 << 10;

/// Bloom filters by (row group, leaf column); `None` when the column chunk has none.
type BloomCache = HashMap<(usize, usize), Option<Arc<Sbbf>>>;

/// One cached data file.
pub(crate) struct CachedFile {
    pub reader_meta: ArrowReaderMetadata,
    /// Bytes from `start` to the end of the file (the whole file when `start` is 0).
    start: u64,
    tail: Bytes,
    blooms: Mutex<BloomCache>,
    mem: usize,
}

impl CachedFile {
    fn metadata(&self) -> &Arc<ParquetMetaData> {
        self.reader_meta.metadata()
    }
}

struct Lru {
    map: HashMap<(String, u64), (Arc<CachedFile>, u64)>,
    tick: u64,
    bytes: usize,
}

/// See the module documentation.
pub(crate) struct FileCache {
    lru: Mutex<Lru>,
    budget: usize,
    /// Small data pages fetched by row offset (the hits of a search), D53.
    pub(crate) pages: Arc<PageCache>,
}

/// Byte ranges of data files fetched by row offset, by (path, range), in a byte-bounded LRU.
/// Data files are never rewritten, so an entry never goes stale.
pub(crate) struct PageCache {
    /// Ranges with their last use, the use counter, and the bytes held.
    map: Mutex<(PageMap, u64, usize)>,
    budget: usize,
}

/// (path, start, end) to the bytes and their last use.
type PageMap = HashMap<(String, u64, u64), (Bytes, u64)>;

/// Ranges larger than this are not kept.
const MAX_CACHED_RANGE: u64 = 64 << 10;

impl PageCache {
    fn get(&self, path: &str, r: &Range<u64>) -> Option<Bytes> {
        let mut g = self.map.lock().ok()?;
        g.1 += 1;
        let t = g.1;
        let e = g.0.get_mut(&(path.to_string(), r.start, r.end))?;
        e.1 = t;
        Some(e.0.clone())
    }

    fn put(&self, path: &str, r: &Range<u64>, b: &Bytes) {
        if r.end - r.start > MAX_CACHED_RANGE || self.budget == 0 {
            return;
        }
        let Ok(mut g) = self.map.lock() else {
            return;
        };
        g.1 += 1;
        let t = g.1;
        g.2 += b.len();
        if let Some((old, _)) =
            g.0.insert((path.to_string(), r.start, r.end), (b.clone(), t))
        {
            g.2 -= old.len();
        }
        if g.2 > self.budget {
            let mut ticks: Vec<u64> = g.0.values().map(|(_, t)| *t).collect();
            ticks.sort_unstable();
            let cut = ticks[ticks.len() / 2];
            g.0.retain(|_, (_, t)| *t > cut);
            g.2 = g.0.values().map(|(b, _)| b.len()).sum();
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        self.map.lock().map(|g| g.2).unwrap_or(0)
    }
}

impl std::fmt::Debug for FileCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCache")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl FileCache {
    pub fn new(budget: usize) -> Self {
        FileCache {
            lru: Mutex::new(Lru {
                map: HashMap::new(),
                tick: 0,
                bytes: 0,
            }),
            budget,
            // An eighth of the file cache's budget, on top of it.
            pages: Arc::new(PageCache {
                map: Mutex::new((HashMap::new(), 0, 0)),
                budget: budget / 8,
            }),
        }
    }

    /// Entries and their bytes.
    pub fn usage(&self) -> (usize, usize) {
        self.lru
            .lock()
            .map(|g| (g.map.len(), g.bytes))
            .unwrap_or_default()
    }

    fn lookup(&self, key: &(String, u64)) -> Option<Arc<CachedFile>> {
        let mut g = self.lru.lock().ok()?;
        g.tick += 1;
        let t = g.tick;
        let e = g.map.get_mut(key)?;
        e.1 = t;
        Some(e.0.clone())
    }

    fn insert(&self, key: (String, u64), f: Arc<CachedFile>) {
        let Ok(mut g) = self.lru.lock() else {
            return;
        };
        g.tick += 1;
        let t = g.tick;
        g.bytes += f.mem;
        if let Some(old) = g.map.insert(key, (f, t)) {
            g.bytes -= old.0.mem;
        }
        if g.bytes > self.budget {
            // Evict the least recently used quarter of the entries at once.
            let mut ticks: Vec<u64> = g.map.values().map(|(_, t)| *t).collect();
            ticks.sort_unstable();
            let cut = ticks[ticks.len() / 4];
            let before: Vec<(String, u64)> = g
                .map
                .iter()
                .filter(|(_, (_, t))| *t <= cut)
                .map(|(k, _)| k.clone())
                .collect();
            for k in before {
                if g.bytes <= self.budget * 3 / 4 {
                    break;
                }
                if let Some((f, _)) = g.map.remove(&k) {
                    g.bytes -= f.mem;
                }
            }
        }
    }

    /// The cached file, loading it on a miss.
    pub async fn get(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        size: u64,
    ) -> Result<Arc<CachedFile>> {
        let key = (path.to_string(), size);
        if let Some(f) = self.lookup(&key) {
            return Ok(f);
        }
        let f = Arc::new(guarded(path, load(store, path, size)).await?);
        self.insert(key, f.clone());
        Ok(f)
    }
}

async fn load(store: &Arc<dyn ObjectStore>, path: &Path, size: u64) -> Result<CachedFile> {
    let start = if size <= WHOLE_FILE { 0 } else { size - TAIL };
    let tail = if start == 0 {
        store.get(path).await?.bytes().await?
    } else {
        store.get_range(path, start..size).await?
    };
    if tail.len() as u64 != size - start {
        return Err(Error::Internal(format!(
            "{path}: read {} bytes of {}",
            tail.len(),
            size - start
        )));
    }
    let mut reader = ObjReader {
        store: store.clone(),
        path: path.clone(),
        start,
        tail: tail.clone(),
        meta: None,
        pages: None,
    };
    let meta = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_prefetch_hint(Some((size - start) as usize))
        .load_and_finish(&mut reader, size)
        .await?;
    let meta = Arc::new(meta);
    // The table's schema is in the log, and `to_schema` casts to it; the Arrow schema embedded
    // in the footer is an unverified flatbuffer whose decoder panics on some corrupt input.
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let reader_meta = ArrowReaderMetadata::try_new(meta.clone(), options)?;
    let mem = tail.len() + meta.memory_size();
    Ok(CachedFile {
        reader_meta,
        start,
        tail,
        blooms: Mutex::new(HashMap::new()),
        mem,
    })
}

/// Parquet's async reader over the store, serving what the cache holds from memory.
#[derive(Clone)]
struct ObjReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    start: u64,
    tail: Bytes,
    meta: Option<Arc<ParquetMetaData>>,
    /// Where small ranges are kept, for reads by row offset.
    pages: Option<Arc<PageCache>>,
}

impl ObjReader {
    fn cached(&self, r: &Range<u64>) -> Option<Bytes> {
        if r.start >= self.start && r.end <= self.start + self.tail.len() as u64 {
            let a = (r.start - self.start) as usize;
            let b = (r.end - self.start) as usize;
            Some(self.tail.slice(a..b))
        } else {
            None
        }
    }
}

impl AsyncFileReader for ObjReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        if let Some(b) = self.cached(&range).or_else(|| {
            self.pages
                .as_ref()
                .and_then(|p| p.get(self.path.as_ref(), &range))
        }) {
            return futures::future::ready(Ok(b)).boxed();
        }
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            let pages = self.pages.clone();
            let mut out: Vec<Option<Bytes>> = ranges
                .iter()
                .map(|r| {
                    self.cached(r)
                        .or_else(|| pages.as_ref().and_then(|p| p.get(self.path.as_ref(), r)))
                })
                .collect();
            let missing: Vec<Range<u64>> = ranges
                .iter()
                .zip(&out)
                .filter(|(_, b)| b.is_none())
                .map(|(r, _)| r.clone())
                .collect();
            if !missing.is_empty() {
                let fetched = self
                    .store
                    .get_ranges(&self.path, &missing)
                    .await
                    .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                if let Some(p) = &pages {
                    for (r, b) in missing.iter().zip(&fetched) {
                        p.put(self.path.as_ref(), r, b);
                    }
                }
                let mut it = fetched.into_iter();
                for slot in out.iter_mut().filter(|b| b.is_none()) {
                    *slot = it.next();
                }
            }
            out.into_iter()
                .map(|b| {
                    b.ok_or_else(|| {
                        parquet::errors::ParquetError::General("range not fetched".into())
                    })
                })
                .collect()
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let m = self.meta.clone();
        async move {
            m.ok_or_else(|| parquet::errors::ParquetError::General("metadata not loaded".into()))
        }
        .boxed()
    }
}

/// What to read from each file of a scan.
pub(crate) struct FileQuery {
    pub table_schema: SchemaRef,
    /// Columns the result has, in order.
    pub output: Vec<String>,
    /// Columns read: the output plus the filter's.
    pub needed: SchemaRef,
    pub filter: Option<Expr>,
    pub physical: Option<Arc<dyn PhysicalExpr>>,
}

/// Whether the bloom filters of a cached file, as far as they are loaded, prove that no row
/// group holds the filter's equality values. `false` when unsure (file not cached, a bloom
/// filter not loaded yet), so the caller reads the file. Costs no request.
pub(crate) fn bloom_excludes(cache: &FileCache, path: &Path, size: u64, filter: &Expr) -> bool {
    let Some(f) = cache.lookup(&(path.to_string(), size)) else {
        return false;
    };
    let mut equalities = Vec::new();
    prune::equality_values(filter, &mut equalities);
    if equalities.is_empty() {
        return false;
    }
    let meta = f.metadata();
    let leaves = leaf_indexes(meta);
    let schema = f.reader_meta.schema();
    let Ok(blooms) = f.blooms.lock() else {
        return false;
    };
    (0..meta.num_row_groups()).all(|rg| {
        equalities.iter().any(|(column, values)| {
            let (Some(&leaf), Ok(field)) = (leaves.get(*column), schema.field_with_name(column))
            else {
                return false;
            };
            match blooms.get(&(rg, leaf)) {
                Some(Some(sbbf)) => !values.iter().any(|v| {
                    prune::cast_to(v, field.data_type())
                        .and_then(|v| bloom_check(sbbf, &v))
                        .unwrap_or(true)
                }),
                _ => false,
            }
        })
    })
}

/// Rows of one file that match the query, with the output columns.
/// Runs Parquet decoding, turning a panic inside the parquet or arrow crates (corrupt input can
/// reach one) into an error for that file instead of unwinding through the caller.
async fn guarded<T>(path: &Path, fut: impl Future<Output = Result<T>>) -> Result<T> {
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(r) => r,
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            Err(Error::from(ParquetError::General(format!(
                "{path}: corrupt file, the decoder panicked: {msg}"
            ))))
        }
    }
}

pub(crate) async fn read_file(
    cache: &FileCache,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    q: &FileQuery,
) -> Result<Vec<RecordBatch>> {
    guarded(path, read_file_unguarded(cache, store, path, size, q)).await
}

async fn read_file_unguarded(
    cache: &FileCache,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    q: &FileQuery,
) -> Result<Vec<RecordBatch>> {
    let f = cache.get(store, path, size).await?;
    let meta = f.metadata().clone();
    let file_schema = f.reader_meta.schema().clone();
    let mut reader = ObjReader {
        store: store.clone(),
        path: path.clone(),
        start: f.start,
        tail: f.tail.clone(),
        meta: Some(meta.clone()),
        pages: None,
    };

    // Row groups that bloom filters do not exclude, and the rows of each that pages may hold.
    let mut equalities = Vec::new();
    if let Some(e) = &q.filter {
        prune::equality_values(e, &mut equalities);
    }
    let leaves = leaf_indexes(&meta);
    let mut groups: Vec<usize> = Vec::new();
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut any_skipped = false;
    for rg in 0..meta.num_row_groups() {
        if !bloom_admits(&f, &mut reader, rg, &equalities, &leaves, &file_schema).await? {
            any_skipped = true;
            continue;
        }
        let rows = meta.row_group(rg).num_rows() as usize;
        let ranges = match &q.filter {
            Some(e) => page_ranges(&meta, rg, rows, e, &leaves, &q.table_schema),
            None => std::iter::once(0..rows).collect(),
        };
        if ranges.is_empty() {
            any_skipped = true;
            continue;
        }
        if ranges.len() != 1 || ranges[0] != (0..rows) {
            any_skipped = true;
        }
        groups.push(rg);
        let mut at = 0;
        for r in ranges {
            if r.start > at {
                selectors.push(RowSelector::skip(r.start - at));
            }
            selectors.push(RowSelector::select(r.end - r.start));
            at = r.end;
        }
        if rows > at {
            selectors.push(RowSelector::skip(rows - at));
        }
    }
    if groups.is_empty() {
        return Ok(Vec::new());
    }

    let roots: Vec<usize> = q
        .needed
        .fields()
        .iter()
        .filter_map(|f| file_schema.index_of(f.name()).ok())
        .collect();
    let mask = ProjectionMask::roots(meta.file_metadata().schema_descr(), roots);
    let mut b = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, f.reader_meta.clone())
        .with_projection(mask)
        .with_row_groups(groups)
        .with_batch_size(8192);
    if any_skipped {
        b = b.with_row_selection(RowSelection::from(selectors));
    }
    let batches: Vec<RecordBatch> = b.build()?.try_collect().await?;
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let batch = to_schema(&batch, &q.needed)?;
        let batch = match &q.physical {
            Some(p) => {
                let mask = p.evaluate(&batch)?.into_array(batch.num_rows())?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::InvalidQuery("the filter is not boolean".into()))?;
                filter_record_batch(&batch, mask)?
            }
            None => batch,
        };
        if batch.num_rows() > 0 {
            out.push(project(&batch, &q.output)?);
        }
    }
    Ok(out)
}

/// Every row of a data file as a stream of batches of `batch_rows` rows in the table schema
/// `schema`, read a row group at a time: memory holds one row group's column chunks, not the
/// file (streaming compaction, D59).
pub(crate) async fn stream_file(
    cache: &FileCache,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    schema: &SchemaRef,
    batch_rows: usize,
) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
    use futures::StreamExt;
    let f = cache.get(store, path, size).await?;
    let meta = f.metadata().clone();
    let reader = ObjReader {
        store: store.clone(),
        path: path.clone(),
        start: f.start,
        tail: f.tail.clone(),
        meta: Some(meta),
        pages: None,
    };
    let s = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, f.reader_meta.clone())
        .with_batch_size(batch_rows)
        .build()?;
    let schema = schema.clone();
    Ok(s.map(move |b| {
        let b = b?;
        to_schema(&b, &schema)
    })
    .boxed())
}

/// `batch` with exactly the columns of `schema`: missing ones (added to the table after the
/// file was written) are null, types are cast where the file stored a compatible type.
pub(crate) fn to_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let cols: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| match batch.column_by_name(f.name()) {
            Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
            Some(c) => Ok(cast(c, f.data_type())?),
            None => Ok(new_null_array(f.data_type(), batch.num_rows())),
        })
        .collect::<Result<_>>()?;
    let opts = arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &opts,
    )?)
}

pub(crate) fn project(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch> {
    let idx: Vec<usize> = columns
        .iter()
        .map(|c| batch.schema().index_of(c))
        .collect::<std::result::Result<_, _>>()?;
    Ok(batch.project(&idx)?)
}

/// Leaf column index of each top-level primitive column.
fn leaf_indexes(meta: &ParquetMetaData) -> HashMap<String, usize> {
    let mut out = HashMap::new();
    for (i, c) in meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
    {
        let parts = c.path().parts();
        if parts.len() == 1 {
            out.insert(parts[0].clone(), i);
        }
    }
    out
}

async fn bloom_admits(
    f: &CachedFile,
    reader: &mut ObjReader,
    rg: usize,
    equalities: &[(&str, Vec<&ScalarValue>)],
    leaves: &HashMap<String, usize>,
    file_schema: &SchemaRef,
) -> Result<bool> {
    for (column, values) in equalities {
        let Some(&leaf) = leaves.get(*column) else {
            continue;
        };
        let Ok(field) = file_schema.field_with_name(column) else {
            continue;
        };
        let Some(sbbf) = bloom(f, reader, rg, leaf).await? else {
            continue;
        };
        let any = values
            .iter()
            .any(|v| match prune::cast_to(v, field.data_type()) {
                Some(v) => bloom_check(&sbbf, &v).unwrap_or(true),
                None => true,
            });
        if !any {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn bloom(
    f: &CachedFile,
    reader: &mut ObjReader,
    rg: usize,
    leaf: usize,
) -> Result<Option<Arc<Sbbf>>> {
    if let Some(b) = f
        .blooms
        .lock()
        .ok()
        .and_then(|m| m.get(&(rg, leaf)).cloned())
    {
        return Ok(b);
    }
    let col = f.metadata().row_group(rg).column(leaf);
    let loaded = match (col.bloom_filter_offset(), col.bloom_filter_length()) {
        (Some(off), Some(len)) if off >= 0 && len > 0 => {
            let bytes = reader
                .get_bytes(off as u64..off as u64 + len as u64)
                .await?;
            Sbbf::from_bytes(&bytes).ok().map(Arc::new)
        }
        _ => None,
    };
    if let Ok(mut m) = f.blooms.lock() {
        m.insert((rg, leaf), loaded.clone());
    }
    Ok(loaded)
}

/// `Some(false)` when the bloom filter proves `v` absent. The value is hashed in Parquet's
/// plain encoding of the column's physical type.
fn bloom_check(b: &Sbbf, v: &ScalarValue) -> Option<bool> {
    Some(match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => b.check(s.as_str()),
        ScalarValue::Int64(Some(x)) | ScalarValue::TimestampMicrosecond(Some(x), _) => b.check(x),
        ScalarValue::Int32(Some(x)) | ScalarValue::Date32(Some(x)) => b.check(x),
        ScalarValue::Int16(Some(x)) => b.check(&i32::from(*x)),
        ScalarValue::Int8(Some(x)) => b.check(&i32::from(*x)),
        ScalarValue::Float64(Some(x)) => b.check(x),
        ScalarValue::Float32(Some(x)) => b.check(x),
        _ => return None,
    })
}

/// Row ranges of row group `rg` whose pages may hold rows matching `filter`: for every
/// conjunct on a single column with a page index, the matching pages' rows, intersected.
fn page_ranges(
    meta: &ParquetMetaData,
    rg: usize,
    rows: usize,
    filter: &Expr,
    leaves: &HashMap<String, usize>,
    schema: &Schema,
) -> Vec<Range<usize>> {
    let mut selected: Vec<Range<usize>> = std::iter::once(0..rows).collect();
    let (Some(ci), Some(oi)) = (meta.column_index(), meta.offset_index()) else {
        return selected;
    };
    for conjunct in datafusion::logical_expr::utils::split_conjunction(filter) {
        let cols = conjunct.column_refs();
        if cols.len() != 1 {
            continue;
        }
        let name = &cols
            .iter()
            .next()
            .map(|c| c.name.clone())
            .unwrap_or_default();
        let (Some(&leaf), Ok(field)) = (leaves.get(name), schema.field_with_name(name)) else {
            continue;
        };
        let (Some(index), Some(offsets)) = (
            ci.get(rg).and_then(|r| r.get(leaf)),
            oi.get(rg).and_then(|r| r.get(leaf)),
        ) else {
            continue;
        };
        let locs = offsets.page_locations();
        if locs.is_empty() || matches!(index, ColumnIndexMetaData::NONE) {
            continue;
        }
        let mut keep: Vec<Range<usize>> = Vec::new();
        for (i, loc) in locs.iter().enumerate() {
            let start = loc.first_row_index as usize;
            let end = locs.get(i + 1).map_or(rows, |n| n.first_row_index as usize);
            let page = PageBounds {
                name,
                range: page_range(index, i, field.data_type(), (end - start) as u64),
            };
            if prune::may_match(conjunct, schema, &page) {
                match keep.last_mut() {
                    Some(last) if last.end == start => last.end = end,
                    _ => keep.push(start..end),
                }
            }
        }
        selected = intersect(&selected, &keep);
        if selected.is_empty() {
            break;
        }
    }
    selected
}

struct PageBounds<'a> {
    name: &'a str,
    range: ColRange,
}

impl Bounds for PageBounds<'_> {
    fn bounds(&self, column: &str) -> Option<ColRange> {
        (column == self.name).then(|| self.range.clone())
    }
}

fn page_range(index: &ColumnIndexMetaData, i: usize, t: &DataType, rows: u64) -> ColRange {
    use ColumnIndexMetaData as C;
    let nulls = index.null_count(i).map(|n| n.max(0) as u64);
    let mut r = ColRange {
        min: None,
        max: None,
        nulls,
        rows: Some(rows),
    };
    if index.is_null_page(i) {
        r.nulls = Some(rows);
        return r;
    }
    let (min, max) = match (index, t) {
        (C::INT64(x), DataType::Int64) => (
            x.min_value(i).map(|v| ScalarValue::Int64(Some(*v))),
            x.max_value(i).map(|v| ScalarValue::Int64(Some(*v))),
        ),
        (C::INT64(x), DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz)) => (
            x.min_value(i)
                .map(|v| ScalarValue::TimestampMicrosecond(Some(*v), tz.clone())),
            x.max_value(i)
                .map(|v| ScalarValue::TimestampMicrosecond(Some(*v), tz.clone())),
        ),
        (C::INT32(x), DataType::Int32) => (
            x.min_value(i).map(|v| ScalarValue::Int32(Some(*v))),
            x.max_value(i).map(|v| ScalarValue::Int32(Some(*v))),
        ),
        (C::INT32(x), DataType::Date32) => (
            x.min_value(i).map(|v| ScalarValue::Date32(Some(*v))),
            x.max_value(i).map(|v| ScalarValue::Date32(Some(*v))),
        ),
        (C::DOUBLE(x), DataType::Float64) => (
            x.min_value(i)
                .filter(|v| v.is_finite())
                .map(|v| ScalarValue::Float64(Some(*v))),
            x.max_value(i)
                .filter(|v| v.is_finite())
                .map(|v| ScalarValue::Float64(Some(*v))),
        ),
        (C::BYTE_ARRAY(x), DataType::Utf8) => (
            x.min_value(i)
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| ScalarValue::Utf8(Some(s.to_string()))),
            x.max_value(i)
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| ScalarValue::Utf8(Some(s.to_string()))),
        ),
        _ => (None, None),
    };
    r.min = min;
    r.max = max;
    r
}

fn intersect(a: &[Range<usize>], b: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let lo = a[i].start.max(b[j].start);
        let hi = a[i].end.min(b[j].end);
        if lo < hi {
            out.push(lo..hi);
        }
        if a[i].end < b[j].end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn intersects_ranges() {
        assert_eq!(intersect(&[0..10], &[2..4, 6..12]), vec![2..4, 6..10]);
        assert_eq!(intersect(&[0..3, 5..8], &[2..6]), vec![2..3, 5..6]);
        assert!(intersect(&[0..3], &[3..5]).is_empty());
    }
}
