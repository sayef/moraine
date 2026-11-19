//! Rows by offset (D12).
//!
//! Index queries name rows by (data file, row offset). [`scan_offsets`] streams chosen columns
//! of one file with the offset of every row, reading only the row groups (and, through the
//! page index, the pages) that hold allowed rows: the exact route and filter evaluation use it.
//! [`fetch_rows`] reads the rows at given offsets, which a search does for its final hits.
//! Both go through the footer cache and the same reader as scans, and decode under the panic
//! guard (D30).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use roaring::RoaringBitmap;

use super::{FileCache, ObjReader, guarded, to_schema};
use crate::error::{Error, Result};

/// What [`scan_offsets`] read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScanCount {
    pub rows: u64,
    pub row_groups: usize,
}

/// Streams `columns` (a projection of the table schema, `needed`) of one file in offset order,
/// calling `f(offsets, batch)` per decoded batch. With `allow`, only allowed rows are read
/// (`offsets` says which they are); row groups without allowed rows are skipped.
pub(crate) async fn scan_offsets(
    cache: &FileCache,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    needed: &SchemaRef,
    allow: Option<&RoaringBitmap>,
    f: &mut (dyn FnMut(&[u64], &RecordBatch) -> Result<()> + Send),
) -> Result<ScanCount> {
    let g = scan_unguarded(cache, store, path, size, needed, allow, 0, f);
    guarded(path, g).await
}

/// [`scan_offsets`] of every row, fetching at most about `window_bytes` of column data at a
/// time: each row group is read in windows of whole pages through the page index, so a build
/// over a large file never holds a whole column chunk (D35).
pub(crate) async fn scan_windows(
    cache: &FileCache,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    needed: &SchemaRef,
    window_bytes: usize,
    f: &mut (dyn FnMut(&[u64], &RecordBatch) -> Result<()> + Send),
) -> Result<ScanCount> {
    let g = scan_unguarded(
        cache,
        store,
        path,
        size,
        needed,
        None,
        window_bytes.max(1),
        f,
    );
    guarded(path, g).await
}

/// One stream's worth of a scan: a row group, the rows selected in it, their offsets.
struct Read {
    /// Row groups read by one stream: one, or every row group an `allow` touches (one stream
    /// with one row selection costs one reader setup instead of one per row group, which is
    /// most of fetching a few rows, D53).
    rgs: Vec<usize>,
    selectors: Option<Vec<RowSelector>>,
    offsets: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
async fn scan_unguarded(
    cache: &FileCache,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    needed: &SchemaRef,
    allow: Option<&RoaringBitmap>,
    window_bytes: usize,
    f: &mut (dyn FnMut(&[u64], &RecordBatch) -> Result<()> + Send),
) -> Result<ScanCount> {
    let file = cache.get(store, path, size).await?;
    let meta = file.metadata().clone();
    let file_schema = file.reader_meta.schema().clone();
    let roots: Vec<usize> = needed
        .fields()
        .iter()
        .filter_map(|fl| file_schema.index_of(fl.name()).ok())
        .collect();
    let mask = ProjectionMask::roots(meta.file_metadata().schema_descr(), roots);
    let mut reads: Vec<Read> = Vec::new();
    let mut multi: Option<Read> = None;
    let mut start = 0u64;
    for rg in 0..meta.num_row_groups() {
        let group = meta.row_group(rg);
        let rows = group.num_rows().max(0) as u64;
        let end = start + rows;
        match allow {
            None if window_bytes > 0 && rows > 0 => {
                let bytes: u64 = group
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask.leaf_included(*i))
                    .map(|(_, c)| c.compressed_size().max(0) as u64)
                    .sum();
                let windows = bytes.div_ceil(window_bytes as u64).clamp(1, rows);
                let per = rows.div_ceil(windows);
                let mut at = 0;
                while at < rows {
                    let n = per.min(rows - at);
                    let mut sel = Vec::new();
                    push(&mut sel, false, at as usize);
                    push(&mut sel, true, n as usize);
                    push(&mut sel, false, (rows - at - n) as usize);
                    sel.retain(|s| s.row_count > 0);
                    reads.push(Read {
                        rgs: vec![rg],
                        selectors: (windows > 1).then_some(sel),
                        offsets: (start + at..start + at + n).collect(),
                    });
                    at += n;
                }
            }
            None => reads.push(Read {
                rgs: vec![rg],
                selectors: None,
                offsets: (start..end).collect(),
            }),
            Some(a) => {
                let lo = u32::try_from(start).unwrap_or(u32::MAX);
                let hi = u32::try_from(end).unwrap_or(u32::MAX);
                if a.range_cardinality(lo..hi) > 0 {
                    let r = multi.get_or_insert_with(|| Read {
                        rgs: Vec::new(),
                        selectors: Some(Vec::new()),
                        offsets: Vec::new(),
                    });
                    r.rgs.push(rg);
                    let sel = r.selectors.get_or_insert_with(Vec::new);
                    let mut at = start;
                    for o in a.range(lo..hi) {
                        let o = u64::from(o);
                        if o > at {
                            push(sel, false, (o - at) as usize);
                        }
                        push(sel, true, 1);
                        r.offsets.push(o);
                        at = o + 1;
                    }
                    if end > at {
                        push(sel, false, (end - at) as usize);
                    }
                }
            }
        }
        start = end;
    }
    reads.extend(multi);
    let mut count = ScanCount::default();
    let mut last_rg = None;
    for read in reads {
        for rg in &read.rgs {
            if last_rg != Some(*rg) {
                count.row_groups += 1;
                last_rg = Some(*rg);
            }
        }
        let reader = ObjReader {
            store: store.clone(),
            path: path.clone(),
            start: file.start,
            tail: file.tail.clone(),
            meta: Some(meta.clone()),
            // Reads by row offset (search hits) keep their small pages.
            pages: allow.is_some().then(|| cache.pages.clone()),
        };
        let mut b =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, file.reader_meta.clone())
                .with_projection(mask.clone())
                .with_row_groups(read.rgs.clone())
                .with_batch_size(1024);
        if let Some(sel) = read.selectors {
            b = b.with_row_selection(RowSelection::from(sel));
        }
        let mut stream = b.build()?;
        let mut at = 0usize;
        while let Some(batch) = stream.try_next().await? {
            let n = batch.num_rows();
            let Some(offs) = read.offsets.get(at..at + n) else {
                return Err(Error::from(parquet::errors::ParquetError::General(
                    format!("{path}: the reader returned more rows than selected"),
                )));
            };
            let batch = to_schema(&batch, needed)?;
            f(offs, &batch)?;
            at += n;
        }
        if at != read.offsets.len() {
            return Err(Error::from(parquet::errors::ParquetError::General(
                format!("{path}: read {at} rows of {}", read.offsets.len()),
            )));
        }
        count.rows += at as u64;
    }
    Ok(count)
}

fn push(sel: &mut Vec<RowSelector>, select: bool, n: usize) {
    if let Some(last) = sel.last_mut()
        && last.skip != select
    {
        last.row_count += n;
        return;
    }
    sel.push(if select {
        RowSelector::select(n)
    } else {
        RowSelector::skip(n)
    });
}

/// The rows at `offsets` (any order, duplicates allowed) of one file, with the columns of
/// `needed`, in the order of `offsets`.
pub(crate) async fn fetch_rows(
    cache: &FileCache,
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    size: u64,
    needed: &SchemaRef,
    offsets: &[u64],
) -> Result<RecordBatch> {
    let mut allow = RoaringBitmap::new();
    for o in offsets {
        let o = u32::try_from(*o)
            .map_err(|_| Error::InvalidQuery(format!("row offset {o} out of range")))?;
        allow.insert(o);
    }
    let mut got: Vec<(Vec<u64>, RecordBatch)> = Vec::new();
    scan_offsets(
        cache,
        store,
        path,
        size,
        needed,
        Some(&allow),
        &mut |o, b| {
            got.push((o.to_vec(), b.clone()));
            Ok(())
        },
    )
    .await?;
    let read: Vec<u64> = got.iter().flat_map(|(o, _)| o.iter().copied()).collect();
    let batches: Vec<RecordBatch> = got.into_iter().map(|(_, b)| b).collect();
    let all = arrow::compute::concat_batches(needed, &batches)?;
    // Restore the caller's order: position of each wanted offset among the rows read.
    let mut idx = Vec::with_capacity(offsets.len());
    for o in offsets {
        let i = read.binary_search(o).map_err(|_| {
            Error::from(parquet::errors::ParquetError::General(format!(
                "{path}: row {o} is beyond the file's rows"
            )))
        })?;
        idx.push(i as u32);
    }
    let idx = arrow::array::UInt32Array::from(idx);
    Ok(arrow::compute::take_record_batch(&all, &idx)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MetricsStore, StoreMetrics};
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    const ROWS: i64 = 50_000;

    async fn file() -> (
        Arc<dyn ObjectStore>,
        Arc<StoreMetrics>,
        Path,
        u64,
        SchemaRef,
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ]));
        let ids = Int64Array::from_iter_values(0..ROWS);
        let s = StringArray::from_iter((0..ROWS).map(|i| (i % 7 != 0).then(|| format!("v{i}"))));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(s)]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(16_384))
            .set_data_page_row_count_limit(1024)
            .set_write_batch_size(1024)
            .build();
        let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        let body = w.into_inner().unwrap();
        let size = body.len() as u64;
        let metrics = Arc::new(StoreMetrics::default());
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("t/data/f.parquet");
        inner.put(&path, PutPayload::from(body)).await.unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(MetricsStore::new(inner, metrics.clone()));
        (store, metrics, path, size, schema)
    }

    fn ids(b: &RecordBatch) -> Vec<i64> {
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[tokio::test]
    async fn fetch_by_offset_equals_a_full_scan() {
        let (store, metrics, path, size, schema) = file().await;
        let cache = FileCache::new(64 << 20);
        let wanted = [49_999u64, 3, 3, 20_000, 17, 16_383, 16_384, 0];
        let b = fetch_rows(&cache, &store, &path, size, &schema, &wanted)
            .await
            .unwrap();
        assert_eq!(
            ids(&b),
            wanted.iter().map(|o| *o as i64).collect::<Vec<_>>()
        );
        let s = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for (i, o) in wanted.iter().enumerate() {
            assert_eq!(s.is_null(i), o % 7 == 0);
        }
        // Warm footer: two rows in one page need, per column, the dictionary page and the data
        // page: four ranges in one `get_ranges` call (one request on stores that coalesce
        // nearby ranges, as S3's does; one per range in memory).
        let before = metrics.snapshot();
        fetch_rows(&cache, &store, &path, size, &schema, &[20_001, 20_500])
            .await
            .unwrap();
        let gets = metrics.snapshot().since(&before).requests();
        assert!(gets <= 6, "{gets} requests for two rows in one page range");
        // Beyond the file: an error, not a panic.
        assert!(
            fetch_rows(&cache, &store, &path, size, &schema, &[ROWS as u64])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn windowed_scan_reads_every_row_in_small_fetches() {
        let (store, metrics, path, size, schema) = file().await;
        let cache = FileCache::new(64 << 20);
        cache.get(&store, &path, size).await.unwrap();
        let before = metrics.snapshot();
        let mut all = Vec::new();
        let n = scan_windows(
            &cache,
            &store,
            &path,
            size,
            &schema,
            32 << 10,
            &mut |o, b| {
                assert_eq!(o.iter().map(|x| *x as i64).collect::<Vec<_>>(), ids(b));
                all.extend_from_slice(o);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(n.rows, ROWS as u64);
        assert_eq!(all, (0..ROWS as u64).collect::<Vec<_>>());
        let read = metrics.snapshot().since(&before);
        assert!(read.requests() > 4, "{read:?}: windows were not split");
    }

    #[tokio::test]
    async fn scan_with_allow_reads_only_allowed_rows() {
        let (store, _, path, size, schema) = file().await;
        let cache = FileCache::new(64 << 20);
        let mut all = Vec::new();
        let n = scan_offsets(&cache, &store, &path, size, &schema, None, &mut |o, b| {
            assert_eq!(o.iter().map(|x| *x as i64).collect::<Vec<_>>(), ids(b));
            all.extend_from_slice(o);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(n.rows, ROWS as u64);
        assert_eq!(all.len(), ROWS as usize);
        let allow: RoaringBitmap = [5u32, 6, 40_000, 49_999].into_iter().collect();
        let mut seen = Vec::new();
        let n = scan_offsets(
            &cache,
            &store,
            &path,
            size,
            &schema,
            Some(&allow),
            &mut |o, b| {
                assert_eq!(o.iter().map(|x| *x as i64).collect::<Vec<_>>(), ids(b));
                seen.extend_from_slice(o);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(seen, vec![5, 6, 40_000, 49_999]);
        assert_eq!(n.row_groups, 3);
    }
}
