//! Data files: sorting a commit's rows by (cluster key, primary key) and writing them as Parquet
//! with the settings of D12 and D19.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{BloomFilterPosition, EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use crate::clock::{Clock, random_name};
use crate::error::Result;
use crate::options::TableDef;
use crate::stats::{self, FileStats};

/// One encoded data file, ready to upload.
pub(crate) struct Encoded {
    pub name: String,
    pub body: Bytes,
    pub stats: FileStats,
    pub sorted_by: Vec<String>,
}

/// Concatenates `batches` and sorts the rows by the table's sort columns. The sort is stable,
/// so rows with equal keys keep their order (later callers after earlier ones).
pub(crate) fn sort_rows(
    schema: &SchemaRef,
    def: &TableDef,
    batches: &[RecordBatch],
) -> Result<RecordBatch> {
    let all = concat_batches(schema, batches)?;
    let cols = def.sort_columns();
    if cols.is_empty() || all.num_rows() < 2 {
        return Ok(all);
    }
    let mut keys: Vec<SortColumn> = cols
        .iter()
        .filter_map(|c| all.column_by_name(c))
        .map(|a| SortColumn {
            values: a.clone(),
            options: None,
        })
        .collect();
    // A row-number tie-breaker makes the sort stable.
    keys.push(SortColumn {
        values: Arc::new(arrow::array::UInt32Array::from_iter_values(
            0..all.num_rows() as u32,
        )),
        options: None,
    });
    let idx = lexsort_to_indices(&keys, None)?;
    Ok(take_record_batch(&all, &idx)?)
}

/// Splits sorted rows into pieces of about `target_bytes` each (by in-memory size).
pub(crate) fn split(batch: RecordBatch, target_bytes: u64) -> Vec<RecordBatch> {
    let size = batch.get_array_memory_size() as u64;
    if size <= target_bytes || batch.num_rows() < 2 {
        return vec![batch];
    }
    let pieces = size.div_ceil(target_bytes).max(1) as usize;
    let per = batch.num_rows().div_ceil(pieces);
    (0..batch.num_rows())
        .step_by(per)
        .map(|start| batch.slice(start, per.min(batch.num_rows() - start)))
        .collect()
}

/// Data page size of vector columns.
const VECTOR_PAGE_BYTES: usize = 64 << 10;

pub(crate) fn writer_props(schema: &SchemaRef, def: &TableDef, rows: usize) -> WriterProperties {
    let mut b = WriterProperties::builder()
        .set_max_row_group_row_count(Some(def.row_group_rows))
        .set_data_page_size_limit(1 << 20)
        .set_data_page_row_count_limit(def.page_row_limit)
        .set_write_batch_size(def.page_row_limit.min(1024))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
        // Bloom filters after the row groups, next to the footer and page index, so one read
        // of the file's tail fetches all three.
        .set_bloom_filter_position(BloomFilterPosition::End);
    let ndv = rows.min(def.row_group_rows).max(1) as u64;
    for c in def.primary_key.iter().chain(&def.cluster_by) {
        let bloomable = schema
            .field_with_name(c)
            .is_ok_and(|f| !matches!(f.data_type(), DataType::Boolean));
        if bloomable {
            let p = ColumnPath::from(c.as_str());
            b = b
                .set_column_bloom_filter_enabled(p.clone(), true)
                .set_column_bloom_filter_fpp(p.clone(), 0.01)
                .set_column_bloom_filter_max_ndv(p, ndv);
        }
    }
    // Vector columns (lists of floats): floats neither compress nor repeat, and an exact scan
    // decodes every value, so they are stored plain and uncompressed. Their
    // pages are limited to 64 KiB (64 rows of 256 dimensions) rather than 1,024 rows: a search
    // fetches the vectors of its candidates to rescore them, and each fetch decodes a whole
    // page (D37). The leaf of a Delta list column is `<name>.list.element`.
    for f in schema.fields() {
        if let DataType::List(item) | DataType::LargeList(item) = f.data_type()
            && matches!(item.data_type(), DataType::Float32 | DataType::Float64)
        {
            let p = ColumnPath::new(vec![f.name().clone(), "list".into(), item.name().clone()]);
            b = b
                .set_column_compression(p.clone(), Compression::UNCOMPRESSED)
                .set_column_data_page_size_limit(p.clone(), VECTOR_PAGE_BYTES)
                .set_column_dictionary_enabled(p, false);
        }
    }
    // String and binary columns: pages of at most 64 KiB as well, so fetching a few rows of
    // long text (the hits of a search) decodes a few kilobytes, not up to 1,024 documents (D53).
    for f in schema.fields() {
        if matches!(
            f.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
        ) {
            b = b.set_column_data_page_size_limit(
                ColumnPath::from(f.name().as_str()),
                VECTOR_PAGE_BYTES,
            );
        }
    }
    b.build()
}

/// Encodes sorted rows as one Parquet file named from `clock`'s randomness.
pub(crate) fn encode(
    schema: &SchemaRef,
    def: &TableDef,
    rows: &RecordBatch,
    clock: &dyn Clock,
) -> Result<Encoded> {
    let props = writer_props(schema, def, rows.num_rows());
    let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props))?;
    w.write(rows)?;
    let body = w.into_inner()?;
    Ok(Encoded {
        name: format!("part-{}.parquet", random_name(clock)),
        body: Bytes::from(body),
        stats: stats::compute(rows)?,
        sorted_by: def.sort_columns(),
    })
}
