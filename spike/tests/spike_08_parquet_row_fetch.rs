#![allow(deprecated)] // ParquetObjectReader is deprecated in parquet 59; see DECISIONS.md.
//! Spike item 8: Parquet data files with the design's settings (16,384-row groups, ~1 MiB pages,
//! page index, bloom filter on the key, Zstandard 3 except the vector column), read by row
//! offset through `ParquetObjectReader` with a `RowSelection`. Fetches 10 scattered rows from a
//! 1,000,000-row file and counts GET requests and bytes.

use std::sync::Arc;
use std::time::Instant;

use deltalake::arrow::array::{
    Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use deltalake::parquet::arrow::async_reader::ParquetObjectReader;
use deltalake::parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties};
use deltalake::parquet::schema::types::ColumnPath;
use futures::TryStreamExt;
use moraine_spike::*;
use object_store::ObjectStoreExt;
use object_store::path::Path;

const ROWS: usize = 1_000_000;
const DIM: i32 = 32;
const OFFSETS: [usize; 10] = [
    3, 98_765, 123_456, 250_001, 400_000, 512_345, 640_000, 777_777, 901_234, 999_999,
];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            true,
        ),
    ]))
}

fn batch(start: usize, len: usize) -> RecordBatch {
    let ids: Vec<String> = (start..start + len).map(|i| format!("id-{i:09}")).collect();
    let titles: Vec<String> = (start..start + len)
        .map(|i| format!("synthetic title number {i} about object storage"))
        .collect();
    let mut x = (start as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let vals: Vec<f32> = (0..len * DIM as usize)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % 10_000) as f32 / 10_000.0
        })
        .collect();
    let vec = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        DIM,
        Arc::new(Float32Array::from(vals)),
        None,
    )
    .unwrap();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int64Array::from(
                (start as i64..(start + len) as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(titles)),
            Arc::new(vec),
        ],
    )
    .unwrap()
}

fn write_file(page_rows: Option<usize>) -> Vec<u8> {
    let mut props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(16_384))
        .set_data_page_size_limit(1 << 20)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_bloom_filter_enabled(ColumnPath::from("id"), true)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_column_compression(
            ColumnPath::new(vec!["embedding".into(), "list".into(), "item".into()]),
            Compression::UNCOMPRESSED,
        )
        // Real embeddings do not repeat values; keep the synthetic ones out of dictionary encoding.
        .set_column_dictionary_enabled(
            ColumnPath::new(vec!["embedding".into(), "list".into(), "item".into()]),
            false,
        );
    if let Some(n) = page_rows {
        props = props.set_data_page_row_count_limit(n);
    }
    let props = props.build();
    let mut buf = Vec::with_capacity(200 << 20);
    let mut w = ArrowWriter::try_new(&mut buf, schema(), Some(props)).unwrap();
    for start in (0..ROWS).step_by(65_536) {
        w.write(&batch(start, (ROWS - start).min(65_536))).unwrap();
    }
    w.close().unwrap();
    buf
}

async fn fetch(
    counting: &Arc<CountingStore>,
    path: &Path,
    size: u64,
    page_index: bool,
    projection: &[usize],
    restrict_row_groups: bool,
) -> (Vec<i64>, u64, u64, u64, u64, f64) {
    counting.take();
    let t0 = Instant::now();
    let reader = ParquetObjectReader::new(counting.clone(), path.clone()).with_file_size(size);
    let opts = ArrowReaderOptions::new().with_page_index(page_index);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, opts)
        .await
        .unwrap();
    let (meta_gets, meta_bytes) = counting.take();
    let md = builder.metadata().clone();
    let mask = ProjectionMask::roots(builder.parquet_schema(), projection.iter().copied());
    let builder =
        if restrict_row_groups {
            // Only the row groups that hold the wanted offsets; the selection is relative to them.
            let mut groups = vec![];
            let mut first_row = vec![];
            let mut acc = 0usize;
            for (i, rg) in md.row_groups().iter().enumerate() {
                let n = rg.num_rows() as usize;
                if OFFSETS.iter().any(|o| *o >= acc && *o < acc + n) {
                    groups.push(i);
                    first_row.push(acc);
                }
                acc += n;
            }
            let mut ranges = vec![];
            let mut base = 0usize;
            for (g, start) in groups.iter().zip(&first_row) {
                let n = md.row_group(*g).num_rows() as usize;
                for o in OFFSETS.iter().filter(|o| **o >= *start && **o < start + n) {
                    ranges.push(base + (o - start)..base + (o - start) + 1);
                }
                base += n;
            }
            builder.with_row_groups(groups).with_row_selection(
                RowSelection::from_consecutive_ranges(ranges.into_iter(), base),
            )
        } else {
            let ranges = OFFSETS.iter().map(|o| *o..*o + 1);
            builder.with_row_selection(RowSelection::from_consecutive_ranges(ranges, ROWS))
        };
    let batches: Vec<RecordBatch> = builder
        .with_projection(mask)
        .build()
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let (gets, bytes) = counting.take();
    let mut ns = vec![];
    for b in &batches {
        if let Some(c) = b.column_by_name("n") {
            let c = c.as_any().downcast_ref::<Int64Array>().unwrap();
            ns.extend((0..c.len()).map(|i| c.value(i)));
        } else {
            ns.extend(std::iter::repeat_n(-1, b.num_rows()));
        }
    }
    (
        ns,
        meta_gets,
        meta_bytes,
        gets,
        bytes,
        t0.elapsed().as_secs_f64() * 1e3,
    )
}

async fn run(name: &str, backend: Backend) {
    for page_rows in [None, Some(1024)] {
        run_variant(name, &backend, page_rows).await;
    }
}

async fn run_variant(name: &str, backend: &Backend, page_rows: Option<usize>) {
    let name = format!("{name} page_row_limit={page_rows:?}");
    let t0 = Instant::now();
    let bytes = write_file(page_rows);
    let write_ms = t0.elapsed().as_secs_f64() * 1e3;
    let size = bytes.len() as u64;
    let (store, root) = backend.store().unwrap();
    let path = Path::from(join(&root, "db/t/data/part-big.parquet"));
    store.put(&path, bytes.into()).await.unwrap();
    let counting = Arc::new(CountingStore::new(store.clone()));

    let reader = ParquetObjectReader::new(counting.clone(), path.clone()).with_file_size(size);
    let b = ParquetRecordBatchStreamBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new().with_page_index(true),
    )
    .await
    .unwrap();
    let md = b.metadata().clone();
    let row_groups = md.num_row_groups();
    let has_page_index = md.offset_index().is_some() && md.column_index().is_some();
    let col_sizes: Vec<(String, i64)> = (0..md.row_group(0).num_columns())
        .map(|c| {
            let total: i64 = md
                .row_groups()
                .iter()
                .map(|rg| rg.column(c).compressed_size())
                .sum();
            (md.row_group(0).column(c).column_path().string(), total)
        })
        .collect();
    let pages_rg0: Vec<usize> = md
        .offset_index()
        .map(|oi| oi[0].iter().map(|c| c.page_locations().len()).collect())
        .unwrap_or_default();
    let codecs: Vec<String> = md
        .row_group(0)
        .columns()
        .iter()
        .map(|c| format!("{:?}", c.compression()))
        .collect();
    counting.take();
    // Bloom filter on the key column of the row group holding row 512,345.
    let rg = 512_345 / 16_384;
    let mut b = b;
    let sbbf = b.get_row_group_column_bloom_filter(rg, 0).await.unwrap();
    let (bloom_gets, bloom_bytes) = counting.take();
    let (present, absent) = match &sbbf {
        Some(f) => (f.check(&"id-000512345"), f.check(&"id-999999999")),
        None => (false, false),
    };

    let all = [0, 1, 2, 3];
    let expect: Vec<i64> = OFFSETS.iter().map(|o| *o as i64).collect();
    println!(
        "RESULT item8 backend={name} rows={ROWS} dim={DIM} file_bytes={size} write_ms={write_ms:.0} row_groups={row_groups} \
         page_index={has_page_index} pages_in_rg0_per_column={pages_rg0:?} codecs={codecs:?} column_bytes={col_sizes:?}"
    );
    println!(
        "RESULT item8 backend={name} bloom: gets={bloom_gets} bytes={bloom_bytes} present_key={present} absent_key_says_maybe={absent}"
    );
    for (label, page_index, proj, restrict) in [
        ("all_columns page_index", true, &all[..], false),
        (
            "all_columns page_index row_groups_restricted",
            true,
            &all[..],
            true,
        ),
        ("all_columns no_page_index", false, &all[..], false),
        ("id_and_n page_index", true, &[0, 1][..], true),
    ] {
        let (ns, mg, mb, g, by, ms) =
            fetch(&counting, &path, size, page_index, proj, restrict).await;
        println!(
            "RESULT item8 backend={name} fetch10 [{label}]: metadata gets={mg} bytes={mb}; data gets={g} bytes={by}; \
             rows_ok={} elapsed_ms={ms:.1}",
            ns == expect
        );
        assert_eq!(ns, expect);
    }
    store.delete(&path).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn item8_local() {
    let dir = tempfile::tempdir().unwrap();
    run("local", Backend::Local(dir.path().into())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item8_s3_compatible() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item8")) else {
        println!("RESULT item8 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    run("s3-rustfs", b.clone()).await;
    b.clear().await.unwrap();
}
