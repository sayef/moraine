//! Spike item 3: commit Parquet files that the library wrote itself, through `Add` actions and
//! `CommitBuilder`, then read them back with a second, fresh table handle.
//!
//! This is what index-before-commit needs: the file bytes exist (and a sidecar could be built
//! from them) before the commit makes the file visible.

use std::sync::Arc;

use deltalake::arrow::array::{Int64Array, RecordBatch, StringArray};
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add};
use deltalake::parquet::arrow::ArrowWriter;
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties};
use deltalake::protocol::{DeltaOperation, SaveMode};
use moraine_spike::*;
use object_store::ObjectStoreExt;
use object_store::path::Path;

const ROWS: usize = 50_000;

fn own_file() -> (Vec<u8>, RecordBatch) {
    let keys: Vec<String> = (0..ROWS).map(|i| format!("key-{i:08}")).collect();
    let batch = RecordBatch::try_new(
        spike_arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![7; ROWS])),
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(
                (0..ROWS as i64).map(|i| i * 3).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    // The design's data-file settings: 16,384-row groups, ~1 MiB pages, page index, bloom filter
    // on the key, Zstandard level 3.
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(16_384))
        .set_data_page_size_limit(1 << 20)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("key".into(), true)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    let mut buf = vec![];
    let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    (buf, batch)
}

async fn run(name: &str, backend: Backend) {
    let rel = "db/t/data";
    let table = create_spike_table(&backend, rel, &[]).await.unwrap();
    let (bytes, _) = own_file();
    let size = bytes.len();
    let file_name = format!("part-00000-{}-c000.zstd.parquet", uuid::Uuid::new_v4());

    // Upload: invisible until the commit.
    let (store, root) = backend.store().unwrap();
    let path = Path::from(format!("{}/{file_name}", join(&root, rel)));
    store.put(&path, bytes.into()).await.unwrap();
    let invisible = backend
        .open(rel)
        .await
        .unwrap()
        .get_file_uris()
        .unwrap()
        .count();

    let stats = serde_json::json!({
        "numRecords": ROWS,
        "minValues": {"writer": 7, "seq": 0, "key": "key-00000000", "value": 0},
        "maxValues": {"writer": 7, "seq": ROWS - 1, "key": format!("key-{:08}", ROWS - 1), "value": (ROWS - 1) * 3},
        "nullCount": {"writer": 0, "seq": 0, "key": 0, "value": 0},
    });
    let add = Add {
        path: file_name.clone(),
        size: size as i64,
        modification_time: chrono_ms(),
        data_change: true,
        stats: Some(stats.to_string()),
        ..Default::default()
    };
    let finalized = CommitBuilder::from(CommitProperties::default())
        .with_actions(vec![Action::Add(add)])
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await
        .unwrap();

    // Second reader: a fresh handle that never saw the write.
    let reader = backend.open(rel).await.unwrap();
    let files: Vec<_> = reader
        .snapshot()
        .unwrap()
        .log_data()
        .iter()
        .map(|f| (f.path().to_string(), f.size(), f.stats()))
        .collect();
    let batches = scan_all(&backend, rel).await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let (by_ws, _) = tally(&batches);
    let stats_back = files[0].2.clone().unwrap_or_default();
    println!(
        "RESULT item3 backend={name} file_bytes={size} visible_before_commit={invisible} committed_version={} \
         files_after={} path_round_trip={} size_round_trip={} stats_round_trip={} rows_read={rows}/{ROWS} distinct_seq={}",
        finalized.version(),
        files.len(),
        files[0].0 == file_name,
        files[0].1 == size as i64,
        stats_back.contains("\"numRecords\":50000"),
        by_ws.len()
    );
    assert_eq!(invisible, 0);
    assert_eq!(rows, ROWS);
    assert_eq!(files[0].0, file_name);
}

fn chrono_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test(flavor = "multi_thread")]
async fn item3_memory() {
    run("memory", Backend::memory()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item3_local() {
    let dir = tempfile::tempdir().unwrap();
    run("local", Backend::Local(dir.path().into())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn item3_s3_compatible() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item3")) else {
        println!("RESULT item3 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    run("s3-rustfs", b.clone()).await;
    b.clear().await.unwrap();
}
