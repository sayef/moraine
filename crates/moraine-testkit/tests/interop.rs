//! T11 (interoperability) and case S8: a table moraine wrote is read by delta-rs as a second
//! Delta reader with the same rows; a table delta-rs wrote, with a checkpoint, is read by
//! moraine; a table with an unsupported feature is refused.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::prelude::SessionContext;
use moraine::expr::{ScalarValue, col, lit};
use moraine::{Database, Error, TableOptions};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("n", DataType::Int64, true),
    ]))
}

fn batch(lo: i64, hi: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                (lo..hi).map(|i| format!("key {i:05} /%")),
            )),
            Arc::new(Int64Array::from_iter_values(lo..hi)),
        ],
    )
    .unwrap()
}

fn sum_n(batches: &[RecordBatch]) -> (usize, i64) {
    let mut rows = 0;
    let mut sum = 0;
    for b in batches {
        rows += b.num_rows();
        let n = b.column_by_name("n").unwrap().as_primitive::<Int64Type>();
        sum += n.iter().flatten().sum::<i64>();
    }
    (rows, sum)
}

#[tokio::test]
async fn delta_rs_reads_what_moraine_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let t = db
        .create_table(
            "docs",
            schema(),
            TableOptions::default()
                .primary_key(["k"])
                .enforce_primary_key(false),
        )
        .await
        .unwrap();
    for i in 0..3 {
        t.append(vec![batch(i * 100, i * 100 + 100)]).await.unwrap();
    }
    let ours = sum_n(&t.scan().collect().await.unwrap());
    assert_eq!(ours, (300, (0..300).sum()));

    let url = url::Url::from_directory_path(dir.path().join("docs/data")).unwrap();
    let table = deltalake::open_table(url).await.unwrap();
    assert_eq!(table.version(), Some(3));
    let ctx = SessionContext::new();
    let theirs = ctx
        .read_table(table.table_provider().await.unwrap())
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(sum_n(&theirs), ours);

    // And moraine's filter and lookup agree with the data.
    let hit = t
        .get(&[ScalarValue::Utf8(Some("key 00150 /%".into()))])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sum_n(&[hit]), (1, 150));
    let few = t
        .scan()
        .filter(col("n").gt_eq(lit(290i64)))
        .select(["n"])
        .collect()
        .await
        .unwrap();
    assert_eq!(sum_n(&few), (10, (290..300).sum()));
}

/// delta-rs reads a checkpoint moraine wrote (D47), with the log before it removed as log
/// cleanup would, and moraine opens the same table from it.
#[tokio::test]
async fn delta_rs_reads_a_moraine_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let t = db
        .create_table("docs", schema(), TableOptions::default())
        .await
        .unwrap();
    for i in 0..5 {
        t.append(vec![batch(i * 10, i * 10 + 10)]).await.unwrap();
    }
    t.compact(&moraine::CompactionPolicy::full()).await.unwrap();
    let cp = t.checkpoint().await.unwrap().expect("a checkpoint");
    for i in 5..7 {
        t.append(vec![batch(i * 10, i * 10 + 10)]).await.unwrap();
    }
    let log = dir.path().join("docs/data/_delta_log");
    for v in 0..cp {
        std::fs::remove_file(log.join(format!("{v:020}.json"))).unwrap();
    }
    let ours = sum_n(&t.scan().collect().await.unwrap());
    assert_eq!(ours, (70, (0..70).sum()));
    let url = url::Url::from_directory_path(dir.path().join("docs/data")).unwrap();
    let table = deltalake::open_table(url).await.unwrap();
    assert_eq!(table.version(), Some(cp + 2));
    let ctx = SessionContext::new();
    let theirs = ctx
        .read_table(table.table_provider().await.unwrap())
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(sum_n(&theirs), ours);
    let fresh = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap()
        .open_table("docs")
        .await
        .unwrap();
    assert_eq!(sum_n(&fresh.scan().collect().await.unwrap()), ours);
    assert_eq!(fresh.snapshot_in_hand().files()[0].level, 1);
}

#[tokio::test]
async fn moraine_reads_a_delta_rs_table_with_a_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ext/data");
    std::fs::create_dir_all(&path).unwrap();
    let url = url::Url::from_directory_path(&path).unwrap();
    let cfg: HashMap<String, Option<String>> =
        HashMap::from([("delta.checkpointInterval".into(), Some("5".into()))]);
    let mut table = deltalake::DeltaTable::try_from_url(url.clone())
        .await
        .unwrap()
        .create()
        .with_columns(vec![
            deltalake::kernel::StructField::new(
                "k",
                deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                false,
            ),
            deltalake::kernel::StructField::new(
                "n",
                deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Long),
                true,
            ),
        ])
        .with_configuration(cfg)
        .await
        .unwrap();
    for i in 0..12 {
        table = table.write(vec![batch(i * 10, i * 10 + 10)]).await.unwrap();
    }
    let log: Vec<String> = std::fs::read_dir(path.join("_delta_log"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        log.iter().any(|n| n.contains("checkpoint.parquet")),
        "delta-rs wrote no checkpoint: {log:?}"
    );
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let t = db.open_table("ext").await.unwrap();
    assert_eq!(t.version(), 12);
    assert_eq!(
        sum_n(&t.scan().collect().await.unwrap()),
        (120, (0..120).sum())
    );
    // moraine appends on top; delta-rs still reads the whole table.
    t.append(vec![batch(120, 130)]).await.unwrap();
    let table = deltalake::open_table(url).await.unwrap();
    let ctx = SessionContext::new();
    let all = ctx
        .read_table(table.table_provider().await.unwrap())
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(sum_n(&all), (130, (0..130).sum()));
}

/// Spike item 11 / case S8: a table with deletion vectors (reader feature) is refused.
#[tokio::test]
async fn unsupported_feature_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("dv/data/_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let schema = r#"{"type":"struct","fields":[{"name":"k","type":"string","nullable":false,"metadata":{}}]}"#;
    let entry = format!(
        "{}\n{}\n",
        serde_json::json!({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]}}),
        serde_json::json!({"metaData": {"id": "x", "format": {"provider": "parquet", "options": {}},
            "schemaString": schema, "partitionColumns": [], "configuration": {}}}),
    );
    std::fs::write(log.join(format!("{:020}.json", 0)), entry).unwrap();
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    assert!(matches!(
        db.open_table("dv").await,
        Err(Error::UnsupportedTableFeature(_))
    ));
    assert!(matches!(
        db.open_table("missing").await,
        Err(Error::TableNotFound(_))
    ));
}

/// T11 for rewrites: after upserts, deletes, an update and an added column, delta-rs reads
/// the same rows as moraine (remove actions, a grown schema, files without the new column).
#[tokio::test]
async fn delta_rs_reads_rewrites_and_an_added_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let t = db
        .create_table("docs", schema(), TableOptions::default().primary_key(["k"]))
        .await
        .unwrap();
    t.upsert(vec![batch(0, 100)]).await.unwrap();
    t.upsert(vec![batch(50, 150)]).await.unwrap();
    t.add_column(Field::new("extra", DataType::Int64, true))
        .await
        .unwrap();
    t.delete(col("n").lt(lit(10i64))).await.unwrap();
    t.delete_keys(&[vec![ScalarValue::Utf8(Some("key 00140 /%".into()))]])
        .await
        .unwrap();
    t.update(
        col("n").gt_eq(lit(100i64)),
        vec![("extra".into(), lit(7i64))],
    )
    .await
    .unwrap();
    let ours = t.scan().collect().await.unwrap();
    let want_n: i64 = (10..150).filter(|n| *n != 140).sum();
    assert_eq!(sum_n(&ours), (139, want_n));

    let url = url::Url::from_directory_path(dir.path().join("docs/data")).unwrap();
    let table = deltalake::open_table(url).await.unwrap();
    assert_eq!(table.version(), Some(t.version()));
    let ctx = SessionContext::new();
    let theirs = ctx
        .read_table(table.table_provider().await.unwrap())
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(sum_n(&theirs), (139, want_n));
    let extra = |bs: &[RecordBatch]| -> (usize, i64) {
        let mut set = 0;
        let mut sum = 0;
        for b in bs {
            let e = b
                .column_by_name("extra")
                .unwrap()
                .as_primitive::<Int64Type>();
            set += e.iter().flatten().count();
            sum += e.iter().flatten().sum::<i64>();
        }
        (set, sum)
    };
    assert_eq!(extra(&ours), (49, 49 * 7));
    assert_eq!(extra(&theirs), extra(&ours));
}

/// D32: delta-rs sees the `txn` actions of moraine's caller transaction ids, moraine sees those
/// delta-rs writes, and caller ids (written without `lastUpdated`) survive a checkpoint whatever
/// the table's transaction retention, while an id that delta-rs considers expired does not.
#[tokio::test]
async fn caller_transaction_ids_interoperate() {
    use deltalake::kernel::Transaction;
    use deltalake::kernel::transaction::CommitProperties;
    use moraine::WriteOutcome;

    let dir = tempfile::tempdir().unwrap();
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let opts = TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false);
    let t = db.create_table("docs", schema(), opts).await.unwrap();
    for v in 1..=3 {
        let o = t
            .txn("wal", v)
            .append(vec![batch(v * 10, v * 10 + 10)])
            .await
            .unwrap();
        assert!(o.is_committed());
    }

    let url = url::Url::from_directory_path(dir.path().join("docs/data")).unwrap();
    let mut table = deltalake::open_table(url.clone()).await.unwrap();
    let txn_version = |table: &deltalake::DeltaTable, app: &'static str| {
        let snap = table.snapshot().unwrap().clone();
        let store = table.log_store();
        async move { snap.transaction_version(store.as_ref(), app).await.unwrap() }
    };
    assert_eq!(txn_version(&table, "wal").await, Some(3));

    // delta-rs commits ids of its own: one current, one whose lastUpdated is long past the
    // table's retention (moraine sets it to the grace period, D14).
    table = table
        .write(vec![batch(100, 105)])
        .with_commit_properties(
            CommitProperties::default()
                .with_application_transaction(Transaction::new("ext", 9))
                .with_application_transaction(Transaction::new_with_last_update("old", 1, Some(0))),
        )
        .await
        .unwrap();
    assert_eq!(t.last_txn_version("ext").await.unwrap(), Some(9));
    assert_eq!(
        t.txn("ext", 9).append(vec![batch(0, 1)]).await.unwrap(),
        WriteOutcome::AlreadyApplied {
            version: 4,
            txn_version: 9
        }
    );

    deltalake::checkpoints::create_checkpoint(&table, None)
        .await
        .unwrap();
    let table = deltalake::open_table(url).await.unwrap();
    assert_eq!(txn_version(&table, "wal").await, Some(3));
    assert_eq!(txn_version(&table, "ext").await, Some(9));
    assert_eq!(txn_version(&table, "old").await, None);

    // A new process loads from the checkpoint and still finds the ids.
    let db = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let t = db.open_table("docs").await.unwrap();
    assert_eq!(t.version(), 4);
    assert_eq!(t.last_txn_version("wal").await.unwrap(), Some(3));
    assert_eq!(t.last_txn_version("ext").await.unwrap(), Some(9));
    assert_eq!(
        t.txn("wal", 2).append(vec![batch(0, 1)]).await.unwrap(),
        WriteOutcome::AlreadyApplied {
            version: 4,
            txn_version: 3
        }
    );
    assert!(
        t.txn("wal", 4)
            .append(vec![batch(200, 201)])
            .await
            .unwrap()
            .is_committed()
    );
    assert_eq!(sum_n(&t.scan().collect().await.unwrap()).0, 36);
}
