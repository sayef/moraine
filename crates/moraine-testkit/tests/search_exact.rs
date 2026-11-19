//! Index queries on the exact route: top-k, ties, filters, row fetch by
//! offset and the exact-scan limit, with the toy `test-sum` kind (cases V1, V10, V13, V15).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::array::{Array, Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use moraine::expr::{col, lit};
use moraine::index::ExactScanPolicy;
use moraine::{Database, Error, IndexSpec, Table, TableOptions};
use moraine_testkit::toykind::SumKind;
use object_store::memory::InMemory;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("n", DataType::Int64, true),
    ]))
}

/// Four files of 300 rows; `n` repeats (ties across and within files) and is null every 9th.
async fn table(builder: moraine::DatabaseBuilder) -> (Database, Table, Vec<(i64, i64)>) {
    let db = builder.index_kind(Arc::new(SumKind)).open().await.unwrap();
    let t = db
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let mut rows = Vec::new();
    for f in 0..4i64 {
        let ids: Vec<i64> = (0..300).map(|i| f * 1000 + i).collect();
        let n: Vec<Option<i64>> = ids
            .iter()
            .map(|id| (id % 9 != 0).then_some((id * 7919) % 50))
            .collect();
        for (id, v) in ids.iter().zip(&n) {
            if let Some(v) = v {
                rows.push((*id, *v));
            }
        }
        let b = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(n)),
            ],
        )
        .unwrap();
        t.append(vec![b]).await.unwrap();
    }
    t.create_index(IndexSpec::new("by_n", "test-sum", &["n"]))
        .await
        .unwrap();
    (db, t, rows)
}

fn col_i64(b: &RecordBatch, name: &str) -> Vec<i64> {
    let a = b.column_by_name(name).unwrap();
    a.as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn scores(b: &RecordBatch) -> Vec<f32> {
    let a = b.column_by_name("_moraine_score").unwrap();
    a.as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn mem() -> moraine::DatabaseBuilder {
    Database::builder_for_store(Arc::new(InMemory::new()), "db")
}

#[tokio::test]
async fn exact_topk_ties_and_fetch() {
    let (_, t, rows) = table(mem()).await;
    let mut want: Vec<i64> = rows.iter().map(|r| r.1).collect();
    want.sort_unstable_by(|a, b| b.cmp(a));
    let r = t
        .search_custom("by_n", Vec::new())
        .limit(25)
        .collect()
        .await
        .unwrap();
    assert_eq!(r.stats.exact_files, 4);
    assert_eq!(r.stats.sidecar_files, 0);
    let got: Vec<i64> = scores(&r.batch).iter().map(|s| *s as i64).collect();
    assert_eq!(got, want[..25]);
    // Rows fetched by offset are the scored rows: their `n` equals the score.
    assert_eq!(col_i64(&r.batch, "n"), got);
    // V10/V15: equal scores are separate hits, ordered by file path, then offset.
    for w in r.hits.windows(2) {
        if w[0].score == w[1].score {
            assert!((w[0].file.as_str(), w[0].offset) < (w[1].file.as_str(), w[1].offset));
        }
    }
    let ids = col_i64(&r.batch, "id");
    for (id, n) in ids.iter().zip(col_i64(&r.batch, "n")) {
        assert!(rows.contains(&(*id, n)));
    }
}

#[tokio::test]
async fn k_zero_and_k_beyond_rows_v1() {
    let (_, t, rows) = table(mem()).await;
    let r = t
        .search_custom("by_n", Vec::new())
        .limit(0)
        .collect()
        .await
        .unwrap();
    assert_eq!(r.batch.num_rows(), 0);
    assert_eq!(r.batch.num_columns(), 3);
    let r = t
        .search_custom("by_n", Vec::new())
        .limit(100_000)
        .collect()
        .await
        .unwrap();
    assert_eq!(
        r.batch.num_rows(),
        rows.len(),
        "every non-null row, nulls never"
    );
    let s = scores(&r.batch);
    assert!(s.windows(2).all(|w| w[0] >= w[1]));
    // Projection: the selected columns, then the score.
    let r = t
        .search_custom("by_n", Vec::new())
        .select(["id"])
        .limit(3)
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = r
        .batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["id", "_moraine_score"]);
}

#[tokio::test]
async fn filtered_exact_search() {
    let (_, t, rows) = table(mem()).await;
    let mut want: Vec<i64> = rows
        .iter()
        .filter(|r| r.0 >= 2000 && r.0 % 2 == 0)
        .map(|r| r.1)
        .collect();
    want.sort_unstable_by(|a, b| b.cmp(a));
    let r = t
        .search_custom("by_n", Vec::new())
        .filter(col("id").gt_eq(lit(2000i64)))
        .filter((col("id") % lit(2i64)).eq(lit(0i64)))
        .limit(40)
        .collect()
        .await
        .unwrap();
    assert_eq!(col_i64(&r.batch, "n"), want[..40]);
    assert!(
        col_i64(&r.batch, "id")
            .iter()
            .all(|id| *id >= 2000 && id % 2 == 0)
    );
    // Files below id 2000 are pruned by statistics before anything is read (V9).
    assert_eq!(r.stats.skipped_files, 2);
    assert_eq!(r.stats.exact_files, 2);
    // A filter that matches nothing returns nothing.
    let r = t
        .search_custom("by_n", Vec::new())
        .filter(col("id").eq(lit(-5i64)))
        .collect()
        .await
        .unwrap();
    assert_eq!(r.batch.num_rows(), 0);
}

#[tokio::test]
async fn exact_scan_limit_v13() {
    let (_, t, _) = table(mem().max_exact_scan_rows(500, ExactScanPolicy::Error)).await;
    let e = t
        .search_custom("by_n", Vec::new())
        .collect()
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ExactScanLimit { limit: 500, .. }), "{e}");
    // A filter narrow enough fits under the limit.
    t.search_custom("by_n", Vec::new())
        .filter(col("id").lt(lit(300i64)))
        .collect()
        .await
        .unwrap();

    let (db, t, _) = table(mem().max_exact_scan_rows(500, ExactScanPolicy::Allow)).await;
    for _ in 0..2 {
        let r = t.search_custom("by_n", Vec::new()).collect().await.unwrap();
        assert_eq!(r.stats.exact_rows, 1200);
    }
    // The overrun is counted once per query.
    let m = db.index_metrics();
    assert_eq!(m.exact_limit_exceeded.load(Ordering::Relaxed), 2);
    assert_eq!(m.exact_rows.load(Ordering::Relaxed), 2400);
}

#[tokio::test]
async fn errors_name_the_problem() {
    let store = Arc::new(InMemory::new());
    let (_, t, _) = table(Database::builder_for_store(store.clone(), "db")).await;
    assert!(matches!(
        t.search_custom("nope", Vec::new()).collect().await,
        Err(Error::IndexNotFound(n)) if n.name == "nope"
    ));
    assert!(matches!(
        t.search_custom("by_n", Vec::new())
            .select(["zz"])
            .collect()
            .await,
        Err(Error::ColumnNotFound(_))
    ));
    let plain = Database::builder_for_store(store, "db")
        .open()
        .await
        .unwrap()
        .open_table("t")
        .await
        .unwrap();
    assert!(matches!(
        plain.search_custom("by_n", Vec::new()).collect().await,
        Err(Error::IndexKindNotRegistered(_))
    ));
}
