//! Rows of one commit keep their append order when their sort columns are equal (the sort of
//! data files is stable; `TableOptions::cluster_by`). Readers such as kv0's newest-wins logic
//! depend on it.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use moraine::{Database, TableOptions};
use object_store::memory::InMemory;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ]))
}

fn batch(runs: &[&str], seqs: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(runs.to_vec())),
            Arc::new(Int64Array::from(seqs.to_vec())),
        ],
    )
    .unwrap()
}

/// `seq` of the rows of `run`, in the order a scan returns them.
async fn order(t: &moraine::Table, run: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in t.scan().collect().await.unwrap() {
        let r = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            if r.value(i) == run {
                out.push(s.value(i));
            }
        }
    }
    out
}

#[tokio::test]
async fn equal_cluster_keys_keep_append_order() {
    let db = Database::builder_for_store(Arc::new(InMemory::new()), "db")
        .small_table_bytes(0)
        .open()
        .await
        .unwrap();
    let opts = TableOptions::default().cluster_by(["run"]);
    let t = db.create_table("t", schema(), opts).await.unwrap();
    // One append of two batches; `seq` descends, so a sort that used it would reverse them.
    let a = batch(&["b", "a", "b", "a", "b"], &[90, 80, 70, 60, 50]);
    let b = batch(&["a", "b", "a"], &[40, 30, 20]);
    t.append(vec![a, b]).await.unwrap();
    assert_eq!(order(&t, "a").await, vec![80, 60, 40, 20]);
    assert_eq!(order(&t, "b").await, vec![90, 70, 50, 30]);
    // A second commit's rows follow the first commit's.
    t.append(vec![batch(&["a", "a"], &[10, 5])]).await.unwrap();
    assert_eq!(order(&t, "a").await, vec![80, 60, 40, 20, 10, 5]);
}

/// Callers merged into one group commit keep their arrival order too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_commit_keeps_caller_order() {
    let db = Database::builder_for_store(Arc::new(InMemory::new()), "db")
        .small_table_bytes(0)
        .group_commit_linger(std::time::Duration::from_millis(50))
        .open()
        .await
        .unwrap();
    let opts = TableOptions::default().cluster_by(["run"]);
    let t = db.create_table("t", schema(), opts).await.unwrap();
    let mut versions = Vec::new();
    let mut tasks = Vec::new();
    for c in 0..4i64 {
        let t = t.clone();
        tasks.push(tokio::spawn(async move {
            // Callers enqueue in this order: each waits for the one before it to be queued.
            tokio::time::sleep(std::time::Duration::from_millis(5 * c as u64)).await;
            t.append(vec![batch(&["x", "x"], &[100 - 2 * c, 99 - 2 * c])])
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        versions.push(task.await.unwrap().version);
    }
    let got = order(&t, "x").await;
    // Within each commit, callers' rows keep arrival order; commits follow version order.
    let mut want: Vec<(u64, i64)> = Vec::new();
    for (c, v) in versions.iter().enumerate() {
        let c = c as i64;
        want.push((*v, 100 - 2 * c));
        want.push((*v, 99 - 2 * c));
    }
    want.sort_by_key(|(v, _)| *v);
    assert_eq!(got, want.into_iter().map(|(_, s)| s).collect::<Vec<_>>());
}
