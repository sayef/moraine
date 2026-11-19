//! The snippets of docs/concepts/consistency.md added with D43 and D44 (background refresh, the
//! snapshot in hand and a version's file list), compiled by `cargo clippy --all-targets` and
//! runnable with `cargo run -p morainedb --example consistency`. Each function holds a snippet
//! as written on that page; `main` runs them against a local directory.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use moraine::{Database, Table, TableOptions};

#[tokio::main]
async fn main() -> moraine::Result<()> {
    let _ = std::fs::remove_dir_all("moraine-consistency");
    let db = Database::builder("moraine-consistency").open().await?;
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let events = db
        .create_table("events", schema.clone(), TableOptions::default())
        .await?;
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .expect("columns match the schema");
    own_writes(&events, batch).await?;
    background("moraine-consistency").await
}

// "Background refresh"
async fn background(location: &str) -> moraine::Result<()> {
    use std::time::Duration;

    use moraine::{Consistency, Database};

    let db = Database::builder(location)
        .consistency(Consistency::Background {
            interval: Duration::from_millis(500),
        })
        .open()
        .await?;
    let table = db.open_table("events").await?;
    let m = table.refresh_metrics();
    println!(
        "{} refreshes, {} new versions",
        m.refreshes.load(std::sync::atomic::Ordering::Relaxed),
        m.new_versions.load(std::sync::atomic::Ordering::Relaxed)
    );
    Ok(())
}

// "Your own writes"
async fn own_writes(table: &Table, batch: RecordBatch) -> moraine::Result<()> {
    let committed = table.append(vec![batch]).await?;
    let snap = table.snapshot_in_hand();
    assert!(snap.version() >= committed.version);
    for f in snap.files() {
        println!(
            "{} ({} bytes, {:?} rows, added in {:?})",
            f.path, f.size, f.rows, f.added_in
        );
    }
    Ok(())
}
