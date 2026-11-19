//! The maintenance code of the documentation, compiled by `cargo clippy --all-targets` and
//! runnable with `cargo run -p morainedb --example maintenance`.
//!
//! The functions hold the snippets of docs/guides/maintenance.md, each as written on that page,
//! and `main` runs them against a table on local disk.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use moraine::{CompactionPolicy, Database, Error, MaintainOptions, Replace, Table, TableOptions};

fn batch(schema: &Arc<Schema>, from: i64, n: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(from..from + n)),
            Arc::new(StringArray::from_iter_values(
                (from..from + n).map(|i| format!("event {i}")),
            )),
        ],
    )
    .expect("batch")
}

#[tokio::main]
async fn main() -> moraine::Result<()> {
    let _ = std::fs::remove_dir_all("moraine-maintenance");
    let db = Database::builder("moraine-maintenance").open().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("what", DataType::Utf8, false),
    ]));
    let events = db
        .create_table("events", schema.clone(), TableOptions::default())
        .await?;
    for i in 0..8 {
        events.append(vec![batch(&schema, i * 10, 10)]).await?;
    }
    one_pass(&events).await?;
    compact_everything(&events).await?;
    pins(&events).await?;
    fenced_replace(&events, batch(&schema, 1_000, 3)).await?;
    queued(&events, batch(&schema, 2_000, 3)).await?;
    check(&events).await?;
    let task = in_the_background(&events);
    task.stop(&events).await?;
    Ok(())
}

/// "One pass".
async fn one_pass(table: &Table) -> moraine::Result<()> {
    let report = table.maintain(&MaintainOptions::default()).await?;
    if let Some(c) = &report.compaction {
        println!(
            "compaction: {} files into {}",
            c.files_removed, c.files_added
        );
    }
    if let Some(l) = &report.log {
        println!("checkpoint: {:?}", l.checkpoint);
    }
    if let Some(v) = &report.vacuum {
        println!(
            "vacuum: {} files, {} bytes",
            v.files_deleted, v.bytes_deleted
        );
    }
    println!("skipped (lease held elsewhere): {:?}", report.skipped);
    Ok(())
}

/// "In the background".
fn in_the_background(table: &Table) -> moraine::MaintenanceTask {
    let task =
        table.spawn_maintenance(MaintainOptions::default().interval(Duration::from_secs(60)));
    // ... the application runs; `task.metrics()` counts passes and failures ...
    task
}

/// "Compaction".
async fn compact_everything(table: &Table) -> moraine::Result<()> {
    let r = table.compact(&CompactionPolicy::full()).await?;
    println!("{} steps, {} bytes written", r.steps, r.bytes_written);
    for f in table.snapshot().await?.files() {
        println!(
            "{} level {} ({} rows)",
            f.path,
            f.level,
            f.rows.unwrap_or(0)
        );
    }
    Ok(())
}

/// "Pins".
async fn pins(table: &Table) -> moraine::Result<()> {
    let v = table.refresh().await?;
    table.pin("before-cleanup", v).await?;
    table
        .delete(moraine::expr::col("id").lt(moraine::expr::lit(5i64)))
        .await?;
    let old = table.snapshot_at_pin("before-cleanup").await?;
    println!(
        "version {} still has {} rows",
        old.version(),
        old.num_rows()
    );
    println!("pins: {:?}", table.pins()?);
    table.unpin("before-cleanup").await?;
    Ok(())
}

/// "Leases" and "Replacing files".
async fn fenced_replace(table: &Table, folded: RecordBatch) -> moraine::Result<()> {
    let ttl = Duration::from_secs(60);
    let Some(lease) = table.try_lease("fold", ttl).await? else {
        return Ok(()); // another client is folding
    };
    let snap = table.snapshot().await?;
    let small: Vec<String> = snap
        .files()
        .into_iter()
        .filter(|f| f.rows.is_some_and(|r| r < 1_000))
        .map(|f| f.path)
        .collect();
    let r = Replace::new()
        .remove_all(small)
        .add_rows(vec![folded])
        .tag("kind", "folded")
        .fenced(&lease);
    match table.replace_files(r).await {
        Ok(c) => println!("replaced in version {}", c.version),
        Err(Error::FilesChanged(p)) => println!("{p} changed meanwhile; plan again"),
        Err(Error::Fenced { .. }) => println!("the lease moved on; stop"),
        Err(e) => return Err(e),
    }
    table.release_lease(&lease).await
}

/// "Queued writes".
async fn queued(table: &Table, rows: RecordBatch) -> moraine::Result<()> {
    let ticket = table.submit(vec![rows]).await?;
    // Any process: commits what the inbox holds, under the `ingest` lease.
    let r = table.ingest(true).await?;
    println!(
        "{} batches, {} rows in version {:?}",
        r.batches, r.rows, r.version
    );
    assert!(table.is_ingested(&ticket).await?);
    Ok(())
}

/// "Verify and repair".
async fn check(table: &Table) -> moraine::Result<()> {
    let r = table.verify().await?;
    if !r.is_ok() {
        println!("missing: {:?}", r.missing_files);
        let fixed = table.repair().await?;
        println!(
            "removed {:?}, {} rows lost",
            fixed.files_removed, fixed.rows_lost
        );
    }
    Ok(())
}
