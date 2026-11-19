//! The program of the getting-started page (docs/getting-started.md), kept identical to it so
//! the page's code is compiled by `cargo clippy --all-targets` and can be run:
//! `cargo run -p morainedb --example getting_started`.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;
use moraine::expr::{ScalarValue, col, lit};
use moraine::{Database, TableOptions};

#[tokio::main]
async fn main() -> moraine::Result<()> {
    // A database is a prefix of an object store: a local directory here, `s3://bucket/prefix`
    // in production. Start from an empty directory each run.
    let _ = std::fs::remove_dir_all("moraine-data");
    let db = Database::builder("moraine-data").open().await?;

    // An append-only table of readings, clustered by device so a read of one device touches
    // few files and pages.
    let readings = Arc::new(Schema::new(vec![
        Field::new("device", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("celsius", DataType::Float64, false),
    ]));
    let events = db
        .create_table(
            "readings",
            readings.clone(),
            TableOptions::default().cluster_by(["device"]),
        )
        .await?;

    let batch = |devices: &[&str], ts: &[i64], celsius: &[f64]| {
        RecordBatch::try_new(
            readings.clone(),
            vec![
                Arc::new(StringArray::from(devices.to_vec())),
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(Float64Array::from(celsius.to_vec())),
            ],
        )
        .expect("columns match the schema")
    };

    // Append: one data PUT and one conditional log PUT per commit.
    let v = events
        .append(vec![batch(
            &["boiler", "attic", "boiler"],
            &[1, 1, 2],
            &[61.5, 14.0, 63.0],
        )])
        .await?;
    println!("appended at version {}", v.version);

    // Scan with a filter and a projection.
    let hot = events
        .scan()
        .filter(col("device").eq(lit("boiler")))
        .select(["ts", "celsius"])
        .collect()
        .await?;
    print_batches(&hot).expect("printable");

    // A keyed table: upsert replaces rows by primary key, get reads one key.
    let devices = Arc::new(Schema::new(vec![
        Field::new("device", DataType::Utf8, false),
        Field::new("room", DataType::Utf8, true),
    ]));
    let registry = db
        .create_table(
            "devices",
            devices.clone(),
            TableOptions::default().primary_key(["device"]),
        )
        .await?;
    let rows = |d: &[&str], r: &[&str]| {
        RecordBatch::try_new(
            devices.clone(),
            vec![
                Arc::new(StringArray::from(d.to_vec())),
                Arc::new(StringArray::from(r.to_vec())),
            ],
        )
        .expect("columns match the schema")
    };
    registry
        .upsert(vec![rows(&["boiler", "attic"], &["cellar", "loft"])])
        .await?;
    registry
        .upsert(vec![rows(&["attic"], &["roof space"])])
        .await?;
    let attic = registry.get(&[ScalarValue::from("attic")]).await?;
    print_batches(&attic.into_iter().collect::<Vec<_>>()).expect("printable");

    // Exactly once: a write tagged (app id, version) applies once, however often it is sent.
    let late = batch(&["attic"], &[2], &[13.5]);
    for attempt in 1..=2 {
        let outcome = events.txn("ingest-1", 7).append(vec![late.clone()]).await?;
        if outcome.is_committed() {
            println!(
                "attempt {attempt}: committed at version {}",
                outcome.version()
            );
        } else {
            println!(
                "attempt {attempt}: already applied in version {}",
                outcome.version()
            );
        }
    }
    println!("{} readings", events.scan().count().await?);
    Ok(())
}
