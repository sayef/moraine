//! The blob code of the documentation, compiled by `cargo clippy --all-targets` and runnable
//! with `cargo run -p morainedb --example blobs`.
//!
//! The functions hold the snippets of docs/guides/blobs.md, each as written on that page, and
//! `main` runs them against a table on local disk.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::TryStreamExt;
use moraine::blob::{self, BlobColumn, BlobRef};
use moraine::expr::{col, lit};
use moraine::{Database, Table, TableOptions};

/// The schema of the guide: an id, a file name and the file's bytes.
fn files_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        blob::field("body", true),
    ]))
}

async fn create(db: &Database) -> moraine::Result<Table> {
    let table = db
        .create_table(
            "files",
            files_schema(),
            TableOptions::default()
                .primary_key(["id"])
                // Values above 256 KiB (the default) are stored as objects of their own.
                .blob_inline_bytes(256 << 10),
        )
        .await?;
    Ok(table)
}

async fn write(table: &Table, schema: SchemaRef) -> moraine::Result<()> {
    let mut body = BlobColumn::new();
    body.push_bytes(b"a small note", Some("text/plain")); // stays in the row
    body.push_bytes(vec![0u8; 4 << 20], Some("application/octet-stream")); // its own object
    body.push_null();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["note.txt", "image.raw", "empty"])),
            body.finish(),
        ],
    )?;
    table.upsert(vec![batch]).await?;
    Ok(())
}

async fn read(table: &Table) -> moraine::Result<()> {
    let rows = table
        .scan()
        .filter(col("id").eq(lit(2i64)))
        .collect()
        .await?;
    for b in table.blobs(&rows[0], "body")?.into_iter().flatten() {
        println!("{} bytes, {:?}", b.size(), b.content_type());
        let header = b.range(0..64).await?; // one ranged GET
        let mut chunks = b.stream().await?; // one GET, a chunk at a time
        let mut total = 0;
        while let Some(chunk) = chunks.try_next().await? {
            total += chunk.len(); // the checksum is checked after the last chunk
        }
        let all = b.bytes().await?; // one GET, checksum verified
        assert_eq!((header.len(), total), (64, all.len()));
    }
    Ok(())
}

async fn reuse(table: &Table, schema: SchemaRef) -> moraine::Result<()> {
    // Read a reference from one row and write it into another: the write copies the object
    // on the server to a key of its own.
    let rows = table
        .scan()
        .filter(col("id").eq(lit(2i64)))
        .collect()
        .await?;
    let r: BlobRef = blob::refs(&rows[0], "body")?.remove(0).expect("a value");
    let mut body = BlobColumn::new();
    body.push_ref(&r);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["image copy.raw"])),
            body.finish(),
        ],
    )?;
    table.upsert(vec![batch]).await?;
    Ok(())
}

async fn staged(table: &Table, schema: SchemaRef) -> moraine::Result<()> {
    // A large value from a stream, in 16 MiB parts: memory stays at two parts.
    let parts = (0..40).map(|_| Ok::<_, std::io::Error>(Bytes::from(vec![1u8; 1 << 20])));
    let r = table
        .upload_blob(futures::stream::iter(parts), Some("video/mp4"))
        .await?;
    let mut body = BlobColumn::new();
    body.push_ref(&r); // committed as uploaded: same handle, within the commit deadline
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(StringArray::from(vec!["clip.mp4"])),
            body.finish(),
        ],
    )?;
    table.upsert(vec![batch]).await?;
    Ok(())
}

async fn collect(table: &Table) -> moraine::Result<()> {
    let report = table.vacuum().await?;
    println!(
        "blobs: {} referenced, {} deleted ({} bytes)",
        report.blobs_referenced, report.blobs_deleted, report.blob_bytes_deleted
    );
    let check = table.verify().await?;
    assert!(check.missing_blobs.is_empty());
    Ok(())
}

#[tokio::main]
async fn main() -> moraine::Result<()> {
    let _ = std::fs::remove_dir_all("moraine-blobs");
    let db = Database::builder("moraine-blobs").open().await?;
    let table = create(&db).await?;
    write(&table, files_schema()).await?;
    read(&table).await?;
    reuse(&table, files_schema()).await?;
    staged(&table, files_schema()).await?;
    collect(&table).await?;
    Ok(())
}
