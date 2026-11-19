# Getting started

This page builds a small program that creates two tables on local disk, appends, upserts, scans,
reads one key, and makes a write exactly-once. It runs as written; the output is at the end.
Moving it to S3 changes one line. A second, shorter program adds a vector index and searches it.

## Requirements

- Rust 1.95 or newer (the minimum supported version, set by delta-rs).
- A C++ compiler only if you turn on the `vector` feature (usearch is built from source).

## Add the dependencies

moraine is on crates.io as a beta. The crate is `morainedb` and the library is `moraine`. Use
the same `arrow` version as moraine, or the record batch types will not match. Cargo picks a beta
only when you name it, so give the full version:

```sh
cargo add morainedb@0.1.0-beta.2
```

```toml
[dependencies]
morainedb = "0.1.0-beta.2"
arrow = "=59.3.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Cargo features of `morainedb`, all off by default:

| Feature | Adds |
|---|---|
| `aws-sdk` | Credentials from the AWS SDK's default chain (profiles, SSO, `credential_process`) and the bucket's own region for `s3://` databases |
| `vector` | The `vec-hnsw` vector index (usearch) |
| `turbovec` | The `vec-tq` vector index (TurboQuant through turbovec) |
| `fts` | The `fts` full-text index (tantivy), `search_text`, and with a vector kind `search_hybrid` |

Turn features on with `cargo add`, for example
`cargo add morainedb@0.1.0-beta.2 --features vector,fts`. The API reference for every type is on
[docs.rs](https://docs.rs/morainedb).

## The program

`src/main.rs`:

```rust
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
```

Run it with `cargo run`. It prints:

```text
appended at version 1
+----+---------+
| ts | celsius |
+----+---------+
| 1  | 61.5    |
| 2  | 63.0    |
+----+---------+
+--------+------------+
| device | room       |
+--------+------------+
| attic  | roof space |
+--------+------------+
attempt 1: committed at version 2
attempt 2: already applied in version 2
4 readings
```

## What happened

- **`Database::builder("moraine-data")`** opened a database in a local directory. A database is
  a prefix of an object store; every table under it is a Delta Lake table. See
  [Local disk](guides/local-disk.md) and [S3 and bucket requirements](guides/s3.md).
- **`create_table`** wrote version 0 of each table's log: the schema and the table options as
  table properties, so every client reads the same settings. Creating a table that exists with
  the same schema and options opens it.
- **`append`** wrote one Parquet file, sorted by the cluster key, and committed it with one
  conditional PUT of `_delta_log/00000000000000000001.json`. See
  [The commit protocol](concepts/commits.md).
- **`scan().filter(..).select(..)`** pinned a snapshot, skipped files and pages whose statistics
  rule out `device = 'boiler'`, and read two columns. See
  [Clustering and pruning](concepts/clustering.md).
- **`upsert`** replaced the rows whose primary key matched and kept the others; **`get`** read one
  key. A keyed table refuses plain `append` by default, so it can never hold a key twice.
- **`txn("ingest-1", 7)`** tagged the write with an application id and a version. The second
  attempt found version 7 of `ingest-1` already in the log and wrote nothing. See
  [Exactly-once writes](concepts/exactly-once.md).

## On S3

Change the location and, if needed, pass storage options:

```rust
use std::collections::HashMap;

let db = Database::builder("s3://my-bucket/moraine")
    .storage_options(HashMap::from([("aws_region".to_string(), "eu-central-1".to_string())]))
    .open()
    .await?;
```

The bucket must honour conditional writes; moraine checks this before the first write. See
[S3 and bucket requirements](guides/s3.md).

## Search vectors

Vector search needs the `vector` feature (usearch, built from C++ source), and index parameters
are JSON:

```toml
[dependencies]
morainedb = { version = "0.1.0-beta.2", features = ["vector"] }
serde_json = "1"
```

This program stores three four-dimension vectors, indexes them and asks for the two nearest
English ones:

```rust
use std::sync::Arc;

use arrow::array::{Float32Builder, Int64Array, ListBuilder, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;
use moraine::expr::{col, lit};
use moraine::index::BuildOptions;
use moraine::{Database, IndexSpec, TableOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> moraine::Result<()> {
    let _ = std::fs::remove_dir_all("moraine-vectors");
    let db = Database::builder("moraine-vectors").open().await?;

    // A vector column is a list of Float32; Delta names the list's item field `element`.
    let element = Arc::new(Field::new("element", DataType::Float32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lang", DataType::Utf8, true),
        Field::new("embedding", DataType::List(element.clone()), true),
    ]));
    let docs = db
        .create_table("docs", schema.clone(), TableOptions::default())
        .await?;

    let mut vectors = ListBuilder::new(Float32Builder::new()).with_field(element);
    for v in [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.9, 0.1, 0.0, 0.0],
    ] {
        vectors.values().append_slice(&v);
        vectors.append(true);
    }
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["en", "de", "en"])),
            Arc::new(vectors.finish()),
        ],
    )
    .expect("columns match the schema");
    docs.append(vec![batch]).await?;

    // The dimension and the metric belong to the index, not the column.
    let spec = IndexSpec::new("emb", "vec-hnsw", &["embedding"])
        .params(json!({"dim": 4, "metric": "cosine"}));
    docs.create_index(spec).await?;
    docs.build_indexes(BuildOptions::default()).await?;

    let result = docs
        .search_vector("emb", vec![1.0, 0.0, 0.0, 0.0])
        .limit(2)
        .filter(col("lang").eq(lit("en")))
        .select(["id"])
        .collect()
        .await?;
    print_batches(&[result.batch]).expect("printable");

    Ok(())
}
```

It prints:

```text
+----+----------------+
| id | _moraine_score |
+----+----------------+
| 1  | 0.0            |
| 3  | 0.006116271    |
+----+----------------+
```

- **`List<Float32>`** is the vector column's type; Delta has no fixed-size list. The dimension and
  the metric are part of the index spec.
- **`create_index`** commits the spec as a table property. Queries can use it at once.
- **`build_indexes`** builds the missing sidecars, one per data file. This file has three rows,
  below the 10,000 rows from which a file gets a sidecar, so it is searched exactly; the answer is
  the same either way.
- **`search_vector(..).limit(2).filter(..).select(..)`** returns the rows best first with their
  cosine distance in `_moraine_score`.

The [indexes and vector search guide](guides/vector-search.md) covers the parameters, building
and caching sidecars, and the TurboQuant kind.

## Search text

Full-text search needs the `fts` feature (tantivy):

```toml
morainedb = { version = "0.1.0-beta.2", features = ["fts"] }
```

```rust
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;
use moraine::index::BuildOptions;
use moraine::{Database, IndexSpec, TableOptions};

#[tokio::main]
async fn main() -> moraine::Result<()> {
    let _ = std::fs::remove_dir_all("moraine-text");
    let db = Database::builder("moraine-text").open().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let notes = db
        .create_table("notes", schema.clone(), TableOptions::default())
        .await?;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "Glaciers",
                "Object storage",
                "Moraines",
            ])),
            Arc::new(StringArray::from(vec![
                "Ice moves slowly down the valley.",
                "Writers commit with one conditional put.",
                "A moraine is rock and soil a glacier left behind.",
            ])),
        ],
    )
    .expect("columns match the schema");
    notes.append(vec![batch]).await?;

    // Every column of a full-text index is a string column; null and empty values are fine.
    notes
        .create_index(IndexSpec::new("text", "fts", &["title", "body"]))
        .await?;
    notes.build_indexes(BuildOptions::default()).await?;

    let result = notes
        .search_text("text", "glacier valley")
        .limit(2)
        .select(["id", "title"])
        .collect()
        .await?;
    print_batches(&[result.batch]).expect("printable");

    Ok(())
}
```

It prints:

```text
+----+----------+----------------+
| id | title    | _moraine_score |
+----+----------+----------------+
| 1  | Glaciers | 1.0596459      |
| 3  | Moraines | 0.8538154      |
+----+----------+----------------+
```

- **`fts`** indexes one or more string columns with tantivy, one index per data file.
- **`search_text`** takes tantivy's query syntax (words, `"phrases"`, `title:word`, `AND`, `OR`,
  `-word`) and returns the rows best first with their BM25 score in `_moraine_score`. Scores use
  statistics summed over every file, so they are those of one index over the whole table.
- Rows of files without a sidecar are indexed in memory when searched; the answer is the same.

The [full-text and hybrid search guide](guides/full-text-search.md) covers tokenizers, boosts,
hybrid search with a vector index, and background refresh.

## Next

- [How a commit works](concepts/commits.md), step by step.
- [Many writers](guides/multi-writer.md): what to expect when processes contend.
- [Indexes and vector search](guides/vector-search.md).
- [Full-text and hybrid search](guides/full-text-search.md).
