# Reading files and rows

A table is a set of data files, and every version lists its own. Most reads go through
`scan`, `get` and the searches, which choose the files themselves. Applications that keep their
own structures on top of a table (an index of where values live, a fold of statistics, a cache)
also need the files: which ones a version holds, their rows, and single rows by position. These
reads all take a `TableSnapshot`, so they see one version from start to end.

## The files of a version

`TableSnapshot::files` lists the live data files in the table's file order, with no request:

```rust
let snap = table.snapshot().await?;
for f in snap.files() {
    println!(
        "{} rows={:?} added in {:?} level {} tags {:?}",
        f.path, f.rows, f.added_in, f.level, f.tags
    );
}
```

A path names the same rows for as long as the file lives: data files are never rewritten. A
version that no longer holds a path has replaced it (a delete, an update, an upsert, a
compaction or a [file replace](maintenance.md#replacing-files)).

## Scanning chosen files

`Scan::files` restricts a scan to some files, read in the order given. Filters, projections and
limits apply as usual, and a file's rows come in the file's own order:

```rust
use moraine::expr::{col, lit};

let stats: Vec<String> = snap
    .files()
    .into_iter()
    .filter(|f| f.tags.get("kind").map(String::as_str) == Some("stats"))
    .map(|f| f.path)
    .collect();
let rows = snap
    .scan()
    .files(stats)
    .filter(col("run").eq(lit("r-17")))
    .select(["run", "metric", "value"])
    .collect()
    .await?;
```

A path the version does not hold is `Error::FilesChanged`: plan again on a newer snapshot.

## Rows by position

`TableSnapshot::take` reads the rows at given offsets of one file (0 is the file's first row, in
the order a scan of the file returns them). It reads only the pages that hold those rows, through
the footer cache, and returns them in the order of the offsets, repeats included:

```rust
let file = &snap.files()[0];
let values = snap
    .take(&file.path, &[42, 7, 42])
    .select(["value"])
    .collect()
    .await?;
assert_eq!(values.num_rows(), 3);
```

An offset beyond the file's rows is `Error::InvalidQuery`. A file that vacuum or a
[purge](maintenance.md#purge-and-erasure) has collected is `Error::SnapshotExpired`: the version
is older than what the store still keeps.

## Streaming a scan

`Scan::collect` returns the whole result at once. `Scan::stream` returns batches as the files are
read, a few files ahead (the database's I/O concurrency), so a large result never sits in memory:

```rust
use futures::TryStreamExt;

let mut s = table.scan().filter(col("year").eq(lit(2026))).stream();
let mut rows = 0;
while let Some(batch) = s.try_next().await? {
    rows += batch.num_rows();
}
```

The batches come in the order `collect` returns them. A data file found missing before the first
batch moves a scan of the table to the newest version once, as `collect` does; after a batch has
been returned, the stream ends with `Error::SnapshotExpired` instead, since those rows cannot be
taken back. A stream is also what a [file replace](maintenance.md#replacing-files) reads from.

## The table id

`Table::id` (and `TableSnapshot::table_id`) returns the id Delta records in the table's metadata:
set when the table is created, kept by every commit and checkpoint, and new when a table of the
same name is dropped and created again. Use it to key caches by table, so a cache never serves a
previous table's files under the same name. Other Delta readers see the same value.
