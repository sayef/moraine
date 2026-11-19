# Blobs

Binary values of any size, from a few bytes to gigabytes, in an ordinary column. Small values are
stored inside the row; large ones are objects of their own under the table's `blobs/` prefix,
referenced from the row. The row is the only record of which objects are alive: a value whose
row is gone is collected by vacuum after the grace period, and nothing else needs cleaning up.

The snippets are compiled as `crates/moraine/examples/blobs.rs`.

## The blob column

A blob column is an Arrow struct that `moraine::blob::field` declares:

| Field | Type | Meaning |
|---|---|---|
| `inline` | Binary | The bytes, when at or below the table's inline threshold |
| `key` | Utf8 | The object under `blobs/`, otherwise |
| `size` | Int64 | Length in bytes |
| `crc32c` | Int64 | CRC-32C of the bytes |
| `content_type` | Utf8 | The media type the writer gave, if any |

Exactly one of `inline` and `key` is set in a non-null value. A zero-length value is stored
inline with size 0, which reads differently from a null. Other Delta readers see the struct as it
is: inline values are readable directly, external ones need the object.

```rust
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
```

`blob_inline_bytes` is a table property, so every client stores values the same way; it can be
at most 16 MiB. A plain `Binary` column also works for small values and is always inline, with a
limit of 16 MiB per value.

## Writing

Build the column with `BlobColumn` and write it with any write: append, upsert, queued writes.

```rust
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
```

The write uploads every value above the threshold before it writes its data file: one PUT per
value, or a multipart upload in 16 MiB parts above 16 MiB, in parallel up to the database's
`io_concurrency`. The size and checksum are computed from the bytes. A write that fails, or a
process that dies before the commit, leaves the uploaded objects unreferenced; collection removes
them after the grace period. An update cannot assign a blob column; upsert the row instead.

## Reading

`Table::blob` (or `Table::blobs` for a column of a batch) gives a handle on a value:

```rust
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
```

- `bytes()` reads the whole value and checks its length and CRC-32C. A mismatch is
  `Error::BlobCorrupt`; wrong bytes are never returned.
- `stream()` reads it in the store's chunks, so memory stays at a chunk. The checksum is checked
  at the end: a mismatch is the stream's last item, an error.
- `range(start..end)` reads part of it with one ranged GET; a part cannot be checked against the
  whole value's checksum, only its length is.
- A missing object is `Error::BlobNotFound` for that value only; the row still reads, and
  `verify` lists the key in `missing_blobs`.

## Reusing a value

A reference read from one row can be written into another. The write does not trust it: it
copies the object on the server to a fresh key (no bytes pass through the client) before it
commits. That is what makes collection safe. Without the copy, a reference read from a row that
was then deleted could be written again after collection took its object.

```rust
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
```

If the source object is already gone, the write fails with `Error::BlobNotFound` and commits
nothing. An application that knows its references stayed referenced without a gap can skip the
copies with `DatabaseBuilder::blob_reuse(BlobReuse::TrustCaller)`; if it is wrong, collection
can delete an object a new row points to.

Rewrites inside moraine never copy: compaction, an update of other columns, and the rows an
upsert or delete keeps carry their references as they are, because those rows were referenced the
whole time. Queued writes take values as bytes only; a reference in a submitted batch is refused.

## Large values from a stream

`Table::upload_blob` uploads a value ahead of the write from a stream of chunks, in 16 MiB parts,
holding at most two parts in memory whatever the value's size:

```rust
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
```

The handle remembers what it uploaded. A write through the same handle commits the reference
without a copy while it is within the commit deadline of the upload (a quarter of the grace
period, 15 minutes by default), and the commit clock then runs from the upload. Later than that,
or through another handle, the write copies it, as it does any other reference: nothing uploaded
longer ago than the commit deadline is committed as it is.

## Collection

Blob collection runs inside vacuum (`Table::vacuum`, or the vacuum job of `Table::maintain`):

1. **Mark**: read the blob columns of every file vacuum keeps (the current files, the files a pin
   names, and the files removed by log entries younger than the grace period) into a set of keys.
   Data files never change, so a handle reads each file's keys once.
2. **Sweep**: list `blobs/` and delete each object whose key is not marked and whose last
   modification, by the store's clock, is older than the grace period.

```rust
let report = table.vacuum().await?;
println!(
    "blobs: {} referenced, {} deleted ({} bytes)",
    report.blobs_referenced, report.blobs_deleted, report.blob_bytes_deleted
);
let check = table.verify().await?;
assert!(check.missing_blobs.is_empty());
```

A pass that has run for half the grace period, or that cannot read a file it keeps, stops
deleting and sets `blob_collection_aborted`. A table without blob columns makes no blob request.

Why it is safe: a write commits within the commit deadline of its first upload or copy, so every
object a commit references is younger than a quarter of the grace period when the commit lands;
the sweep deletes only objects older than the grace period that the newest version, the pinned
versions and every version younger than the grace period leave unreferenced. The simulation
checks this against the store's full history under crashes, faults, pins and time running past
the grace period ([Testing](../design/testing.md)).

## Costs

| Operation | Requests |
|---|---|
| Write with only inline values | As without blobs: one data PUT, one log PUT |
| Each external value up to 16 MiB | One PUT |
| Each external value above 16 MiB | One initiation, one PUT per 16 MiB part, one completion |
| Each reused reference | One server-side copy |
| `bytes()`, `stream()` | One GET |
| `range()` | One ranged GET |
| Vacuum of a table with blob columns | One listing of `blobs/`, one read of each new kept file's blob columns, one DELETE per collected object |

Measured throughput and range-read latency, on local disk and an S3-compatible server, are on
the [Benchmarks](../benchmarks.md) page.
