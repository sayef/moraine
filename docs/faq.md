# FAQ

## Is moraine ready for production?

No. moraine is pre-1.0. It has tables with append, upsert, delete, update and added columns,
exactly-once writes, sidecar indexes with vector, full-text and hybrid search, maintenance
(compaction, checkpoints, log cleanup, vacuum, leases, pins, queued writes) and blobs, all tested
by simulation. The real-cloud runs are still to come, and the API may still change before 1.0.

## Why not use delta-rs directly?

moraine is built on delta-rs and writes standard Delta tables. What it adds is a commit path
that costs one data PUT and one log PUT per append with no listing (delta-rs's own commit path
lists the log and runs post-commit work on every commit), group commit inside a process,
exactly-once writes from caller transaction ids, primary keys with `upsert`, files sorted by
cluster and primary key with per-version caches, and sidecar indexes for vector search. The
[benchmarks](benchmarks.md) compare the two on the same workloads.

## Why not Lance?

Lance is a good format with its own vector indexes. moraine's tables are Delta tables, readable
by Spark, DuckDB, delta-rs and the rest of the Delta ecosystem, and its design targets many
concurrent writers on one table with no coordinator. The [benchmarks](benchmarks.md) show where
moraine is faster and where it is not.

## Can Spark, DuckDB or delta-rs read the tables?

Yes. Each table's `data/` directory is a standard Delta table (reader version 1, writer
version 2, no deletion vectors). The interoperability tests read tables moraine wrote with
delta-rs. Sidecar indexes live outside the Delta root and are invisible to other readers.

## Can other Delta writers write to a moraine table?

moraine reads what they write, as long as the table needs no reader or writer feature moraine
does not implement; otherwise it refuses to open the table. Files from other writers are not
sorted the way moraine sorts its own, so their key index holds sorted (fingerprint, row offset)
pairs instead of page keys, and their statistics are trusted less (long string maxima are
ignored and timestamp maxima widened). moraine's exactly-once and conflict guarantees hold among
moraine writers; another writer does not run moraine's conflict checks or honour its primary
keys.

## Which object stores are supported?

S3 and S3-compatible servers that honour conditional writes, local disk, and in-memory stores for
tests. Google Cloud Storage and Azure Blob Storage are planned, together with real-cloud test
runs. See [S3 and bucket requirements](guides/s3.md).

## Has it run against real AWS S3?

The test suite and benchmarks ran against an S3-compatible server (RustFS). The real-S3 runs are
written as tests that are skipped unless a bucket and credentials are configured, and they are
still owed.

## How do I keep a table compact?

Run maintenance: `Table::maintain` once in a while, `Table::spawn_maintenance` in a long-lived
process, or the `moraine` command from a scheduled job. A pass compacts, checkpoints, cleans up
the log and vacuums; none of it is needed for correctness. See [Maintenance](guides/maintenance.md).

## What about full-text search?

It is there, behind the `fts` feature: a tantivy sidecar per data file,
searched with `search_text`, with BM25 statistics summed over every file so scores equal those of
one tantivy index over the whole table, and `search_hybrid`, which fuses a vector and a full-text
ranking. See [Full-text and hybrid search](guides/full-text-search.md).

## And large binary payloads?

Supported: large payloads are stored as separate blobs referenced from rows, with range reads and
collection of unreferenced blobs. See [Blobs](guides/blobs.md).

## Are there bindings for Python or other languages?

No. moraine is a Rust library only.

## Can a table be partitioned?

Not yet. `TableOptions` has no partitioning option; clustering by a cluster key is the way to
keep related rows together.

## Does it need synchronised clocks?

No. Expiry decisions compare object ages with the object store's own time, read from the
last-modified time of a probe object, so a client clock that is wrong by hours changes nothing.

## What is the licence?

Apache-2.0 or MIT, at your option.

## How do I report a security issue?

Privately, as described in
[SECURITY.md](https://github.com/sayef/moraine/blob/main/SECURITY.md). Please do not open a
public issue.
