# Clustering and pruning

A read is fast when it touches few files, few pages within those files, and few requests. moraine
gets there by controlling how it writes data files, and by keeping what it learns about them in
memory for as long as a version lasts.

## Keys and sort order

A table can declare two kinds of key at creation:

- a **primary key** (`TableOptions::primary_key`), which `upsert`, `delete_keys` and `get` use;
- a **cluster key** (`TableOptions::cluster_by`), the columns rows are usually read by, for
  example a run id or a device id.

Every data file moraine writes is sorted by the cluster key and then the primary key. Both are
stored as table properties, so every client sorts the same way.

```rust
let table = db
    .create_table(
        "points",
        schema,
        TableOptions::default().primary_key(["series", "step"]).cluster_by(["run_id"]),
    )
    .await?;
```

## Parquet layout

| Setting | Default | Why |
|---|---|---|
| `page_row_limit` | 1,024 rows | With small pages the page index can narrow a lookup to one small range read (10 rows from a 1,000,000-row file: 1.7 MB instead of 10.9 MB in the spike) |
| `row_group_rows` | 16,384 rows | |
| `target_file_bytes` | 256 MiB | One commit's data is split into files of about this size |
| Bloom filters | on primary-key and cluster columns, 1 % false positives | Rejects a file that merely overlaps a key's range |
| Page index | on | Finds the pages that hold a key in sorted files |

The page index and bloom filters are written at the end of the file next to the footer.

## Pruning, from cheapest to dearest

A point lookup or a filtered scan narrows the work in stages. Each stage costs nothing when the
previous one already excluded a file.

1. **File key ranges.** The snapshot keeps each file's minimum and maximum of every column, from
   the `add` statistics moraine writes itself. A file whose range excludes the key or filter is
   skipped with no request.
2. **Key index.** For keyed tables, a small sidecar per file holds a binary fuse filter (a kind
   of xor filter) over primary-key fingerprints (about 9 bits per key, measured 0.39 % false
   positives), so an absent key costs no read. See [Sidecar indexes](indexes.md).
3. **Bloom filter.** Files without a key index fall back to the Parquet bloom filter of the key
   column.
4. **Page index.** In a sorted file, a binary search over the cached page statistics finds the
   page holding the key, and one range GET reads it.

With warm caches, an absent key over five overlapping files costs no request, and a present key
at most two range reads.

## Caches per version

Data files are immutable and never renamed, so anything cached about one stays correct for as
long as the file exists.

- **Footers.** The footer cache fetches a file's last 128 KiB in one request (whole files up to
  256 KiB), which holds the footer, the page index and the bloom filters. Entries are keyed by
  (path, size) in a byte-bounded LRU shared by every table of a `Database`
  (`DatabaseBuilder::file_cache_bytes`).
- **Snapshots** are incremental: a refresh applies only the new log entries to the snapshot in
  hand.
- **Small tables** are materialised in memory as Arrow batches for the version in hand, when
  their live data is under 64 MiB *and* they hold at most 250,000 rows
  (`DatabaseBuilder::small_table_bytes` and `small_table_rows`). Reads then filter in memory, and a
  version that moved by appends only extends the copy with the new files.

## The key index sidecar

A key index is built for every table with a primary key. Its sections depend on how the file is
sorted:

| Section | When | What it holds |
|---|---|---|
| filter | always | binary fuse filter over primary-key fingerprints |
| `pages` | file sorted by the primary key alone | the first key of every page, binary-searched |
| `pairs` | file sorted by (cluster key, primary key), or written by another Delta writer | sorted (fingerprint, row offset) pairs |
| `cluster` | file with a cluster key where each value forms one run | the row range of each distinct value |

Keys are encoded so that byte order equals value order (strings escaped, integers big-endian with
the sign bit flipped, floats in total order, nulls first). The builder detects the order from the
rows it sees rather than trusting a tag.

The writer builds the key index inline, in the same round trip as the data PUT, for files of at
least 100,000 rows (`DatabaseBuilder::key_index_inline_min_rows`). Smaller files keep the budget
of one data PUT and one log PUT per flush; `Table::build_indexes` covers them later, and until
then they are found through bloom filters and page statistics, still exactly.

## Compaction keeps the order

Appends write one sorted file per commit, so a busy table's key ranges overlap from file to
file until compaction merges them. Compaction is a sort-merge by (cluster key, primary key): the
rows of a run of adjacent small files are merged in that order and written as larger files whose
key ranges no longer overlap, so a cluster value ends up in one file, usually in a few pages.
Rows with equal keys keep the order they were committed in. See
[Maintenance: compaction](../guides/maintenance.md#compaction).

## What is not done yet

- **Cluster reads through the key index.** Cluster-key scans still go through page statistics,
  which in sorted files already narrow a value to its pages; the `cluster` section is built and
  tested but not yet used by scans.
- **Partitioning.** Not supported yet.
