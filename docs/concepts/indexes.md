# Sidecar indexes

moraine indexes data files, not tables. Every index is a set of **sidecars**: one immutable
object per data file and index, built from that file alone and stored next to the data, outside
the Delta table root. A data file never changes, so its sidecar never needs updating, and indexes
need no consistency protocol of their own.

## Always correct, sometimes faster

A query asks each data file for its answer. A file that has a valid sidecar answers from it; a
file without one (not built yet, failed to download, corrupt, or written by another Delta writer)
answers by the index kind's **exact route**, which reads the file's rows and scores them. Both
routes return the same answer, so results are correct whether or not indexing has caught up.
Indexing is an optimisation only: deleting every sidecar loses speed, never rows.

`SearchResult::stats` reports how a search was answered: files in the snapshot, files skipped by
statistics or by an empty filter result, files answered from a sidecar, files answered exactly,
and rows scored on the exact route.

## Where sidecars live

```text
<table>/idx/<data file path>/<index name>.<spec hash>.osix
```

The spec hash is the first 16 hex characters of SHA-256 over the canonical JSON of the bundle
format version, the kind and its format version, the indexed columns, the hashed parameters, and
a library fingerprint. Changing any of them yields a new hash and so new sidecars; old ones are
ignored and later collected.

Sidecar existence is not recorded in the Delta log. A table lists `idx/` once per snapshot change
and at most once per poll interval otherwise (`DatabaseBuilder::sidecar_poll_interval`, default
10 s). A listing that fails or is cut short keeps the previous map; it is never read as "no
sidecars".

## The bundle format

Every kind writes the same container, so validation, caching and collection are shared:

| Part | Contents |
|---|---|
| Header, 64 bytes | magic `OSIX`, bundle version, the kind as an 8-byte ASCII name, the kind's format version, spec hash, the source file's row count and size, indexed row count, section count |
| Sections | raw bytes, each aligned to 4,096 bytes so they can be memory-mapped |
| Section table | name, offset, length and XXH3-64 checksum per section |
| Footer, 24 bytes | offset of the section table, a checksum of header and table, magic `XISO` |

On open, moraine checks both magics, the versions, the spec hash, and that the source row count
and size match the `add` action of the file it claims to index. A sidecar that fails any check is
treated as missing and marked bad for 60 seconds; its file takes the exact route. A file with
nothing to index still gets a bundle with zero indexed rows, so builders do not retry it forever.

## Index specs

An index is declared with an `IndexSpec` and stored in the table property `moraine.indexes`:

| Field | Hashed | Meaning |
|---|---|---|
| `name` | | unique per table, `[a-z0-9][a-z0-9_-]{0,63}` |
| `kind` | yes | the registered kind, for example `vec-hnsw` |
| `columns` | yes | the indexed columns |
| `params` | yes | what a sidecar contains (dimension, metric, graph connectivity, bits) |
| `policy` | no | which files get a sidecar (for example `min_rows_for_ann`) |
| `search` | no | query defaults a search may override (for example `expansion_search`) |

The kind fills in every default when the index is created, so a stored spec never changes
meaning when a later release adds a parameter. Unknown fields are refused rather than ignored.
`create_index`, `drop_index` and `replace_index` commit a metadata-only log entry; appends in
flight rebase over it.

## Kinds

| Kind | Feature | What it is |
|---|---|---|
| `key` | always | The key index for tables with a primary key: a binary fuse filter plus page or row positions. Used by `Table::get`. See [Clustering and pruning](clustering.md). |
| `vec-hnsw` | `vector` | An HNSW graph built with usearch, memory-mapped from the bundle. Metrics `cosine`, `l2` and `ip`. |
| `vec-tq` | `turbovec` | TurboQuant codes built with turbovec, 2 to 4 bits per dimension (136 bytes per 256-dimension vector at 4 bits). Cosine and inner product; the dimension must be a multiple of 8. Codes are copied into memory on open. |
| `fts` | `fts` | A tantivy index over one or more string columns, one segment per file, served in place from the bundle. Searched with `search_text`, ranked by BM25 with statistics summed over every file. See [Full-text and hybrid search](../guides/full-text-search.md). |

Approximate candidates are rescored from their rows before they compete with exact hits, so
results from sidecar files and exact files merge into one correct top k. Files below the
policy's `min_rows_for_ann` (default 10,000 rows) are always answered exactly.

The full-text kind's exact route is an index built in memory from the file's rows with the same
builder, kept in a byte-bounded cache. Its scores do not come from one file alone: a search sums
the BM25 statistics of every file first, so a row scores as it would in one index over the whole
table, whichever files have sidecars.

Kinds are pluggable. The traits in `moraine::index::kind` are public and object-safe, and an
application registers its own with `DatabaseBuilder::index_kind`; the test kit includes a toy
kind written against the public API only, to keep that true.

`vec-hnsw` is the default choice. The D24 benchmark
([D37](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d37-the-d24-benchmark-usearch-stays-the-default-vector-defaults-the-d21-vector-miss))
found `vec-tq` 5 to 85 times faster to build and 8.6 times smaller, but it scans every code of a
file, so at a million rows it answers in 13 to 15 ms where usearch takes 2.4 to 4.3 ms. `vec-tq`
suits tables whose files are rewritten often; its merge concatenates codes instead of rebuilding
a graph. The numbers are on the [benchmarks page](../benchmarks.md#vector-search).

## Building sidecars

`Table::build_indexes(BuildOptions)` builds the missing sidecars for the newest version, largest
files first. Any number of clients may run it at once: a builder that finds a sidecar already
uploaded counts it and moves on. `Table::index_status()` reports, per index, the files and rows
covered, the files left to the exact route by policy, failures, and sidecar bytes.

Builds are memory-bounded. One build holds at most 256 MiB by default and all builds of a
database at most 1 GiB (`DatabaseBuilder::build_memory`); a file larger than the budget is built
in parts. Input is read in windows of about 4 MiB through the page index. A 300,000-row,
256-dimension file (300 MiB of floats) built under a 64 MiB budget grew the process by 57 to
61 MiB with usearch and 73 MiB with turbovec.

## The local cache

Downloaded sidecars are kept in memory by default, up to 1 GiB (`CacheMode::Memory`).
`DatabaseBuilder::sidecar_cache` switches to a disk cache in a directory you choose
(`CacheMode::Disk`), or turns sidecars off (`CacheMode::Disabled`, every file takes the exact
route); a directory that cannot be written fails at open. Opened sidecars are reused up to `DatabaseBuilder::max_open_sidecars`.

Writers build their files' sidecars before the commit (index before commit), within limits
each index's policy sets, so a new file rarely waits for the build job; compaction does it for
every index, and merges `vec-tq` codes from its inputs' sidecars instead of rebuilding them. See
[Index before commit](../guides/vector-search.md#index-before-commit).

Sidecars of removed files and of old specs are collected by maintenance once they are older
than the grace period ([Maintenance](../guides/maintenance.md#vacuum)). A file larger than the
build budget is built as several graphs, which a search runs at once, one thread each up to the
machine's parallelism.
