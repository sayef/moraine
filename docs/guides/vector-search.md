# Indexes and vector search

This guide adds a vector index to a table, searches it, and keeps it built. How sidecars work is
in [Sidecar indexes](../concepts/indexes.md).

## Enable a vector kind

Vector kinds are behind cargo features:

```toml
[dependencies]
morainedb = { version = "0.1.0-beta.2", features = ["vector"] }   # vec-hnsw (usearch)
# features = ["turbovec"] adds vec-tq (TurboQuant); both can be on
serde_json = "1"                                                  # index parameters
```

`vec-hnsw` is the one to start with. In the D24 benchmark it answered faster than `vec-tq`
at every size; `vec-tq` builds 5 to 85 times faster and its sidecars are 8.6 times smaller, which
suits tables whose files are rewritten often. See [Benchmarks](../benchmarks.md#vector-search).

Every client that searches or builds an index needs its kind enabled. A client without it
reports the index in `index_status` with an empty spec hash, and `build_indexes` skips it.

## The vector column

Delta has no fixed-size list type, so vectors are stored as `List<Float32>`. A
`FixedSizeList<Float32, n>` column is refused at table creation like any type the log cannot
represent. The dimension belongs to the index, not the column.

```rust
use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema};

let element = Arc::new(Field::new("element", DataType::Float32, true));
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("lang", DataType::Utf8, true),
    Field::new("embedding", DataType::List(element), true),
]));
```

A vector of the wrong length, a null, a non-finite value, or (under cosine) a zero vector is
skipped by every route rather than refused at write time, so writes never depend on indexes
created later.

## Create the index

```rust
use serde_json::json;
use moraine::IndexSpec;

let spec = IndexSpec::new("emb", "vec-hnsw", &["embedding"])
    .params(json!({"dim": 384, "metric": "cosine"}));
let resolved = table.create_index(spec).await?;
println!("{}", resolved.search); // {"expansion_search":128,"rerank":0}
```

`create_index` returns the spec with every default filled in, which is what is stored. The
parameters that matter most:

| Where | Key | Kind | Default |
|---|---|---|---|
| `params` | `dim` | both | required |
| `params` | `metric` | both | `cosine`; `vec-hnsw` also takes `l2` and `ip`, `vec-tq` takes `cosine` and `ip` |
| `params` | `connectivity` | `vec-hnsw` | 16 |
| `params` | `quantization` | `vec-hnsw` | `f32`; also `f16` and `i8` |
| `params` | `bits` | `vec-tq` | 4 (2 to 4) |
| `policy` | `min_rows_for_ann` | both | 10,000: smaller files are always searched exactly |
| `search` | `expansion_search` | `vec-hnsw` | 128 |
| `search` | `rerank` | both | how many extra candidates are rescored from the rows (`vec-hnsw`: 0 with `f32` vectors, 2 when quantised; `vec-tq`: 8) |

`params` change what a sidecar contains and are part of the spec hash, so changing them
(`replace_index`) means new sidecars. `policy` and `search` do not. `vec-tq` needs a dimension
that is a multiple of 8, and refuses `l2`: TurboQuant scores inner products only.

## Change or drop an index

```rust
let spec = IndexSpec::new("emb", "vec-hnsw", &["embedding"])
    .params(json!({"dim": 384, "metric": "cosine", "quantization": "f16"}));
table.replace_index(spec).await?;

table.drop_index("emb").await?;
```

`replace_index` swaps the spec of the same name in one commit, or creates it. When the hashed
parameters change, every file takes the exact route until `build_indexes` has built the new
sidecars; searches stay correct meanwhile. `drop_index` of a name that does not exist changes
nothing. Both are metadata-only commits, so appends in flight rebase over them.

## Search

```rust
use moraine::expr::{col, lit};

let result = table
    .search_vector("emb", query_vector)
    .limit(10)
    .filter(col("lang").eq(lit("en")))
    .select(["id", "lang"])
    .collect()
    .await?;

// Rows best first: the selected columns plus `_moraine_score`, a distance (smaller is closer).
println!("{} rows", result.batch.num_rows());
// Where each hit came from: (data file, row offset, score).
for hit in &result.hits {
    println!("{} @ {}: {}", hit.file, hit.offset, hit.score);
}
// How the search was answered.
println!(
    "{} files from sidecars, {} exact, {} skipped",
    result.stats.sidecar_files, result.stats.exact_files, result.stats.skipped_files
);
```

A search runs against a snapshot taken when `collect` is called. Filters are DataFusion
expressions; a file whose statistics rule the filter out is skipped, and a file where the filter
admits few rows (fewer than `DatabaseBuilder::exact_filter_threshold`, default 10,000) is
searched exactly even when it has a sidecar. `.param("expansion_search", 512)` overrides a search
default for one query.

## Build the sidecars

Sidecars are built by `Table::build_indexes`, which any client can run, as often as it likes,
from as many processes as it likes. Until a file has a sidecar, searches read it exactly.

```rust
use moraine::index::BuildOptions;

let report = table.build_indexes(BuildOptions::default()).await?;
println!("built {}, already present {}", report.built, report.already_present);

for status in table.index_status().await? {
    println!(
        "{}: {} of {} files covered",
        status.name, status.files_covered, status.files_total
    );
}
```

`BuildOptions` limits the run to some indexes, sets how many builds run at once (default 1), and
caps the number of builds per call. Build memory is bounded per build and per database with
`DatabaseBuilder::build_memory` (defaults 256 MiB and 1 GiB).

Maintenance passes run the build for every index ([Maintenance](maintenance.md)). Most files
never need it: writers and compaction build sidecars before they commit (next section).

## Index before commit

A write builds the sidecars of every file it writes before the commit that adds the file, from
the rows it already holds, and puts them with the data file in the same round trip. Readers
never see such a file without its sidecars, so its rows never take the exact route. This holds
for appends, the new rows of an upsert, the files a delete or update rewrites, and compaction's
merged files.

An append pays for it: one more PUT (put-if-absent) per index per file, and the build's time
before the commit. The index's `policy` bounds that cost; files beyond the bounds are left to
`build_indexes` as before:

| Policy key | Default | Meaning |
|---|---|---|
| `before_commit` | `true` | Build before the commit at all |
| `before_commit_max_rows` | 1,000,000 | Largest file built before the commit, in rows |
| `before_commit_max_build_bytes` | 2 GiB | Largest estimated build work: rows times the kind's bytes per row (about 2.3 KiB for 256-dimension usearch F32) |

Files below the kind's own threshold (`min_rows_for_ann`, 10,000 rows by default) get no
sidecar either way: scoring them exactly is cheaper than opening a graph. Compaction merges them
into files above it. `policy` is not part of the spec hash, so changing it keeps every sidecar.

```rust
use moraine::IndexSpec;
use serde_json::json;

// Build sidecars of appended files of up to 200,000 rows before their commit; larger
// appends leave theirs to the build job.
let spec = IndexSpec::new("embedding_bc", "vec-hnsw", &["embedding"])
    .params(json!({"dim": 384, "metric": "cosine"}))
    .policy(json!({
        "min_rows_for_ann": 10_000,
        "before_commit": true,
        "before_commit_max_rows": 200_000,
        "before_commit_max_build_bytes": 1u64 << 30
    }));
articles.create_index(spec).await?;
for s in articles.index_status().await? {
    println!(
        "{}: {} rows pending, {:.1} % on the exact route",
        s.name,
        s.rows_pending(),
        100.0 * s.exact_share()
    );
}
```

`IndexStatus::rows_pending` counts rows of files the kind wants a sidecar for that have none
yet; `exact_share` is the share of all rows a search scores exactly, files below the threshold
included. Under a steady append and compaction load the [benchmarks](../benchmarks.md) measure
how that share behaves over time.

`vec-tq` sidecars of compacted files are merged from the inputs' sidecars rather than rebuilt:
TurboQuant codes depend only on their own vector, so the merged file's codes are the inputs'
codes in the merged row order, byte for byte what a rebuild would produce.

## Caching sidecars

Searches download each file's sidecar once and keep it:

```rust
use moraine::index::CacheMode;

let db = Database::builder("s3://my-bucket/prefix")
    .sidecar_cache(CacheMode::Disk {
        dir: "/var/cache/moraine".into(),
        max_bytes: 20 << 30,
    })
    .open()
    .await?;
```

The default is `CacheMode::Memory` up to 1 GiB; `CacheMode::Disabled` sends every file to the
exact route. The disk cache can be shared by several processes; it relies
on atomic renames only, and a disk problem never fails a query (the sidecar falls back to
memory, or its file to the exact route). `vec-hnsw` sidecars are opened as usearch views over
the bundle; `vec-tq` copies its codes into memory on open.

## Your own index kind

Kinds are pluggable: implement the traits in `moraine::index::kind` and register the kind on
every client that builds or searches it.

```rust
let db = Database::builder("s3://my-bucket/prefix")
    .index_kind(Arc::new(MyKind))
    .open()
    .await?;
```

A kind declares its own query bytes and is searched with `table.search_custom(index, bytes)`.
The test kit has a small kind written against the public API only.

## The key index

Tables with a primary key also get a `key` index automatically. It needs no spec and no feature:
writers build it inline for files of at least 100,000 rows, and `build_indexes` builds it for the
rest. `Table::get` uses it to skip files that do not hold the key without reading them. See
[Clustering and pruning](../concepts/clustering.md).

## Full text and hybrid search

Full-text search with tantivy, ranked with BM25 statistics of the whole table, and hybrid search
that fuses a vector and a full-text ranking, have their own guide:
[Full-text and hybrid search](full-text-search.md).
