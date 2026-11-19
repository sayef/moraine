# Architecture

Everything runs inside the client process. The object store is the only shared component, and
the Delta log of each table is the only place where writers compete.

```text
Clients share nothing except the object store

+-- client process (any number, on any machine) -------------------------------+
|                                                                                |
|  public API: Database, Table, TableSnapshot, Scan, TxnWrite, Search            |
|                                                                                |
|  write path               read path                  index layer               |
|  validate, sort,          snapshot, prune by key     specs, sidecar builds,    |
|  group commit, conflict   ranges, bloom filters,     discovery, exact route,   |
|  checks, rebase, retry    page index; fetch, merge   top-k merge               |
|                                                                                |
|  commit layer             caches                     store layer               |
|  Delta log entries:       footers and page indexes,  retries, request metrics, |
|  encode, put-if-absent,   small tables, key indexes, capability probe, server  |
|  read by GET              sidecars (memory or disk)  time, tags, local CAS     |
+--------------------------------------------------------------------------------+
        ^ log entries, data files            ^ sidecars             ^ probes
        v                                    v                      v
+-- object store: S3, an S3-compatible server, or local disk --------------------+
|  <table>/data/       Delta table root: _delta_log/ and Parquet data files      |
|  <table>/idx/        one sidecar per data file and index (derived, rebuildable)|
|  _meta/              short-lived probe objects                                 |
+--------------------------------------------------------------------------------+
```

Only `data/` is needed to read rows. Everything under `idx/` is derived from data files and can
be deleted without losing anything but speed.

## Layers

1. **Public API.** `Database` owns configuration, the store and the caches shared by its tables.
   `Table` owns a table's current snapshot and its group-commit queue; clones share both.
2. **Write path.** Validates batches, fills missing nullable columns, sorts by (cluster key,
   primary key), writes data files with their statistics, and runs the
   [commit protocol](../concepts/commits.md): group commit, conflict checks, rebase,
   re-execution, the commit deadline and ambiguous-outcome resolution.
3. **Read path.** Holds a snapshot, prunes files by key ranges and filters, reads only the pages
   it needs, and merges results. See [Clustering and pruning](../concepts/clustering.md).
4. **Index layer.** Index specs in table properties, the bundle format, sidecar discovery and
   caching, memory-bounded builds, and the exact route for files without a sidecar. Kinds plug
   in through a public trait. See [Sidecar indexes](../concepts/indexes.md).
5. **Commit layer.** Knows the Delta log format: encodes an entry, puts it with put-if-absent,
   reads entry `n` by GET, loads from a listing and a checkpoint, and converts Arrow schemas to
   their Delta form. It is the only code that uses delta-rs.
6. **Store layer.** `Arc<dyn ObjectStore>` with thin wrappers: retry with backoff, request and
   byte counters, the conditional-write probe, server time, object tags, and compare-and-swap
   for the local filesystem. Tests insert a fault-injecting store here.

## The crates

| Path | Package | Published | Contents |
|---|---|---|---|
| `crates/moraine` | `morainedb` (library `moraine`) | yes | The library. |
| `crates/moraine-testkit` | `moraine-testkit` | no | Simulation and test tooling, and the integration tests. See [Testing and simulation](testing.md). |
| `crates/moraine-cli` | `moraine-cli` | no | The `moraine` command (see [The moraine command](../cli.md)) and the multi-process stress test T8. |
| `spike/` | `moraine-spike` | no | The spike: programs that checked the design's assumptions against delta-rs, object_store, usearch and tantivy before any library code existed. |
| `bench/` | `moraine-bench` | no | The comparison harness against Lance and Delta Lake, a workspace of its own so the main workspace never compiles Lance. |

## Modules of the library

| Module | What it holds |
|---|---|
| `db` | `Database` and `DatabaseBuilder`: locations, options, opening stores |
| `table` | `Table`, `TableSnapshot`, `Scan`, `TxnWrite`; writes, mutations, scans and index operations |
| `write` | Data file writing: sorting, Parquet settings, statistics |
| `read` | Pruning, page selection and row fetches |
| `snapshot` | The in-memory state of one version: files, statistics, transaction ids |
| `log` | The commit layer; `log/delta.rs` is the only module that names a delta-rs type; `log/checkpoint.rs` writes and reads checkpoints |
| `maintain` | Maintenance jobs: leases, compaction, checkpoints and log cleanup, vacuum and collectors, the queued-write inbox, verify and repair |
| `commit` | The generic commit retry loop and conflict kinds |
| `index` | Bundles, specs, kinds (`key`, `vector`), discovery, cache, builds, queries, top-k merge |
| `store` | Store wrappers: retry, metrics, probe, server time, tags, local compare-and-swap, AWS SDK credentials |
| `options` | `TableOptions`, `TableDef`, `Consistency`, `Committed`, `WriteOutcome`, `DuplicatePolicy` |
| `schema`, `stats`, `names` | Schema conformance, file statistics, not-found suggestions |
| `clock` | The client clock, injectable so the simulation controls time and randomness |
| `error` | The `Error` enum, `StorageError` and `LibraryError` |
| `testing` | Internals the test kit, benchmarks and fuzz targets use; hidden, not covered by the versioning policy |

## Dependencies

moraine pins exact versions of delta-rs (`deltalake-core`), object_store, Arrow, Parquet and
DataFusion, because their types have to line up with each other. Filters are DataFusion
expressions, re-exported as `moraine::expr`. Index libraries are optional: usearch behind the
`vector` feature, turbovec behind `turbovec`, tantivy behind `fts`. Credentials through the AWS SDK are behind
`aws-sdk`. The workspace targets Rust edition 2024 with a minimum Rust version of 1.95, which
`deltalake-core` requires.

## What is not here yet

Partitioned tables are not yet supported, and neither are Google Cloud Storage and Azure Blob
Storage (see [S3 and bucket requirements](../guides/s3.md#other-object-stores)). Every edge case
the design must handle is listed in [Test and edge-case IDs](test-ids.md).
