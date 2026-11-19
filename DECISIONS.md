# Decisions

Every design decision, and every departure from the original design, is recorded here with its
reason, newest last. Spike findings that change the design are summarised here and detailed in
[`docs/SPIKE.md`](docs/SPIKE.md).

## D1. Name: moraine, not osdb

The original design's working name `osdb` is replaced. The Cargo package is `morainedb` (the bare `moraine`
name is taken on crates.io) and the library name is `moraine`, so code says `use moraine::…`.
The original design's `osdb-testkit` and `osdb-cli` become `moraine-testkit` and `moraine-cli`; the CLI
binary is `moraine`. Table properties use the `moraine.` prefix instead of `osdb.`
(`moraine.formatVersion`, `moraine.indexes`, …). The sidecar extension and magic (`.osix`,
`OSIX`/`XISO`) are left as the original design has them until the bundle format is defined (D33).

## D2. Licence: Apache-2.0 OR MIT

The original design's open question on licence defaults to "internal". I license my Rust projects under the
usual dual licence, Apache-2.0 or MIT at the licensee's option, so the workspace says so. The
repository stays local and unpublished (`publish = false` everywhere) until that question is
settled. It is settled now: `morainedb` sets `publish = true` for its first release, and the test
kit, the CLI and the spike stay unpublished.

## D3. Workspace additions: `spike/`, no `benches/` yet

`spike/` is a workspace member (`moraine-spike`, never published) holding the spike's
programs. It can be deleted once the table tests have turned the important checks into regression
tests. `benches/` from the original design's layout is created with the first benchmark rather than as an
empty directory.

## D4. Toolchain: edition 2024, MSRV 1.95

`deltalake-core` 1.1.4 declares `rust-version = 1.95`, so that is the workspace MSRV. Edition
2024 is the current edition for that toolchain.

## D5. Pinned versions

Exact pins live in the root `Cargo.toml` (`=x.y.z`) and the committed `Cargo.lock`:
`deltalake` 1.1.2 (resolves `deltalake-core` 1.1.4, `deltalake-aws` 1.0.0, `buoyant_kernel`
0.28.1), `object_store` 0.13.2, `arrow` and `parquet` 59.3.2, `datafusion` 55.2.0, `tantivy`
0.27.0, `usearch` 2.26.6, `tokio` 1.54.1. Arrow, Parquet, DataFusion and object_store are the
exact versions deltalake resolves to; using any other version would give two incompatible copies
of the types. `deltalake` is used with `default-features = false` plus `rustls` and `datafusion`
(merge, delete, update and optimise need DataFusion); `s3`, `gcs` and `azure` become features of
`morainedb` that switch on the matching deltalake features.

## D6. API names that differ from the original design

The original design's API names were written from memory. In the pinned crates:

- There is no `DeltaOps`; operations are methods on `DeltaTable` (`write`, `merge`, `delete`,
  `update`, `optimize`, `vacuum`, `set_tbl_properties`, `add_columns`, `create`).
- `CommitBuilder::build(Some(snapshot), log_store, DeltaOperation)` commits arbitrary actions;
  `CommitProperties::with_application_transaction(Transaction::new(app_id, version))` adds a
  `txn` action; `with_max_retries(n)` sets delta-rs's internal rebase attempts (default 15).
- Custom `moraine.*` properties need `CreateBuilder::with_raise_if_key_not_exists(false)` and
  `SetTablePropertiesBuilder::with_raise_if_not_exists(false)`; otherwise creation fails with
  "Error parsing property". They come back in `table_properties().unknown_properties`.
- `object_store` 0.13 moved `get`, `put`, `delete` and `head` to the `ObjectStoreExt` trait;
  the `ObjectStore` trait itself has `put_opts`, `get_opts`, `get_ranges`, `delete_stream`,
  `list`, `list_with_delimiter` and `copy_opts`.
- `parquet` 59 deprecates `ParquetObjectReader` (implement `AsyncFileReader` directly instead)
  and `ArrowReaderOptions::with_page_index` (use `with_page_index_policy`). The spike uses the
  deprecated forms; the sidecar reader should be its own `AsyncFileReader` over `object_store`, which
  it needs anyway for request counting and the footer cache.
- tantivy 0.27: `TopDocs::with_limit(n).order_by_score()`; `Searcher::search_with_statistics_provider`
  takes a `&dyn Bm25StatisticsProvider` directly.
- usearch 2.26: `Index::view(path)` maps a whole file; `unsafe Index::view_from_buffer(&[u8])`
  maps a byte slice, which is how a section inside a bundle can be used without copying.

## D7. Debug builds optimise usearch and tantivy

`[profile.dev.package.usearch]` and `[profile.dev.package.tantivy]` use `opt-level = 3`. Their
inner loops live in those crates (usearch's C++ is compiled with the package's optimisation
level), and unoptimised they make index tests far slower without making them more useful.

## D8. Commits: delta-rs's default log store, no DynamoDB, no custom `LogStore` (spike item 1)

delta-rs 1.x commits to S3 with `PutMode::Create` (`If-None-Match: *`) by default and no longer
supports a DynamoDB lock. The original design's fallback for item 1 is therefore what delta-rs already does,
and no custom `LogStore` is written. The capability probe stays. The real-S3 run (8 processes x 200
appends) is still owed and becomes a T7 regression test.

## D9. Metadata changes do not rebase over in-flight writes (spike item 7)

The original write-path design lists `add_column`, `create_index` and `drop_index` as rebasing, with in-flight
appends still committing (C10, C11). In delta-rs a committed metadata change fails every write
prepared before it ("Metadata changed since last commit"); the metadata change itself does rebase
over appends. The write path treats that error like a conflict: reload and re-execute, reusing
uploaded files when the new schema accepts them. C10's expected behaviour becomes "the append is
retried against the new schema and commits; the new column reads as null for its rows".
Index specs stay in table properties (`moraine.indexes`); `meta/indexes.json` is not used.

## D10. moraine fills missing nullable columns itself (spike item 7)

delta-rs rejects a batch that lacks a nullable column the table has. To give edge case D3's
behaviour, the write path adds null columns for missing nullable fields and reorders by name before
calling delta-rs.

## D11. Vacuum ages tombstones by server time (spike item 4)

delta-rs vacuum ages a removed file by the `deletionTimestamp` the removing client wrote with its
own clock, which breaks I7 and G1 when that clock is slow. moraine's vacuum builds its own list:
tombstones aged by the server modification time of the commit file that removed them, orphans aged
by object modification time, both compared with server time. Whether the final delete goes through
delta-rs or straight to the store is decided with maintenance (D50). Tables still set
`delta.deletedFileRetentionDuration` to the grace period, which also lets delta-rs's own vacuum
accept a 1-hour retention.

## D12. Parquet pages limited to 1,024 rows (spike item 8)

The original design's data-file settings (16,384-row groups, about 1 MiB pages) leave narrow columns with one
page per row group, so a row fetch by offset reads whole column chunks: 10.9 MB for 10 rows of a
1,000,000-row file. Adding a page row limit of 1,024 cut that to 1.7 MB (265 KB for two narrow
columns) at the cost of a larger page index (269 KB instead of 57 KB per million rows). The limit
is a table option with 1,024 as default; the footer cache covers the larger page index.

## D13. Checkpoints follow delta-rs's version rule (spike item 5)

delta-rs writes a checkpoint after the commit whose `version + 1` is a multiple of
`delta.checkpointInterval` (19, 39, …). The maintenance job's "checkpoint when the version is a
multiple of the interval" uses the same rule and only fills in checkpoints a crashed writer
skipped.

## D14. Transaction retention (spike item 6)

Tables set `delta.setTransactionRetentionDuration` to the grace period, so `txn` entries from
writer handles that no longer exist drop out of checkpoints. The ambiguous-outcome lookup happens
within one commit deadline, well inside that window.

## D15. Local filesystem gets a compare-and-swap wrapper (spike item 12)

`LocalFileSystem` supports `PutMode::Create` but not `PutMode::Update`. The store layer wraps it
with an `Update` emulation for single-host use, so leases work on local disk. Commits only need
`Create` and work unwrapped.

## D16. Vector search defaults are provisional (spike item 10)

With the original design's usearch defaults (connectivity 16, expansion 128 and 64), recall@10 on the spike's
synthetic clustered vectors was 0.55; `expansion_search` 512 reached 0.94. The original design's
`expansion_search` default of 64 was left open here, to be revisited with the vector index against
T9's recall bound on more realistic data. The D24 benchmark settled it at 128, which is what the code uses
(D37).

## D17. Performance is a requirement: moraine must beat Lance and a Delta engine

The sibling project kv0 measured its delta-rs engine against its Lance engine on the same tracker
workloads (2,000 runs and 4.8 million points logged one flush per run, a cold start-up that lists
the runs and reads each run's parameters and series, record lookups by item id and by run id).
Delta was 70-86 % slower at logging, 1.5-2.7 times slower at start-up and 2-7 times slower at
lookups by key; it won on disk size (about half), optimise time and memory. The causes it found
are the ones the original write and read paths would reproduce: several object-store round trips per commit
(a log listing, the commit write, post-commit work, an extra checkpoint listing; four table
commits per flush), point reads that open every file whose statistics overlap, no clustering
after compaction, and the same small tables re-read for every run.

A storage engine that is slower than both of the obvious alternatives on the paths users touch
most has no reason to exist, so performance becomes a design requirement rather than a late
tuning task. D18 to D20 set out how, D21 sets the targets, and D22 the harness that checks
them. Correctness still comes first: no target justifies weakening an invariant (I1 to I10).

## D18. Group commit is the default, and a commit makes no request it does not need

Changes the original write path ("Group commit inside one process") and the post-commit behaviour that
spike item 5 relied on.

- **Every write goes through the handle's commit queue.** There is no fixed window: while one
  commit is in flight, new writes queue; when it returns, everything compatible in the queue
  becomes the next commit. The original design's 10 ms window becomes an optional linger (default 0).
  Appends from any number of callers merge into one commit; their batches are concatenated,
  sorted by the table's cluster key and primary key (D19) and written as one data file per
  partition up to the target size, instead of one small file per caller. Upserts, deletes and
  updates merge per kind as the original design says; a mixed queue becomes consecutive commits in arrival
  order, so per-caller order is kept (C16). Each caller still gets its own result, and one
  caller's invalid batch fails only that caller (C17).
- **A group carries one transaction id.** The `txn` action is per handle (`appId` = writer UUID)
  with the group's sequence number as its version, so the ambiguous-outcome lookup (S1) answers
  for every caller in the group at once.
- **Request budget per commit.** With a current snapshot, an append commit costs one PUT per data
  file plus one conditional PUT of the next log entry, and nothing else: no listing, no HEAD, no
  read-back. When the put finds the version taken, the writer reads the following entries with
  GETs of `<version>.json` until one is missing (no listing), checks conflicts, and puts again.
  Snapshot refresh uses the same GET probing; a listing happens only when a handle opens, when
  `_last_checkpoint` is newer than the handle's version, or when a probed entry was already
  cleaned up.
- **Checkpoints and log cleanup leave the write path.** delta-rs's post-commit hook is switched
  off for moraine commits (no checkpoint, no expired-log cleanup, no extra listing); the
  maintenance job writes checkpoints at delta-rs's version rule (D13) and cleans the log. Any
  client may do it, so a table with no maintenance running only reads more log entries on open.
- **Requests are counted.** The store layer counts requests and bytes per operation (from the
  start). Since tables were added, a test asserts the exact request count of an append commit, a conflicting
  commit and a snapshot refresh on small fixed scenarios, so a regression fails the build. If
  delta-rs's commit path cannot meet the budget, the `CommitLayer` implementation writes the
  commit entry itself (`PutMode::Create` on `_delta_log/<version>.json`, which is what delta-rs
  does anyway, D8) and keeps using delta-rs for snapshots, conflict checks and merge planning.

Consequence for the tracker workload: one flush touches several tables, and Delta has no
cross-table commit (a non-goal). The commits for different tables of one flush are issued
concurrently, so a flush costs about one round trip of latency, not four.

## D19. Point lookups and cluster-key lookups are index-driven

Changes the non-goal "scalar secondary indexes", the original read path ("A local key cache is out of
scope for version 1") and the original compaction rules.

- **Declared keys.** A table may declare a primary key (as before) and one cluster key
  (`TableOptions::cluster_by`, for example `run_id`). Both are table properties.
- **Sorted data files.** Every file moraine writes is sorted by (cluster key, primary key). With
  the 1,024-row page limit (D12) the Parquet page index then works as a sparse index: one binary
  search over cached page statistics finds the page holding a key, and one range GET reads it.
- **File-level key ranges.** The snapshot keeps each file's min and max of both keys (from the
  `add` statistics moraine writes itself, spike item 3) in memory, so a lookup skips files whose
  range excludes the key without any request.
- **Key-index sidecar.** For each data file a small sidecar of a new kind, `KeyIndex`, in the
  same bundle format as the vector and full-text sidecars: an xor filter over primary-key
  fingerprints (about 9 bits per key) so files that merely overlap in range are rejected without a
  read, the first key of every page, and for the cluster key the row range of each distinct
  value. For a file that is not sorted (written by another Delta writer) it holds sorted
  (fingerprint, row offset) pairs instead. Writers build it before the commit (the file is small
  and the keys are in memory), so fresh files never fall back to a scan for key lookups; the
  index builder covers files written by others. A missing key index means bloom filter plus page
  statistics, still exact.
- **Clustering at compaction.** Compaction is a sort-merge by (cluster key, primary key) written
  by moraine and committed with `commit_files`, not delta-rs bin-packing, so after compaction the
  files' key ranges are disjoint and a run's rows sit in one file, usually in a few pages. This
  moves own compaction forward, into the maintenance work instead of after blobs.
- **Lookup cost target.** With warm caches (D20), `get(key)` costs at most one range GET of data
  per matching file, and a cluster-key read costs one range GET per file holding that value.

## D20. Per-version caches for snapshots, footers, key indexes and small tables

Changes the original cache design ("Data files themselves are not cached in version 1").

- **Snapshots are incremental.** A refresh applies only the new log entries to the snapshot in
  hand (D18); it never reloads from the checkpoint when it can avoid it.
- **Immutable things are cached by immutable keys.** Parquet footers with their page indexes, and
  decoded key-index sidecars, are cached in memory by (path, size) in a byte-bounded LRU shared by
  every table of a `Database`. Data files are never rewritten (UUID names), so entries are never
  invalidated, only evicted.
- **Small tables are materialised per version.** A table whose live data is below a threshold
  (default 64 MiB) is held in memory as Arrow batches for the version in hand, and reads filter in
  memory. When the version moves by appends only, the copy is extended with the new files; any
  other change rebuilds it. A tracker's start-up, which reads a run catalogue and then the
  parameters and series of every run, then costs one scan per table, not one per run.
- **Repeated queries do not re-list or re-fetch.** Within one version, nothing moraine has already
  fetched is fetched again while it is in the cache.

## D21. Performance targets are acceptance criteria

The comparison runs the same workloads on three engines on one machine, release builds only:
Lance and Delta Lake through kv0 (`KV0_ENGINE=lance` and `delta`), and moraine (D22). "Lance"
and "Delta" below mean those two kv0 engines. A feature a target applies to is not done until
`docs/BENCH.md` shows the target met, or a new entry here explains the miss and what replaces it.

| Workload | Target for moraine | Applies from |
|---|---|---|
| Append-heavy logging: 2,000 runs x 2,400 points, one flush per run, local disk | Wall time at most 0.7 x Lance; flush p99 at most Lance's p50 x 2 | Tables and appends |
| Requests per flush (S3-compatible server) | One data PUT per touched table plus one log PUT per table, zero listings | Tables and appends |
| Multi-writer appends, 8 processes, S3-compatible server | Commit p50 and p99 at most 0.5 x Delta and at most Lance; all rows present | Tables and appends |
| Multi-writer appends, 8 processes, local disk (replaced by D28) | Commits per second at least 2 x Lance; commit p99 at most 0.5 x Delta and at most Lance; every row present exactly once (raw row count) | Tables and appends; upserts and deletes |
| Cold start-up: list runs, then parameters and series of every run | At most 0.5 x Lance, before and after compaction | Tables (small-table cache); compaction |
| Full series and 100-step window | At most Lance before compaction; at most Lance after compaction | Tables; compaction |
| Point lookup by primary key, 1,000,000 rows | p50 at most 0.5 x Lance, p99 at most Lance | Tables (sorted files, footer cache); the key index |
| Lookup by cluster key (31 run ids out of 2,000, 1,000,000 rows) | At most 0.5 x Lance, before and after compaction | The key index; compaction |
| Disk size after compaction | At most Delta (about half of Lance) | Compaction |
| Compaction time and peak memory | At most Delta | Compaction |
| Vector search, recall at 10 of at least 0.95 | p50 and p99 at most Lance at equal recall | The vector index |
| Full-text search, ranking equal to a merged index | p50 and p99 at most Lance | The full-text index |

The ratios are against the same run of the harness, not against the numbers recorded in kv0.
They are first targets, to be tightened once moraine has numbers of its own.

## D22. The comparison harness: `bench/`, a workspace of its own

The original design's `benches/` (Criterion benchmarks and a load generator) is not built yet. The
comparison against Lance and Delta Lake that D21 makes an acceptance criterion lives in `bench/`,
package `moraine-bench`, and has run since before moraine had tables.

- **Its own workspace, sharing the root's target directory.** It depends on kv0 by path
  (`../kv0/crates/kv0`, features `aws` and `delta`, read-only use), which pulls in Lance, Arrow 58
  and protoc. Keeping it out of the library workspace means `cargo clippy --workspace` and the
  pre-commit hooks never compile Lance. `bench/.cargo/config.toml` points at `../target`, so no
  second target tree exists. Measurements use release builds only, and the release build is
  deleted after each measurement because the disk is shared.
- **The same workloads as kv0's benchmark gate**, through kv0's Rust API with `KV0_ENGINE`
  choosing Lance or Delta, and through moraine's own tables once those exist (until then
  the moraine engine reported "not yet implemented"): logging 2,000 runs x 2,400 points with one
  flush per run; cold start-up (runs, then parameters and series of every run); full and
  100-step reads of one series; compaction; all of these again after compaction; point lookups
  by key in 1,000,000 records; lookups by run id in 2,000 x 500 records; 8 writer processes
  appending to one table. Vector and full-text queries joined with the vector and full-text indexes.
- **How it measures.** Every phase is a separate process, so start-ups are cold and each phase
  has its own peak memory (`VmHWM`). On the S3-compatible server every child sends its requests
  through a counting proxy in the same process, so requests and bytes are counted outside the
  engines and identically for all three. On local disk, bytes read is the process's `rchar`
  (system-call reads, page-cache hits included) and requests are not counted. Sizes come from
  the directory or the bucket listing.
- **Differences from the gate's numbers.** The gate drove kv0 through Python; this harness calls
  the Rust API, so absolute times are lower and only the ratios are comparable with kv0's
  report. The WAL directory is inside the harness's work directory.

## D23. Test kit and store layer: details that differ from the original design

- **Error enum.** Adds `InvalidConfig` (case G6 needs a variant; the original design lists none).
  `is_retriable()` is true for conflicts, deadline, expired snapshot, throttling, timeouts and
  transient storage errors, and false for `OutcomeUnknown`, which must not be retried blindly.
- **Request metrics are counters on the store handle**, not the `metrics` crate. Tests need exact
  counts per handle and per operation kind (D18); a process-global recorder cannot give that.
  Exporting them through the `metrics` facade can be added with the public API.
- **The client clock carries randomness.** `moraine::clock::Clock` has `now`, `sleep` and
  `random_u64`; backoff jitter and probe object names come from it, so the simulation replays a
  seed exactly.
- **Retry rules.** Reads, listings, overwrites and deletes are retried on any transient error.
  Conditional writes are retried only when the request certainly did not take effect (throttled,
  connection refused); an ambiguous outcome goes back to the caller unchanged, because a repeated
  put-if-absent would report its own earlier success as "already exists". A 409
  `ConditionalRequestConflict` counts as ambiguous. Listings are collected completely before
  anything is yielded, so a listing that fails part-way is retried or returned as an error, never
  shortened (S4). Whole-object reads are buffered to check their length (S13); data files are
  read with range requests, so this costs memory only for small objects.
- **The commit retry loop came first, with the test kit** (`moraine::commit::commit_with_retries`),
  with the original design's table of commit outcomes: conflicts re-execute, throttling retries without counting
  as a conflict, an ambiguous outcome is always looked up before anything else, and an error is
  only returned once any open ambiguity is resolved. The toy counter and the Delta commit path
  share it. delta-rs's "Metadata changed since last commit" is classified as a
  re-runnable conflict (`ConflictKind::MetadataChanged`, D9); delta-rs error texts are matched in
  `ConflictKind::from_message` only, so no delta-rs type leaves the commit layer.
- **`SimStore` instead of `object_store`'s `InMemory`** under the simulation: last-modified times
  come from the simulated clock (tests jump past the grace period), and contents are ordered, so
  every decision depends on the seed alone.
- **Local compare-and-swap (D15) also fixes ETag reuse.** The wrapper locks the current file,
  compares ETags, and renames a new file into place. The first version lost about a third of
  concurrent increments: `LocalFileSystem` ETags are inode, modification time and size, the
  kernel records times at a coarse tick and reuses inode numbers, so a later version could get an
  earlier version's ETag. Each new version's modification time is now set at least one
  microsecond after the replaced file's.
- **CI is local.** The repository has no remote, so `scripts/check.sh` runs the per-commit set
  (format, clippy with warnings denied, all library and test-kit tests with the simulation).
  The pre-commit hooks run `cargo fmt --check` and clippy on any change to Rust sources or Cargo
  manifests; before this they also ran at the commit-message stage, where they always reported
  "(no files to check) Skipped".

## D24. Benchmarking turbovec against usearch for the vector sidecars

Changes the original index design ("Vector index (usearch)") and the vector index's acceptance criteria.

The per-file vector sidecar sits behind moraine's vector-index trait (`IndexKind`), so the index
library is a choice, not a fixed part of the design. I implemented two candidates side by
side and measures them, with Lance's vector index as the reference:

- **usearch** (the original design's choice, spike item 10): HNSW graphs, memory-mapped from the bundle.
- **turbovec** (crate `turbovec`, MIT, Rust): Google Research's TurboQuant, 2 to 4 bits per
  dimension, searched as compressed codes with SIMD. Quantisation is per vector and needs no
  training, so building a sidecar for a fresh or compacted file should be cheap.

Measured for both: sidecar build time (on ingest and on compaction rebuilds), sidecar size,
recall@10 with and without re-ranking the top candidates with full vectors read from Parquet,
query p50 and p99 for small and large files and for many-file counts, filtered search, and
behaviour after deletes (rewritten files). If turbovec wins on build cost and size at equal recall,
it becomes the default vector index and usearch stays as an optional feature; otherwise usearch
stays the default. The result and the choice are recorded here and in `docs/BENCH.md`.

What I already know about TurboQuant shapes the comparison. With a fixed rotation seed, building
the same rows gives byte-identical sidecars, and merging two files' sidecars is a concatenation
of their codes, so compaction need not re-quantise. The dimension must be a multiple of 8, and
codes take 2 to 4 bits per dimension: about 136 bytes per 256-dimension vector at 4 bits. The
index is rebuilt in memory from the codes on the first search of a file; it is not mapped and
used in place the way a usearch view is. That trade lands in resident memory and in the first
query against a file, so the benchmark reports both, next to warm latency, for both libraries.

**Result (D37):** usearch stays the default vector index and turbovec stays behind
its `turbovec` feature. turbovec builds 5 to 85 times faster (the larger the file, the more)
and its sidecars are 8.6 times smaller, but at a million rows it needs to rescore 8 candidates per hit to reach recall 0.95 and
then answers in 13 to 15 ms at the median against usearch's 2.4 to 4.3 ms.

## D25. Tables, append and read: details that differ from the original design

- **The commit layer is narrower than the original design's trait.** `CommitLayer` (crate-internal) knows the
  Delta log format and reads or writes one entry: encode an entry, put it with put-if-absent,
  read entry `n` by GET, load from a listing (and from a checkpoint through delta-rs), and turn
  an Arrow schema into its canonical Delta form. The commit protocol on top (rebase over other
  appends, transaction-id lookup, refresh by probing, the stale-snapshot guard) is moraine's, so
  D18's budget is enforced in one place and tested exactly. This is D18's fallback taken from the
  start: delta-rs's commit path lists the log and runs post-commit work on every commit. delta-rs
  still supplies the action types and their serialisation, the schema conversion and checkpoint
  loading, and stays the only crate that knows the format; no delta-rs type leaves `log/delta.rs`.
- **Append conflicts.** An append whose version was taken reads that entry and puts the same
  bytes at the next version without backoff, unless the entry is its own (an earlier ambiguous
  attempt landed) or changes the schema or table options. In that last case the write fails with
  `InvalidBatch` and nothing is visible; re-validating against the new schema (D9) arrives with
  `add_column` (D27), which is the first moraine operation that changes metadata.
- **Group commit has no background task.** The first caller to find no commit in flight leads:
  it commits the queue's prefix (up to the target file size) and hands leadership to the next
  waiting caller. A leader dropped mid-commit answers its group with `OutcomeUnknown` once the log
  put may have been sent (otherwise a plain failure) and passes leadership on. Waiting goes
  through `Clock::wait_for`, which the simulation steps through. The transaction `appId` is per
  `Database` handle (one random writer id), its version a per-handle group counter.
- **Eventual consistency refreshes lazily.** Instead of a background timer, a query refreshes
  first when the snapshot was last confirmed more than `max_staleness` ago. A read costs at most
  one GET per `max_staleness`, and nothing runs when nothing reads.
- **Stale snapshots list once.** A handle whose snapshot was last confirmed more than a commit
  deadline ago lists the log after its version before probing, because log cleanup (maintenance)
  may have removed the entry it would probe; a gap means a reload from the checkpoint. Writers
  apply the same guard before committing, so a commit can never land at a cleaned-up version.
- **The capability probe is cached per `Database`**, not per process: the simulation runs many
  stores with the same display name in one process. A process normally has one handle.
- **Not in the first table release:** partitioned tables (`partition_by` must be empty: `InvalidSchema`),
  `DuplicatePolicy::LastWins` (added with upsert), restarting a write with fresh uploads
  after `CommitDeadlineExceeded` (case C6, added with upsert; the error is returned and nothing is
  visible), and a streaming scan (`Scan::collect` returns batches; a stream follows with the
  search API).
- **Errors.** `PrimaryKeyEnforced` is new: `append` on a table that enforces its key (case C3).
  Arrow, Parquet and DataFusion errors have their own variants instead of `Internal`. Types Delta
  cannot represent (unsigned integers, for example) are `InvalidSchema` at creation, naming the
  type to declare instead.
- **Small tables have a row limit too.** D20's threshold is 64 MiB of live data; Parquet compresses
  metric points and records so well that tables of millions of rows fell under it, and
  materialising them made the first query slow and every later one filter millions of rows. A
  table is materialised only when it also has at most 250,000 rows
  (`DatabaseBuilder::small_table_rows`).
- **Statistics.** moraine writes full string bounds up to 256 characters; a longer maximum is cut
  and capped with U+10FFFF so it stays an upper bound. Timestamps are rounded outwards to
  milliseconds (Delta's precision). Files moraine wrote carry `tags["moraine.sortedBy"]`; bounds
  of other writers' files are widened (string maxima of 32 or more characters are ignored,
  timestamp maxima get one millisecond), because those writers cut strings without a cap.
- **Point lookups before the key index.** Data files carry Parquet bloom filters (1 % false
  positives, sized to the rows written) on primary-key and cluster columns, placed at the end of
  the file next to the page index. The footer cache fetches a file's last 128 KiB in one request
  (whole files up to 256 KiB), which holds footer, page index and bloom filters, so a warm
  `get` skips files by range, then by bloom filter, then reads only the pages the page index
  selects. Without key enforcement `get` returns the row committed last. The `KeyIndex` sidecar
  of D19 waits for the sidecar framework (D36).
- **T6 runs on local disk and the S3-compatible server (RustFS).** fake-gcs-server and Azurite
  wait until the `gcs` and `azure` features are wired to object_store, with the real-cloud runs.
- **Simulation.** Clients run with `io_concurrency(1)`: the scheduler admits one store call per
  task, and parallel requests inside one task would break its accounting. Several tasks may share
  a handle (that is how group commit is simulated). A restarted client process gets a fresh clock
  seed; otherwise it would reuse the crashed process's writer id and file names, which a real
  process never does.

## D26. The first table benchmark results, and the two contention targets not met

`docs/BENCH.md` ("Tables: append and read") has the three-way tables. Every target of D21 for tables and appends is met
except two, both about eight processes appending to one table as fast as they can:

- **Local multi-writer commit p50: 6.1 ms against 2.3 ms (Lance).** moraine commits about eight
  times as often as Lance (1,035 against 125 commits per second), and each commit waits for the
  others ahead of it on the log, so the median is the queue: eight writers times about 1 ms. Its
  p99 is 22 ms against Lance's 492 ms; Lance's distribution is split into fast commits and a few
  that back off for half a second.
- **S3 multi-writer commit p99: 2,427 ms against 2,351 ms (half of Delta).** A profile against
  RustFS (`backends::s3_contention_profile`, 8 handles x 20 appends) shows no failed request other
  than lost log puts: about six lost races per commit (13.9 requests per commit in the harness,
  open included). A writer that has just committed puts its next entry in one round trip, while a
  loser needs two (read the winner's entry, put again), so a writer can lose ten or more times in
  a row. The p50 (97 ms against 143 ms) is met.

Tried and reverted: a jittered pause after repeated lost races. It brought the local p50 to about
1 ms but the p99 to about 400 ms, because pausing losers only makes them lose more.

The harness change that made these numbers comparable is recorded here too: multi-writer
processes now open their handles first and start appending at one common instant. Before, each
started as soon as it had opened, and an engine with a slow start (Lance) barely overlapped its
own writers.

**What follows.** Upserts and deletes rework the retry loop for upserts and deletes, whose losers must
re-execute, so losing a race becomes more expensive there and a fair order among losing writers is
needed anyway; both rows are measured again as part of their acceptance, and the targets
stand. Candidates: let a loser read every entry up to the head before putting (GETs are cheaper
than failed conditional puts on S3), and have a writer that sees other writers in the entries it
reads leave one round trip of room after its own commit before its next put.

## D27. Upsert, delete and update: details that differ from the original design

- **Conflicts are moraine's own rules, checked entry by entry.** As for appends (D25), a write
  whose version was taken reads that entry with one GET and decides. An upsert, delete or update
  conflicts with an entry that changes metadata, removes a file the write removes, or adds a file
  holding a row the write matches; the last is checked exactly, by reading the added file's key
  or filter columns (statistics and cached bloom filters answer most cases without a request).
  Anything else rebases: the same entry is put at the next version. The original design's rule ("any commit
  that added or removed rows the merge read") would make every pair of concurrent upserts
  conflict once their key ranges overlap, which random keys always do.
- **Re-execution is incremental.** After a conflict the write re-executes against the newer
  snapshot, but a source file it already examined is not read again while it is live (files are
  immutable), and rewritten and newly written files are reused. All of it is discarded when the
  commit deadline restarts the write.
- **Rewrites start from the newest version.** Upsert, delete, update and `add_column` refresh the
  snapshot before executing (one GET when nothing is new), so a delete that finds nothing returns
  a version that includes every commit acknowledged before it started (case C19).
- **Group commit per kind, refined.** Upserts merge only with upserts whose keys they do not
  share, so every caller's rows commit whole and the order of callers inside a commit never
  matters; deletes by key merge with deletes by key, deletes by filter with deletes by filter (the
  filters are OR-ed). Updates and added columns commit alone: merging updates would make the
  result depend on their order inside one commit.
- **Updates cannot assign primary-key columns** (`InvalidQuery`): a changed key is a delete and
  an upsert, and keeping it so keeps uniqueness to one code path.
- **`add_column` (D9, case C10).** The column must be nullable. Adding a column that exists with
  the same type changes nothing and returns the current version; a different type is
  `InvalidSchema`. The metadata entry rebases over data commits and re-executes over another
  metadata change. An append in flight when a column is added keeps its uploaded files (they read
  the column as null), re-encodes its entry against the new schema and rebases without backoff; a
  queued append is conformed to the new schema when its group runs. Any other metadata change
  (from another Delta writer) re-conforms and re-uploads appends and re-executes rewrites.
- **Commit deadline (cases C6, C20).** The commit clock starts when a group's write starts. An
  attempt that finds it past the deadline, before or between log puts, stops; the write then
  starts once more with a fresh clock, fresh uploads, a fresh retry budget and the same
  transaction id (an earlier ambiguous put is resolved before the restart, so the id cannot be
  committed twice). A second overrun returns `CommitDeadlineExceeded` and nothing is visible.
- **API.** `upsert`, `upsert_with(batches, DuplicatePolicy)`, `delete(Expr)`,
  `delete_keys(&[Vec<ScalarValue>])`, `update(Expr, Vec<(String, Expr)>)` and
  `add_column(Field)`, each returning the committed version. `DuplicatePolicy::LastWins` keeps a
  key's last row in batch order.
- **Rewrites hold a file in memory.** A rewritten file is read whole, filtered or updated, and
  written again; streaming rewrites wait for compaction, which needs them anyway.
- **The simulation checks stored rows, not only reads.** The simulation reads every
  version's live files from the store without moraine and compares them with the model (I3 at
  every version), in addition to the reads clients made. Rows name the operation that wrote
  them, so upserts, appends, updates and added columns are placed by their trace; deletes and
  writes that changed nothing are placed by the version they were acknowledged with, and a
  version is accepted only if some order of its operations explains what it stores (the deletes
  that took effect first, everything else changing nothing). Upsert, delete and update were accepted with
  100,000 seeds of each of `mutations_random` (random faults), `mutations_no_faults`,
  `upsert_same_key_race` (scenario 2) and `schema_add_column_race` (scenario 15), built in
  release mode and run in 16 shards: about 11.7 million operations, 2.6 million injected faults
  (0.72 million of them ambiguous commits), no violation and no unknown outcome. The first full
  run failed eight seeds, all in the test's explanation or polling, none in moraine; they are
  regression seeds now.

## D28. A starvation guard for lost log races, and the multi-writer rows re-measured

Follows D26. `docs/BENCH.md` ("Multi-writer re-measured") has the tables: ten runs per backend, all three
engines in each, half of them with kv0's background maintenance off, medians over runs.

- **What the S3 profile showed.** Without any ordering, about 45 % of commits lost no race while
  the slowest lost up to 62 in a row: the same writers lose round after round. The p99 of seven
  runs ranged from 1,857 to 2,680 ms around the 2,350 ms target.
- **Fair orderings cost the median.** Eight writers at about 21 commits per second hold a commit
  for about 380 ms on average whatever the order (the number of writers divided by the commit
  rate). Ranking every put by the races its job had lost, so newcomers wait longest, brought the
  p99 to about 1,000 ms and the p50 to 340-470 ms, far above the 136 ms target. Pausing a writer
  that has just committed (D26's second candidate) left the p99 where it was or raised the p50.
  Reading every entry up to the head before putting again (D26's first candidate) changed
  nothing measurable and costs a GET per lost race, so it was dropped.
- **The guard.** While a handle's log puts are contended (it lost a race in this job, or its last
  commit lost one within the last second), a job that has lost fewer than two races per
  contending writer waits half a log-put round trip before each put; a job that has lost more
  (it has been passed over twice) puts at once and wins the next version. Contending writers are
  counted from the writer ids of the entries the handle lost to (the last 64), round trips are a
  moving average of its own log puts, and nothing waits when the round trip is under 2 ms. Only
  writers deep in a losing streak are reordered, so the median barely moves: on S3 the p99 fell
  to 1,148 ms (997-1,662) and the p50 rose from about 100 to 113 ms. Both S3 targets are met with
  kv0's maintenance on and off.
- **Local p50: not met, and what replaces the target.** On local disk moraine commits 1,470 times
  a second (Lance 270, Delta 113), with a p99 of 27 ms (Lance 321 ms), but its p50 is 3.5 ms
  against Lance's 2.4 ms. A commit loses five to six races at the median, each a read of the
  winner's entry and a put, about 0.6 ms together; the guard cannot divide a 0.3 ms round trip.
  The median of a busy, fairly served log is the queue, while Lance's comes from a split between
  fast commits and ones that back off for hundreds of milliseconds. Making moraine's median lower
  without more throughput needs the same split (the jittered pause of D26 did exactly that), which
  the p99 target and the starvation it causes rule out.
- **The local target, replaced (decided).** I dropped the local p50 target and replaced it with
  one that measures what a busy local log is for: on local disk, commits per second at least
  2 x Lance's, commit p99 at most min(0.5 x Delta, Lance) (the existing bound), and every row
  present exactly once, checked on the raw row count so a commit applied twice fails it. I chose
  throughput because the median of a fairly served queue is set by the number of writers, not by
  how fast the log is, and a lower median bought by starving some writers would break the p99
  bound. The S3 targets stay as D21 states them. moraine meets the new target (5.4 x Lance's
  rate). Raising local throughput further (fewer filesystem operations per lost race) is
  not done yet.
- **Fairness of the comparison.** kv0's maintenance was measured both ways because it runs inside
  the timed phase; turning it off made Delta slower on local disk and faster on S3, and left Lance
  about where it was. The multi-writer verification now also counts raw rows, and every moraine run stored each
  row exactly once, so the rebase by re-put (D25) never committed an append twice.

## D29. Hardening before indexes, and where the other adopted techniques go

I adopted a set of techniques from completype, a project of mine with a smaller storage layer of
the same kind, and placed each with the feature whose code it touches, as a deliverable and an
acceptance check of that feature, so no feature is finished with one open.

- **Hardening, before sidecar indexes.** Five items that need no index code and harden what exists:
  deterministic corruption fuzz tests that run with the normal tests on stable Rust (the nightly
  `cargo-fuzz` targets of T13 stay, but a parser regression should fail the next commit, not the
  next night); results bit-identical whatever the file segmentation, under concurrent commits
  (strengthens I6 before sidecars depend on it); not-found errors that suggest the closest name;
  the AWS SDK credential chain and bucket region resolution behind a cargo feature; object tags
  on every write. They come first because sidecar indexes add a parser (the bundle) and many more
  objects, and both are cheaper to get right before that.
- **With sidecar indexes.** Memory-bounded index builds, the TurboQuant facts of D24 in its benchmark, and
  `IndexKind` pluggable from outside the crate.
- **With full-text search.** Optional background snapshot refresh with cache prewarm and carry-over, and a
  soak test with a counting global allocator that proves the D20 caches stay bounded.
- **With maintenance.** Generation-file leases with fencing replace the compare-and-swap leases of
  the original maintenance design: every lease change creates `<job>/<generation + 1>.json` with put-if-absent,
  expiry stays in server time, and maintenance commits are fenced through a Delta transaction
  (`appId` the job, `version` the generation). Leases then need nothing beyond put-if-absent, so
  `PutMode::Update` and ETag support stop being requirements. Tiered compaction levels per file
  are kept in the Delta `add.tags`. An opt-in queued write mode (writers put batches into an
  inbox, a lease-elected ingestor commits them, exactly once through a Delta transaction per
  submitter `appId` and sequence) answers contention where many writers share one table.
- **The exception.** Leases and the inbox move forward, before sidecar indexes, only
  if the fair multi-writer rerun shows the commit rate collapsing as the number of writers grows.
  Otherwise contention is already handled well enough by D28 and they wait for maintenance.

## D30. Hardening: details that differ from the original design

- **Not-found errors carry what exists.** `Error::TableNotFound` holds a `NotFound` (the name,
  the closest existing name when it is within a third of the name's length in edits, and every
  available name), and a new `Error::ColumnNotFound` replaces `InvalidQuery("no column ...")` for
  projections, filters, deletes and updates. Finding the table names costs one listing of the
  database prefix, on the error path only; a failed listing leaves them out rather than hiding
  the original error.
- **Object tags.** `DatabaseBuilder::object_tags` sets up to 10 tags (S3's limits, checked at
  open) that a `TagStore` at the bottom of the store stack adds to every put and multipart
  upload, including log entries and capability probes. Tags ride on the write request, so the
  request-budget tests run every scenario with and without them and assert equal counts, and
  check that every write reaching the store carried them.
- **Corruption fuzz tests run with the normal tests.** `tests/fuzz.rs` in the test kit mutates,
  from a seed, the log entries, table properties, file statistics and stored objects of a small
  table that has seen every operation, with byte-level edits and edits that keep JSON valid but
  change one value (including JSON held inside strings). Log entries go straight into the parser
  and the snapshot built from them (through a hidden `moraine::fuzzing` module); stored objects
  go through the public API (open, scan, filter, key lookup, upsert). A counting global allocator
  bounds what one input may allocate. The default sizes (4,000 log inputs, 20,000 property and
  statistics inputs, 300 store inputs) take about 15 s in a debug build.
- **What the fuzz tests found.** A corrupt data file could panic inside the Parquet and Arrow
  crates in two places: decoding the Arrow schema embedded in the footer (an unverified
  flatbuffer) and slicing a page whose header lies about its length. moraine now skips the
  embedded Arrow schema (the table schema is in the log and every batch is cast to it), and runs
  all Parquet decoding of a file under a guard that turns a panic into `Error::Parquet` for that
  file. The guard depends on unwinding; a build with `panic = "abort"` loses it.
- **Segmentation invariance** is a simulation of its own (`tests/segmentation.rs`) rather than a
  property of every scenario: the same final rows reach one table through a single append and
  another through concurrent clients with random chunks, stale upserts, deleted junk rows, group
  commit and random page, row-group and file limits, and every query must return the same values
  bit for bit (floats by their bits) once in key order. Bytes under null slots are not compared;
  Arrow leaves them unspecified.
- **AWS SDK credentials and region (feature `aws-sdk`).** Without static keys or unsigned
  requests, an `s3://` database takes credentials from the SDK's default chain (profiles, SSO,
  `credential_process`, web identity, container and instance metadata), cached until five
  minutes before expiry; the storage option `aws_profile` picks the profile. Without a region or
  a custom endpoint, it uses the bucket's own region (an unauthenticated `HeadBucket` against
  AWS), falling back to the SDK's. A custom endpoint keeps the configured region, so S3-compatible
  servers behave as before. The feature adds `aws-config` and `aws-credential-types`; both are
  optional dependencies, so a default build is unchanged.

## D31. The fair comparison: what changed in the numbers, and the early-leases exception not triggered

`docs/BENCH.md` ("Fair comparison") has the method and the tables: five repetitions on local
disk, three on the S3-compatible server, every value read checked against what was written.

- **The headline is now kv0's layout with strong consistency.** I chose to report moraine
  storing metric points the way kv0 does, and reading with a log check per query, as the main
  column, because otherwise the comparison measures my table design and my staleness setting
  as much as the engine. moraine's own layout and eventual consistency stay as extra columns,
  since they are what a tracker built on moraine would use.
- **Where moraine loses.** On local disk, a full series read in kv0's layout takes 7.4 ms
  against 5.2 ms for compacted Lance (a window 8.8 against 5.6 ms): without compaction (not built yet),
  kv0's series id cannot prune files. Writing and rewriting records is 9 % slower than Delta
  locally (3.35 against 3.07 s) and level on S3. The S3 8-writer commit p99 is 1,405 ms against
  a bound of 1,283 ms: turning off Delta's background maintenance improved Delta's p99, which
  halved is the bound. I am keeping the target and recording the miss rather than loosening it;
  the starvation guard (D28) and the queued write mode of maintenance are where it gets fixed.
- **Where the old ratios overstated moraine.** Logging is still about 40 times faster locally
  and 5 times on S3, but kv0's write-ahead log, statistics rows and sequential table commits are
  in its numbers and not in the adapter's, so these are upper bounds. Series reads were about
  200 times faster locally in the old tables; in the same layout they are 21 times faster than
  uncompacted Lance and slower than compacted Lance.
- **The exception of D29 is not triggered.** D29 moves leases and the inbox before sidecar indexes only if the commit
  rate collapses as writers grow. It does not: moraine committed 885, 1,772, 1,032 and 2,577
  times a second locally and 18.4, 19.7, 21.5 and 20.7 on S3 at 2, 4, 8 and 16 writers, every
  row exactly once in every run. What grows is the p99 (425 ms to 3.5 s on S3), which is the
  queue of a log with a flat commit rate. Both stay with maintenance.

## D32. Caller transaction ids for exactly-once writes, pulled forward

kv0's write-ahead log has to be replayed exactly once: a process that crashes after a flush
committed, or that is told "outcome unknown", replays its log on restart, and every entry that
is already in the table must be skipped, whichever process wrote it. moraine already wrote a Delta
`txn` action per group commit (D18), but its id belongs to the handle and dies with it. Delta's
own answer is the application transaction id: the caller names `(appId, version)`, and a write
whose version is not above the newest one committed for that `appId` is not applied.

I pulled this forward from maintenance (where the queued write mode of D29 needs the same
mechanism per submitter) into a step of its own, before sidecar indexes, because kv0's move to
moraine cannot start without exactly-once replay. Contention is not the reason: D29 reserved that
early step for leases and the inbox if the commit rate collapsed as writers grew, and D31 records
that this trigger did not fire, so leases and the inbox stay with maintenance. The early step
holds this work instead.

- **API.** `Table::txn(app_id, version)` returns a `TxnWrite` with `append`,
  `append_unchecked`, `upsert`, `upsert_with`, `delete`, `delete_keys` and `update`, each
  returning a `WriteOutcome`: `Committed(version)`, or `AlreadyApplied { version, txn_version }`
  when the application has committed this version or a later one (`version` is a table version
  that holds the earlier commit). "Already applied" is not an error, and nothing is written. The
  methods without an id are unchanged. `Table::last_txn_version(app_id)` refreshes first (one GET
  when nothing is new); `TableSnapshot::last_txn_version` answers for its pinned version. I chose
  a value that carries the id over an options argument on every method because the id is per
  write and the result type changes with it; a builder keeps the plain methods' signatures.
- **Rule.** Delta's: the newest `txn` per `appId`, in log order, and a version at or below it is
  applied. Versions may skip but must grow, so an application sends its writes in order and
  retries each until it is committed or already applied before the next; a version sent after a
  higher one is reported applied, never written.
- **Exactly once, everywhere.** The check runs where the commit protocol already reads the log,
  so it adds no request: against the snapshot a job prepares on, and against every entry it loses
  a race to (a write commits only after reading each entry between its snapshot and its version,
  D25). A job that finds one of its ids applied stops as superseded; its group answers that member
  `AlreadyApplied` and commits the rest again. After an ambiguous put the existing lookup by the
  handle's own id still decides first (S1); if it cannot, the caller's id makes the next attempt
  safe anyway, because the earlier entry, if it landed, is either in the snapshot or in the way.
  Across processes and restarts the ids come from the log: every snapshot keeps the newest version
  per application id (shared, folded in batches, so applying an entry stays cheap), and a handle
  that loads from a checkpoint reads the checkpoint's `txn` column itself, which costs one more GET
  of the checkpoint on that path only (delta-rs looks ids up but cannot list them).
- **A write with an id always commits.** A delete or update that matches nothing, an upsert of no
  rows and `append_unchecked(vec![])` still put an entry that holds the id. Otherwise a replay
  could apply a delete to rows written after the original found nothing. `append_unchecked(vec![])`
  is therefore the id-only commit kv0's mark migration needs.
- **Group commit.** One entry holds one `txn` action per caller id, beside the handle's own. A
  write whose application is already in the group starts the next group (Delta allows one `txn`
  per `appId` per commit, and this keeps an application's writes in queue order). Members whose id
  the handle's snapshot already holds are answered before the group runs.
- **Retention.** Tables set `delta.setTransactionRetentionDuration` to the grace period (D14), and
  delta-rs, its kernel and checkpoint writers drop a `txn` older than that by its `lastUpdated`.
  Caller ids are written without `lastUpdated`, which Delta never expires, so a replay after a
  long outage is still recognised; the handle's own ids keep it and expire as D14 says. The
  interop test checks this through delta-rs: after a checkpoint, a caller id survives and an id
  with an old `lastUpdated` does not. moraine's own map applies no expiry, so it can only report
  an id as applied when Delta has forgotten it, never the reverse.
- **Not part of this work.** kv0 also needs free-form table properties and the latest commit
  timestamp next to the ids; they are small, independent, and wait for the step of kv0's move
  that uses them.
- **Acceptance.** `tests/txn.rs` (API, stale handles, every write kind, group commit and
  superseded members), a request-budget test (an id adds no request; a replay the snapshot
  answers costs none; several applications still share one commit), the interop test above, the
  model and I11 in the checker, and three simulation scenarios in `tests/txn_sim.rs`: processes
  that crash at a random request and resume from `last_txn_version` or earlier, log puts that land
  and lose their response followed by resends, and two processes (and two tasks of one handle)
  replaying the same logs at once. With the snapshot check disabled, the concurrent scenario
  fails on its first seed with an id applied twice. Exactly-once writes were accepted with 2,000 seeds of each
  scenario: 99,000 log entries, 189,000 attempts, 84,000 "already applied" answers, 5,300
  process crashes, 72,000 ambiguous log puts, 420 commits holding two applications' ids, and no
  violation. The first crash run failed one seed in the test itself: processes were seeded with
  `seed ^ life * GOLDEN`, and since SplitMix64 steps its state by that constant, two processes
  could draw the same data file names and the later upload overwrote a committed file. The
  seeds are hashed now; `tests/crash_points.rs` seeds its restarts the same way and has the same
  weakness.
- **deltalake aligned with kv0.** kv0 pins `deltalake-core =1.1.4`; moraine pinned the
  `deltalake` meta-package at `=1.1.2`, which is that package's newest release and already
  resolved to `deltalake-core` 1.1.4. moraine now depends on `deltalake-core =1.1.4` directly
  (under the name `deltalake`, so no code changed); the spike keeps the meta-package for its S3
  handlers. `cargo tree -d` shows no second Arrow, Parquet, DataFusion or object_store in
  moraine's graph. In the bench workspace, which builds kv0 and moraine together, there is one
  `deltalake-core`; the duplicates there (Arrow 58, DataFusion 54, object_store 0.14) all come from
  Lance and go when Lance does. kv0's own graph has DataFusion only through Lance, because kv0
  builds `deltalake-core` without its `datafusion` feature; moraine turns it on, so a combined
  build compiles DataFusion 55 for both.

## D33. Sidecar indexes: details that differ from the original design (bundles, specs and kinds)

- **Bundle header.** The kind is an 8-byte ASCII name rather than a `u8`, so a kind defined
  outside the crate needs no central number, and the header carries the kind's format version
  so a collector can tell an older library's bundle from a newer one's without computing
  hashes. Everything else is the original design's layout (64-byte header, 4,096-aligned sections, a table
  with XXH3-64 checksums, a 24-byte footer). A bundle with zero indexed rows is valid (V4).
- **Vector columns are `List<Float32>`.** Delta has no fixed-size list type; a
  `FixedSizeList<Float32, n>` column is stored as `List<Float32>` and comes back as one, so
  moraine refuses it at creation like any type the log cannot represent. The dimension is
  therefore a spec parameter (`params.dim`), and a vector of another length, a null, a
  non-finite value or (under cosine) a zero vector is skipped by every route rather than
  refused at write time: the original design's write-time check would tie table writes to index specs
  created later. Vector columns are written plain and uncompressed, as the original storage layout says.
- **Specs.** `IndexSpec` is generic: `kind`, `columns`, hashed `params`, unhashed `policy`
  (which files get a sidecar) and `search` (defaults a query may override). The kind resolves
  every default at creation, so a stored spec never changes meaning; unknown fields are refused
  rather than ignored. The spec hash covers the bundle version, the kind and its format
  version, the columns, the params and a library fingerprint (usearch's major and minor
  version; for turbovec the codes of a fixed probe).
- **The kind trait.** `IndexKind` keeps its original design with two changes: the exact
  route is an `ExactScorer` that scores batches the driver reads (so a kind does no I/O), and
  `merge` takes opened bundles with a `RowMap` rather than opened handles. A toy kind in the
  test kit uses only the public API, which is the D29 check that kinds plug in from outside.
- **Index changes commit outside the write queue.** `create_index`, `drop_index` and
  `replace_index` write a metadata-only entry (`SET TBLPROPERTIES`) through the same retry loop
  as writes, reapplying the change after a lost race; appends rebase over it because the schema
  is unchanged. This keeps the exactly-once write path (D32) untouched.
- **Default cache mode is memory (1 GiB).** The original design's default is a 10 GiB disk cache, but a
  library has no directory it may assume; `DatabaseBuilder::sidecar_cache` turns the disk mode
  on, and a directory that cannot be written fails at open (K8).
- **Search results** carry the selected columns plus `_moraine_score` (a distance for vector
  kinds), and `SearchResult` also returns the hits (file, offset) and how many files took each
  route.
- **Key lookups never list on their own account more than once per poll interval.** A
  handle uses the key indexes it already knows (from a listing, or its own inline writes) and
  lists `idx/` again only when its listing is older than `sidecar_poll_interval`, whatever the
  version, so `get` keeps its request count.

## D34. TurboQuant and xorf, checked against their sources

Two facts about TurboQuant and xorf were verified before code relied on them.

- **turbovec scores inner products only.** `TurboQuantIndex` has no metric parameter; its
  search returns estimated inner products, best first, and its documentation says to normalise
  for cosine. `vec-tq` therefore normalises vectors and queries under cosine, uses
  `1 - score` as the distance under both cosine and inner product, and refuses L2. The rotation
  seed is a constant in the crate (`ROTATION_SEED`), the dimension must be a positive multiple
  of 8 up to 16,384, and bits are 2 to 4. Tests now check the D24 facts: byte-identical sidecars
  for equal rows whatever the batch split, merged codes equal to the merged file's sidecar,
  136 bytes per 256-dimension vector at 4 bits (128 of codes, 4 of scale, 4 of row offset), and
  codes copied into memory on open (a usearch view holds a few kilobytes).
- **A xorf binary fuse 8 filter serialises as a 20-byte descriptor and the fingerprints.**
  `BinaryFuse8` has public `descriptor` and `fingerprints`; `DmaSerializable` writes the
  descriptor as seed (`u64`), segment length, segment length mask and segment count length
  (`u32` each), little-endian, and `BinaryFuse8Ref::from_dma` reads it back without a copy.
  Construction is deterministic (its seed generator starts at a constant). moraine writes that
  layout through `DmaSerializable` rather than serde, and checks the descriptor on open
  (segment length a power of two, the mask one less, enough fingerprints for every index a
  lookup can compute), since a lookup indexes the fingerprints without bounds checks. The
  measured false-positive rate is 0.39 %.

## D35. Memory-bounded builds: windows of input, measured estimates

The original design's mechanism holds (a byte semaphore shared by the database's builds, parts sized from
a per-row estimate, a part sealed when the builder reports its budget), with two findings from
measuring the peak resident memory of a build in its own process.

- **Input in windows.** Reading a row group at a time fetched whole column chunks: 16 MiB for
  16,384 rows of 256-dimension floats. Alternating those buffers with small allocations let
  glibc's dynamic mmap threshold grow the heap by 50 to 100 MiB, varying from run to run. The
  builder now reads each row group in windows of about 4 MiB through the page index, and the
  growth is flat.
- **Estimates from measurement.** usearch reports about 1.9 KiB per 256-dimension F32 row at
  connectivity 16; the estimate is twice the vector plus its base-layer links (2.25 KiB).
  turbovec's codes grow by doubling and it keeps working copies while encoding, so its estimate
  is four times the code, scale and offset bytes, and the builder counts capacity, not length.
- **Result.** A 300,000-row, 256-dimension file (300 MiB of floats) built under a 64 MiB
  budget grows the process by 57 to 61 MiB with usearch (13 parts) and 73 MiB with turbovec
  (4 parts), under the bound of the budget plus 24 MiB. The test reads `VmHWM`,
  so it runs on Linux only.

## D36. The key index: order-preserving key bytes, pages only for key-sorted files

- **Key bytes preserve order.** The first encoding wrote strings and binary length-prefixed;
  that breaks byte order (`"b"` would sort before `"aa"`), and the `pages` section binary-
  searches first keys. Strings and binary escape `0x00` as `0x00 0xFF` and end with
  `0x00 0x00`; integers, dates and timestamps are 8 bytes big-endian with the sign bit flipped;
  floats are their bits in total order; nulls are a tag alone and sort first. The encoding is
  pinned by a unit test and the key goldens.
- **Pages only for files sorted by the primary key alone.** With a cluster key, files are
  sorted by (cluster key, primary key), so first keys of pages are not in key order; those
  files get `pairs`, like files written by other Delta writers. The builder detects order from
  the rows it sees rather than trusting `moraine.sortedBy`. Files with a cluster key also get
  the `cluster` section when each value forms one run.
- **Lookups.** `get` asks each candidate file's key index first: an absent key costs no read
  (the filter's 0.39 % false positives, then `pairs` or `pages`), a present one a fetch of its
  page or rows. Without a known key index the read is the path from before key indexes (bloom filter and page
  statistics), still exact. With warm caches an absent key over five overlapping files costs no
  request and a present one at most two range reads.
- **Who builds it.** The writer builds it inline for files of at least
  `key_index_inline_min_rows` rows (default 100,000), in the same round trip as the data PUT;
  smaller files keep the one data PUT and one log PUT per flush, and `build_indexes` covers them.
- **Not yet used by scans.** Cluster-key reads still go through page statistics, which on
  sorted files already narrow a value to its pages; the `cluster` section is built and tested
  so compaction and the cluster-read target can use it.

## D37. The D24 benchmark: usearch stays the default; vector defaults; the D21 vector miss

`docs/BENCH.md` ("Vector sidecars") has the method and the tables; the raw results
are `bench/results/2026-11-17-vector.jsonl`. Synthetic clustered vectors (64 Gaussian
clusters), 256 dimensions at 100,000 and 1,000,000 rows and 768 dimensions at 100,000, each in
1, 10 and 100 files; 1,000 held-out queries, k = 10, exact ground truth by brute force; release
build, local disk.

- **The decision rule of D24 fails for turbovec.** At recall@10 of at least 0.95,
  turbovec (4 bits) builds 5 to 85 times faster than usearch, the more the larger the file
  (2.2 s against 187 s for a 1,000,000-row file on one thread), and stores 136 bytes per
  256-dimension vector against 1,173. Its cold first query is faster too (0.5 to 1 s against
  3 to 6 s at a million rows: less to download). But it scans every code, so its latency grows with the rows of a file, and at a
  million rows it needs a rerank of 8 for 0.965: p50 13 to 15 ms and p99 22 to 30 ms, against
  Lance's 3.1 and 5.1 ms on the 100-file layout. The rule asks that it still meet the D21 vector
  target there, so usearch stays the default. turbovec remains a feature for tables that are
  rebuilt often (fresh files, compaction: its merge concatenates the codes of two 10,000-row
  files in 2 ms, where usearch rebuilds the 20,000 rows in 2 s) and where a few milliseconds more per query are acceptable.
- **Where turbovec is weaker than expected.** Its first query against a file whose bundle is
  already on disk takes 200 to 400 ms (copying the codes and preparing the search layout),
  where usearch maps its graph in 10 to 80 ms; the handle cache keeps this to the first query
  per file. 2 and 3 bits never reach 0.95 on these sets, with any rerank tried.
- **Defaults (D16 revisited).** `expansion_search` stays at 128 (D16 left 64 open): 64 already
  gives 0.96 to 1.0 on every set here, 128 gives 0.994 to 1.0 for 0.1 to 0.9 ms more, and T9's bound
  must hold at the defaults, not at hand-tuned values. usearch's `rerank` default stays 0 for
  F32 and 2 for F16 and I8. turbovec's `rerank` default becomes 8 (it was 4: 0.90 at a million
  rows). F16 and I8 save 45 % and 65 % of the sidecar; I8 loses recall at 768 dimensions (0.91
  at best here), so F32 stays the default quantisation.
- **Vector pages of 64 KiB.** Rescoring fetches the candidates' vectors, and each fetch decodes
  a whole Parquet page. With the 1,024-row page limit of D12 a 256-dimension page is 1 MiB, and
  rescoring 30 candidates took 9.5 ms; vector columns now have pages of at most 64 KiB (64 rows at
  256 dimensions), and it takes 1.1 ms. The page index grows accordingly.
- **The D21 vector target is met at 100,000 rows and missed at a million.** Lance's IVF-PQ needs
  a quarter as many sub-vectors as dimensions and a refine factor to reach 0.95 (with Lance's
  dimension-over-eight setting it never got there at a million rows). At 100,000 rows of 256
  dimensions, usearch answers at a p50 of 0.9 to 2.7 ms and a p99 of 1.5 to 4.9 ms against
  Lance's 6.7 and 15.5 ms. At a million rows Lance answers in 3.1 and 5.1 ms, usearch in 2.4 to
  4.3 and 7.8 to 9.2 ms: the p50 is met on the 10-file layout only, the p99 on none. At 768
  dimensions the 100-file layout misses too (6.8 and 9.8 ms against 5.6 and 7.6). I am keeping the
  target. What should close it: a file built under the default 256 MiB budget is 9 graphs at a
  million rows, each searched in turn; searching a file's parts concurrently, and a build budget
  that keeps a compacted file in one or two parts, belong with compaction (which decides file
  sizes), and the benchmark reruns then.
- **Filters and mixed routes.** With 10,000-row files a 50 % filter leaves 5,000 rows per file,
  below the 10,000-row `exact_filter_threshold`, so every file goes exact (recall 1.0, p50 20 ms
  at 100,000 rows); half the sidecars removed gives recall 0.98 to 1.0 at about 23 ms. Both are as
  specified; a threshold relative to the file's rows is an option once compaction decides file
  sizes.
- **Determinism.** Rebuilding the same rows gives byte-identical sidecars for both libraries
  (single-threaded usearch builds included), as T12's goldens also check.

## D38. A commit error that does not say "not written" is ambiguous; equal keys keep their order

- **Ambiguous commit errors.** A conditional log PUT that failed with an error moraine could not
  classify (`object_store::Error::Generic` with an unrecognised message, or any variant that
  does not say the write was refused) was treated as permanent, so a commit that had in fact
  landed was reported as an error without its transaction id being looked up. kv0's port found
  it by injecting such an error after the write took effect. Now only errors that say the write
  did not happen end an attempt: conflicts, refusals before the effect (throttling, an
  unreachable store) and requests the store rejected outright (permission, authentication, an
  invalid path, an unsupported mode, a missing bucket). Everything else is ambiguous and
  resolved like a timeout: the transaction id is looked up, and `OutcomeUnknown` is returned when
  the lookup keeps failing. `ChaosStore` gained the fault (`generic_after_commits`, also drawn in
  random runs), and the scenario `generic_error_after_commit_is_resolved` fails on the old
  classification and passes on the new one.
- **Equal sort keys keep their append order.** The sort of a data file's rows by (cluster key,
  primary key) has always been stable (a row-number tie-breaker); it is now a documented
  guarantee on `TableOptions::cluster_by` with tests: rows of one commit with equal sort columns
  keep batch order and row order, and callers merged into one group commit keep their arrival
  order. kv0's newest-wins reading depends on it.

## D39. Follow-ups from porting kv0: pins, file lists, part-parallel search, checkpoints, list items

Five gaps came up after sidecar indexes landed, most of them while porting kv0 onto moraine (D38).
None blocks the vector index; each now goes in with the feature it belongs to, as a deliverable
and an acceptance check.

- **Named pins (with maintenance).** kv0 tags versions of a table and reads them later. Vacuum will
  remove files that no current version references, so a tag needs the files of its version kept.
  I am adding named pins rather than general time travel: `Table::pin(name, version)`,
  `Table::unpin(name)` and `Table::pins()`, and vacuum keeps every file a pinned version
  references. Time travel to any version would make vacuum keep everything; pins keep only what
  someone named. kv0's tags become pins.
- **A public file list and a cheaper own-commit snapshot (with full-text search).** `TableSnapshot`
  exposes the number of files and rows but not the files themselves, which a caller that keeps
  per-file bookkeeping needs, so it gets a public list of its files. A handle that has just committed knows the entry it wrote; taking its snapshot right
  after the commit should not cost another GET of the log.
- **Concurrent search across graph parts (with maintenance).** D37 traced the D21 vector miss at a
  million rows to a file built as 9 graphs, each searched in turn. Searching a file's parts
  concurrently is the fix, alongside compaction deciding file sizes, and the D24 benchmark
  reruns there.
- **Checkpoints (with maintenance).** moraine reads checkpoints but does not write them yet, so
  opening a table that only moraine has written reads every log entry since version 0. The
  checkpoints that maintenance writes bound that.
- **List item names (any time).** Arrow names a list's item field `item` by default and
  Delta names it `element`; the guides build vector columns with `element`. moraine should
  accept the Arrow default, either as is or by renaming the field to `element` on write. It is
  small and not tied to other work; it goes in with full-text search, the next piece of work.

## D40. The full-text kind: details that differ from the original design

- **Sections served in place.** The original design unpacks a full-text bundle into a cache directory and
  opens it with tantivy's `MmapDirectory`. A read-only tantivy `Directory` over the bundle bytes
  does the same without a second copy on disk, and works unchanged in the memory cache mode: the
  bytes are already mapped and 4,096-aligned, and every file tantivy opens is a slice of them.
  tantivy's owned-bytes type needs the `stable_deref_trait` marker for that, the one new
  dependency besides tantivy itself.
- **One segment, document id = row offset.** The writer runs on one thread with no merge policy;
  every part the build driver seals is a committed segment, and the finished build merges the
  parts in row order into one segment. A document id is then the row offset, which the build
  checks against the `_row` fast field before packing. The sections are `meta.json`, with the
  segment id replaced by zeros, and `seg.<extension>` per segment file, so rebuilding the same
  rows gives a byte-identical bundle; a build split into parts has the same segment files and
  differs only in tantivy's commit stamp.
- **The search path is the kind's own.** The generic per-file route (a sidecar, or an
  `ExactScorer` over batches) cannot express BM25 with table-wide statistics: every file must be
  opened before any is scored. `search_text` therefore has its own driver: a searcher for every
  file of the snapshot (sidecar, or the in-memory index the original design describes), the summed
  statistics, then the per-file searches. An outside kind cannot do this two-phase search
  through the public traits yet; I left the traits as they are rather than generalise from one
  case.
- **Statistics cover the whole table, whatever the filter.** A filter only drops rows, as it
  would in a merged index, so a row's score does not depend on the filter. Files the filter's
  statistics rule out are not searched but still contribute their statistics, so they are
  opened (or indexed in memory) too.
- **Ties rank by row.** The per-file collector keeps the best `k` by score, then row, with
  tantivy's block-max pruning (a document must beat the current `k`-th score strictly, and
  documents arrive in row order). The global merge breaks ties by file path, then row, as for
  vectors, so every route ranks equal scores the same way.
- **Boosts and the default operator are search defaults.** The original design's `TextField` carries a
  boost in the spec. A boost changes no sidecar, so, as D33 does for `expansion_search`, it lives
  in the unhashed `search` object (`boosts`, `conjunction`) and a query may override it.
  `params` holds what the sidecar depends on: per column a tokenizer and what is recorded
  (`basic`, `freqs`, `positions`), and `max_indexed_chars`.
- **Tokenizers.** Built in: `default`, `raw`, `whitespace`, eighteen Snowball stemmers
  (`en_stem`, ...) and `ngram2`/`ngram3` for case F11. Their names are fixed: registering a
  custom tokenizer under a built-in name is refused, because a spec hashes the name, not the
  behaviour. An unknown name is `TokenizerNotRegistered` at index creation and at search.
- **Queries.** A query that is empty, tokenises to nothing, or holds only excluded terms returns
  no rows (case F1 extended: tantivy refuses a query of exclusions alone, and no row could match
  it). Column names that the query syntax cannot name (whitespace, `:` `.` `^`, quotes, brackets,
  a leading `-` `_` `+`) are refused at index creation.
- **Build memory.** The writer holds 32 MiB; parts are sized at 2 KiB per row of the build
  budget, and each sealed part becomes a segment on disk, so a large file's build stays near the
  writer's memory. Builds use a private directory under the system temporary directory, removed
  when the build ends.

## D41. Full-text ranking equals one merged index (T9, case F5)

`tests/fts_index.rs` builds random corpora (Zipf-distributed words from vocabularies of 8 to 61
words, 1 to 9 files of up to 400 documents, null and empty values, `default` or `en_stem`,
`positions` or `freqs`), gives a random share of the files sidecars and leaves the others to the
in-memory route, and checks each of 12 random queries per corpus (terms, field terms, phrases,
`AND`/`OR`, required and excluded terms, boosts, the conjunction default, filters, `k` from 1 to
39) against one tantivy index over all rows built by tantivy alone. A ranking is equal when it
has the same number of hits, every hit's score is its merged score, the `i`-th scores agree, and
every row the merged index ranks strictly above the `k`-th score is present; only rows with
equal scores may be ordered differently.

Over 200 corpora and 2,400 queries (45,495 hits, 5,772 searches of sidecar files and 8,196 of
in-memory ones) every ranking was equal. 2,165 of the 2,400 were equal to the last bit; the
largest score difference was 1.9e-6, from the order in which a multi-term query adds its terms'
scores, well inside the 1e-5 of case F5. The default run uses 40 corpora; the simulation
scenario of D44 checks the same against the final rows of every seed.

## D42. Hybrid search: reciprocal rank fusion, hits first

`search_hybrid` runs both searches against one snapshot, each for `k * 4` hits with the same
filter and without fetching rows, fuses them with reciprocal rank fusion (`1 / (60 + rank)`,
summed over the lists), breaks ties by file and row, and fetches the rows of the fused top `k`
once. This is the original design's method; I kept it because it needs no calibration between a distance
and a BM25 score. The fusion is a trait (`Fusion`) that receives both ranked lists, so weighted
or normalised fusions can replace it; `oversample` and each search's parameters are per query.

## D43. Background refresh, prewarm and carry-over (D29)

- **A consistency mode.** `Consistency::Background { interval }` starts a task per table handle
  that refreshes every interval; queries use the snapshot in hand like `Manual`, so the first
  query after a commit makes no snapshot request. If the task has not confirmed the snapshot for
  four intervals and a second (the store unreachable), a query refreshes by itself, so a stuck
  task cannot freeze a reader. The task ends with the last clone of the handle or a mode change,
  and needs a Tokio runtime.
- **Prewarm.** For each new version: footers and page indexes of the added files, the sidecar
  listing at the new version (so the next search does not list), the added files' sidecars of
  every index, the in-memory full-text indexes of added files without one, and a small table's
  in-memory copy.
- **Carry-over needs no code of its own.** Every cache is keyed by an immutable data file (path
  and size) or a sidecar (key and ETag), so entries of files a new version keeps stay valid; the
  small table was already extended rather than rebuilt after appends (D20). Removed files' entries
  age out of their LRUs.
- **Tests, not simulation.** The task runs on the Tokio runtime with the database's clock; the
  tests run it in real time with a 20 ms interval and count requests after the prewarm of a
  version: a scan and a full-text search after another writer's commit make none at all.

## D44. D39's items done with full-text search, and the simulation of indexing under writes

- **The snapshot after an own commit.** A handle's commit already moved its snapshot to the
  committed version (it applies its own entry, D18); the GET kv0 paid came from strong
  consistency, under which `snapshot()` must read the next entry because another writer may have
  committed since. I kept that meaning and added `Table::snapshot_in_hand()`, which returns the
  handle's snapshot without any request in every mode. The request-budget test pins it: an append
  and the snapshot after it cost one data PUT and one log PUT, also after a lost race (plus the
  GET of the winner's entry, as before), and `TableSnapshot::files()` costs nothing.
- **List item names.** Table creation and `add_column` store every list item field, at any depth,
  under Delta's name `element`, and a batch whose list column differs only in item field names is
  cast on write. Arrow's default `item` appends, reads back, and delta-rs reads the table.
- **Simulation.** `tests/fts_sim.rs` runs, under the scheduler, two or three writers bringing a
  keyed table to a known final state through appends, upserts that rewrite files (stale versions
  corrected later) and deletes of junk rows, a builder building sidecars between their commits,
  and a reader comparing, at every version it sees on two clients, the answers with sidecars and
  without. At the end a fresh client's rankings must equal a merged tantivy index over the final
  rows. There was no compaction yet, so the merges are Delta's copy-on-write
  rewrites by upsert and delete. 10,000 seeds in 16 shards passed: 99,082 sidecars built while
  writes ran, 40,863 comparisons of answers with and without sidecars at the same version, and
  60,000 final queries equal to the merged index.

## D45. The soak test (D29)

`tests/soak.rs` installs a counting global allocator and loops: appends of 60 and 400 rows, a
delete of old rows every 8 rounds (files rewritten), index builds every 4, a filtered vector
search, a full-text search, a hybrid search and a filtered scan per round, against a handle that
refreshes in the background every 10 ms, with a 4 MiB file cache, a 16 MiB sidecar cache, 16
open sidecars and 8 MiB of in-memory full-text indexes. Every round checks each cache against its
limit; after the first quarter the live heap must stay within 24 MiB of the first quarter's peak.
The default is 240 rounds. A 1,000-round run wrote 173,960 rows and picked up 1,083 versions in
the background; the live heap measured 12, 14, 12 and 14 MiB after a quarter, half, three
quarters and all of the rounds, with a 31 MiB peak in the warm-up and 33 MiB after it.

## D46. The full-text benchmark

`docs/BENCH.md` ("Full-text sidecars") has the method and tables; the raw results
are `bench/results/2026-11-18-fts.jsonl`. Synthetic Zipf documents (50,000-word vocabulary,
20 to 120 words), 1,000 queries of one to three words, k = 10, at 100,000 and 1,000,000 rows on
1, 10 and 100 files; the reference is one tantivy index over all rows built by tantivy alone;
Lance's inverted index is the comparison.

- **Ranking equality (D21: "ranking equal to a merged index") is met.** All 1,000 queries ranked
  as the merged index on every layout, with sidecars and with every file indexed in memory; the
  largest score difference was 9.5e-7.
- **The D21 latency target (p50 and p99 at most Lance) is met at a million rows** on every layout
  (p50 1.53 to 1.57 ms against 2.21 ms; p99 2.74 to 4.12 ms against 13.84 ms), and at 100,000
  rows on 10 and 100 files. **It is missed on one file of 100,000 rows at the median**: 0.89 ms
  against Lance's 0.75 ms (p99 1.62 against 3.68, met). I am keeping the target. A search's fixed
  cost, which the merged index does not pay (0.05 ms at the median there), is the snapshot's
  searchers, the summed statistics and the fetch of the hits' rows from Parquet; with more files
  the per-file searches run in parallel and hide it. Caching the summed statistics per snapshot
  and query terms, and reading the selected columns of the top hits in one request per file
  (they are often in one page), are what I would try first.
- **The merged index itself is faster.** It answers in 0.05 / 0.67 ms at 100,000 rows and 0.31 /
  4.14 ms at a million; moraine reproduces its ranking at a cost of 0.5 to 1.2 ms at the median.
  At a million rows moraine's p99 is equal or lower, from searching files in parallel.
- **Builds are slower than Lance's.** A million rows build in 6.3 to 10.7 s (merged tantivy on
  one thread: 9.9 s) against Lance's 2.7 s, and the sidecars take 151.8 to 213.0 MB against the
  merged index's 146.6 MB and Lance's 102.0 MB; per-file indexes repeat each file's term
  dictionary.
- **First queries.** Opening every sidecar from the disk cache costs 77 ms to 1.5 s on the first
  query (more files, more opens); with no sidecar, indexing the files in memory costs 0.2 to 4.2 s,
  once, which background refresh (D43) moves off the query path for new files.
- **Lance ranks differently.** Its top 10 shares 94 to 96 % of the merged index's, and the same
  first hit for 904 to 938 queries of 1,000: its BM25 statistics and parameters are not tantivy's.
- **A bug the benchmark found.** tantivy flushes a segment whenever its writer memory is full,
  not only on commit, so a large part could hold several segments and the build failed; builds
  now merge every segment in row order (commit "Merge every segment of a full-text build in row
  order", with a regression test).

## D47. Checkpoints: written and read by moraine, at the version in hand

- **moraine writes its own checkpoints.** delta-rs checkpoints through the kernel, which loads
  the table by listing and reading the log itself (outside moraine's store stack, its request
  counts and the simulation's scheduler) and keeps none of moraine's `add.tags`. moraine writes
  the state of its snapshot as a classic single-file checkpoint: the actions it would write as
  log lines, decoded into the Delta checkpoint schema with Arrow's JSON reader and written as
  Parquet. It reads checkpoints the other way round (Parquet rows to JSON lines into the log
  parser), so one parser knows what an action means, and falls back to delta-rs for checkpoints
  it cannot decode (other writers' multi-part or V2 checkpoints). delta-rs reads moraine's
  checkpoints (interop test). Checkpoints hold no `remove` tombstones: moraine's vacuum ages
  removals by the log entries that made them (D50), and delta-rs's vacuum by file age.
- **At the version in hand, not at delta-rs's version rule (changes D13).** A maintenance pass
  checkpoints the version it has once `delta.checkpointInterval` entries have accumulated since
  the newest checkpoint, with put-if-absent, and only then points `_last_checkpoint` at it.
  Writing at exactly the versions delta-rs would (19, 39, ...) would need the state at an older
  version, which costs a reload.
- **Transaction ids expire from checkpoints (D14).** Ids with a `lastUpdated` (moraine's own
  group ids) older than the grace period are left out; callers' ids, lease fences and queued
  submitters carry none and stay.
- **Opening reads `_last_checkpoint`** and lists the log from the checkpoint it names, so a long
  log costs one listing of its tail, the checkpoint and the entries after it (four GETs and one
  listing in the test with two entries after the checkpoint). A missing, stale or unreadable hint
  costs a full listing, never correctness. A gap in a listing is read by GET rather than
  reported: T8 found a local directory listing that missed an entry other processes had
  created, on the overlay filesystem the tests run on.
- **Log cleanup** is delta-rs's rule on server time: entries and checkpoints older than the
  grace period, and older than the newest checkpoint at or before the oldest entry inside the
  window, go.
- **A bug the simulation found.** A handle idle for longer than a commit deadline listed the log
  after its version before probing (D25), but counted only JSON entries: when cleanup had removed
  every entry before the newest checkpoint, the listing looked empty, a probe found nothing, and
  the next commit put a new entry at a version cleanup had freed. The listing now counts
  checkpoints, and a probe that cannot reach what the listing showed reloads from the checkpoint.

## D48. Compaction on moraine's commit path, tiered, with sidecars before the commit

- **The commit path is moraine's (D18, D25), not delta-rs optimise.** A compaction commit is a
  file swap, `Work::Replace`, on the same queue and retry loop as every write: the files to
  remove must be live, and a lost race conflicts only when the winning entry removed one of them
  (`Error::FilesChanged`); then compaction yields, as the original design asks (case P5). Rewrites racing a
  compaction already conflict through the files both remove (scenario 3), and a rewrite rebasing
  over an entry that changes no rows skips the match check of its added files.
- **Tiered levels in `add.tags` (D29).** `moraine.level` counts how often rows were merged.
  A step takes the lowest level with a run of `fanout` adjacent small files, so a stream of
  appends costs a logarithmic number of rewrites per row; with no run and more than `max_files`
  small files, the longest run of any levels merges. completype's hidden-fraction bound does not
  apply: moraine's rewrites drop superseded rows at once (copy-on-write).
- **Adjacent runs, and the merged files take their place.** An entry that changes no rows
  (`dataChange` false) puts its added files where the first removed file was in the snapshot's
  file order, in every client's snapshot and checkpoint alike. Merging only adjacent files keeps
  the relative order of rows with equal keys across files, which kv0's newest-wins reads depend
  on (D38).
- **Sorted merge (D19) and sidecars before the commit.** The merged rows are sorted by cluster
  and primary key, stably, and every index's sidecar is built before the commit (own compaction, which
  the original design put after blobs, moved here), so readers never see a large unindexed file. turbovec
  could concatenate its codes instead of rebuilding (D37); I rebuild every kind the same way
  instead and recorded concatenation as a deferral (D55).
- **The commit clock starts at the uploads.** The original design's commit deadline protects a write's
  uploads from vacuum: committed within a quarter of the grace period of their upload, or never.
  The first compaction started the clock at its commit, after uploading and building, and the
  simulation found a merged file vacuumed as an orphan while the commit that would reference it
  was in flight. A swap of pre-uploaded files now starts its clock at the first upload and never
  restarts with fresh uploads; a step that runs past the deadline yields.
- **A public file replace.** `Table::replace_files` exposes the same swap with rows to write,
  file tags and optional lease fencing, for kv0's own compaction and statistics folds.
  `rows_unchanged` gives it compaction's placement.

## D49. Pins: a table property and a manifest

`Table::pin(name, version)` writes a manifest, `meta/pins/<name>/<version>.json`, holding the
version's metadata and file list in the log-entry format, then commits `moraine.pins` (name to
version) as a property change, like an index change (D9). A pinned version is read from its
manifest, without the log, which cleanup shortens. Vacuum keeps every file a current pin's
manifest names, and sidecar collection their sidecars.

A version can be pinned while vacuum cannot yet have taken its files: the current version, or
an older one whose next entry is younger than three quarters of the grace period (server time),
which leaves a commit deadline for the pin to land. An older version is
`Error::VersionNotAvailable`. Rebuilding an older version reads the newest checkpoint at or
before it and the entries up to it.

## D50. Vacuum, collectors and leases on server time

- **Vacuum deletes straight from the store (D11 settled).** The keep set is the current files,
  the pinned files, and the files removed by log entries younger than the grace period; an
  object younger than the grace period is never deleted either. The recent removals come from
  the entries themselves, read backwards from the version in hand until one is older than the
  grace period, never from a listing, so a listing cut short can only make vacuum delete less
  (scenario 18). Entries read are kept per handle. Only `*.parquet` objects outside hidden
  directories are candidates.
- **Sidecar collection** keeps a sidecar of a kept file under a current spec (an unregistered
  kind's index keeps all of its sidecars), and anything younger than the grace period.
- **Leases are generation files (D29).** Taking, renewing and releasing a lease create the next
  generation with put-if-absent; a generation expires its time to live after its object's
  server modification time, so leases need neither compare-and-swap nor client clocks. A record
  names its holder, so a put whose outcome was unknown is recognised as one's own. Fences are
  Delta transactions without `lastUpdated` (they never expire from checkpoints). A client that
  found a lease held asks again after a second at most (a third of the time to live if that is
  shorter), and one server-time probe serves a handle for a minute (elapsed time from the monotonic clock), so an ingest loop that
  runs every few milliseconds makes no lease or clock requests while another client holds the
  lease.
- **Queries restart once.** A scan, a key lookup or a search through the table handle that hits
  a missing data file moves to the newest version and runs again; a second miss, or a miss under
  an explicit `TableSnapshot`, is `Error::SnapshotExpired` (scenario 6).

## D51. Queued writes: an inbox and a lease-elected ingestor

`Table::submit` writes a batch as one Arrow IPC object, `meta/inbox/<submitter>/<sequence>.arrow`,
with put-if-absent. The submitter id is per handle; a handle submits one batch at a time, and a
put whose outcome is unknown is put again, with the same bytes, before the next one, so a
submitter's sequence never has a gap. The ingestor lists the inbox, takes per submitter the
batches after the newest sequence the log records for it, contiguous only, and commits them as
one swap that records `(submitter, last sequence)` as a Delta transaction and the `ingest` lease
as a fence. Committed batches are then deleted; a batch that cannot be decoded or no longer fits
the schema is copied to `meta/inbox-rejected/` and its sequence recorded, so the rest are not
held up.

Queued writes are appends only, and refused on a table that enforces its primary key (an
upsert's conflict check needs the writer to read). Delta's transaction is a high-water mark,
and the simulation found what that means for a swap that covers several sequences: an ingest
planned on a stale snapshot committed a batch a second time, because the commit checked only
the last sequence it records. A swap now carries per transaction id the first sequence it
covers, and stops (`Error::AlreadyIngested`) when the log holds any of them.

## D52. Searching the parts of a vector file at once (D39)

A vector file built under the memory budget holds one usearch graph per part (9 at a million
rows of 256 dimensions, D37). A search now runs the parts on scoped threads, up to the
machine's parallelism, and merges their candidates in part order, so a file's result is the same
as before; the D24 benchmark rerun is in D54.

## D53. The open full-text items of D46

- **Statistics per version.** The table-wide BM25 statistics (D40) depend only on a version's
  files, so they are kept per (version, spec hash), with the document frequencies of the terms
  already asked for; the newest two are kept per table.
- **Row fetches.** A profile put 0.8 ms of the 0.9 ms median on one file of 100,000 rows in
  fetching the hits' rows. Two changes: a byte-bounded cache of the small page ranges that reads
  by row offset fetch (an eighth of the file cache's budget, on top of it, reported as
  `CacheUsage::page_bytes`; data files are never rewritten, so entries never go stale), and one
  Parquet reader over every row group a fetch touches instead of one per row group. String and
  binary columns are also written in data pages of at most 64 KiB, as vector columns are (D37),
  so fetching a hit's long text decodes a few kilobytes rather than up to 1,024 documents. The
  median went to 0.77 ms (D54); what remains is decoding a page per hit.
- **Prewarm only after a text search.** Background prewarm (D43) builds in-memory text indexes
  for new files without a sidecar only once the table has been searched by text through the
  handle's database, since building them is most of a prewarm's cost.
- **Golden files.** A full-text bundle built from 2,000 documents, and the hits of five queries,
  are checked in; a rebuild must be byte-identical (D40).

## D54. The maintenance benchmarks, and the targets they settle

`docs/BENCH.md` ("Rerun: compaction, part-parallel vector search, full-text fetches") has the method and the tables; raw results are
`bench/results/2026-11-18-rerun-*.jsonl`.

- **The kv0-layout metrics table is clustered by `(series_id, step)`.** kv0's own optimise sorts
  its points by series and step and indexes the series id; the fair comparison (D31) left
  moraine's kv0-layout table unclustered, which made compaction a concatenation that pages could
  not prune. Declaring the cluster key is moraine's equivalent of kv0's sort. It makes logging
  slower (1.89 s against 1.52 s: every flush's file is sorted) and pre-compaction series reads
  faster (1.63 ms against 7.43 ms).
- **Series reads after compaction: met.** A full series in 0.50 ms against compacted Lance's
  3.02 ms (p99 0.83 against 16.68 ms), a window in 0.47 against 3.12 ms; start-up after
  compaction 0.11 s against 13.25 s.
- **Compaction time: met; compaction memory: missed.** 1.98 s against Delta's 4.54 s (fair
  comparison) and Lance's 7.33 s; 753 MB peak against Delta's 368 MB and Lance's 880 MB. A step
  reads up to `max_input_bytes` (256 MiB, compressed) of input into memory, and the full policy
  merged the metric table's 80 MB in one step. Merging in a streaming sort over the sorted inputs
  is what would bound it; I recorded it as a deferral (D55) and keep the target.
- **Disk size after compaction: met.** 143.7 MB against Delta's 154.3 MB, both counting the
  replaced files that the grace period keeps.
- **The D21 vector target at a million rows: met on the layout compaction produces, missed on
  10 and 100 files.** One file of a million vectors (11 parts) answers in 2.45 / 4.02 ms against
  Lance's 3.1 / 5.1 ms (the D37 run; Lance measured 8.85 / 16.36 ms in this one). Ten files
  answer in 2.33 / 5.84 ms (p99 over), a hundred in 4.50 / 9.37 ms. A table of a million
  256-dimension vectors compacts at the default 256 MiB target into about four files, a layout
  not measured here. Searching parts on threads whatever the fan-out made ten files slower (5.95
  / 11.87 ms), so a file gets threads only when the cores divided by the files searched leave
  four or more (D52).
- **The D21 full-text target on one file of 100,000 rows: met in this run.** 0.77 / 1.66 ms
  against Lance's 1.42 / 6.32 ms (median of three; Lance's medians ranged from 0.94 to 1.65 ms).
  Against Lance in the D46 run (0.75 ms) the median would still miss by a few hundredths of a
  millisecond, so I count it met only against the same run, as D21 asks.
- **The S3 8-writer p99: met.** 1,113 ms (1,038-1,373) against the 1,283 ms bound of the fair
  comparison and 2,270 ms in this run (half of Delta's 4,539 ms); the p50 is 123 ms. Nothing in
  the maintenance work touches the write path; the difference from the fair comparison's 1,405 ms is within
  run-to-run spread of RustFS.
- **Queued mode does not pay on this server at 8 writers.** Submission to visibility takes
  612 ms at the median (p99 1,015 ms, one run 5,192 ms) against 123 ms for direct appends, at
  12.2 commits a second against 20.1: an ingest round lists the inbox, reads every batch, writes a
  data file, puts the log entry and deletes the batches, and RustFS's listings are slow. Its
  value is that a writer's own request never contends, so its p99 stays bounded as writers grow;
  measuring it at 16 and 32 writers and on real S3 is not done yet.

## D55. Deferrals and open problems of maintenance

- **Streaming compaction.** A step holds its input in memory (up to `max_input_bytes`, 256 MiB
  compressed); a k-way merge over the already sorted input files would bound memory by pages.
- **turbovec merge by concatenation** (D37) is not used: every kind's sidecar is rebuilt.
- **Partitioned tables** stay unsupported, so compaction never has to respect partitions (P4).
- **Tombstones in checkpoints** are not written (D47); delta-rs's own vacuum does not need them.
- **A pin that takes longer than the grace period to commit** could have its manifest collected
  before the property lands; vacuum then fails on the missing manifest rather than delete
  anything. The pin's commit goes through the retry policy's deadline (60 s by default).
- **Compaction steps must finish within the commit deadline** (a quarter of the grace period,
  15 minutes by default), sidecar builds included; a larger step yields every time. A usearch
  build of a million rows takes about three minutes on one thread (D37).
- **T8 ran on local disk only.** The original design runs it nightly against MinIO and a real cloud too; the
  S3-compatible run and the real-cloud runs are not done yet.
- **A store call in the simulation waits at most 30 s of simulated time.** The scheduler used
  to fire any sleeper's timer while a call waited at its gate, so a commit's log PUT could take
  ten simulated minutes, longer than the grace period; vacuum then took a data file the commit
  referenced a moment later. That is the frozen writer of case C7, which `repair`
  exists for, and a real request times out long before. The scheduler now fires a timer that
  would move time more than 30 s past the oldest waiting call only when no call is waiting
  (`partial_listing` seed 2728 found it).
- **Simulation seeds.** The new scenarios and the earlier ones ran 10,000
  seeds each; the nightly 100,000 seeds of T3 and T4 are not run on this machine.

## D56. A cancelled write never stalls a handle's group-commit queue

Found while reviewing the public API before 1.0: wrapping a write in `tokio::time::timeout` was enough to
make every later write on the same table handle wait forever.

- **Root cause.** A write joins the handle's queue and marks it busy before its first await;
  leadership then passes from caller to caller by a message in the next waiter's mailbox. Two
  cancellation points broke the chain. A *waiter* dropped while queued stayed at the front of
  the queue, so the next hand-off went to a mailbox nobody read. A *leader* dropped during the
  group-commit linger, before it took its group, had not yet created the guard that hands
  leadership on, so the queue stayed busy with no leader. In both cases the queue was busy, no
  caller would lead, and new writes queued behind it forever.
- **Fix.** The guard that hands leadership on (`LeadGuard`) is armed at the start of a lead,
  before the linger. A second guard (`QueuedGuard`) is held by every caller from the moment its
  write is queued until it has its answer; when the caller is cancelled first, it removes the
  write if no leader has taken it yet and, if leadership was handed to it and not used, passes
  it on. Both steps run under the queue lock, so no hand-off can slip in between.
- **What a cancelled write means.** A write cancelled before a leader took it is never
  committed. One taken into a group commits or fails with the group; its caller just does not
  hear. A leader cancelled after taking its group answers its members as before: an error
  before the log put, `OutcomeUnknown` once the put may have been sent. So one caller's
  cancellation can still fail other callers' writes in its group; running the group commit on
  a task owned by the handle would remove that.
- **Tests.** `tests/cancellation.rs` reproduces both hangs. `tests/cancel_sim.rs`
  (`cancelled_writes`) drops writes after a random number of polls and aborts spawned writes
  after a random delay, so cancellations land while queued, while leading and lingering, while
  uploading and around the log put; it checks that the run finishes, that acknowledged writes
  are in the log and failed ones are not, and that nothing lands twice.

## D57. Blobs: details that differ from the original design

The original blob design holds, with these changes.

- **Delta types.** `size` and `crc32c` are `Int64`: Delta has no unsigned integers, and the
  canonical schema check (D25) refuses a type the log cannot store. `crc32c` holds the
  unsigned 32-bit value, so other Delta readers see a non-negative number. Every field of the
  struct is nullable, which keeps the struct's type the same through the Delta round trip.
- **Keys.** A key is `<two hex characters>/<UUID v7>` under the table's `blobs/` prefix. The two
  characters are the UUID's last two, from its random part: the original design's first two characters of a
  UUID v7 are timestamp bits, the same for every blob for years, which would put every object
  under one prefix and defeat the store's partitioning. A key that does not match the pattern is
  refused (`InvalidBatch`), so a row cannot name an object outside `blobs/`. References are
  relative to the table: reusing a value across tables is not supported in version 1.
- **The write path.** Blob preparation runs once per write job, before the data file is
  encoded: values above `blob_inline_bytes` (a table property, 256 KiB by default, at most
  16 MiB) are uploaded with one PUT each, or a multipart upload in 16 MiB parts above 16 MiB;
  references the job did not upload are copied on the server. The prepared rows are kept across
  attempts, so a lost race reuses the objects; a restart past the commit deadline (case C6)
  prepares again, with fresh uploads and copies. Sizes and checksums of inline values are
  recomputed from the bytes.
- **Staged uploads.** `Table::upload_blob` streams a value up ahead of the write with two parts
  in memory at most (case B6). The handle records when each upload began; a write through that
  handle commits the reference without a copy while the upload is younger than the commit
  deadline, and the job's commit clock then starts at the upload, so the original design's rule that the
  deadline is measured from the first upload holds. Later, or through another handle, the write
  copies it. This is what scenario 7 exercises.
- **The mark set is vacuum's keep set, not one snapshot.** The original design marks the keys of the
  current snapshot. A reader of an older snapshot younger than the grace period, or of a pinned
  version, may still read a deleted row's blob; marking only the current snapshot would let the
  sweep take it while the data file holding the row is still kept. I mark the blob columns of
  every file vacuum keeps (current, pinned, removed less than the grace period ago), cached per
  handle by file, since data files never change. A kept file that cannot be read, or a pass past
  half the grace period, aborts the sweep (case B11). A table without blob columns makes no blob
  request at all.
- **Local disk copies.** object_store's local copy is a hard link, which shares the source's
  modification time; the age test would treat a fresh copy of an old object as old.
  `LocalCasStore` copies with a read and a put instead (D15's wrapper).
- **What may not carry a reference.** An update may not assign a blob column (it would need a
  copy the update path does not make); upsert the row instead. Queued writes refuse references:
  the ingestor would copy them later, possibly after the referenced object was collected.
- **CRC-32C.** The `crc32c` crate is a new dependency: it uses the CPU's CRC-32C instructions
  (SSE 4.2, ARMv8), where the only CRC crate already in the graph (`crc`) computes from tables.
  Every byte of every blob is checksummed on write and on whole reads, so the checksum must not
  be what bounds blob throughput (D62 has the throughput).
- **Request counts.** The metrics store now counts multipart parts and completions
  (`Op::Part`, `Op::Complete`), so the budget tests can assert every blob path exactly.

## D58. Index before commit

The original design builds sidecars before the commit for appends and for moraine's own
compaction. I did it for every write that writes a file: appends, the new rows of an upsert,
the files a delete, update or upsert rewrites, and compaction's merged files, through one path
(`index::inline`). The rows are in memory already; the builder gets them in batches of 1,024,
and the sidecar is put (put-if-absent) beside the data file in the same round trip. Readers
never see such a file without its sidecars.

- **Limits per index.** An append builds an index's sidecar before the commit only when the
  index's `policy` allows: `before_commit` (default `true`), `before_commit_max_rows` (default
  1,000,000) and `before_commit_max_build_bytes`, the file's rows times the kind's build bytes per
  row (default 2 GiB, about 900,000 rows of 256-dimension usearch F32). Beyond them the file waits
  for the build job, as before. The policy is not hashed, so changing it keeps every sidecar.
  Compaction builds every index whatever the policy (D48), unless `build_sidecars` is off.
- **The kind's threshold still applies.** A file below `min_rows_for_ann` (10,000 rows by
  default) gets no vector sidecar either way; scoring it exactly is cheaper than opening a graph
  (case V3). Those rows are on the exact route by design until compaction merges them; D61 is
  about how many there are.
- **The D21 request budget.** A flush of a table without indexes, or of files below every
  threshold, still costs one data PUT and one log PUT. Each sidecar built before the commit adds
  one put-if-absent, in parallel with the data PUT (the request-budget tests assert both).
  The key index of large files (D19) moved onto this path with its threshold unchanged.
- **Compaction reads nothing back.** D48 built a merged file's sidecars by reading the uploaded
  file back, after a HEAD per sidecar. Building from the rows being written removes both.
- **Status.** `IndexStatus` gains `rows_exact_by_policy`, `rows_pending()` (rows the kind wants
  a sidecar for that have none) and `exact_share()` (rows a search scores exactly, over all).

## D59. Streaming compaction (D55)

A compaction step whose inputs are all sorted by the table's current sort columns (every file
moraine writes is; `sorted_by` records it) is a k-way merge. Each input is read a row group at a
time in batches of 1,024 rows; the merge compares the sort columns in Arrow's row format and on
equal keys takes the input that comes first in file order, so the result equals the stable sort
of the concatenated inputs that the in-memory path computes (a test compares the two row by
row). Rows go to a Parquet writer on a local temporary file and to the sidecar builders a chunk
of up to 8,192 rows at a time; each output file is uploaded when complete. The number of outputs
is the input's stored size over the target size, rounded up, with the rows spread evenly, so
the row count of every output, which the bundle header needs, is known before the merge.

One pass reads at most 32 inputs. The first version read every input of a step at once and
picked each row by comparing every input's current key. On the tracker benchmark's metrics
table, whose full compaction merges about two thousand flush files in one step, that took
667 s and 1,130 MB, worse on both counts than the in-memory merge it replaced (16 s and 756 MB
in the same, heavily loaded run). Each open input holds a Parquet reader, its row group's
column chunks and a decoded batch, and the per-row scan was quadratic in the inputs. Now
groups of 32 adjacent inputs are merged into local temporary files first (equal keys still go
to the earlier group), and a pick takes a run of rows from the leading input for as long as it
stays ahead of the runner-up, so a clustered input costs one comparison per row. Turbovec
merges (D60) need one pass, so a step of more than 32 inputs rebuilds its sidecars instead.

Inputs not sorted by the current sort columns (written by other tools, or before a cluster key
was declared) still merge in memory, as does every step with `CompactionPolicy::streaming` off.
The measurement is in D62.

## D60. turbovec codes merged on compaction (D55)

`vec-tq` already had `IndexKind::merge` (D24) but compaction did not use it. A streaming step
now asks each index kind whether it merges; for one that does, it fetches the inputs' sidecars
under the current spec, validates them like a reader, and records where each input row goes in
each output file. When the file is complete, the kind gathers the inputs' codes and scales in the
output's row order. TurboQuant codes depend only on their own vector (fixed rotation, no
training), so the merged sidecar is byte for byte what a rebuild from the merged file produces;
a test checks exactly that. If any input lacks a valid sidecar the index is rebuilt from rows
instead. usearch and tantivy keep rebuilding: their structures depend on every row.

## D61. The exact-route bound: what index before commit guarantees, and the 5 % figure missed

The original design asks that under a steady append and compaction load the share of rows on
the exact route stay below 5 %, provisionally. I measured it with `moraine-bench exact-route`
(D62 has the method): one process appends 64-dimension vectors to a table with a `vec-hnsw`
index (`min_rows_for_ann` 10,000) while a second task runs a maintenance pass every 10 s, and
every 10 s the share is read from `index_status` and checked against a search's own count of
rows scored exactly.

| Load | Before commit | Exact share, mean (max) | Pending share, max | Append p50 / p99 |
|---|---|---|---|---|
| 20,000 rows every 2 s, 5 min | on | 0.0 % (0.0 %) | 0 | 9,176 / 10,582 ms |
| 20,000 rows every 2 s, 5 min | off | 77.8 % (87.7 %) | 100 % | 30 / 64 ms |
| 1,000 rows every 100 ms, 5 min | on | 95.4 % (100 %) | 0 | 2.0 / 11.9 ms |
| 1,000 rows every 100 ms, `min_file_age` 0, 5 min | on | 84.3 % (93.4 %) | 0 | 1.3 / 7.9 ms |
| 1,000 rows every 500 ms, `min_file_age` 0, 64 steps, 10 min | on | 48.5 % (66.2 %) | 0 | 1.9 / 8.5 ms |

What holds, and what I take as the bound: **no row of a file within its index's before-commit
limits is ever on the exact route** (pending share 0 in every sample of every run with the policy
on, and the tests assert it per write). Files at or above the kind's threshold never wait for the
build job. That part of the goal is met, and it is a guarantee rather than a share.

What does not hold is the 5 % figure for the rows below the threshold, and I did not meet it.
Those rows are on the exact route by design (V3) until compaction merges their files into ones
big enough for a graph, so their share is set by how fast compaction builds graphs against how
fast rows arrive. On this host (20 cores shared with other work, load averages of 60 to 250
during the runs) a single-threaded usearch build ran at roughly 1,000 rows a second: the last
run's passes took 5 s to 499 s as one step merged 20 to 435 small files into one file and built
its graph. The share fell only where the build kept up. Large appends meet the bound outright,
at the cost of the build in each append's latency; without the policy the builder job fell as
far behind (two passes in five minutes).

So I record the bound as the guarantee above and keep the 5 % share as an open target, together
with what would move it: building one file's graph on several threads (`BuildCtx::threads`
exists and is always 1 today), and a compaction policy that caps a step's
output rows so a run of small files becomes several medium files instead of one large graph.

## D62. The blob, streaming compaction and exact-route benchmarks

`docs/BENCH.md` ("Blobs, streaming compaction, index before commit") has the method and the tables; raw results are
`bench/results/2026-11-18-{blobs,exact-route,local-compaction}*.jsonl`. The host carried other heavy work through every run, so
I compare only rows measured in the same run; peak memory does not depend on the load.

- **Compaction memory: met.** The tracker tables compact at 264 MB peak (263-265) with the
  streaming merge against 754 MB (751-754) for the in-memory merge in the same build and run,
  under Delta's 368 MB. The merge is slower: 6.97 s against 6.13 s (median of three), the price
  of the second pass through local files for a step of about two thousand inputs (D59). Whether
  the time target against Delta (4.54 s) still holds needs a run on an unloaded host;
  D54 measured 1.98 s for the in-memory merge, so the streaming merge would need to be
  more than twice as slow to miss it.
- **Blob throughput.** On local disk, writes of 1 to 64 MiB values run at 0.7 to 1.2 GB/s,
  whole reads at 1.5 to 2.2 GB/s, streamed reads at 0.4 to 0.7 GB/s, and a 4 KiB range at 31 to
  54 µs (p50). On RustFS, writes of 1 MiB values reach only 21.6 MB/s (each append of four is
  about 200 ms of PUTs and its commit), values from 16 MiB about 100 MB/s; whole reads 0.2 to
  0.5 GB/s; a 4 KiB range 2.6 to 3.1 ms. Large values are uploaded one part after another; I
  count that as a loss (D63).
- **Copies.** Committing a reused reference adds one server copy: 0.7 ms (1 MiB) to 65 ms
  (64 MiB) on local disk, where moraine's copy reads and writes the bytes (D57), and 114 ms to
  0.8 s on RustFS, whose copy takes longer with the size.
- **The exact-route share.** D61.

## D63. Deferrals and open problems of blobs, index before commit and streaming compaction

- **The 5 % exact-route share** of all rows, below-threshold files included, stays open
  (D61): graph builds on several threads, and compaction steps whose outputs stay small enough to
  build within a pass.
- **Blob parts are uploaded in sequence.** A value above 16 MiB goes up one 16 MiB part at a time
  (values of one write go in parallel). Parallel parts with a bound on the bytes in flight would
  lift the 100 MB/s of large values on the S3-compatible server.
- **Streams are slower than whole reads** on local disk; the per-chunk checksum and stream
  plumbing are where I would look first.
- **Cross-table blob references** are refused (keys are relative to the table). Copying from
  another table's `blobs/` prefix would be a server copy as well; it needs a key form that names
  the table.
- **Blob collection marks every kept file's blob columns once per handle.** A handle that starts
  vacuum on a large table reads the blob columns of every file it keeps before its first sweep.
  A per-file manifest of blob keys written beside the data file would turn that into small GETs.
- **turbovec merges need one pass** (at most 32 inputs); larger steps rebuild from rows.
- **Simulation seeds.** The new scenarios and every earlier simulation scenario ran 10,000 seeds
  each in release builds, after merging main (docs/design/testing.md); the nightly
  100,000 seeds of T3 and T4 are not run on this machine.

## D64. Group commits run on a task owned by the table handle (D56)

D56 left one limit: a caller whose future was dropped after its write had been taken into a
group took the whole group down with it, so one caller's timeout could fail other callers'
writes. The queue now has no leader among the callers. The first write queued on an idle handle
spawns the handle's commit task, which lingers, takes the queue's compatible prefix, commits it
(`run_job`, unchanged), answers every member, and goes on with the next group until the queue is
empty; it marks the queue idle under the same lock that finds it empty, so a write queued in
between starts a new task. Callers only queue and wait for their answer.

- **What a cancelled write means now.** One cancelled before the task took it is removed from
  the queue and never committed (`QueuedGuard`, as before). One already taken commits or fails
  with its group; its caller does not hear. No caller's cancellation fails anybody else.
- **Panics.** A panic in a group commit is caught on the task and raised again in every caller
  of the group, so a write that panics still panics for its caller (the crash simulations
  inject panics at failpoints on the commit path and depend on it). Should the task itself stop
  without answering (a runtime shutting down), a guard answers the group with an unknown outcome
  once a log put may have been sent, every queued caller with a failure, and marks the queue
  idle: nothing waits for an answer that cannot come.
- **The task keeps the handle alive** until the queue is empty; a handle dropped with writes
  queued still commits them.
- **The simulation's scheduler.** `Clock` gains `spawn` (default `tokio::spawn`). The
  simulation's clock registers the task with its scheduler, which counts it as a running client
  until it finishes, so the scheduler still knows when every client is stopped at a gate. The
  blob simulation's crash model waited, before dropping a write, until its client had no gate
  open; the commit task shares the client's store, so its gate kept that true while the write
  itself had none, and `slow_writer_vs_collectors` hung. A crashed write is now dropped at its
  next poll: the scheduler withdraws a gate that has not opened when its future is dropped.
- **Tests.** `tests/cancellation.rs` aborts the caller whose write started a group while the
  group is held before its log put; the other caller's write and the aborted one both commit.
  `cancelled_writes` (`tests/cancel_sim.rs`) now requires that, without faults, every write that
  was not cancelled commits; before, a cancelled leader's group was allowed to fail.

## D65. A write deletes its own uploads once it is over

The commit deadline protects a write's uploads from vacuum, so a write that did not commit left
them for vacuum to take after the grace period. A purge (D66) cannot wait that long for copies of
deleted rows, so a write now deletes what it uploaded and no commit references, as soon as it is
over.

- **What a write records.** Every data file before its PUT (appends, upsert inserts, rewritten
  files, replace rows, and the files compaction uploaded for its swap), every blob key it uploads
  or copies before the request, per round (a restart past the commit deadline, case C6, starts a
  new one), the versions its log puts targeted whose outcome was unknown, and the data files of
  the entry it committed, taken from what was put or from the entry read back, never from a
  later plan.
- **When it deletes.** After a commit: the files the committed entry does not hold (a rewrite
  planned again after a conflict, an earlier round), and the blobs of rounds none of whose files
  committed. After a definite failure (`FilesChanged`, `Fenced`, the deadline, a superseded
  group, compaction yielding, any error that is not an unknown outcome): everything. A data
  file's sidecars go with it (one listing of `idx/<file>/`, only when the table has indexes).
- **Ambiguity.** Nothing is deleted after `OutcomeUnknown`. A definite failure that followed an
  unknown put deletes only once every version such a put targeted holds another writer's entry:
  the lookup that called the put "not committed" found the version empty, and a put still in
  flight could land there later. Until then the uploads stay for vacuum.
- **Best effort.** A failed delete leaves the object for vacuum. `Table::write_metrics()` counts
  objects deleted (`uploads_deleted`) and left (`uploads_left`).
- **A missing data file while planning** a rewrite is now a conflict, not a fatal error: it can
  only have been removed by a commit the attempt has not seen and then collected, so the next
  attempt plans on a fresh snapshot.
- **What is not covered.** A compaction step that fails before its commit (a failed read while
  merging) leaves the files it already wrote to vacuum; only its commit records them.
- **Tests.** `tests/failed_uploads.rs` (a lost replace, a fenced one, a rewrite planned again,
  compaction yielding, the deadline with and without restart). `failed_uploads`
  (`tests/failed_uploads_sim.rs`): every file a log entry adds is still in the store, and a file
  no entry adds is left only when an outcome was unknown, a delete failed or a compaction step
  failed, never without faults. The maintenance simulations' history check (`maintcheck`) no longer
  flags a file no entry ever added being deleted inside the grace period: that is now the write
  deleting its own upload, and "never added by any entry" is what makes it safe.

## D66. Purge and erasure, owned by moraine

kv0 rewrote moraine's log itself to erase deleted rows' statistics and deleted removed files
before the grace period. `Table::purge` does both inside moraine, coordinated with every other
job.

- **The marker.** A purge takes the `purge` lease (`Error::LeaseHeld` while another client
  holds it) and commits the table property `moraine.purged` set to the version the commit
  follows (P), with the lease's fence (`Error::Fenced` when a newer generation committed). Like
  every property change it is computed again on each attempt, so P is exactly the version
  before the marker. From then on: no version before P can be pinned (`VersionNotAvailable`,
  checked again on every attempt of the pin's commit; a refused pin deletes its manifest unless
  the same pin is already in place, which `purges_everywhere` found: a pin refused by a purge
  deleted the manifest of the identical pin committed before it);
  checkpoints older than P are stale; pin manifests of versions before P are stale. Version P
  itself stays pinnable: the marker removes nothing, so its files are live after the marker.
- **The keep set** at the version in hand (S ≥ P + 1): S's files, the files the manifests of S's
  pins name, and the files entries after P removed (live at P).
- **Collect now.** The caller gives a cutoff (`removed_before`, default the purge's start): a
  statement that no reader holds a snapshot loaded before it. A listed data file outside the
  keep set goes when the entry that removed it, at or before P, is older than the cutoff. A
  file no walked entry removed goes only by vacuum's rule (older than the grace period), so an
  upload of a commit in flight is never taken; such uploads are what D65 deletes. The collected
  files' sidecars and the blobs only they reference go with them, whatever their age. Sidecars
  and blobs of every file that stays, kept or removed too recently, stay: blobs are marked from
  all of them first, and an unreadable one stops the blob part (case B11). The first version
  marked only the keep set, and `crash_everywhere` caught a purge taking the key-index sidecar
  of a file removed seconds earlier.
  Readers of older snapshots that meet a missing file move to the newest version once (D50).
- **Erase** (`Purge::erase`). A checkpoint at S; then every entry up to P is rewritten without
  the minimum and maximum of the erased files (removed before the cutoff, not kept), without
  statistics on removes, and without commit information that may hold values (operation
  parameters other than `mode`, user metadata). Row and null counts stay, so readers only prune
  less. Then checkpoints older than P and manifests of versions before P are deleted. A rewrite is
  a conditional put on the entry's ETag, so an entry log cleanup deleted meanwhile does not come
  back; a store without conditional updates gets a plain overwrite, under the lease. Rewritten
  entries look new to vacuum and log cleanup, which only makes them keep things longer.
- **Racing maintenance.** A checkpoint pass that read the log before the marker and writes its
  checkpoint X < P afterwards would bring erased statistics back. After writing, a checkpoint
  pass refreshes; when the newest purge is after X and a checkpoint at or after it exists, it
  deletes X. If that checkpoint does not exist yet, the purge writes it later and deletes every
  checkpoint older than P, X included: the pass's put precedes its refresh, and the marker
  precedes the purge's listing, so one of the two always sees the other. Log cleanup deletes
  checkpoints older than the newest purge whatever their age, once a checkpoint at or after it
  exists, and vacuum deletes such manifests whatever their age, so what a crashed client leaves
  goes on the next pass. Loading retries once with a fresh listing when the checkpoint it
  listed is gone.
- **Why a marker commit rather than a separate object.** The pin check and the checkpoint check
  need an order between the purge and other commits, and the log is the only total order moraine
  has; a property change conflicts with every other metadata change, so pins and the marker
  serialise without new machinery.
- **What it does not reach.** Other processes' in-memory caches (footers, pages, materialised
  small tables) and the inbox of queued writes. A crashed checkpoint pass can leave a stale
  checkpoint until the next log cleanup.
- **Tests.** `tests/purge.rs` case by case; `purges_everywhere` (`tests/purge_sim.rs`) runs purges
  with and without erasure among writers, pins, readers and maintenance with faults, and checks
  the store's history: no file deleted while the version in force or a current pin held it, no
  commit or pin of a deleted file, and after a last purge no removed file, no erased statistics
  in any entry, no older checkpoint or stale manifest. `crash_maintenance` crashes purges at five
  failpoints.

## D67. File-level reads: chosen files, rows by offset, the table id, streams

kv0 read moraine's data files itself, by path. moraine now covers what it needed:

- **`Scan::files(paths)`** reads only those files, in the order given, with filters, projections
  and limits as usual; a path the version does not hold is `Error::FilesChanged`. Files keep
  their own row order, which makes offsets meaningful.
- **`TableSnapshot::take(path, offsets)`** reads the rows at those offsets of one file, in the
  order of the offsets, through the footer cache and the page index (`fetch_rows`, which
  searches already use). An offset beyond the file's rows is `InvalidQuery`.
- **`Table::id()` and `TableSnapshot::table_id()`** return Delta's `metaData.id`: set at
  creation, kept by every commit and checkpoint, new for a table created again under the same
  name. I did not add a separate moraine property: the Delta id already has these properties and
  other Delta readers see the same value.
- **`Scan::stream()`** returns batches as files are read, a few files
  ahead (the I/O concurrency). A file found missing before the first batch moves a scan of the
  table to the newest version once, as `collect` does; after a batch it ends the stream with
  `SnapshotExpired`, since rows already returned cannot be taken back. `collect` runs on the same
  per-snapshot stream.

## D68. File replaces from a stream; compaction keeps caller tags; pins still commit

- **`Replace::add_stream` and `add_batches`.** The rows are read as the replace runs and written
  one target file size at a time, each file sorted on its own, before the replace is queued; the
  swap then commits them as uploaded files, as compaction does. The commit deadline runs from
  the first upload, and the files are deleted if the stream fails or the commit does (D65). A
  key the table enforces is checked across the whole stream. Tables with blob columns are
  refused for now: blob values would need preparing per chunk outside a write job.
- **Compaction keeps caller tags.** A run now merges only files whose tags outside `moraine.`
  are equal, and the merged files carry them. Merging a tagged file with untagged ones would
  either drop the tag or give it to rows it does not describe; kv0 marks files of statistics this
  way. Runs stay adjacent, so the order of rows with equal keys (D38) is unchanged.
- **Pins still bump the version.** I considered pins that write only a manifest. A pin is part of
  what vacuum and purge must keep, and the log orders it against them: vacuum's keep set and a
  purge's marker read the pins of one version, and a pin of a version before a purge is refused
  by the commit that would record it (D66). A manifest-only pin would need compare-and-swap or
  generation files for the name, and a listing per vacuum, to get the same order. The version a
  pin adds changes no rows, so readers' caches stay valid across it.

## D69. Deferrals and open problems after D64 to D68

- **Compaction's own failures** before its commit leave its merged files to vacuum (D65).
- **Streamed replaces of tables with blob columns** are refused (D68).
- **Erasure does not reach other processes' memory** or the inbox of queued writes (D66).
- **`Clock::spawn` needs a Tokio runtime**, as the scans' and builds' tasks already did.

## D80. Index builds on several threads

D61 left a graph build on one thread (`BuildCtx::threads` was always 1), so a run of small files
waited for compaction to build its graph at roughly a thousand rows a second. A build now gets
threads from the database's `build_threads` setting (`DatabaseBuilder::build_threads`, default
the machine's parallelism), capped by its input (one thread per 1,024 rows) and by its budget
(each thread is assumed to hold 1 MiB of scratch, and threads may take an eighth of the
budget). Builds before a commit, compaction's builds and the builder job all use it;
`BundleBuild::new` keeps one thread.

- **usearch.** The builder gathers rows until each thread has 1,024 of them and adds them on
  scoped threads that take rows from a shared counter in blocks of 64. usearch accepts
  concurrent adds once it has a slot per thread and room for every row, both reserved first.
- **Rows that concurrent adds leave unreachable.** The first threaded build of a million
  vectors in 100 files answered with recall@10 0.997 at every `expansion_search` up to 512,
  where the one-thread build reached 0.9999; a graph of 8,000 rows built on eight threads did
  not return 26 of its rows when each was searched for. Adding the first rows on one thread did
  not change it. Each row added on the threads now looks itself up on the same threads, and the
  few not found as their own nearest (or at distance zero) are removed and added again on the
  builder's thread. Recall is back at 0.9999, and a test checks that every row of a threaded
  build is found.
- **Determinism.** A graph built on more than one thread is not byte-identical to another build
  of the same rows: the graph depends on how the threads interleave. The golden files are built
  through `BundleBuild::new` on one thread and are unchanged, and `build_threads(1)` restores
  identical rebuilds. turbovec, the key index and tantivy ignore the threads; tantivy's segments
  would differ with them, and its goldens must stay identical (D40).
- **Several indexes of one file** now build on threads of their own, both while rows arrive
  and when the builds finish.
- **What it bought** (2026-11-19, D85): a million 256-dimension vectors in 10 files build in
  54 s against 131 s, in 100 files in 22 s against 78 s; a 20,000-row append with its graph
  built before the commit takes 1.26 s at the median against 2.47 s on the same host.

## D81. A compaction step's rows fit its builds into the commit deadline

A step that merges many small files into one large file rebuilds that file's graph from rows,
and a build longer than the commit deadline loses the step (D61 saw passes of 499 s). A handle
now times the sidecar builds of its streaming compaction steps for each table and keeps a moving
average of the rows a second they run at (builds under 1,024 rows or 10 ms are not counted).
A step on a table with declared indexes takes a prefix of its run of at most the rows those
builds finish in half the commit deadline, still at least two files; before a build has been
timed it assumes 1,000 rows a second, D61's single-thread rate on a loaded host, which at the
default grace period of an hour allows 450,000 rows. The rate is read from the database's
clock, so the simulation, whose builds take no simulated time, never caps a step by it.

## D82. Blob parts in parallel, streams as ranged GETs

D63 deferred both.

- **Uploads.** A multipart blob upload keeps up to four 16 MiB parts in flight
  (`blob::PARTS_IN_FLIGHT`), for values written with a batch and for `Table::upload_blob`; part
  numbers follow the order the parts were started in, and a failure aborts the upload. Memory
  in flight is four parts. On RustFS it did not measurably help: 64 MiB values wrote at
  185 MB/s (105-207, three runs) on 2026-11-19, but a pair of single runs one after the other
  under the same load gave 107 MB/s with parallel parts against 125 MB/s without, and the
  server takes 239 MB/s of plain PUTs at that concurrency. The rate is the server's.
- **Small values.** I looked for per-object overhead to remove and found it in the server: the
  blob benchmark now also PUTs the same values straight to the store, as many at once as its
  writes have in flight. At 1 MiB, RustFS takes 36 MB/s of four concurrent PUTs with no table
  at all; moraine writes 22 MB/s, because each append adds its data file PUT and its log PUT
  after the values. Keeping four appends of one handle in flight (the group commit batches
  them) gives 31 MB/s against the server's 58 MB/s for sixteen concurrent PUTs. Uploading the
  values and the data file at once would close part of the rest; it is a change to the write
  path, which I left as it is (D87).
- **Streams.** `Blob::stream` reads 4 MiB ranged GETs, up to four ahead of the consumer
  (`STREAM_CHUNK_BYTES`, `STREAM_READ_AHEAD`), instead of one GET consumed in the store's small
  chunks. The first GET also checks the object's size; the checksum is still verified at the
  end, so a corrupt value is the stream's last item. On local disk streams are now as fast as
  whole reads or faster (1 MiB: 1,081 MB/s against 899; 64 MiB: 5,231 against 3,294); on a
  quiet host the earlier build streamed 1 MiB values at 873 MB/s against 2,573 for whole reads.
  A stream costs one request per 4 MiB instead of one.

## D83. Vector searches over many files share one work-stealing pool

D54 left the vector target missed on the 10- and 100-file layouts at a million rows. A search
can now open every file's sidecar first and then search the parts of all files together on
rayon's pool, one task per part (`FileIndex::pieces` and `search_piece`; a kind without parts
is one piece), so idle threads take what is left and the slowest file no longer sets the
latency by itself. rayon is already compiled into every build with `fts` or `turbovec`.

Measured on a million 256-dimension vectors (usearch F32, `expansion_search` 64, 1,000
queries, 2026-11-19, load 5 to 16 on 20 cores), pool against per-file tasks:

| Layout | Pool p50 / p99 (ms) | Per-file tasks p50 / p99 (ms) |
|---|---|---|
| 1 file, 11 parts | 2.00 / 4.82 | 2.07 / 2.90 |
| 10 files, 20 parts | 3.23 / 9.13 | 2.74 / 9.16 |
| 100 files, 100 parts | 4.03 / 6.15 | 4.07 / 9.21 |

So the pool pays only when the files outnumber the tasks a search runs at once
(`io_concurrency`, 16), which otherwise run in waves; a search uses it then, and D52's part
threads otherwise. The search parameter `moraine.pool` (1 or 0) forces either path. The D21
target is still missed on 10 and 100 files (D86): the graph searches are not what dominates
those latencies.

## D84. The full-text simulation was slow, not hung, and every seed now has a watchdog

A release run of `fts_sim` at 10,000 seeds was stopped after 2 h 8 min with 21 min of CPU, which
looked like a hang in the threaded builds. I ran the same seeds in ten shards of 1,000 under a
per-seed limit of 120 s: every seed passed and none came near the limit, the shards taking 15
to 20 min each, about 2.8 h in all. The same 150 seeds took 139 to 159 s on the commit before
any of D80 to D83 and 144 s with them, with about the same CPU time (31 s of user and system time
before, 34 s after): the scenario spends most of its wall time waiting on the build directory's files, not
computing, so a long run at a load of 20 to 40 shows little CPU. Builds in the simulation run on
threads of their own and never wait for the scheduler or the simulated clock: the builds of a
file's several indexes each produce the same bytes whichever finishes first, and usearch uses
more than one thread only from 2,048 rows, which no simulation's vector files reach, so the
simulations stay deterministic.

What I changed: `run_seed` now starts a watchdog thread around the whole seed, and a seed that
runs longer than `MORAINE_SIM_SEED_TIMEOUT_S` seconds of real time (default 120, about a hundred
times a normal seed's wall time on this host) ends the process with its seed and the command
that replays it. A hang now fails the run instead of stalling it. No seed hung, so there is no
regression seed to add.

A second release run on 2026-11-19 showed the same pattern (19 min of wall time, 3 min of CPU)
and was again progressing: its read and write counters and its CPU time grew between samples,
and its threads were tantivy's merge and indexing threads, the runtime's workers waiting for
work, and tantivy's document-store compressor waiting in the kernel for the file system's
journal (`jbd2_log_wait_commit`). Full-text builds write through tantivy's `MmapDirectory` into
a temporary directory, which syncs every file it finishes and the directory on every commit;
on a disk shared with other builds each sync waits for the journal. The simulation's full-text
clients build sidecars throughout, so most of its wall time is those syncs. The simulations run
with `io_concurrency` 1, so the search pool is never used there, and their vector files stay
below the rows that start threaded graph builds. Syncing a temporary build directory buys
nothing, since the bundle is uploaded and the directory deleted; a directory that skips the
syncs is in D87.

## D85. Measurements of the threaded builds, the blob paths and the search pool

`docs/BENCH.md` ("Threaded builds, blob parts and streams, the search pool, 2026-11-19") has the
tables, the commands and the host load of every run; raw results are
`bench/results/2026-11-19-*.jsonl`. Each before-and-after pair ran one after the other, both
starting at a 1-minute load under 10 on 20 cores; the earlier build is `f05eca7`.

- **The exact-route share (D61): met with `min_file_age` 0, not at the default.** At 1,000 rows
  every 0.5 s the mean share fell from 26.6 % to 3.3 % (1.8 % over the second half), under the
  5 % aimed for; at 1,000 rows every 0.1 s from 82.6 % to 27.9 %. With the default
  `min_file_age` of 60 s the 0.5 s load stays at 28.0 %: every small file waits a minute before
  compaction may take it. Files within the before-commit limits are never on the exact route,
  as before.
- **20,000-row appends with their graph built before the commit**: 1,265 ms at the median
  against 2,474 ms (D61's 9,176 ms was on a host at a load of 60 to 250).
- **Vector builds and search at a million rows**: builds 2.4 and 3.5 times faster on 10 and 100
  files; latencies unchanged within the noise (D83), so the D21 vector target stays missed on
  those layouts (D86).
- **Blobs**: streams on local disk 2.9 to 5.3 times faster than before and now at or above
  whole reads; RustFS writes unchanged (D82).
- **Compaction against Delta Lake on a quiet host**: 2.63 s and 264 MB against 4.88 s and
  356 MB, in one run with the engines interleaved at a load of 2. D62 left the time target open
  because the streaming merge had only been timed on a loaded host; it is met.
- **Not rerun: the fair comparison.** After these runs the host's load stayed between 10 and
  58 for the rest of the day, from other users; two attempts at its first repetition were
  discarded when the load passed 20 during them. The tracker, full-text and Lance-side vector
  numbers of the 2026-11-17 and 2026-11-18 runs stand, and `docs/BENCH.md` marks the sections
  whose numbers these changes make stale.

## D86. Defaults reviewed against the measurements

- **`build_threads`** (new): the machine's parallelism, for D85's build times; `build_threads(1)`
  gives byte-identical rebuilds.
- **`expansion_search` stays 128.** With threaded builds, 64 gives recall@10 of 0.977 to 0.980
  on one and 10 files of a million rows and 128 gives 0.996 to 0.998 for 0.3 to 0.8 ms more at
  the median; D37's reason, that the recall bound must hold at the defaults with margin, stands.
- **`min_file_age` stays 60 s,** although D85 shows it is what keeps the exact-route share at
  28 % on an append-only vector table. It keeps compaction off files an upsert workload is about
  to rewrite, and I have no measurement of that side, so I am not changing it on one workload's
  data. An append-only table with a vector index should set it to 0; D87 keeps a policy that
  exempts small files below an index's threshold as the open question.
- **Unchanged, with no measurement to justify a change:** `blob_inline_bytes` (256 KiB: values
  either side of it are bound by the append, not the threshold, at 4.9 and 22 MB/s on RustFS),
  the cache sizes, `target_file_bytes`, the compaction fan-out and step limits, and the
  group-commit linger (D64's).
- **New constants:** four blob parts in flight, 4 MiB stream ranges with four read ahead
  (D82), the step-row cap at half the commit deadline (D81), and the search pool for searches
  over more files than `io_concurrency` (D83).
- **Targets.** D21's vector target at a million rows is still missed on 10 and 100 files (p50
  2.7 and 4.0 ms, p99 9.2 and 6.2 ms against Lance's 3.1 and 5.1 ms of D37), met on one file.
  The 5 % exact-route share is met at `min_file_age` 0 only. Compaction time and memory are
  met against Delta Lake.

## D87. Deferrals and open problems after D80 to D86

- **The fair comparison on a quiet host**, all engines, local disk and RustFS, three
  repetitions, with the full-text and vector benchmarks: not run (D85). The harness and the
  rules (start under a load of 14, discard a repetition whose load passes 20) are ready.
- **Compaction re-measured with D65 and D68**, which changed its commit after the quiet run.
- **Vector latency on 10 and 100 files at a million rows.** Neither the pool nor the part
  threads moved it: the graph searches are not the larger part of those latencies. The next
  place to look is the rest of a search (opening sidecars, filter bitmaps, fetching the hits'
  rows), with a profile on a quiet host.
- **Small blob values on S3** stay at about 60 % of the server's PUT rate: an append uploads its
  values, then its data file, then its log entry. Uploading the data file with the values would
  close part of it; it is a write-path change.
- **`min_file_age` for append-only indexed tables** (D86): a planner that exempts files below an
  index's `min_rows_for_ann` from the age limit would give the 3.3 % share at the default.
- **Small appends took about 2 ms longer** in the build with D64 merged than in the one before
  (D85's table); I have not separated D64 from D80 to D83 there.
- **Full-text builds sync their temporary files** (D84): a tantivy directory that skips
  `sync_data` and `sync_directory` for build directories would remove most of the full-text
  simulation's wall time and the build's waits on a busy disk.
- **Threaded usearch builds re-link a few rows each** (D80); a usearch release that keeps every
  row reachable under concurrent adds would make the lookups unnecessary.

## D90. The public API for 0.1: what changed, what stays, and how betas are versioned

Before the first public release I went through the whole public API of `morainedb` once more,
against [the versioning policy](docs/SEMVER.md), so that 0.1 starts from an API I can grow
without breaking it. Every change below is a break, which is why it happens now: nothing is
released yet.

- **A smaller surface.** `commit`, the sidecar machinery (`index::{build, cache, discover, inline,
  key, query, spec, topk}`), the vector and full-text internals and the store wrappers are no
  longer public. What applications use is re-exported where they look for it: `IndexSpec`,
  `BuildOptions`, `BuildReport`, `IndexStatus`, `CacheMode`, `ExactScanPolicy`, the `Search*`
  types and `SCORE_COLUMN` from `moraine::index`; `HnswKind` and `TurboKind` from
  `moraine::index::vector`; `Tokenizers` and `FtsKind` from `moraine::index::fts`;
  `RetryPolicy`, `StoreMetrics`, `MetricsSnapshot`, `Op`, `OpCount` and `LocalCasStore` from
  `moraine::store`; `Error`, `StorageError`, `StorageErrorKind` and `LibraryError` from the
  root. What only the test kit, the benchmarks and the fuzz targets need (the commit retry
  loop, the store stack and its fault classes, the capability probe, server time, sidecar
  discovery, the caches, the key index) is in `moraine::testing`, which is `#[doc(hidden)]` and
  outside the policy. The `error` module is private; its types are at the root.
- **`#[non_exhaustive]`** on every public enum (`WriteOutcome`, `Consistency`,
  `DuplicatePolicy`, `CacheMode`, `ExactScanPolicy`, `Metric`, `Op`, `Order`, `BlobReuse`,
  `StorageErrorKind`, besides `Error`, `BundleError` and `Query`), on every report struct, on
  `TableDef`, `NotFound`, `Committed`, `IndexSpec`, the bundle header types, `BuildCtx` and
  `MergeInput`, and on the option structs. `Op::ALL`, an array whose length would change with a
  new request kind, became `Op::all()`. The test kit's and CLI's matches on `WriteOutcome` gained
  a wildcard arm or stopped matching.
- **Options are built with setters.** `TableOptions`, `MaintainOptions`, `CompactionPolicy`,
  `BuildOptions` and `RetryPolicy` start from `Default` (or `CompactionPolicy::full`,
  `RetryPolicy::COMMIT`, the new `MaintainOptions::none`) and take one unprefixed setter per
  field; the fields stay public to read. `RetryPolicy`'s setters are `const`, so a policy can
  still be a constant. `TableOptions::primary_key` is now a `Vec` (empty for no key) rather than
  an `Option<Vec>` whose `Some(vec![])` was an error, and `partition_by`, which had to be empty,
  is gone until partitioning exists. `IndexSpec::with_params`, `with_policy` and `with_search`
  became `params`, `policy` and `search`, the style of every other builder. Kinds' tests build
  `BuildCtx::new` and `MergeInput::new`.
- **`limit` names the result size** everywhere: `Search::limit` and `HybridSearch::limit`
  replace `k`, as on `Scan`, and `FileIndex::search` calls its argument `limit`. There is no
  deprecated `k`.
- **Plain writes return `Committed`.** `append`, `append_unchecked`, `upsert`, `upsert_with`,
  `delete`, `delete_keys`, `update`, `add_column` and `replace_files` return
  `Committed { version }` instead of a bare `u64`, and `WriteOutcome::Committed` carries the
  same struct. A row count was proposed as a second field; I left it out because a group commit
  does not count rows per member, and a field that is `None` for most writes would only
  mislead. The struct is non-exhaustive, so it can come later. `Committed::new` is public for
  tests of code that consumes write results. `refresh()` keeps returning the version as a
  `u64`: it is a read, and the version is all it has to say.
- **Errors.**
  - Every "not found" carries a `NotFound` (the name, the closest existing name, the existing
    names): `IndexNotFound`, `IndexKindNotRegistered`, `TokenizerNotRegistered`, `PinNotFound`
    and `BlobNotFound`, besides tables and columns.
  - `Error::Cancelled` is new. Since D64 a caller's own cancellation returns nothing to it (its
    future is gone) and fails nobody else, so the one case left is the table's commit task
    stopping before it took a write, when its runtime shuts down. That was `Internal`, which
    now means a bug only. It is retriable: nothing was written.
  - `Error` is `Clone`. The errors of other libraries are held in an `Arc`: `Storage` holds a
    `StorageError` with `kind()`, `is_not_found()` and `is_throttled()`, and `Arrow`, `Parquet`
    and `DataFusion` hold a `LibraryError`; `Io` holds an `Arc<io::Error>`. The library's error
    is the `source()`. A group commit now gives each member a clone of the group's error, so the
    hand-written copy that turned unknown variants into `Internal` and lost the store error's
    kind is gone.
  - Upgrading object_store, Arrow, Parquet or DataFusion no longer changes a variant. I kept the
    `From` conversions from their errors: they are what makes `?` work in code that mixes
    moraine and Arrow, and those libraries' major versions already break callers through
    `RecordBatch`, `Expr` and `ObjectStore`, so the conversions add no new coupling.
  - `CorruptLog(String)` became `Corrupt { object, reason }`: it was also returned for table
    properties, pin manifests and lease records, and the object is now a field rather than the
    start of the message.
- **Send and Sync are asserted at compile time** (`src/assertions.rs`) for the handles,
  builders, results, options and errors, and the futures of the public async methods are
  checked to be `Send` in a function that is never called, so a field change that breaks
  `tokio::spawn` breaks the build. `Replace` is `Send` only, since it may hold caller streams.
- **Features.** The empty `s3`, `gcs` and `azure` features are gone: S3 and local disk are
  always built in, and other stores open through `Database::builder_for_store`. I kept the other
  names. `vector` is the default vector kind (usearch, D37) and `turbovec` the alternative, so
  someone who wants "vector search" finds it by that name; `fts` is named the same way. The
  default stays empty: tables, scans, the key index, blobs and maintenance need no feature, and
  the index kinds pull in a C++ build (usearch) or a search engine (tantivy) that an application
  should choose. `failpoints` is documented as not for production and outside the policy.
- **Documentation.** Every public item has a doc comment, `#![warn(missing_docs)]` keeps it
  that way, and CI's clippy runs with `-D warnings`, so a missing comment fails it. The crate
  page has an overview and a quick start that runs as a doctest. docs.rs builds with every
  feature and `--cfg docsrs`, which shows the feature each gated item needs.

What I kept on purpose:

- `Database::builder_for_store(store, prefix)`. `builder_with_store` was suggested to read like
  `builder(location)`, but "for a store" says what it is, and a rename buys nothing else.
- `Table::search_custom(index, bytes)` with opaque bytes. A typed `Query` borrows its parts, so
  taking one would need an owned query type; that can be added beside it later without a break.
- `LocalCasStore` stays public: it is what an application wraps a `LocalFileSystem` in before
  `builder_for_store`. `StoreMetrics`'s counting methods stay public for store wrappers that
  count their own requests.
- The bundle reader (`BundleView`, `BundleBytes`, `Expect`, `SectionSink`) stays public, because
  an application's own kind needs it to test its sidecars. `BundleMeta`, `write_to_vec` and
  `FtsIndex` stay reachable but `#[doc(hidden)]`.
- `refresh()` returning `u64`, and the extension traits under `moraine::index` as they are; the
  policy already says they change more often than the table API before 1.0.

**Versioning of the betas.** `0.1.0-beta.N` releases may break the API from one beta to the
next, without deprecations; the changelog lists each break. Cargo uses a pre-release only when a
dependency names one, so nobody gets a beta by accident. From 0.1.0 the policy's rules for 0.x
apply, and from 1.0 `cargo-semver-checks` enforces them on every release.

## D91. A zero minimum file age merges every file, whatever the clocks say

`CompactionPolicy::full()` promises to merge small files "whatever their level or age", but it
compared each file's age with zero: server time minus the `modificationTime` the writing
client stamped from its own wall clock. The two clocks never agree exactly. Server time is the
last-modified time of a probe object, which S3 reports in whole seconds and a local file system
takes from a coarse kernel clock a few milliseconds behind the wall clock, and `ServerClock`
already documents that it runs early. A file written just before a full compaction therefore
often looked younger than zero and was skipped. On a local disk this left all five files of
`delta_rs_reads_a_moraine_checkpoint` at level 0 in most runs, which is the flaky
`left: 0, right: 1` of that test; on S3 it would skip every file written in the last second.

The flake was not introduced by a recent change: the test has failed since the commit that added
it, and the age check is as old as compaction. The test's assumption
(a full compaction right after the writes merges them) is the documented behaviour, so I fixed
the library, not the test.

- **A minimum age of zero skips the age check.** With `min_file_age` above zero the comparison
  stays as it was: the clocks can still disagree by up to a second or by the writer's skew, which
  only moves when a file counts as old enough. That is a heuristic against rewriting files an
  upsert workload is about to rewrite, not a correctness rule, so I did not try to correct it.
- **Regression test.** `full_compaction_merges_files_stamped_ahead_of_server_time` in
  `tests/maintenance.rs` writes with a client wall clock 5 s ahead of the simulated store, so
  every file is in the future by server time; a full compaction must merge them to one level-1
  file, and a 1 ms minimum age must still hold the next ones back. It fails deterministically
  without the fix.
- **Benchmarks.** The exact-route runs in `docs/BENCH.md` with `min_file_age` 0 compacted under
  this bug: on S3, each pass left out the files written in about the second before it. I have not
  rerun them.
