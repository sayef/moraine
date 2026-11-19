# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version may break the API.

## [Unreleased]

## [0.1.0-beta.1](https://github.com/sayef/moraine/releases/tag/v0.1.0-beta.1) - 2026-11-19

The first release, published as betas (0.1.0-beta.N) before 0.1.0, contains the following.

### Added

- Databases on S3 (`s3://bucket/prefix`), S3-compatible servers, local disk and memory, with a
  capability probe that refuses to write to a store that ignores conditional writes.
- Tables stored as Delta Lake tables (reader version 1, writer version 2): create and open,
  `append`, `upsert`, `delete`, `delete_keys`, `update` and `add_column`.
- Group commit per table handle: one PUT per data file and one conditional PUT of the next log
  entry per commit, with no listing on the write path; conflict-checked rebase after a lost
  race, a starvation guard, and a commit deadline.
- Ambiguous commit outcomes resolved through the handle's own transaction id.
- Exactly-once writes with caller transaction ids: `Table::txn(app_id, version)`,
  `WriteOutcome::AlreadyApplied` and `last_txn_version`.
- Reads at four consistency levels (`Strong`, `Eventual`, `Background`, `Manual`), scans with filters and
  projections, point lookups by primary key, and pinned snapshots.
- Data files sorted by cluster key and primary key, with 1,024-row pages, bloom filters on key
  columns, and pruning by file key ranges, bloom filters and the page index; per-version caches
  for footers, key indexes and small tables.
- Sidecar index bundles with a pluggable `IndexKind`: the key index (used by `get`), vector
  search with usearch (`vector` feature) and TurboQuant through turbovec (`turbovec` feature),
  the exact route for files without a sidecar, memory-bounded index builds, `build_indexes` and
  `index_status`.
- Full-text search with tantivy (`fts` feature): the `fts` index kind, `Table::search_text`,
  BM25 statistics summed over every data file so rankings equal one index over the whole table,
  an in-memory index for files without a sidecar (`DatabaseBuilder::fts_memory`), and a tokenizer
  registry with stemmers for 18 languages, character n-grams and custom tokenizers
  (`DatabaseBuilder::tokenizer`).
- Hybrid search, `Table::search_hybrid`: a vector and a full-text search against one snapshot,
  fused with reciprocal rank fusion or a `Fusion` of the caller's.
- `Consistency::Background`: a task per table handle refreshes the snapshot and prewarms the
  caches for each new version; `Table::refresh_metrics`.
- `Table::snapshot_in_hand`, `TableSnapshot::files` and `Database::cache_usage`.
- List columns with any item field name (Arrow's default `item` included), stored under Delta's
  `element`.
- Credentials and bucket region from the AWS SDK (`aws-sdk` feature), and object tags on every
  write.
- Maintenance: `Table::maintain` runs the index build, the ingestor of queued writes,
  compaction, checkpoints and log cleanup, then vacuum and sidecar collection, each under an
  advisory lease; `Table::spawn_maintenance` runs passes on a timer.
- Tiered, sorted compaction (`Table::compact`, `CompactionPolicy`): levels in the Delta
  `add.tags`, sidecars built before the commit, commits that keep the file order.
- Checkpoints written and read by moraine, `_last_checkpoint`, and log cleanup on server time;
  opening a checkpointed table reads the checkpoint and the entries after it.
- Vacuum on server time (`Table::vacuum`), keeping what snapshots younger than the grace period
  and pins need, and collection of sidecars, pin manifests and abandoned clock probes.
- Named pins: `Table::pin`, `unpin`, `pins` and `snapshot_at_pin`.
- An atomic, conflict-checked file replace: `Table::replace_files` and `Replace`, with file tags
  and optional lease fencing.
- Generation-file leases fenced through Delta transactions: `Table::try_lease`, `renew_lease`,
  `release_lease`, and `Error::Fenced`.
- Queued writes: `Table::submit`, `ingest` and `is_ingested`, applied exactly once.
- `Table::verify` and `Table::repair`; `Database::table_names`.
- Queries move to the newest version once when vacuum removed a file under an old snapshot,
  then report `Error::SnapshotExpired`.
- The `moraine` command: `tables`, `inspect`, `maintain`, `compact`, `checkpoint`, `vacuum`,
  `ingest`, `verify`, `repair`, `pins`, `pin`, `unpin` and the `stress` writer of T8.
- Blobs (`moraine::blob`): a blob column type whose values stay in the row up to the table's
  `blob_inline_bytes` (256 KiB by default) and are stored as objects of their own above it, with
  multipart uploads above 16 MiB; `BlobColumn`, `blob::refs`, `Table::blob` and `Table::blobs`
  with whole, ranged and streamed reads checked against a CRC-32C; server-side copies of reused
  references (`BlobReuse`); `Table::upload_blob` for values streamed ahead of a write; blob
  collection in vacuum; missing blobs in `verify`; `Error::BlobCorrupt`.
- Index before commit: writers build their files' sidecars before the commit, within each
  index's `before_commit`, `before_commit_max_rows` and `before_commit_max_build_bytes` policy;
  `IndexStatus::rows_exact_by_policy`, `rows_pending` and `exact_share`.
- Streaming compaction (`CompactionPolicy::streaming`, on by default): a k-way merge over the
  sorted inputs with memory bounded by row groups; `vec-tq` sidecars of merged files are merged
  from the inputs' codes; `CompactionReport::streamed` and `sidecars_merged`.
- Store metrics count multipart parts and completions (`Op::Part`, `Op::Complete`).
- Purge and erasure: `Table::purge` with `Purge` (`removed_before`, `erase`) collects removed
  files at once rather than after the grace period, with their sidecars and blobs, and erases
  their statistics from the log, older checkpoints and stale pin manifests; `PurgeReport`,
  the `moraine.purged` table property and `Error::LeaseHeld`.
- File-level reads: `Scan::files`, `TableSnapshot::take` (rows by file and offset, `Take`),
  `Table::id` and `TableSnapshot::table_id`, and `Scan::stream`.
- File replaces from a stream or an iterator of batches: `Replace::add_stream` and
  `add_batches`.
- `Table::write_metrics` (`WriteMetrics`): uploads deleted by writes that did not commit them,
  and uploads left for vacuum.
- `Clock::spawn`, which runs a table handle's commit task.
- `moraine-testkit`: a fault-injecting store, a simulated clock and store, a seeded scheduler, a
  reference model, an invariant checker, deterministic corruption fuzzing, text corpora with a
  merged tantivy reference index, a soak test with a counting allocator, and maintenance
  simulations checked against the store's full history.
- Index builds on several threads: `DatabaseBuilder::build_threads` (default: the machine's
  parallelism) for usearch graphs, and the builds of a file's several indexes at once.
- Compaction steps of indexed tables capped to the rows their sidecar builds finish in half
  the commit deadline, at the rate this handle's builds have run.
- Blob uploads with four parts in flight, and blob streams read as 4 MiB ranged GETs with
  read-ahead.
- Vector searches over more files than `io_concurrency` search every file's parts on one
  work-stealing pool; the `moraine.pool` search parameter forces either path.
- `moraine-testkit`: a per-seed watchdog (`MORAINE_SIM_SEED_TIMEOUT_S`) that fails a
  simulation seed which runs too long.

### Changed

- Group commits run on a task owned by the table handle: a caller that stops waiting no longer
  fails the other writes of its group, and a panic in a group commit is raised in every caller of
  the group.
- A write that does not commit deletes the data files, sidecars and blobs it uploaded once its
  outcome is settled, instead of leaving them to vacuum.
- Compaction merges only files with the same caller tags, and the merged files keep them.
- Pinning a version older than the newest purge is refused with `Error::VersionNotAvailable`.
- A checkpoint pass deletes its own checkpoint when a purge committed after it read the log;
  log cleanup deletes checkpoints, and vacuum pin manifests, older than the newest purge.
- `Replace` is no longer `Clone`: it may hold a stream.
- Compaction builds its merged files' sidecars from the rows it writes, without reading the
  files back and without a HEAD per sidecar.
- Writes build the key index of large files through the same before-commit path as other
  indexes.
- Vector files built in parts search their graphs concurrently.
- Full-text statistics are kept per version; background prewarm builds in-memory text indexes
  only once the table has been searched by text; string columns are written in pages of at most
  64 KiB.
