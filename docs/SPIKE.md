# Verification spike

This is how I validated the approach before writing any library code. The design rests on a few
facts about delta-rs, object stores, Parquet, tantivy and usearch that I could not confirm from
their documentation, and a wrong one would have changed the design, so I checked each with a
small program first. I keep the record because it shows what was measured and what was assumed.

Later work changed some of the consequences below. Most importantly, moraine now writes the
Delta log entries itself instead of committing through delta-rs, and leaves checkpoints to
maintenance ([`DECISIONS.md`](../DECISIONS.md) D18 and D25). The results themselves still stand.

The design listed twelve assumptions to check before any library code is written. Each
one has a test under `spike/tests/`, run against three backends where it makes sense: the
`object_store` in-memory store, the local filesystem, and an S3-compatible server (RustFS in
Docker). This page records what ran, the numbers, and what each result means for the design.

## Summary

| # | Assumption | Result | Key numbers | Design consequence |
|---|---|---|---|---|
| 1 | Safe commits on S3 without DynamoDB | **Pass** (S3-compatible, local, memory); real AWS not run | 8 writers x 200 appends: 16,000/16,000 rows, versions 0..1,600 gapless, no duplicate `txn` on all three backends | No DynamoDB and no custom `LogStore` needed. delta-rs 1.x has dropped DynamoDB and commits with `PutMode::Create` on S3 by default. Keep the 8-process run as the real-S3 regression test (T7). |
| 2 | Conflict checker fails a merge that should have seen concurrent rows | **Pass** | 0 duplicate keys in 2,800 contested rounds (memory, local, S3), including the default `WriteSerializable` level | Use delta-rs merge as the upsert. The write path must re-execute on `CommitConflict`; delta-rs only rebases non-conflicting commits. Fallback not needed. |
| 3 | Commit self-written Parquet files with `Add` + `CommitBuilder` | **Pass** | 50,000-row file invisible before commit, read back by a fresh handle with path, size and stats intact | Index-before-commit is possible. moraine must compute `Add` statistics itself. |
| 4 | Vacuum with 1 h retention, only inside the table root | **Partial** | Accepted only when `delta.deletedFileRetentionDuration` is at most 1 h; siblings untouched; tombstone age comes from the removing client's clock | Take the design's fallback for expiry: moraine decides tombstone age from server time (see item 4). |
| 5 | Checkpoints and log cleanup; concurrent checkpoints harmless | **Pass** | 500 commits from 4 writers: checkpoints every 20 versions, old commit files removed, same-version checkpoints byte-identical | Checkpoint versions are 19, 39, …, not multiples of 20. Rely on the post-commit hook; maintenance only needs to cover gaps. |
| 6 | `txn` action written and read back | **Pass** | Survives merge, delete, checkpoint and cleanup; open plus lookup 15-69 ms with 301 app ids | Use it for ambiguous outcomes. Set `delta.setTransactionRetentionDuration`, or every writer handle ever opened stays in the snapshot. |
| 7 | Custom `moraine.*` properties round-trip | **Pass, with a design change** | Round-trip works; but a property change or `add_column` makes every in-flight append fail with "Metadata changed" | The design's write path says metadata changes rebase over in-flight appends; they do not. Writers must re-execute on that error. delta-rs also rejects batches missing a nullable column. |
| 8 | Page index, bloom filter, row fetch by offset | **Pass, with a settings change** | 10 rows from a 1,000,000-row file: 10.9 MB with the design's settings, 1.7 MB with a 1,024-row page limit (265 KB for two narrow columns); bloom probe 16 KB | Add a page row limit to the Parquet settings and cache the footer and page index. |
| 9 | tantivy with external BM25 statistics | **Pass** | Scores equal the merged index exactly (difference 0) on four queries; per-file statistics differ by up to 0.18 and reorder the top 20 | Table-wide BM25 as planned. |
| 10 | usearch `view`, `filtered_search`, `u64` keys | **Pass** (with a recall warning) | View maps 13.2 MB using 160 KB of heap; no predicate violations; keys up to `u64::MAX - 1`; distances equal `1 - cos` exactly; recall@10 only 0.55 at `expansion_search` 64 on the synthetic set, 0.94 at 512 | The design's default `expansion_search` (64) is too low for the 0.95 recall target on this data; tune it with the vector index. `view_from_buffer` can map a bundle section in place. |
| 11 | Tables without deletion vectors; features visible | **Pass** | Default protocol is reader 1 / writer 2 with no features; a DV-enabled table shows `deletionVectors` in both feature lists | Refuse to open any table whose reader or writer features fall outside an allow-list. |
| 12 | Memory and local backends support `PutMode::Create` and `Update` | **Partial** | Memory and RustFS: both modes, stale ETag gives `Precondition`, 1 of 32 concurrent creates wins. Local filesystem: `Create` works, `Update` is `NotImplemented` | Wrap the local store to emulate `Update` (leases need it), as the design's fallback says. |

Nothing in items 1 to 3 changes the core design. The fallback for item 1 (a custom `LogStore`
writing `_delta_log/<version>.json` with `PutMode::Create`) was not needed: it is what delta-rs
already does. Two smaller fallbacks are taken: own tombstone-age logic for vacuum (item 4) and a
compare-and-swap wrapper for the local filesystem (item 12). Item 7 changes one row of the
write path's operations table.

## Environment and versions

- Rust 1.99.1, edition 2024, x86_64 Linux, 20 threads (13th Gen Intel Core i5-13500), 62 GB RAM.
- **All timings are from debug builds** (`cargo test`, unoptimised delta-rs and DataFusion; usearch,
  numkong and tantivy at `opt-level = 3`). They show orders of magnitude and relative costs only.
  Release-build commit throughput is not measured here; the benchmarks in [`BENCH.md`](BENCH.md) cover it.
- Crates: `deltalake` 1.1.2 (`deltalake-core` 1.1.4, `deltalake-aws` 1.0.0, `buoyant_kernel`
  0.28.1), `object_store` 0.13.2, `arrow` and `parquet` 59.3.2, `datafusion` 55.2.0, `tantivy`
  0.27.0, `usearch` 2.26.6 (`numkong` 7.8.7), `tokio` 1.54.1. All pinned in `Cargo.toml` and
  `Cargo.lock`.
- S3-compatible server: RustFS 1.0.6 (`rustfs/rustfs:latest`, image
  `sha256:ee717072290bec9b11abfc0365f8310b00df1cd9b3639943edae47bdef05044a`), started by
  `spike/s3-server.sh` on `127.0.0.1:19000`.
- Real AWS S3, GCS and Azure Blob: **not run** (needs a real bucket and credentials). Items 1 and
  12 have `#[ignore]` variants that run when `MORAINE_SPIKE_REAL_S3_BUCKET` and AWS credentials are
  set; GCS and Azure variants are left to T7.

Before trusting any S3 result, the server's conditional writes were checked directly with the AWS
CLI: a second `put-object --if-none-match '*'` and a `put-object --if-match` with a stale ETag
both returned `412 PreconditionFailed`. Item 12 repeats the check through `object_store`.

To reproduce: see `README.md`. Every test prints `RESULT` lines; the numbers below are copied
from them.

## 1. Safe commits on S3 without DynamoDB

**Run.** `spike_01_concurrent_appends.rs`. 8 writers, 200 appends each, 10 rows per append, each
commit carrying a `txn` action (`spike-writer-<w>`, `seq`). Local and S3: 8 separate OS processes
(`spike/src/bin/appender.rs`). Memory: 8 tasks with separate table handles on one shared store.
delta-rs's internal commit retries set to 10,000 so the writers never give up. The check reads the
table with a fresh handle and verifies every `(writer, seq)` has exactly 10 rows, every version
from 0 to 1,600 has a commit file, each writer's last `txn` version is 199, and no `txn` appears
in two commits.

**What the code does.** `deltalake-aws` 1.0.0 logs an error if `AWS_S3_LOCKING_PROVIDER=dynamodb`
is set ("delta-rs no longer supports S3DynamoDBLogStores") and always returns the default log
store. Its `write_commit_entry` writes `_delta_log/<version>.json` with `PutMode::Create`, and
`object_store` 0.13's S3 client defaults `conditional_put` to `ETagMatch`, which sends
`If-None-Match: *`; a 412 (or 304, as R2 returns) becomes `AlreadyExists`, which delta-rs turns
into "version exists, re-check conflicts and try the next version". A 409 is also mapped to
`AlreadyExists`; AWS returns 409 `ConditionalRequestConflict` when two conditional writes to one
key overlap, and in that case delta-rs re-reads the latest version before retrying, so it is
safe, but it is exactly the case the real-S3 regression test must exercise.

**Results.**

| Backend | Wall time | Commits/s | Rows | Latest version | Gapless | Bad groups | Duplicate `txn` |
|---|---|---|---|---|---|---|---|
| memory (8 tasks) | 355 s | 4.5 | 16,000/16,000 | 1,600 | yes | 0 | 0 |
| local (8 processes) | 485 s | 3.3 | 16,000/16,000 | 1,600 | yes | 0 | 0 |
| RustFS (8 processes) | 671 s | 2.4 | 16,000/16,000 | 1,600 | yes | 0 | 0 |

Per-commit latency per writer (debug build): p50 0.12-0.73 s on memory and local, 1.2-2.5 s on
RustFS; p99 4-39 s; worst single commit 57 s. No writer ever saw an error. The count of delta-rs
internal retries per commit was meant to come from `commitInfo.operationMetrics.numRetries`, but
appends do not record it: **not measured**.

The latency tail comes from contention: every lost race re-reads the winning commits and the log
since the last checkpoint (default interval 100 here). Item 5 ran 500 commits from 4 writers with a
checkpoint every 20 versions in 12 s on the local disk, against 485 s for 1,600 commits from 8
writers here. Commit throughput under contention is the design's stated weak point and
needs release-build numbers.

**Consequence.** Pass on everything that could be run. delta-rs's own log store is the commit
point; no custom `LogStore`, no DynamoDB. Real-S3 confirmation is still owed (T7), with attention
to 409 responses. The write path's capability probe stays, since a store that ignores
`If-None-Match` would silently lose commits.

## 2. Conflict checker and concurrent upserts of one key

**Run.** `spike_02_upsert_race.rs`. Each round uses a new key. Two writers, each with its own
handle, refresh, meet at a barrier, then write the key at the same moment. A writer whose commit
fails reloads and runs its operation again (what the design's write path does). At the end every key
must have exactly one row.

| Backend | Variant | Isolation | Rounds | Keys with duplicates | Conflicts retried (w0, w1) |
|---|---|---|---|---|---|
| memory | merge vs merge | WriteSerializable (default) | 1,000 | 0 | 836, 164 |
| memory | merge vs merge | Serializable | 300 | 0 | 0, 300 |
| memory | append vs merge | WriteSerializable | 300 | 0 | 0, 300 |
| memory | append vs merge | Serializable | 300 | 0 | 0, 300 |
| memory | merge vs merge, delta-rs retries 0 | WriteSerializable | 300 | 0 | 0, 300 |
| memory | append vs merge, delta-rs retries 0 | WriteSerializable | 300 | 0 | 0, 300 |
| local | merge vs merge | WriteSerializable | 300 | 0 | 300, 0 |
| local | append vs merge | WriteSerializable | 300 | 0 | 0, 300 |
| RustFS | merge vs merge | WriteSerializable | 200 | 0 | 176, 24 |
| RustFS | append vs merge | WriteSerializable | 200 | 0 | 0, 200 |

Every round produced exactly one conflict, and the loser's re-execution turned its insert into an
update. The merge records its read predicate in `commitInfo` as `key = 'round-N'` (derived from
the source keys), and the conflict checker matches the winner's added file against it using the
file statistics.

In the append-vs-merge rounds the append always committed first (it is cheaper), so the merge
always lost and re-read. The opposite order, a merge committing first and a blind append of the
same key after it, cannot conflict: the append reads nothing. That is the design's C3 case and the
reason for `enforce_primary_key`; the spike confirms it is needed rather than testing it.

**Consequence.** The design's assumption holds; the fallback (retries zero for every merge) is not
needed, and the default delta-rs retry count can stay for appends. Open for upserts: how the
recorded predicate looks for a large upsert batch (a long `IN` list or a range), since a broad
predicate turns into false conflicts.

## 3. Committing files the library wrote itself

**Run.** `spike_03_commit_own_files.rs`. Write a 50,000-row Parquet file with `ArrowWriter` and
the design's data-file settings, upload it under the table root, check a fresh handle does not see
it, then commit an `Add` (path, size, modification time, `dataChange`, statistics JSON) through
`CommitBuilder` with `DeltaOperation::Write { mode: Append }`, and read it back with another fresh
handle.

**Result.** On all three backends: 462,945-byte file, invisible before the commit, committed as
version 1, path, size and `numRecords` statistics round-trip, all 50,000 rows read back.

**Consequence.** Pass. Sidecars can be built from the file bytes before the commit makes the file
visible, so index before commit stays possible in full. delta-rs's helper that derives `Add` statistics from
a Parquet footer is crate-private, so moraine computes min, max and null counts itself (from the
writer's metadata). The second-tool read (DuckDB or delta-rs Python) is left to T11.

## 4. Vacuum: short retention, scope and age source

**Run.** `spike_04_vacuum.rs`. Table at `db/t/data` with siblings `db/t/idx/…`, `db/t/blobs/…`,
`db/t/meta/…`, another table's file at `db/t-other/…`, and `_moraine/marker` and `.hidden` inside
the table root. Three data files; delete one through delta-rs (tombstone stamped with this client's
clock); remove a second with a hand-written `Remove` whose `deletionTimestamp` is two hours in the
past (a client whose clock runs two hours slow); upload an orphan file that is never committed.
Then vacuum in Lite and Full mode, with the real clock and with an injected clock two hours ahead.

| Step | Result (same on all three backends) |
|---|---|
| 1 h retention, table with default properties | Error: "minimum retention for vacuum is configured to be greater than 168 hours" |
| 1 h retention, table with `delta.deletedFileRetentionDuration = interval 1 hours` | Accepted |
| Lite, real clock | Deleted the file whose tombstone was backdated, seconds after its removal |
| Full, real clock | Did not delete the fresh orphan (protected by its object modification time) |
| Lite, clock +2 h | Deleted both tombstoned files |
| Full, clock +2 h | Deleted the orphan as well |
| Afterwards | Siblings, the other table and the hidden or underscore-prefixed objects untouched; live rows 5 of 5 |

**Consequence.** Partial pass. Retention and scope behave as the design needs, provided the table
sets `delta.deletedFileRetentionDuration` (it already does). The age source does not: for removed
files, vacuum compares the tombstone's `deletionTimestamp`, written by whichever client removed the
file using its own clock, against "now". A client with a slow clock makes files collectable early,
which breaks invariant I7 and edge case G1. Untracked files (Full mode) are aged by object
modification time against the vacuum's clock, which can be injected (`with_clock`), so passing
server time works there.

Taking the design's fallback in part: moraine's vacuum computes the candidate list itself from the
snapshot (tombstones plus a listing of `data/`), ages a tombstone by the server modification time
of the commit file that removed it, ages orphans by object modification time, and compares both
with server time. delta-rs's vacuum can still do the deleting given an explicit list, or moraine
deletes directly; the decision belongs with maintenance.

## 5. Checkpoints and log cleanup

**Run.** `spike_05_checkpoints.rs`. Table with `delta.checkpointInterval = 20`,
`delta.logRetentionDuration = interval 2 seconds` (short, so cleanup is visible inside a test) and
expired-log cleanup on. 4 writers x 125 appends = 500 commits (processes on local and RustFS,
tasks in memory). Then two handles checkpoint the same version, first one after the other and
then concurrently.

| Backend | Wall time | Commit files left | Checkpoints left | `_last_checkpoint` | Rows | `txn` intact |
|---|---|---|---|---|---|---|
| memory | 10.6 s | 62 (from 439) | 439, 459, 479, 499 | version 499 | 2,000/2,000 | yes |
| local | 12.1 s | 82 (from 419) | 419, …, 499 | version 499 | 2,000/2,000 | yes |
| RustFS | 42.2 s | 22 (from 479) | 479, 499 | version 499 | 2,000/2,000 | yes |

Same-version checkpoints were byte-identical (155 KB for 500 files) on every backend, and two
concurrent checkpoints both succeeded with the table loading normally afterwards.

**Consequence.** Pass. Checkpoints are written by the committing client's post-commit hook at
versions where `(version + 1)` is a multiple of the interval, and cleanup removes old commit files
and old checkpoints together, never past the latest checkpoint. The design's maintenance step
"checkpoint when the version is a multiple of the interval" should follow the same rule, and only
needs to cover a checkpoint a crashed writer skipped. A smaller interval also keeps contended
commits fast (see item 1).

## 6. Application transaction IDs

**Run.** In `spike_06_07_11_log_metadata.rs`. One writer ID (a UUID) commits an append, a merge
and a delete with `txn` versions 1, 2, 3; reopen and look up; checkpoint and look up again; scan
the commit files from the base version for a given `(appId, version)`; then add 300 more app IDs
and time a fresh open plus lookup.

**Result.** On all three backends: lookup gives 3 after the three operations and after the
checkpoint; the scan finds version 1 in the commit where the append landed and nothing for an
absent version; open plus lookup among 301 app IDs took 15 ms (memory), 31 ms (local), 69 ms
(RustFS).

**Consequence.** Pass. `CommitProperties::with_application_transaction` works on writes, merges
and deletes. The lookup returns only the latest version per app ID, so resolving an ambiguous
outcome should compare against the counter the writer used (the design's writer UUID plus counter
fits). Without `delta.setTransactionRetentionDuration`, every writer handle ever opened leaves a
`txn` entry in every future checkpoint; set it at table creation (it needs to exceed the commit
deadline, so the grace period is a natural value).

## 7. Custom table properties

**Run.** Same file. Create a table with `moraine.formatVersion`, `moraine.indexes` (JSON) and
`moraine.primaryKey`; change one with `set_tbl_properties`; then four races between a metadata
change and an append, each prepared against the same version.

| Check | Result (same on all three backends) |
|---|---|
| Create with a `moraine.*` key, default key check | Error: "Error parsing property 'moraine.formatVersion'" |
| Create with `with_raise_if_key_not_exists(false)` | Accepted; all three keys come back in `table_properties().unknown_properties` |
| `set_tbl_properties` with `with_raise_if_not_exists(false)` | Accepted; new value visible after reopening |
| Append prepared, then a property change commits first | **Append fails: "Metadata changed since last commit"** |
| Property change prepared, then an append commits first | Property change commits (rebases) |
| `add_column` prepared, then an append commits first | `add_column` commits (rebases) |
| Append prepared, then `add_column` commits first | **Append fails: "Metadata changed since last commit"** |
| Batch without a nullable column the table has | Fails before committing: "Cannot cast schema, number of fields does not match" |

**Consequence.** Pass for storage of index specs and options in table properties, so
`meta/indexes.json` is not needed. Two corrections to the design:

- The design's write path says `add_column`, `create_index` and `drop_index` rebase and that in-flight
  appends with the old schema still commit (C10, C11). In delta-rs any metadata commit fails every
  transaction prepared before it. The write path must treat that error like a conflict: reload the
  snapshot and re-execute (for an append, the uploaded files can be committed again against the new
  version when the schema change is compatible). Index changes are rare, so the cost is a retry,
  not a design change.
- Edge case D3 says a missing nullable column is filled with null. delta-rs does not do that,
  so moraine fills missing nullable columns before handing a batch to delta-rs.

## 8. Parquet page index, bloom filter and row fetch by offset

**Run.** `spike_08_parquet_row_fetch.rs`. A 1,000,000-row file with `id` (string key, bloom
filter), `n` (int64), `title` (string) and `embedding` (`FixedSizeList<Float32, 32>`, uncompressed,
no dictionary); row groups of 16,384 rows (62 groups), page index on, Zstandard level 3 elsewhere.
Written twice: with the design's settings only (1 MiB page size limit), and with an extra page row
limit of 1,024 rows. Then fetch the rows at 10 scattered offsets through
`ParquetRecordBatchStreamBuilder` over `ParquetObjectReader` with a `RowSelection`, counting GET
requests and bytes with a wrapping store.

| File | Pages per column chunk | Footer + page index | 10 rows, all columns | 10 rows, `id` + `n` | Without page index |
|---|---|---|---|---|---|
| Design settings (141 MB) | 1, 1, 1, 2 | 57.5 KB in 3 GETs | 10.9 MB in 70 GETs | 756 KB in 40 GETs | 20.7 MB |
| + 1,024-row page limit (140 MB) | 16 each | 269 KB in 3 GETs | **1.69 MB** in 70 GETs | **265 KB** in 40 GETs | 20.6 MB |

Bloom filter probe of one row group: 1 GET of 16 KB; present key found, absent key rejected.
Timings on RustFS: 84-168 ms for the all-column fetch with the page limit, 63 ms for two columns.
Restricting the read to the row groups that hold the offsets did not change the bytes (the reader
already skips unselected groups) but cut latency by about a third.

**Consequence.** Pass for the mechanism, with a settings change. A 1 MiB page size limit alone
leaves one page per column chunk for narrow columns, so a single-row fetch reads the whole chunk.
Add `data_page_row_count_limit = 1024` (a table option) to the data-file settings. The cost is a
larger page index (269 KB per million rows here), which is one more reason for the footer cache the
design already has. 70 GETs for 10 rows means the reader should coalesce nearby ranges; the sidecar
work writes its own `AsyncFileReader` (the stock one is deprecated in parquet 59) and can do that there.
The Zstandard level is not stored in the file, so reading the metadata reports level 1.

## 9. tantivy with external BM25 statistics

**Run.** `spike_09_tantivy_bm25.rs`. 20,000 synthetic documents (Zipf-distributed words from a
40-word vocabulary), split into two per-file indexes plus one merged index over all of them. Each
per-file index is searched with `Searcher::search_with_statistics_provider` and a provider that
sums `total_num_docs`, `total_num_tokens` and `doc_freq` over both searchers.

| Query | Max score difference vs merged (top 50), with provider | Same, per-file statistics | Top 20 equal (provider / per file) |
|---|---|---|---|
| `sidecar compaction` | 0 | 0.102 | yes / no |
| `vacuum OR lease` | 0 | 0.032 | yes / no |
| `checkpoint backoff conflict` | 0 | 0.178 | yes / no |
| `"object storage"` (phrase) | 0 | 0.0018 | yes / no |

Building the three indexes took 185 ms.

**Consequence.** Pass. Table-wide BM25 statistics give exactly the merged index's scores, well
inside F5's 1e-5. Per-file scores are visibly different, so the provider is worth having.

## 10. usearch: view, filtered search, `u64` keys

**Run.** `spike_10_usearch.rs`. 20,000 clustered synthetic vectors (50 centres plus noise), 128
dimensions, cosine metric, connectivity 16, expansion 128 when adding and 64 when searching, keys
equal to row offsets. 100 queries compared with an exact search computed separately.

| Measure | Result |
|---|---|
| Build, single thread | 1.1 s (F32), 0.9 s (F16), 0.8 s (I8) |
| Saved size (raw vectors are 10.2 MB) | 13.2 MB (F32), 8.1 MB (F16), 5.5 MB (I8) |
| Heap after `view` vs `load` | 160 KB vs 33.7 MB |
| Distance vs `1 - cosine similarity` computed by hand | identical (maximum difference 0) |
| Recall@10, `expansion_search` 64 / 128 / 256 / 512 | 0.551 / 0.656 / 0.801 / 0.935 |
| Search time per query at those settings | 63 / 85 / 127 / 266 µs |
| `filtered_search`, 10% and 1% of keys allowed | recall 0.936 and 0.999, no predicate violations, never short of 10 results |
| Keys `u64::MAX - 1`, `2^40`, 5 | stored and returned unchanged |
| `view_from_buffer` on a slice at offset 4,096 of a larger buffer | works, same results as `view` |

Recall was the same for the viewed and the loaded index, and there were no near-ties at the tenth
place, so the low figure is the graph search on this data, not a measurement artefact.

**Consequence.** Pass for every capability the design needs. `view_from_buffer` means a vector
section can be used straight from a memory-mapped bundle without unpacking (the caller must keep
the mapping alive and unchanged). The design's default `expansion_search` of 64 does not reach the
0.95 recall target on this set; the vector index work should measure on more realistic vectors (T9) and
probably raise the default. The synthetic data here is harder than typical embeddings, so this
number is a warning, not a verdict.

## 11. Deletion vectors and table features

**Run.** Same file as items 6 and 7. Inspect the `protocol` action of a default table and of one
created with `delta.enableDeletionVectors = true`, through the commit JSON and through the typed
snapshot (`snapshot().protocol()`, serialised); then delete a row in the second table.

**Result.** Default table: `{"minReaderVersion":1,"minWriterVersion":2}`, no features. DV-enabled
table: reader 3 / writer 7, `readerFeatures: ["deletionVectors"]`,
`writerFeatures: ["appendOnly","invariants","deletionVectors"]` (order varies), identical through the
typed snapshot. delta-rs's delete on that table rewrote the file and wrote no deletion vector.

**Consequence.** Pass. moraine creates tables with `delta.enableDeletionVectors = false` (already
the default) and, on open, refuses any table whose reader or writer features include something
outside an allow-list, with `UnsupportedTableFeature`.

## 12. Conditional writes on the test backends

**Run.** `spike_12_conditional_put.rs`. `PutMode::Create` twice; `PutMode::Update` with the
current ETag, again with the now stale one, and against a missing object; then 32 concurrent
creates of one new key.

| Backend | Create / again | Update current / stale / missing | Concurrent creates won |
|---|---|---|---|
| memory | ok / `AlreadyExists` | ok / `Precondition` / `Precondition` | 1 of 32 |
| local filesystem | ok / `AlreadyExists` | `NotImplemented` (all three) | 1 of 32 |
| RustFS | ok / `AlreadyExists` | ok / `Precondition` / `Precondition` | 1 of 32 |

**Consequence.** Partial pass. Commits (which need only `Create`) work on every backend. The local
filesystem cannot compare-and-swap, which the advisory leases under `meta/` need. Take the design's
fallback: the store layer wraps `LocalFileSystem` with an `Update` emulation (a per-key lock plus
an ETag check and rename), which is enough for the single-host use the design allows (S11). Without
it, leases on local disk are simply off, which is still correct because leases are only hints.

## Not measured

- Commit throughput and latency in a release build.
- delta-rs internal retries per commit (appends do not record `numRetries`).
- Anything on real AWS S3, GCS or Azure Blob.
- usearch recall on real embeddings and at the design's 384 to 1,536 dimensions.
- tantivy build time and bundle size per data file at realistic text lengths.
