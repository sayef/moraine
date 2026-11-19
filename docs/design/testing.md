# Testing and simulation

A database without a server has to be correct under every interleaving of clients, every
request that fails, and every request that succeeds but loses its response. Those cases are rare
in a test run against a real store and common in production. moraine's answer is a
deterministic simulation: whole clusters of clients run on one thread against a simulated store,
a seed decides every interleaving and every fault, and a failing seed replays exactly.

The test kit is the crate `moraine-testkit`. It was built first, before tables existed, and every
feature since was accepted with it. The IDs used below (invariants, edge cases, suites and
numbered scenarios) are defined in [Test and edge-case IDs](test-ids.md).

## The pieces

| Piece | What it does |
|---|---|
| `SimStore` | An in-memory object store whose timestamps come from the simulated clock, and whose contents are ordered, so every decision depends on the seed alone. |
| `ChaosStore` | Wraps any store and injects faults from a seed and a rule list: errors, throttling, timeouts, and **ambiguous outcomes**, where a write is applied and its response is lost. It also counts requests and bytes. |
| `SimClock`, `ClientClock` | Server time and per-client time under the test's control; client clocks can be wrong by hours, as real ones can. Backoff jitter and object names also come from the clock, so they replay. |
| `Scheduler` | Runs logical clients on one thread and picks, from the seed, which pending store call or timer proceeds next. |
| `model` | The reference model of a table: what its rows must be after a given sequence of operations. |
| `check` | The invariant checker. Clients record what they asked for and what they were told; the checker compares that with the true commit log read from the store. |
| `table_log` | Reads a table's history from the store without moraine, so moraine's own log reader is checked too. |
| `fuzz` | Deterministic byte-level and JSON-aware mutations for the corruption tests. |

## The invariants

| ID | Invariant |
|---|---|
| I1 | An acknowledged operation is in the log at the version it was acknowledged with. |
| I2 | An operation that returned an error (other than "outcome unknown") is not in the log. |
| I3 | Versions are gapless, every read equals the model at its version, and the rows physically stored at every version equal the model too. |
| I4 | With keys enforced, no read and no version holds a key twice. |
| I5 | A client's reads never go back in time and never miss its own acknowledged writes. |
| I10 | No operation is in the log twice, and each entry is exactly what was intended. |
| I11 | A caller transaction id commits at most once, and an operation told "already applied" is not in the log. |

Since upserts and deletes, the checker reads every version's live files from the store and compares them with
the model, not only the reads clients made. A version is accepted only if some order of its
operations explains what it stores.

## Scenarios

| Test | What it simulates |
|---|---|
| `toy_counter` | A counter object updated by compare-and-swap: the first check of the harness itself |
| `tables_sim` | Appends, group commit, snapshot reads, point lookups and refresh: `no_lost_appends`, `ambiguous_commit_is_idempotent`, `create_table_race`, `store_without_conditional_put`, and `tables_random` with random faults |
| `mutations_sim` | Upsert, delete, update and added columns from several clients and tasks: `upsert_same_key_race`, `schema_add_column_race`, `mutations_random`, `mutations_no_faults` |
| `txn_sim` | Exactly-once replay: `txn_crash_replay`, `txn_ambiguous_replay`, `txn_concurrent_replay` |
| `segmentation` | The same final rows written in one append and through concurrent clients in random chunks must give bit-identical results for every query |
| `fts_sim` | Full-text sidecar builds racing appends, upserts and deletes from several writers; at every version a reader sees, answers with and without sidecars must be identical, and at the end rankings must equal one tantivy index over the final rows |
| `crash_points` | A writer that crashes at each step boundary of a commit (failpoints), restarts and carries on |
| `maintenance_sim` | Maintenance racing writes, readers and itself, with simulated time passing the grace period: `delete_never_resurrects` (3), `reader_survives_vacuum` (6), `maintenance_everywhere` (12), `partial_listing` (18), `maintenance_random`, and `queued_writes_exactly_once`; every run also checks `collector_never_deletes_live` (13) and `collector_eventually_cleans` (14) against the store's full history, deleted objects included |
| `crash_maintenance` | Scenario 5, `crash_everywhere`: a crash at every rewrite, compaction, replace, checkpoint, log cleanup, vacuum, purge, submit and ingest failpoint, a restart, then maintenance past the grace period and the full history checks |
| `blobs_sim` | Blob uploads, pass-through copies and staged uploads racing blob collection, pins, crashes mid-write and random faults: `blob_upload_vs_collection`, `blob_copy_vs_collection`, `blob_collection_respects_pins`, and scenario 7 `slow_writer_vs_collectors`; every run checks against the store's history that no object a version younger than the grace period or a pinned version references was deleted, that every read gets every blob's bytes, and that `blobs/` ends holding exactly the live references |
| `failed_uploads_sim` | Appends, upserts, deletes, file replaces and fenced compaction from several clients with random faults and no vacuum (`failed_uploads`): every file a log entry adds stays in the store, and a file no entry adds is left only when a write's outcome was unknown, a delete failed or a compaction step failed before its commit, never without faults (D65) |
| `purge_sim` | Purges with and without erasure racing writers, pins, readers and maintenance with random faults (`purges_everywhere`): no data file deleted while the version in force or a current pin held it, no commit or pin of a deleted file, and after a last purge no removed file, no erased statistics in any log entry, no older checkpoint and no stale pin manifest (D66) |
| `crash_blobs` | A crash right after blob uploads, after a copy, after a staged upload, between blob collection's mark and sweep, and before a streamed compaction output goes up; a restart, then collection past the grace period |
| `t8` (in `moraine-cli`) | T8: eight `moraine stress` processes with random `kill -9` on local disk, then `verify` and the acknowledged writes checked |

A store call waits at most 30 seconds of simulated time at its gate: a timer that would move time
further fires only when no call is waiting, as a real request times out instead of staying in
flight for minutes.

## What the acceptance runs covered

- **The harness:** the toy client passed 10,000 seeds, and a deliberately broken version failed
  with a replayable seed.
- **Upsert, delete and update:** 100,000 seeds of each of `mutations_random`, `mutations_no_faults`,
  `upsert_same_key_race` and `schema_add_column_race`, built in release mode and run in 16
  shards: about 11.7 million operations and 2.6 million injected faults (0.72 million of them
  ambiguous commits), with no violation and no unknown outcome. The first full run failed eight
  seeds, all in the test's own explanation or polling code, none in moraine; they are regression
  seeds now.
- **Exactly-once writes:** 2,000 seeds of each of the three replay scenarios: 99,000 log entries,
  189,000 attempts, 84,000 "already applied" answers, 5,300 process crashes, 72,000 ambiguous log
  puts, and no violation. With the snapshot check disabled on purpose, the concurrent scenario
  fails on its first seed.

- **Full-text search:** 10,000 seeds of `fts_sim`, run in 16 shards: 99,082 sidecars built while
  writes ran, 40,863 comparisons of answers with and without sidecars at the same version, and
  60,000 final queries equal to a merged tantivy index, with no failure.

- **Maintenance:** 10,000 seeds of each maintenance scenario, release build, ten shards each:
  `maintenance_random` (182,000 versions, 321,000 injected faults), `maintenance_everywhere`
  (336,000 maintenance passes with leases off), `delete_never_resurrects` (70,000 passes between
  182,000 acknowledged upserts and deletes), `partial_listing` (466,000 faults, mostly cut
  listings), `reader_survives_vacuum` (7,298 readers past the grace period, each restarted once
  through its handle and told `SnapshotExpired` on its held snapshot) and
  `queued_writes_exactly_once` (381,000 acknowledged queued batches under 126,000 faults); about
  680,000 data files, 470,000 sidecars and 1.1 million log entries deleted by collectors, each
  checked against the store's history. `crash_everywhere` crashed in every one of 10,000 seeds at
  each of its 15 failpoints. The earlier scenarios ran 10,000 seeds each again
  (`mutations_sim`, `txn_sim`, `segmentation`, `fts_sim`, `tables_sim`). The runs found three
  faults in moraine, fixed before maintenance was merged (D47, D48, D51), and one in the scheduler
  (a store call could wait longer than the grace period at its gate). T8 ran for ten minutes on
  local disk: eight processes, three of them running maintenance, 242 `kill -9` and restarts,
  118,452 versions and 1,066,945 rows, with every acknowledged append and queued batch present
  once and `verify` clean.

- **Blobs, index before commit and streaming compaction:** 10,000 seeds of each scenario, release build, ten shards each, after main's
  cancellation fix was merged: the four `blobs_sim` scenarios (0.99 to 1.27 million blob reads
  each, checked against the bytes their rows were written with; about 33,000 pass-through copies
  in each of two of them; 145,000
  staged uploads, 41,500 clients dropped mid-write, 1.39 million injected faults, and in
  `slow_writer_vs_collectors` 30,000 staged uploads committed after a pause around the commit
  deadline: none committed uncopied past it), `crash_blobs` at each of its five failpoints
  (47,700 crashes), and again every earlier scenario: `mutations_sim`, `cancel_sim`, `tables_sim`,
  `txn_sim`, `segmentation`, `fts_sim` (79,221 sidecars built while writes ran), the six
  `maintenance_sim` scenarios (now compacting by streaming), `crash_points` and `crash_everywhere`
  at all 15 failpoints. The toy counter ran 10,000 seeds. No seed failed. Getting there, the
  blob simulation found two faults in its own harness, none in moraine: a restarted client drew
  the same random file names as the one it replaced, and a crash modelled by dropping a write
  while its gate was still shut miscounted the scheduler's running clients.
- **File-level reads, deleted uploads, purges and the commit task (2026-11-18):** 10,000 seeds of
  each scenario, release build, ten shards each (twenty for `crash_everywhere`):
  `failed_uploads` (180,390 operations; 6,885 uploads deleted by the writes that made them, 5,316
  replaces lost to other writers, 2,992 compaction steps yielded), `purges_everywhere` (50,542
  purges, 41,858 data files collected, 41,617 log entries rewritten, 2,596 pins refused),
  `cancelled_writes` (248,343 writes, 85,842 of them cancelled, 52,847 cancelled writes
  committed by the handle's task), and again `mutations_sim`, `txn_sim`, the six
  `maintenance_sim` scenarios, the four `blobs_sim` scenarios and `crash_everywhere` at all 20
  failpoints, each crashing in all 10,000 seeds. No seed failed in the final runs. Getting
  there, `purges_everywhere` found a pin refused by a purge deleting the manifest of the same pin
  committed before it (nine seeds, now regression seeds, D66), `crash_everywhere` found a purge
  taking the sidecar of a file removed seconds earlier (D66), and `slow_writer_vs_collectors`
  hung on its own crash model, which assumed one gate per client (D64).

A failing seed is never fixed by weakening a check. It goes into a regression list that runs on
every commit.

## Beyond the simulation

- **Request budget** (`request_budget`): the exact request count of an append commit, a commit
  that lost its version, a group commit and snapshot refreshes, with and without object tags.
- **Interoperability** (`interop`): delta-rs reads tables moraine wrote, including rewrites and
  an added column; moraine reads a table delta-rs wrote with a checkpoint; a table with an
  unsupported feature is refused; caller transaction ids survive a checkpoint.
- **Corruption fuzzing** (`fuzz`): log entries, table properties, file statistics and stored
  objects of a small table are mutated from a seed and fed through the parser and the public API.
  A counting allocator bounds what one input may allocate. It runs with the normal tests; it
  found two panics inside the Parquet and Arrow crates on corrupt files, which moraine now guards.
- **Full-text ranking** (`fts_index`): random corpora against one tantivy index over all rows
  (T9), scenario 9 and 10 for full text, and cases F1 to F12. See D41.
- **Soak** (`soak`): a counting global allocator over a long loop of writes, searches and
  background refreshes; every cache within its limit, the heap flat after the warm-up. See D45.
- **Golden files** (`golden/`): sidecar bundles and their expected hits for each index kind, so a
  format change cannot slip through.
- **Backends** (`backends`): the same suites against local disk and an S3-compatible server.

## Running the tests

```sh
scripts/check.sh                 # format, clippy, all tests, simulations at their default seeds
scripts/check.sh --quick         # the same with 1,000 seeds

cargo test -p moraine-testkit --test mutations_sim                     # one suite
MORAINE_SIM_SEEDS=100000 cargo test --release -p moraine-testkit --test mutations_sim
MORAINE_SIM_SEED_START=50000 MORAINE_SIM_SEEDS=10000 \
  cargo test --release -p moraine-testkit --test mutations_sim         # one shard of a long run
MORAINE_SIM_SEED=1234 cargo test -p moraine-testkit --test tables_sim  # replay one seed

MORAINE_FUZZ_ITERS=20000 cargo test -p moraine-testkit --test fuzz     # longer fuzzing
MORAINE_FUZZ_SEED=7 cargo test -p moraine-testkit --test fuzz          # another fuzz seed
```

Tests that need the S3-compatible server start it with `spike/s3-server.sh start` and read its
settings from `source <(spike/s3-server.sh env)`; without them those tests are skipped.
