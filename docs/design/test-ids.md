# Test and edge-case IDs

The decision log, the benchmark log and the tests refer to invariants, edge cases, test suites
and named scenarios by short IDs: I3, C6, V7, T9, scenario 12. This page defines them. The
[testing page](testing.md) describes the tooling and the runs.

Every edge case states the behaviour that keeps the invariants true, and each needs at least one
test that names its ID.

## Invariants

The [testing page](testing.md#the-invariants) gives the exact form the invariant checker
asserts; I11 (caller transaction ids commit at most once) is defined there.

| ID | Invariant |
|---|---|
| I1 | **Durability.** An acknowledged write is visible in every snapshot at or after its version, until a later commit changes it. |
| I2 | **Atomicity.** A write is entirely visible or entirely invisible. A write that returned an error other than `OutcomeUnknown` is invisible. |
| I3 | **Serial equivalence.** The table at version N equals the result of applying commits 0 to N in order to an empty table. |
| I4 | **Key uniqueness.** With `enforce_primary_key`, no snapshot holds two rows with the same key. |
| I5 | **Monotonic reads and read-your-writes** per handle. |
| I6 | **Index transparency.** Full-text and filtered-exact results are identical with sidecars on and off. Approximate vector results meet the recall bound against exact. |
| I7 | **Collector safety.** No object reachable from a snapshot younger than the grace period is deleted, and no object a commit within its deadline will reference is deleted. |
| I8 | **Collector liveness.** An unreachable object is deleted within the grace period plus two maintenance passes. |
| I9 | **Sidecar validity.** A sidecar that passes validation was built from exactly the data file it is named after, with the current spec. |
| I10 | **No lost or duplicated rows.** Compaction and concurrent writers never change the multiset of rows beyond what commits specify. |

## Edge cases

### Storage and provider

| ID | Case | Required behaviour |
|---|---|---|
| S1 | Commit request times out but the object was created | Look up the transaction ID before any retry or re-execution. Found means success. Never double-apply. |
| S2 | Store ignores conditional writes (some S3-compatible services) | Capability probe fails; open for writing returns `ConditionalWritesUnsupported`. Read-only works. |
| S3 | Throttling (429, 503 SlowDown) | Backoff with jitter. Not counted as a conflict. `Throttled` after the operation deadline. |
| S4 | Listing has more than 1,000 keys, or fails part-way | Follow pagination. A partial listing is an error. Collectors abort; readers keep previous knowledge. |
| S5 | Multipart upload abandoned | Invisible. Document the need for a bucket rule that aborts incomplete multipart uploads. |
| S6 | Bucket versioning is on | Deletes leave old versions. Document the lifecycle rule to expire non-current versions. |
| S7 | Bucket lifecycle rule deletes by age under the table prefix | Would destroy live data. Document as forbidden; `verify` detects the damage. |
| S8 | Another Delta writer (for example Spark) writes to `data/` | Rows are read correctly. Sidecars are built lazily. Unknown table features: refuse with `UnsupportedTableFeature`. |
| S9 | Credentials expire mid-operation | Error surfaces. Nothing partial is visible. The call is retriable. |
| S10 | Partition values with spaces, slashes, Unicode, percent signs | Sidecar key uses the encoded path from the `add` action unchanged. Round-trip test over generated values. |
| S11 | Local filesystem backend | Supported for tests and single-host use. Network filesystems are unsupported and documented as such. |
| S12 | Slow or stalled download | Per-request timeout, then retry. A query respects its overall deadline. |
| S13 | Object store returns a truncated body | Length and checksum checks reject it; retry. |

### Commits and concurrency

| ID | Case | Required behaviour |
|---|---|---|
| C1 | Many writers append at once | All commit. Versions are gapless. Row count equals the sum. |
| C2 | Two writers upsert the same key at once | Both succeed in some order. One row remains, from the later commit. |
| C3 | Blind append of an existing key | Rejected when `enforce_primary_key`. Otherwise documented as producing a duplicate. |
| C4 | Delete runs while compaction rewrites the same file | One fails and yields. A deleted row never reappears. |
| C5 | Upsert runs while compaction rewrites the file holding its key | Same as C4. No duplicate, no lost update. |
| C6 | Writer stalls longer than the commit deadline before committing | Deadline check aborts and restarts with fresh uploads. |
| C7 | Writer frozen between the deadline check and the commit for most of the grace period | Commit may reference vacuumed files. `verify` reports it, `repair` removes the entries. Documented residual risk. |
| C8 | Process killed at any step | After upload, before commit: orphans, collected. After commit, before sidecar: exact route. During sidecar upload: no object. During compaction: nothing visible. |
| C9 | Conflict storm from many upserting writers | Bounded retries with jitter; `TooManyConflicts` carries the attempt count. No livelock where nobody commits: at least one writer commits each round. |
| C10 | Schema gains a column while appends with the old schema are in flight | Appends commit. The new column reads as null for those rows. |
| C11 | Two clients change the index list at once | The loser reloads and reapplies. Neither change is lost. |
| C12 | Two clients create the same table at once | One creates. The other opens and compares; a mismatch is `TableExistsWithDifferentSchema`. |
| C13 | Two clients write the same checkpoint | Same content. Harmless. |
| C14 | Reader loads the log while cleanup deletes old entries | Start from the checkpoint. On a missing entry, reload from `_last_checkpoint` or a listing. |
| C15 | Log has a gap or an unparsable entry | `Corrupt`. No automatic repair. |
| C16 | Many concurrent calls on one handle | Group commit merges compatible calls. Each caller gets its own result. Per-caller order is preserved. |
| C17 | Group commit batch fails validation for one caller | Only that caller fails. The rest commit. |
| C18 | Table prefix is deleted while clients are active | Not supported concurrently. Clients get `TableNotFound` or storage errors and never recreate the table by accident. |
| C19 | A write that changes nothing (empty batch, delete matching no rows) | No commit. Returns the current version. |
| C20 | A single write too large to finish within the commit deadline | `CommitDeadlineExceeded` with advice to split. Never commits late. |

### Data and schema

| ID | Case | Required behaviour |
|---|---|---|
| D1 | Null in a primary key column | `InvalidBatch`. |
| D2 | Duplicate key inside one batch | `DuplicateKeyInBatch` by default; last wins under `DuplicatePolicy::LastWins`. |
| D3 | Batch schema differs from the table (missing nullable column, extra column, different type, different order) | Missing nullable: filled with null. Reordered: accepted by name. Extra or retyped: `InvalidBatch`. |
| D4 | Column names differing only by case, or using a reserved prefix | `InvalidSchema` at creation. |
| D5 | Vector with the wrong dimension, NaN or infinity | `InvalidBatch`. |
| D6 | Zero vector under the cosine metric | Stored. Not indexed. Never returned by vector search. |
| D7 | Composite or non-string primary keys (integers, binary, timestamps) | Supported. Comparison is by value, bytewise for strings and binary. |
| D8 | Strings that are equal only after Unicode normalisation | Treated as different keys. Documented. |
| D9 | Very wide rows and nested types (lists, structs, maps) | Stored and returned. Not indexable. |
| D10 | Empty table | Scans and searches return nothing. Maintenance is a no-op. |
| D11 | Binary value above 16 MiB in a plain `Binary` column | `InvalidBatch` naming the `Blob` type. |
| D12 | Timestamps with time zones | Stored as UTC microseconds. |

### Vector search

| ID | Case | Required behaviour |
|---|---|---|
| V1 | `k` larger than the row count; `k` of zero | All rows by distance; an empty result. |
| V2 | Data file with no sidecar yet | Exact scan. The result set equals the indexed route up to the recall bound. |
| V3 | File below the approximate-index threshold | Always exact. The builder writes no sidecar for it. |
| V4 | Every vector in a file is null | Empty bundle. The builder does not retry. |
| V5 | Sidecar is truncated, corrupt, or has the wrong row count or source size | Treated as missing. Metric incremented. Exact route. |
| V6 | Index parameters changed | New spec hash. Old sidecars are ignored at once and collected later. |
| V7 | usearch upgrade changes the file format | Format version is part of the spec hash, so sidecars are rebuilt. A golden-file test detects an unnoticed change. |
| V8 | Filter matches very few rows | Exact distances over the matching rows. |
| V9 | Filter matches nothing in a file, or statistics exclude the file | File skipped without opening a sidecar. |
| V10 | Equal distances | Order by data file path, then row offset. |
| V11 | Query vector has the wrong dimension or a NaN | `InvalidQuery`. |
| V12 | Thousands of sidecars open at once | Open-handle limit evicts. No file descriptor or address space exhaustion. |
| V13 | Large unindexed backlog | `max_exact_scan_rows` policy applies. `index_status` shows the backlog. |
| V14 | Two builders upload a sidecar for the same file | Either object is valid. Readers holding either copy return correct results. |
| V15 | Identical vectors in several rows | All are returned as separate hits. |

### Full-text search

| ID | Case | Required behaviour |
|---|---|---|
| F1 | Empty query, or only stop words | Empty result. |
| F2 | Query syntax error; phrase query on a field without positions | `InvalidQuery` naming the problem. |
| F3 | Tokenizer not registered in this client | `TokenizerNotRegistered` at index open. |
| F4 | Null or empty text values | Indexed as empty documents so counts match the row count. |
| F5 | Scores across files | With table-wide statistics, scores match a single merged index within 1e-5. |
| F6 | Text longer than `max_indexed_chars` | Truncated for indexing. The stored value is untouched. |
| F7 | File with no sidecar | In-memory index with the same tokenizer. Same results as the sidecar route. |
| F8 | In-memory fallback exceeds its memory limit | Least recently used entries are dropped. The query still completes. |
| F9 | tantivy upgrade changes the index format | As V7. |
| F10 | Corrupt or partial bundle | Treated as missing. In-memory route. |
| F11 | Languages without whitespace word boundaries | An n-gram tokenizer option is provided and tested with CJK text. |
| F12 | Multi-field query with boosts | Boosts from the spec apply on both routes. |

### Blobs

| ID | Case | Required behaviour |
|---|---|---|
| B1 | Blob uploaded, commit fails | Orphan. Collected after the grace period. |
| B2 | Collector runs while a slow writer holds uncommitted blobs | The age test plus the commit deadline keep them. |
| B3 | Caller re-inserts a reference from a row deleted earlier | Server-side copy to a new key. The old object's collection cannot break the new row. |
| B4 | Referenced object is missing | `BlobNotFound` for that value. The row is still readable. `verify` reports it. |
| B5 | Zero-length blob; null blob | Stored inline with size 0; stored as null. Distinct on read. |
| B6 | Blob larger than one upload part, up to the store's object limit | Streaming multipart upload with bounded memory. |
| B7 | Upload fails part-way | Nothing visible. The write fails or retries the upload. |
| B8 | Compaction or an unrelated update rewrites the row | The reference is carried unchanged. No copy. |
| B9 | Row deleted or its blob replaced | Old object collected after the grace period. |
| B10 | Checksum mismatch on read | Error for that value. Never returns wrong bytes. |
| B11 | Collector listing or mark scan incomplete, or the pass overruns half the grace period | Abort without further deletes. |

### Cache

| ID | Case | Required behaviour |
|---|---|---|
| K1 | Two threads or processes download the same sidecar | Both succeed. One installed copy. |
| K2 | Crash during download | Temporary directory removed at next start. No entry without `.ok` is ever used. |
| K3 | Disk full; cache smaller than one sidecar | Memory fallback or exact route. The query succeeds. |
| K4 | Cache directory removed while running | Recreated. Entries downloaded again. |
| K5 | Cached file corrupted on disk | Detected at open, deleted, downloaded once more. |
| K6 | Eviction while an entry is in use | Pinned entries are skipped. |
| K7 | Same key with a new ETag (duplicate build) | A separate entry. The old one ages out. |
| K8 | Read-only filesystem | `CacheMode::Memory` works. Disk mode fails at open with a clear error. |

### Collectors and time

| ID | Case | Required behaviour |
|---|---|---|
| G1 | Client clock wrong by hours | No effect. Expiry uses server time. |
| G2 | Query pinned to a snapshot older than the grace period | One transparent restart, then `SnapshotExpired`. |
| G3 | Two clients collect at once | Idempotent. "Not found" on delete is ignored. |
| G4 | Collector crashes part-way | The next pass continues. No state to recover. |
| G5 | Sidecar belongs to an index spec created after the collector's snapshot | Kept by the age test. |
| G6 | Configuration violates the grace-period inequality | Rejected at open. |
| G7 | Clients opened with different grace periods | The value in the table properties wins. A handle option that disagrees is an error. |

### Compaction

| ID | Case | Required behaviour |
|---|---|---|
| P1 | Compaction during steady writes | Row multiset unchanged (I10). |
| P2 | Constant upserts keep rewriting small files | `min_file_age` selection avoids them. Compaction eventually commits or reports that it yielded. |
| P3 | Merged file has no sidecars yet | With index-before-commit, sidecars exist before the commit. Otherwise the exact route serves the file until they are built. |
| P4 | Partitioned table | Files are merged only within one partition. |
| P5 | Two compactors | The loser aborts. No duplicate rows. |

### Scale

| ID | Case | Required behaviour |
|---|---|---|
| L1 | Snapshot with 10,000 data files | Loads from a checkpoint. Query fan-out stays bounded. A metric warns that compaction is behind. |
| L2 | 100,000 commits | Log stays bounded by checkpoints and cleanup. |
| L3 | One table with 1,000,000,000 rows | Out of the tested range for now. The documentation states the tested limit once it is measured. |
| L4 | Hundreds of tables in one database | Handles are independent. No global listing on the hot path. |

### Security

| ID | Case | Required behaviour |
|---|---|---|
| X1 | Table or index name with path characters | Rejected by the name pattern. |
| X2 | Filter built from untrusted input | Filters are expression trees. A string form is parsed by DataFusion, never concatenated into SQL by moraine. |
| X3 | Malicious or malformed bundle | The parser is bounds-checked and fuzzed. No panic, no out-of-bounds read, no unbounded allocation. |
| X4 | Secrets in logs | Storage options are redacted in `Debug` output and traces. |

## Test suites

| Suite | Scope | Runs on | Covers |
|---|---|---|---|
| T1 Unit | Bundle read and write, spec hash stability, top-k merge, rank fusion, backoff, name validation, config validation, key encoding, bitmap filters | Every commit | S10, D1 to D12, G6, G7, X1, X4 |
| T2 Property | With `proptest`: random operation sequences from one client against the model; bundle round-trip for arbitrary sections; merge of per-file top-k equals global sort; exact vector route equals a naive loop | Every commit | I3, I4, I6, V1, V10, V15, C19 |
| T3 Simulation | 2 to 16 clients, random mix of append, upsert, delete, update, index changes, search, maintenance; in-memory store; 1,000 seeds per CI run, 100,000 nightly | Every commit and nightly | I1 to I10, C1 to C5, C9 to C13, C16, C17, P1, P2, P5, G3, V14 |
| T4 Fault injection | T3 with `ChaosStore` rules and failpoints enabled; each failpoint also has a targeted test that crashes there and reopens | Every commit and nightly | S1, S3, S4, S9, S12, S13, C6, C8, C14, C20, B1, B2, B7, B11, G4, K2 |
| T5 Time | Tests that move `SimClock`: readers pinned past the grace period, collectors with skewed clients, slow writers, lease expiry and takeover | Every commit | C6, C7, G1, G2, G5, B2, B3, B9 |
| T6 Emulators | The public API scenario suite against MinIO, fake-gcs-server and Azurite through `testcontainers`, plus the local filesystem | Every pull request | S2, S11, C12, capability probe |
| T7 Real clouds | Same scenario suite plus an 8-process append and upsert run on real S3, GCS and Azure Blob buckets | Nightly | S1 to S6, spike item 1 kept as a regression test |
| T8 Multi-process stress | 8 to 32 `moraine stress` processes for 10 minutes: random operations, random `kill -9`, maintenance in three of them; then full invariant check and `verify` | Nightly, against MinIO and one real cloud | I1 to I10, C8, K1, G3 |
| T9 Search quality | Vector: recall at 10 against exact on 100,000 vectors, uniform and clustered, 1 file and 50 files, with and without filters. Full text: top 20 and scores equal to one merged tantivy index over the same rows | Every pull request (small), nightly (full) | I6, V2 to V9, F1 to F12 |
| T10 Cache | Concurrent downloads from threads and processes, eviction under load, corruption, disk-full through a small tmpfs, read-only directory | Every pull request | K1 to K8, V12 |
| T11 Interoperability | Rows written by moraine read with a second Delta reader (delta-rs Python or DuckDB) and compared. A table with an unsupported feature is refused | Every pull request | S8 |
| T12 Format stability | Golden bundles and a golden table checked into the repository. Each release must read them or reject them with the documented error | Every commit | V7, F9, I9 |
| T13 Fuzzing | `cargo-fuzz` targets: bundle parser, index spec JSON, filter string parser, lease and property JSON | Nightly, 10 minutes per target | X2, X3, V5, F10 |
| T14 Benchmarks | See below | Weekly and before releases | L1 to L4 |

## Named scenarios

Each runs under many simulation seeds, with and without injected faults, and the tests refer to
them by name or number.

1. `no_lost_appends`: N writers, M appends each; final count is N times M times batch size.
2. `upsert_same_key_race`: two writers upsert one new key in a loop; exactly one row per key at every version.
3. `delete_never_resurrects`: deletes race with compaction and with upserts of other keys; a deleted key is absent from every later snapshot.
4. `ambiguous_commit_is_idempotent`: every commit put fails after taking effect; row counts still match the model.
5. `crash_everywhere`: for each failpoint, crash there, reopen with a new client, run maintenance, check invariants.
6. `reader_survives_vacuum`: a query starts, the clock passes the grace period, vacuum runs, the query restarts once and succeeds.
7. `slow_writer_vs_collectors`: a writer pauses between blob upload and commit for just under, and just over, the commit deadline.
8. `blob_reference_reuse`: delete a row, wait past the grace period with collection running, re-insert its old reference; the new row's blob is readable.
9. `index_transparency`: same queries with `CacheMode::Disabled`, with all sidecars built, and with a random half deleted from the store; results identical for full text, within the recall bound for vectors.
10. `corrupt_sidecar_is_ignored`: flip bytes, truncate, swap sidecars between files, change the header row count; queries stay correct.
11. `index_change_during_writes`: create, drop and recreate indexes with different parameters while writers and builders run.
12. `maintenance_everywhere`: every client runs every maintenance job on every step with leases disabled.
13. `collector_never_deletes_live`: after every collector pass, every object reachable from every recent snapshot exists.
14. `collector_eventually_cleans`: after writes stop and two passes beyond the grace period, the listing equals exactly the reachable set.
15. `schema_add_column_race`: add a column while appends with the old schema are in flight.
16. `create_table_race`: identical and conflicting definitions.
17. `store_without_conditional_put`: `ChaosStore` ignores conditions; open for writing is refused.
18. `partial_listing`: listings are cut short at random; no collector deletes anything it should not, and readers never drop known sidecars.
19. `probe_under_faults`: faults during open and the capability probe, with probe writes resent and their responses lost; an honouring store is never refused, an ignoring or rejecting one always is (D92).
