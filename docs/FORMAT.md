# On-disk format

Everything moraine writes to a bucket, what version number guards it, which changes are
breaking, and what a release must do with files an earlier release wrote. The rules for when
these versions may change are in the [versioning policy](SEMVER.md).

## Layout

A `Database` is a prefix of an object store. Each table owns one prefix with three siblings;
only `data/` is a Delta table.

```text
<database prefix>/
  _meta/                                      capability probes, deleted at once
  <table>/
    data/                                     the Delta table root
      _delta_log/<20-digit version>.json      log entries
      _delta_log/<20-digit version>.checkpoint.parquet
      _delta_log/_last_checkpoint
      part-<uuid>.parquet                     row data, never overwritten
    idx/
      <data file path>/<index>.<spec hash>.osix   sidecar bundles
    meta/
      leases/<job>/<20-digit generation>.json      advisory leases (D50)
      pins/<name>/<version>.json                   pin manifests (D49)
      inbox/<submitter>/<20-digit sequence>.arrow  queued writes (D51)
      inbox-rejected/<submitter>/<sequence>.arrow  queued writes that could not be committed
      clock/<random>                               server-time probes, deleted at once
```

Nothing under `idx/` or `meta/` is needed to read rows correctly, with two exceptions that hold
only while they exist: a pinned version is read from its manifest, and a queued write is not in
the table until it is ingested.

## Versions

| What | Version | Recorded in | A reader that does not know it |
|---|---|---|---|
| Table format | 1 | Table property `moraine.formatVersion` | Refuses the table: `Error::UnsupportedTableFeature` |
| Delta protocol | reader 1, writer 2 | `protocol` action | Refuses the table: `Error::UnsupportedTableFeature` |
| Bundle format | 1 | Bytes 4–5 of a sidecar | Treats the sidecar as missing: `BundleError::Unsupported` |
| Bundle flags | 0 | Bytes 6–7 of a sidecar | Treats the sidecar as missing: `BundleError::Unsupported` |
| Index kind format | 1 per built-in kind | Bytes 52–55 of a sidecar | Treats the sidecar as missing: `BundleError::Unsupported` |
| Index library format | tantivy's and usearch's | Inside the spec hash | Never finds the sidecar (another name) |

A sidecar that is treated as missing costs speed, never correctness: its file takes the exact
route until maintenance builds a sidecar this release can read.

## The Delta table

moraine writes a standard Delta Lake table at reader version 1 and writer version 2, so any
Delta reader can read the rows.

- **Reads:** reader version 1, or 3 with no reader features; writer version 2 or lower, or 7
  with no writer feature other than `appendOnly`. Anything else is
  `Error::UnsupportedTableFeature`, including deletion vectors and column mapping.
- **Log entries** are newline-separated JSON actions: `protocol`, `metaData`, `txn`, `add`,
  `remove` and `commitInfo`. moraine ignores actions it does not know (`cdc`,
  `domainMetadata`, newer ones); a table feature that matters shows up in the protocol.
- **`metaData.configuration`** holds moraine's table definition:

  | Property | Value |
  |---|---|
  | `moraine.formatVersion` | `1` |
  | `moraine.primaryKey` | JSON list of column names, absent for a keyless table |
  | `moraine.enforcePrimaryKey` | `true` or `false` |
  | `moraine.clusterBy` | JSON list of column names |
  | `moraine.indexes` | JSON list of index specs |
  | `moraine.pins` | JSON object, pin name to version, absent with no pins |
  | `moraine.gracePeriodSeconds`, `moraine.pageRowLimit`, `moraine.rowGroupRows`, `moraine.targetFileBytes` | decimal integers |
  | `delta.checkpointInterval` | `20` |
  | `delta.logRetentionDuration`, `delta.deletedFileRetentionDuration` | the grace period |

  A missing moraine property takes its default, so a table created by an older release opens
  with the defaults of the release that created it. A property that does not parse is
  `Error::Corrupt`.
- **`add.tags`**: `moraine.level` (how often the file's rows were merged by compaction, D48)
  and `moraine.sortedBy` (the sort the file was written in). Unknown tags are kept and ignored.
- **`txn` actions** carry exactly-once ids. Callers' ids (D32), lease fences
  (`moraine.lease.<job>`, D50) and queued-write submitters (`moraine.inbox.<submitter>`, D51)
  have no `lastUpdated` and never expire; moraine's own group-commit ids have one and expire
  from checkpoints after the grace period (D14, D47).
- **Checkpoints** are classic single-file Parquet checkpoints in the Delta checkpoint schema
  (`txn`, `add`, `remove`, `metaData`, `protocol` columns), without `remove` tombstones (D47).
  `_last_checkpoint` is JSON with `version`, `size`, `sizeInBytes` and `numOfAddFiles`. A
  checkpoint moraine's reader cannot decode (another writer's multi-part or V2 checkpoint) is
  read through delta-rs; a missing or unreadable `_last_checkpoint` costs a full listing of the
  log, never correctness.
- **Data files** are Parquet with the page index on, Zstandard level 3 except for vector
  columns, and bloom filters on key columns. Their layout settings are performance, not format:
  a reader must accept any valid Parquet file the log lists.

## Sidecar bundles (`.osix`)

One container wraps every index kind (D33). All integers are little-endian.

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | Magic `OSIX` |
| 4 | 2 | Bundle format version (1) |
| 6 | 2 | Flags (0; readers reject unknown bits) |
| 8 | 8 | Kind name, ASCII, NUL-padded |
| 16 | 8 | Spec hash (16 hex characters as 8 bytes) |
| 24 | 8 | Row count of the source data file |
| 32 | 8 | Size in bytes of the source data file |
| 40 | 8 | Indexed row count |
| 48 | 4 | Section count |
| 52 | 4 | Kind format version |
| 56 | 8 | Reserved (0) |
| 64 | … | Sections, each at a multiple of 4,096 bytes |
| T | 64 × n | Section table: name (32 bytes, NUL-padded), offset, length, XXH3-64, reserved |
| end − 24 | 24 | Footer: table offset, XXH3-64 of header and table, reserved (4 bytes), `XISO` |

The **spec hash** is the first 16 hex characters of SHA-256 over the canonical JSON of the
index's kind, columns and parameters (not its policy or search defaults), the bundle format
version, the kind's format version and its library's fingerprint (`spec_hash` in
`moraine::index`). A change to any of them gives the sidecar another name, so old and new sidecars never
meet: readers look only for the name they compute.

What a kind keeps in its sections is that kind's format, guarded by its kind format version:

| Kind | Sections |
|---|---|
| `key` (`_moraine_key`) | Sorted encoded keys with their row offsets, in pages, and the page directory (D19) |
| `vec-hnsw` | One usearch graph file per part (D37) |
| `vec-tq` | TurboQuant codes and rerank vectors per part (turbovec) |
| `fts` | One tantivy segment, one section per segment file (D40) |

## Objects under `meta/`

| Object | Format | Read by |
|---|---|---|
| `leases/<job>/<generation>.json` | JSON `{"holder": string, "ttl_ms": integer, "released": bool}`; the generation is the 20-digit file name; it expires `ttl_ms` after its object's modification time (server time) | Every client taking the lease |
| `pins/<name>/<version>.json` | A log entry: `commitInfo`, `metaData` and one `add` per file of the pinned version | `Table::snapshot_at_pin`, vacuum, sidecar collection |
| `inbox/<submitter>/<sequence>.arrow` | One Arrow IPC stream holding the submitted batches | The ingestor |
| `inbox-rejected/...` | The same bytes, moved aside when they could not be committed | People |
| `clock/<random>` | Empty; its modification time is server time | The client that wrote it |

Unknown fields in a lease record are ignored; a lease or pin manifest that does not parse is
`Error::Corrupt`.

## Which changes are breaking

A change is **breaking** when a release that does not know it could misread the table or lose
data. Breaking changes bump `moraine.formatVersion` (or, for sidecars, the bundle or kind format
version) so that older releases refuse instead of misreading:

- a new meaning for an existing property, tag, `txn` id convention or object under `meta/`;
- anything a reader must understand to return the right rows, such as deletion vectors,
  partitioning or a new action kind that changes the file list;
- a new layout of a kind's sections, or of the bundle container;
- a Delta protocol version or feature that moraine starts to write.

A change is **additive** when older releases ignore it safely, and needs no version bump:

- a new table property, file tag or `commitInfo` field moraine reads only when present;
- a new index kind (older releases report `Error::IndexKindNotRegistered` for its queries and
  scan the table as before);
- a new object kind under `meta/` that only maintenance reads;
- new optional fields in a lease record or `_last_checkpoint`.

## What a release must do

Every release must **read every file an earlier release of the same format version wrote, or
reject it with the documented error** in the table above. The golden files in
`crates/moraine-testkit/golden/` hold one of each kind, written by a past release and checked
in:

| Golden | Holds | Test |
|---|---|---|
| `*.osix`, `*.hits.json`, `vectors.parquet`, `text.parquet` | A bundle of every kind and the hits it gave | `tests/golden.rs` |
| `table/` and `table.expected.json` | A database of two tables with every object kind above: log entries with every action kind, a checkpoint and `_last_checkpoint`, data files, a sidecar, a pin manifest, a lease generation and a queued write | `tests/golden_table.rs` |

The tests also change copies of them (a newer bundle or kind format version, an unknown flag, a
newer `moraine.formatVersion`, an unsupported Delta protocol) and check that each is refused
with the documented error. They run on every commit.

When a format changes on purpose:

1. Bump the version as above.
2. Keep the previous golden files under a versioned name, and assert that the new release reads
   them (or, for sidecars, refuses them with `BundleError::Unsupported`).
3. Regenerate with `cargo run -p moraine-testkit --bin golden`, review the diff, and record the
   change in `DECISIONS.md` and the changelog.

A golden file is never regenerated just to make a test pass.
