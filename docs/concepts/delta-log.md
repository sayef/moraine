# The Delta log

A moraine table is a standard Delta Lake table. The rows live in Parquet files, the table's
history lives in the Delta transaction log next to them, and the log is the only place where
writers coordinate. Nothing else is needed to read the rows: Spark, DuckDB, delta-rs or any
other Delta reader can open the table root directly.

## What a database looks like on storage

A `Database` is a prefix of an object store. Each table owns one directory under it, and only
the `data/` directory inside it is a Delta table.

```text
<database prefix>/
  _meta/                                   short-lived probe objects (capability checks)
  <table>/
    data/                                  the Delta table root
      _delta_log/00000000000000000041.json
      _delta_log/00000000000000000042.json
      part-<uuid>.parquet                  immutable row data, sorted by the table's keys
    idx/
      <data file path>/<index name>.<spec hash>.osix
```

- **Table names** match `[a-z0-9][a-z0-9_-]{0,63}`; anything else is refused, which also rules
  out path traversal.
- **Data files are never overwritten.** Each has a unique name, so anything cached about a file
  (its footer, its page index, its sidecar) stays valid for as long as the file exists.
- **Sidecars sit outside the Delta root.** `idx/` holds one bundle per data file and index; a
  Delta vacuum, whatever its rules, never touches it. Nothing under `idx/` is needed to read rows
  correctly. See [Sidecar indexes](indexes.md).

## Log entries

Every commit is one JSON file, `_delta_log/<version>.json`, with the version zero-padded to 20
digits. moraine encodes the entry itself and writes it with a single conditional PUT that only
succeeds if the file does not exist yet (`If-None-Match: *` on S3). Whoever creates
`<version>.json` first owns that version; everyone else learns about it by reading it. The
[commit protocol](commits.md) describes what a writer does when it loses that race.

An entry holds the usual Delta actions:

| Action | What moraine puts in it |
|---|---|
| `add` | A new data file with its size, row count and statistics (min, max and null counts), computed by moraine, plus `tags["moraine.sortedBy"]` on files moraine wrote sorted. |
| `remove` | A file replaced by an upsert, delete or update (copy-on-write). |
| `metaData` | The schema and table properties, at creation and when a column is added or an index changes. |
| `txn` | The writer handle's own transaction id, and any caller transaction ids ([exactly-once writes](exactly-once.md)). |
| `commitInfo` | The operation, for readers that display history. |

delta-rs supplies the action types, their serialisation, schema conversion and checkpoint
loading. It is confined to one module of the library (`log/delta.rs`); no delta-rs type appears
in the public API.

## Table properties

Table options are stored as Delta table properties at creation, so every client of the table
reads the same values:

| Property | Meaning |
|---|---|
| `moraine.formatVersion` | moraine's table format version |
| `moraine.primaryKey` | JSON list of primary-key columns, absent for a keyless table |
| `moraine.enforcePrimaryKey` | whether plain `append` is refused so no snapshot holds a key twice |
| `moraine.clusterBy` | JSON list of cluster-key columns |
| `moraine.indexes` | JSON list of index specs |
| `moraine.gracePeriodSeconds` | how long removed files and old log entries are kept (default 3,600) |
| `moraine.pageRowLimit`, `moraine.rowGroupRows`, `moraine.targetFileBytes` | Parquet layout of new data files |

Tables also set `delta.enableDeletionVectors` to false, `delta.checkpointInterval`, and the
retention durations (`delta.logRetentionDuration`, `delta.deletedFileRetentionDuration`,
`delta.setTransactionRetentionDuration`) to the grace period.

## Delta features moraine uses and refuses

Updates and deletes are copy-on-write: affected files are rewritten and the old ones removed.
Deletion vectors are not used. When moraine opens a table it checks the protocol action and
refuses (`Error::UnsupportedTableFeature`) any table that needs a reader or writer feature it
does not implement. It accepts reader version 1 (or 3 with no reader features) and writer
version 2 or lower (or 7 with only `appendOnly`).

Tables written by other Delta writers can be read. Their file statistics are trusted less:
string maxima of 32 characters or more are ignored and timestamp maxima are widened by a
millisecond, because those writers may truncate strings without capping them. Their files are
not sorted, which the key index handles with a different section (see
[Clustering and pruning](clustering.md)).

## Partitioning

Partitioned tables are not supported yet: `partition_by` must be empty, or table creation fails
with `InvalidSchema`.

## Checkpoints and log cleanup

The write path never writes checkpoints or deletes old log entries: delta-rs's post-commit hook
is switched off for moraine commits, so a commit costs no listing and no extra request.
Maintenance writes them ([Maintenance](../guides/maintenance.md#checkpoints-and-log-cleanup)): a
classic single-file checkpoint of the version in hand once enough entries have accumulated,
`_last_checkpoint` pointing at it, and log cleanup on the store's clock. Other Delta readers read
these checkpoints; moraine keeps its own file tags (sort columns, compaction levels) in them.

A handle opens a table by reading `_last_checkpoint`, listing the log from that checkpoint on,
and reading the checkpoint and the entries after it. Checkpoints moraine cannot decode itself
(another writer's multi-part or V2 checkpoints) are read through delta-rs, with the checkpoint's
`txn` column read separately, so caller transaction ids survive checkpoints.
