# Exactly-once writes

A write that times out may or may not have committed. A process that crashes after a write may
not know whether it landed. Retrying such a write is only safe if the table can tell that it has
already applied it. moraine does this with Delta's application transaction ids: the caller names
each write with an application id and a version, and a write whose version the table has already
seen for that application is not applied again.

## The API

`Table::txn(app_id, version)` returns a `TxnWrite` with the same write methods as the table:
`append`, `append_unchecked`, `upsert`, `upsert_with`, `delete`, `delete_keys` and `update`. Each
returns a `WriteOutcome`:

| Outcome | Meaning |
|---|---|
| `WriteOutcome::Committed(committed)` | The write was applied at table version `committed.version`. |
| `WriteOutcome::AlreadyApplied { version, txn_version }` | The application has already committed this version or a later one (`txn_version`); `version` is a table version that holds the earlier commit. Nothing was written. |

"Already applied" is not an error. The methods without an id are unchanged and return a
`Committed`, whose `version` is the table version that holds the write. `WriteOutcome` may gain
variants in a minor release, so a `match` on it ends with a wildcard arm.

```rust
use moraine::WriteOutcome;

// Replaying entry 7 of a write-ahead log, possibly for the second time.
match table.txn("ingest-1", 7).append(vec![batch]).await? {
    WriteOutcome::Committed(c) => println!("committed at version {}", c.version),
    WriteOutcome::AlreadyApplied { version, .. } => println!("already in version {version}"),
    other => println!("in version {}", other.version()),
}

// After a restart: where did this application get to?
let next = table.last_txn_version("ingest-1").await?.map_or(1, |v| v + 1);
```

`Table::last_txn_version(app_id)` refreshes the snapshot first (one GET when nothing is new).
`TableSnapshot::last_txn_version` answers for a pinned version without a request.

## The rule

It is Delta's rule: the newest `txn` action per application id, in log order, is the
application's version, and a write at or below it is already applied. Versions may skip numbers
but must grow. An application therefore sends its writes in order, and retries each one until it
is committed or already applied before sending the next. A version sent after a higher one is
reported as applied and never written.

## A write with an id always commits

A delete or update that matches nothing, an upsert of no rows, and `append_unchecked(vec![])`
still commit an entry that carries the id. Otherwise a replay could apply a delete to rows
written after the original found nothing to delete. `append_unchecked(vec![])` is therefore the
way to record "this application reached version n" with no data.

## Where the check happens

The check costs no request of its own. It runs where the commit protocol already reads the log:
against the snapshot a write prepares on, and against every entry the write loses a race to,
since a write commits only after reading every entry between its snapshot and its version.

- A write whose id the snapshot already holds is answered before it runs.
- A write that finds its id applied in an entry it lost to stops as superseded.
- After an ambiguous log put, the next attempt is safe whatever happened: if the earlier entry
  landed, it is either in the snapshot or in the way.

Across processes and restarts the ids come from the log itself. Every snapshot keeps the newest
version per application id, and a handle that loads from a checkpoint reads the checkpoint's
`txn` column (one more GET of the checkpoint, on that path only).

## Group commit

One log entry can hold several applications' ids, one `txn` action per caller id beside the
handle's own. Delta allows one `txn` per application id per commit, so a second write of an
application that is already in the group starts the next group, which also keeps an
application's writes in queue order.

## Retention

Delta drops a `txn` action older than `delta.setTransactionRetentionDuration` by its
`lastUpdated` field. moraine writes caller ids without `lastUpdated`, which Delta never expires,
so a replay after a long outage is still recognised. The handle's own ids carry `lastUpdated` and
expire with the grace period. The interoperability tests check through delta-rs that after a
checkpoint a caller id survives and an id with an old `lastUpdated` does not.

## How it is tested

Three simulation scenarios replay write logs under injected faults: processes that crash at a
random request and resume from `last_txn_version` or earlier, log puts that land and lose their
response followed by resends, and two processes (and two tasks of one handle) replaying the same
logs at once. With the snapshot check disabled, the concurrent scenario fails on its first seed
with an id applied twice. See [Testing and simulation](../design/testing.md).
