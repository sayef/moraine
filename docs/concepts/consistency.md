# Consistency modes

A query reads a **snapshot**: one version of the table, with its list of live files. Writes are
always made against the newest version (the [commit protocol](commits.md) guarantees that). What
varies is how fresh the snapshot is that a *read* uses, and how much that freshness costs.

## The four modes

| Mode | Before a query | Cost per query |
|---|---|---|
| `Consistency::Strong` | Reads the next log entry to check for newer commits | One small GET (more if commits arrived) |
| `Consistency::Eventual { max_staleness }` | Refreshes only if the snapshot was last confirmed more than `max_staleness` ago | At most one GET per `max_staleness`; nothing when nobody reads |
| `Consistency::Background { interval }` | Nothing: a task refreshes the handle every `interval` and prewarms its caches | None; the task makes one GET per interval |
| `Consistency::Manual` | Nothing: only `Table::refresh` and the handle's own writes move the snapshot | None |

The default is `Eventual` with a `max_staleness` of 5 seconds. It has no timer: a query that
finds its snapshot older than the limit refreshes first, then runs. `Background` moves that work
off the query path; see [Background refresh](#background-refresh).

Set the mode for a whole database, or change it per table handle:

```rust
use std::time::Duration;
use moraine::{Consistency, Database};

let db = Database::builder("s3://bucket/prefix")
    .consistency(Consistency::Strong)
    .open()
    .await?;

let table = db.open_table("events").await?;
table.set_consistency(Consistency::Eventual {
    max_staleness: Duration::from_secs(1),
});
```

## What a refresh costs

A refresh never lists the log. It reads entries after the snapshot's version by GET, one at a
time, until one is missing, and applies only those entries to the snapshot in hand. When nothing
new has been committed, that is one GET of a file that does not exist.

A handle whose snapshot was last confirmed longer ago than a commit deadline lists the log once
after its version before probing, because log cleanup may have removed the entry it would probe.

## Measured cost

On local disk a log check is a small file read, and strong and eventual consistency are within
the noise of each other in every benchmark row. On an S3-compatible server the extra request
shows. From the [fair comparison](../benchmarks.md), median of three runs:

| Workload (S3-compatible server) | Strong | Eventual (5 s) |
|---|---|---|
| Lookup by item id (filtered scan), p50 | 0.81 ms, 1.3 requests | 0.19 ms, 0.3 requests |
| Lookup by primary key, p50 | 1.10 ms | 0.44 ms |
| Cluster key: one run-id query, p50 | 0.97 ms | 0.21 ms |
| Process-cold start-up (requests) | 1,809 | 1,408 |

Choose `Strong` when a reader must see every write acknowledged before its query started, for
example a process that reads back what another process just confirmed. Choose `Eventual` for
dashboards, search and other reads that tolerate a few seconds of lag.

## Pinned snapshots

`Table::snapshot()` returns a `TableSnapshot`: a fixed version that does not move however many
commits arrive. Every query against it sees the same rows, which is how to run several reads
that must agree with each other.

```rust
let snap = table.snapshot().await?;
let version = snap.version();
let total = snap.scan().count().await?;
let open = snap
    .scan()
    .filter(col("status").eq(lit("open")))
    .count()
    .await?;
// `total` and `open` describe the same version.
```

Files a newer version removed stay in storage for at least the table's grace period (one hour
by default): only vacuum deletes them, and it ages them by the store's clock, not the client's.
A snapshot younger than the grace period therefore always reads. An older one may find a file
gone: a query through the table handle then moves to the newest version and runs once more,
and a query on an explicit `TableSnapshot` returns `Error::SnapshotExpired`
([Maintenance: vacuum](../guides/maintenance.md#vacuum)).

## Background refresh

Under `Consistency::Background { interval }` each table handle runs a task that reads new log
entries every `interval`, so a query uses the snapshot in hand and never waits for the log. For
every new version the task also prewarms the caches for the files that version adds:

- their Parquet footers and page indexes;
- the sidecar listing at the new version, and those files' sidecars, downloaded, checked and
  opened;
- the in-memory full-text indexes of new files that have no sidecar;
- for a small table, its in-memory copy, extended with the new files.

Files the new version still holds keep their cache entries: every cache is keyed by an immutable
data file or sidecar, so nothing is fetched twice. The first query after another writer's commit
therefore makes no snapshot request, and in the tests a scan or a full-text search right after
one makes no request at all.

```rust
use std::time::Duration;

use moraine::{Consistency, Database};

let db = Database::builder(location)
    .consistency(Consistency::Background {
        interval: Duration::from_millis(500),
    })
    .open()
    .await?;
let table = db.open_table("events").await?;
let m = table.refresh_metrics();
println!(
    "{} refreshes, {} new versions",
    m.refreshes.load(std::sync::atomic::Ordering::Relaxed),
    m.new_versions.load(std::sync::atomic::Ordering::Relaxed)
);
```

If the task cannot reach the store, queries keep the snapshot they have; once it has gone
unconfirmed for four intervals and a second, a query refreshes by itself, as under `Eventual`.
The task stops when the mode of the handle changes and when the last clone of the handle is
dropped. It needs a Tokio runtime; a handle opened outside one refreshes like `Eventual`.

## Your own writes

Every write returns the version it committed at, and moves the handle's snapshot to at least
that version. A read on the same handle after a write therefore sees that write in every mode,
`Manual` included.

`Table::snapshot_in_hand()` returns that snapshot without any request, whatever the mode, so a
caller that keeps per-file bookkeeping can see what its write added without reading the log
again. `TableSnapshot::files()` lists a version's data files, also without a request:

```rust
let committed = table.append(vec![batch]).await?;
let snap = table.snapshot_in_hand();
assert!(snap.version() >= committed.version);
for f in snap.files() {
    println!("{} ({} bytes, {:?} rows, added in {:?})", f.path, f.size, f.rows, f.added_in);
}
```

Under `Strong`, `Table::snapshot()` still reads the next log entry: another writer may have
committed since.
