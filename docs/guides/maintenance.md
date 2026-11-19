# Maintenance

A moraine table needs no server and no maintenance to stay correct: every job on this page only
makes queries faster and storage smaller. Any client may run any job, at the same time as other
clients run it too. This guide shows how to run the jobs, what each one does, and the tools
around them: pins, leases, file replaces, queued writes, and `verify` and `repair`.

The snippets are compiled as `crates/moraine/examples/maintenance.rs`.

## One pass

`Table::maintain` runs one pass of every job, in this order:

1. **Index build**: sidecars for data files that have none (see [Sidecar indexes](../concepts/indexes.md)).
2. **Ingest**: commits [queued writes](#queued-writes) waiting in the inbox.
3. **Compaction**: merges small files into larger, sorted ones.
4. **Checkpoint and log cleanup**: a checkpoint every 20 log entries, and old entries removed.
5. **Vacuum and collection**: deletes data files, sidecars and [blobs](blobs.md#collection) no
   snapshot needs any more.

```rust
use moraine::MaintainOptions;

let report = table.maintain(&MaintainOptions::default()).await?;
if let Some(c) = &report.compaction {
    println!("compaction: {} files into {}", c.files_removed, c.files_added);
}
if let Some(l) = &report.log {
    println!("checkpoint: {:?}", l.checkpoint);
}
if let Some(v) = &report.vacuum {
    println!("vacuum: {} files, {} bytes", v.files_deleted, v.bytes_deleted);
}
println!("skipped (lease held elsewhere): {:?}", report.skipped);
```

| Option | Default | Meaning |
|---|---|---|
| `leases` | `true` | Take each job's [lease](#leases) first; skip a job another client holds |
| `lease_ttl` | 5 minutes | How long a lease lasts without renewal |
| `build_indexes` | all indexes | `None` skips the index build |
| `ingest` | `true` | Commit queued writes |
| `compaction` | `CompactionPolicy::default()` | `None` skips compaction |
| `checkpoint` | `true` | Write checkpoints and clean up the log |
| `checkpoint_interval` | the table's `delta.checkpointInterval` (20) | Log entries between checkpoints |
| `vacuum` | `true` | Vacuum and collect sidecars |
| `interval` | 60 s | Time between passes of `spawn_maintenance` |

A pass reads the store's clock once (one small object under `meta/clock/`, deleted at once), and
every expiry decision of the pass uses that server time, so a client whose own clock is wrong
by hours does no harm.

## In the background

`Table::spawn_maintenance` runs a pass every `interval` on the Tokio runtime until the returned
task is dropped or stopped, or the last handle of the table goes. A failed pass is counted and
the next one runs as planned.

```rust
let task = table.spawn_maintenance(MaintainOptions::default().interval(Duration::from_secs(60)));
// ... the application runs; `task.metrics()` counts passes and failures ...
```

`task.stop(&table).await` stops it and releases the leases the handle holds, so another
client can take over at once instead of after the leases expire.

For deployments whose processes are short-lived, the [`moraine` CLI](../cli.md) runs the same
passes from a scheduled job: `moraine s3://bucket/db maintain events --every 60`.

## Compaction

Appends and rewrites produce many small files; compaction merges them. Every file carries a
compaction level in its Delta `add.tags` (`moraine.level`): appended files are level 0, and a
merge writes level `n + 1`. A step takes the lowest level that has a run of at least `fanout`
small files next to each other in the table's file order, merges their rows sorted by the
table's cluster key and primary key, writes them as files of the next level (split at the
target file size), builds their sidecars, and swaps them in with one commit. The new files take
the place of the old ones in the file order, so rows with equal keys keep the order they were
committed in. A run only takes files with the same caller tags (those set with
[`Replace::tag`](#replacing-files)), and the merged files keep them.

| `CompactionPolicy` field | Default | Meaning |
|---|---|---|
| `fanout` | 4 | Files of one level a step merges, at least |
| `max_files` | 16 | With no such run, merge anyway once more small files than this are live |
| `min_file_age` | 60 s | Leave younger files alone (an upsert workload rewrites them anyway); 0 takes files of any age |
| `max_level` | 8 | Levels stop growing here |
| `max_input_bytes` | 256 MiB | Input (as stored) one step merges |
| `max_steps` | 8 | Steps per pass |
| `build_sidecars` | `true` | Build the merged files' sidecars before the commit |
| `streaming` | `true` | Merge by streaming over the sorted inputs, memory bounded by row groups |

A step whose input files are all sorted by the table's current sort columns (every file moraine
writes is) streams: it reads each input a row group at a time, takes rows in key order (equal
keys in file order, so the result equals a stable sort of the concatenated inputs), writes the
merged file to a local temporary file and feeds the sidecar builders as it goes, and uploads
the file when it is complete. Its memory is about one row group per input plus the builders'
budgets, whatever the input's size. Other steps, and every step with `streaming: false`, read
their input into memory and sort it. Merged `vec-tq` sidecars are gathered from the inputs'
sidecars when every input has one.

A file of three quarters of the target size or more is full and never merged. `CompactionPolicy::full()`
merges every run of two or more small files, whatever their level or age, until none is left:
the most compact layout, for a quiet table or before a benchmark.

```rust
use moraine::CompactionPolicy;

let r = table.compact(&CompactionPolicy::full()).await?;
println!("{} steps, {} bytes written", r.steps, r.bytes_written);
for f in table.snapshot().await?.files() {
    println!("{} level {} ({} rows)", f.path, f.level, f.rows.unwrap_or(0));
}
```

A compaction commit changes no row, and loses to every write that changed the same files: a
delete, an update or an upsert that removed one of its input files makes it yield, and the pass
moves on. Its files are uploaded within one commit deadline (a quarter of the grace period) of
the commit, or not committed at all, as a writer's own are.

## Checkpoints and log cleanup

Without checkpoints, opening a table reads every log entry since version 0. The log job writes
a checkpoint (a Parquet file of the table's state, in the Delta format, readable by other Delta
readers) once `checkpoint_interval` entries have accumulated since the newest one, and points
`_last_checkpoint` at it. Opening a table then reads `_last_checkpoint`, lists the log from the
checkpoint on, reads the checkpoint and the entries after it.

`Table::checkpoint` writes one at the newest version on its own.

Log cleanup removes log entries and checkpoints that are older than the grace period and that a
newer checkpoint makes unnecessary. A handle that has not looked at the log for longer than a
commit deadline lists it before reading on, so it never mistakes a cleaned-up entry for "nothing
new".

## Vacuum

Vacuum deletes a data file of the table's `data/` prefix when all of these hold:

- the current version does not reference it;
- no pin's version references it;
- no log entry younger than the grace period removed it (an older snapshot may still be read);
- the object itself is older than the grace period (a write that has not committed yet may own
  it).

Sidecar collection deletes a sidecar older than the grace period whose data file vacuum would
not keep, or whose index no longer exists, or whose spec hash is not the index's current one.
Blob collection deletes an object under `blobs/` older than the grace period that no row of a
kept data file references ([Blobs](blobs.md#collection)).
Pin manifests of removed pins and abandoned clock probes go after the grace period too.

A query whose snapshot is older than the grace period may find one of its files gone. A scan
or search through the table handle then moves to the newest version and runs once more; a
second miss, or a miss under an explicit `TableSnapshot`, is `Error::SnapshotExpired`.

`Table::vacuum` runs vacuum and sidecar collection on their own. To collect removed files before the grace period, and to erase
their values from the log, use a [purge](#purge-and-erasure).

## Pins

A pin keeps one version readable through vacuum, under a name. There is no general time
travel: once the grace period has passed, only pinned versions can be read.

```rust
let v = table.refresh().await?;
table.pin("before-cleanup", v).await?;
table
    .delete(moraine::expr::col("id").lt(moraine::expr::lit(5i64)))
    .await?;
let old = table.snapshot_at_pin("before-cleanup").await?;
println!("version {} still has {} rows", old.version(), old.num_rows());
println!("pins: {:?}", table.pins()?);
table.unpin("before-cleanup").await?;
```

A pin is a table property (`moraine.pins`) plus a manifest object, `meta/pins/<name>/<version>.json`,
that lists the version's files. Pinning a name again moves it. The version must still be
readable when it is pinned: the newest one, or an older one whose files were replaced less than
three quarters of the grace period ago, and not older than the newest [purge](#purge-and-erasure);
otherwise `Error::VersionNotAvailable`. After `unpin`,
vacuum collects the files only the pin kept, once the grace period has passed.

## Leases

Each maintenance job takes an advisory lease first, so that two clients do not both do the same
work. A lease is a sequence of generation files, `meta/leases/<job>/<generation>.json`: taking,
renewing or releasing it creates the next generation with put-if-absent, so exactly one client
wins each generation on any store, with no compare-and-swap. A lease expires its time to live
after its generation file was written, by the store's clock.

The generation is a fencing token. A commit made under a lease carries it as a Delta
transaction, and is refused with `Error::Fenced` once a newer generation of the same lease has
committed: a client that lost its lease without noticing cannot commit over the new holder's
work. Leases stay hints: every job is correct when two clients run it at once, and moraine's
tests run every scenario with leases off as well.

Applications can use leases for their own jobs, and fence their [replaces](#replacing-files)
with them:

```rust
let ttl = Duration::from_secs(60);
let Some(lease) = table.try_lease("fold", ttl).await? else {
    return Ok(()); // another client is folding
};
// ... the work, renewing with `table.renew_lease(&mut lease, ttl)` before the time to live
// runs out ...
table.release_lease(&lease).await?;
```

## Replacing files

`Table::replace_files` removes some files and adds others in one atomic commit. It conflicts
only with another commit that removed one of the same files, and then returns
`Error::FilesChanged` with nothing written; files that other writers added meanwhile stay.

```rust
let snap = table.snapshot().await?;
let small: Vec<String> = snap
    .files()
    .into_iter()
    .filter(|f| f.rows.is_some_and(|r| r < 1_000))
    .map(|f| f.path)
    .collect();
let r = Replace::new()
    .remove_all(small)
    .add_rows(vec![folded])
    .tag("kind", "folded")
    .fenced(&lease);
match table.replace_files(r).await {
    Ok(c) => println!("replaced in version {}", c.version),
    Err(Error::FilesChanged(p)) => println!("{p} changed meanwhile; plan again"),
    Err(Error::Fenced { .. }) => println!("the lease moved on; stop"),
    Err(e) => return Err(e),
}
```

`tag` sets `add.tags` on the new files; `SnapshotFile::tags` reads them back. `rows_unchanged()`
declares a rewrite for layout only: the commit records no data change, and the new files take
the place of the first removed one in the file order, as compaction's do. moraine does not check
the new rows against the removed ones; on a table that enforces its primary key, the rows must
not duplicate a key another file holds.

Rows can also come from a stream, read as the replace runs and written one target file size at
a time, so they are never all in memory. A scan of the files being replaced is the usual source
([Reading files and rows](reading-files.md)):

```rust
let kept = snap
    .scan()
    .files(old.clone())
    .filter(moraine::expr::col("k").not_eq(moraine::expr::lit("gone")))
    .stream();
table
    .replace_files(Replace::new().remove_all(old).add_stream(kept))
    .await?;
```

`add_batches` takes an iterator of batches the same way. Each file is sorted on its own, and the
files follow the stream's order. The files are uploaded before the commit, so the commit
deadline (a quarter of the grace period) runs from the first of them. Tables with blob columns
take rows in memory only.

A write that does not commit deletes what it uploaded when it is over: a replace that lost to
another commit, was fenced, or ran past its deadline, a rewrite planned again after a conflict,
and compaction that yielded. Only a write whose outcome is unknown leaves its files to vacuum.
`Table::write_metrics()` counts both.

## Purge and erasure

Vacuum keeps a removed file for the grace period, for readers of older snapshots. When you know
no reader holds a snapshot loaded before some time, a purge collects the files removed before
it at once; with `erase`, it also takes their values out of the log:

```rust
use moraine::Purge;
// Every reader has moved on past `cutoff` (server time).
let report = table
    .purge(&Purge::new().removed_before(cutoff).erase())
    .await?;
println!(
    "{} files collected, {} log entries rewritten",
    report.files_deleted, report.entries_scrubbed
);
```

`Purge::new()` alone takes the purge's own start as the cutoff. A purge:

1. takes the `purge` lease (`Error::LeaseHeld` while another client purges) and commits the
   table property `moraine.purged`, the version it follows, fenced by the lease. No older
   version can be pinned from then on;
2. deletes the data files that log entries up to that version removed before the cutoff, unless
   the current version or a pin still holds them, with their sidecars and the blobs only they
   reference. Uploads of commits in flight are never taken: a file no log entry removed goes
   only after the grace period, as with vacuum;
3. with `erase`: writes a checkpoint, rewrites every log entry up to that version without the
   minimum and maximum of the collected files (row and null counts stay, so readers only prune
   less) and without commit information that may hold values, then deletes older checkpoints
   and the manifests of pins of older versions.

It is safe alongside writers and every other maintenance job. A checkpoint pass that read the
log before the purge and writes an older checkpoint afterwards deletes it again, and log cleanup
and vacuum delete what a crashed client leaves on their next pass. A reader of a snapshot
loaded before the cutoff may find a file gone: a query through the table handle then moves to
the newest version once, as after vacuum.

A purge does not reach other processes' memory (their caches of footers and pages) or queued
writes waiting in the inbox.

## Queued writes

Many processes appending to one table contend for the next log entry. Queued writes take the
contention away from the writers: `Table::submit` puts the rows into an inbox, one
put-if-absent of one object, and an ingestor commits everything waiting as one append.

```rust
let ticket = table.submit(vec![rows]).await?;
// Any process: commits what the inbox holds, under the `ingest` lease.
let r = table.ingest(true).await?;
println!("{} batches, {} rows in version {:?}", r.batches, r.rows, r.version);
assert!(table.is_ingested(&ticket).await?);
```

Each batch is applied exactly once: the ingest commit records, per submitting handle, the
newest sequence number it committed as a Delta transaction, so a second ingestor, a retried
commit or a crash between the commit and the cleanup of the inbox never applies a batch twice.
A handle's batches are committed in the order it submitted them. The rows become visible when
an ingestor commits them: run `Table::ingest` in a loop in one or more processes (the lease
makes one of them do the work), or let maintenance passes do it.

Queued writes are appends only, and a table that enforces its primary key refuses them. They
are opt-in: plain writes do not touch the inbox.

## Verify and repair

`Table::verify` checks, against the newest version, that every data file exists with its
recorded size and that every sidecar of a live file and a current index validates.
`Table::repair` removes the table's references to missing files in one commit and reports the
rows they held.

```rust
let r = table.verify().await?;
if !r.is_ok() {
    println!("missing: {:?}", r.missing_files);
    let fixed = table.repair().await?;
    println!("removed {:?}, {} rows lost", fixed.files_removed, fixed.rows_lost);
}
```

Repair is manual. It exists for the one failure the design cannot rule out: a writer frozen
between its deadline check and its commit for longer than three quarters of the grace period,
whose files vacuum may have taken meanwhile.
