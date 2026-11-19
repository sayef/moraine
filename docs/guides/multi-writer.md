# Many writers

Any number of processes, on any number of machines, can write the same table at the same time.
They share nothing but the object store: no lock service, no coordinator, no leader election.
Every writer commits by creating the next log entry with a conditional PUT, and a writer that
loses the race reads the winner's entry and either rebases or re-executes
([The commit protocol](../concepts/commits.md)).

## Inside one process: share one handle

Open one `Database` per process and share it (it is cheap to clone). Writes from many tasks on
one table handle go through one group-commit queue, so concurrent appends from a hundred tasks
become a few commits instead of a hundred commits racing each other on the log.

```rust
let db = Database::builder("s3://my-bucket/prefix").open().await?;
let table = db.open_table("events").await?;

let mut tasks = Vec::new();
for batch in batches {
    let table = table.clone();
    tasks.push(tokio::spawn(async move { table.append(vec![batch]).await }));
}
// Each task gets its own result; appends that queued together share one commit.
```

A second handle on the same table in the same process works, but it competes with the first on
the log like a separate process would.

## How contention behaves

These numbers come from the [fair comparison](../benchmarks.md): separate processes appending to
one table as fast as they can, starting at one common instant. Median over repetitions, range in
brackets. Every moraine run stored every row exactly once, checked on the raw row count.

**Local disk** (five repetitions):

| Writers | Commits per second | Commit p50 | Commit p99 |
|---|---|---|---|
| 2 | 885 (721-1,882) | 1.6 ms | 15.9 ms |
| 4 | 1,772 (1,503-2,857) | 0.8 ms | 21.0 ms |
| 8 | 1,032 (961-1,268) | 6.2 ms | 29.2 ms |
| 16 | 2,577 (2,107-2,716) | 2.0 ms | 72.2 ms |

**S3-compatible server** (RustFS in Docker, three repetitions):

| Writers | Commits per second | Commit p50 | Commit p99 | Requests per commit |
|---|---|---|---|---|
| 2 | 18.4 | 84.7 ms | 425 ms | 4.9 |
| 4 | 19.7 | 136.6 ms | 653 ms | 8.3 |
| 8 | 21.5 | 107.3 ms | 1,405 ms | 13.9 |
| 16 | 20.7 | 73.0 ms | 3,485 ms | 24.7 |

What the numbers say:

- **The commit rate holds as writers grow.** It does not collapse at 16 writers, on either
  backend. In the same runs Lance and Delta Lake (through kv0) stayed at 157-254 and 68-127
  commits a second locally, and at 16 writers one of their writers gave up in some runs.
- **The tail grows with the queue.** With a flat commit rate, a commit waits about
  writers / rate on average: 0.8 s at 16 writers on S3. The p99 grows accordingly.
- **One target is missed.** At 8 writers on S3 the p99 target is at most half of Delta's p99
  (1,283 ms); moraine's is 1,405 ms (1,233-1,578). The target stands, and the miss is recorded
  rather than the bound loosened.
- **Local runs are short.** They last 0.1 to 0.4 s, so their rates vary by up to a factor of two
  between repetitions.

## Reducing contention

- **Batch writes.** One commit costs about the same whether it holds ten rows or a hundred
  thousand. Commit what a process has accumulated rather than row by row; group commit does
  this for concurrent tasks automatically.
- **Write different tables from different processes** where the data allows it. Writers only
  compete on the log of the table they write.
- **Prefer appends.** An append that loses a race rebases by putting the same bytes again. An
  upsert, delete or update may have to re-execute if a concurrent commit added rows it matches.
- **Give retries an id.** If a write can time out or a process can crash mid-write, tag writes
  with [caller transaction ids](../concepts/exactly-once.md) so a retry never applies twice.

## Keys across writers

With a primary key, `enforce_primary_key` defaults to true: plain `append` is refused
(`Error::PrimaryKeyEnforced`) and rows go in with `upsert`, so no snapshot ever holds a key
twice. Two writers that upsert the same new key at once both succeed, one after the other: the
second finds the first's row in the conflict check, re-executes, and its insert becomes an
update. A table that sets `enforce_primary_key: Some(false)` accepts appends without checking
keys; `get` then returns the row committed last.

`upsert_with(batches, DuplicatePolicy::LastWins)` accepts a batch that repeats a key and keeps
the key's last row in batch order; the default refuses it with `Error::DuplicateKeyInBatch`.

## Queued writes

When many processes append to one table, they can put their batches into an inbox instead
(`Table::submit`, one uncontended PUT each), and a lease-elected ingestor commits everything
waiting as one append, exactly once per batch. See
[Maintenance: queued writes](maintenance.md#queued-writes). Maintenance jobs take
generation-file leases, so they run in any client without duplicating work
([Leases](maintenance.md#leases)).
