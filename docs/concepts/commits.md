# The commit protocol

moraine has no server and no lock service. Writers coordinate through one primitive: a
conditional PUT that creates `_delta_log/<version>.json` only if it does not exist. This page
describes what a write does from the moment it is called until it returns a version.

## Group commit inside a process

Every write on a table handle goes through that handle's commit queue. There is no fixed time
window and no background task:

- The first caller that finds no commit in flight becomes the **leader**. It takes the queue's
  compatible prefix (up to the target file size), commits it as one entry, and hands leadership
  to the next waiting caller.
- While one commit is in flight, new writes wait in the queue. When it returns, everything
  compatible that queued up meanwhile becomes the next commit.
- Appends from any number of callers merge into one commit: their batches are concatenated,
  sorted by the table's cluster key and primary key, and written as one data file instead of
  one small file per caller.
- Upserts merge only with upserts whose keys they do not share; deletes by key merge with deletes
  by key, and deletes by filter with deletes by filter (the filters are OR-ed). Updates and added
  columns commit alone. A mixed queue becomes consecutive commits in arrival order, so each
  caller's own writes keep their order.
- Each caller still gets its own result, and one caller's invalid batch fails only that caller.

`DatabaseBuilder::group_commit_linger` lets a leader wait a little for more writes before it
commits; the default is zero.

## The request budget

With a current snapshot, an append commit costs one PUT per data file plus one conditional PUT
of the next log entry, and nothing else: no listing, no HEAD, no read-back. Writes of other
tables in the same process are independent, so a flush that touches several tables can issue
their commits concurrently and costs about one round trip of latency.

Request counts are asserted exactly in the test suite (`tests/request_budget.rs`) for an append,
a commit that lost its version, a group commit and snapshot refreshes, so a change that adds a
request fails the build.

## One commit, step by step

```text
writer A                          object store                        writer B
--------                          ------------                        --------
PUT data/part-a.parquet  ------>  stored
                                  stored               <------  PUT data/part-b.parquet
PUT _delta_log/42.json   ------>  created, A owns 42
(If-None-Match: *)                412 Precondition     <------  PUT _delta_log/42.json
                                                       ------>  GET _delta_log/42.json
                                                                (B reads A's entry, checks
                                                                 for a conflict: none)
                                  created, B owns 43   <------  PUT _delta_log/43.json
                                                                (same bytes, next version)
```

1. **Prepare.** Validate the batches against the schema, fill missing nullable columns with
   nulls, sort by (cluster key, primary key), write the data files and compute their statistics.
   Files are not visible to anyone until a log entry names them.
2. **Put the entry.** Encode the next log entry and PUT it with put-if-absent at the version
   after the snapshot in hand.
3. **On success**, the write is committed at that version.
4. **On a lost race** (the version exists), read that entry with one GET and decide:
   if it is the writer's own entry from an earlier ambiguous attempt, the write is done;
   if it conflicts, re-execute; otherwise **rebase** by putting the same bytes at the next
   version, without backoff. Repeat until the PUT succeeds.

A writer never lists the log to find the head. It reads entries `n`, `n+1`, ... by GET until one
is missing. Listing happens only when a handle opens (from the newest checkpoint on), and when a
handle has not looked at the log for longer than a commit deadline: log cleanup may have
removed the entry it would read, so it lists first, and reloads from the checkpoint when the
listing shows a gap or entries its reads cannot reach.

## Conflicts

An append conflicts only with an entry that changes the schema or table options. An append in
flight when a column is added keeps its uploaded files (they read the new column as null),
re-encodes its entry against the new schema and rebases.

An upsert, delete or update conflicts with an entry that:

- changes metadata;
- removes a file the write also removes; or
- adds a file holding a row the write matches. This is checked exactly, by reading the added
  file's key or filter columns. File statistics and cached bloom filters answer most cases
  without a request.

Anything else rebases. Rewrites start from the newest version (one GET when nothing is new), and
re-execution is incremental: a source file already examined is not read again while it is live,
and files already rewritten or written are reused. Two concurrent upserts with random keys
therefore do not conflict just because their key ranges overlap.

## Fairness under contention

When many writers race for the same log, a writer that just committed puts its next entry in
one round trip, while a loser needs two (read the winner's entry, put again), so without care
the same writers can lose many times in a row. moraine applies a starvation guard: while a
handle's log puts are contended, a job that has lost fewer than two races per contending writer
waits half a log-put round trip before each put, and a job that has lost more puts at once. Only
writers deep in a losing streak are reordered, so the median barely moves. Nothing waits when
the round trip is under 2 ms. The measured effect is in [Many writers](../guides/multi-writer.md).

## The commit deadline

The commit clock starts when a group's write starts. The deadline is a quarter of the table's
grace period (15 minutes with the default of one hour). An attempt that finds itself past the
deadline stops; the write then starts once more with a fresh clock, fresh uploads, a fresh retry
budget and the same transaction id. A second overrun returns `Error::CommitDeadlineExceeded`, and
nothing is visible. The deadline keeps a commit from naming files that the grace period has
already allowed to be removed.

## Ambiguous outcomes

A conditional PUT can time out after the store applied it. Retrying it blindly would report the
writer's own success as "already exists". moraine never retries an ambiguous log put on its own:

- Every entry carries a Delta `txn` action for the writing handle: the application id is the
  handle's random writer id, and the version is the handle's group counter.
- After an ambiguous put, the writer looks for its own transaction id in the entries at and
  after the version it tried. If it finds it, the write committed; if it finds the version taken
  by someone else and no trace of its own id, it carries on as after a lost race.
- When a write carries a caller transaction id as well, the next attempt is safe in any case:
  the earlier entry, if it landed, is either in the snapshot or in the way. See
  [Exactly-once writes](exactly-once.md).

A group leader dropped in the middle of a commit answers its group with `Error::OutcomeUnknown`
once the log put may have been sent, and a plain failure before that. `OutcomeUnknown` is the one
error that must not be retried blindly; with a caller transaction id it is safe to resend.

## Where this is recorded

The protocol is moraine's own code on top of delta-rs's log format. The reasons are in
[DECISIONS.md](https://github.com/sayef/moraine/blob/main/DECISIONS.md), entries D18 (request
budget), D25 (the commit layer), D27 (conflict rules) and D28 (the starvation guard).
