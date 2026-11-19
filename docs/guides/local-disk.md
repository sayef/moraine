# Local disk

A database can live in a directory on local disk. This is the quickest way to try moraine, to
run tests, and to serve a single machine that has many processes writing the same tables.

```rust
use moraine::Database;

// A relative or absolute directory path; it is created if it does not exist.
let db = Database::builder("moraine-data").open().await?;

// The same, as a URL.
let db = Database::builder("file:///var/lib/myapp/moraine").open().await?;
```

The layout on disk is the same as on S3 (see [The Delta log](../concepts/delta-log.md)), so a
table written locally can be copied to a bucket, or opened by any Delta reader pointed at
`<directory>/<table>/data`.

## How commits work on a filesystem

Commits only need put-if-absent, which object_store's local filesystem backend implements by
creating the log entry exclusively. Compare-and-swap (`If-Match` on S3) is not implemented by
that backend, so moraine wraps it: the wrapper locks the current file, compares ETags, and
renames a new file into place. Leases need compare-and-swap, and the capability probe requires
it before any write, so the wrapper is always on for local databases.

The local backend's ETag is made from the inode, modification time and size. The kernel records
times at a coarse tick and reuses inode numbers, so a later version of a file could get an
earlier version's ETag; the first version of the wrapper lost about a third of concurrent
increments to this. Each new version's modification time is now set at least one microsecond
after the replaced file's.

## Limits

- **One host.** The wrapper's locking and the exclusive create are only as good as the
  filesystem's guarantees across processes on one machine. Do not share a local database
  between machines over a network filesystem.
- **Many processes are fine.** Processes on the same host coordinate through the log exactly as
  they would on S3. The multi-writer benchmark ran 2 to 16 writer processes on local disk with
  every row stored exactly once; see [Many writers](multi-writer.md).
- **No tags.** Object tags configured with `DatabaseBuilder::object_tags` are ignored by the
  local filesystem.

## In memory, for tests

`memory://` opens a database in object_store's in-memory store. It lives as long as the
`Database` value and is visible to that process only, which makes it useful for unit tests.

```rust
let db = Database::builder("memory://").open().await?;
```

## Bring your own store

`Database::builder_for_store(store, prefix)` opens a database under a prefix of any
`Arc<dyn ObjectStore>` you already have: a store with custom client options, a wrapper that
records requests, or a test double. The test kit uses it to run every simulation on its own
in-memory store with injected faults. Writes still require the store to pass the
conditional-write probe.

```rust
use std::sync::Arc;
use object_store::memory::InMemory;

let store = Arc::new(InMemory::new());
let db = moraine::Database::builder_for_store(store, "db").open().await?;
```

## Reading request counts

`Database::store_metrics()` returns the request and byte counters of everything the handle sent
to the store, per operation kind. On local disk they show what the same workload would cost on
S3 in requests, which is a quick way to check a change against the request budget before running
it against a bucket.
