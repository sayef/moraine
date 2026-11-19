# The `moraine` command

`moraine` is the operator tool for moraine databases: it inspects tables and runs the
[maintenance](guides/maintenance.md) jobs from a shell or a scheduled job, for deployments whose
application processes are too short-lived to run them.

The library is on crates.io; the command is not published there, so install it from the
repository:

```sh
cargo install --git https://github.com/sayef/moraine moraine-cli
```

```text
moraine [--option KEY=VALUE]... LOCATION COMMAND [ARGS]
```

`LOCATION` is a database URL (`s3://bucket/prefix`, `file:///path`) or a local directory.
Storage options (credentials, endpoint, region) come from `--option` and from the environment,
as `object_store` reads it (`AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT`, and so on). Every command
except `stress` prints what it did; errors go to standard error with exit status 1.

## Commands

| Command | What it does |
|---|---|
| `tables` | Lists the tables of the database |
| `inspect TABLE` | Version, files, rows, bytes, files per compaction level, pins and indexes |
| `maintain TABLE [--no-leases] [--every SECONDS]` | One maintenance pass, or one every `SECONDS` until stopped; prints each pass's report |
| `compact TABLE [--full]` | Compaction alone, without a lease; `--full` merges every run of small files |
| `checkpoint TABLE` | Writes a checkpoint at the newest version if none exists for it |
| `vacuum TABLE` | Vacuum and sidecar collection alone, without a lease |
| `ingest TABLE` | Commits [queued writes](guides/maintenance.md#queued-writes) from the inbox, under the `ingest` lease |
| `verify TABLE` | Checks data files and sidecars; exit status 1 when something is wrong |
| `repair TABLE` | Removes the table's references to missing data files and reports the rows lost |
| `pins TABLE` | Lists the pins, name and version |
| `pin TABLE NAME [VERSION]` | Pins a version under `NAME` (the newest by default) |
| `unpin TABLE NAME` | Removes a pin |
| `stress TABLE --seconds N --process ID [--maintain]` | One writer of the multi-process stress test (T8); see below |

## Examples

```sh
# A pass every minute, from a container that runs nothing else.
moraine s3://bucket/db maintain events --every 60

# Against an S3-compatible server.
moraine --option aws_endpoint=http://localhost:9000 --option aws_allow_http=true \
  s3://bucket/db inspect events

# Keep a version readable through vacuum, and read what is pinned.
moraine ./data pin events release-1
moraine ./data pins events
```

## The stress test

`moraine LOCATION stress TABLE --seconds N --process ID [--maintain]` is one writer process of
the multi-process stress test T8 of the [test suites](design/test-ids.md#test-suites): it appends batches with
transaction ids, deletes its own junk rows, queues batches for the ingestor and reads its rows
back, and with `--maintain` runs a maintenance pass every two seconds. Every acknowledged write
is appended to the file `MORAINE_STRESS_ACKS` names before the next one starts. The test
`crates/moraine-cli/tests/t8.rs` runs eight of them on one table with random `kill -9` and
restarts, then checks the table against the acknowledgements:

```sh
MORAINE_T8_SECONDS=600 cargo test --release -p moraine-cli --test t8 -- --ignored
```
