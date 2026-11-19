# Design decisions

moraine started from a written design, and every decision since, including every departure from
that design, is recorded with its reason in
[DECISIONS.md](https://github.com/sayef/moraine/blob/main/DECISIONS.md). This page summarises
the entries that shape the library most; each heading links to the full entry.

Before writing any library code I ran a verification spike: twelve throwaway programs that
checked the assumptions the design rests on against delta-rs, object_store, usearch and tantivy.
[docs/SPIKE.md](https://github.com/sayef/moraine/blob/main/docs/SPIKE.md) is the record of how I
validated the approach, with the numbers.

## Name and licence

[D1](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d1-name-moraine-not-osdb): the design's
working name `osdb` became moraine. The bare `moraine` crate name was taken on crates.io, so the
package is `morainedb` and the library is `moraine`.
[D2](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d2-licence-apache-20-or-mit): I license
my Rust projects under Apache-2.0 or MIT, and moraine follows.

## Commits: delta-rs's format, moraine's protocol

[D8](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d8-commits-delta-rss-default-log-store-no-dynamodb-no-custom-logstore-spike-item-1):
the spike showed that delta-rs 1.x commits to S3 with a plain put-if-absent and no DynamoDB lock,
so no lock service was needed at all.

[D18](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d18-group-commit-is-the-default-and-a-commit-makes-no-request-it-does-not-need):
I made group commit the default and set a request budget: an append costs one PUT per data file
and one conditional PUT of the log entry, with no listing, HEAD or read-back. Checkpoints and log
cleanup moved out of the write path into maintenance.

[D25](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d25-tables-append-and-read-details-that-differ-from-the-original-design):
delta-rs's own commit path lists the log and runs post-commit work on every commit, so I wrote
the commit protocol in moraine and kept delta-rs for what it does well: the action types, their
serialisation, schema conversion and checkpoint loading. It stays confined to one module, which
is what makes the budget enforceable and exactly testable.

## Performance as a requirement

[D17](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d17-performance-is-a-requirement-moraine-must-beat-lance-and-a-delta-engine):
kv0, another project of mine, had measured its delta-rs engine against its Lance engine and found
Delta 70 to 86 % slower at logging and slower at start-up and key lookups. A storage engine
slower than both obvious alternatives on the paths users touch most has no reason to exist, so I
made performance a design requirement instead of a late tuning task.
[D21](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d21-performance-targets-are-acceptance-criteria)
turns that into targets that are acceptance criteria: a feature with a target is not done
until the benchmarks show it met, or an entry explains the miss.

## Keys, sorted files and caches

[D19](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d19-point-lookups-and-cluster-key-lookups-are-index-driven):
the original design had no scalar indexes. I added a declared cluster key, files sorted by (cluster key,
primary key), file key ranges held in memory, and a key-index sidecar, so a point lookup costs
at most one range read per matching file.
[D20](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d20-per-version-caches-for-snapshots-footers-key-indexes-and-small-tables):
because data files are immutable, footers, page indexes and key indexes are cached by (path,
size) and never invalidated, snapshots are refreshed incrementally, and small tables are held in
memory per version.

## Contention, and a target I replaced

[D26](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d26-the-first-table-benchmark-results-and-the-two-contention-targets-not-met)
records two contention targets missed in the first table benchmarks, and a jittered pause I tried and reverted
because it lowered the median by starving some writers.
[D28](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d28-a-starvation-guard-for-lost-log-races-and-the-multi-writer-rows-re-measured)
adds the starvation guard that met the S3 targets at the time. On local disk I dropped the commit
p50 target and replaced it with throughput, a p99 bound and every row exactly once, because the
median of a fairly served queue is set by the number of writers, not by how fast the log is.
[D31](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d31-the-fair-comparison-what-changed-in-the-numbers-and-the-early-leases-exception-not-triggered)
records the fair comparison, including where moraine loses, and the S3 p99 target that is now
missed; I kept the target rather than loosening it.

## Hardening before indexes

[D29](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d29-hardening-before-indexes-and-where-the-other-adopted-techniques-go):
before sidecar indexes added a parser and many more objects, I put in deterministic corruption fuzz
tests that run with the normal tests, a segmentation-invariance simulation, not-found errors
that suggest the closest name, AWS SDK credentials and region resolution, and object tags.

## Exactly-once writes

[D32](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d32-caller-transaction-ids-for-exactly-once-writes-pulled-forward):
kv0's write-ahead log has to be replayed exactly once after a crash or an unknown outcome, so I
pulled caller transaction ids forward from the maintenance work. I chose a `txn(app_id, version)` builder
that returns a `WriteOutcome` over an extra argument on every method, because the id is per
write and the result type changes with it. See [Exactly-once writes](../concepts/exactly-once.md).

## Indexes

[D33](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d33-sidecar-indexes-details-that-differ-from-the-original-design-bundles-specs-and-kinds):
the bundle header names the kind with 8 ASCII bytes so kinds defined outside the crate need no
central registry; vector columns are `List<Float32>` because Delta has no fixed-size list; specs
resolve every default at creation so a stored spec never changes meaning.
[D34](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d34-turboquant-and-xorf-checked-against-their-sources)
checks the TurboQuant and xor-filter facts against their sources before code relied on them.
[D35](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d35-memory-bounded-builds-windows-of-input-measured-estimates)
bounds index-build memory with windows of input and estimates taken from measurement.
[D36](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d36-the-key-index-order-preserving-key-bytes-pages-only-for-key-sorted-files)
encodes keys so byte order equals value order, and uses the page section only for files sorted
by the primary key alone.
[D37](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d37-the-d24-benchmark-usearch-stays-the-default-vector-defaults-the-d21-vector-miss)
records the vector benchmark: usearch stays the default because TurboQuant, though far faster to
build and smaller, scans every code and is 3 to 6 times slower at a million rows. It also settles
the defaults that
[D16](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d16-vector-search-defaults-are-provisional-spike-item-10)
left open (`expansion_search` 128, TurboQuant `rerank` 8) and keeps the D21 vector target, met at
100,000 rows and missed at a million, with the fix planned alongside compaction. See
[Benchmarks](../benchmarks.md#vector-search).

## Ambiguous commits and follow-ups

[D38](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d38-a-commit-error-that-does-not-say-not-written-is-ambiguous-equal-keys-keep-their-order):
a commit error that does not say the write was refused is now treated as ambiguous and resolved
through the transaction id, like a timeout; and rows with equal sort keys keep their append
order, which is now a documented guarantee.
[D39](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d39-follow-ups-from-porting-kv0-pins-file-lists-part-parallel-search-checkpoints-list-items)
places the follow-ups from porting kv0: named pins instead of general time travel,
a public file list on snapshots, concurrent search of a file's graph parts, checkpoints, and
Arrow's default list item name.

## Maintenance

[D47](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d47-checkpoints-written-and-read-by-moraine-at-the-version-in-hand):
moraine writes and reads its own Delta checkpoints, at the version in hand, keeping its file tags
in them; opening a table reads `_last_checkpoint` and lists the log from there.
[D48](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d48-compaction-on-moraines-commit-path-tiered-with-sidecars-before-the-commit):
compaction runs on moraine's own commit path as a file swap, merges adjacent runs of one level
sorted by cluster and primary key, builds the merged files' sidecars before the commit, and
keeps the file order so equal keys keep their commit order; the same swap is public as
`Table::replace_files`.
[D49](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d49-pins-a-table-property-and-a-manifest):
named pins are a table property plus a manifest of the pinned version's files.
[D50](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d50-vacuum-collectors-and-leases-on-server-time):
vacuum and the collectors decide by the store's clock and by the log, never by a listing alone;
leases are generation files fenced through Delta transactions.
[D51](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d51-queued-writes-an-inbox-and-a-lease-elected-ingestor):
queued writes go through an inbox and a lease-elected ingestor, exactly once per batch.
[D52](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d52-searching-the-parts-of-a-vector-file-at-once-d39)
and [D53](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d53-the-open-full-text-items-of-d46)
close the search items the vector and full-text work left open. The simulation found three
faults on the way, each fixed before maintenance was merged: a stale handle could commit at a version log
cleanup had freed, compaction could commit files vacuum had already taken as orphans, and an
ingest could commit a queued batch twice.

## Blobs and index before commit

[D57](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d57-blobs-details-that-differ-from-the-original-design):
a blob column is a struct of Delta types; keys spread over prefixes by their random bits; a
write copies every reference it did not upload, and a value streamed up ahead of a write is
committed as it is only through the same handle within the commit deadline; collection marks
every file vacuum keeps, not just the current snapshot, so readers of older and pinned versions
keep their blobs.
[D58](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d58-index-before-commit): every
write builds its files' sidecars before the commit, within size and cost limits each index's
policy sets.
[D59](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d59-streaming-compaction-d55) and
[D60](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d60-turbovec-codes-merged-on-compaction-d55):
compaction streams a k-way merge over its sorted inputs, and merges TurboQuant codes instead of
rebuilding them.
[D61](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d61-the-exact-route-bound-what-index-before-commit-guarantees-and-the-5-figure-missed):
no row of a file within its index's before-commit limits is ever on the exact route; the
provisional 5 % share of all rows was measured and missed where small files waited for
compaction's graph builds, and stays an open target.
[D62](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d62-the-blob-streaming-compaction-and-exact-route-benchmarks)
has the measurements, and
[D63](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d63-deferrals-and-open-problems-of-blobs-index-before-commit-and-streaming-compaction)
what is left.

## The public API

[D90](https://github.com/sayef/moraine/blob/main/DECISIONS.md#d90-the-public-api-for-01-what-changed-what-stays-and-how-betas-are-versioned):
before the first release I shrank the public surface to what applications use, made every
public enum, report and option struct `#[non_exhaustive]` with setters for the options, gave
plain writes a `Committed` result, made `Error` cloneable with other libraries' errors behind
sources, and stated how the `0.1.0` betas are versioned.
