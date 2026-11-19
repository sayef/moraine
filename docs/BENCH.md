# Benchmarks: moraine against Lance and Delta Lake

moraine has to be faster than both of the obvious alternatives on the paths a tracker uses most
(DECISIONS.md D17). This page is the evidence: the same workloads on three engines on one
machine, release builds only. Lance and Delta Lake run through kv0 (`KV0_ENGINE=lance` and
`KV0_ENGINE=delta`, kv0 `main` built with its `delta` feature); moraine runs through its own
tables once it has them. The harness is `bench/` (D22), the targets are D21.

## Reproduce

```sh
cd bench
cargo build --release                       # builds kv0 with both engines; needs protoc
BIN=../target/release/moraine-bench

# Local disk, the gate's sizes (2,000 runs x 2,400 points, 1,000,000 records), the fair
# comparison: five repetitions, multi-writer at 2, 4, 8 and 16 writers.
$BIN run --backend local --scale full --repeat 5 --mw-writers 2,4,8,16 \
  --workdir /tmp/moraine-bench --out results-local.jsonl

# S3-compatible server (RustFS in Docker), smaller sizes, three repetitions.
../spike/s3-server.sh start && source <(../spike/s3-server.sh env)
$BIN run --backend s3 --scale quick --repeat 3 --mw-writers 2,4,8,16 \
  --workdir /tmp/moraine-bench --out results-s3.jsonl
../spike/s3-server.sh stop

$BIN report results-local.jsonl results-s3.jsonl   # the tables below
cargo clean --release                               # the build is large; delete it afterwards
```

`--engines` picks engines: `lance`, `delta`, `moraine` (kv0's metric layout, strong
consistency), `moraine-own` (moraine's layout) and `moraine-eventual` (eventual consistency);
the default is all five. The multi-writer processes open their handles first and start
appending at one common instant (5 s after they are spawned), so the writers overlap the same
way whatever each engine's start-up costs; the baseline below predates this and started each
writer as soon as it had opened. Each workload phase is a separate process, so start-ups are
process-cold (the page cache stays warm) and peak memory is per phase. On S3, requests and bytes
are counted by a proxy inside each process; on local disk, bytes read is the process's `rchar`
(system-call reads including page-cache hits) and requests are not counted.

`--workloads log,records_key,records_cluster,multi_writer` (default all) runs a subset;
`--repeat N` repeats everything with engines interleaved; `--mw-writers` lists the writer
counts of the multi-writer workload. kv0's background maintenance is off in every phase since
the fair comparison (`--kv0-maintenance on` restores it; the multi-writer re-measure below measured it
both ways). The multi-writer verification also counts the rows moraine stores without
collapsing keys (`raw_rows`), so a commit applied twice fails it; kv0's reads return the newest
row per key and cannot show that.

## Threaded builds, blob parts and streams, the search pool, 2026-11-19

moraine alone except for compaction, release builds. Analysis: DECISIONS.md D80 to D86. The
host is shared with other users, and on 19 November its load never stayed low for long: each
run started at a 1-minute load under 10 on 20 cores, and the tables give the load at the start
and the end of each. A before-and-after pair ran one after the other, the earlier build
(`f05eca7`, graphs on one thread, parts in sequence, one-GET streams, per-file searches) first.
The fair comparison of all three engines was not rerun: the load stayed at 10 to 58 for the
rest of the day, so the numbers of the 2026-11-17 and 2026-11-18 sections stand, and where the
changes below make one stale this section says so.

Raw results: `bench/results/2026-11-19-{exact-route,exact-route-one-thread,blobs-local,blobs-s3,blobs-s3-in-flight,local-compaction-quiet,vector-builds-one-thread,vector-builds-threaded-per-file,vector-builds-threaded-pool}.jsonl`.

```sh
cd bench && cargo build --release
BIN=../target/release/moraine-bench
$BIN exact-route --minutes 10 --rows-per-append 1000 --interval-ms 500 --min-file-age-s 0 \
  --max-steps 64 --out exact.jsonl                     # and the other rows of the table below
$BIN blobs --backend local --workdir /tmp/moraine-blobs --out blobs-local.jsonl   # three times
$BIN blobs --backend s3 --count 16 --out blobs-s3.jsonl                           # three times
$BIN blobs --backend s3 --count 16 --in-flight 4 --out blobs-s3-in-flight.jsonl
$BIN run --backend local --scale full --engines moraine,delta --workloads log --repeat 3 \
  --workdir /tmp/moraine-bench --out compaction.jsonl
$BIN vector-d24 --rows 1000000 --layouts 1,10,100 --configs usearch-f32 --lance off \
  --workdir /tmp/moraine-vector --out vector.jsonl     # MORAINE_BENCH_POOL=0 or 1 forces a path
```

### Exact-route share under load, local disk

As in the 2026-11-18 section: 64-dimension vectors, `min_rows_for_ann` 10,000, a maintenance
pass every 10 s, the share sampled every 10 s with the first minute left out. "Late" is the mean
over the second half of the run.

| Load | Build | Exact share, mean (max) | Late | Append p50 / p99 (ms) | Host load, start / end |
|---|---|---|---|---|---|
| 20,000 rows every 2 s, 5 min | one thread | 0.0 % (0.0 %) | 0.0 % | 2,474 / 2,543 | 10.0 / 2.2 |
| 20,000 rows every 2 s, 5 min | threaded | 0.0 % (0.0 %) | 0.0 % | 1,265 / 2,380 | 6.1 / 23.4 |
| 1,000 rows every 0.5 s, `min_file_age` 0, 64 steps, 10 min | one thread | 26.6 % (52.2 %) | 34.5 % | 1.87 / 27.95 | 4.3 / 30.5 |
| 1,000 rows every 0.5 s, `min_file_age` 0, 64 steps, 10 min | threaded | 3.3 % (14.5 %) | 1.8 % | 4.18 / 9.30 | 9.3 / 3.0 |
| 1,000 rows every 0.5 s, 64 steps, 10 min (`min_file_age` 60 s) | threaded | 28.0 % (100 %) | 15.7 % | 4.26 / 6.86 | 3.0 / 8.5 |
| 1,000 rows every 0.1 s, `min_file_age` 0, 5 min | one thread | 82.6 % (91.7 %) | 82.1 % | 1.28 / 5.10 | 9.4 / 2.3 |
| 1,000 rows every 0.1 s, `min_file_age` 0, 5 min | threaded | 27.9 % (54.5 %) | 26.0 % | 3.47 / 11.62 | 8.5 / 18.0 |

The threaded build is the branch with main merged, so it also carries the group-commit task of
D64; small appends took about 2 ms longer at the median than with the earlier build (an
earlier threaded build without that merge measured 2.2 and 3.0 ms on the 0.5 s and 0.1 s rows),
and I have not separated the two.

### Vector builds and search, 1,000,000 rows of 256 dimensions, local disk

usearch F32, 1,000 queries, `expansion_search` 64 (recall@10 0.977 to 0.980 on one and 10
files, 0.9999 on 100). Build is the child process's wall time for all files.

| Layout | Build | Search path | Build (s) | p50 / p99 (ms) | Recall@10 | Host load, start / end |
|---|---|---|---|---|---|---|
| 10 files, 20 parts | one thread | per file | 130.8 | 2.52 / 8.06 | 0.9805 | 4.5 / 12.0 |
| 10 files, 20 parts | threaded | per file (the default here) | 53.9 | 2.74 / 9.16 | 0.9799 | 8.0 / 13.6 |
| 10 files, 20 parts | threaded | pool | 53.2 | 3.23 / 9.13 | 0.9790 | 5.7 / 16.7 |
| 100 files, 100 parts | one thread | per file | 78.1 | 3.79 / 6.15 | 0.9999 | 4.5 / 12.0 |
| 100 files, 100 parts | threaded | per file | 22.0 | 4.07 / 9.21 | 0.9999 | 8.0 / 13.6 |
| 100 files, 100 parts | threaded | pool (the default here) | 22.4 | 4.03 / 6.15 | 0.9999 | 5.7 / 16.7 |
| 1 file, 11 parts | threaded | per file (the default here) | 54.3 | 2.07 / 2.90 | 0.9767 | 8.0 / 13.6 |
| 1 file, 11 parts | threaded | pool | 54.3 | 2.00 / 4.82 | 0.9779 | 5.7 / 16.7 |

The builds are 2.4 and 3.5 times faster. The latencies did not improve: the D21 target (Lance's
3.1 / 5.1 ms) is still missed on 10 and 100 files, and the vector section's build times on the
benchmarks page are stale. The first threaded build lost a few rows (recall 0.997 at every
setting on 100 files, D80); the rows above are from the build that re-links them.

### Blobs

Three runs each, median (range), on the current build. Write is through appends of four values;
"raw PUT" PUTs the same values straight to the store, four at a time, with no table.

| Value size | Local write (MB/s) | Local whole / stream (MB/s) | RustFS write (MB/s) | RustFS raw PUT (MB/s) | RustFS whole / stream (MB/s) |
|---|---|---|---|---|---|
| 64 KiB (inline) | 95 (85-254) | 7,319 / 8,067 | 4.9 (4.0-6.4) | 5.8 (4.9-5.9) | 2,176 / 2,321 |
| 1 MiB | 1,422 (980-2,135) | 899 / 1,081 | 22.1 (21.8-24.9) | 36.0 (28.2-37.6) | 95 / 361 |
| 16 MiB | 2,995 (2,867-3,107) | 2,003 / 4,331 | 103 (80-128) | 145 (115-182) | 566 / 514 |
| 64 MiB | 3,170 (3,139-3,291) | 3,294 / 5,231 | 185 (105-207) | 239 (214-377) | 1,147 / 978 |

Host load at the start of the local runs 8.4 to 9.6, of the RustFS runs 6.6 to 7.6. With four
appends of one handle in flight (one run, load 6.3), RustFS takes 1 MiB values at 30.6 MB/s
against a raw 58.1 MB/s for sixteen PUTs at once, 64 KiB values at 22.1 MB/s.

One run each of the earlier and the current build, one after the other (local load 2.3 and 2.4,
RustFS 5.7 and 5.6 at the start):

| Value size | Local stream, before / after (MB/s) | Local whole, before / after | RustFS write, before / after | RustFS stream, before / after |
|---|---|---|---|---|
| 1 MiB | 873 / 2,532 | 2,573 / 3,371 | 19.6 / 20.1 | 132 / 407 |
| 16 MiB | 986 / 4,344 | 1,658 / 2,858 | 86.4 / 85.8 | 661 / 783 |
| 64 MiB | 962 / 5,098 | 3,099 / 2,765 | 124.7 / 106.5 | 1,051 / 906 |

Streams gained on local disk at every size and on RustFS for small values. Parallel parts did
not raise RustFS's write rate for 64 MiB values (D82). The blob section of the benchmarks page
now reads the three-run table.

### Compaction against Delta Lake, quiet host

The tracker tables at scale full, `--repeat 3` with moraine and Delta Lake interleaved, at a
1-minute load of 2.3 at the start and 2.0 at the end. moraine's streaming merge (the build at
`2c78a20`, before D64 to D68 were merged; compaction has since changed in the tags it keeps
and in the replace commit it uses, D65 and D68, so this is the figure to re-measure first):
**2.63 s (2.59-2.69)** and 264 MB (262-267); Delta Lake 4.88 s (4.70-5.33) and 356 MB
(344-381); size after compaction 143.6 MB against 154.3 MB. The earlier build, in the run before
(load 7.9 to 2.3): moraine 2.70 s (2.62-2.71), Delta Lake 4.60 s (4.33-4.67). The D21 compaction
targets are met on time and memory; the 6.97 s of 2026-11-18 was the loaded host.

## Blobs, streaming compaction, index before commit, 2026-11-18

> The code changed after this run: since 2026-11-19 graphs build on several threads, blob parts upload four at a time, streams read ranged GETs ahead and wide vector searches share one pool (D80 to D83). The numbers below that these touch (vector build times and latencies, blob writes and streams, the exact-route share and append latency) are re-measured or noted as stale in the 2026-11-19 section.

moraine alone, release build. Analysis: DECISIONS.md D61 and D62. Raw results:
`bench/results/2026-11-18-{blobs-local,blobs-s3,local-compaction-memory,local-compaction-streaming,exact-route}.jsonl`.

The host was shared with other heavy work throughout (load averages of 60 to 250 on 20 cores),
so wall times are slower and noisier than the 18 November rerun's on the same machine; logging that took
1.89 s then took 5.5 to 9.5 s here. Compare rows within this section, not with earlier ones.
Peak memory does not depend on the load.

```sh
cd bench && cargo build --release
BIN=../target/release/moraine-bench
# Compaction before and after, in one build: the in-memory merge is the earlier path.
MORAINE_BENCH_COMPACTION=memory $BIN run --backend local --scale full --engines moraine \
  --workloads log --workdir /tmp/moraine-bench --out compaction-memory.jsonl
$BIN run --backend local --scale full --engines moraine --workloads log \
  --workdir /tmp/moraine-bench --out compaction-streaming.jsonl
# Blobs: 64 KiB to 64 MiB values, 32 of each on local disk, 16 on the S3-compatible server.
$BIN blobs --backend local --workdir /tmp/moraine-blobs --out blobs-local.jsonl
../spike/s3-server.sh start && source <(../spike/s3-server.sh env)
$BIN blobs --backend s3 --count 16 --out blobs-s3.jsonl
../spike/s3-server.sh stop
# The exact-route share under load (one row of the table below per run).
$BIN exact-route --minutes 5 --rows-per-append 20000 --interval-ms 2000 --out exact.jsonl
$BIN exact-route --minutes 10 --rows-per-append 1000 --interval-ms 500 --min-file-age-s 0 \
  --max-steps 64 --out exact.jsonl
```

### Compaction of the tracker tables, local disk, scale full: three repetitions each, median (range)

| Measure | In memory (earlier path) | Streaming |
|---|---|---|
| Peak memory (MB) | 754 (751-754) | 264 (263-265) |
| Wall time (s) | 6.13 (5.52-6.84) | 6.97 (6.20-7.78) |
| Bytes read (MB, `rchar`) | 96.4 | 323.9 |
| Size on store after compaction (MB) | 143.7 | 143.6 |

Delta Lake, in the fair comparison (an unloaded host): 368 MB and 4.54 s.

### Blobs, local disk: three repetitions, median (range)

32 values per size, written 4 per append; 64 KiB values are below the 256 KiB inline threshold and
stay in the rows. Throughput is payload bytes over wall time; latencies are per call.

| Value size | Write (MB/s) | Read whole (MB/s) | Stream (MB/s) | 4 KiB range p50 / p99 (ms) | Append of a copied reference p50 / p99 (ms) |
|---|---|---|---|---|---|
| 64 KiB (inline) | 235 (229-389) | 8,808 | 8,204 | 0.000 / 0.000 | 0.71 / 1.04 |
| 1 MiB | 688 (548-784) | 2,189 | 576 | 0.031 / 0.19 | 1.44 / 2.58 |
| 16 MiB | 762 (656-1,224) | 1,944 | 435 | 0.035 / 0.10 | 12.5 / 27.8 |
| 64 MiB | 1,219 (859-1,292) | 1,490 | 678 | 0.054 / 0.61 | 64.5 / 110.0 |

### Blobs, S3-compatible server (RustFS): three repetitions, median (range)

16 values per size, written 4 per append.

| Value size | Write (MB/s) | Read whole (MB/s) | Stream (MB/s) | 4 KiB range p50 / p99 (ms) | Append of a copied reference p50 / p99 (ms) |
|---|---|---|---|---|---|
| 64 KiB (inline) | 3.3 (3.1-3.7) | 10,717 | 12,227 | 0.000 / 0.000 | 67.6 / 121.6 |
| 1 MiB | 21.6 (19.4-27.5) | 184 | 188 | 3.10 / 8.23 | 114 / 158 |
| 16 MiB | 95.8 (79.0-170.4) | 517 | 526 | 2.58 / 4.73 | 258 / 349 |
| 64 MiB | 98.8 (85.2-103.6) | 439 | 482 | 3.13 / 7.17 | 777 / 1,007 |

### Exact-route share under load, local disk

64-dimension vectors, a `vec-hnsw` index with `min_rows_for_ann` 10,000, a maintenance pass every
10 s in the same process; the share is sampled every 10 s and the first minute is left out.

| Load | Before commit | Exact share, mean (max) | Pending share, max | Append p50 / p99 (ms) |
|---|---|---|---|---|
| 20,000 rows every 2 s, 5 min | on | 0.0 % (0.0 %) | 0 | 9,176 / 10,582 |
| 20,000 rows every 2 s, 5 min | off | 77.8 % (87.7 %) | 100 % | 30 / 64 |
| 1,000 rows every 100 ms, 5 min | on | 95.4 % (100 %) | 0 | 2.0 / 11.9 |
| 1,000 rows every 100 ms, `min_file_age` 0, 5 min | on | 84.3 % (93.4 %) | 0 | 1.3 / 7.9 |
| 1,000 rows every 500 ms, `min_file_age` 0, 64 steps, 10 min | on | 48.5 % (66.2 %) | 0 | 1.9 / 8.5 |

### What the blobs run says

- **Compaction memory: met.** 264 MB against the in-memory path's 754 MB in the same run, and
  under Delta's 368 MB, which closes the compaction memory miss of D54. It costs time: 6.97 s against
  6.13 s here (14 % slower), from the second pass through local files that a step of two
  thousand inputs needs, which `rchar` shows as the extra bytes read.
- **Blob reads.** Whole reads run at 1.5 to 2.2 GB/s on local disk and 0.2 to 0.5 GB/s on RustFS.
  Streams are slower than whole reads on local disk (435 to 678 MB/s against 1.5 to 1.9 GB/s):
  each chunk is checksummed and passed through the stream separately. A 4 KiB range is one
  ranged GET: 30 to 54 µs on local disk, 2.6 to 3.1 ms on RustFS.
- **Blob writes on RustFS are slow for small external values**: 21.6 MB/s at 1 MiB, where each
  append is about 200 ms of PUTs and the log commit; values above 16 MiB reach about 100 MB/s,
  one value's parts going up one after another. That is a loss against what the server allows,
  and parallel parts are not done yet (D63).
- **A copied reference costs a server copy**: 0.7 to 65 ms on local disk (moraine's local copy
  reads and writes the bytes, D57), 68 ms to 0.8 s on RustFS, whose copy grows with the size.
- **The exact-route share**: zero for every file within the index's before-commit limits, as
  designed; the 5 % share of all rows was not met where small files waited for compaction's
  graph builds (D61).

## Rerun: compaction, part-parallel vector search, full-text fetches, 2026-11-18

> The code changed after this run: since 2026-11-19 graphs build on several threads, blob parts upload four at a time, streams read ranged GETs ahead and wide vector searches share one pool (D80 to D83). The numbers below that these touch (vector build times and latencies, blob writes and streams, the exact-route share and append latency) are re-measured or noted as stale in the 2026-11-19 section.

The rows that compaction and the search changes should move, measured again with each one's own method: the tracker
tables after compaction (fair comparison, local disk, three repetitions, Lance and moraine), the
D24 vector benchmark at a million rows, the full-text benchmark at 100,000 rows on one file (three
repetitions), and the 8-writer commit latency on the S3-compatible server (three repetitions),
direct and in queued mode. Analysis: DECISIONS.md D54. Raw results:
`bench/results/2026-11-18-rerun-{local,vector,vector-first,fts,s3-multi-writer}.jsonl`.

```sh
cd bench && cargo build --release
BIN=../target/release/moraine-bench
$BIN run --backend local --scale full --repeat 3 --engines lance,moraine --workloads log \
  --workdir /tmp/moraine-bench --out results-local.jsonl
$BIN vector-d24 --rows 1000000 --dims 256 --layouts 1,10,100 --configs usearch-f32 \
  --workdir /tmp/moraine-vector --out results-vector.jsonl
$BIN fts --rows 100000 --layouts 1 --queries 1000 --workdir /tmp/moraine-fts --out results-fts.jsonl
../spike/s3-server.sh start && source <(../spike/s3-server.sh env)
$BIN run --backend s3 --scale quick --repeat 3 --engines lance,delta,moraine,moraine-queued \
  --workloads multi_writer --mw-writers 8 --workdir /tmp/moraine-bench --out results-s3.jsonl
../spike/s3-server.sh stop
```

**What changed in the harness.** moraine's compaction phase now runs: every table is compacted
with `CompactionPolicy::full()` and checkpointed. The metrics table in kv0's layout declares
`(series_id, step)` as its cluster key, the order kv0's own optimise sorts its points into, so
moraine writes each flush's file in that order and compaction merges in it (D54); this also
changes the logging and pre-compaction rows. `moraine-queued` is moraine's queued write mode for
the multi-writer workload: each writer submits its batch to the inbox and waits until it is
committed (the latency is submission to visibility), and every writer runs an ingest loop that
does the work while it holds the ingest lease and releases it when the writer exits.

### Tracker tables, local disk, scale full: three repetitions, median (range)

| Measure | Lance | moraine |
|---|---|---|
| Logging: wall time (s) | 60.22 (58.25-61.79) | 1.89 (1.87-1.89) |
| Logging: finish and flush of one run, p99 (ms) | 57.02 (54.19-59.05) | 1.34 (1.15-1.37) |
| Size on store before compaction (MB) | 869.9 | 88.50 |
| Process-cold start-up (s) | 417.1 (395.4-421.1) | 0.62 (0.61-0.63) |
| Full series, p50 / p99 (ms) | 161.3 / 293.0 | 1.63 / 7.26 |
| 100-step window, p50 / p99 (ms) | 142.5 / 301.5 | 1.90 / 5.97 |
| Compaction of the tracker tables: wall time (s) | 7.33 (7.17-7.43) | 1.98 (1.97-2.05) |
| Compaction of the tracker tables: peak memory (MB) | 880.0 (798.0-901.0) | 753.0 (753.0-754.0) |
| Size on store after compaction (MB) | 973.6 | 143.7 |
| Process-cold start-up after compaction (s) | 13.25 (8.26-14.45) | 0.11 (0.11-0.12) |
| Full series after compaction, p50 (ms) | 3.02 (1.97-4.03) | 0.50 (0.30-0.56) |
| Full series after compaction, p99 (ms) | 16.68 (12.64-18.45) | 0.83 (0.55-1.04) |
| 100-step window after compaction, p50 (ms) | 3.12 (2.31-5.21) | 0.47 (0.30-0.57) |
| 100-step window after compaction, p99 (ms) | 18.40 (11.40-21.51) | 1.18 (0.61-1.39) |

Sizes after compaction count every object under the table, the files compaction replaced
included, for both engines (moraine's vacuum keeps them for the grace period). Delta was not
run again; in the fair comparison its compaction took 4.54 s and 368 MB, and left 154.3 MB.

### Vector search, 256 dimensions, 1,000,000 rows, usearch F32, `expansion_search` 64

| Layout | Recall@10 | D37 run p50 / p99 (ms) | Parts searched at once, always (ms) | Parts at once only with cores to spare (ms) |
|---|---:|---:|---:|---:|
| 1 file (11 parts) | 0.977 | 4.0 / 7.8 | 2.43 / 3.96 | 2.45 / 4.02 |
| 10 files (20 parts) | 0.981 | 2.4 / 9.2 | 5.95 / 11.87 | 2.33 / 5.84 |
| 100 files | 1.000 | 4.3 / 8.2 | 3.97 / 8.76 | 4.50 / 9.37 |

Lance (IVF-PQ, 64 sub-vectors, `nprobes` 20, refine 10, recall 0.956) answered in 8.85 / 16.36 ms
in this run, against 3.1 / 5.1 ms in the D37 run; the target below uses the faster run. The
first column of new numbers searched a file's parts on threads whatever the fan-out; across ten
files that oversubscribed the cores, so the final rule gives a file threads only when the cores
divided by the files searched leave four or more (D52).

### Full-text search, 100,000 rows, one file: three repetitions, median (range)

| p50 / p99, ms | merged tantivy | moraine, sidecar | moraine, in memory | Lance |
|---|---:|---:|---:|---:|
| Full-text run (one run) | 0.05 / 0.67 | 0.89 / 1.62 | - | 0.75 / 3.68 |
| This rerun | 0.05 / 0.66 | 0.77 (0.76-0.78) / 1.66 (1.57-1.76) | 0.76 / 1.49 | 1.42 (0.94-1.65) / 6.32 (6.27-7.07) |

Every ranking equal to the merged index in every repetition (1,000 of 1,000; largest score
difference 9.5e-7). A profile of the full-text run's build put 0.8 ms of the 0.9 ms median in
fetching the hits' `id` column; caching the small pages those fetches read (D53) brought it to
0.77 ms. The rest is decoding: a page per hit, through one Parquet reader per row group.

### Multi-writer, 8 writers, S3-compatible server, scale quick: three repetitions, median (range)

| | Lance | Delta | moraine | moraine, queued |
|---|---:|---:|---:|---:|
| Commits per second | 6.10 (4.80-6.30) | 9.70 (9.40-9.70) | 20.10 (15.30-20.80) | 12.2 (8.7-13.5) |
| Commit p50 (ms) | 238.9 | 291.4 | 123.2 (119.2-196.4) | 612.0 (586.0-655.0) |
| Commit p99 (ms) | 9,533 | 4,539 | 1,113 (1,038-1,373) | 1,015 (1,008-5,192) |
| Requests per commit | 18.5 | 43.2 | 14.9 | 48.1 |
| Runs with every row once | 3 of 3 | 3 of 3 | 3 of 3 | 3 of 3 |

The queued column's first run, before the writers released the ingest lease on exit, had a p99
of 300 s: a writer that held the lease finished and left, and the others' batches waited for the
lease to expire. That run is in the raw file too.

## Full-text sidecars, 2026-11-18

moraine's `fts` sidecars against one merged tantivy index over the same rows (ranking equality
and latency), and against Lance's full-text index from the `lance` crate kv0 compiles. Analysis:
DECISIONS.md D46. Raw results: `bench/results/2026-11-18-fts.jsonl`.

```sh
cd bench && cargo build --release && cd ..
target/release/moraine-bench fts --rows 100000,1000000 --layouts 1,10,100 --queries 1000 \
  --workdir /tmp/moraine-fts --out results-fts.jsonl
```

**Method.** Synthetic documents of 20 to 120 words drawn with a Zipf distribution (exponent 1)
from a 50,000-word vocabulary; 1,000 queries of one to three words drawn log-uniformly over the
vocabulary ranks, OR'ed, k = 10. The merged index is built by tantivy alone (one thread, merged
into one segment, the default tokenizer) and searched with `TopDocs`; its full ranking of every
query is the reference. moraine splits the rows into 1, 10 or 100 appends (the one-append layout
of a million rows becomes two data files at the default target file size), builds every sidecar
(`min_rows_for_sidecar` 1, ten builds at once), and is searched warm through a fresh handle with
a disk cache, and through a handle with sidecars disabled (every file indexed in memory on the
first query). A ranking counts as equal under the rule of D41. Lance writes one dataset with an
inverted index (simple tokenizer, lower case, no stemming or stop words) and is searched with
`full_text_search`. Latency is measured after 50 warm-up queries. moraine's latency includes
fetching the hits' `id` column from Parquet; the merged index reads it from a fast field. Release
build, local disk, 20 cores.

| p50 / p99, ms | merged tantivy | moraine, 1 file | 10 files | 100 files | Lance |
|---|---:|---:|---:|---:|---:|
| 100,000 rows | 0.05 / 0.67 | 0.89 / 1.62 | 0.72 / 1.09 | 0.58 / 0.98 | 0.75 / 3.68 |
| 1,000,000 rows | 0.31 / 4.14 | 1.53 / 4.12 | 1.57 / 2.83 | 1.56 / 2.74 | 2.21 / 13.84 |

| | 100,000 rows | 1,000,000 rows |
|---|---:|---:|
| Rankings equal to the merged index (sidecars; in memory) | 1,000; 1,000 of 1,000 on every layout | 1,000; 1,000 of 1,000 on every layout |
| Largest score difference | 9.5e-7 | 9.5e-7 |
| Build, merged tantivy (one thread) | 0.8 s | 9.9 s |
| Build, moraine, 1 / 10 / 100 files | 1.3 / 1.2 / 8.2 s | 10.7 / 6.3 / 9.5 s |
| Build, Lance | 0.4 s | 2.7 s |
| Index bytes, merged / moraine 1, 10, 100 files / Lance | 16.6 / 16.6, 21.3, 34.3 / 13.8 MB | 146.6 / 151.8, 166.6, 213.0 / 102.0 MB |
| First query, sidecars on disk not yet opened, 1 / 10 / 100 files | 77 / 172 / 926 ms | 473 / 474 / 1,523 ms |
| First query, no sidecars (indexing in memory), 1 / 10 / 100 files | 780 / 177 / 195 ms | 4,207 / 2,153 / 1,110 ms |
| Lance's top 10 against the merged index: overlap, same first hit | 96.4 %, 938 of 1,000 | 94.4 %, 904 of 1,000 |

## Vector sidecars (D24), 2026-11-17

> The code changed after this run: since 2026-11-19 graphs build on several threads, blob parts upload four at a time, streams read ranged GETs ahead and wide vector searches share one pool (D80 to D83). The numbers below that these touch (vector build times and latencies, blob writes and streams, the exact-route share and append latency) are re-measured or noted as stale in the 2026-11-19 section.

moraine's two vector kinds side by side, usearch HNSW graphs (`vec-hnsw`) and TurboQuant codes
(`vec-tq`, through the `turbovec` crate), with Lance's IVF-PQ index as the reference, built from
the `lance` crate kv0 already compiles. Decision and analysis: DECISIONS.md D37. Raw results:
`bench/results/2026-11-17-vector.jsonl`.

```sh
cd bench && cargo build --release && cd ..
BIN=target/release/moraine-bench
$BIN vector-d24 --rows 100000,1000000 --dims 256 --layouts 1,10,100 --queries 1000 \
  --workdir /tmp/moraine-vector --out results-vector.jsonl
$BIN vector-d24 --rows 100000 --dims 768 --layouts 1,10,100 --out results-vector.jsonl
```

**Method.** Synthetic vectors in 64 Gaussian clusters (seeded), cosine metric; 1,000 held-out
queries, k = 10, recall against brute-force ground truth. The rows are split into 1, 10 or 100
data files (100 files of 1,000 rows are skipped). Every index is built in a child process
(`build_indexes`, one thread for one file, ten for more; peak resident memory of that
process), then searched warm through one handle after a 50-query warm-up, at each search
setting. Cold first query: a new process with an empty disk cache (download, open, first
search); warm-disk first query: a new process whose disk cache holds the bundles but which has
no open handle. Resident memory is the growth of the process's resident set over the first
query, which opens every file's sidecar (usearch's mapped graph pages count here; they are not
heap). Every sidecar is built (`min_rows_for_ann` 1). Release build, local disk, 20 cores; the
machine ran nothing else.

Each row shows the cheapest setting that reached recall@10 of 0.95, or the best one if none did.
turbovec at 2 and 3 bits never reached 0.95 (best recall 0.40 to 0.69 and 0.80 to 0.94) and is left out;
the JSON lines have every setting.

| Data | Files | Config | Setting at recall >= 0.95 | Recall@10 | p50 ms | p99 ms | Build s (threads) | Build peak MiB | Sidecar B/vector | Cold first query ms | Warm-disk first query ms | Resident after first query MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 256-d, 100k | 1 | usearch-f32 | expansion_search 64 | 0.962 | 1.1 | 1.5 | 18.3 (1) | 179.7 | 1172.6 | 431.18 | 9.78 | 127.8 |
| 256-d, 100k | 1 | usearch-f16 | expansion_search 64 | 0.952 | 1.9 | 3.0 | 13.7 (1) | 138.8 | 660.6 | 317.08 | 19.86 | 81.0 |
| 256-d, 100k | 1 | usearch-i8 | expansion_search 128 | 0.982 | 1.9 | 3.5 | 8.7 (1) | 114.0 | 404.6 | 164.93 | 16.06 | 56.5 |
| 256-d, 100k | 1 | turbovec-4bit | rerank 4 | 0.968 | 2.9 | 4.5 | 0.4 (1) | 96.0 | 136.0 | 271.06 | 204.29 | 51.3 |
| 256-d, 100k | 10 | usearch-f32 | expansion_search 64 | 1.000 | 0.9 | 2.4 | 6.7 (10) | 132.5 | 1172.9 | 1091.54 | 10.26 | 133.1 |
| 256-d, 100k | 10 | usearch-f16 | expansion_search 64 | 1.000 | 1.6 | 4.4 | 4.8 (10) | 151.0 | 660.9 | 176.14 | 8.95 | 86.6 |
| 256-d, 100k | 10 | usearch-i8 | expansion_search 64 | 0.991 | 1.6 | 4.2 | 3.4 (10) | 129.0 | 404.9 | 142.67 | 8.93 | 62.1 |
| 256-d, 100k | 10 | turbovec-4bit | rerank 4 | 0.968 | 2.4 | 9.0 | 0.4 (10) | 114.0 | 136.4 | 278.17 | 224.0 | 64.1 |
| 256-d, 100k | 100 | usearch-f32 | expansion_search 64 | 1.000 | 2.7 | 4.9 | 2.0 (10) | 126.3 | 1177.1 | 710.21 | 22.6 | 183.9 |
| 256-d, 100k | 100 | usearch-f16 | expansion_search 64 | 1.000 | 3.3 | 7.0 | 1.5 (10) | 123.0 | 665.1 | 438.57 | 23.93 | 138.2 |
| 256-d, 100k | 100 | usearch-i8 | expansion_search 64 | 0.991 | 2.5 | 7.0 | 1.1 (10) | 104.6 | 409.1 | 399.73 | 13.07 | 113.9 |
| 256-d, 100k | 100 | turbovec-4bit | rerank 4 | 0.968 | 2.6 | 6.8 | 0.4 (10) | 151.3 | 140.2 | 409.27 | 262.72 | 59.0 |
| 256-d, 1M | 1 | usearch-f32 | expansion_search 64 | 0.977 | 4.0 | 7.8 | 187.0 (1) | 200.2 | 1172.6 | 6100.38 | 78.72 | 1194.8 |
| 256-d, 1M | 1 | usearch-f16 | expansion_search 128 | 0.987 | 6.6 | 12.4 | 193.0 (1) | 215.0 | 660.6 | 2327.87 | 74.2 | 686.9 |
| 256-d, 1M | 1 | usearch-i8 | expansion_search 256 | 0.965 | 6.1 | 10.8 | 156.3 (1) | 237.8 | 404.6 | 1191.99 | 61.27 | 447.4 |
| 256-d, 1M | 1 | turbovec-4bit | rerank 8 | 0.965 | 15.4 | 30.4 | 2.2 (1) | 245.8 | 136.0 | 974.17 | 407.9 | 397.4 |
| 256-d, 1M | 10 | usearch-f32 | expansion_search 64 | 0.981 | 2.4 | 9.2 | 129.3 (10) | 540.4 | 1172.6 | 3038.32 | 22.17 | 1202.2 |
| 256-d, 1M | 10 | usearch-f16 | expansion_search 64 | 0.977 | 4.2 | 13.1 | 96.4 (10) | 377.9 | 660.6 | 979.99 | 46.51 | 709.5 |
| 256-d, 1M | 10 | usearch-i8 | expansion_search 64 | 0.950 | 4.1 | 8.0 | 61.3 (10) | 336.1 | 404.6 | 729.39 | 43.73 | 464.6 |
| 256-d, 1M | 10 | turbovec-4bit | rerank 8 | 0.965 | 13.2 | 22.1 | 1.7 (10) | 251.1 | 136.0 | 511.84 | 255.97 | 472.8 |
| 256-d, 1M | 100 | usearch-f32 | expansion_search 64 | 1.000 | 4.3 | 8.2 | 78.0 (10) | 176.3 | 1172.9 | 3069.52 | 28.44 | 1232.6 |
| 256-d, 1M | 100 | usearch-f16 | expansion_search 64 | 1.000 | 5.5 | 15.5 | 54.8 (10) | 166.6 | 660.9 | 4001.58 | 25.87 | 748.1 |
| 256-d, 1M | 100 | usearch-i8 | expansion_search 64 | 0.971 | 4.5 | 9.5 | 36.2 (10) | 192.2 | 404.9 | 4145.22 | 22.75 | 503.1 |
| 256-d, 1M | 100 | turbovec-4bit | rerank 8 | 0.965 | 13.5 | 25.1 | 1.9 (10) | 123.6 | 136.4 | 849.14 | 278.38 | 423.5 |
| 256-d, 100k | 1 | lance-ivf-pq-sv32 | nprobes 20, refine 30 | 0.960 | 6.7 | 15.5 | 6.2 (1) | - | 40.5 | - | - | - |
| 256-d, 100k | 1 | lance-ivf-pq-sv64 | nprobes 20, refine 10 | 0.993 | 9.4 | 15.7 | 7.9 (1) | - | 72.6 | - | - | - |
| 256-d, 1M | 1 | lance-ivf-pq-sv32 | nprobes 80, refine 100 (best; < 0.95) | 0.947 | 7.9 | 33.0 | 11.7 (1) | - | 36.6 | - | - | - |
| 256-d, 1M | 1 | lance-ivf-pq-sv64 | nprobes 20, refine 10 | 0.958 | 3.1 | 5.1 | 14.7 (1) | - | 68.7 | - | - | - |
| 768-d, 100k | 1 | usearch-f32 | expansion_search 128 | 0.999 | 1.6 | 2.1 | 25.3 (1) | 196.6 | 3220.6 | 860.35 | 22.17 | 323.6 |
| 768-d, 100k | 1 | usearch-f16 | expansion_search 64 | 0.979 | 2.2 | 3.9 | 21.0 (1) | 202.0 | 1684.6 | 510.09 | 20.87 | 178.8 |
| 768-d, 100k | 1 | usearch-i8 | expansion_search 512 (best; < 0.95) | 0.914 | 2.3 | 2.8 | 16.2 (1) | 201.8 | 916.6 | 420.45 | 18.99 | 105.5 |
| 768-d, 100k | 1 | turbovec-4bit | rerank 4 | 0.967 | 4.5 | 10.0 | 0.8 (1) | 236.6 | 392.0 | 372.16 | 223.01 | 124.7 |
| 768-d, 100k | 10 | usearch-f32 | expansion_search 64 | 1.000 | 1.3 | 4.3 | 12.9 (10) | 285.4 | 3220.9 | 648.81 | 12.92 | 328.6 |
| 768-d, 100k | 10 | usearch-f16 | expansion_search 64 | 1.000 | 1.8 | 8.6 | 7.9 (10) | 191.0 | 1684.9 | 455.89 | 12.34 | 185.1 |
| 768-d, 100k | 10 | usearch-i8 | expansion_search 128 (best; < 0.95) | 0.914 | 1.9 | 9.6 | 3.3 (10) | 184.5 | 916.9 | 219.92 | 19.14 | 111.4 |
| 768-d, 100k | 10 | turbovec-4bit | rerank 4 | 0.967 | 4.7 | 6.7 | 1.0 (10) | 173.6 | 392.4 | 482.31 | 412.35 | 159.7 |
| 768-d, 100k | 100 | usearch-f32 | expansion_search 64 | 1.000 | 6.8 | 9.8 | 5.3 (10) | 156.3 | 3225.1 | 3390.73 | 21.13 | 379.3 |
| 768-d, 100k | 100 | usearch-f16 | expansion_search 64 | 1.000 | 5.3 | 9.1 | 3.4 (10) | 158.4 | 1689.1 | 935.88 | 11.79 | 236.1 |
| 768-d, 100k | 100 | usearch-i8 | expansion_search 64 (best; < 0.95) | 0.914 | 4.3 | 9.8 | 1.6 (10) | 131.8 | 921.1 | 1046.0 | 35.31 | 162.7 |
| 768-d, 100k | 100 | turbovec-4bit | rerank 4 | 0.967 | 4.4 | 13.1 | 0.8 (10) | 176.3 | 396.2 | 388.34 | 264.14 | 139.1 |
| 768-d, 100k | 1 | lance-ivf-pq-sv96 | nprobes 40, refine 50 | 0.983 | 5.6 | 7.6 | 19.3 (1) | - | 116.3 | - | - | - |
| 768-d, 100k | 1 | lance-ivf-pq-sv192 | nprobes 20, refine 10 | 0.991 | 9.0 | 23.6 | 28.7 (1) | - | 212.5 | - | - | - |

**Merge, determinism, filters** (10,000-row files, 256 dimensions, 100,000 rows):

| | usearch F32 | turbovec 4 bits |
|---|---|---|
| Rebuilding the same rows | byte-identical | byte-identical |
| Merging two files (20,000 rows) | 2.0 s: rebuild from 20 MB of floats | 2.4 ms: concatenate 2.7 MB of codes |
| Filter admitting 50 % / 5 % / 0.5 % | recall 1.0; p50 21 / 5.9 / 1.4 ms | recall 1.0; p50 20 / 5.3 / 2.6 ms |
| Half the sidecars deleted | recall 1.0; p50 22 ms | recall 0.98; p50 24 ms |

With 10,000-row files every filter here leaves fewer than `exact_filter_threshold` (10,000)
rows per file, so filtered searches take the exact route in every file, by design (case V8).

**The D21 vector target** (p50 and p99 at most Lance's at recall@10 of at least 0.95; Lance's
fastest setting that reaches 0.95):

| Data | Lance p50 / p99 | usearch F32 p50 / p99 (1, 10, 100 files) | Met |
|---|---|---|---|
| 256-d, 100k | 6.7 / 15.5 ms | 1.1 / 1.5, 0.9 / 2.4, 2.7 / 4.9 ms | yes |
| 256-d, 1M | 3.1 / 5.1 ms | 4.0 / 7.8, 2.4 / 9.2, 4.3 / 8.2 ms | no |
| 768-d, 100k | 5.6 / 7.6 ms | 1.6 / 2.1, 1.3 / 4.3, 6.8 / 9.8 ms | 1 and 10 files only |

Lance reached 0.95 only with a quarter as many PQ sub-vectors as dimensions, or a refine factor
of 30 and more (the file also holds a first pass, `lance-ivf-pq`, that tried refine factors up
to 10 only); at its default-like setting (dimensions over 8, refine 10) it stayed at 0.80 at
100,000 rows and 0.56 at a million. Its index is 37 to 213 bytes per vector.

**Decision** (D37): usearch stays the default vector index and turbovec stays behind its
feature. turbovec builds 5 to 85 times faster and is 8.6 times smaller, but its full scan of the
codes makes it 3 to 6 times slower than usearch at a million rows, far from the D21 target that
the decision rule requires on the 100-file layout.

## Fair comparison, 2026-11-17

The earlier tables compared engines that did different amounts of work. The kv0 side logged one
point per call with a context built inside the timer, ran its background maintenance inside the
timed phases, read with a version check per call while moraine read a snapshot up to 5 s old, and
read and parsed every record's data column while moraine projected three columns. moraine stored
metric points with a run id column and clustering that kv0's layout does not have, and its
lookups used a primary-key path where kv0 runs a filtered scan. Several reads were not checked
for their values. The harness now removes each of those differences, or reports both sides of it.

**What changed in the harness**

- **Consistency.** moraine reads with `Consistency::Strong` (a log check before every query), as
  kv0 checks for a newer version on its reads. Eventual consistency (5 s) is an extra column.
- **Maintenance.** kv0's background maintenance (automatic compaction at 32 fragments, version
  cleanup, mark pruning) is off in every timed phase. Compaction is a phase of its own for every
  table set: the tracker tables, the key-lookup tables and the cluster-key table. moraine had no
  compaction yet, so its row says so.
- **Layout.** The headline moraine column stores metric points the way kv0 does: a series id
  hashed from run id, name and context, no run id column, no clustering. moraine's own layout
  (run id column, clustered by run id and series id) is a separate column. kv0 after its own
  compaction is the layout-equalised kv0 figure.
- **Logging.** kv0 logs each series with one `track_many` call, with its points and context
  built before the timer (kv0's native input, as the tuples are the harness's). moraine's native
  input is Arrow batches, and building them is timed as moraine's logging calls, which counts
  work against moraine that a tracker on it would do. The logging calls and the finish-and-flush
  are timed separately, and the old "flush p99" row, which measured a whole run, is now named for
  what it measures.
- **Reads.** moraine reads every column of a record and parses its data JSON, as kv0 returns it.
  A lookup by item id is a filtered scan on `item_id` for every engine; moraine's primary-key
  `get` is a separate row.
- **Checked values.** Every full series is compared point for point (steps and value bits)
  against the run that was written, and the checksum of all compared points is recorded per
  engine; every window must return its 100 points; every lookup must return the rewritten value
  (`-k` for the first half of the keys); the full read must return N rows whose values sum to the
  expected total; every run-id query must return each item of its run once; the multi-writer
  verification counts raw rows where the engine can.
- **Compaction parity.** Lookups run before compaction for every engine (the headline) and again
  after compaction for the engines that have it.
- **Cache state.** The window read uses a different run from the full read, and the first read of
  each kind in a process is reported on its own. Every phase is a fresh process, so "cold" means
  process-cold: the operating system's page cache is warm for every engine alike.
- **Repetitions.** Five repetitions on local disk and three on S3, engines interleaved within
  each, reported as median with the range in brackets.
- **Records.** moraine rewrites the half of the keys with `upsert` (the key-lookup table is keyed
  by run id and item id); kv0 appends and keeps the newest row per key on read, as before.

**Integration differences that remain.** kv0 is a tracker with a write-ahead log: points are
durable when the logging call returns, and a flush commits its four tables one after another,
with run status and per-series statistics rows. The moraine adapter has no write-ahead log, keeps
no statistics rows, and commits its four tables concurrently; neither commit is atomic across
tables. These are differences of the integration, not of the engines, and they favour moraine on
the logging rows.

Raw lines: `bench/results/2026-11-17-fair-{local,s3}.jsonl`. Every value check passed in every
repetition on both backends, and the series checksum is the same for all five columns, so the
engines returned identical points.

### Local disk, scale full: five repetitions, median (range)

| Measure | Lance | Delta | moraine | moraine, own layout | moraine, eventual |
|---|---|---|---|---|---|
| Logging: wall time (s) | 58.99 (52.29-61.36) | 474.1 (472.4-474.8) | 1.52 (1.50-1.53) | 2.24 (2.21-5.37) | 1.52 (1.46-4.74) |
| Logging: points per second | 81373 (78232-91797) | 10125 (10110-10161) | 3162161 (3130285-3204178) | 2146549 (894130-2170337) | 3150552 (1012931-3277560) |
| Logging: logging calls of one run, p50 (ms) | 0.86 (0.45-1.00) | 0.42 (0.42-0.43) | 0.02 (0.02-0.02) | 0.02 (0.02-0.07) | 0.02 (0.02-0.06) |
| Logging: finish and flush of one run, p50 (ms) | 26.50 (20.24-28.52) | 233.8 (232.3-235.5) | 0.67 (0.66-0.68) | 1.03 (1.02-2.44) | 0.67 (0.66-2.10) |
| Logging: finish and flush of one run, p99 (ms) | 61.71 (54.89-128.4) | 454.4 (451.4-464.9) | 1.15 (1.13-1.26) | 1.41 (1.36-8.04) | 1.15 (0.95-6.80) |
| Logging: one run, calls + finish + flush, p50 (ms) | 27.67 (20.81-29.74) | 234.3 (232.7-235.9) | 0.69 (0.68-0.70) | 1.06 (1.04-2.51) | 0.69 (0.68-2.16) |
| Logging: one run, calls + finish + flush, p99 (ms) | 65.21 (56.72-129.1) | 454.9 (451.9-465.3) | 1.18 (1.16-1.33) | 1.44 (1.40-8.12) | 1.18 (0.97-6.86) |
| Logging: mean run, first tenth (ms) | 22.70 (14.20-23.60) | 58.80 (56.20-60.20) | 0.80 (0.70-0.90) | 1.10 (1.00-3.20) | 0.70 (0.70-2.50) |
| Logging: mean run, last tenth (ms) | 36.10 (32.50-45.10) | 422.3 (417.8-423.2) | 0.80 (0.70-0.90) | 1.10 (1.00-3.10) | 0.80 (0.80-2.50) |
| Logging: peak memory (MB) | 347.0 (337.0-350.0) | 1096 (1069-1116) | 72.00 (71.00-72.00) | 73.00 (73.00-73.00) | 71.00 (71.00-72.00) |
| Size on store before compaction (MB) | 869.9 (869.9-869.9) | 105.5 (105.5-105.5) | 79.80 (79.80-79.80) | 80.80 (80.80-80.80) | 79.80 (79.80-79.80) |
| Process-cold start-up: runs + params + series per run (s) | 422.7 (411.1-431.5) | 45.80 (39.59-55.40) | 0.60 (0.58-0.61) | 0.60 (0.60-1.94) | 0.57 (0.56-1.79) |
| Process-cold start-up: bytes read (MB) | 14888 (14888-14888) | 19.59 (19.59-19.59) | 19.97 (19.96-19.97) | 20.19 (20.19-20.20) | 19.96 (19.96-19.97) |
| Full series, first read of the process (ms) | 284.0 (192.6-355.5) | 149.2 (75.61-219.6) | 10.50 (1.97-21.40) | 2.01 (1.63-2.58) | 13.50 (4.57-182.6) |
| Full series, p50 (ms) | 154.5 (132.1-167.7) | 101.5 (84.47-115.0) | 7.43 (6.90-8.96) | 0.67 (0.56-1.84) | 7.22 (6.31-115.8) |
| Full series, p99 (ms) | 311.0 (192.2-340.0) | 177.6 (165.0-342.9) | 14.78 (11.81-18.02) | 1.18 (0.95-10.70) | 18.53 (14.80-162.0) |
| 100-step window, first read (ms) | 175.6 (116.4-246.4) | 141.2 (80.57-251.0) | 15.92 (8.32-22.15) | 1.34 (0.88-6.00) | 12.82 (6.97-83.30) |
| 100-step window, other run, p50 (ms) | 147.6 (131.7-163.1) | 100.5 (90.07-114.2) | 8.80 (7.37-9.77) | 0.66 (0.57-1.85) | 8.02 (7.52-105.5) |
| 100-step window, other run, p99 (ms) | 276.9 (186.6-328.4) | 173.6 (167.4-282.4) | 16.22 (12.07-22.07) | 1.12 (0.73-9.08) | 22.10 (13.91-179.8) |
| Compaction of the tracker tables: wall time (s) | 7.57 (7.08-7.78) | 4.54 (4.04-5.77) | not yet implemented | not yet implemented | not yet implemented |
| Compaction of the tracker tables: peak memory (MB) | 830.0 (759.0-844.0) | 368.0 (356.0-400.0) | not yet implemented | not yet implemented | not yet implemented |
| Size on store after compaction (MB) | 973.6 (973.6-973.6) | 154.3 (154.2-154.3) | - | - | - |
| Process-cold start-up after compaction (s) | 12.86 (8.07-13.54) | 19.01 (18.07-20.75) | - | - | - |
| Full series after compaction, p50 (ms) | 5.15 (3.00-5.59) | 14.09 (10.55-19.81) | - | - | - |
| Full series after compaction, p99 (ms) | 16.87 (12.07-20.77) | 36.57 (31.16-39.30) | - | - | - |
| 100-step window after compaction, p50 (ms) | 5.62 (3.32-5.94) | 14.44 (11.80-18.60) | - | - | - |
| 100-step window after compaction, p99 (ms) | 15.58 (10.92-17.33) | 37.21 (26.58-40.69) | - | - | - |
| Records: write 2.5 x N rows, half rewritten (s) | 3.48 (3.34-4.39) | 3.07 (2.92-3.31) | 3.35 (3.32-3.37) | - | 3.40 (3.30-6.94) |
| Lookup by item id (filtered scan), first (ms) | 41.87 (20.43-77.18) | 119.0 (98.81-130.6) | 5.19 (4.41-5.49) | - | 5.23 (4.05-5.88) |
| Lookup by item id (filtered scan), p50 (ms) | 38.04 (24.25-45.88) | 5.27 (3.95-6.22) | 1.31 (1.29-3.35) | - | 1.34 (1.15-2.77) |
| Lookup by item id (filtered scan), p99 (ms) | 70.10 (67.65-74.99) | 17.85 (11.09-20.75) | 5.18 (3.17-6.29) | - | 4.12 (1.89-5.89) |
| Lookup by primary key, p50 (ms) | - | - | 0.96 (0.90-1.00) | - | 0.89 (0.86-1.10) |
| Lookup by primary key, p99 (ms) | - | - | 3.91 (1.75-7.76) | - | 2.41 (1.68-3.28) |
| Records: full read, JSON parsed (s) | 1.56 (1.48-1.61) | 1.66 (1.52-1.72) | 0.40 (0.38-0.40) | - | 0.39 (0.39-0.49) |
| Compaction of the records tables (s) | 1.74 (1.64-2.22) | 1.46 (1.37-1.47) | not yet implemented | - | not yet implemented |
| Lookup by item id after compaction, p50 (ms) | 3.91 (1.81-4.80) | 7.83 (7.58-8.39) | - | - | - |
| Lookup by item id after compaction, p99 (ms) | 7.98 (5.80-8.74) | 22.66 (21.39-33.29) | - | - | - |
| Records: full read after compaction (s) | 1.40 (1.28-1.47) | 1.40 (1.36-1.41) | - | - | - |
| Cluster key: write (s) | 0.60 (0.55-0.82) | 0.98 (0.86-1.32) | 0.62 (0.61-0.63) | - | 0.63 (0.62-1.41) |
| Cluster key: first run-id query (ms) | 9.75 (9.41-10.69) | 1.56 (1.49-1.79) | 0.46 (0.36-0.64) | - | 0.42 (0.37-0.69) |
| Cluster key: 31 run-id queries (ms) | 870.1 (769.8-887.7) | 115.7 (90.02-141.2) | 11.86 (9.39-27.40) | - | 8.54 (8.30-16.32) |
| Cluster key: one run-id query, p50 (ms) | 29.01 (26.70-30.65) | 3.21 (2.92-3.88) | 0.33 (0.23-0.68) | - | 0.21 (0.19-0.38) |
| Cluster key: one run-id query, p99 (ms) | 45.41 (44.95-47.08) | 8.76 (4.56-12.10) | 0.75 (0.36-1.80) | - | 0.56 (0.42-0.87) |
| Cluster key: one IN query of 31 run ids (ms) | 77.28 (47.60-87.12) | 44.61 (36.06-53.80) | 5.46 (3.49-10.01) | - | 6.00 (3.65-12.04) |
| Compaction of the cluster-key table (s) | 0.69 (0.64-0.74) | 0.57 (0.53-0.57) | not yet implemented | - | not yet implemented |
| Cluster key after compaction: one run-id query, p50 (ms) | 1.55 (1.15-3.15) | 3.00 (2.81-4.25) | - | - | - |
| Cluster key after compaction: IN query (ms) | 36.63 (23.73-41.88) | 37.23 (34.29-50.57) | - | - | - |

| Multi-writer | Lance | Delta | moraine |
|---|---|---|---|
| 2 writers: commits per second | 237.4 (47.90-632.5) | 127.2 (121.0-159.0) | 885.4 (721.1-1882) |
| 2 writers: commit p50 (ms) | 3.20 (1.20-8.60) | 6.90 (5.90-8.40) | 1.60 (0.50-2.00) |
| 2 writers: commit p99 (ms) | 26.60 (10.80-250.4) | 121.2 (65.70-167.2) | 15.90 (6.30-23.80) |
| 2 writers: wall time (s) | 0.42 (0.16-2.09) | 0.79 (0.63-0.83) | 0.11 (0.05-0.14) |
| 2 writers: runs with every row once | 5 of 5 | 5 of 5 | 5 of 5 |
| 4 writers: commits per second | 222.0 (20.80-270.2) | 102.8 (98.50-106.0) | 1772 (1503-2857) |
| 4 writers: commit p50 (ms) | 3.60 (2.40-4.40) | 11.40 (9.60-13.10) | 0.80 (0.60-1.60) |
| 4 writers: commit p99 (ms) | 164.0 (158.0-912.9) | 226.3 (141.7-362.5) | 21.00 (7.80-25.70) |
| 4 writers: wall time (s) | 0.90 (0.74-9.61) | 1.95 (1.89-2.03) | 0.11 (0.07-0.13) |
| 4 writers: runs with every row once | 5 of 5 | 5 of 5 | 5 of 5 |
| 8 writers: commits per second | 156.7 (96.10-305.1) | 68.20 (64.70-83.30) | 1032 (961.4-1268) |
| 8 writers: commit p50 (ms) | 3.10 (1.30-3.20) | 22.60 (14.50-26.70) | 6.20 (3.50-6.60) |
| 8 writers: commit p99 (ms) | 502.5 (254.2-559.2) | 1017 (911.3-1179) | 29.20 (22.10-73.40) |
| 8 writers: wall time (s) | 2.55 (1.31-4.16) | 5.86 (4.80-6.18) | 0.39 (0.32-0.42) |
| 8 writers: runs with every row once | 5 of 5 | 5 of 5 | 5 of 5 |
| 16 writers: commits per second | 253.5 (152.4-313.5), 2 failed | 89.20 (85.40-89.30), 2 failed | 2577 (2107-2716) |
| 16 writers: commit p50 (ms) | 2.20 (1.70-2.30), 2 failed | 22.60 (22.20-25.20), 2 failed | 2.00 (1.40-2.30) |
| 16 writers: commit p99 (ms) | 967.0 (836.0-1113), 2 failed | 1750 (1672-1768), 2 failed | 72.20 (48.90-131.1) |
| 16 writers: wall time (s) | 3.16 (2.55-5.25), 2 failed | 8.97 (8.95-9.37), 2 failed | 0.31 (0.29-0.38) |
| 16 writers: runs with every row once | 3 of 5 | 3 of 5 | 5 of 5 |

**What the local run says, row by row.**

- **Logging: moraine is far ahead, and part of the lead is the integration.** 1.5 s against
  59 s (Lance) and 474 s (Delta); a run's finish and flush takes 0.67 ms against 26.5 ms and
  234 ms. kv0 writes and syncs a write-ahead log, keeps statistics rows and commits its four
  tables in sequence, which the moraine adapter does not do, so the ratio is an upper bound on
  the engine difference. Delta without its background maintenance is 2.6 times slower than with
  it (180 s in the first table run): its log grows without checkpoints, and later runs pay for it
  (the last tenth of runs takes 422 ms against 58 ms for the first).
- **Process-cold start-up: moraine wins by more than an order of magnitude, before and after
  kv0's compaction.** 0.60 s against 423 s (Lance, which reads 14.9 GB through 2,000 small
  fragments per table when nothing compacts in the background) and 46 s (Delta); after kv0's
  compaction Lance needs 12.9 s and Delta 19 s.
- **Series reads in kv0's layout: moraine loses to compacted Lance.** Uncompacted, moraine reads
  a full series in 7.4 ms against 155 ms (Lance) and 102 ms (Delta). After compaction Lance takes
  5.2 ms and Delta 14.1 ms, so moraine, which cannot compact yet, is 1.4 times slower than Lance
  for a full series and 1.6 times for a window (8.8 against 5.6 ms), and still faster than
  Delta. In its own layout moraine reads either in 0.67 ms. Most of the earlier two-orders gap
  was layout: kv0's series id cannot prune files, so every read checks every file's footer and
  bloom filter. Sort-merge compaction is what has to close this.
- **Consistency costs nothing measurable locally.** Strong and eventual moraine are within the
  noise on every row: a log check on local disk is one small file read.
- **Records writes: Delta wins.** 3.07 s against moraine's 3.35 s and Lance's 3.48 s, for
  loading 2 x 1,000,000 rows and upserting 500,000. Upsert has to find the files holding the
  rewritten keys; Delta's append defers that to its reads.
- **Lookups and scans: moraine wins with nobody compacted, and against compacted kv0.** A lookup
  by item id as a filtered scan takes 1.3 ms against 38 ms (Lance) and 5.3 ms (Delta), and
  3.9 ms and 7.8 ms after their compaction; moraine's primary-key get takes 1.0 ms. The full
  read with JSON parsing takes 0.40 s against 1.56 and 1.66 s. A run-id query takes 0.33 ms
  against 29 ms and 3.2 ms (1.6 ms for compacted Lance); the IN query of 31 runs 5.5 ms against
  77 and 45 ms (37 ms compacted).
- **Size and memory.** 80 MB on disk before compaction against 870 MB (Lance) and 106 MB
  (Delta); Delta's compaction grows it to 154 MB. Peak memory while logging is 72 MB against
  347 MB and 1.1 GB.
- **Multi-writer: no collapse as writers grow.** moraine commits 885, 1,772, 1,032 and 2,577
  times a second at 2, 4, 8 and 16 writers, with p99 between 16 and 72 ms, and stores every
  row exactly once in every run. Lance and Delta stay at 157-254 and 68-127 commits a second (medians),
  and at 16 writers one of their writers gave up in two of five runs (rows missing). At 8
  writers moraine meets D28's local target: 1,032 commits a second against 2 x 157 for Lance,
  p99 29 ms against min(0.5 x 1,017, 503) ms. Its p50 at 8 writers (6.2 ms) is still above
  Lance's 3.1 ms, as D28 describes. These runs last 0.1-0.4 s, so their rates vary by up to a
  factor of two between repetitions.

### S3-compatible server, scale quick: three repetitions, median (range)

| Measure | Lance | Delta | moraine | moraine, own layout | moraine, eventual |
|---|---|---|---|---|---|
| Logging: wall time (s) | 86.76 (73.51-86.98) | 75.07 (57.98-78.31) | 17.66 (14.36-20.93) | 14.44 (13.54-217.6) | 14.29 (13.47-15.49) |
| Logging: points per second | 5532 (5519-6530) | 6394 (6130-8278) | 27182 (22938-33417) | 33230 (2206-35452) | 33585 (30994-35637) |
| Logging: logging calls of one run, p50 (ms) | 1.85 (0.52-1.85) | 0.55 (0.52-0.63) | 0.06 (0.05-0.14) | 0.07 (0.04-0.10) | 0.06 (0.04-0.14) |
| Logging: finish and flush of one run, p50 (ms) | 418.7 (341.9-433.0) | 366.3 (256.9-384.6) | 68.83 (68.40-72.99) | 69.61 (61.97-355.6) | 67.30 (64.52-71.98) |
| Logging: finish and flush of one run, p99 (ms) | 642.5 (622.0-723.9) | 571.9 (515.0-923.2) | 312.0 (105.1-1291) | 150.7 (102.4-5408) | 133.9 (110.9-154.3) |
| Logging: one run, calls + finish + flush, p50 (ms) | 420.1 (342.2-434.2) | 366.8 (257.4-385.8) | 68.88 (68.45-73.12) | 69.67 (62.01-355.7) | 67.36 (64.58-72.04) |
| Logging: one run, calls + finish + flush, p99 (ms) | 644.0 (623.3-724.3) | 573.0 (515.7-923.7) | 312.2 (105.2-1292) | 150.7 (102.4-5408) | 133.9 (111.0-154.4) |
| Logging: mean run, first tenth (ms) | 320.9 (242.2-325.7) | 290.4 (287.4-296.9) | 92.90 (70.20-180.9) | 74.20 (68.30-3557) | 67.90 (66.80-82.50) |
| Logging: mean run, last tenth (ms) | 549.6 (334.2-572.3) | 494.5 (337.1-507.6) | 71.30 (67.20-77.20) | 90.50 (72.70-93.50) | 69.20 (67.00-84.70) |
| Logging: requests per run | 16.40 (16.40-16.40) | 36.70 (36.70-36.70) | 8.00 (8.00-8.00) | 8.00 (8.00-8.00) | 8.00 (8.00-8.00) |
| Logging: peak memory (MB) | 154.0 (151.0-155.0) | 132.0 (131.0-133.0) | 60.00 (59.00-61.00) | 59.00 (57.00-59.00) | 60.00 (58.00-60.00) |
| Size on store before compaction (MB) | 19.00 (19.00-19.00) | 8.70 (8.70-8.70) | 8.00 (8.00-8.00) | 8.10 (8.10-8.10) | 8.00 (8.00-8.00) |
| Process-cold start-up: runs + params + series per run (s) | 87.62 (40.82-112.9) | 5.64 (2.32-6.62) | 0.64 (0.47-0.82) | 0.77 (0.72-1.14) | 0.43 (0.26-0.60) |
| Process-cold start-up: requests | 81612 (81612-81612) | 3272 (3272-3272) | 1809 (1809-1809) | 1809 (1809-1809) | 1408 (1408-1408) |
| Process-cold start-up: bytes read (MB) | 224.6 (224.6-224.6) | 4.25 (4.25-4.25) | 2.85 (2.85-2.85) | 2.88 (2.88-2.88) | 2.71 (2.71-2.71) |
| Full series, first read of the process (ms) | 222.5 (213.3-373.4) | 57.26 (41.97-69.48) | 17.05 (15.13-31.04) | 3.73 (2.00-5.98) | 15.16 (14.70-60.44) |
| Full series, p50 (ms) | 121.0 (78.32-160.6) | 54.22 (26.55-58.16) | 3.80 (3.13-7.14) | 3.77 (2.77-5.62) | 2.35 (1.45-5.83) |
| Full series, p99 (ms) | 223.9 (101.5-267.6) | 138.4 (39.60-165.7) | 20.92 (6.19-26.78) | 6.03 (5.59-7.99) | 12.65 (5.84-26.12) |
| 100-step window, first read (ms) | 100.4 (99.33-117.3) | 62.21 (56.15-152.5) | 16.06 (1.90-42.23) | 3.07 (1.33-4.20) | 1.73 (1.30-12.65) |
| 100-step window, other run, p50 (ms) | 112.1 (79.18-144.8) | 52.52 (22.23-56.01) | 3.64 (2.95-6.67) | 3.72 (2.69-5.80) | 2.25 (1.57-6.34) |
| 100-step window, other run, p99 (ms) | 228.6 (99.89-246.5) | 118.9 (41.75-230.3) | 11.94 (11.93-17.42) | 5.78 (4.67-7.81) | 7.34 (5.17-14.69) |
| Compaction of the tracker tables: wall time (s) | 7.64 (5.55-8.95) | 3.11 (1.68-3.51) | not yet implemented | not yet implemented | not yet implemented |
| Compaction of the tracker tables: peak memory (MB) | 371.0 (364.0-390.0) | 150.0 (149.0-152.0) | not yet implemented | not yet implemented | not yet implemented |
| Size on store after compaction (MB) | 30.00 (30.00-30.00) | 13.60 (13.60-13.60) | - | - | - |
| Process-cold start-up after compaction (s) | 36.89 (27.53-45.86) | 4.03 (1.69-4.44) | - | - | - |
| Full series after compaction, p50 (ms) | 105.3 (59.63-109.6) | 17.23 (8.51-23.03) | - | - | - |
| Full series after compaction, p99 (ms) | 192.1 (73.76-198.7) | 28.69 (12.93-38.90) | - | - | - |
| 100-step window after compaction, p50 (ms) | 126.6 (63.20-152.8) | 16.78 (8.54-27.19) | - | - | - |
| 100-step window after compaction, p99 (ms) | 212.1 (79.25-215.9) | 28.23 (12.86-53.79) | - | - | - |
| Records: write 2.5 x N rows, half rewritten (s) | 1.99 (1.62-2.05) | 1.62 (1.57-2.72) | 1.58 (1.45-6.48) | - | 1.39 (1.33-1.58) |
| Lookup by item id (filtered scan), first (ms) | 87.09 (55.78-114.0) | 51.96 (26.51-92.15) | 41.25 (32.51-43.17) | - | 50.66 (30.33-52.67) |
| Lookup by item id (filtered scan), p50 (ms) | 31.30 (11.17-32.04) | 14.69 (7.95-17.05) | 0.81 (0.62-1.37) | - | 0.19 (0.19-0.45) |
| Lookup by item id (filtered scan), p99 (ms) | 40.93 (13.68-41.43) | 31.42 (12.24-43.50) | 1.39 (1.35-3.04) | - | 0.25 (0.23-0.87) |
| Lookup by item id: requests | 11.30 (11.30-11.30) | 6.60 (6.60-6.60) | 1.30 (1.30-1.30) | - | 0.30 (0.30-0.30) |
| Lookup by primary key, p50 (ms) | - | - | 1.10 (1.05-1.83) | - | 0.44 (0.41-0.89) |
| Lookup by primary key, p99 (ms) | - | - | 1.85 (1.49-9.95) | - | 0.47 (0.47-0.96) |
| Records: full read, JSON parsed (s) | 0.13 (0.13-0.15) | 0.17 (0.14-0.24) | 0.05 (0.04-0.06) | - | 0.04 (0.04-0.06) |
| Compaction of the records tables (s) | 0.92 (0.78-1.05) | 0.51 (0.32-0.55) | not yet implemented | - | not yet implemented |
| Lookup by item id after compaction, p50 (ms) | 19.87 (7.78-22.77) | 19.38 (12.42-21.78) | - | - | - |
| Lookup by item id after compaction, p99 (ms) | 36.51 (13.40-36.70) | 31.44 (15.91-43.85) | - | - | - |
| Records: full read after compaction (s) | 0.13 (0.13-0.16) | 0.14 (0.12-0.26) | - | - | - |
| Cluster key: write (s) | 0.67 (0.54-0.79) | 0.50 (0.35-0.57) | 0.36 (0.28-0.42) | - | 0.36 (0.27-0.38) |
| Cluster key: first run-id query (ms) | 21.42 (8.37-28.96) | 3.80 (3.56-7.26) | 1.26 (1.18-1.72) | - | 0.23 (0.23-0.32) |
| Cluster key: 31 run-id queries (ms) | 86.65 (31.49-98.11) | 36.22 (18.95-49.33) | 4.22 (3.73-7.76) | - | 1.06 (1.05-1.67) |
| Cluster key: one run-id query, p50 (ms) | 22.55 (7.70-23.41) | 8.77 (4.82-11.27) | 0.97 (0.84-1.56) | - | 0.21 (0.21-0.30) |
| Cluster key: one run-id query, p99 (ms) | 27.40 (9.44-28.40) | 12.01 (5.80-22.09) | 3.90 (1.08-4.87) | - | 0.25 (0.23-0.42) |
| Cluster key: one IN query of 31 run ids (ms) | 28.76 (9.61-35.33) | 14.52 (7.98-25.52) | 2.02 (1.43-2.77) | - | 0.66 (0.66-1.03) |
| Compaction of the cluster-key table (s) | 0.31 (0.31-0.38) | 0.15 (0.06-0.18) | not yet implemented | - | not yet implemented |
| Cluster key after compaction: one run-id query, p50 (ms) | 12.67 (5.02-14.93) | 8.89 (4.44-19.55) | - | - | - |
| Cluster key after compaction: IN query (ms) | 22.66 (7.79-23.95) | 15.99 (9.26-23.38) | - | - | - |

| Multi-writer | Lance | Delta | moraine |
|---|---|---|---|
| 2 writers: commits per second | 10.50 (10.00-13.20) | 16.60 (15.10-25.20) | 18.40 (17.80-20.40) |
| 2 writers: commit p50 (ms) | 92.10 (66.10-114.1) | 109.1 (77.20-135.1) | 84.70 (79.00-90.90) |
| 2 writers: commit p99 (ms) | 1569 (1096-2187) | 189.4 (109.5-238.5) | 424.7 (422.0-430.3) |
| 2 writers: requests per commit | 6.80 (6.60-8.40) | 13.50 (13.40-13.50) | 4.90 (4.90-4.90) |
| 2 writers: wall time (s) | 3.80 (3.03-3.98) | 2.41 (1.59-2.65) | 2.17 (1.96-2.25) |
| 2 writers: runs with every row once | 3 of 3 | 3 of 3 | 3 of 3 |
| 4 writers: commits per second | 6.30 (4.90-8.40) | 15.10 (13.80-23.90) | 19.70 (11.10-23.90) |
| 4 writers: commit p50 (ms) | 136.8 (111.0-156.1) | 167.7 (102.6-193.2) | 136.6 (113.0-137.1) |
| 4 writers: commit p99 (ms) | 2858 (2359-3659) | 899.9 (463.8-942.3) | 653.0 (559.8-2743) |
| 4 writers: requests per commit | 12.10 (11.90-13.80) | 23.00 (22.80-24.70) | 8.30 (8.20-8.50) |
| 4 writers: wall time (s) | 12.71 (9.54-16.18) | 5.32 (3.34-5.79) | 4.05 (3.34-7.22) |
| 4 writers: runs with every row once | 3 of 3 | 3 of 3 | 3 of 3 |
| 8 writers: commits per second | 6.10 (5.60-7.00) | 13.80 (10.10-20.80) | 21.50 (21.30-23.40) |
| 8 writers: commit p50 (ms) | 219.8 (175.3-290.8) | 268.0 (136.5-463.5) | 107.3 (79.60-110.2) |
| 8 writers: commit p99 (ms) | 8313 (7568-8377) | 2566 (1640-2711) | 1405 (1233-1578) |
| 8 writers: requests per commit | 17.80 (15.10-18.10) | 45.70 (44.80-47.00) | 13.90 (13.60-14.30) |
| 8 writers: wall time (s) | 26.35 (22.88-28.67) | 11.63 (7.68-15.87) | 7.45 (6.84-7.50) |
| 8 writers: runs with every row once | 3 of 3 | 3 of 3 | 3 of 3 |
| 16 writers: commits per second | failed | 7.90 (7.10-11.40) | 20.70 (18.40-23.60) |
| 16 writers: commit p50 (ms) | failed | 899.1 (347.2-925.3) | 73.00 (70.50-134.4) |
| 16 writers: commit p99 (ms) | failed | 9243 (8750-10689) | 3485 (2094-3593) |
| 16 writers: requests per commit | failed | 90.70 (89.10-90.90) | 24.70 (24.30-24.90) |
| 16 writers: wall time (s) | failed | 40.45 (27.98-45.19) | 15.44 (13.55-17.42) |
| 16 writers: runs with every row once | 0 of 3 | 3 of 3 | 3 of 3 |

**What the S3 run says.** On S3 the fixed costs are round trips, so the requests rows matter as
much as the times.

- **Logging: moraine about 5 times faster, from fewer requests.** 17.7 s against 87 s (Lance)
  and 75 s (Delta); 8 requests per run against 16.4 and 36.7, and a finish-and-flush p50 of
  69 ms against 419 and 366 ms. The integration differences above apply here too. One
  repetition of the own-layout column took 218 s with flush p99 of 5.4 s; the others took
  13.5-14.4 s, so I read it as an outlier of the local server, not of the layout, but it is in
  the raw lines.
- **Start-up: moraine wins by an order of magnitude.** 0.64 s and 1,809 requests against 88 s
  and 81,612 requests (Lance) and 5.6 s and 3,272 requests (Delta). Eventual consistency saves
  401 requests (0.43 s); after kv0's compaction Lance needs 37 s and Delta 4.0 s.
- **Series reads in kv0's layout: moraine wins on S3, unlike local disk.** 3.8 ms against
  121 ms and 54 ms, and against 105 ms and 17 ms after kv0's compaction. moraine keeps footers
  and bloom filters in memory per version (D20), so the per-file checks that cost it on local
  disk cost no requests here, while Delta and Lance pay round trips. The first read of a process
  costs 15-17 ms.
- **Strong consistency costs one request per query, visibly.** Lookups by item id take 0.81 ms
  and 1.3 requests strong against 0.19 ms and 0.3 requests eventual; run-id queries 0.97 against
  0.21 ms. Against kv0 either is far ahead: 31 ms (Lance) and 15 ms (Delta) before compaction,
  20 and 19 ms after.
- **Records writes: level with Delta.** 1.58 s against 1.62 s (Delta) and 1.99 s (Lance); one
  moraine repetition took 6.5 s.
- **Multi-writer: moraine's rate holds as writers grow; its p99 grows with the queue.** 18.4,
  19.7, 21.5 and 20.7 commits a second at 2, 4, 8 and 16 writers, every row once in every run.
  Delta falls from 16.6 to 7.9 commits a second at 16 writers; Lance lost a writer in all three
  16-writer runs. moraine's p99 rises from 425 ms to 3.5 s at 16 writers: with a flat commit rate
  a commit waits about writers / rate on average, 0.8 s at 16.

### The targets under the fair comparison

| Target (D21, D28) | Required | moraine | Result |
|---|---|---|---|
| Logging wall time, local | at most 0.7 x Lance = 41.3 s | 1.52 s | met |
| Logging wall time, S3 | at most Delta = 75.1 s | 17.7 s | met |
| Requests per flush, S3 | 4 data PUTs + 4 log PUTs | 8.0 | met |
| Multi-writer, local, 8 writers (D28) | at least 2 x Lance = 313 commits/s; p99 at most min(0.5 x Delta, Lance) = 502 ms; every row once | 1,032 commits/s; p99 29.2 ms; every run | met |
| Multi-writer commit p50, S3, 8 writers | at most min(0.5 x Delta, Lance) = 134 ms | 107 ms | met |
| Multi-writer commit p99, S3, 8 writers | at most min(0.5 x Delta, Lance) = 1,283 ms | 1,405 ms (1,233-1,578) | **not met** |
| Cold start-up | at most 0.5 x Lance: 211 s local, 44 s S3 | 0.60 s, 0.64 s | met |
| Full series and window before compaction | at most Lance | 7.4 / 8.8 ms local, 3.8 / 3.6 ms S3 | met |
| Point lookup by key (filtered scan; before the key index) | p50 at most 0.5 x Lance, p99 at most Lance | 1.31 / 5.2 ms local against 19.0 / 70.1; 0.81 / 1.4 ms S3 against 15.7 / 40.9 | met |

The S3 p99 miss is new, and comes from making Delta fair: with its background maintenance off,
Delta's 8-writer p99 fell from 3,838 ms (multi-writer re-measure, maintenance on) to 2,566 ms, which halves
to 1,283 ms; moraine's 1,405 ms is about where it was (1,148 ms in the multi-writer re-measure, 997-1,662).
Every repetition of moraine stayed under Lance's 8,313 ms. Rows that compare moraine without
compaction against compacted kv0 are not targets until moraine has compaction, but the local series row is
the one moraine loses today: 7.4 ms against compacted Lance's 5.2 ms.

## Multi-writer re-measured, 2026-11-17

Upserts and deletes reworked the commit loop (rebase or re-execute per entry, D27) and added a guard
against starving writers (D28), and both multi-writer rows were measured again: 8 processes
appending 100-row batches to one table as fast as they can, 50 appends each on local disk, 20 on
the S3-compatible server (RustFS in Docker). Single runs of this workload vary by a factor of two
or more, so each backend ran it ten times with all three engines in every run, five with kv0's
maintenance on and five with it off (`--workloads multi_writer`, `--kv0-maintenance off`).
Figures are medians over runs, with the range in brackets. moraine has no background maintenance,
so its ten runs are pooled. Runs in which a kv0 writer failed (one Lance run on local disk, two
Lance runs with maintenance off on S3; the verification then found 5,000 and 2,000 rows missing)
are left out. Raw lines: `bench/results/2026-11-17-multi-writer-{local,s3}.jsonl`.

### local, 8 writers x 50 appends

| Measure | Lance | Delta | Lance, no maintenance | Delta, no maintenance | moraine |
|---|---|---|---|---|---|
| Commits per second | 270.5 (262-636) | 112.9 (95-156) | 265.8 (157-277) | 68.7 (66-102) | 1,470 (1,094-2,201) |
| Commit p50 (ms) | 2.4 (1.3-2.9) | 17.4 (17.3-20.8) | 2.6 (1.9-2.9) | 20.7 (13.0-22.7) | 3.5 (0.8-5.8) |
| Commit p99 (ms) | 321.1 (190-505) | 355.4 (326-486) | 429.1 (339-500) | 980.7 (740-1,157) | 27.4 (22.0-97.6) |
| Wall time (s) | 1.5 | 3.5 | 1.5 | 5.8 | 0.3 |

### s3, 8 writers x 20 appends

| Measure | Lance | Delta | Lance, no maintenance | Delta, no maintenance | moraine |
|---|---|---|---|---|---|
| Commits per second | 6.6 (4.4-8.6) | 9.5 (8.4-15.6) | 6.1 (5.5-8.2) | 12.1 (9.4-19.3) | 21.1 (18.9-26.5) |
| Commit p50 (ms) | 191.0 (134-383) | 271.8 (239-384) | 213.4 (131-364) | 250.2 (186-279) | 113.2 (98.6-144.6) |
| Commit p99 (ms) | 11,214 (8,186-14,073) | 3,838 (2,194-4,133) | 11,660 (6,436-14,303) | 2,823 (1,996-3,793) | 1,148 (997-1,662) |
| Requests per commit | 24.2 | 67.2 | 18.7 | 44.4 | 14.1 |
| Wall time (s) | 24.1 | 16.9 | 26.4 | 13.3 | 7.6 |

### Multi-writer targets of the first table run, re-measured (D21)

| Target | Required (maintenance on / off) | moraine | Result |
|---|---|---|---|
| Commit p50, local (replaced by D28) | min(0.5 x Delta, Lance) = 2.4 / 2.6 ms | 3.5 ms | **not met** |
| Commits per second, local (D28) | at least 2 x Lance = 541 / 532 | 1,470 | met |
| Commit p99, local | min(0.5 x Delta, Lance) = 177.7 / 429.1 ms | 27.4 ms | met |
| Commit p50, S3 | min(0.5 x Delta, Lance) = 135.9 / 125.1 ms | 113.2 ms | met |
| Commit p99, S3 | min(0.5 x Delta, Lance) = 1,919 / 1,412 ms | 1,148 ms | met |
| All rows present, none twice | 40,001 / 16,001 rows, raw count equal | every moraine run | met |

**The guard (D28) on S3.** The same build without the guard, measured the same afternoon in seven
moraine-only runs, had a p50 of 100 ms (75-109) and a p99 of 2,103 ms (1,857-2,680), one of the
seven above the 2,350 ms target of the first table run. About 45 % of its commits lost no race and
the slowest lost up to 62 in a row, because nothing stops the same writers from losing round after
round. With the guard the p99 is 1,148 ms and the p50 113 ms. Strict orderings were tried first
and rejected: ranking every put by races lost (newcomers wait longest) brought the p99 to
950-1,900 ms but the p50 to 340-470 ms, and a pause for the writer that has just won either left
the p99 where it was (half a round trip) or raised the p50 to 160-210 ms (two round trips). With 8 writers at about 21 commits per second the mean commit takes about 380 ms whatever
the order (eight writers divided by the commit rate), so an order fair to everyone pushes the
median up to that mean; the guard only reorders the few writers deep in a losing streak.

**Local p50, still missed.** On local disk a log put takes about 0.3 ms, and with eight writers a
commit loses five to six races at the median, each costing a read of the winner's entry and a
put (about 0.6 ms together); the median is that queue. Lance's median of 2.4 ms comes with 270
commits per second, against moraine's 1,470, and a p99 twelve times moraine's. The guard does not
engage below a 2 ms round trip (timers cannot divide it), so the local numbers are the earlier
loop's. DECISIONS.md D28 records the miss and the target that replaces it on local disk: commits per
second at least 2 x Lance's, p99 within the existing bound, every row present exactly once.

## Tables: append and read, 2026-11-17

All three engines in one run per backend, same machine and versions as the baseline (moraine
with tables, append with group commit, scans, key lookups; no compaction yet). Raw lines are in `bench/results/2026-11-17-tables-*.jsonl`. Lance and Delta compact before their
key and run-id lookups (the workload calls `optimize`); moraine's lookups run on the uncompacted
files. "not yet implemented" rows are compaction.

### local, scale full

| Measure | Lance | Delta | moraine |
|---|---|---|---|
| Logging: wall time (s) | 97.5 | 180.02 | 2.38 |
| Logging: points per second | 49230 | 26663 | 2016440 |
| Logging: one run incl. flush, p50 (ms) | 37.3 | 84.2 | 1.1 |
| Logging: one run incl. flush, p99 (ms) | 252.7 | 198.5 | 1.8 |
| Logging: mean run, first tenth (ms) | 37.3 | 56.9 | 1.2 |
| Logging: mean run, last tenth (ms) | 59.7 | 111.0 | 1.2 |
| Logging: peak memory (MB) | 1058 | 610 | 73 |
| Size on store before compaction (MB) | 1064.8 | 563.3 | 81.3 |
| Cold start-up: runs + params + series per run (s) | 19.56 | 47.52 | 0.57 |
| Cold start-up: bytes read (MB) | 740.11 | 459.08 | 20.18 |
| Full series, p50 (ms) | 135.39 | 104.63 | 0.52 |
| Full series, p99 (ms) | 195.92 | 139.3 | 1.37 |
| 100-step window, p50 (ms) | 131.0 | 109.72 | 0.4 |
| 100-step window, p99 (ms) | 202.12 | 143.48 | 0.62 |
| Compaction: wall time (s) | 3.42 | 2.31 | not yet implemented |
| Compaction: peak memory (MB) | 1055 | 311 | not yet implemented |
| Size on store after compaction (MB) | 1190.0 | 624.8 | not yet implemented |
| Cold start-up after compaction (s) | 14.97 | 50.5 | not yet implemented |
| Full series after compaction, p50 (ms) | 4.23 | 17.82 | not yet implemented |
| Full series after compaction, p99 (ms) | 17.61 | 38.9 | not yet implemented |
| 100-step window after compaction, p50 (ms) | 4.57 | 18.34 | not yet implemented |
| 100-step window after compaction, p99 (ms) | 17.42 | 44.21 | not yet implemented |
| Records: write 2.5 x N rows (s) | 3.86 | 3.13 | 2.18 |
| Point lookup by key, p50 (ms) | 4.42 | 6.26 | 0.76 |
| Point lookup by key, p99 (ms) | 10.38 | 22.37 | 4.06 |
| Records: full read (s) | 1.39 | 1.43 | 0.33 |
| Cluster key: 31 run-id queries (ms) | 69.22 | 254.48 | 27.13 |
| Cluster key: one run-id query, p50 (ms) | 1.92 | 7.61 | 0.91 |
| Cluster key: one run-id query, p99 (ms) | 5.87 | 19.15 | 1.93 |
| Cluster key: one IN query of 31 run ids (ms) | 25.98 | 50.49 | 11.05 |
| Multi-writer: commits per second | 125.0 | 142.2 | 1035.4 |
| Multi-writer: commit p50 (ms) | 2.3 | 16.7 | 6.1 |
| Multi-writer: commit p99 (ms) | 492.4 | 369.3 | 22.0 |
| Multi-writer: wall time (s) | 3.2 | 2.81 | 0.39 |

### s3, scale quick

| Measure | Lance | Delta | moraine |
|---|---|---|---|
| Logging: wall time (s) | 103.16 | 82.62 | 14.65 |
| Logging: points per second | 4653 | 5810 | 32758 |
| Logging: one run incl. flush, p50 (ms) | 465.2 | 404.5 | 69.1 |
| Logging: one run incl. flush, p99 (ms) | 1210.6 | 619.3 | 119.1 |
| Logging: mean run, first tenth (ms) | 378.1 | 347.4 | 70.2 |
| Logging: mean run, last tenth (ms) | 676.9 | 450.8 | 77.5 |
| Logging: requests per run (flush) | 48.6 | 93.4 | 8.0 |
| Logging: peak memory (MB) | 390 | 179 | 61 |
| Size on store before compaction (MB) | 59.6 | 26.7 | 8.1 |
| Cold start-up: runs + params + series per run (s) | 49.73 | 5.67 | 0.4 |
| Cold start-up: requests | 3226 | 1356 | 1408 |
| Cold start-up: bytes read (MB) | 74.04 | 6.3 | 2.73 |
| Full series, p50 (ms) | 167.39 | 125.62 | 1.57 |
| Full series, p99 (ms) | 308.34 | 179.19 | 4.82 |
| 100-step window, p50 (ms) | 151.34 | 128.58 | 0.8 |
| 100-step window, p99 (ms) | 240.57 | 174.14 | 1.48 |
| Compaction: wall time (s) | 3.21 | 0.92 | not yet implemented |
| Compaction: peak memory (MB) | 281 | 134 | not yet implemented |
| Size on store after compaction (MB) | 72.7 | 32.8 | not yet implemented |
| Cold start-up after compaction (s) | 49.14 | 4.73 | not yet implemented |
| Full series after compaction, p50 (ms) | 131.27 | 34.6 | not yet implemented |
| Full series after compaction, p99 (ms) | 229.24 | 61.39 | not yet implemented |
| 100-step window after compaction, p50 (ms) | 140.47 | 34.66 | not yet implemented |
| 100-step window after compaction, p99 (ms) | 242.28 | 58.56 | not yet implemented |
| Records: write 2.5 x N rows (s) | 1.97 | 1.79 | 1.23 |
| Point lookup by key, p50 (ms) | 12.97 | 23.24 | 0.62 |
| Point lookup by key, p99 (ms) | 107.77 | 134.38 | 35.44 |
| Point lookup by key: requests | 6.2 | 3.8 | 0.4 |
| Records: full read (s) | 0.19 | 0.13 | 0.01 |
| Cluster key: 31 run-id queries (ms) | 54.62 | 22.4 | 0.86 |
| Cluster key: one run-id query, p50 (ms) | 15.29 | 7.01 | 0.23 |
| Cluster key: one run-id query, p99 (ms) | 19.06 | 21.59 | 0.44 |
| Cluster key: one IN query of 31 run ids (ms) | 23.84 | 12.25 | 0.83 |
| Multi-writer: commits per second | 6.2 | 9.8 | 21.5 |
| Multi-writer: commit p50 (ms) | 402.1 | 286.8 | 97.4 |
| Multi-writer: commit p99 (ms) | 7923.2 | 4701.0 | 2426.7 |
| Multi-writer: requests per commit | 27.3 | 65.1 | 13.9 |
| Multi-writer: wall time (s) | 25.99 | 16.38 | 7.43 |

### Targets for tables and appends (D21)

| Target | Required | moraine | Result |
|---|---|---|---|
| Logging wall time, local | at most 0.7 x Lance = 68.3 s | 2.38 s | met |
| Logging flush p99, local | at most 2 x Lance's p50 = 74.6 ms | 1.8 ms | met |
| Logging wall time, S3 (BENCH addition) | at most Delta = 82.6 s | 14.65 s | met |
| Requests per flush, S3 | 4 data PUTs + 4 log PUTs, no listing | 8.0 per run (p50 and mean), PUT only; the 4 GETs of the phase are the listings of opening the tables | met |
| Multi-writer commit p50, local | at most min(0.5 x Delta, Lance) = 2.3 ms | 6.1 ms | **not met** |
| Multi-writer commit p99, local | at most min(0.5 x Delta, Lance) = 184.7 ms | 22.0 ms | met |
| Multi-writer commit p50, S3 | at most min(0.5 x Delta, Lance) = 143.4 ms | 97.4 ms | met |
| Multi-writer commit p99, S3 | at most min(0.5 x Delta, Lance) = 2350.5 ms | 2426.7 ms | **not met** |
| Multi-writer: all rows present | every row once | verified on both backends | met |
| Cold start-up, before compaction | at most 0.5 x Lance: 9.8 s local, 24.9 s S3 | 0.57 s, 0.4 s | met |
| Full series and 100-step window, before compaction | at most Lance (p50 and p99) | 0.52 / 1.37 ms and 0.4 / 0.62 ms local; 1.57 / 4.82 and 0.8 / 1.48 ms S3 | met |
| Point lookup by key (before the key index) | p50 at most 0.5 x Lance, p99 at most Lance | 0.76 / 4.06 ms against 2.21 / 10.38 local; 0.62 / 35.44 ms against 6.49 / 107.77 S3 | met |

The cluster-key, size and compaction targets wait for the key index and compaction; the cluster-key rows
already meet "at most 0.5 x Lance" (0.91 ms against 0.96 ms local, 0.23 against 7.6 ms S3).

**The two misses** are both contention between eight writers that append as fast as they can to
one log, and DECISIONS.md D26 records them with what follows. In short: moraine commits eight
times as often as Lance (1,035 against 125 commits per second locally) and every commit waits
its turn, so the local p50 is the queue (eight writers times about 1 ms each) while Lance's
distribution is split into fast commits and a few that back off for half a second. On S3 the
profile (8 handles x 20 appends on RustFS, `backends::s3_contention_profile`) shows no failed
request other than lost log puts: about six lost races per commit, and a writer that has just
committed retries in one round trip while a loser needs two, so unlucky writers lose many times
in a row. A jittered pause after repeated losses brought the local p50 to about 1 ms but raised
the p99 to about 400 ms by starving the losers, and was reverted.

### What the first table run says

- **Logging is 41 times faster than Lance on local disk and 5.6 times faster than Delta on S3**,
  at 8 requests per run instead of 49 (Lance) and 93 (Delta), with a fifteenth of Lance's memory.
  Files are a thirteenth of Lance's size before compaction: zstd on sorted columns.
- **Start-up and series reads are two orders of magnitude faster**: each tracker table is read
  once per version (D20), metric points of a run sit in one file whose run-id range excludes all
  others, and the page index narrows a series to its pages (D19). Cold start-up on S3 costs about
  as many requests as Delta's (1,408 against 1,356: one listing and one GET per log entry plus one
  per small file) but reads 2.7 MB instead of 6.3 MB and finishes in 0.4 s instead of 5.7 s
  because the requests run in parallel.
- **Key and run-id lookups beat compacted Lance and Delta without compaction**, through bloom
  filters and page indexes held in the footer cache: 0.4 requests per lookup on S3 (most lookups
  are answered from cached file tails).
- **Contention** is the weak spot, as described above.

## Baseline, 2026-11-17

Lance and Delta Lake only; moraine had no tables yet. Raw lines are in
`bench/results/2026-11-17-*.jsonl`. Machine: x86_64 Linux, 20 threads (Intel Core i5-13500),
62 GB RAM, shared and noisy; Rust 1.99.1; kv0 `main` at `e0b9598` (Lance 14.0.2, deltalake-core
1.1.4); RustFS 1.0.6 in Docker on the same host. One run of each; differences under about 20 %
are within the noise of this machine.

"Run incl. flush" is one run of the logging workload: creating it, logging its 2,400 points,
finishing it and the flush (four table commits in kv0). Lookups and series reads use a fresh
handle and are medians over 50 queries (31 x 5 for run ids).

### local, scale full

| Measure | Lance | Delta | moraine |
|---|---|---|---|
| Logging: wall time (s) | 102.25 | 181.57 | not yet implemented |
| Logging: points per second | 46944.0 | 26435.0 | not yet implemented |
| Logging: one run incl. flush, p50 (ms) | 39.6 | 85.6 | not yet implemented |
| Logging: one run incl. flush, p99 (ms) | 212.9 | 197.9 | not yet implemented |
| Logging: mean run, first tenth (ms) | 36.6 | 60.9 | not yet implemented |
| Logging: mean run, last tenth (ms) | 64.9 | 107.1 | not yet implemented |
| Logging: peak memory (MB) | 1058.0 | 485.0 | not yet implemented |
| Size on store before compaction (MB) | 1059.5 | 552.2 | not yet implemented |
| Cold start-up: runs + params + series per run (s) | 18.1 | 39.31 | not yet implemented |
| Cold start-up: bytes read (MB) | 703.63 | 453.74 | not yet implemented |
| Full series, p50 (ms) | 83.08 | 104.16 | not yet implemented |
| Full series, p99 (ms) | 208.17 | 117.33 | not yet implemented |
| 100-step window, p50 (ms) | 94.96 | 108.91 | not yet implemented |
| 100-step window, p99 (ms) | 191.85 | 121.19 | not yet implemented |
| Compaction: wall time (s) | 3.58 | 2.36 | not yet implemented |
| Compaction: peak memory (MB) | 960.0 | 281.0 | not yet implemented |
| Size on store after compaction (MB) | 1184.6 | 613.6 | not yet implemented |
| Cold start-up after compaction (s) | 15.82 | 47.63 | not yet implemented |
| Full series after compaction, p50 (ms) | 3.34 | 17.53 | not yet implemented |
| Full series after compaction, p99 (ms) | 17.19 | 51.86 | not yet implemented |
| 100-step window after compaction, p50 (ms) | 3.75 | 16.16 | not yet implemented |
| 100-step window after compaction, p99 (ms) | 12.85 | 39.27 | not yet implemented |
| Records: write 2.5 x N rows (s) | 3.82 | 2.96 | not yet implemented |
| Point lookup by key, p50 (ms) | 1.18 | 5.97 | not yet implemented |
| Point lookup by key, p99 (ms) | 4.49 | 58.49 | not yet implemented |
| Records: full read (s) | 1.42 | 1.41 | not yet implemented |
| Cluster key: 31 run-id queries (ms) | 91.49 | 195.01 | not yet implemented |
| Cluster key: one run-id query, p50 (ms) | 2.7 | 6.18 | not yet implemented |
| Cluster key: one run-id query, p99 (ms) | 6.23 | 9.41 | not yet implemented |
| Cluster key: one IN query of 31 run ids (ms) | 27.76 | 41.48 | not yet implemented |
| Multi-writer: commits per second | 336.3 | 96.5 | not yet implemented |
| Multi-writer: commit p50 (ms) | 2.0 | 17.3 | not yet implemented |
| Multi-writer: commit p99 (ms) | 474.5 | 527.2 | not yet implemented |
| Multi-writer: wall time (s) | 1.19 | 4.15 | not yet implemented |

The S3 run uses the `quick` sizes (200 runs x 2,400 points, 100,000 records, 200 x 500 records,
8 writers x 20 appends of 100 rows): at 0.4 to 0.5 s per logged run, the gate's sizes would take well
over an hour per engine through one local container.

### s3, scale quick

| Measure | Lance | Delta | moraine |
|---|---|---|---|
| Logging: wall time (s) | 101.48 | 77.36 | not yet implemented |
| Logging: points per second | 4730.0 | 6205.0 | not yet implemented |
| Logging: one run incl. flush, p50 (ms) | 458.7 | 371.9 | not yet implemented |
| Logging: one run incl. flush, p99 (ms) | 1227.6 | 742.4 | not yet implemented |
| Logging: mean run, first tenth (ms) | 346.4 | 296.7 | not yet implemented |
| Logging: mean run, last tenth (ms) | 701.4 | 421.2 | not yet implemented |
| Logging: requests per run (flush) | 50.1 | 92.7 | not yet implemented |
| Logging: peak memory (MB) | 404.0 | 166.0 | not yet implemented |
| Size on store before compaction (MB) | 59.4 | 26.7 | not yet implemented |
| Cold start-up: runs + params + series per run (s) | 48.54 | 5.59 | not yet implemented |
| Cold start-up: requests | 2620 | 1354 | not yet implemented |
| Cold start-up: bytes read (MB) | 73.72 | 6.3 | not yet implemented |
| Full series, p50 (ms) | 167.4 | 130.0 | not yet implemented |
| Full series, p99 (ms) | 267.44 | 181.12 | not yet implemented |
| 100-step window, p50 (ms) | 176.06 | 120.49 | not yet implemented |
| 100-step window, p99 (ms) | 252.96 | 172.4 | not yet implemented |
| Compaction: wall time (s) | 3.22 | 0.84 | not yet implemented |
| Compaction: peak memory (MB) | 279.0 | 126.0 | not yet implemented |
| Size on store after compaction (MB) | 72.5 | 32.8 | not yet implemented |
| Cold start-up after compaction (s) | 51.79 | 5.28 | not yet implemented |
| Full series after compaction, p50 (ms) | 149.96 | 29.93 | not yet implemented |
| Full series after compaction, p99 (ms) | 228.83 | 50.98 | not yet implemented |
| 100-step window after compaction, p50 (ms) | 140.86 | 29.63 | not yet implemented |
| 100-step window after compaction, p99 (ms) | 207.92 | 49.28 | not yet implemented |
| Records: write 2.5 x N rows (s) | 1.94 | 1.43 | not yet implemented |
| Point lookup by key, p50 (ms) | 20.87 | 11.57 | not yet implemented |
| Point lookup by key, p99 (ms) | 79.23 | 115.4 | not yet implemented |
| Point lookup by key: requests | 6.2 | 3.8 | not yet implemented |
| Records: full read (s) | 0.14 | 0.16 | not yet implemented |
| Cluster key: 31 run-id queries (ms) | 46.86 | 22.64 | not yet implemented |
| Cluster key: one run-id query, p50 (ms) | 14.32 | 5.98 | not yet implemented |
| Cluster key: one run-id query, p99 (ms) | 20.55 | 24.89 | not yet implemented |
| Cluster key: one IN query of 31 run ids (ms) | 22.14 | 10.67 | not yet implemented |
| Multi-writer: commits per second | 6.3 | 9.7 | not yet implemented |
| Multi-writer: commit p50 (ms) | 184.7 | 274.0 | not yet implemented |
| Multi-writer: commit p99 (ms) | 10422.3 | 3468.5 | not yet implemented |
| Multi-writer: requests per commit | 21.6 | 64.8 | not yet implemented |
| Multi-writer: wall time (s) | 25.3 | 16.45 | not yet implemented |

### What the baseline says

- **On local disk the gate's findings reproduce through the Rust API.** Delta logs 78 % slower
  than Lance (181.6 s against 102.3 s; 85.6 ms against 39.6 ms per run), its cold start-up is 2.2
  times Lance's before compaction and 3.0 times after, its key lookups are 5 times slower at p50
  and 13 times at p99, its run-id queries about twice as slow, and its series reads after
  compaction 4 to 5 times slower. It wins on size (552 MB against 1,060 MB), compaction time and
  memory (281 MB against 960 MB). Eight local writers commit 3.5 times faster on Lance.
- **On the S3-compatible server the picture changes.** Delta is ahead on most rows: logging 24 %
  faster, cold start-up 9 times faster (Lance reads 74 MB in 2,620 requests where Delta reads 6 MB
  in 1,354), series reads after compaction 5 times faster, and key and run-id lookups about twice
  as fast at p50. Delta needs far more requests per commit (93 per logged run against 50, 65 per
  multi-writer commit against 22), which is the cost D18 removes; Lance's multi-writer p99 of
  10.4 s shows its conflict retries.
- **What moraine has to beat** is therefore the better of the two per row, which differs by
  backend: Lance on local disk for logging, start-up, lookups and series reads; Delta on S3 for
  most of the same rows; Delta everywhere for size, compaction time and memory.

## When moraine's columns fill in

| moraine feature | Workloads that start reporting | Targets (D21) |
|---|---|---|
| Append, scan, point lookup, group commit, snapshot refresh | Logging, multi-writer, cold start-up, series reads, records write and full read, key lookups without the key index | Logging wall time at most 0.7 x Lance on local disk (at most 72 s here) and at most Delta on S3; requests per flush: one data PUT and one log PUT per touched table, no listing (Delta needs 93 per run, Lance 50); multi-writer commit p50 and p99 at most 0.5 x Delta and at most Lance on S3; on local disk (D28) commits per second at least 2 x Lance, p99 within the same bound, every row exactly once; cold start-up at most 0.5 x Lance (at most 9 s local) |
| Upsert, delete, update | Multi-writer re-measured (above). Records with rewritten keys switched from append-and-supersede to upsert in the fair comparison | As for append |
| Sidecars, key index, vector index | Key and run-id lookups through the key index; vector search against Lance's vector index | Key lookup p50 at most 0.5 x Lance (0.6 ms local) and p99 at most Lance (4.5 ms); run-id queries at most 0.5 x Lance (46 ms for 31 local); vector p50 and p99 at most Lance at recall@10 of at least 0.95 |
| Full-text index | Full-text queries against Lance's FTS | p50 and p99 at most Lance |
| Compaction (sort-merge by cluster key), maintenance | Every "after compaction" row, size, compaction time and memory | Size at most Delta (614 MB local); compaction time and memory at most Delta (2.4 s, 281 MB); start-up and series reads after compaction at most Lance local (15.8 s, 3.3 ms) and at most Delta on S3 |

The moraine engine in `bench/src/engine.rs` maps the tracker operations onto moraine tables
from the first table run: runs, parameters and series as small tables (cached per version, D20), metric points
clustered by series id, records keyed by (run id, item id) and clustered by run id (D19).
