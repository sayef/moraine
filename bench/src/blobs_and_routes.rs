//! Measurements of moraine alone: blob throughput and range-read latency
//! (`blobs`), and the share of rows on the exact route under a steady append and compaction
//! load (`exact-route`).
//!
//! ```sh
//! moraine-bench blobs --backend local --workdir /tmp/moraine-blobs --out blobs.jsonl
//! moraine-bench exact-route --minutes 10 --workdir /tmp/moraine-exact --out exact.jsonl
//! ```
//!
//! `blobs` writes `--count` values of each size in `--sizes` (bytes, comma-separated) through
//! appends of `--per-append` rows, then reads each one whole, streams it, reads `--ranges`
//! random 4 KiB ranges, and copies `--copies` references (pass-through copies on the server).
//! `--in-flight N` keeps N appends of one handle in flight at once (default 1).
//! Throughput is payload bytes over wall time; latencies are per call.
//!
//! `exact-route` appends `--rows-per-append` 64-dimension vectors every `--interval-ms` and runs
//! a maintenance pass (compaction with the default tiered policy, index build, vacuum) every
//! `--maintain-s` seconds, for `--minutes`, on local disk. Every 10 seconds it records the share
//! of rows on the exact route: rows in files without a sidecar (below the kind's threshold, or
//! not yet built) over all rows, from `index_status`, and the same split for a search.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float32Builder, Int64Array, ListBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::{StreamExt, TryStreamExt};
use moraine::blob::{self, BlobColumn, BlobRef};
use moraine::{CompactionPolicy, Database, IndexSpec, MaintainOptions, TableOptions};
use serde_json::{Value, json};

use crate::{Rng, arg, arg_usize, ms, pct, round};

fn storage(backend: &str) -> HashMap<String, String> {
    let mut s = HashMap::new();
    if backend == "s3" {
        let env = |k: &str| std::env::var(k).unwrap_or_default();
        s.insert("aws_endpoint".into(), env("MORAINE_SPIKE_S3_ENDPOINT"));
        s.insert("aws_access_key_id".into(), env("AWS_ACCESS_KEY_ID"));
        s.insert("aws_secret_access_key".into(), env("AWS_SECRET_ACCESS_KEY"));
        s.insert("aws_region".into(), "us-east-1".into());
        s.insert("aws_allow_http".into(), "true".into());
    }
    s
}

fn location(backend: &str, workdir: &std::path::Path, what: &str) -> String {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    if backend == "s3" {
        let bucket =
            std::env::var("MORAINE_SPIKE_S3_BUCKET").unwrap_or_else(|_| "moraine-spike".into());
        format!("s3://{bucket}/blobs-{stamp:x}/{what}")
    } else {
        let p = workdir.join(format!("{what}-{stamp:x}"));
        std::fs::create_dir_all(&p).expect("workdir");
        p.to_string_lossy().into_owned()
    }
}

fn write_line(out: &PathBuf, v: &Value) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out)
        .expect("out");
    writeln!(f, "{v}").expect("write");
    println!("{v}");
}

fn mb_per_s(bytes: u64, d: Duration) -> f64 {
    round(bytes as f64 / 1e6 / d.as_secs_f64().max(1e-9), 1)
}

pub async fn blobs(args: &[String]) {
    let backend = arg(args, "--backend").unwrap_or_else(|| "local".into());
    let workdir =
        PathBuf::from(arg(args, "--workdir").unwrap_or_else(|| "/tmp/moraine-blobs".into()));
    let out = PathBuf::from(arg(args, "--out").unwrap_or_else(|| "blobs.jsonl".into()));
    let sizes: Vec<usize> = arg(args, "--sizes")
        .unwrap_or_else(|| "65536,1048576,16777216,67108864".into())
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    let count = arg_usize(args, "--count", 32);
    let per_append = arg_usize(args, "--per-append", 4);
    let ranges = arg_usize(args, "--ranges", 200);
    let copies = arg_usize(args, "--copies", 16);
    // Appends one writer keeps in flight at once; the handle's group commit batches them.
    let in_flight = arg_usize(args, "--in-flight", 1).max(1);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob::field("body", true),
    ]));
    let mut rng = Rng(7);
    for &size in &sizes {
        let loc = location(&backend, &workdir, &format!("blobs-{size}"));
        let db = Database::builder(&loc)
            .storage_options(storage(&backend))
            .open()
            .await
            .expect("open");
        let t = db
            .create_table("b", schema.clone(), TableOptions::default())
            .await
            .expect("create");
        let value: Vec<u8> = (0..size).map(|i| (i * 31 + size) as u8).collect();
        // Writes: appends of `per_append` values each, `in_flight` at a time.
        let mut batches = Vec::new();
        let mut id = 0i64;
        while (id as usize) < count {
            let n = per_append.min(count - id as usize);
            let mut c = BlobColumn::new();
            for _ in 0..n {
                c.push_bytes(&value, None);
            }
            let ids = Int64Array::from_iter_values(id..id + n as i64);
            batches.push(
                RecordBatch::try_new(schema.clone(), vec![Arc::new(ids) as ArrayRef, c.finish()])
                    .expect("batch"),
            );
            id += n as i64;
        }
        let started = Instant::now();
        let mut append_ms: Vec<f64> = futures::stream::iter(batches)
            .map(|b| {
                let t = t.clone();
                async move {
                    let t0 = Instant::now();
                    t.append(vec![b]).await.expect("append");
                    ms(t0.elapsed())
                }
            })
            .buffer_unordered(in_flight)
            .collect()
            .await;
        let write = started.elapsed();
        // What the store allows: the same values PUT straight to it, as many at once as the
        // writes above had in flight, with no table, data file or commit.
        let raw = {
            let url = url::Url::parse(&if backend == "s3" {
                loc.clone()
            } else {
                format!("file://{loc}")
            })
            .expect("url");
            let (store, base) =
                object_store::parse_url_opts(&url, storage(&backend)).expect("store");
            let store: Arc<dyn object_store::ObjectStore> = Arc::from(store);
            let body = object_store::PutPayload::from(value.clone());
            let started = Instant::now();
            futures::stream::iter(0..count)
                .map(|i| {
                    let (store, body) = (store.clone(), body.clone());
                    let at = base.clone().join("raw").join(i.to_string());
                    async move {
                        object_store::ObjectStoreExt::put(store.as_ref(), &at, body)
                            .await
                            .expect("raw put")
                    }
                })
                .buffer_unordered(per_append * in_flight)
                .collect::<Vec<_>>()
                .await;
            started.elapsed()
        };
        let rows = t.scan().collect().await.expect("scan");
        let refs: Vec<BlobRef> = rows
            .iter()
            .flat_map(|b| blob::refs(b, "body").expect("refs"))
            .flatten()
            .collect();
        // Whole reads.
        let started = Instant::now();
        let mut read_ms = Vec::new();
        for r in &refs {
            let t0 = Instant::now();
            let b = t.blob(r).expect("blob").bytes().await.expect("bytes");
            read_ms.push(ms(t0.elapsed()));
            assert_eq!(b.len(), size);
        }
        let read = started.elapsed();
        // Streamed reads.
        let started = Instant::now();
        for r in &refs {
            let n = t
                .blob(r)
                .expect("blob")
                .stream()
                .await
                .expect("stream")
                .try_fold(0usize, |n, c| async move { Ok(n + c.len()) })
                .await
                .expect("chunks");
            assert_eq!(n, size);
        }
        let stream = started.elapsed();
        // Range reads of 4 KiB.
        let mut range_ms = Vec::new();
        for _ in 0..ranges {
            let r = &refs[(rng.next() as usize) % refs.len()];
            let len = 4096.min(size) as u64;
            let at = rng.next() % (size as u64 - len + 1);
            let t0 = Instant::now();
            let b = t
                .blob(r)
                .expect("blob")
                .range(at..at + len)
                .await
                .expect("range");
            range_ms.push(ms(t0.elapsed()));
            assert_eq!(b.len() as u64, len);
        }
        // Pass-through copies, one value per append.
        let mut copy_ms = Vec::new();
        for (k, r) in refs.iter().enumerate().take(copies) {
            let mut c = BlobColumn::new();
            c.push_ref(r);
            let ids = Int64Array::from_iter_values([1_000_000 + k as i64]);
            let b =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(ids) as ArrayRef, c.finish()])
                    .expect("batch");
            let t0 = Instant::now();
            t.append(vec![b]).await.expect("append");
            copy_ms.push(ms(t0.elapsed()));
        }
        let bytes = (size * refs.len()) as u64;
        write_line(
            &out,
            &json!({
                "bench": "blobs",
                "backend": backend,
                "size": size,
                "count": refs.len(),
                "in_flight": in_flight,
                "write_mb_s": mb_per_s(bytes, write),
                "raw_put_mb_s": mb_per_s((size * count) as u64, raw),
                "append_p50_ms": round(pct(&mut append_ms, 0.5), 2),
                "append_p99_ms": round(pct(&mut append_ms, 0.99), 2),
                "read_mb_s": mb_per_s(bytes, read),
                "read_p50_ms": round(pct(&mut read_ms, 0.5), 2),
                "stream_mb_s": mb_per_s(bytes, stream),
                "range_4k_p50_ms": round(pct(&mut range_ms, 0.5), 3),
                "range_4k_p99_ms": round(pct(&mut range_ms, 0.99), 3),
                "copy_append_p50_ms": round(pct(&mut copy_ms, 0.5), 2),
                "copy_append_p99_ms": round(pct(&mut copy_ms, 0.99), 2),
            }),
        );
        if backend == "local" {
            let _ = std::fs::remove_dir_all(&loc);
        }
    }
}

const DIM: usize = 64;

fn vector_batch(rng: &mut Rng, first: i64, n: usize) -> RecordBatch {
    let schema = vector_schema();
    let mut b = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new(
        "element",
        DataType::Float32,
        true,
    )));
    for _ in 0..n {
        for _ in 0..DIM {
            b.values()
                .append_value((rng.next() % 2000) as f32 / 1000.0 - 1.0);
        }
        b.append(true);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(first..first + n as i64)) as ArrayRef,
            Arc::new(b.finish()),
        ],
    )
    .expect("batch")
}

fn vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "v",
            DataType::List(Arc::new(Field::new("element", DataType::Float32, true))),
            true,
        ),
    ]))
}

pub async fn exact_route(args: &[String]) {
    let workdir =
        PathBuf::from(arg(args, "--workdir").unwrap_or_else(|| "/tmp/moraine-exact".into()));
    let out = PathBuf::from(arg(args, "--out").unwrap_or_else(|| "exact.jsonl".into()));
    let minutes = arg_usize(args, "--minutes", 10) as u64;
    let rows_per_append = arg_usize(args, "--rows-per-append", 1000);
    let interval = Duration::from_millis(arg_usize(args, "--interval-ms", 100) as u64);
    let maintain_every = Duration::from_secs(arg_usize(args, "--maintain-s", 10) as u64);
    let min_rows = arg_usize(args, "--min-rows", 10_000) as u64;
    let before_commit = arg(args, "--before-commit").as_deref() != Some("off");
    let fanout = arg_usize(args, "--fanout", 4);
    let min_age = Duration::from_secs(arg_usize(args, "--min-file-age-s", 60) as u64);
    let max_steps = arg_usize(args, "--max-steps", 8);
    let loc = location("local", &workdir, "exact");
    // Threads per index build (D80); 0 keeps the library's default.
    let build_threads = arg_usize(args, "--build-threads", 0);
    let mut builder = Database::builder(&loc);
    if build_threads > 0 {
        builder = builder.build_threads(build_threads);
    }
    let db = builder.open().await.expect("open");
    let t = db
        .create_table("v", vector_schema(), TableOptions::default())
        .await
        .expect("create");
    t.create_index(
        IndexSpec::new("emb", "vec-hnsw", &["v"])
            .params(json!({"dim": DIM, "metric": "cosine"}))
            .policy(json!({"min_rows_for_ann": min_rows, "before_commit": before_commit})),
    )
    .await
    .expect("index");
    let maint = MaintainOptions::default().leases(false).compaction(Some(
        CompactionPolicy::default()
            .fanout(fanout)
            .min_file_age(min_age)
            .max_steps(max_steps),
    ));
    // The writer and the maintenance loop run concurrently, as two clients would.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w = {
        let (t, stop) = (t.clone(), stop.clone());
        tokio::spawn(async move {
            let mut rng = Rng(11);
            let mut id = 0i64;
            let mut lat = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let t0 = Instant::now();
                t.append(vec![vector_batch(&mut rng, id, rows_per_append)])
                    .await
                    .expect("append");
                lat.push(ms(t0.elapsed()));
                id += rows_per_append as i64;
                tokio::time::sleep(interval.saturating_sub(t0.elapsed())).await;
            }
            lat
        })
    };
    let m = {
        let (t, stop) = (t.clone(), stop.clone());
        tokio::spawn(async move {
            let mut passes = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(maintain_every).await;
                let t0 = Instant::now();
                let r = t.maintain(&maint).await.expect("maintain");
                let c = r.compaction.unwrap_or_default();
                passes.push(json!({
                    "s": round(t0.elapsed().as_secs_f64(), 2),
                    "steps": c.steps,
                    "removed": c.files_removed,
                    "sidecars": c.sidecars_built,
                    "yielded": c.yielded,
                }));
            }
            passes
        })
    };
    let started = Instant::now();
    let mut shares = Vec::new();
    let mut pending_max = 0f64;
    let mut rng = Rng(5);
    while started.elapsed() < Duration::from_secs(minutes * 60) {
        tokio::time::sleep(Duration::from_secs(10)).await;
        t.refresh().await.expect("refresh");
        let st = t.index_status().await.expect("status").remove(0);
        let q = vector_batch(&mut rng, 0, 1);
        let qv: Vec<f32> = {
            use arrow::array::AsArray;
            let l = q.column(1).as_list::<i32>();
            l.value(0)
                .as_primitive::<arrow::datatypes::Float32Type>()
                .values()
                .to_vec()
        };
        let r = t
            .search_vector("emb", qv)
            .limit(10)
            .collect()
            .await
            .expect("search");
        let share = st.exact_share();
        let pending = st.rows_pending() as f64 / st.rows_total.max(1) as f64;
        pending_max = pending_max.max(pending);
        shares.push(share);
        write_line(
            &out,
            &json!({
                "bench": "exact_route_sample",
                "t_s": round(started.elapsed().as_secs_f64(), 1),
                "rows": st.rows_total,
                "files": st.files_total,
                "rows_covered": st.rows_covered,
                "rows_exact_by_policy": st.rows_exact_by_policy,
                "rows_pending": st.rows_pending(),
                "exact_share": round(share, 4),
                "search_exact_rows": r.stats.exact_rows,
                "search_sidecar_files": r.stats.sidecar_files,
                "search_exact_files": r.stats.exact_files,
            }),
        );
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut lat = w.await.expect("writer");
    let passes = m.await.expect("maintenance");
    // Skip the first minute: the table is small and every row is fresh.
    let steady: Vec<f64> = shares.iter().skip(6).copied().collect();
    let mut s2 = steady.clone();
    // The second half of the run, when the table's size changes least between samples.
    let late: Vec<f64> = shares.iter().skip(shares.len() / 2).copied().collect();
    write_line(
        &out,
        &json!({
            "bench": "exact_route",
            "before_commit": before_commit,
            "minutes": minutes,
            "rows_per_append": rows_per_append,
            "interval_ms": interval.as_millis() as u64,
            "min_rows_for_ann": min_rows,
            "fanout": fanout,
            "min_file_age_s": min_age.as_secs(),
            "max_steps": max_steps,
            "build_threads": build_threads,
            "passes": passes.len(),
            "pass_detail": passes,
            "append_p50_ms": round(pct(&mut lat, 0.5), 2),
            "append_p99_ms": round(pct(&mut lat, 0.99), 2),
            "share_steady_mean": round(steady.iter().sum::<f64>() / steady.len().max(1) as f64, 4),
            "share_steady_max": round(pct(&mut s2, 1.0), 4),
            "share_late_mean": round(late.iter().sum::<f64>() / late.len().max(1) as f64, 4),
            "pending_share_max": round(pending_max, 4),
        }),
    );
    let _ = std::fs::remove_dir_all(&loc);
}
