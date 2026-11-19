//! The D24 side-by-side benchmark: moraine's usearch (`vec-hnsw`) and TurboQuant (`vec-tq`)
//! sidecars, with Lance's IVF-PQ index as the reference.
//!
//! ```text
//! moraine-bench vector-d24 [--rows 100000,1000000] [--dims 256] [--layouts 1,10,100]
//!                          [--queries 1000] [--workdir DIR] [--out results.jsonl] [--lance on]
//! ```
//!
//! Per data set (clustered synthetic vectors: 64 Gaussian clusters, seeded) the exact top 10 of
//! every query is computed by brute force once. Per layout (the rows split into 1, 10 or 100
//! files) each configuration is built in a child process (build time and peak resident
//! memory), then searched warm with 1,000 queries at each search setting (recall@10, p50, p99).
//! Cold first query and resident memory come from two more child processes per configuration:
//! one with an empty disk cache (download, open, first search) and one with the cache filled
//! but no open handle. Merge cost, determinism, filters and a half-unindexed table are measured
//! on the 10-file layout of the smallest data set. Every result is one JSON line in `--out`.

use std::collections::HashSet;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float32Builder, Int64Array, ListBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use moraine::expr::{col, lit};
use moraine::index::CacheMode;
use moraine::index::bundle::{BundleBytes, BundleView};
use moraine::index::vector::HnswKind;
use moraine::index::vector::TurboKind;
use moraine::index::{BuildCtx, BuildOptions, IndexKind, IndexSpec, MergeInput, RowMap};
use moraine::testing::BundleBuild;
use moraine::{Database, Table, TableOptions};
use serde_json::{Value, json};

use crate::{arg, dir_size, ms, pct, proc_field, round};

const CLUSTERS: usize = 64;
const SPREAD: f32 = 0.4;
const K: usize = 10;

/// A seeded generator (SplitMix64) and standard normal samples.
struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        moraine::testing::splitmix64(self.0)
    }
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
    fn normal(&mut self) -> f32 {
        let (u1, u2) = (self.unit(), self.unit());
        ((-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()) as f32
    }
}

/// `n` clustered vectors of `dim` dimensions, flat.
fn vectors(seed: u64, n: usize, dim: usize) -> Vec<f32> {
    let mut g = Gen(1);
    let centres: Vec<f32> = (0..CLUSTERS * dim).map(|_| g.normal()).collect();
    let mut g = Gen(seed);
    let mut out = Vec::with_capacity(n * dim);
    for _ in 0..n {
        let c = (g.next() % CLUSTERS as u64) as usize;
        for j in 0..dim {
            out.push(centres[c * dim + j] + SPREAD * g.normal());
        }
    }
    out
}

/// The exact top `K` row ids by cosine distance of each query, among rows `allow` admits.
fn ground_truth(
    data: &[f32],
    dim: usize,
    queries: &[f32],
    allow: &(dyn Fn(usize) -> bool + Sync),
) -> Vec<Vec<u64>> {
    let n = data.len() / dim;
    let norms: Vec<f32> = (0..n)
        .map(|i| {
            data[i * dim..(i + 1) * dim]
                .iter()
                .map(|x| x * x)
                .sum::<f32>()
                .sqrt()
        })
        .collect();
    let nq = queries.len() / dim;
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut out = vec![Vec::new(); nq];
    std::thread::scope(|s| {
        for (t, chunk) in out.chunks_mut(nq.div_ceil(threads)).enumerate() {
            let norms = &norms;
            s.spawn(move || {
                let base = t * nq.div_ceil(threads);
                for (i, slot) in chunk.iter_mut().enumerate() {
                    let q = &queries[(base + i) * dim..(base + i + 1) * dim];
                    let qn = q.iter().map(|x| x * x).sum::<f32>().sqrt();
                    let mut best: Vec<(f32, u64)> = Vec::with_capacity(K + 1);
                    for r in 0..n {
                        if !allow(r) {
                            continue;
                        }
                        let x = &data[r * dim..(r + 1) * dim];
                        let d = 1.0
                            - q.iter().zip(x).map(|(a, b)| a * b).sum::<f32>() / (qn * norms[r]);
                        if best.len() < K || d < best[K - 1].0 {
                            let at = best.partition_point(|b| b.0 <= d);
                            best.insert(at, (d, r as u64));
                            best.truncate(K);
                        }
                    }
                    *slot = best.into_iter().map(|b| b.1).collect();
                }
            });
        }
    });
    out
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sel", DataType::Int64, false),
        Field::new(
            "v",
            DataType::List(Arc::new(Field::new("element", DataType::Float32, true))),
            true,
        ),
    ]))
}

/// Rows `first..first + n` of `data` as a batch (`sel = id % 1000`, for filters).
fn batch(data: &[f32], dim: usize, first: usize, n: usize) -> RecordBatch {
    let ids: Vec<i64> = (first..first + n).map(|i| i as i64).collect();
    let sel: Vec<i64> = ids.iter().map(|i| i % 1000).collect();
    let mut b = ListBuilder::new(Float32Builder::with_capacity(n * dim))
        .with_field(Arc::new(Field::new("element", DataType::Float32, true)));
    for r in first..first + n {
        b.values().append_slice(&data[r * dim..(r + 1) * dim]);
        b.append(true);
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(sel)),
            Arc::new(b.finish()) as ArrayRef,
        ],
    )
    .expect("batch")
}

/// One index configuration: its kind, build params and the search settings tried.
#[derive(Clone)]
struct Config {
    label: String,
    kind: &'static str,
    params: Value,
    /// (search parameter, values).
    sweep: (&'static str, Vec<u64>),
}

fn configs(dim: usize) -> Vec<Config> {
    let mut v = Vec::new();
    for q in ["f32", "f16", "i8"] {
        v.push(Config {
            label: format!("usearch-{q}"),
            kind: "vec-hnsw",
            params: json!({"dim": dim, "metric": "cosine", "quantization": q, "connectivity": 16, "expansion_add": 128}),
            sweep: ("expansion_search", vec![64, 128, 256, 512]),
        });
    }
    for bits in [2, 3, 4] {
        v.push(Config {
            label: format!("turbovec-{bits}bit"),
            kind: "vec-tq",
            params: json!({"dim": dim, "metric": "cosine", "bits": bits}),
            sweep: ("rerank", vec![0, 2, 4, 8]),
        });
    }
    v
}

fn exe() -> PathBuf {
    std::env::current_exe().expect("current exe")
}

fn child_json(args: &[&str]) -> Value {
    let out = std::process::Command::new(exe())
        .arg("vector-child")
        .args(args)
        .output()
        .expect("child");
    if !out.status.success() {
        panic!(
            "vector-child {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    serde_json::from_slice(&out.stdout).expect("child json")
}

fn emit(out: &Path, mut v: Value) {
    use std::io::Write;
    v["bench"] = json!("vector");
    let line = v.to_string();
    println!("{line}");
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out)
        .expect("out");
    writeln!(f, "{line}").expect("write");
}

async fn open_db(dir: &Path, cache: CacheMode) -> Database {
    Database::builder(dir.to_str().expect("utf-8"))
        .sidecar_cache(cache)
        .small_table_bytes(0)
        .open()
        .await
        .expect("database")
}

async fn ids(
    t: &Table,
    q: &[f32],
    param: (&str, u64),
    filter: Option<moraine::expr::Expr>,
) -> (Vec<u64>, f64) {
    let mut s = t
        .search_vector("emb", q.to_vec())
        .limit(K)
        .select(["id"])
        .param(param.0, param.1);
    // `MORAINE_BENCH_POOL=0` or `1` turns the shared search pool off or on (default: moraine's
    // choice by file count).
    if let Ok(p) = std::env::var("MORAINE_BENCH_POOL")
        && let Ok(p) = p.parse::<u64>()
    {
        s = s.param("moraine.pool", p);
    }
    if let Some(f) = filter {
        s = s.filter(f);
    }
    let t0 = Instant::now();
    let r = s.collect().await.expect("search");
    let took = ms(t0.elapsed());
    let a = r
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids");
    (a.values().iter().map(|x| *x as u64).collect(), took)
}

fn recall(got: &[Vec<u64>], truth: &[Vec<u64>]) -> f64 {
    let mut hit = 0;
    let mut total = 0;
    for (g, t) in got.iter().zip(truth) {
        let t: HashSet<&u64> = t.iter().collect();
        hit += g.iter().filter(|x| t.contains(x)).count();
        total += t.len();
    }
    hit as f64 / total.max(1) as f64
}

/// Recall and latency of `qs` at one search setting (after a warm-up pass).
async fn measure(
    t: &Table,
    qs: &[Vec<f32>],
    truth: &[Vec<u64>],
    param: (&str, u64),
    filter: Option<moraine::expr::Expr>,
) -> Value {
    for q in qs.iter().take(50) {
        ids(t, q, param, filter.clone()).await;
    }
    let mut got = Vec::with_capacity(qs.len());
    let mut lat = Vec::with_capacity(qs.len());
    for q in qs {
        let (g, l) = ids(t, q, param, filter.clone()).await;
        got.push(g);
        lat.push(l);
    }
    json!({
        "param": param.0, "value": param.1,
        "recall_at_10": round(recall(&got, truth), 4),
        "p50_ms": round(pct(&mut lat.clone(), 0.5), 3),
        "p99_ms": round(pct(&mut lat, 0.99), 3),
    })
}

fn write_queries(path: &Path, qs: &[Vec<f32>]) {
    let mut b = Vec::new();
    for q in qs {
        for x in q {
            b.extend_from_slice(&x.to_le_bytes());
        }
    }
    std::fs::write(path, b).expect("queries");
}

/// `vector-d24`.
pub async fn run(args: &[String]) {
    let list = |name: &str, d: &str| -> Vec<usize> {
        arg(args, name)
            .unwrap_or_else(|| d.into())
            .split(',')
            .map(|x| x.trim().parse().expect("number"))
            .collect()
    };
    let rows_list = list("--rows", "100000,1000000");
    let dims = list("--dims", "256");
    let layouts = list("--layouts", "1,10,100");
    let nq = list("--queries", "1000")[0];
    let lance_on = arg(args, "--lance").as_deref() != Some("off");
    // `--only lance` runs the reference alone (appending to the same results).
    let only_lance = arg(args, "--only").as_deref() == Some("lance");
    // `--configs usearch-f32,...` runs those configurations only (default: all).
    let wanted: Option<Vec<String>> =
        arg(args, "--configs").map(|c| c.split(',').map(|x| x.trim().to_string()).collect());
    let work =
        PathBuf::from(arg(args, "--workdir").unwrap_or_else(|| "target/bench-vector".into()));
    let out =
        PathBuf::from(arg(args, "--out").unwrap_or_else(|| "bench/results/vector.jsonl".into()));
    std::fs::create_dir_all(&work).expect("workdir");
    let date = chrono_date();
    for &dim in &dims {
        for &rows in &rows_list {
            let set = format!("d{dim}_n{rows}");
            eprintln!("== {set}: generating and computing ground truth");
            let data = vectors(2, rows, dim);
            let qflat = vectors(3, nq, dim);
            let qs: Vec<Vec<f32>> = qflat.chunks(dim).map(<[f32]>::to_vec).collect();
            let t0 = Instant::now();
            let truth = ground_truth(&data, dim, &qflat, &|_| true);
            eprintln!("   ground truth in {:.1} s", t0.elapsed().as_secs_f64());
            let qfile = work.join(format!("{set}.queries"));
            write_queries(&qfile, &qs);
            for &files in &layouts {
                if only_lance || rows / files < 1000 {
                    continue;
                }
                let db_dir = work.join(format!("{set}_f{files}"));
                let _ = std::fs::remove_dir_all(&db_dir);
                let db = open_db(&db_dir, CacheMode::Memory { max_bytes: 8 << 30 }).await;
                let opts = TableOptions::default().target_file_bytes(16 << 30);
                let t = db.create_table("t", schema(), opts).await.expect("table");
                let per = rows / files;
                for f in 0..files {
                    let n = if f == files - 1 { rows - per * f } else { per };
                    t.append(vec![batch(&data, dim, per * f, n)])
                        .await
                        .expect("append");
                }
                let data_bytes = dir_size(&db_dir.join("t").join("data"));
                for c in configs(dim) {
                    if wanted.as_ref().is_some_and(|w| !w.contains(&c.label)) {
                        continue;
                    }
                    eprintln!("   {set} {files} files: {}", c.label);
                    let spec = IndexSpec::new("emb", c.kind, &["v"])
                        .params(c.params.clone())
                        .policy(json!({"min_rows_for_ann": 1}));
                    t.replace_index(spec).await.expect("index");
                    // Earlier configurations' sidecars stay until collection after the grace
                    // period; remove them so sizes and cold reads see this configuration only.
                    let _ = std::fs::remove_dir_all(db_dir.join("t").join("idx"));
                    let parallel = if files == 1 { 1 } else { 10 };
                    let built =
                        child_json(&["build", db_dir.to_str().unwrap(), &parallel.to_string()]);
                    let idx_bytes = dir_size(&db_dir.join("t").join("idx"));
                    // Warm search through this process's handles.
                    let fresh = open_db(&db_dir, CacheMode::Memory { max_bytes: 8 << 30 }).await;
                    let tt = fresh.open_table("t").await.expect("open");
                    let mut sweep = Vec::new();
                    for v in &c.sweep.1 {
                        sweep.push(measure(&tt, &qs, &truth, (c.sweep.0, *v), None).await);
                    }
                    // Cold: empty disk cache, then filled cache without handles.
                    let cache = work.join("cache");
                    let _ = std::fs::remove_dir_all(&cache);
                    let p = json!({c.sweep.0: c.sweep.1[1]}).to_string();
                    let cold = child_json(&[
                        "query",
                        db_dir.to_str().unwrap(),
                        cache.to_str().unwrap(),
                        qfile.to_str().unwrap(),
                        &p,
                    ]);
                    let warm_disk = child_json(&[
                        "query",
                        db_dir.to_str().unwrap(),
                        cache.to_str().unwrap(),
                        qfile.to_str().unwrap(),
                        &p,
                    ]);
                    let _ = std::fs::remove_dir_all(&cache);
                    emit(
                        &out,
                        json!({
                            "date": date, "set": set, "dim": dim, "rows": rows, "files": files,
                            "config": c.label, "kind": c.kind, "params": c.params,
                            "data_bytes": data_bytes,
                            "sidecar_bytes": idx_bytes,
                            "sidecar_bytes_per_vector": round(idx_bytes as f64 / rows as f64, 1),
                            "build": built,
                            "sweep": sweep,
                            "cold": cold, "warm_disk": warm_disk,
                        }),
                    );
                }
                if files == 10 && rows == rows_list[0] {
                    extras(&out, &date, &set, &db_dir, &t, &data, dim, &qs).await;
                }
            }
            if lance_on {
                lance_ref(&out, &date, &set, &work, &data, dim, &qs, &truth).await;
            }
        }
    }
}

fn chrono_date() -> String {
    let out = std::process::Command::new("date")
        .arg("-u")
        .arg("+%Y-%m-%d")
        .output();
    out.map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

/// Merge cost, determinism, filters and a half-unindexed table (10-file layout).
#[allow(clippy::too_many_arguments)]
async fn extras(
    out: &Path,
    date: &str,
    set: &str,
    db_dir: &Path,
    t: &Table,
    data: &[f32],
    dim: usize,
    qs: &[Vec<f32>],
) {
    let rows = data.len() / dim;
    let per = rows / 10;
    let sch = schema();
    let qflat: Vec<f32> = qs.iter().flatten().copied().collect();
    for (kind, params, param) in [
        (
            &HnswKind as &dyn IndexKind,
            json!({"dim": dim, "metric": "cosine"}),
            ("expansion_search", 128u64),
        ),
        (
            &TurboKind as &dyn IndexKind,
            json!({"dim": dim, "metric": "cosine", "bits": 4}),
            ("rerank", 4u64),
        ),
    ] {
        let spec = kind
            .resolve(
                &IndexSpec::new("emb", kind.name(), &["v"]).params(params.clone()),
                &sch,
            )
            .expect("spec");
        let build = |first: usize, n: usize| -> (Vec<u8>, f64) {
            let b = batch(data, dim, first, n).project(&[2]).expect("v");
            let t0 = Instant::now();
            let mut w =
                BundleBuild::new(kind, &spec, n as u64, 1, 256 << 20, Cursor::new(Vec::new()))
                    .expect("build");
            let mut at = 0;
            while at < n {
                let m = 1024.min(n - at);
                w.add(at as u64, &b.slice(at, m)).expect("add");
                at += m;
            }
            let bytes = w.finish().expect("finish").0.into_inner();
            (bytes, ms(t0.elapsed()))
        };
        // Determinism: the same rows twice.
        let (a, _) = build(0, per);
        let (b, _) = build(0, per);
        // Merge of two files: codes concatenated (turbovec) or a rebuild from floats (usearch).
        let (b2, _) = build(per, per);
        let t0 = Instant::now();
        let merged = {
            let (va, vb) = (
                BundleView::parse(BundleBytes::copy(&a).unwrap()).unwrap(),
                BundleView::parse(BundleBytes::copy(&b2).unwrap()).unwrap(),
            );
            let (ma, mb) = (
                RowMap {
                    new: (0..per as u64).map(Some).collect(),
                },
                RowMap {
                    new: (per as u64..2 * per as u64).map(Some).collect(),
                },
            );
            let inputs = [MergeInput::new(&va, &ma), MergeInput::new(&vb, &mb)];
            let ctx = BuildCtx::new(2 * per as u64, 256 << 20, 1);
            kind.merge(&spec, &inputs, &ctx).expect("merge")
        };
        let (merge_ms, read_bytes, how) = match merged {
            Some(builder) => {
                BundleBuild::from_builder(
                    kind,
                    &spec,
                    builder,
                    2 * per as u64,
                    1,
                    Cursor::new(Vec::new()),
                )
                .expect("merge build")
                .finish()
                .expect("finish");
                (
                    ms(t0.elapsed()),
                    (a.len() + b2.len()) as u64,
                    "concatenate codes",
                )
            }
            None => {
                let (_, took) = build(0, 2 * per);
                (took, (2 * per * dim * 4) as u64, "rebuild from floats")
            }
        };
        emit(
            out,
            json!({
                "date": date, "set": set, "files": 10, "kind": kind.name(), "extra": "merge_determinism",
                "byte_identical_rebuild": a == b,
                "merge": {"rows": 2 * per, "ms": round(merge_ms, 1), "bytes_read": read_bytes, "how": how},
            }),
        );
        // Filters and a half-unindexed table, on the table's own sidecars for this kind.
        t.replace_index(
            IndexSpec::new("emb", kind.name(), &["v"])
                .params(params.clone())
                .policy(json!({"min_rows_for_ann": 1})),
        )
        .await
        .expect("index");
        let _ = std::fs::remove_dir_all(db_dir.join("t").join("idx"));
        child_json(&["build", db_dir.to_str().unwrap(), "10"]);
        let fresh = open_db(db_dir, CacheMode::Memory { max_bytes: 8 << 30 }).await;
        let tt = fresh.open_table("t").await.expect("open");
        let mut filtered = Vec::new();
        for (label, below) in [("50%", 500i64), ("5%", 50), ("0.5%", 5)] {
            let truth = ground_truth(data, dim, &qflat, &|r| (r as i64 % 1000) < below);
            let m = measure(
                &tt,
                &qs[..qs.len().min(300)],
                &truth[..qs.len().min(300)],
                param,
                Some(col("sel").lt(lit(below))),
            )
            .await;
            filtered.push(json!({"selectivity": label, "result": m}));
        }
        // Half the sidecars removed: half the files go exact, mixed with the others.
        let truth = ground_truth(data, dim, &qflat, &|_| true);
        let idx = db_dir.join("t").join("idx");
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(&idx)
            .expect("idx")
            .flatten()
            .map(|e| e.path())
            .collect();
        dirs.sort();
        for d in dirs.iter().step_by(2) {
            let _ = std::fs::remove_dir_all(d);
        }
        let half = open_db(db_dir, CacheMode::Memory { max_bytes: 8 << 30 }).await;
        let th = half.open_table("t").await.expect("open");
        let mixed = measure(
            &th,
            &qs[..qs.len().min(300)],
            &truth[..qs.len().min(300)],
            param,
            None,
        )
        .await;
        emit(
            out,
            json!({
                "date": date, "set": set, "files": 10, "kind": kind.name(), "extra": "filters_mixed",
                "filtered": filtered, "half_unindexed": mixed,
            }),
        );
    }
}

/// Lance's IVF-PQ index through the `lance` crate kv0 builds, on the same rows and queries.
#[allow(clippy::too_many_arguments)]
async fn lance_ref(
    out: &Path,
    date: &str,
    set: &str,
    work: &Path,
    data: &[f32],
    dim: usize,
    qs: &[Vec<f32>],
    truth: &[Vec<u64>],
) {
    use futures::TryStreamExt;
    use lance::deps::arrow_array::{
        self as la, Array as _, FixedSizeListArray, Float32Array, Int64Array as LInt64,
        RecordBatchIterator,
    };
    use lance::deps::arrow_schema as ls;
    use lance::index::DatasetIndexExt;
    let rows = data.len() / dim;
    let uri = work.join(format!("lance_{set}"));
    let _ = std::fs::remove_dir_all(&uri);
    let schema = Arc::new(ls::Schema::new(vec![
        ls::Field::new("id", ls::DataType::Int64, false),
        ls::Field::new(
            "v",
            ls::DataType::FixedSizeList(
                Arc::new(ls::Field::new("item", ls::DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ),
    ]));
    let mut batches = Vec::new();
    for start in (0..rows).step_by(100_000) {
        let n = 100_000.min(rows - start);
        let values = Float32Array::from(data[start * dim..(start + n) * dim].to_vec());
        let v = FixedSizeListArray::try_new(
            Arc::new(ls::Field::new("item", ls::DataType::Float32, true)),
            dim as i32,
            Arc::new(values),
            None,
        )
        .expect("fsl");
        let ids = LInt64::from_iter_values((start..start + n).map(|i| i as i64));
        batches.push(Ok(la::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(v)],
        )
        .expect("batch")));
    }
    let reader = RecordBatchIterator::new(batches, schema.clone());
    let mut ds = lance::Dataset::write(reader, uri.to_str().unwrap(), None)
        .await
        .expect("lance write");
    let partitions = ((rows as f64).sqrt() as usize).max(16);
    // Lance's defaults use dim / 16 sub-vectors; dim / 8 and dim / 4 trade size for accuracy.
    for sub in [dim / 8, dim / 4] {
        let params = lance::index::vector::VectorIndexParams::ivf_pq(
            partitions,
            8,
            sub,
            lance_linalg::distance::MetricType::Cosine,
            50,
        );
        let t0 = Instant::now();
        ds.create_index(
            &["v"],
            lance_index::IndexType::Vector,
            Some("emb".into()),
            &params,
            true,
        )
        .await
        .expect("lance index");
        let build_ms = ms(t0.elapsed());
        // The newest index directory: a replaced index stays on disk until cleanup.
        let index_bytes = std::fs::read_dir(uri.join("_indices"))
            .expect("indices")
            .flatten()
            .max_by_key(|e| e.metadata().and_then(|m| m.modified()).ok())
            .map_or(0, |e| dir_size(&e.path()));
        let mut sweep = Vec::new();
        for (nprobes, refine) in [
            (20, None),
            (20, Some(10u32)),
            (20, Some(30)),
            (40, Some(50)),
            (80, Some(100)),
        ] {
            let search = |q: &[f32]| {
                let ds = ds.clone();
                let q = Float32Array::from(q.to_vec());
                async move {
                    let mut s = ds.scan();
                    s.nearest("v", &q, K).expect("nearest").nprobes(nprobes);
                    if let Some(r) = refine {
                        s.refine(r);
                    }
                    s.project(&["id"]).expect("project");
                    let t0 = Instant::now();
                    let b: Vec<la::RecordBatch> = s
                        .try_into_stream()
                        .await
                        .expect("scan")
                        .try_collect()
                        .await
                        .expect("rows");
                    let took = ms(t0.elapsed());
                    let ids: Vec<u64> = b
                        .iter()
                        .flat_map(|b| {
                            let a = b.column_by_name("id").expect("id");
                            let a = a.as_any().downcast_ref::<LInt64>().expect("ids");
                            a.values().iter().map(|x| *x as u64).collect::<Vec<_>>()
                        })
                        .collect();
                    (ids, took)
                }
            };
            for q in qs.iter().take(50) {
                search(q).await;
            }
            let mut got = Vec::new();
            let mut lat = Vec::new();
            for q in qs {
                let (g, l) = search(q).await;
                got.push(g);
                lat.push(l);
            }
            sweep.push(json!({
                "nprobes": nprobes, "refine": refine,
                "recall_at_10": round(recall(&got, truth), 4),
                "p50_ms": round(pct(&mut lat.clone(), 0.5), 3),
                "p99_ms": round(pct(&mut lat, 0.99), 3),
            }));
        }
        emit(
            out,
            json!({
                "date": date, "set": set, "dim": dim, "rows": rows, "files": 1, "config": format!("lance-ivf-pq-sv{sub}"),
                "kind": "lance", "build": {"ms": round(build_ms, 0), "partitions": partitions, "sub_vectors": sub},
                "sidecar_bytes": index_bytes, "sweep": sweep,
            }),
        );
    }
    let _ = std::fs::remove_dir_all(&uri);
}

fn rss() -> f64 {
    proc_field("/proc/self/status", "VmRSS:").map_or(0.0, |kb| kb / 1024.0)
}

fn hwm() -> f64 {
    proc_field("/proc/self/status", "VmHWM:").map_or(0.0, |kb| kb / 1024.0)
}

/// `vector-child build <db> <parallelism>` and `vector-child query <db> <cache> <queries> <params>`.
pub async fn child(args: &[String]) -> String {
    match args.first().map(String::as_str) {
        Some("build") => {
            let db = open_db(
                Path::new(&args[1]),
                CacheMode::Memory { max_bytes: 1 << 30 },
            )
            .await;
            let t = db.open_table("t").await.expect("open");
            let parallelism: usize = args[2].parse().expect("parallelism");
            let before = hwm();
            let t0 = Instant::now();
            let r = t
                .build_indexes(BuildOptions::default().parallelism(parallelism))
                .await
                .expect("build");
            let took = ms(t0.elapsed());
            json!({
                "ms": round(took, 0), "files": r.built, "parallelism": parallelism,
                "ms_per_file_thread": round(took * parallelism.min(r.built.max(1)) as f64 / r.built.max(1) as f64, 1),
                "parts": r.parts, "peak_rss_mb": round(hwm(), 1), "rss_before_mb": round(before, 1),
            })
            .to_string()
        }
        Some("query") => {
            let cache = PathBuf::from(&args[2]);
            let db = Database::builder(&args[1])
                .sidecar_cache(CacheMode::Disk {
                    dir: cache,
                    max_bytes: 64 << 30,
                })
                .small_table_bytes(0)
                .open()
                .await
                .expect("database");
            let t = db.open_table("t").await.expect("open");
            let raw = std::fs::read(&args[3]).expect("queries");
            let q: Vec<f32> = raw
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
            let params: Value = serde_json::from_str(&args[4]).expect("params");
            let (key, value) = params
                .as_object()
                .and_then(|m| m.iter().next())
                .map(|(k, v)| (k.clone(), v.clone()))
                .expect("one param");
            let dim = t.indexes().expect("specs")[0].params["dim"]
                .as_u64()
                .expect("dim") as usize;
            let rss0 = rss();
            let t0 = Instant::now();
            let r = t
                .search_vector("emb", q[..dim].to_vec())
                .limit(K)
                .select(["id"])
                .param(&key, value.clone())
                .collect()
                .await
                .expect("search");
            let first = ms(t0.elapsed());
            let rss1 = rss();
            let t1 = Instant::now();
            t.search_vector("emb", q[dim..2 * dim].to_vec())
                .limit(K)
                .select(["id"])
                .param(&key, value)
                .collect()
                .await
                .expect("search");
            let second = ms(t1.elapsed());
            json!({
                "first_query_ms": round(first, 2), "second_query_ms": round(second, 2),
                "sidecar_files": r.stats.sidecar_files, "exact_files": r.stats.exact_files,
                "resident_after_first_mb": round(rss1 - rss0, 1),
            })
            .to_string()
        }
        _ => "{\"error\":\"usage\"}".into(),
    }
}
