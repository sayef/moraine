//! Memory-bounded index builds (D29, D35): a build over a file larger than
//! its budget stays under it (measured in a child process, native memory included), a
//! multi-part sidecar answers like a single-part one, and concurrent builds never reserve more
//! than the database's build memory.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use moraine::index::BuildOptions;
use moraine::index::CacheMode;
use moraine::{Database, IndexSpec, Table, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::vectors::{batch, centres, clustered, schema};
use object_store::memory::InMemory;
use serde_json::json;

/// Builds the peak test's table: one file of `rows` vectors of `dim` dimensions.
async fn big_table(
    dir: &std::path::Path,
    rows: usize,
    dim: usize,
    kind: &str,
    params: serde_json::Value,
) {
    let db = Database::builder(dir.to_str().unwrap())
        .open()
        .await
        .unwrap();
    let opts = TableOptions::default().target_file_bytes(4 << 30);
    let t = db.create_table("t", schema(), opts).await.unwrap();
    let c = centres(3, 64, dim);
    let mut rng = SimRng::new(4);
    let mut batches = Vec::new();
    for i in 0..rows.div_ceil(20_000) {
        let n = (rows - i * 20_000).min(20_000);
        let v: Vec<Option<Vec<f32>>> = clustered(&mut rng, &c, n, 0.4)
            .into_iter()
            .map(Some)
            .collect();
        batches.push(batch((i * 20_000) as i64, &v));
    }
    t.append(batches).await.unwrap();
    assert_eq!(t.snapshot().await.unwrap().num_files(), 1);
    t.create_index(IndexSpec::new("emb", kind, &["v"]).params(params))
        .await
        .unwrap();
}

fn child(dir: &std::path::Path, budget_mib: usize) -> serde_json::Value {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_build_peak"))
        .args([dir.to_str().unwrap(), "t", &budget_mib.to_string()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

/// The acceptance test of D35: a 300,000-row, 256-dimension file (about
/// 300 MiB of floats) built under a 64 MiB budget peaks below 64 MiB plus a fixed 24 MiB
/// allowance for the runtime and the reader. `MORAINE_PEAK_ROWS` shrinks it for quick runs.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_over_budget_stays_under_it() {
    peak("vec-hnsw").await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbovec_build_over_budget_stays_under_it() {
    peak("vec-tq").await;
}

#[cfg(target_os = "linux")]
async fn peak(kind: &str) {
    let rows: usize = std::env::var("MORAINE_PEAK_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300_000);
    let budget = 64usize;
    let dir = tempfile::tempdir().unwrap();
    big_table(
        dir.path(),
        rows,
        256,
        kind,
        json!({"dim": 256, "metric": "cosine"}),
    )
    .await;
    let r = child(dir.path(), budget);
    let before = r["hwm_before"].as_u64().unwrap();
    let after = r["hwm_after"].as_u64().unwrap();
    let grew = after.saturating_sub(before);
    println!(
        "{kind} build of {rows} x 256: peak RSS {} MiB (was {} MiB before the build, grew {} MiB), {} parts, builder peak {} MiB",
        after >> 20,
        before >> 20,
        grew >> 20,
        r["parts"],
        r["peak_builder_bytes"].as_u64().unwrap() >> 20
    );
    assert_eq!(r["built"], 1);
    assert_eq!(r["kinds"], json!([kind]));
    assert!(
        r["parts"].as_u64().unwrap() > 1,
        "the file did not need splitting"
    );
    assert!(
        grew <= ((budget + 24) << 20) as u64,
        "the build grew the process by {} MiB, more than {budget} MiB + 24 MiB",
        grew >> 20
    );
}

async fn mem_db(
    store: &Arc<InMemory>,
    per_build: usize,
    total: usize,
    cache: CacheMode,
) -> Database {
    mem_db_threads(store, per_build, total, cache, 1).await
}

async fn mem_db_threads(
    store: &Arc<InMemory>,
    per_build: usize,
    total: usize,
    cache: CacheMode,
    threads: usize,
) -> Database {
    Database::builder_for_store(store.clone(), "db")
        .build_memory(per_build, total)
        .build_threads(threads)
        .sidecar_cache(cache)
        .sidecar_poll_interval(std::time::Duration::ZERO)
        .open()
        .await
        .unwrap()
}

type Key = (String, u64);

async fn top(t: &Table, q: &[f32]) -> Vec<Key> {
    let r = t
        .search_vector("emb", q.to_vec())
        .limit(10)
        .select(["id"])
        .collect()
        .await
        .unwrap();
    r.hits.into_iter().map(|h| (h.file, h.offset)).collect()
}

/// A sidecar split into parts answers like one built in a single part, and a graph built on
/// several threads like one built on one (D80), all within the recall bound against the exact
/// route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_part_search_equals_single_part() {
    let mut recalls = Vec::new();
    for (per_build, threads) in [
        (1usize << 20, 1),
        (1 << 20, 8),
        (256 << 20, 1),
        (256 << 20, 8),
    ] {
        let store = Arc::new(InMemory::new());
        let db = mem_db_threads(
            &store,
            per_build,
            per_build,
            CacheMode::Memory { max_bytes: 1 << 30 },
            threads,
        )
        .await;
        let t = db
            .create_table("t", schema(), TableOptions::default())
            .await
            .unwrap();
        let c = centres(5, 16, 32);
        let v: Vec<Option<Vec<f32>>> = clustered(&mut SimRng::new(6), &c, 20_000, 0.4)
            .into_iter()
            .map(Some)
            .collect();
        t.append(vec![batch(0, &v)]).await.unwrap();
        t.create_index(IndexSpec::new("emb", "vec-hnsw", &["v"]).params(json!({"dim": 32})))
            .await
            .unwrap();
        let r = t.build_indexes(BuildOptions::default()).await.unwrap();
        let exact = mem_db(&store, per_build, per_build, CacheMode::Disabled)
            .await
            .open_table("t")
            .await
            .unwrap();
        let qs = clustered(&mut SimRng::new(7), &c, 30, 0.4);
        let (mut found, mut total) = (0, 0);
        for q in &qs {
            let truth: HashSet<Key> = top(&exact, q).await.into_iter().collect();
            let got: HashSet<Key> = top(&t, q).await.into_iter().collect();
            found += truth.intersection(&got).count();
            total += truth.len();
        }
        let recall = found as f64 / total as f64;
        println!(
            "budget {} MiB, {threads} threads: {} parts, recall@10 {recall:.3}",
            per_build >> 20,
            r.parts
        );
        if per_build == 1 << 20 {
            assert!(r.parts > 5, "{r:?}");
        } else {
            assert_eq!(r.parts, 1);
        }
        recalls.push(recall);
    }
    assert!(recalls.iter().all(|r| *r >= 0.95), "{recalls:?}");
}

/// Builds running in parallel reserve their budgets from one semaphore: the reserved total
/// never exceeds the database's build memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn semaphore_never_oversubscribed() {
    let store = Arc::new(InMemory::new());
    let per = 4usize << 20;
    let db = mem_db(
        &store,
        per,
        2 * per,
        CacheMode::Memory { max_bytes: 1 << 30 },
    )
    .await;
    let t = db
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let c = centres(8, 8, 16);
    let mut rng = SimRng::new(9);
    for f in 0..8 {
        let v: Vec<Option<Vec<f32>>> = clustered(&mut rng, &c, 1_000, 0.4)
            .into_iter()
            .map(Some)
            .collect();
        t.append(vec![batch(f * 1_000, &v)]).await.unwrap();
    }
    t.create_index(
        IndexSpec::new("emb", "vec-hnsw", &["v"])
            .params(json!({"dim": 16}))
            .policy(json!({"min_rows_for_ann": 10})),
    )
    .await
    .unwrap();
    let r = t
        .build_indexes(BuildOptions::default().parallelism(8))
        .await
        .unwrap();
    assert_eq!(r.built, 8);
    let peak = db.index_metrics().build_memory_peak.load(Ordering::SeqCst);
    assert!(peak > per as u64, "builds never overlapped: {peak}");
    assert!(
        peak <= 2 * per as u64,
        "reserved {peak} bytes of {}",
        2 * per
    );
}
