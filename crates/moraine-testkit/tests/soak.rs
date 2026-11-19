//! Soak test with a counting global allocator (D29, D45): a long loop of appends,
//! deletes, index builds, vector, full-text and hybrid searches, scans and background
//! refreshes, with every cache set small. Every cache must stay within its limit after every
//! round, and the heap must stop growing once the caches are full: the bytes live at the end
//! of each later round stay within a fixed margin of the most seen during the warm-up.
//!
//! Longer: `MORAINE_SOAK_ROUNDS=<n>` (default 240).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use moraine::expr::{col, lit};
use moraine::index::BuildOptions;
use moraine::index::CacheMode;
use moraine::{CacheUsage, Consistency, Database, IndexSpec, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::text::{document, word};
use moraine_testkit::vectors::{centres, clustered, list_type};
use serde_json::json;

/// Counts the bytes allocated and not yet freed.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call is forwarded to the system allocator unchanged; the counter only adds
// and subtracts the sizes the caller passed.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            LIVE.fetch_add(new, Ordering::Relaxed);
            LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const DIM: usize = 16;
const FILE_CACHE: usize = 4 << 20;
const SIDECARS: u64 = 16 << 20;
const OPEN: (usize, usize) = (16, 8 << 20);
const FTS: usize = 8 << 20;
/// Heap the process may hold beyond its warm-up peak once the caches are full.
const MARGIN: usize = 24 << 20;

fn schema() -> Arc<Schema> {
    let mut f: Vec<Field> = moraine_testkit::text::schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    f.push(Field::new("v", list_type(), true));
    Arc::new(Schema::new(f))
}

fn rows(rng: &mut SimRng, c: &[Vec<f32>], first: i64, n: usize) -> RecordBatch {
    let docs: Vec<_> = (0..n).map(|_| document(rng, 60, 30)).collect();
    let text = moraine_testkit::text::batch(first, &docs);
    let v: Vec<Option<Vec<f32>>> = clustered(rng, c, n, 0.4).into_iter().map(Some).collect();
    let vb = moraine_testkit::vectors::batch(first, &v);
    let mut cols: Vec<ArrayRef> = text.columns().to_vec();
    cols.push(vb.column_by_name("v").unwrap().clone());
    RecordBatch::try_new(schema(), cols).unwrap()
}

fn check_limits(u: &CacheUsage) {
    assert!(u.file_bytes <= FILE_CACHE, "file cache: {u:?}");
    assert!(u.sidecar_memory_bytes <= SIDECARS, "sidecars: {u:?}");
    assert!(u.open_sidecars <= OPEN.0, "open sidecars: {u:?}");
    assert!(u.fts_bytes <= FTS, "in-memory text indexes: {u:?}");
    assert!(u.filter_bitmaps <= 4096, "filter bitmaps: {u:?}");
}

#[test]
fn caches_stay_bounded() {
    let rounds: usize = std::env::var("MORAINE_SOAK_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(240);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(soak(rounds));
}

async fn soak(rounds: usize) {
    // A local directory: the store's bytes are on disk, so the heap holds only moraine's state.
    let dir = tempfile::tempdir().unwrap();
    let open = |c: Consistency| {
        Database::builder(dir.path().to_str().unwrap())
            .consistency(c)
            .file_cache_bytes(FILE_CACHE)
            .sidecar_cache(CacheMode::Memory {
                max_bytes: SIDECARS,
            })
            .max_open_sidecars(OPEN.0, OPEN.1)
            .fts_memory(FTS)
            .small_table_bytes(1 << 20)
            .sidecar_poll_interval(Duration::from_millis(50))
            .open()
    };
    let w = open(Consistency::Strong).await.unwrap();
    let tw = w
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let c = centres(5, 8, DIM);
    let mut rng = SimRng::new(17);
    tw.append(vec![rows(&mut rng, &c, 0, 400)]).await.unwrap();
    tw.create_index(
        IndexSpec::new("emb", "vec-hnsw", &["v"])
            .params(json!({"dim": DIM}))
            .policy(json!({"min_rows_for_ann": 300})),
    )
    .await
    .unwrap();
    tw.create_index(
        IndexSpec::new("fts", "fts", &["title", "body"])
            .policy(json!({"min_rows_for_sidecar": 300})),
    )
    .await
    .unwrap();
    let r = open(Consistency::Background {
        interval: Duration::from_millis(10),
    })
    .await
    .unwrap();
    let tr = r.open_table("t").await.unwrap();
    // A text search first: background prewarm builds in-memory text indexes only once the
    // table has been searched by text (D53), and the warm-up should see that work too.
    tr.search_text("fts", "warm")
        .limit(1)
        .collect()
        .await
        .unwrap();

    let warmup = rounds / 4;
    let mut warm_peak = 0usize;
    let mut late_max = 0usize;
    let mut samples: Vec<usize> = Vec::with_capacity(rounds);
    let mut id = 400i64;
    for round in 0..rounds {
        // Writes: a file per round, small or large; old rows deleted now and then, so the
        // table (and its file list) stays about the same size.
        let n = if round % 3 == 0 { 400 } else { 60 };
        tw.append(vec![rows(&mut rng, &c, id, n)]).await.unwrap();
        id += n as i64;
        if round % 8 == 7 {
            tw.delete(col("id").lt(lit(id - 3_000))).await.unwrap();
        }
        if round % 4 == 1 {
            tw.build_indexes(BuildOptions::default()).await.unwrap();
        }
        // Reads at whatever version the background task has reached.
        let q = clustered(&mut rng, &c, 1, 0.4).remove(0);
        let text = format!("{} {}", word(&mut rng, 60), word(&mut rng, 60));
        let g = rng.below(7) as i64;
        tr.search_vector("emb", q.clone())
            .limit(10)
            .filter(col("g").lt_eq(lit(g)))
            .collect()
            .await
            .unwrap();
        tr.search_text("fts", &text)
            .limit(10)
            .collect()
            .await
            .unwrap();
        tr.search_hybrid("emb", q, "fts", &text)
            .limit(10)
            .select(["id"])
            .collect()
            .await
            .unwrap();
        tr.scan()
            .filter(col("g").eq(lit(g)))
            .select(["id", "title"])
            .collect()
            .await
            .unwrap();
        if round % 5 == 0 {
            tr.refresh().await.unwrap();
        }
        check_limits(&r.cache_usage());
        check_limits(&w.cache_usage());
        let live = LIVE.load(Ordering::Relaxed);
        samples.push(live);
        if round < warmup {
            warm_peak = warm_peak.max(live);
        } else {
            late_max = late_max.max(live);
        }
    }
    let q = |f: usize| samples[(samples.len() - 1) * f / 4] >> 20;
    println!(
        "RESULT soak rounds={rounds} rows_written={id} live_mib(q0..q4)={},{},{},{},{} \
         warm_peak_mib={} late_max_mib={} refreshes={} new_versions={} usage={:?}",
        q(0),
        q(1),
        q(2),
        q(3),
        q(4),
        warm_peak >> 20,
        late_max >> 20,
        tr.refresh_metrics().refreshes.load(Ordering::Relaxed),
        tr.refresh_metrics().new_versions.load(Ordering::Relaxed),
        r.cache_usage(),
    );
    assert!(
        late_max <= warm_peak + MARGIN,
        "the heap kept growing: warm-up peak {} MiB, later {} MiB",
        warm_peak >> 20,
        late_max >> 20
    );
}
