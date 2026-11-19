//! Spike item 10: usearch can save an index, memory-map it (`view`), search with a predicate
//! (`filtered_search`) and use `u64` keys (row offsets). Also records build time, file size,
//! recall at 10 against exact search, and whether usearch's cosine distance equals
//! `1 - cosine similarity` computed by hand (the design's exact route must match it).

use std::time::Instant;

use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

const N: usize = 20_000;
const DIM: usize = 128;
const QUERIES: usize = 100;

fn vectors(n: usize, seed: u64) -> Vec<Vec<f32>> {
    // Clustered synthetic data: 50 centres plus noise.
    let mut x = seed | 1;
    let mut rnd = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        (x % 1_000_000) as f32 / 1_000_000.0 - 0.5
    };
    let centres: Vec<Vec<f32>> = (0..50).map(|_| (0..DIM).map(|_| rnd()).collect()).collect();
    (0..n)
        .map(|i| centres[i % 50].iter().map(|c| c + 0.3 * rnd()).collect())
        .collect()
}

fn cos_dist(a: &[f32], b: &[f32]) -> f32 {
    let (mut d, mut na, mut nb) = (0f64, 0f64, 0f64);
    for (x, y) in a.iter().zip(b) {
        d += (*x as f64) * (*y as f64);
        na += (*x as f64) * (*x as f64);
        nb += (*y as f64) * (*y as f64);
    }
    (1.0 - d / (na.sqrt() * nb.sqrt())) as f32
}

fn exact(data: &[Vec<f32>], q: &[f32], k: usize, allow: impl Fn(u64) -> bool) -> Vec<(u64, f32)> {
    let mut all: Vec<(u64, f32)> = data
        .iter()
        .enumerate()
        .filter(|(i, _)| allow(*i as u64))
        .map(|(i, v)| (i as u64, cos_dist(q, v)))
        .collect();
    all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
    all.truncate(k);
    all
}

fn options(q: ScalarKind) -> IndexOptions {
    IndexOptions {
        dimensions: DIM,
        metric: MetricKind::Cos,
        quantization: q,
        connectivity: 16,
        expansion_add: 128,
        expansion_search: 64,
        multi: false,
    }
}

#[test]
fn item10_usearch_view_filter_u64() {
    let data = vectors(N, 7);
    let queries = vectors(QUERIES, 99);
    let dir = tempfile::tempdir().unwrap();

    let mut sizes = vec![];
    for q in [ScalarKind::F32, ScalarKind::F16, ScalarKind::I8] {
        let index = Index::new(&options(q)).unwrap();
        index.reserve(N).unwrap();
        let t0 = Instant::now();
        for (i, v) in data.iter().enumerate() {
            index.add(i as u64, v).unwrap();
        }
        let build_ms = t0.elapsed().as_secs_f64() * 1e3;
        let path = dir.path().join(format!("{q:?}.usearch"));
        index.save(path.to_str().unwrap()).unwrap();
        sizes.push((
            format!("{q:?}"),
            std::fs::metadata(&path).unwrap().len(),
            build_ms,
        ));
    }
    println!(
        "RESULT item10 n={N} dim={DIM} build_and_size(quant, bytes, build_ms)={sizes:?} raw_f32_bytes={}",
        N * DIM * 4
    );

    // View the F32 file without loading it.
    let path = dir.path().join("F32.usearch");
    let viewed = Index::new(&options(ScalarKind::F32)).unwrap();
    viewed.view(path.to_str().unwrap()).unwrap();
    let mem_view = viewed.memory_usage();
    let loaded = Index::new(&options(ScalarKind::F32)).unwrap();
    loaded.load(path.to_str().unwrap()).unwrap();
    let mem_loaded = loaded.memory_usage();

    // Recall at 10 and distance agreement, unfiltered. Search time excludes the exact baseline.
    let truths: Vec<Vec<(u64, f32)>> = queries
        .iter()
        .map(|q| exact(&data, q, 10, |_| true))
        .collect();
    let recall_of = |index: &Index| {
        let (mut hit, mut total, mut max_diff, mut secs) = (0usize, 0usize, 0f32, 0f64);
        for (q, truth) in queries.iter().zip(&truths) {
            let t0 = Instant::now();
            let m = index.search(q, 10).unwrap();
            secs += t0.elapsed().as_secs_f64();
            let tset: std::collections::HashSet<u64> = truth.iter().map(|t| t.0).collect();
            hit += m.keys.iter().filter(|k| tset.contains(k)).count();
            total += 10;
            for (k, d) in m.keys.iter().zip(&m.distances) {
                max_diff = max_diff.max((cos_dist(q, &data[*k as usize]) - d).abs());
            }
        }
        (
            hit as f64 / total as f64,
            secs * 1e6 / QUERIES as f64,
            max_diff,
        )
    };
    let (recall, search_us, max_dist_diff) = recall_of(&viewed);
    let (recall_loaded, _, _) = recall_of(&loaded);
    let mut by_expansion = vec![];
    for e in [128usize, 256, 512] {
        viewed.change_expansion_search(e);
        let (r, us, _) = recall_of(&viewed);
        by_expansion.push(format!("ef={e}: recall={r:.3} us={us:.0}"));
    }
    viewed.change_expansion_search(64);
    // Ties: how many exact top-10 lists have a distance gap under 1e-6 at the 10th place.
    let near_ties = queries
        .iter()
        .filter(|q| {
            let t = exact(&data, q, 11, |_| true);
            (t[10].1 - t[9].1).abs() < 1e-6
        })
        .count();

    // Filtered: only every tenth row is allowed (10% selectivity), then 1%.
    let mut filtered = vec![];
    for modulo in [10u64, 100] {
        let (mut fhit, mut ftotal, mut violations, mut short) = (0, 0, 0, 0);
        for q in &queries {
            let m = viewed.filtered_search(q, 10, |k| k % modulo == 0).unwrap();
            violations += m.keys.iter().filter(|k| *k % modulo != 0).count();
            if m.keys.len() < 10 {
                short += 1;
            }
            let truth = exact(&data, q, 10, |k| k % modulo == 0);
            let tset: std::collections::HashSet<u64> = truth.iter().map(|t| t.0).collect();
            fhit += m.keys.iter().filter(|k| tset.contains(k)).count();
            ftotal += 10;
        }
        filtered.push(format!(
            "1/{modulo}: recall={:.3} predicate_violations={violations} short_results={short}",
            fhit as f64 / ftotal as f64
        ));
    }

    // u64 keys beyond u32 and close to u64::MAX.
    let big = Index::new(&options(ScalarKind::F32)).unwrap();
    big.reserve(3).unwrap();
    let keys = [u64::MAX - 1, 1u64 << 40, 5];
    for (k, v) in keys.iter().zip(&data) {
        big.add(*k, v).unwrap();
    }
    let got = big.search(&data[0], 3).unwrap().keys;

    // View from a buffer at a 4,096-byte offset inside a larger "bundle", as the sidecar format
    // would hold it.
    let raw = std::fs::read(&path).unwrap();
    let mut bundle = vec![0u8; 4096];
    bundle.extend_from_slice(&raw);
    let from_buf = Index::new(&options(ScalarKind::F32)).unwrap();
    // SAFETY: `bundle` outlives `from_buf`'s use below and is not modified.
    let buf_ok = unsafe { from_buf.view_from_buffer(&bundle[4096..]) }.is_ok();
    let buf_search_ok = buf_ok
        && from_buf
            .search(&queries[0], 10)
            .map(|m| m.keys == viewed.search(&queries[0], 10).unwrap().keys)
            .unwrap_or(false);
    let ptr_align = (bundle[4096..].as_ptr() as usize) % 64;

    println!(
        "RESULT item10 view: memory_usage view={mem_view} load={mem_loaded}; recall@10={recall:.3} \
         search_us_per_query={search_us:.0} max_abs_distance_diff_vs_1_minus_cos={max_dist_diff:.2e} \
         recall_loaded={recall_loaded:.3} near_ties_at_10th={near_ties}/{QUERIES}"
    );
    println!(
        "RESULT item10 expansion_search: {}",
        by_expansion.join("; ")
    );
    println!("RESULT item10 filtered_search: {}", filtered.join("; "));
    println!("RESULT item10 u64_keys: inserted={keys:?} returned={got:?}");
    println!(
        "RESULT item10 view_from_buffer_at_offset_4096: ok={buf_ok} same_results_as_view={buf_search_ok} slice_ptr_mod_64={ptr_align}"
    );
    assert!(got.contains(&(u64::MAX - 1)) && got.contains(&(1u64 << 40)));
    assert!(max_dist_diff < 1e-4);
}
