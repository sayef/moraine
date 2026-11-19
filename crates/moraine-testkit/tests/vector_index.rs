//! The `vec-hnsw` kind end to end: exact and usearch distances agree,
//! T9 recall (small), scenarios 9, 10 and 11, and cases V2 to V9, V11, V13 and V14.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use moraine::expr::{col, lit};
use moraine::index::BuildOptions;
use moraine::index::CacheMode;
use moraine::index::bundle::{BundleBytes, BundleView};
use moraine::{Database, Error, IndexSpec, Table, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::vectors::{batch, centres, clustered, schema};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde_json::json;

const DIM: usize = 32;

struct Env {
    store: Arc<InMemory>,
    db: Database,
    t: Table,
    /// The same table through a client whose cache is disabled: the exact route everywhere.
    exact: Table,
    centres: Vec<Vec<f32>>,
    spread: f32,
}

async fn db(store: &Arc<InMemory>, cache: CacheMode) -> Database {
    Database::builder_for_store(store.clone(), "db")
        .sidecar_cache(cache)
        .sidecar_poll_interval(std::time::Duration::ZERO)
        .open()
        .await
        .unwrap()
}

/// Both vector kinds run every test (D24: they are two routes to the same answers).
const KINDS: &[&str] = &["vec-hnsw", "vec-tq"];

tokio::task_local! {
    static KIND: &'static str;
}

fn kind() -> &'static str {
    KIND.try_with(|k| *k).unwrap_or("vec-hnsw")
}

fn spec(metric: &str) -> IndexSpec {
    IndexSpec::new("emb", kind(), &["v"])
        .params(json!({"dim": DIM, "metric": metric}))
        .policy(json!({"min_rows_for_ann": 1000}))
}

/// `s` as the kind stores it.
fn resolved(s: IndexSpec) -> IndexSpec {
    use moraine::index::IndexKind;
    let k: Box<dyn IndexKind> = match kind() {
        "vec-tq" => Box::new(moraine::index::vector::TurboKind),
        _ => Box::new(moraine::index::vector::HnswKind),
    };
    k.resolve(&s, &schema()).unwrap()
}

/// `files` files of `rows + file number` clustered vectors (so no two files have the same row
/// count); every 50th vector is null.
async fn env(files: usize, rows: usize, metric: &str) -> Env {
    env_spread(files, rows, metric, 0.35).await
}

async fn env_spread(files: usize, rows: usize, metric: &str, spread: f32) -> Env {
    let store = Arc::new(InMemory::new());
    let db = db(&store, CacheMode::Memory { max_bytes: 1 << 30 }).await;
    let t = db
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let centres = centres(1, 32, DIM);
    let mut rng = SimRng::new(2);
    for f in 0..files {
        let v: Vec<Option<Vec<f32>>> = clustered(&mut rng, &centres, rows + f, spread)
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i % 50 != 7).then_some(v))
            .collect();
        t.append(vec![batch((f * (rows + files)) as i64, &v)])
            .await
            .unwrap();
    }
    t.create_index(spec(metric)).await.unwrap();
    let exact = db_exact(&store).await;
    Env {
        store,
        db,
        t,
        exact,
        centres,
        spread,
    }
}

async fn db_exact(store: &Arc<InMemory>) -> Table {
    db(store, CacheMode::Disabled)
        .await
        .open_table("t")
        .await
        .unwrap()
}

fn queries(env: &Env, n: usize) -> Vec<Vec<f32>> {
    clustered(&mut SimRng::new(99), &env.centres, n, env.spread)
}

type Key = (String, u64);

async fn hits(
    t: &Table,
    q: &[f32],
    k: usize,
    filter: Option<moraine::expr::Expr>,
) -> Vec<(Key, f32)> {
    let mut s = t.search_vector("emb", q.to_vec()).limit(k).select(["id"]);
    if let Some(f) = filter {
        s = s.filter(f);
    }
    let r = s.collect().await.unwrap();
    r.hits
        .into_iter()
        .map(|h| ((h.file, h.offset), h.score))
        .collect()
}

/// Mean recall@k of `ann` against `exact` over `qs`.
async fn recall(
    ann: &Table,
    exact: &Table,
    qs: &[Vec<f32>],
    k: usize,
    filter: Option<moraine::expr::Expr>,
) -> f64 {
    let mut found = 0usize;
    let mut total = 0usize;
    for q in qs {
        let truth: HashSet<Key> = hits(exact, q, k, filter.clone())
            .await
            .into_iter()
            .map(|h| h.0)
            .collect();
        let got: HashSet<Key> = hits(ann, q, k, filter.clone())
            .await
            .into_iter()
            .map(|h| h.0)
            .collect();
        found += truth.intersection(&got).count();
        total += truth.len();
    }
    found as f64 / total.max(1) as f64
}

/// The recall bound (T9, D21): recall@10 of at least 0.95 at default parameters.
const RECALL: f64 = 0.95;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_and_usearch_distances_agree() {
    for k in KINDS {
        KIND.scope(k, exact_and_usearch_distances_agree_for()).await;
    }
}

async fn exact_and_usearch_distances_agree_for() {
    let metrics: &[&str] = if kind() == "vec-tq" {
        &["cosine", "ip"]
    } else {
        &["cosine", "l2", "ip"]
    };
    for &metric in metrics {
        let env = env(1, 3000, metric).await;
        let r = env.t.build_indexes(BuildOptions::default()).await.unwrap();
        assert_eq!(r.built, 1, "{metric}");
        for q in queries(&env, 10) {
            let a = hits(&env.t, &q, 10, None).await;
            let e = hits(&env.exact, &q, 10, None).await;
            let exact: std::collections::HashMap<Key, f32> = e.into_iter().collect();
            for (k, d) in a {
                if let Some(x) = exact.get(&k) {
                    assert!(
                        (d - x).abs() <= 1e-4 * x.abs().max(1.0),
                        "{} {metric}: sidecar {d} exact {x}",
                        kind()
                    );
                }
            }
        }
    }
}

/// T9 (small): recall@10 against exact search at default parameters, 1 file and 10 files,
/// with and without filters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t9_recall_small() {
    for k in KINDS {
        KIND.scope(k, t9_recall_small_for()).await;
    }
}

async fn t9_recall_small_for() {
    // Tight clusters, and a spread so wide the data is close to uniform (the hard case).
    for (files, rows, spread) in [
        (1, 20_000, 0.35),
        (10, 2_000, 0.35),
        (1, 20_000, 3.0),
        (10, 2_000, 3.0),
    ] {
        let env = env_spread(files, rows, "cosine", spread).await;
        env.t.build_indexes(BuildOptions::default()).await.unwrap();
        let qs = queries(&env, 40);
        let r = recall(&env.t, &env.exact, &qs, 10, None).await;
        println!(
            "T9 small: {} spread {spread}, {files} files, recall@10 {r:.3}",
            kind()
        );
        assert!(r >= RECALL, "{files} files: recall {r:.3}");
        let wide = col("grp").lt(lit(5i64));
        let r = recall(&env.t, &env.exact, &qs, 10, Some(wide)).await;
        println!(
            "T9 small: {} spread {spread}, {files} files, 50% filter, recall@10 {r:.3}",
            kind()
        );
        assert!(r >= RECALL, "{files} files, 50% filter: recall {r:.3}");
        // A narrow filter goes exact (V8): recall is 1.
        let narrow = col("id").lt(lit(500i64));
        let r = recall(&env.t, &env.exact, &qs[..5], 10, Some(narrow)).await;
        assert_eq!(r, 1.0);
    }
}

/// Scenario 9: the same queries with the cache disabled, with every sidecar built, and with a
/// random half of them deleted from the store; the results agree within the recall bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_transparency() {
    for k in KINDS {
        KIND.scope(k, index_transparency_for()).await;
    }
}

async fn index_transparency_for() {
    let env = env(8, 2_000, "cosine").await;
    let qs = queries(&env, 30);
    // No sidecars yet: both clients take the exact route (V2), identically.
    for q in &qs[..5] {
        assert_eq!(
            hits(&env.t, q, 10, None).await,
            hits(&env.exact, q, 10, None).await
        );
    }
    env.t.build_indexes(BuildOptions::default()).await.unwrap();
    let all = recall(&env.t, &env.exact, &qs, 10, None).await;
    assert!(all >= RECALL, "all sidecars: {all:.3}");
    // Delete a random half of the sidecars; a fresh client lists what remains.
    let mut rng = SimRng::new(5);
    let listed: Vec<_> =
        futures::TryStreamExt::try_collect::<Vec<_>>(env.store.list(Some(&Path::from("db/t/idx"))))
            .await
            .unwrap();
    assert_eq!(listed.len(), 8);
    for m in &listed {
        if rng.chance(0.5) {
            env.store.delete(&m.location).await.unwrap();
        }
    }
    let half = db(&env.store, CacheMode::Memory { max_bytes: 1 << 30 })
        .await
        .open_table("t")
        .await
        .unwrap();
    let r = recall(&half, &env.exact, &qs, 10, None).await;
    assert!(r >= RECALL, "half the sidecars: {r:.3}");
    let s = half
        .search_vector("emb", qs[0].clone())
        .collect()
        .await
        .unwrap();
    assert_eq!(s.stats.sidecar_files + s.stats.exact_files, 8);
    assert!(
        s.stats.exact_files > 0 && s.stats.sidecar_files > 0,
        "{:?}",
        s.stats
    );
    let _ = env.db;
}

/// Scenario 10: corrupted, truncated, swapped and mismatched sidecars are ignored; queries
/// stay correct and each bad sidecar is counted (V5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_sidecar_is_ignored() {
    for k in KINDS {
        KIND.scope(k, corrupt_sidecar_is_ignored_for()).await;
    }
}

async fn corrupt_sidecar_is_ignored_for() {
    let env = env(4, 2_000, "cosine").await;
    env.t.build_indexes(BuildOptions::default()).await.unwrap();
    let mut listed: Vec<_> =
        futures::TryStreamExt::try_collect::<Vec<_>>(env.store.list(Some(&Path::from("db/t/idx"))))
            .await
            .unwrap();
    listed.sort_by(|a, b| a.location.cmp(&b.location));
    let get = |p: Path| {
        let s = env.store.clone();
        async move { s.get(&p).await.unwrap().bytes().await.unwrap().to_vec() }
    };
    let a = get(listed[0].location.clone()).await;
    let b = get(listed[1].location.clone()).await;
    // Flip a byte inside a section (caught by the section checksum).
    let mut flipped = a.clone();
    flipped[5000] ^= 0xff;
    env.store
        .put(&listed[0].location, PutPayload::from(flipped))
        .await
        .unwrap();
    // Swap: file 1 gets file 0's (original) sidecar: wrong source size and rows.
    env.store
        .put(&listed[1].location, PutPayload::from(a.clone()))
        .await
        .unwrap();
    // Truncate file 2's.
    let c = get(listed[2].location.clone()).await;
    env.store
        .put(
            &listed[2].location,
            PutPayload::from(c[..c.len() / 2].to_vec()),
        )
        .await
        .unwrap();
    // File 3: a header claiming another row count, with the footer checksum recomputed.
    let mut d = get(listed[3].location.clone()).await;
    d[24] ^= 1;
    moraine::fuzzing::bundle_reseal(&mut d);
    assert!(BundleView::parse(BundleBytes::copy(&d).unwrap()).is_ok());
    env.store
        .put(&listed[3].location, PutPayload::from(d))
        .await
        .unwrap();
    let _ = b;

    let fresh = db(&env.store, CacheMode::Memory { max_bytes: 1 << 30 }).await;
    let t = fresh.open_table("t").await.unwrap();
    for q in queries(&env, 10) {
        assert_eq!(
            hits(&t, &q, 10, None).await,
            hits(&env.exact, &q, 10, None).await
        );
    }
    let bad = fresh.index_metrics().bad_sidecars.load(Ordering::Relaxed);
    assert_eq!(bad, 4, "each bad sidecar counted once, then marked bad");
}

/// Scenario 11: indexes created, dropped and recreated with other parameters while writers
/// append and builders run. Nothing fails, and the final index answers within the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_change_during_writes() {
    for k in KINDS {
        KIND.scope(k, index_change_during_writes_for()).await;
    }
}

async fn index_change_during_writes_for() {
    let env = env(2, 2_000, "cosine").await;
    let writer = {
        let t = env.t.clone();
        let centres = env.centres.clone();
        tokio::spawn(async move {
            let mut rng = SimRng::new(77);
            for i in 0..8 {
                let v: Vec<Option<Vec<f32>>> = clustered(&mut rng, &centres, 1500, 0.35)
                    .into_iter()
                    .map(Some)
                    .collect();
                t.append(vec![batch(100_000 + i * 1500, &v)]).await.unwrap();
            }
        })
    };
    let builder = {
        let b = db(&env.store, CacheMode::Memory { max_bytes: 1 << 30 })
            .await
            .open_table("t")
            .await
            .unwrap();
        tokio::spawn(async move {
            for _ in 0..12 {
                b.build_indexes(BuildOptions::default()).await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let changer = {
        let t = db(&env.store, CacheMode::Memory { max_bytes: 1 << 30 })
            .await
            .open_table("t")
            .await
            .unwrap();
        tokio::spawn(KIND.scope(kind(), async move {
            for i in 0..6 {
                match i % 3 {
                    0 => t.drop_index("emb").await.unwrap(),
                    1 => {
                        t.create_index(spec("cosine").params(
                            json!({"dim": DIM, "metric": "cosine", "connectivity": 12, "bits": 3}),
                        ))
                        .await
                        .unwrap();
                    }
                    _ => {
                        t.replace_index(spec("cosine")).await.unwrap();
                    }
                }
            }
        }))
    };
    writer.await.unwrap();
    builder.await.unwrap();
    changer.await.unwrap();
    env.t.refresh().await.unwrap();
    let specs = env.t.indexes().unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(
        specs[0],
        resolved(spec("cosine")),
        "the last change was a replace"
    );
    env.t.build_indexes(BuildOptions::default()).await.unwrap();
    let st = env.t.index_status().await.unwrap();
    assert_eq!(st[0].files_covered, st[0].files_total, "{st:?}");
    let exact = db_exact(&env.store).await;
    let r = recall(&env.t, &exact, &queries(&env, 20), 10, None).await;
    assert!(r >= RECALL, "{r:.3}");
}

/// V3, V4, V6, V13: files below the threshold get no sidecar; an all-null file gets an empty
/// bundle once; a changed spec starts uncovered; `index_status` shows the backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_policy_nulls_and_spec_changes() {
    for k in KINDS {
        KIND.scope(k, status_policy_nulls_and_spec_changes_for())
            .await;
    }
}

async fn status_policy_nulls_and_spec_changes_for() {
    let env = env(2, 2_000, "cosine").await;
    // A small file (below min_rows_for_ann) and an all-null file.
    let small: Vec<Option<Vec<f32>>> = clustered(&mut SimRng::new(3), &env.centres, 100, 0.3)
        .into_iter()
        .map(Some)
        .collect();
    env.t.append(vec![batch(50_000, &small)]).await.unwrap();
    env.t
        .append(vec![batch(60_000, &vec![None; 1500])])
        .await
        .unwrap();
    // The all-null file, appended after the index exists, got its (empty) bundle before its
    // commit (index before commit); the two files written before the index await the build.
    let st = &env.t.index_status().await.unwrap()[0];
    assert_eq!(
        (st.files_total, st.files_covered, st.files_exact_by_policy),
        (4, 1, 1)
    );
    assert_eq!(st.rows_total, 2000 + 2001 + 100 + 1500);
    assert_eq!(st.rows_pending(), 2000 + 2001);
    let r = env.t.build_indexes(BuildOptions::default()).await.unwrap();
    assert_eq!((r.built, r.exact_by_policy, r.failed), (2, 1, 0), "{r:?}");
    let st = &env.t.index_status().await.unwrap()[0];
    assert_eq!(st.files_covered, 3);
    assert_eq!(st.rows_covered, 2000 + 2001 + 1500);
    assert!(st.sidecar_bytes > 0);
    // V4: nothing left to build, the empty bundle counts.
    let r = env.t.build_indexes(BuildOptions::default()).await.unwrap();
    assert_eq!(r.built, 0);
    // V6: new parameters, new hash; old sidecars are ignored at once.
    env.t
        .replace_index(
            spec("cosine").params(json!({"dim": DIM, "metric": "cosine", "expansion_add": 64})),
        )
        .await
        .unwrap();
    let st = &env.t.index_status().await.unwrap()[0];
    assert_eq!(st.files_covered, 0);
    let q = queries(&env, 1).remove(0);
    let s = env.t.search_vector("emb", q).collect().await.unwrap();
    assert_eq!(s.stats.sidecar_files, 0);
    assert_eq!(s.stats.exact_files, 4);
}

/// V11: a query with the wrong dimension, a NaN or (under cosine) no direction is refused.
#[tokio::test]
async fn invalid_queries_v11() {
    for k in KINDS {
        KIND.scope(k, invalid_queries_v11_for()).await;
    }
}

async fn invalid_queries_v11_for() {
    let env = env(1, 100, "cosine").await;
    for q in [
        vec![1.0; DIM - 1],
        {
            let mut v = vec![1.0; DIM];
            v[3] = f32::NAN;
            v
        },
        vec![0.0; DIM],
    ] {
        let e = env.t.search_vector("emb", q).collect().await.unwrap_err();
        assert!(matches!(e, Error::InvalidQuery(_)), "{e}");
    }
    // A bad spec is refused at creation.
    let e = env
        .t
        .create_index(IndexSpec::new("x", kind(), &["v"]).params(json!({"metric": "cosine"})))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::InvalidIndexSpec(_)), "{e}");
    let e = env
        .t
        .create_index(IndexSpec::new("x", kind(), &["id"]).params(json!({"dim": 8})))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::InvalidIndexSpec(_)), "{e}");
}

/// V14: two builders race for the same files; both succeed, readers holding either copy are
/// correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_builders_race_v14() {
    for k in KINDS {
        KIND.scope(k, two_builders_race_v14_for()).await;
    }
}

async fn two_builders_race_v14_for() {
    let env = env(3, 2_000, "cosine").await;
    let other = db(&env.store, CacheMode::Memory { max_bytes: 1 << 30 })
        .await
        .open_table("t")
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        env.t.build_indexes(BuildOptions::default()),
        other.build_indexes(BuildOptions::default())
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.built + a.already_present, 3);
    assert_eq!(b.built + b.already_present, 3);
    let qs = queries(&env, 20);
    for t in [&env.t, &other] {
        let r = recall(t, &env.exact, &qs, 10, None).await;
        assert!(r >= RECALL, "{r:.3}");
    }
}

/// Vector columns are stored without compression or dictionary, in pages of
/// at most 64 KiB so a rescore decodes little (D37).
#[tokio::test]
async fn vector_columns_are_plain() {
    for k in KINDS {
        KIND.scope(k, vector_columns_are_plain_for()).await;
    }
}

async fn vector_columns_are_plain_for() {
    use parquet::basic::{Compression, Encoding};
    let env = env(1, 3000, "cosine").await;
    let listed: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(
        env.store.list(Some(&Path::from("db/t/data"))),
    )
    .await
    .unwrap();
    let file = listed
        .iter()
        .find(|m| m.location.as_ref().ends_with(".parquet"))
        .unwrap();
    let body = env
        .store
        .get(&file.location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let opts = parquet::file::serialized_reader::ReadOptionsBuilder::new()
        .with_page_index()
        .build();
    let r = parquet::file::reader::SerializedFileReader::new_with_options(body, opts).unwrap();
    let meta = parquet::file::reader::FileReader::metadata(&r);
    let rg = meta.row_group(0);
    // 3,001 vectors of 32 floats are 384 KiB: at least six 64 KiB pages, where the 1,024-row
    // limit alone would give three.
    let pages = &meta.offset_index().unwrap()[0];
    for (i, c) in rg.columns().iter().enumerate() {
        if c.column_path().string().starts_with("v.") {
            let n = pages[i].page_locations().len();
            assert!(n >= 6, "{n} pages");
        }
    }
    for c in rg.columns() {
        let path = c.column_path().string();
        if path.starts_with("v.") {
            assert_eq!(c.compression(), Compression::UNCOMPRESSED, "{path}");
            assert!(
                !c.encodings().any(|e| e == Encoding::RLE_DICTIONARY),
                "{path}"
            );
        } else {
            assert!(matches!(c.compression(), Compression::ZSTD(_)), "{path}");
        }
    }
}
