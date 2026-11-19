//! The key-index sidecar (D19): the filter's false-positive rate, sorted files (pages) and
//! files not sorted by the key (pairs), lookups and cluster ranges equal to the no-sidecar
//! route, the writer's inline build above the threshold and the request budget below it.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use moraine::expr::ScalarValue;
use moraine::index::BuildOptions;
use moraine::index::CacheMode;
use moraine::index::bundle::{BundleBytes, BundleView};
use moraine::index::{IndexKind, IndexSpec};
use moraine::testing::BundleBuild;
use moraine::testing::encode_row;
use moraine::testing::{KeyIndex, KeyKind, KeyRows, key_spec};
use moraine::{Database, Table, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::vectors::schema;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;

fn key(id: i64) -> Vec<u8> {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    encode_row(&[a], 0).unwrap()
}

fn open(bundle: Vec<u8>, spec: &IndexSpec) -> Arc<dyn moraine::index::FileIndex> {
    KeyKind
        .open(
            spec,
            BundleView::parse(BundleBytes::copy(&bundle).unwrap()).unwrap(),
        )
        .unwrap()
}

#[test]
fn fuse_filter_false_positive_rate() {
    let spec = IndexSpec::new("_moraine_key", "key", &["id"])
        .params(json!({"primary_key": ["id"], "cluster": [], "page_rows": 1024}));
    let n = 100_000i64;
    // Keys in a shuffled order: the builder writes pairs.
    let mut rng = SimRng::new(1);
    let mut ids: Vec<i64> = (0..n).map(|i| i * 2).collect();
    for i in (1..ids.len()).rev() {
        ids.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap();
    let mut b = BundleBuild::new(
        &KeyKind,
        &spec,
        n as u64,
        1,
        64 << 20,
        Cursor::new(Vec::new()),
    )
    .unwrap();
    b.add(0, &batch).unwrap();
    let (w, len, _) = b.finish().unwrap();
    let idx = open(w.into_inner(), &spec);
    let k = idx.as_any().downcast_ref::<KeyIndex>().unwrap();
    assert!(k.has_pairs() && !k.has_pages());
    for i in 0..n {
        assert!(
            matches!(k.lookup(&key(i * 2)), KeyRows::Rows(r) if r.len() == 1),
            "no false negative"
        );
    }
    // Absent keys pass the filter at about 2^-8; pairs then reject all but fingerprint clashes.
    let mut passed = 0;
    for i in 0..n {
        if k.lookup(&key(i * 2 + 1)) != KeyRows::Absent {
            passed += 1;
        }
    }
    println!("fuse: {len} bytes for {n} keys; {passed} absent keys passed");
    assert!(passed <= 1, "{passed} absent keys were not rejected");
    // Sorted keys get pages instead of pairs, so only the filter rejects: its rate shows.
    let sorted = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from_iter_values((0..n).map(|i| i * 2))) as ArrayRef,
    )])
    .unwrap();
    let mut b = BundleBuild::new(
        &KeyKind,
        &spec,
        n as u64,
        1,
        64 << 20,
        Cursor::new(Vec::new()),
    )
    .unwrap();
    b.add(0, &sorted).unwrap();
    let idx = open(b.finish().unwrap().0.into_inner(), &spec);
    let k = idx.as_any().downcast_ref::<KeyIndex>().unwrap();
    assert!(k.has_pages());
    let fp = (0..n)
        .filter(|i| k.lookup(&key(i * 2 + 1)) != KeyRows::Absent)
        .count();
    let rate = fp as f64 / n as f64;
    println!(
        "fuse filter alone: false-positive rate {:.3} %",
        rate * 100.0
    );
    assert!(rate < 0.006, "false-positive rate {rate}");
    assert!(len < 1_000_000 + 1_400_000, "bundle of {len} bytes");
}

async fn db(store: &Arc<InMemory>, inline: u64, cache: CacheMode) -> Database {
    Database::builder_for_store(store.clone(), "db")
        .key_index_inline_min_rows(inline)
        .small_table_rows(0)
        .small_table_bytes(0)
        .sidecar_cache(cache)
        .open()
        .await
        .unwrap()
}

fn rows(ids: &[i64]) -> RecordBatch {
    let v: Vec<Option<Vec<f32>>> = ids.iter().map(|_| None).collect();
    let b = moraine_testkit::vectors::batch(0, &v);
    let grp: Vec<i64> = ids.iter().map(|i| i.rem_euclid(7)).collect();
    RecordBatch::try_new(
        b.schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(grp)),
            b.column(2).clone(),
        ],
    )
    .unwrap()
}

/// Five files of 3,000 even keys each, interleaved so every file spans the whole range.
async fn fill(t: &Table) -> BTreeMap<i64, i64> {
    let mut model = BTreeMap::new();
    for f in 0..5 {
        let ids: Vec<i64> = (0..3000).map(|i| (i * 10 + f * 2) as i64).collect();
        t.upsert(vec![rows(&ids)]).await.unwrap();
        for id in &ids {
            model.insert(*id, id.rem_euclid(7));
        }
    }
    model
}

async fn get(t: &Table, id: i64) -> Option<i64> {
    let b = t.get(&[ScalarValue::Int64(Some(id))]).await.unwrap()?;
    Some(
        b.column_by_name("grp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
    )
}

/// Lookups through key indexes equal the no-sidecar route, for sorted files (pages) and for
/// clustered ones (pairs), and the writer's own index serves its lookups at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookups_equal_the_no_sidecar_route() {
    for cluster in [false, true] {
        let store = Arc::new(InMemory::new());
        let d = db(&store, 1000, CacheMode::Memory { max_bytes: 1 << 30 }).await;
        let opts = TableOptions::default()
            .primary_key(["id"])
            .cluster_by(if cluster { vec!["grp"] } else { Vec::new() });
        let t = d.create_table("t", schema(), opts).await.unwrap();
        let model = fill(&t).await;
        let plain = db(&store, u64::MAX, CacheMode::Disabled)
            .await
            .open_table("t")
            .await
            .unwrap();
        for id in (-10..30_100).step_by(7) {
            let want = model.get(&id).copied();
            assert_eq!(get(&t, id).await, want, "id {id}");
            assert_eq!(get(&plain, id).await, want, "id {id} without sidecars");
        }
        // The sidecars written inline are in the store, one per file.
        let listed: Vec<_> =
            futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&Path::from("db/t/idx"))))
                .await
                .unwrap();
        assert_eq!(listed.len(), 5, "{cluster}");
        // Pages for files sorted by the key alone, pairs and cluster ranges otherwise.
        let spec = key_spec(&t.definition()).unwrap();
        for m in &listed {
            let b = store
                .get(&m.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            let v = BundleView::parse(BundleBytes::copy(&b).unwrap()).unwrap();
            assert_eq!(v.section("pages").is_some(), !cluster);
            assert_eq!(v.section("pairs").is_some(), cluster);
            assert_eq!(v.section("cluster").is_some(), cluster);
            if cluster {
                let k = open(b, &spec);
                let k = k.as_any().downcast_ref::<KeyIndex>().unwrap();
                // Rows are sorted by (grp, id): 3,000 rows in 7 runs that tile the file.
                let mut next = 0;
                for g in 0..7i64 {
                    let a: ArrayRef = Arc::new(Int64Array::from(vec![g]));
                    let r = k
                        .cluster_rows(&encode_row(&[a], 0).unwrap())
                        .unwrap()
                        .unwrap();
                    assert_eq!(r.start, next);
                    next = r.end;
                }
                assert_eq!(next, 3000);
                let a: ArrayRef = Arc::new(Int64Array::from(vec![99]));
                assert_eq!(k.cluster_rows(&encode_row(&[a], 0).unwrap()), Some(None));
            }
        }
    }
}

/// D19's lookup cost with warm caches: an absent key costs no data read, a present one at
/// most one range read of data per file holding it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookup_requests_with_warm_caches() {
    let store = Arc::new(InMemory::new());
    let d = db(&store, 1000, CacheMode::Memory { max_bytes: 1 << 30 }).await;
    let opts = TableOptions::default().primary_key(["id"]);
    let t = d.create_table("t", schema(), opts).await.unwrap();
    fill(&t).await;
    t.set_consistency(moraine::Consistency::Manual);
    // Warm footers and key indexes.
    for id in [0, 2, 4, 6, 8] {
        get(&t, id).await.unwrap();
    }
    let m = d.store_metrics();
    let before = m.snapshot();
    assert_eq!(
        get(&t, 14_997).await,
        None,
        "absent, inside every file's range"
    );
    let absent = m.snapshot().since(&before);
    assert_eq!(absent.requests(), 0, "{absent}");
    let before = m.snapshot();
    assert!(get(&t, 7_502).await.is_some());
    let present = m.snapshot().since(&before);
    assert!(present.requests() <= 2, "{present}");
}

/// Below the inline threshold the flush budget is unchanged (one data PUT and one log PUT);
/// above it the key index rides along as one more PUT. The builder job covers small files.
#[tokio::test]
async fn inline_threshold_and_builder() {
    let store = Arc::new(InMemory::new());
    let d = db(&store, 2000, CacheMode::Memory { max_bytes: 1 << 30 }).await;
    let opts = TableOptions::default().primary_key(["id"]);
    let t = d.create_table("t", schema(), opts).await.unwrap();
    let puts = |s: &moraine::store::MetricsSnapshot| {
        s.count(moraine::store::Op::PutOverwrite) + s.count(moraine::store::Op::PutCreate)
    };
    let m = d.store_metrics();
    let ids: Vec<i64> = (0..1999).collect();
    let before = m.snapshot();
    t.upsert(vec![rows(&ids)]).await.unwrap();
    let small = m.snapshot().since(&before);
    let ids: Vec<i64> = (10_000..12_000).collect();
    let before = m.snapshot();
    t.upsert(vec![rows(&ids)]).await.unwrap();
    let large = m.snapshot().since(&before);
    assert_eq!(puts(&small), 2, "{small}");
    assert_eq!(puts(&large), 3, "{large}");
    assert_eq!(small.listings(), 0);
    // The builder job builds the small file's key index.
    let r = t.build_indexes(BuildOptions::default()).await.unwrap();
    assert_eq!(r.built, 1, "{r:?}");
    let r = t.build_indexes(BuildOptions::default()).await.unwrap();
    assert_eq!(r.built, 0);
}
