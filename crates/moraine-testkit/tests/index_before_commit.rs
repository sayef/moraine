//! Index before commit (D58): a writer builds its new file's sidecars before the commit,
//! within each index's limits; compaction does it for every index, streams its merge, and
//! merges turbovec codes without reading rows. Every path's request budget is asserted.

use std::sync::Arc;

use moraine::index::CacheMode;
use moraine::store::{MetricsSnapshot, Op};
use moraine::{CompactionPolicy, Database, IndexSpec, MaintainOptions, Table, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::vectors::{batch, centres, clustered, schema};
use object_store::memory::InMemory;
use serde_json::json;

const DIM: usize = 32;

fn counts(m: &MetricsSnapshot) -> Vec<(&'static str, u64)> {
    Op::all()
        .iter()
        .filter(|o| m.count(**o) > 0)
        .map(|o| (o.name(), m.count(*o)))
        .collect()
}

async fn db(store: &Arc<InMemory>) -> Database {
    Database::builder_for_store(store.clone(), "db")
        .sidecar_cache(CacheMode::Memory { max_bytes: 1 << 30 })
        .sidecar_poll_interval(std::time::Duration::ZERO)
        .open()
        .await
        .unwrap()
}

fn spec(kind: &str, policy: serde_json::Value) -> IndexSpec {
    IndexSpec::new("emb", kind, &["v"])
        .params(json!({"dim": DIM, "metric": "cosine"}))
        .policy(policy)
}

fn vectors(rng: &mut SimRng, n: usize) -> Vec<Option<Vec<f32>>> {
    let c = centres(1, 8, DIM);
    clustered(rng, &c, n, 0.3).into_iter().map(Some).collect()
}

async fn table(store: &Arc<InMemory>, spec: IndexSpec) -> (Database, Table) {
    let db = db(store).await;
    let t = db
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    t.create_index(spec).await.unwrap();
    (db, t)
}

/// An append above the kind's threshold puts its sidecar beside its data file: one more PUT
/// (put-if-absent),
/// nothing else, and the index covers every row at once. Below the threshold, or with the
/// policy off or over its limits, the budget is D18's: one data PUT and one log PUT.
#[tokio::test]
async fn append_builds_sidecars_within_limits() {
    for kind in ["vec-hnsw", "vec-tq"] {
        let store = Arc::new(InMemory::new());
        let (db, t) = table(&store, spec(kind, json!({"min_rows_for_ann": 500}))).await;
        let mut rng = SimRng::new(1);
        let m = || db.store_metrics().snapshot();

        let before = m();
        t.append(vec![batch(0, &vectors(&mut rng, 600))])
            .await
            .unwrap();
        let d = m().since(&before);
        // The sidecar is a put-if-absent, like the log entry.
        assert_eq!(
            counts(&d),
            vec![("put", 1), ("put_create", 2)],
            "{kind}: {d}"
        );
        let st = &t.index_status().await.unwrap()[0];
        assert_eq!((st.rows_covered, st.rows_pending()), (600, 0), "{kind}");

        // Below the threshold: exact by policy, no sidecar.
        let before = m();
        t.append(vec![batch(1000, &vectors(&mut rng, 100))])
            .await
            .unwrap();
        assert_eq!(
            counts(&m().since(&before)),
            vec![("put", 1), ("put_create", 1)]
        );
        let st = &t.index_status().await.unwrap()[0];
        assert_eq!((st.rows_exact_by_policy, st.rows_pending()), (100, 0));

        // The search sees both files, the first through its sidecar.
        let q = vectors(&mut rng, 1).remove(0).unwrap();
        let r = t.search_vector("emb", q).limit(5).collect().await.unwrap();
        assert_eq!(r.hits.len(), 5);
        assert_eq!((r.stats.sidecar_files, r.stats.exact_files), (1, 1));
        assert_eq!(r.stats.exact_rows, 100);
    }
}

/// The policy's switches: off, a row limit, a build-cost limit. Files beyond them wait for
/// the build job, as before index before commit.
#[tokio::test]
async fn policy_limits_leave_files_to_the_builder() {
    for policy in [
        json!({"min_rows_for_ann": 100, "before_commit": false}),
        json!({"min_rows_for_ann": 100, "before_commit_max_rows": 500}),
        json!({"min_rows_for_ann": 100, "before_commit_max_build_bytes": 1000}),
    ] {
        let store = Arc::new(InMemory::new());
        let (db, t) = table(&store, spec("vec-hnsw", policy.clone())).await;
        let mut rng = SimRng::new(2);
        let before = db.store_metrics().snapshot();
        t.append(vec![batch(0, &vectors(&mut rng, 600))])
            .await
            .unwrap();
        let d = db.store_metrics().snapshot().since(&before);
        assert_eq!(
            counts(&d),
            vec![("put", 1), ("put_create", 1)],
            "{policy}: {d}"
        );
        let st = &t.index_status().await.unwrap()[0];
        assert_eq!(st.rows_pending(), 600, "{policy}");
        t.build_indexes(Default::default()).await.unwrap();
        let st = &t.index_status().await.unwrap()[0];
        assert_eq!(st.rows_pending(), 0, "{policy}");
    }
}

/// An upsert's new rows and rewritten files get their sidecars before the commit too.
#[tokio::test]
async fn rewrites_build_before_commit() {
    let store = Arc::new(InMemory::new());
    let db = db(&store).await;
    let t = db
        .create_table("t", schema(), TableOptions::default().primary_key(["id"]))
        .await
        .unwrap();
    t.create_index(spec("vec-hnsw", json!({"min_rows_for_ann": 200})))
        .await
        .unwrap();
    let mut rng = SimRng::new(3);
    t.upsert(vec![batch(0, &vectors(&mut rng, 400))])
        .await
        .unwrap();
    t.upsert(vec![batch(300, &vectors(&mut rng, 250))])
        .await
        .unwrap();
    let st = &t.index_status().await.unwrap()[0];
    assert_eq!(st.rows_total, 550);
    assert_eq!(st.rows_pending(), 0, "{st:?}");
}

/// Compaction streams its merge, builds the merged file's sidecars before its commit, and for
/// turbovec gathers the inputs' codes instead (no rows read for the index): the merged sidecar
/// is byte-identical to one built from the merged file's rows.
#[tokio::test]
async fn compaction_streams_and_merges_turbovec_codes() {
    for kind in ["vec-hnsw", "vec-tq"] {
        let store = Arc::new(InMemory::new());
        let (_db, t) = table(&store, spec(kind, json!({"min_rows_for_ann": 100}))).await;
        let mut rng = SimRng::new(4);
        for f in 0..4 {
            t.append(vec![batch(f * 1000, &vectors(&mut rng, 300))])
                .await
                .unwrap();
        }
        let r = t
            .maintain(
                &MaintainOptions::default()
                    .leases(false)
                    .compaction(Some(CompactionPolicy::full())),
            )
            .await
            .unwrap()
            .compaction
            .unwrap();
        assert_eq!(r.streamed, r.steps, "{kind}: {r:?}");
        assert!(r.steps > 0);
        assert_eq!(r.sidecars_merged > 0, kind == "vec-tq", "{kind}: {r:?}");
        assert_eq!(t.snapshot_in_hand().files().len(), 1);
        let st = &t.index_status().await.unwrap()[0];
        assert_eq!((st.rows_total, st.rows_covered), (1200, 1200), "{kind}");
        assert_eq!(t.scan().count().await.unwrap(), 1200);
        if kind == "vec-tq" {
            // Rebuilt from the merged file's rows, the sidecar is byte for byte the same.
            let live = t.snapshot_in_hand().files()[0].path.clone();
            let prefix = format!("db/t/idx/{live}");
            let merged = sidecar_bytes(&store, &prefix).await;
            assert_eq!(merged.len(), 1);
            delete_prefix(&store, &prefix).await;
            let b = t.build_indexes(Default::default()).await.unwrap();
            assert_eq!(b.built, 1);
            assert_eq!(sidecar_bytes(&store, &prefix).await, merged);
        }
    }
}

async fn delete_prefix(store: &Arc<InMemory>, prefix: &str) {
    use futures::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};
    let p = object_store::path::Path::from(prefix);
    let metas: Vec<_> = store.list(Some(&p)).try_collect().await.unwrap();
    for m in metas {
        store.delete(&m.location).await.unwrap();
    }
}

async fn sidecar_bytes(store: &Arc<InMemory>, prefix: &str) -> Vec<Vec<u8>> {
    use futures::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};
    let p = object_store::path::Path::from(prefix);
    let metas: Vec<_> = store.list(Some(&p)).try_collect().await.unwrap();
    let mut out = Vec::new();
    for m in metas {
        out.push(
            store
                .get(&m.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec(),
        );
    }
    out
}

/// Streaming and in-memory compaction produce the same rows in the same order, on a table with
/// a cluster key and equal keys across files, over more inputs than one merge pass reads.
#[tokio::test]
async fn streaming_equals_in_memory() {
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    let mut results = Vec::new();
    for streaming in [true, false] {
        let store = Arc::new(InMemory::new());
        let db = db(&store).await;
        let t = db
            .create_table(
                "t",
                schema(),
                TableOptions::default()
                    .cluster_by(["grp"])
                    .row_group_rows(256),
            )
            .await
            .unwrap();
        let mut rng = SimRng::new(5);
        // More files than one merge pass reads (32): the streamed merge takes two passes.
        for f in 0..40i64 {
            t.append(vec![batch(f * 7, &vectors(&mut rng, 120))])
                .await
                .unwrap();
        }
        let r = t
            .maintain(
                &MaintainOptions::default()
                    .leases(false)
                    .compaction(Some(CompactionPolicy::full().streaming(streaming))),
            )
            .await
            .unwrap()
            .compaction
            .unwrap();
        assert_eq!(r.streamed > 0, streaming);
        let rows: Vec<(i64, i64)> = t
            .scan()
            .collect()
            .await
            .unwrap()
            .iter()
            .flat_map(|b| {
                let id = b.column(0).as_primitive::<Int64Type>().clone();
                let g = b.column(1).as_primitive::<Int64Type>().clone();
                (0..b.num_rows())
                    .map(move |i| (g.value(i), id.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();
        results.push(rows);
    }
    assert_eq!(results[0].len(), 40 * 120);
    assert_eq!(results[0], results[1]);
    let mut sorted = results[0].clone();
    sorted.sort_by_key(|r| r.0);
    assert_eq!(results[0], sorted, "clustered by grp, stable");
}
