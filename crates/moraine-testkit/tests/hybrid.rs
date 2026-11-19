//! Hybrid search: a vector and a full-text search against one snapshot, fused
//! with reciprocal rank fusion, or with a fusion of the caller's.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{Field, Int64Type, Schema};
use moraine::expr::{col, lit};
use moraine::index::BuildOptions;
use moraine::index::hybrid::{Fusion, Rrf};
use moraine::index::{SearchHit, SearchResult};
use moraine::{Database, IndexSpec, Table, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::text::{document, word};
use moraine_testkit::vectors::{centres, clustered, list_type};
use object_store::memory::InMemory;
use serde_json::json;

const DIM: usize = 16;

fn schema() -> Arc<Schema> {
    let mut f: Vec<Field> = moraine_testkit::text::schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    f.push(Field::new("v", list_type(), true));
    Arc::new(Schema::new(f))
}

/// `n` rows from `first`, text and vectors.
fn rows(rng: &mut SimRng, c: &[Vec<f32>], first: i64, n: usize) -> RecordBatch {
    let docs: Vec<_> = (0..n).map(|_| document(rng, 40, 25)).collect();
    let text = moraine_testkit::text::batch(first, &docs);
    let v: Vec<Option<Vec<f32>>> = clustered(rng, c, n, 0.4).into_iter().map(Some).collect();
    let vb = moraine_testkit::vectors::batch(first, &v);
    let mut cols: Vec<ArrayRef> = text.columns().to_vec();
    cols.push(vb.column_by_name("v").unwrap().clone());
    RecordBatch::try_new(schema(), cols).unwrap()
}

async fn env() -> (Table, Vec<Vec<f32>>) {
    let store = Arc::new(InMemory::new());
    let db = Database::builder_for_store(store, "db")
        .sidecar_poll_interval(std::time::Duration::ZERO)
        .open()
        .await
        .unwrap();
    let t = db
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let c = centres(4, 8, DIM);
    let mut rng = SimRng::new(1);
    for f in 0..5 {
        t.append(vec![rows(&mut rng, &c, f * 400, 400)])
            .await
            .unwrap();
    }
    t.create_index(
        IndexSpec::new("emb", "vec-hnsw", &["v"])
            .params(json!({"dim": DIM}))
            .policy(json!({"min_rows_for_ann": 100})),
    )
    .await
    .unwrap();
    t.create_index(
        IndexSpec::new("fts", "fts", &["title", "body"])
            .policy(json!({"min_rows_for_sidecar": 100})),
    )
    .await
    .unwrap();
    (t, c)
}

type Key = (String, u64);

fn keys(hits: &[SearchHit]) -> Vec<Key> {
    hits.iter().map(|h| (h.file.clone(), h.offset)).collect()
}

/// RRF by hand over the two lists.
fn rrf(lists: &[&[SearchHit]], k: usize) -> Vec<(Key, f32)> {
    let mut m: HashMap<Key, f32> = HashMap::new();
    for l in lists {
        for (i, h) in l.iter().enumerate() {
            *m.entry((h.file.clone(), h.offset)).or_default() += 1.0 / (60.0 + i as f32 + 1.0);
        }
    }
    let mut v: Vec<(Key, f32)> = m.into_iter().collect();
    v.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    v.truncate(k);
    v
}

fn fused(r: &SearchResult) -> Vec<(Key, f32)> {
    r.hits
        .iter()
        .map(|h| ((h.file.clone(), h.offset), h.score))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_fuses_both_searches() {
    let (t, c) = env().await;
    let mut rng = SimRng::new(7);
    for built in [false, true] {
        if built {
            t.build_indexes(BuildOptions::default()).await.unwrap();
        }
        for i in 0..10 {
            let q = clustered(&mut rng, &c, 1, 0.4).remove(0);
            let text = format!("{} {}", word(&mut rng, 40), word(&mut rng, 40));
            let k = 5 + i;
            let filter = (i % 3 == 0).then(|| col("g").lt(lit(4i64)));
            let mut h = t
                .search_hybrid("emb", q.clone(), "fts", &text)
                .limit(k)
                .select(["id", "title"]);
            let mut vs = t.search_vector("emb", q.clone()).limit(k * 4);
            let mut ts = t.search_text("fts", &text).limit(k * 4);
            if let Some(f) = &filter {
                h = h.filter(f.clone());
                vs = vs.filter(f.clone());
                ts = ts.filter(f.clone());
            }
            let r = h.collect().await.unwrap();
            let (v, x) = (vs.collect().await.unwrap(), ts.collect().await.unwrap());
            let want = rrf(&[&v.hits, &x.hits], k);
            assert_eq!(fused(&r), want, "query {i}, built {built}");
            assert_eq!(r.batch.num_rows(), want.len());
            assert_eq!(
                r.batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                ["id", "title", "_moraine_score"]
            );
            if let Some(_f) = &filter {
                let g = r
                    .batch
                    .column_by_name("id")
                    .unwrap()
                    .as_primitive::<Int64Type>();
                assert!(g.values().iter().all(|id| id % 7 < 4));
            }
            // Rows found by both searches get both contributions.
            let both: std::collections::HashSet<Key> = keys(&v.hits)
                .into_iter()
                .filter(|k| keys(&x.hits).contains(k))
                .collect();
            for (key, s) in fused(&r) {
                if both.contains(&key) {
                    assert!(s > 1.0 / 61.0, "{key:?} {s}");
                }
            }
        }
    }
}

/// A fusion that keeps the full-text list alone gives the full-text ranking.
#[derive(Debug)]
struct TextOnly;

impl Fusion for TextOnly {
    fn fuse(&self, lists: &[Vec<SearchHit>]) -> Vec<(SearchHit, f32)> {
        lists[1].iter().map(|h| (h.clone(), h.score)).collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fusion_is_pluggable() {
    let (t, c) = env().await;
    let q = c[0].clone();
    let r = t
        .search_hybrid("emb", q, "fts", "glacier river")
        .limit(7)
        .oversample(1)
        .fusion(Arc::new(TextOnly))
        .select(["id"])
        .collect()
        .await
        .unwrap();
    let x = t
        .search_text("fts", "glacier river")
        .limit(7)
        .collect()
        .await
        .unwrap();
    assert_eq!(fused(&r), fused(&x));
    // Rrf with another constant changes scores but not the rule.
    let r = t
        .search_hybrid("emb", c[1].clone(), "fts", "glacier")
        .fusion(Arc::new(Rrf { k: 1.0 }))
        .collect()
        .await
        .unwrap();
    assert!(r.hits.first().is_some_and(|h| h.score > 0.4));
    // Errors of either search are the hybrid search's.
    assert!(
        t.search_hybrid("emb", vec![1.0; 3], "fts", "x")
            .collect()
            .await
            .is_err()
    );
    assert!(
        t.search_hybrid("emb", c[0].clone(), "emb", "x")
            .collect()
            .await
            .is_err()
    );
}
