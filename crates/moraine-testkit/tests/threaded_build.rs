//! usearch graphs built on several threads (D80): every row stays reachable, so a search for a
//! row's own vector finds it, as with a graph built on one thread.

use std::io::Cursor;
use std::sync::Arc;

use moraine::index::bundle::{BundleBytes, BundleView};
use moraine::index::{IndexKind, IndexSpec, Query};
use moraine::testing::BundleBuild;
use moraine_testkit::SimRng;
use moraine_testkit::vectors::{batch, centres, clustered, schema};
use serde_json::json;

#[test]
fn every_row_of_a_threaded_build_is_reachable() {
    let (dim, n) = (64, 8_000);
    let c = centres(3, 64, dim);
    let rows = clustered(&mut SimRng::new(4), &c, n, 0.4);
    let b = batch(0, &rows.iter().cloned().map(Some).collect::<Vec<_>>());
    let v = b.project(&[b.schema().index_of("v").unwrap()]).unwrap();
    let kind: Arc<dyn IndexKind> = Arc::new(moraine::index::vector::HnswKind);
    let spec = kind
        .resolve(
            &IndexSpec::new("emb", "vec-hnsw", &["v"])
                .params(json!({"dim": dim, "metric": "cosine"})),
            &schema(),
        )
        .unwrap();
    for threads in [1, 8] {
        let mut build = BundleBuild::with_threads(
            kind.as_ref(),
            &spec,
            n as u64,
            1,
            256 << 20,
            threads,
            Cursor::new(Vec::new()),
        )
        .unwrap();
        build.add(0, &v).unwrap();
        let (w, _, summary) = build.finish().unwrap();
        assert_eq!(summary.indexed_rows, n as u64);
        let view = BundleView::parse(BundleBytes::copy(&w.into_inner()).unwrap()).unwrap();
        let index = kind.open(&spec, view).unwrap();
        let params = json!({});
        let missing: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(i, r)| {
                let q = Query::Vector {
                    vector: r,
                    params: &params,
                };
                let h = index.search(&q, None, 1).unwrap();
                h.first()
                    .is_none_or(|h| h.offset != *i as u64 && h.score > 1e-6)
            })
            .map(|(i, _)| i)
            .collect();
        assert!(
            missing.is_empty(),
            "{threads} threads: {missing:?} not found"
        );
    }
}
