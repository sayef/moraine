//! The TurboQuant facts the design relies on (D24), each checked rather than assumed: a fixed
//! rotation seed makes equal rows give byte-identical sidecars; two sidecars' codes
//! concatenate into the merged file's sidecar; the dimension must be a multiple of 8 and bits
//! are 2 to 4; a 256-dimension vector costs 136 bytes at 4 bits; the codes are copied into
//! memory on open rather than used in place; and the library fingerprint is in the spec hash.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use moraine::index::bundle::{BundleBytes, BundleView};
use moraine::index::vector::HnswKind;
use moraine::index::vector::TurboKind;
use moraine::index::{
    BuildCtx, ExactScorer, FileIndex, IndexBuilder, IndexKind, IndexSpec, MergeInput, Order, Query,
    RowMap,
};
use moraine::testing::BundleBuild;
use moraine::testing::spec_hash;
use moraine::{Error, Result};
use moraine_testkit::SimRng;
use moraine_testkit::vectors::{batch, centres, clustered, schema};
use serde_json::json;

fn spec(dim: usize, extra: serde_json::Value) -> IndexSpec {
    let mut p = json!({"dim": dim});
    if let serde_json::Value::Object(e) = extra {
        p.as_object_mut().unwrap().extend(e);
    }
    let s = IndexSpec::new("emb", "vec-tq", &["v"]).params(p);
    TurboKind.resolve(&s, &schema()).unwrap()
}

fn rows(seed: u64, n: usize, dim: usize) -> RecordBatch {
    let c = centres(1, 8, dim);
    let v: Vec<Option<Vec<f32>>> = clustered(&mut SimRng::new(seed), &c, n, 0.4)
        .into_iter()
        .map(Some)
        .collect();
    batch(0, &v).project(&[2]).unwrap()
}

/// A bundle from rows fed in batches of `step` (the batch split must not matter).
fn build(kind: &dyn IndexKind, s: &IndexSpec, b: &RecordBatch, step: usize) -> Vec<u8> {
    let mut w = BundleBuild::new(
        kind,
        s,
        b.num_rows() as u64,
        1234,
        256 << 20,
        Cursor::new(Vec::new()),
    )
    .unwrap();
    let mut at = 0;
    while at < b.num_rows() {
        let n = step.min(b.num_rows() - at);
        w.add(at as u64, &b.slice(at, n)).unwrap();
        at += n;
    }
    w.finish().unwrap().0.into_inner()
}

fn view(b: &[u8]) -> BundleView {
    BundleView::parse(BundleBytes::copy(b).unwrap()).unwrap()
}

#[test]
fn equal_rows_give_byte_identical_sidecars() {
    let s = spec(64, json!({}));
    let b = rows(2, 3000, 64);
    let a = build(&TurboKind, &s, &b, 1024);
    assert_eq!(a, build(&TurboKind, &s, &b, 1024));
    assert_eq!(
        a,
        build(&TurboKind, &s, &b, 333),
        "batch boundaries do not matter"
    );
}

#[test]
fn concatenated_codes_equal_the_merged_files_sidecar() {
    let s = spec(64, json!({}));
    let (x, y) = (rows(3, 1500, 64), rows(4, 900, 64));
    let merged_rows = arrow::compute::concat_batches(&x.schema(), [&x, &y]).unwrap();
    let whole = build(&TurboKind, &s, &merged_rows, 1024);
    let (bx, by) = (
        view(&build(&TurboKind, &s, &x, 1024)),
        view(&build(&TurboKind, &s, &y, 1024)),
    );
    let mx = RowMap {
        new: (0..1500).map(Some).collect(),
    };
    let my = RowMap {
        new: (1500..2400).map(Some).collect(),
    };
    let ctx = BuildCtx::new(2400, 256 << 20, 1);
    let inputs = [MergeInput::new(&bx, &mx), MergeInput::new(&by, &my)];
    let builder = TurboKind
        .merge(&s, &inputs, &ctx)
        .unwrap()
        .expect("vec-tq merges");
    let out =
        BundleBuild::from_builder(&TurboKind, &s, builder, 2400, 1234, Cursor::new(Vec::new()))
            .unwrap()
            .finish()
            .unwrap()
            .0
            .into_inner();
    assert_eq!(
        out, whole,
        "merging codes is the same as quantising the merged file"
    );
    // A merge that drops rows (deleted by a rewrite) keeps the others' codes.
    let drop_odd = RowMap {
        new: (0..1500u64)
            .map(|i| (i % 2 == 0).then_some(i / 2))
            .collect(),
    };
    let only = [MergeInput::new(&bx, &drop_odd)];
    let b = TurboKind.merge(&s, &only, &ctx).unwrap().unwrap();
    let out = BundleBuild::from_builder(&TurboKind, &s, b, 750, 1, Cursor::new(Vec::new()))
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(out.2.indexed_rows, 750);
    // usearch has no merge: compaction rebuilds from the merged file's floats.
    let h = HnswKind
        .resolve(
            &IndexSpec::new("emb", "vec-hnsw", &["v"]).params(json!({"dim": 64})),
            &schema(),
        )
        .unwrap();
    assert!(HnswKind.merge(&h, &[], &ctx).unwrap().is_none());
}

#[test]
fn dimension_bits_and_metric_are_checked() {
    let sch: Arc<Schema> = schema();
    let bad = |p: serde_json::Value| {
        TurboKind.resolve(&IndexSpec::new("e", "vec-tq", &["v"]).params(p), &sch)
    };
    assert!(matches!(
        bad(json!({"dim": 12})),
        Err(Error::InvalidIndexSpec(_))
    ));
    assert!(matches!(
        bad(json!({"dim": 16, "bits": 1})),
        Err(Error::InvalidIndexSpec(_))
    ));
    assert!(matches!(
        bad(json!({"dim": 16, "bits": 5})),
        Err(Error::InvalidIndexSpec(_))
    ));
    assert!(matches!(
        bad(json!({"dim": 16, "metric": "l2"})),
        Err(Error::InvalidIndexSpec(_))
    ));
    for bits in [2, 3, 4] {
        bad(json!({"dim": 16, "bits": bits})).unwrap();
    }
    assert!(matches!(
        bad(json!({"dim": 32768})),
        Err(Error::InvalidIndexSpec(_))
    ));
}

#[test]
fn bytes_per_256_dimension_vector_at_4_bits() {
    let s = spec(256, json!({"bits": 4}));
    let n = 2000;
    let b = build(&TurboKind, &s, &rows(5, n, 256), 1024);
    let v = view(&b);
    let section = v.section("tq.0").unwrap().len();
    // 128 bytes of codes, 4 of scale and 4 of row offset, plus a 12-byte part header.
    assert_eq!(section, 12 + n * 136);
}

#[test]
fn codes_are_copied_into_memory_on_open() {
    let s = spec(256, json!({}));
    let n = 4000;
    let b = rows(6, n, 256);
    let tq = TurboKind
        .open(&s, view(&build(&TurboKind, &s, &b, 1024)))
        .unwrap();
    assert!(
        tq.resident_bytes() >= n * 128,
        "turbovec holds its codes: {} bytes",
        tq.resident_bytes()
    );
    let h = HnswKind
        .resolve(
            &IndexSpec::new("emb", "vec-hnsw", &["v"]).params(json!({"dim": 256})),
            &schema(),
        )
        .unwrap();
    let hn = HnswKind
        .open(&h, view(&build(&HnswKind, &h, &b, 1024)))
        .unwrap();
    assert!(
        hn.resident_bytes() < n * 128 / 4,
        "usearch views the mapped section: {} bytes",
        hn.resident_bytes()
    );
    // And both answer.
    let q: Vec<f32> = (0..256).map(|i| (i as f32 * 0.1).cos()).collect();
    let params = json!({});
    let query = Query::Vector {
        vector: &q,
        params: &params,
    };
    assert_eq!(tq.search(&query, None, 5).unwrap().len(), 5);
    assert_eq!(hn.search(&query, None, 5).unwrap().len(), 5);
}

/// A kind identical to vec-tq but for its library fingerprint.
struct Drifted;

impl IndexKind for Drifted {
    fn name(&self) -> &'static str {
        "vec-tq"
    }
    fn format_version(&self) -> u32 {
        TurboKind.format_version()
    }
    fn library_fingerprint(&self) -> String {
        format!("{}-changed", TurboKind.library_fingerprint())
    }
    fn order(&self) -> Order {
        Order::Ascending
    }
    fn resolve(&self, spec: &IndexSpec, schema: &Schema) -> Result<IndexSpec> {
        TurboKind.resolve(spec, schema)
    }
    fn wants_sidecar(&self, _: &IndexSpec, _: u64) -> bool {
        true
    }
    fn build_bytes_per_row(&self, _: &IndexSpec) -> usize {
        1
    }
    fn builder(&self, s: &IndexSpec, c: &BuildCtx) -> Result<Box<dyn IndexBuilder>> {
        TurboKind.builder(s, c)
    }
    fn open(&self, s: &IndexSpec, b: BundleView) -> Result<Arc<dyn FileIndex>> {
        TurboKind.open(s, b)
    }
    fn exact(&self, s: &IndexSpec, q: &Query<'_>) -> Result<Box<dyn ExactScorer>> {
        TurboKind.exact(s, q)
    }
}

#[test]
fn library_fingerprint_is_in_the_spec_hash() {
    let s = spec(64, json!({}));
    let fp = TurboKind.library_fingerprint();
    assert!(fp.starts_with("turbovec-1-"), "{fp}");
    assert_ne!(spec_hash(&TurboKind, &s), spec_hash(&Drifted, &s));
}
