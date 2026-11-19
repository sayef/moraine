//! T12 format stability: the checked-in golden bundles must be read by this release and give
//! the recorded hits; deterministic kinds must rebuild them byte for byte (others must rebuild
//! to the same hits); and changed copies must be rejected with the documented error (cases V7,
//! I9). Regenerate deliberately with `cargo run -p moraine-testkit --bin golden`.

use moraine::fuzzing;
use moraine::index::bundle::{BundleBytes, BundleError, BundleView, Expect};
use moraine::testing::spec_hash;
use moraine_testkit::golden;
use serde_json::Value;

fn read(name: &str) -> Vec<u8> {
    let p = golden::dir().join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("{}: {e}; run the golden binary", p.display()))
}

fn view(b: &[u8]) -> Result<BundleView, BundleError> {
    BundleView::parse(BundleBytes::copy(b).expect("copy"))
}

/// Kinds whose builds are byte-identical for equal input.
const DETERMINISTIC: &[&str] = &["vec-tq", "key"];

/// Hits equal: same offsets in the same order, distances within float noise (SIMD kernels may
/// differ in the last bits between machines).
fn same_hits(a: &Value, b: &Value) -> bool {
    let (Some(a), Some(b)) = (a.as_array(), b.as_array()) else {
        return false;
    };
    if a.iter().any(|x| {
        x.as_array()
            .is_some_and(|x| !x.first().is_some_and(Value::is_array))
    }) {
        // Key lookups: exact JSON.
        return a == b;
    }
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            let (x, y) = (x.as_array().unwrap(), y.as_array().unwrap());
            x.len() == y.len()
                && x.iter().zip(y).all(|(h, g)| {
                    h[0] == g[0] && (h[1].as_f64().unwrap() - g[1].as_f64().unwrap()).abs() < 1e-5
                })
        })
}

#[test]
fn toy_bundle_is_current_and_reads_back() {
    let b = read("test-sum.osix");
    assert!(
        b == fuzzing::sample_bundle(),
        "the bundle writer's output changed"
    );
    let v = view(&b).unwrap();
    v.verify_sections().unwrap();
    assert_eq!(v.header().kind, "test-sum");
    assert_eq!(v.header().spec_hash, "0123456789abcdef");
    assert_eq!(v.header().indexed_rows, 99);
    assert_eq!(v.section("a").unwrap(), b"first");
}

#[test]
fn index_goldens_read_and_rebuild() {
    let parquet = read("vectors.parquet");
    for (kind, spec) in golden::kinds() {
        let name = kind.name();
        let stem = golden::file_stem(kind.as_ref(), &spec);
        let bundle = read(&format!("{stem}.osix"));
        let recorded: Value = serde_json::from_slice(&read(&format!("{stem}.hits.json"))).unwrap();
        // The release opens the golden bundle with the spec hash it computes today: a change to
        // canonicalisation or the library fingerprint fails here, not as a silent rebuild.
        let v = view(&bundle).unwrap();
        v.check(&Expect {
            kind: name,
            kind_format: kind.format_version(),
            spec_hash: &spec_hash(kind.as_ref(), &spec),
            source_rows: Some(2000),
            source_size: parquet.len() as u64,
        })
        .unwrap_or_else(|e| panic!("{name}: {e}"));
        v.verify_sections().unwrap();
        assert!(
            same_hits(&golden::hits(kind.as_ref(), &spec, &bundle), &recorded),
            "{name}: the golden bundle answers differently"
        );
        let rebuilt = golden::build(kind.as_ref(), &spec, &parquet);
        if DETERMINISTIC.contains(&name) {
            assert!(rebuilt == bundle, "{name}: rebuild is not byte-identical");
        }
        assert!(
            same_hits(&golden::hits(kind.as_ref(), &spec, &rebuilt), &recorded),
            "{name}: a rebuild answers differently"
        );
    }
}

/// The full-text golden (D53): the checked-in bundle opens under today's spec hash and answers
/// as recorded, and a rebuild from the checked-in text is byte-identical (D40).
#[test]
fn fts_golden_reads_and_rebuilds() {
    let text = read("text.parquet");
    let (kind, spec) = golden::fts();
    let bundle = read("fts.osix");
    let recorded: Value = serde_json::from_slice(&read("fts.hits.json")).unwrap();
    let v = view(&bundle).unwrap();
    v.check(&Expect {
        kind: "fts",
        kind_format: kind.format_version(),
        spec_hash: &spec_hash(kind.as_ref(), &spec),
        source_rows: Some(2000),
        source_size: text.len() as u64,
    })
    .unwrap();
    v.verify_sections().unwrap();
    assert!(same_hits(
        &golden::fts_hits(kind.as_ref(), &spec, &bundle),
        &recorded
    ));
    let rebuilt = golden::build(kind.as_ref(), &spec, &text);
    assert!(rebuilt == bundle, "fts: rebuild is not byte-identical");
}

#[test]
fn changed_versions_are_unsupported() {
    for name in [
        "fts.osix",
        "test-sum.osix",
        "vec-hnsw.osix",
        "vec-tq.osix",
        "key-cluster.osix",
        "key-pages.osix",
    ] {
        let b = read(name);
        let mut bumped = b.clone();
        bumped[4] = 2; // bundle format version
        fuzzing::bundle_reseal(&mut bumped);
        assert!(
            matches!(view(&bumped), Err(BundleError::Unsupported(_))),
            "{name}"
        );
        let mut flagged = b.clone();
        flagged[6] = 0x01; // an unknown flag
        fuzzing::bundle_reseal(&mut flagged);
        assert!(
            matches!(view(&flagged), Err(BundleError::Unsupported(_))),
            "{name}"
        );
        // A newer kind format version: readable structure, refused by the kind check.
        let mut kind = b.clone();
        kind[52] += 1;
        fuzzing::bundle_reseal(&mut kind);
        let v = view(&kind).unwrap();
        let h = v.header().clone();
        let e = v.check(&Expect {
            kind: &h.kind,
            kind_format: h.kind_format - 1,
            spec_hash: &h.spec_hash,
            source_rows: None,
            source_size: h.source_size,
        });
        assert!(matches!(e, Err(BundleError::Unsupported(_))), "{name}");
    }
}
