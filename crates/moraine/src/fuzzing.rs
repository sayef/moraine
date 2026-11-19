//! Entry points for the corruption fuzz tests (D29; case X3): each runs one of moraine's
//! parsers over bytes as they would arrive from the store, so the tests can feed them mutated
//! input directly, thousands of times a second. Not part of the stable API.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use object_store::memory::InMemory;
use object_store::path::Path;
use url::Url;

use crate::error::Result;
use crate::log::Loaded;
use crate::log::delta::DeltaLog;
use crate::options::TableDef;
use crate::snapshot::Snapshot;

/// Parses `bodies` as log entries 0, 1, ... and builds the snapshot they describe, as opening a
/// table without a checkpoint does: actions, metadata and its schema, table properties, file
/// statistics and tags, and the version sequence. Returns the snapshot's file count.
pub fn log_entries(bodies: &[&[u8]]) -> Result<usize> {
    let url = Url::parse("memory:///fuzz/data").expect("static URL");
    let log = DeltaLog::new(Arc::new(InMemory::new()), Path::from("fuzz/data"), url, 1);
    let mut entries = Vec::with_capacity(bodies.len());
    let mut schema: Option<SchemaRef> = None;
    for (v, body) in bodies.iter().enumerate() {
        let e = log.parse(v as u64, body, schema.as_ref())?;
        if let Some(m) = &e.meta {
            schema = Some(m.schema.clone());
        }
        entries.push(e);
    }
    let snap = Snapshot::from_loaded(
        Loaded {
            base: None,
            base_txns: Vec::new(),
            base_txn_times: Vec::new(),
            entries,
        },
        Duration::ZERO,
    )?;
    Ok(snap.files.len())
}

/// Reads moraine's table options back from Delta table properties, and the index specs
/// (`moraine.indexes`) with their canonical JSON.
pub fn table_properties(p: &BTreeMap<String, String>) -> Result<()> {
    TableDef::from_properties(p)?;
    for s in crate::index::spec::parse_specs(p)? {
        let _ = crate::index::spec::canonical_json(&s.params);
    }
    Ok(())
}

/// Parses an `add` action's statistics JSON against `schema`. Malformed statistics are not an
/// error (the file is then never pruned), so this returns how many columns kept a range.
pub fn file_stats(text: &str, schema: &SchemaRef, exact: bool) -> usize {
    let s = crate::stats::from_delta_json(Some(text), schema, exact);
    schema
        .fields()
        .iter()
        .filter(|f| s.column(f.name()).is_some())
        .count()
}

/// Parses `bytes` as a sidecar bundle and, when the structure holds, verifies its section
/// checksums (what installing a downloaded bundle in the cache does). Returns the section count.
pub fn bundle(bytes: &[u8]) -> std::result::Result<usize, crate::index::bundle::BundleError> {
    use crate::index::bundle::{BundleBytes, BundleView};
    let v = BundleView::parse(BundleBytes::copy(bytes)?)?;
    v.verify_sections()?;
    for s in v.sections() {
        let _ = v.section(&s.name);
    }
    Ok(v.sections().len())
}

/// Recomputes a bundle's footer checksum over its header and section table, where the footer
/// still locates a table inside the bytes, so mutated headers and tables reach the checks
/// behind the checksum. Leaves other inputs alone.
pub fn bundle_reseal(bytes: &mut [u8]) {
    let n = bytes.len();
    if n < 64 + 24 {
        return;
    }
    let f = n - 24;
    let table = u64::from_le_bytes(bytes[f..f + 8].try_into().unwrap_or([0; 8]));
    if table < 64 || table > f as u64 {
        return;
    }
    let mut d = xxhash_rust::xxh3::Xxh3::new();
    d.update(&bytes[..64]);
    d.update(&bytes[table as usize..f]);
    bytes[f + 8..f + 16].copy_from_slice(&d.digest().to_le_bytes());
}

/// A small valid bundle with three sections, for the fuzz tests to mutate.
pub fn sample_bundle() -> Vec<u8> {
    use crate::index::bundle::{BundleMeta, write_to_vec};
    write_to_vec(
        BundleMeta {
            kind: "test-sum".into(),
            kind_format: 1,
            spec_hash: "0123456789abcdef".into(),
            source_rows: 100,
            source_size: 4096,
        },
        &[("a", b"first"), ("b.0", &[3u8; 700]), ("b.1", b"")],
        99,
    )
    .unwrap_or_default()
}

/// Parses `body` as a checkpoint file (D47) with moraine's own reader and builds the snapshot
/// it describes, as opening a table from a checkpoint does. Returns the snapshot's file count.
/// (A checkpoint this reader rejects falls back to delta-rs when a table opens; that path is
/// delta-rs's and not fuzzed here.)
pub fn checkpoint(body: &[u8]) -> Result<usize> {
    let e = crate::log::delta::parse_checkpoint(7, bytes::Bytes::copy_from_slice(body))?;
    let meta = e
        .meta
        .ok_or_else(|| crate::Error::corrupt("checkpoint", "no metadata"))?;
    let snap = Snapshot::from_loaded(
        Loaded {
            base: Some((7, meta, e.adds)),
            base_txns: e.txns,
            base_txn_times: e.txn_times,
            entries: Vec::new(),
        },
        Duration::ZERO,
    )?;
    Ok(snap.files.len())
}

/// Reads `_last_checkpoint` as opening a table does: the version it names, if any.
pub fn last_checkpoint(body: &[u8]) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.get("version")?.as_u64()
}

/// Parses `body` as a pin manifest (D49), `meta/pins/<name>/<version>.json`, and builds the
/// pinned snapshot from it, as `Table::snapshot_at_pin` does. Returns its file count.
pub fn pin_manifest(body: &[u8]) -> Result<usize> {
    use crate::log::CommitLayer;
    let url = Url::parse("memory:///fuzz/data").expect("static URL");
    let log = DeltaLog::new(Arc::new(InMemory::new()), Path::from("fuzz/data"), url, 1);
    let e = log.parse_entry(3, body)?;
    let meta = e
        .meta
        .ok_or_else(|| crate::Error::corrupt("pin manifest", "no metadata"))?;
    let snap = Snapshot::from_loaded(
        Loaded {
            base: Some((3, meta, e.adds)),
            base_txns: Vec::new(),
            base_txn_times: Vec::new(),
            entries: Vec::new(),
        },
        Duration::ZERO,
    )?;
    Ok(snap.files.len())
}

/// Parses a lease generation object (D50) at `location` (`.../<20 digits>.json`) as acquiring
/// the lease does. Returns the generation, the holder and the time to live in milliseconds.
pub fn lease(location: &str, body: &[u8]) -> Result<(u64, String, u64)> {
    crate::maintain::lease::parse_generation(location, body)
}

/// Decodes a queued-write inbox object (D51), one Arrow IPC stream, as the ingestor does.
/// Returns the row count.
pub fn inbox(body: &[u8]) -> Result<usize> {
    let batches = crate::maintain::inbox::decode(bytes::Bytes::copy_from_slice(body))?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

/// Parses `text` as a full-text query against a two-column index, with
/// `search` as the search parameters (`conjunction`, `boosts`). `Ok(false)` for a query that
/// matches nothing by construction (case F1).
#[cfg(feature = "fts")]
pub fn text_query(search: &serde_json::Value, text: &str) -> Result<bool> {
    use crate::index::IndexKind;
    use crate::index::fts::{FtsKind, Tokenizers};
    use arrow::datatypes::{DataType, Field, Schema};
    let kind = FtsKind::new(Arc::new(Tokenizers::default()));
    let schema = Schema::new(vec![
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]);
    let spec = kind.resolve(
        &crate::IndexSpec::new("fts", "fts", &["title", "body"]),
        &schema,
    )?;
    crate::index::fts::check_search(search, &spec.columns)?;
    Ok(crate::index::fts::search::parse(&kind, &spec, search, text)?.is_some())
}

/// The pins table property (`moraine.pins`, D49) in `p`.
pub fn pins(p: &BTreeMap<String, String>) -> Result<usize> {
    Ok(crate::table::parse_pins(p)?.len())
}

/// A schema with a column of every type file statistics are read for, for [`file_stats`].
pub fn stats_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    Arc::new(Schema::new(vec![
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("ls", DataType::LargeUtf8, true),
        Field::new("d", DataType::Date32, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "ntz",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("b", DataType::Boolean, true),
    ]))
}

/// Parses `raw` as the `moraine.indexes` table property, then resolves each spec with the
/// built-in kind it names (against a schema with an integer key, a vector and two text
/// columns) and computes its spec hash, as creating an index and matching its sidecars do.
/// Returns how many specs resolved.
pub fn index_specs(raw: &str) -> Result<usize> {
    use crate::index::IndexKind;
    use arrow::datatypes::{DataType, Field, Schema};
    let p = BTreeMap::from([(
        crate::index::spec::INDEXES_PROPERTY.to_string(),
        raw.to_string(),
    )]);
    let specs = crate::index::spec::parse_specs(&p)?;
    let item = Arc::new(Field::new("element", DataType::Float32, true));
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::List(item), true),
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]);
    let kinds: Vec<Arc<dyn IndexKind>> = vec![
        Arc::new(crate::index::key::KeyKind),
        #[cfg(feature = "vector")]
        Arc::new(crate::index::vector::usearch::HnswKind),
        #[cfg(feature = "turbovec")]
        Arc::new(crate::index::vector::turbo::TurboKind),
        #[cfg(feature = "fts")]
        Arc::new(crate::index::fts::FtsKind::new(Arc::new(
            crate::index::fts::Tokenizers::default(),
        ))),
    ];
    let mut resolved = 0;
    for s in specs {
        let _ = crate::index::spec::canonical_json(&s.params);
        let Some(kind) = kinds.iter().find(|k| k.name() == s.kind) else {
            continue;
        };
        if let Ok(r) = kind.resolve(&s, &schema) {
            let _ = crate::index::spec::spec_hash(kind.as_ref(), &r);
            resolved += 1;
        }
    }
    Ok(resolved)
}
