//! The golden files of T12: inputs and expected outputs that every release must read, or
//! reject with the documented error. `src/bin/golden.rs` writes them; `tests/golden.rs` checks
//! them on every commit.
//!
//! - `test-sum.osix`: a toy bundle (format only).
//! - `vectors.parquet`: 2,000 rows of 16-dimension vectors, the source of the vector goldens.
//! - `<kind>.osix` and `<kind>.hits.json`: each vector kind's bundle built from that file, and
//!   the hits its queries returned when it was recorded.
//! - `text.parquet`, `fts.osix` and `fts.hits.json`: 2,000 documents, the full-text bundle
//!   built from them, and the hits of [`TEXT_QUERIES`] (D53).

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use moraine::index::bundle::{BundleBytes, BundleView};
use moraine::index::{IndexKind, IndexSpec, Query};
use moraine::testing::BundleBuild;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

use crate::SimRng;
use crate::vectors::{batch, centres, clustered};

pub const DIM: usize = 16;
const ROWS: usize = 2_000;

/// The directory holding the checked-in golden files.
pub fn dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("golden")
}

/// The kinds with goldens (vector kinds and the key index), and the spec each was built with.
pub fn kinds() -> Vec<(Arc<dyn IndexKind>, IndexSpec)> {
    let schema = crate::vectors::schema();
    let mut v: Vec<(Arc<dyn IndexKind>, IndexSpec)> = Vec::new();
    let hnsw: Arc<dyn IndexKind> = Arc::new(moraine::index::vector::HnswKind);
    let s = IndexSpec::new("emb", "vec-hnsw", &["v"]).params(json!({"dim": DIM}));
    let s = hnsw.resolve(&s, &schema).expect("golden spec");
    v.push((hnsw, s));
    let tq: Arc<dyn IndexKind> = Arc::new(moraine::index::vector::TurboKind);
    let s = IndexSpec::new("emb", "vec-tq", &["v"]).params(json!({"dim": DIM}));
    let s = tq.resolve(&s, &schema).expect("golden spec");
    v.push((tq, s));
    let key: Arc<dyn IndexKind> = Arc::new(moraine::testing::KeyKind);
    let s = IndexSpec::new("_moraine_key", "key", &["id", "grp"])
        .params(json!({"primary_key": ["id"], "cluster": ["grp"], "page_rows": 1024}));
    let s = key.resolve(&s, &schema).expect("golden spec");
    v.push((key, s));
    let key: Arc<dyn IndexKind> = Arc::new(moraine::testing::KeyKind);
    let s = IndexSpec::new("_moraine_key", "key", &["id"])
        .params(json!({"primary_key": ["id"], "cluster": [], "page_rows": 256}));
    v.push((key, s));
    v
}

/// The golden Parquet file (deterministic: fixed seed, fixed writer settings).
pub fn vectors_parquet() -> Vec<u8> {
    let c = centres(11, 8, DIM);
    let mut rng = SimRng::new(12);
    let rows: Vec<Option<Vec<f32>>> = clustered(&mut rng, &c, ROWS, 0.4)
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i % 97 != 3).then_some(v))
        .collect();
    let b = batch(0, &rows);
    let mut w = ArrowWriter::try_new(Vec::new(), b.schema(), None).expect("writer");
    w.write(&b).expect("write");
    w.into_inner().expect("parquet")
}

/// The queries the hit files record.
pub fn queries() -> Vec<Vec<f32>> {
    clustered(&mut SimRng::new(13), &centres(11, 8, DIM), 4, 0.4)
}

/// Builds `kind`'s bundle from `parquet` (rows fed in 1,024-row batches, as the builder job
/// does).
pub fn build(kind: &dyn IndexKind, spec: &IndexSpec, parquet: &[u8]) -> Vec<u8> {
    let r = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(parquet))
        .expect("golden parquet")
        .with_batch_size(1024)
        .build()
        .expect("reader");
    let mut b = BundleBuild::new(
        kind,
        spec,
        ROWS as u64,
        parquet.len() as u64,
        64 << 20,
        Cursor::new(Vec::new()),
    )
    .expect("build");
    let mut at = 0u64;
    for batch in r {
        let batch: RecordBatch = batch.expect("batch");
        let idx: Vec<usize> = spec
            .columns
            .iter()
            .map(|c| batch.schema().index_of(c).expect("golden column"))
            .collect();
        let v = batch.project(&idx).expect("indexed columns");
        b.add(at, &v).expect("add");
        at += batch.num_rows() as u64;
    }
    b.finish().expect("finish").0.into_inner()
}

/// The hits of [`queries`] against `bundle`, as recorded JSON: `[[offset, distance], ...]` per
/// query.
pub fn hits(kind: &dyn IndexKind, spec: &IndexSpec, bundle: &[u8]) -> Value {
    let view = BundleView::parse(BundleBytes::copy(bundle).expect("copy")).expect("bundle");
    let index = kind.open(spec, view).expect("open");
    let params = spec.search.clone();
    if kind.name() == "key" {
        // Candidate rows of present and absent keys: how many, and the first.
        let out: Vec<Value> = [0i64, 5, 1999, 777, 5000, -1]
            .iter()
            .map(|id| {
                let a: arrow::array::ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![*id]));
                let key = moraine::testing::encode_row(&[a], 0).expect("key");
                let h = index
                    .search(&Query::Key { keys: &[key] }, None, 5000)
                    .expect("lookup");
                json!([h.len(), h.first().map(|h| h.offset)])
            })
            .collect();
        return Value::Array(out);
    }
    let out: Vec<Value> = queries()
        .iter()
        .map(|q| {
            let h = index
                .search(
                    &Query::Vector {
                        vector: q,
                        params: &params,
                    },
                    None,
                    5,
                )
                .expect("search");
            Value::Array(h.iter().map(|h| json!([h.offset, h.score])).collect())
        })
        .collect();
    Value::Array(out)
}

/// The full-text kind and the spec of its golden bundle.
pub fn fts() -> (Arc<dyn IndexKind>, IndexSpec) {
    let kind: Arc<dyn IndexKind> = Arc::new(moraine::index::fts::FtsKind::new(Arc::new(
        moraine::index::fts::Tokenizers::default(),
    )));
    let s = IndexSpec::new("fts", "fts", &["title", "body"]);
    let s = kind
        .resolve(&s, &crate::text::schema())
        .expect("golden spec");
    (kind, s)
}

/// The golden text file: 2,000 documents of the test kit's Zipf vocabulary.
pub fn text_parquet() -> Vec<u8> {
    let mut rng = SimRng::new(21);
    let docs: Vec<_> = (0..ROWS)
        .map(|_| crate::text::document(&mut rng, 60, 30))
        .collect();
    let b = crate::text::batch(0, &docs);
    let mut w = ArrowWriter::try_new(Vec::new(), b.schema(), None).expect("writer");
    w.write(&b).expect("write");
    w.into_inner().expect("parquet")
}

/// The queries the full-text hit file records.
pub const TEXT_QUERIES: [&str; 5] = [
    "the",
    "title:of",
    "\"of the\"",
    "and AND -the",
    "body:in OR title:to",
];

/// The top 5 of [`TEXT_QUERIES`] against the bundle, with its own statistics, as recorded
/// JSON: `[[row, score], ...]` per query.
pub fn fts_hits(kind: &dyn IndexKind, spec: &IndexSpec, bundle: &[u8]) -> Value {
    let view = BundleView::parse(BundleBytes::copy(bundle).expect("copy")).expect("bundle");
    let index = kind.open(spec, view).expect("open");
    let fts = index
        .as_any()
        .downcast_ref::<moraine::index::fts::FtsIndex>()
        .expect("a full-text index");
    let searcher = fts.searcher();
    let schema = searcher.index().schema();
    let fields: Vec<tantivy::schema::Field> = ["title", "body"]
        .iter()
        .map(|n| schema.get_field(n).expect("golden field"))
        .collect();
    let parser = tantivy::query::QueryParser::for_index(searcher.index(), fields);
    let out: Vec<Value> = TEXT_QUERIES
        .iter()
        .map(|q| {
            let q = parser.parse_query(q).expect("golden query");
            let top = searcher
                .search(
                    &q,
                    &tantivy::collector::TopDocs::with_limit(5).order_by_score(),
                )
                .expect("search");
            Value::Array(
                top.iter()
                    .map(|(score, at)| json!([at.doc_id, score]))
                    .collect(),
            )
        })
        .collect();
    Value::Array(out)
}

/// Every golden file as (file name, bytes).
pub fn all() -> Vec<(String, Vec<u8>)> {
    let mut v = vec![("test-sum.osix".into(), moraine::fuzzing::sample_bundle())];
    let parquet = vectors_parquet();
    for (kind, spec) in kinds() {
        let bundle = build(kind.as_ref(), &spec, &parquet);
        let hits = hits(kind.as_ref(), &spec, &bundle);
        let name = file_stem(kind.as_ref(), &spec);
        v.push((format!("{name}.osix"), bundle));
        v.push((
            format!("{name}.hits.json"),
            format!("{hits:#}\n").into_bytes(),
        ));
    }
    v.push(("vectors.parquet".into(), parquet));
    let text = text_parquet();
    let (kind, spec) = fts();
    let bundle = build(kind.as_ref(), &spec, &text);
    let hits = fts_hits(kind.as_ref(), &spec, &bundle);
    v.push(("fts.osix".into(), bundle));
    v.push(("fts.hits.json".into(), format!("{hits:#}\n").into_bytes()));
    v.push(("text.parquet".into(), text));
    v
}

/// The golden file name of a kind and spec: the kind, plus the key index's variant.
pub fn file_stem(kind: &dyn IndexKind, spec: &IndexSpec) -> String {
    match kind.name() {
        "key" if spec.columns.len() > 1 => "key-cluster".into(),
        "key" => "key-pages".into(),
        other => other.into(),
    }
}

/// The golden table's database prefix inside `golden/table/`.
pub const TABLE_DB: &str = "db";

/// Rows of the golden table `t`: keys `key-<n>`, the operation that wrote them, a row number
/// and a group (every seventh null).
pub fn table_rows(op: &str, keys: std::ops::Range<usize>) -> RecordBatch {
    use arrow::array::{Int64Array, StringArray};
    let n = keys.len();
    RecordBatch::try_new(
        crate::datagen::row_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                keys.clone().map(|k| format!("key-{k:04}")),
            )),
            Arc::new(StringArray::from(vec![op; n])),
            Arc::new(Int64Array::from_iter_values(keys.clone().map(|k| k as i64))),
            Arc::new(Int64Array::from_iter(
                keys.map(|k| (k % 7 != 0).then_some((k % 3) as i64)),
            )),
        ],
    )
    .expect("golden rows")
}

/// A golden database written by this release (T12), as (object path, bytes): every kind of
/// object moraine writes into a table, from a fixed seed, so the files are the same on
/// every run of the same release.
///
/// - `t` (primary key `k`, clustered by `g`): appends, upserts, a delete, an update and an added
///   column, so its log holds every action kind moraine writes; a checkpoint and
///   `_last_checkpoint` after the first entries (D47); a pin and its manifest (D49); and the
///   generation file of a lease this handle still holds (D50).
/// - `v` (keyless, vectors): a `vec-tq` index and its sidecar, and one queued-write batch left
///   in the inbox (D51).
pub fn table() -> Vec<(String, Vec<u8>)> {
    use futures::TryStreamExt;
    use moraine::expr::{col, lit};
    use moraine::{Database, MaintainOptions, TableOptions};
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let store = Arc::new(InMemory::new());
        let clock = Arc::new(crate::ClientClock::new(crate::SimClock::new(), None, 0, 31));
        let db = Database::builder_for_store(store.clone(), TABLE_DB)
            .clock(clock)
            .open()
            .await
            .expect("golden database");

        // `t`: every action kind, then a checkpoint, a pin and a lease.
        let opts = TableOptions::default().primary_key(["k"]).cluster_by(["g"]);
        let t = db
            .create_table("t", crate::datagen::row_schema(), opts)
            .await
            .expect("table t");
        t.upsert(vec![table_rows("first", 0..40)])
            .await
            .expect("upsert");
        t.upsert(vec![table_rows("second", 30..60)])
            .await
            .expect("upsert");
        let outcome = t
            .txn("golden-app", 1)
            .upsert(vec![table_rows("third", 55..70)])
            .await
            .expect("upsert with an id");
        assert!(outcome.is_committed());
        t.delete(col("g").eq(lit(1i64))).await.expect("delete");
        t.update(col("g").eq(lit(2i64)), vec![("op".into(), lit("updated"))])
            .await
            .expect("update");
        let checkpoint = MaintainOptions::default()
            .leases(false)
            .build_indexes(None)
            .ingest(false)
            .compaction(None)
            .checkpoint_interval(Some(1))
            .vacuum(false);
        t.maintain(&checkpoint).await.expect("checkpoint");
        t.add_column(arrow::datatypes::Field::new(
            "note",
            arrow::datatypes::DataType::Utf8,
            true,
        ))
        .await
        .expect("add column");
        let v = t.refresh().await.expect("refresh");
        t.pin("golden", v).await.expect("pin");
        t.try_lease("golden", std::time::Duration::from_secs(300))
            .await
            .expect("lease")
            .expect("a free lease");

        // `v`: a vector index with a sidecar, and a batch left in the inbox.
        let v = db
            .create_table("v", crate::vectors::schema(), TableOptions::default())
            .await
            .expect("table v");
        v.create_index(
            IndexSpec::new("emb", "vec-tq", &["v"])
                .params(json!({"dim": DIM}))
                .policy(json!({"min_rows_for_ann": 1})),
        )
        .await
        .expect("vector index");
        let c = centres(11, 8, DIM);
        let mut rng = SimRng::new(32);
        let rows: Vec<Option<Vec<f32>>> = clustered(&mut rng, &c, 64, 0.4)
            .into_iter()
            .map(Some)
            .collect();
        v.append(vec![batch(0, &rows)]).await.expect("append");
        v.build_indexes(Default::default()).await.expect("build");
        let queued: Vec<Option<Vec<f32>>> = clustered(&mut rng, &c, 8, 0.4)
            .into_iter()
            .map(Some)
            .collect();
        v.submit(vec![batch(1000, &queued)]).await.expect("submit");

        let metas: Vec<_> = store.list(None).try_collect().await.expect("list");
        let mut out = Vec::new();
        for m in metas {
            let body = store
                .get(&m.location)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("body");
            out.push((m.location.to_string(), body.to_vec()));
        }
        out.sort();
        out
    })
}

/// Loads golden table files, as (object path, bytes), into a fresh in-memory store.
pub fn table_store(files: &[(String, Vec<u8>)]) -> Arc<object_store::memory::InMemory> {
    use object_store::{ObjectStoreExt, PutPayload};
    let store = Arc::new(object_store::memory::InMemory::new());
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    rt.block_on(async {
        for (name, bytes) in files {
            store
                .put(
                    &object_store::path::Path::from(name.as_str()),
                    PutPayload::from(bytes.clone()),
                )
                .await
                .expect("put");
        }
    });
    store
}

/// Reads the checked-in golden table files from `golden/table/`.
pub fn table_files() -> Vec<(String, Vec<u8>)> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<(String, Vec<u8>)>) {
        for e in std::fs::read_dir(dir).expect("golden table directory") {
            let p = e.expect("entry").path();
            if p.is_dir() {
                walk(root, &p, out);
            } else {
                let name = p.strip_prefix(root).expect("under the root");
                let name = name.to_str().expect("UTF-8 name").replace('\\', "/");
                out.push((name, std::fs::read(&p).expect("golden table file")));
            }
        }
    }
    let root = dir().join("table");
    let mut out = Vec::new();
    walk(&root, &root, &mut out);
    out.sort();
    out
}

/// Batches as JSON rows, sorted by their text.
fn json_rows(batches: &[RecordBatch]) -> Vec<Value> {
    let mut rows: Vec<Value> = Vec::new();
    for b in batches {
        let mut w = arrow::json::ArrayWriter::new(Vec::new());
        w.write(b).expect("json rows");
        w.finish().expect("json rows");
        let buf = w.into_inner();
        if !buf.is_empty() {
            let v: Value = serde_json::from_slice(&buf).expect("json rows");
            rows.extend(v.as_array().cloned().unwrap_or_default());
        }
    }
    rows.sort_by_key(|r| r.to_string());
    rows
}

/// What this release reads from golden table files, as recorded JSON: per table its version,
/// columns and rows; the pin, the transaction id, the index state, a vector search; and each
/// standalone object kind through the parser that reads it (checkpoint, `_last_checkpoint`,
/// log entries, pin manifest, lease generation, inbox batch). The golden test compares this
/// with `golden/table.expected.json`.
pub fn table_summary(files: &[(String, Vec<u8>)]) -> Value {
    use moraine::{Database, fuzzing};
    let store = table_store(files);
    let find = |pred: &dyn Fn(&str) -> bool| -> Vec<&(String, Vec<u8>)> {
        files.iter().filter(|(n, _)| pred(n)).collect()
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let db = Database::builder_for_store(store, TABLE_DB)
            .open()
            .await
            .expect("open the golden database");
        let mut names = db.table_names().await.expect("table names");
        names.sort();

        let t = db.open_table("t").await.expect("open t");
        let t_rows = json_rows(&t.scan().collect().await.expect("scan t"));
        let pinned = t.snapshot_at_pin("golden").await.expect("pinned version");
        let pinned_rows = pinned.scan().count().await.expect("scan the pin");

        let v = db.open_table("v").await.expect("open v");
        let v_rows = v.scan().count().await.expect("scan v");
        let status: Vec<Value> = v
            .index_status()
            .await
            .expect("index status")
            .iter()
            .map(|s| json!({"name": s.name, "files": s.files_total, "covered": s.files_covered}))
            .collect();
        let query = clustered(&mut SimRng::new(33), &centres(11, 8, DIM), 1, 0.4)
            .pop()
            .expect("a query");
        let hits = v
            .search_vector("emb", query)
            .limit(3)
            .collect()
            .await
            .expect("vector search");
        let ids = hits
            .batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Int64 ids");
        let hit_ids: Vec<i64> = ids.values().to_vec();

        // The standalone object kinds, through their parsers.
        let t_log: Vec<&[u8]> =
            find(&|n| n.starts_with("db/t/data/_delta_log/") && n.ends_with(".json"))
                .iter()
                .map(|(_, b)| b.as_slice())
                .collect();
        let checkpoints: Vec<Value> = find(&|n| n.ends_with(".checkpoint.parquet"))
            .iter()
            .map(|(n, b)| json!([n, fuzzing::checkpoint(b).expect("checkpoint")]))
            .collect();
        let last_checkpoint: Vec<Value> = find(&|n| n.ends_with("/_last_checkpoint"))
            .iter()
            .map(|(n, b)| json!([n, fuzzing::last_checkpoint(b)]))
            .collect();
        let manifests: Vec<Value> = find(&|n| n.contains("/meta/pins/"))
            .iter()
            .map(|(n, b)| json!([n, fuzzing::pin_manifest(b).expect("pin manifest")]))
            .collect();
        let leases: Vec<Value> = find(&|n| n.contains("/meta/leases/"))
            .iter()
            .map(|(n, b)| {
                let (g, holder, ttl) = fuzzing::lease(n, b).expect("lease");
                json!([n, g, holder, ttl])
            })
            .collect();
        let inbox: Vec<Value> = find(&|n| n.contains("/meta/inbox/"))
            .iter()
            .map(|(n, b)| json!([n, fuzzing::inbox(b).expect("inbox batch")]))
            .collect();
        let sidecars: Vec<Value> = find(&|n| n.ends_with(".osix"))
            .iter()
            .map(|(n, b)| json!([n, fuzzing::bundle(b).expect("sidecar")]))
            .collect();

        json!({
            "tables": names,
            "t": {
                "version": t.version(),
                "columns": t.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>(),
                "rows": t_rows,
                "pins": t.pins().expect("pins"),
                "pinned_rows": pinned_rows,
                "txn": t.last_txn_version("golden-app").await.expect("txn"),
                "log_files": fuzzing::log_entries(&t_log).expect("log entries"),
            },
            "v": {
                "version": v.version(),
                "rows": v_rows,
                "indexes": status,
                "hits": hit_ids,
            },
            "checkpoints": checkpoints,
            "last_checkpoint": last_checkpoint,
            "pin_manifests": manifests,
            "leases": leases,
            "inbox": inbox,
            "sidecars": sidecars,
        })
    })
}

/// The golden table and the summary this release reads from it, as written by the golden
/// binary: `table/...` files and `table.expected.json`.
pub fn table_with_summary() -> (Vec<(String, Vec<u8>)>, Vec<u8>) {
    let files = table();
    let summary = table_summary(&files);
    (files, format!("{summary:#}\n").into_bytes())
}
