//! The full-text benchmark (D46): moraine's per-file `fts` sidecars against one merged
//! tantivy index over the same rows (latency and ranking equality), and against Lance's
//! full-text index (latency and agreement of the top 10), from the `lance` crate kv0 builds.
//!
//! ```text
//! moraine-bench fts [--rows 100000,1000000] [--layouts 1,10,100] [--queries 1000]
//!                   [--workdir DIR] [--out results.jsonl] [--lance on]
//! ```
//!
//! The corpus is synthetic: words drawn from a 50,000-word vocabulary with a Zipf distribution
//! (exponent 1), 20 to 120 words per document. Queries are one to three words, OR'ed, drawn
//! log-uniformly over the vocabulary ranks so common and rare words both appear; k = 10.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use moraine::index::BuildOptions;
use moraine::index::CacheMode;
use moraine::{Database, IndexSpec, Table, TableOptions};
use serde_json::{Value, json};

use crate::{arg, dir_size, ms, pct, round};

const VOCAB: usize = 50_000;
const K: usize = 10;
const TOL: f32 = 1e-5;

/// A seeded generator (SplitMix64).
struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        moraine::testing::splitmix64(self.0)
    }
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
}

/// The word of rank `r`: letters only, so every tokenizer sees one token.
fn word(r: usize) -> String {
    let mut n = r + 26;
    let mut s = Vec::new();
    while n > 0 {
        s.push(b'a' + (n % 26) as u8);
        n /= 26;
    }
    String::from_utf8(s).expect("ascii")
}

struct Corpus {
    words: Vec<String>,
    /// Cumulative Zipf weights.
    cdf: Vec<f64>,
}

impl Corpus {
    fn new() -> Corpus {
        let mut cdf = Vec::with_capacity(VOCAB);
        let mut s = 0.0;
        for r in 0..VOCAB {
            s += 1.0 / (r as f64 + 1.0);
            cdf.push(s);
        }
        for c in &mut cdf {
            *c /= s;
        }
        Corpus {
            words: (0..VOCAB).map(word).collect(),
            cdf,
        }
    }

    fn zipf(&self, g: &mut Gen) -> &str {
        let u = g.unit();
        let r = self.cdf.partition_point(|c| *c < u).min(VOCAB - 1);
        &self.words[r]
    }

    fn doc(&self, g: &mut Gen) -> String {
        let n = 20 + (g.next() % 101) as usize;
        let mut d = String::with_capacity(n * 7);
        for i in 0..n {
            if i > 0 {
                d.push(' ');
            }
            d.push_str(self.zipf(g));
        }
        d
    }

    fn query(&self, g: &mut Gen) -> String {
        let n = 1 + (g.next() % 3) as usize;
        (0..n)
            .map(|_| {
                let r = ((VOCAB as f64).powf(g.unit()) as usize).min(VOCAB) - 1;
                self.words[r].clone()
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]))
}

fn emit(out: &Path, mut v: Value) {
    use std::io::Write;
    v["bench"] = json!("fts");
    let line = v.to_string();
    println!("{line}");
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out)
        .expect("out");
    writeln!(f, "{line}").expect("write");
}

/// (id, score) lists, best first.
type Ranking = Vec<(i64, f32)>;

/// One tantivy index over every row, built and merged into one segment on one thread like a
/// moraine sidecar, with the same tokenizer.
struct Merged {
    reader: tantivy::IndexReader,
    index: tantivy::Index,
    body: tantivy::schema::Field,
    bytes: u64,
    build_ms: f64,
}

fn merged(dir: &Path, docs: &[String]) -> Merged {
    use tantivy::schema::{FAST, Schema as TSchema, TEXT};
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("dir");
    let mut sb = TSchema::builder();
    let body = sb.add_text_field("body", TEXT);
    let id = sb.add_i64_field("id", FAST);
    let t0 = Instant::now();
    let index = tantivy::Index::create_in_dir(dir, sb.build()).expect("index");
    let mut w: tantivy::IndexWriter = index.writer_with_num_threads(1, 256 << 20).expect("writer");
    for (i, d) in docs.iter().enumerate() {
        let mut doc = tantivy::TantivyDocument::default();
        doc.add_i64(id, i as i64);
        doc.add_text(body, d);
        w.add_document(doc).expect("add");
    }
    w.commit().expect("commit");
    let segs = index.searchable_segment_ids().expect("segments");
    if segs.len() > 1 {
        w.merge(&segs).wait().expect("merge");
    }
    w.wait_merging_threads().expect("wait");
    let build_ms = ms(t0.elapsed());
    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()
        .expect("reader");
    Merged {
        reader,
        index,
        body,
        bytes: dir_size(dir),
        build_ms,
    }
}

impl Merged {
    fn search(&self, q: &str) -> (Ranking, f64) {
        use tantivy::collector::TopDocs;
        let t0 = Instant::now();
        let parser = tantivy::query::QueryParser::for_index(&self.index, vec![self.body]);
        let query = parser.parse_query(q).expect("query");
        let s = self.reader.searcher();
        let top = s
            .search(&query, &TopDocs::with_limit(K).order_by_score())
            .expect("search");
        let r: Ranking = top
            .into_iter()
            .map(|(score, a)| {
                let ids = s
                    .segment_reader(a.segment_ord)
                    .fast_fields()
                    .i64("id")
                    .expect("id");
                (ids.first(a.doc_id).expect("id"), score)
            })
            .collect();
        (r, ms(t0.elapsed()))
    }

    /// The full ranking of `q`, for the equality check (ties at the k-th place).
    fn all(&self, q: &str) -> Ranking {
        use tantivy::collector::TopDocs;
        let parser = tantivy::query::QueryParser::for_index(&self.index, vec![self.body]);
        let query = parser.parse_query(q).expect("query");
        let s = self.reader.searcher();
        let n = (s.num_docs() as usize).max(1);
        let top = s
            .search(&query, &TopDocs::with_limit(n).order_by_score())
            .expect("search");
        let mut r: Ranking = top
            .into_iter()
            .map(|(score, a)| {
                let ids = s
                    .segment_reader(a.segment_ord)
                    .fast_fields()
                    .i64("id")
                    .expect("id");
                (ids.first(a.doc_id).expect("id"), score)
            })
            .collect();
        r.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        r
    }
}

/// Whether `got` ranks like the merged index (see `moraine_testkit::text::check_ranking`):
/// the same scores at every place, every hit's score its merged score, and everything ranked
/// strictly above the k-th score present. Returns the largest score difference.
fn equal_ranking(got: &Ranking, want: &Ranking) -> Option<f32> {
    if got.len() != K.min(want.len()) {
        return None;
    }
    let by_id: std::collections::HashMap<i64, f32> = want.iter().copied().collect();
    let mut diff = 0f32;
    for (i, (id, s)) in got.iter().enumerate() {
        let m = by_id.get(id)?;
        diff = diff.max((m - s).abs()).max((want[i].1 - s).abs());
    }
    if diff > TOL {
        return None;
    }
    if let Some(last) = got.last() {
        let mine: std::collections::HashSet<i64> = got.iter().map(|x| x.0).collect();
        if want
            .iter()
            .any(|(id, s)| *s > last.1 + TOL && !mine.contains(id))
        {
            return None;
        }
    }
    Some(diff)
}

async fn search(t: &Table, q: &str) -> (Ranking, f64) {
    let t0 = Instant::now();
    let r = t
        .search_text("fts", q)
        .limit(K)
        .select(["id"])
        .collect()
        .await
        .expect("search");
    let took = ms(t0.elapsed());
    let ids = r
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids");
    let got = ids
        .values()
        .iter()
        .zip(&r.hits)
        .map(|(i, h)| (*i, h.score))
        .collect();
    (got, took)
}

/// p50, p99 and ranking equality of `qs` on `t` after a 50-query warm-up.
async fn measure(t: &Table, qs: &[String], want: &[Ranking]) -> Value {
    for q in qs.iter().take(50) {
        search(t, q).await;
    }
    let mut lat = Vec::with_capacity(qs.len());
    let (mut equal, mut max_diff) = (0usize, 0f32);
    for (q, w) in qs.iter().zip(want) {
        let (got, l) = search(t, q).await;
        lat.push(l);
        if let Some(d) = equal_ranking(&got, w) {
            equal += 1;
            max_diff = max_diff.max(d);
        }
    }
    json!({
        "p50_ms": round(pct(&mut lat.clone(), 0.5), 3),
        "p99_ms": round(pct(&mut lat, 0.99), 3),
        "ranking_equal": equal, "queries": qs.len(), "max_score_diff": max_diff,
    })
}

fn chrono_date() -> String {
    let out = std::process::Command::new("date")
        .arg("-u")
        .arg("+%Y-%m-%d")
        .output()
        .expect("date");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

pub async fn run(args: &[String]) {
    let rows_list: Vec<usize> = arg(args, "--rows")
        .unwrap_or_else(|| "100000,1000000".into())
        .split(',')
        .filter_map(|x| x.parse().ok())
        .collect();
    let layouts: Vec<usize> = arg(args, "--layouts")
        .unwrap_or_else(|| "1,10,100".into())
        .split(',')
        .filter_map(|x| x.parse().ok())
        .collect();
    let nq: usize = arg(args, "--queries")
        .and_then(|x| x.parse().ok())
        .unwrap_or(1000);
    let work = PathBuf::from(arg(args, "--workdir").unwrap_or_else(|| "bench-fts".into()));
    let out = PathBuf::from(arg(args, "--out").unwrap_or_else(|| "results-fts.jsonl".into()));
    let lance_on = arg(args, "--lance").as_deref() != Some("off");
    std::fs::create_dir_all(&work).expect("workdir");
    let date = chrono_date();
    let corpus = Corpus::new();
    for rows in rows_list {
        let set = format!("zipf{}k", rows / 1000);
        let mut g = Gen(rows as u64);
        let docs: Vec<String> = (0..rows).map(|_| corpus.doc(&mut g)).collect();
        let mut g = Gen(7);
        let qs: Vec<String> = (0..nq).map(|_| corpus.query(&mut g)).collect();

        // The reference: one merged index.
        let m = merged(&work.join("merged"), &docs);
        for q in qs.iter().take(50) {
            m.search(q);
        }
        let mut lat = Vec::new();
        for q in &qs {
            lat.push(m.search(q).1);
        }
        let want: Vec<Ranking> = qs.iter().map(|q| m.all(q)).collect();
        emit(
            &out,
            json!({
                "date": date, "set": set, "rows": rows, "files": 1, "engine": "tantivy-merged",
                "build_ms": round(m.build_ms, 0), "index_bytes": m.bytes,
                "p50_ms": round(pct(&mut lat.clone(), 0.5), 3),
                "p99_ms": round(pct(&mut lat, 0.99), 3),
            }),
        );
        let merged_top: Vec<Ranking> = qs.iter().map(|q| m.search(q).0).collect();
        drop(m);
        let _ = std::fs::remove_dir_all(work.join("merged"));

        for &files in &layouts {
            moraine_layout(&out, &date, &set, &work, &docs, files, &qs, &want).await;
        }
        if lance_on {
            lance_ref(&out, &date, &set, &work, &docs, &qs, &merged_top).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn moraine_layout(
    out: &Path,
    date: &str,
    set: &str,
    work: &Path,
    docs: &[String],
    files: usize,
    qs: &[String],
    want: &[Ranking],
) {
    let rows = docs.len();
    let dir = work.join(format!("moraine_{set}_{files}"));
    let cache = work.join(format!("cache_{set}_{files}"));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&cache);
    std::fs::create_dir_all(&dir).expect("dir");
    let open = |mode: CacheMode| {
        Database::builder(dir.to_str().expect("utf-8"))
            .sidecar_cache(mode)
            .small_table_bytes(0)
            .fts_memory(16 << 30)
            .open()
    };
    let disk = CacheMode::Disk {
        dir: cache.clone(),
        max_bytes: 64 << 30,
    };
    let db = open(disk.clone()).await.expect("db");
    let t = db
        .create_table("t", schema(), TableOptions::default())
        .await
        .expect("table");
    let per = rows / files;
    for f in 0..files {
        let lo = f * per;
        let hi = if f + 1 == files { rows } else { lo + per };
        let b = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(lo as i64..hi as i64)),
                Arc::new(StringArray::from_iter_values(docs[lo..hi].iter())),
            ],
        )
        .expect("batch");
        t.append(vec![b]).await.expect("append");
    }
    t.create_index(
        IndexSpec::new("fts", "fts", &["body"]).policy(json!({"min_rows_for_sidecar": 1})),
    )
    .await
    .expect("index");
    let t0 = Instant::now();
    let r = t
        .build_indexes(BuildOptions::default().parallelism(files.min(10)))
        .await
        .expect("build");
    let build_ms = ms(t0.elapsed());
    let idx_bytes = dir_size(&dir.join("t").join("idx"));
    // Sidecars, disk cache: the first query downloads and opens every sidecar.
    let fresh = open(disk.clone()).await.expect("db");
    let ft = fresh.open_table("t").await.expect("open");
    let t0 = Instant::now();
    search(&ft, &qs[0]).await;
    let first_ms = ms(t0.elapsed());
    let warm = measure(&ft, qs, want).await;
    emit(
        out,
        json!({
            "date": date, "set": set, "rows": rows, "files": files, "engine": "moraine-sidecars",
            "build_ms": round(build_ms, 0), "built": r.built, "index_bytes": idx_bytes,
            "first_query_ms": round(first_ms, 1), "warm": warm,
        }),
    );
    drop(ft);
    drop(fresh);
    // No sidecars: every file indexed in memory on the first query.
    let none = open(CacheMode::Disabled).await.expect("db");
    let nt = none.open_table("t").await.expect("open");
    let t0 = Instant::now();
    search(&nt, &qs[0]).await;
    let first_ms = ms(t0.elapsed());
    let warm = measure(&nt, qs, want).await;
    emit(
        out,
        json!({
            "date": date, "set": set, "rows": rows, "files": files, "engine": "moraine-in-memory",
            "first_query_ms": round(first_ms, 1), "memory_bytes": none.cache_usage().fts_bytes,
            "warm": warm,
        }),
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&cache);
}

/// Lance's full-text (inverted) index on the same rows: simple tokenizer, lower case, no
/// stemming or stop words, as the tantivy default tokenizer.
async fn lance_ref(
    out: &Path,
    date: &str,
    set: &str,
    work: &Path,
    docs: &[String],
    qs: &[String],
    merged_top: &[Ranking],
) {
    use futures::TryStreamExt;
    use lance::deps::arrow_array::{self as la, Int64Array as LInt64, RecordBatchIterator};
    use lance::deps::arrow_schema as ls;
    use lance::index::DatasetIndexExt;
    use lance_index::scalar::FullTextSearchQuery;
    use lance_index::scalar::InvertedIndexParams;
    let rows = docs.len();
    let uri = work.join(format!("lance_{set}"));
    let _ = std::fs::remove_dir_all(&uri);
    let schema = Arc::new(ls::Schema::new(vec![
        ls::Field::new("id", ls::DataType::Int64, false),
        ls::Field::new("body", ls::DataType::Utf8, false),
    ]));
    let mut batches = Vec::new();
    for start in (0..rows).step_by(100_000) {
        let n = 100_000.min(rows - start);
        batches.push(Ok(la::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(LInt64::from_iter_values(
                    (start..start + n).map(|i| i as i64),
                )),
                Arc::new(la::StringArray::from_iter_values(
                    docs[start..start + n].iter(),
                )),
            ],
        )
        .expect("batch")));
    }
    let reader = RecordBatchIterator::new(batches, schema.clone());
    let mut ds = lance::Dataset::write(reader, uri.to_str().unwrap(), None)
        .await
        .expect("lance write");
    let params = InvertedIndexParams::default()
        .stem(false)
        .remove_stop_words(false)
        .lower_case(true)
        .ascii_folding(false);
    let t0 = Instant::now();
    ds.create_index(
        &["body"],
        lance_index::IndexType::Inverted,
        Some("fts".into()),
        &params,
        true,
    )
    .await
    .expect("lance index");
    let build_ms = ms(t0.elapsed());
    let index_bytes = dir_size(&uri.join("_indices"));
    let search = |q: &str| {
        let ds = ds.clone();
        let q = q.to_string();
        async move {
            let mut s = ds.scan();
            s.full_text_search(FullTextSearchQuery::new(q))
                .expect("fts")
                .limit(Some(K as i64), None)
                .expect("limit")
                .project(&["id"])
                .expect("project");
            let t0 = Instant::now();
            let b: Vec<la::RecordBatch> = s
                .try_into_stream()
                .await
                .expect("scan")
                .try_collect()
                .await
                .expect("rows");
            let took = ms(t0.elapsed());
            let ids: Vec<i64> = b
                .iter()
                .flat_map(|b| {
                    let a = b.column_by_name("id").expect("id");
                    let a = a.as_any().downcast_ref::<LInt64>().expect("ids");
                    a.values().to_vec()
                })
                .collect();
            (ids, took)
        }
    };
    for q in qs.iter().take(50) {
        search(q).await;
    }
    let mut lat = Vec::new();
    let (mut overlap, mut total, mut same_top1) = (0usize, 0usize, 0usize);
    for (q, m) in qs.iter().zip(merged_top) {
        let (ids, l) = search(q).await;
        lat.push(l);
        let mine: std::collections::HashSet<i64> = m.iter().map(|x| x.0).collect();
        overlap += ids.iter().filter(|i| mine.contains(i)).count();
        total += m.len();
        same_top1 += usize::from(ids.first() == m.first().map(|x| &x.0));
    }
    emit(
        out,
        json!({
            "date": date, "set": set, "rows": rows, "files": 1, "engine": "lance-inverted",
            "build_ms": round(build_ms, 0), "index_bytes": index_bytes,
            "p50_ms": round(pct(&mut lat.clone(), 0.5), 3),
            "p99_ms": round(pct(&mut lat, 0.99), 3),
            "top10_overlap_with_merged": round(overlap as f64 / total.max(1) as f64, 4),
            "top1_equal": same_top1, "queries": qs.len(),
        }),
    );
    let _ = std::fs::remove_dir_all(&uri);
}
