//! Spike item 9: tantivy scores with external BM25 statistics (`Bm25StatisticsProvider`), so
//! per-file indexes can score as one table-wide index would.
//!
//! 20,000 synthetic documents (Zipf-distributed words from a fixed vocabulary) split across two
//! per-file indexes, plus one merged index over all of them. Each per-file index is searched with
//! a provider that sums the statistics of both; the scores are compared with the merged index.

use std::collections::HashMap;
use std::time::Instant;

use tantivy::collector::TopDocs;
use tantivy::query::{Bm25StatisticsProvider, QueryParser};
use tantivy::schema::{FAST, Field, INDEXED, STORED, Schema, TEXT};
use tantivy::{Index, IndexWriter, Searcher, Term, doc};

const DOCS: u64 = 20_000;
const VOCAB: &[&str] = &[
    "object",
    "storage",
    "vector",
    "search",
    "delta",
    "table",
    "commit",
    "writer",
    "reader",
    "index",
    "sidecar",
    "parquet",
    "bucket",
    "snapshot",
    "compaction",
    "vacuum",
    "lease",
    "blob",
    "cache",
    "query",
    "latency",
    "replica",
    "segment",
    "token",
    "score",
    "filter",
    "merge",
    "append",
    "upsert",
    "delete",
    "schema",
    "column",
    "row",
    "file",
    "version",
    "checkpoint",
    "log",
    "retry",
    "conflict",
    "backoff",
];

fn text(i: u64) -> String {
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let len = 5 + (i % 40) as usize;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Zipf-like: index ~ 1 / u
            let u = ((x % 10_000) as f64 + 1.0) / 10_000.0;
            let r = ((1.0 / u) as usize).min(VOCAB.len()) - 1;
            VOCAB[r]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn build(schema: &Schema, body: Field, row: Field, range: std::ops::Range<u64>) -> Index {
    let index = Index::create_in_ram(schema.clone());
    let mut w: IndexWriter = index.writer(50_000_000).unwrap();
    for i in range {
        w.add_document(doc!(body => text(i), row => i)).unwrap();
    }
    w.commit().unwrap();
    index
}

struct Summed<'a>(Vec<&'a Searcher>);

impl Bm25StatisticsProvider for Summed<'_> {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        self.0.iter().map(|s| s.total_num_tokens(field)).sum()
    }
    fn total_num_docs(&self) -> tantivy::Result<u64> {
        self.0.iter().map(|s| s.total_num_docs()).sum()
    }
    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        self.0.iter().map(|s| s.doc_freq(term)).sum()
    }
}

fn hits(
    searcher: &Searcher,
    query: &dyn tantivy::query::Query,
    provider: Option<&dyn Bm25StatisticsProvider>,
) -> Vec<(u64, f32)> {
    let collector = TopDocs::with_limit(100).order_by_score();
    let top = match provider {
        Some(p) => searcher
            .search_with_statistics_provider(query, &collector, p)
            .unwrap(),
        None => searcher.search(query, &collector).unwrap(),
    };
    top.into_iter()
        .map(|(score, addr)| {
            let seg = searcher.segment_reader(addr.segment_ord);
            let rows = seg.fast_fields().u64("row").unwrap();
            (rows.first(addr.doc_id).unwrap(), score)
        })
        .collect()
}

#[test]
fn item9_external_bm25_statistics() {
    let mut sb = Schema::builder();
    let body = sb.add_text_field("body", TEXT);
    let row = sb.add_u64_field("row", FAST | INDEXED | STORED);
    let schema = sb.build();
    let t0 = Instant::now();
    let a = build(&schema, body, row, 0..DOCS / 2);
    let b = build(&schema, body, row, DOCS / 2..DOCS);
    let m = build(&schema, body, row, 0..DOCS);
    let build_ms = t0.elapsed().as_secs_f64() * 1e3;
    let (ra, rb, rm) = (
        a.reader().unwrap(),
        b.reader().unwrap(),
        m.reader().unwrap(),
    );
    let (sa, sbr, sm) = (ra.searcher(), rb.searcher(), rm.searcher());
    let provider = Summed(vec![&sa, &sbr]);

    for q in [
        "sidecar compaction",
        "vacuum OR lease",
        "checkpoint backoff conflict",
        "\"object storage\"",
    ] {
        // Positions are recorded for TEXT fields, so the phrase query runs on both routes.
        let parser = QueryParser::for_index(&m, vec![body]);
        let query = parser.parse_query(q).unwrap();
        let merged: HashMap<u64, f32> = hits(&sm, &query, None).into_iter().collect();
        let mut with_stats: Vec<(u64, f32)> = hits(&sa, &query, Some(&provider));
        with_stats.extend(hits(&sbr, &query, Some(&provider)));
        let mut per_file: Vec<(u64, f32)> = hits(&sa, &query, None);
        per_file.extend(hits(&sbr, &query, None));
        let sort = |v: &mut Vec<(u64, f32)>| {
            v.sort_by(|x, y| y.1.partial_cmp(&x.1).unwrap().then(x.0.cmp(&y.0)))
        };
        sort(&mut with_stats);
        sort(&mut per_file);
        let mut merged_sorted: Vec<(u64, f32)> = merged.iter().map(|(k, v)| (*k, *v)).collect();
        sort(&mut merged_sorted);
        let max_diff = |v: &[(u64, f32)]| {
            v.iter()
                .take(50)
                .filter_map(|(d, s)| merged.get(d).map(|ms| (ms - s).abs()))
                .fold(0f32, f32::max)
        };
        let top20 = |v: &[(u64, f32)]| v.iter().take(20).map(|x| x.0).collect::<Vec<_>>();
        println!(
            "RESULT item9 query={q:?} max_abs_score_diff(top50): provider={:.2e} per_file={:.2e}; \
             top20_equal: provider={} per_file={}; build_three_indexes_ms={build_ms:.0}",
            max_diff(&with_stats),
            max_diff(&per_file),
            top20(&with_stats) == top20(&merged_sorted),
            top20(&per_file) == top20(&merged_sorted),
        );
        assert!(max_diff(&with_stats) <= 1e-5);
    }
}
