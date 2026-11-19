//! Text data for full-text tests (a data generator): documents of words drawn
//! with a Zipf-like distribution from a vocabulary, as row batches with an id, a group and two
//! text columns, `title` and `body`.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::rng::SimRng;

/// Words the generators draw from: ordinary English words, some sharing a stem
/// (`write`, `writer`, `writes`), so stemming tokenizers change results.
pub const WORDS: &[&str] = &[
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
    "write",
    "writes",
    "writing",
    "reads",
    "reading",
    "indexes",
    "indexing",
    "searches",
    "searching",
    "glacier",
    "moraine",
    "valley",
    "stone",
    "river",
    "summit",
    "ridge",
    "snow",
    "ice",
    "melt",
    "flow",
];

/// The table schema of [`batch`]: `id` (key), `g` (a group, `id % 7`), `title` and `body`
/// (nullable strings).
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("g", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]))
}

/// One word of the first `vocab` words, the `r`-th with weight about `1 / (r + 1)`.
pub fn word(rng: &mut SimRng, vocab: usize) -> &'static str {
    let vocab = vocab.clamp(1, WORDS.len());
    let u = (rng.below(1_000_000) as f64 + 1.0) / 1_000_000.0;
    // Inverse of the harmonic distribution's tail, cut at the vocabulary.
    let r = ((vocab as f64 + 1.0).powf(u) - 1.0) as usize;
    WORDS[r.min(vocab - 1)]
}

/// `n` words joined by spaces.
pub fn sentence(rng: &mut SimRng, vocab: usize, n: usize) -> String {
    (0..n)
        .map(|_| word(rng, vocab))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A document: a title of 1 to 6 words and a body of 0 to `max_body` words; about 5 % of
/// titles and bodies are null and 3 % empty (case F4).
pub fn document(
    rng: &mut SimRng,
    vocab: usize,
    max_body: usize,
) -> (Option<String>, Option<String>) {
    let mut text = |lo: u64, hi: u64| -> Option<String> {
        match rng.below(100) {
            0..5 => None,
            5..8 => Some(String::new()),
            _ => {
                let n = rng.range(lo, hi + 1) as usize;
                Some(sentence(rng, vocab, n))
            }
        }
    };
    let title = text(1, 6);
    let body = text(0, max_body.max(1) as u64);
    (title, body)
}

/// Rows `first_id..` holding `docs`.
pub fn batch(first_id: i64, docs: &[(Option<String>, Option<String>)]) -> RecordBatch {
    let ids: Vec<i64> = (0..docs.len() as i64).map(|i| first_id + i).collect();
    let g: Vec<i64> = ids.iter().map(|i| i % 7).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(g)),
            Arc::new(StringArray::from_iter(docs.iter().map(|d| d.0.clone()))),
            Arc::new(StringArray::from_iter(docs.iter().map(|d| d.1.clone()))),
        ],
    )
    .expect("text batch")
}

/// One tantivy index over every row of a table, built with tantivy alone: the reference that
/// moraine's per-file full-text search must rank like (T9, case F5).
pub struct MergedIndex {
    index: tantivy::Index,
    reader: tantivy::IndexReader,
    fields: Vec<(String, tantivy::schema::Field)>,
}

/// A row of a [`MergedIndex`]: id and the text of each column (`None` for null).
pub type TextRow = (i64, Vec<Option<String>>);

impl MergedIndex {
    /// Indexes `rows` with one text field per name in `columns`, each tokenised by the
    /// tantivy tokenizer named in `tokenizers` (tantivy's own defaults, or `ngram2`) and
    /// recording `record`.
    pub fn build(
        rows: &[TextRow],
        columns: &[(&str, &str, tantivy::schema::IndexRecordOption)],
    ) -> MergedIndex {
        use tantivy::schema::{FAST, Schema, TextFieldIndexing, TextOptions};
        let mut sb = Schema::builder();
        let fields: Vec<(String, tantivy::schema::Field)> = columns
            .iter()
            .map(|(name, tok, rec)| {
                let opts = TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(tok)
                        .set_index_option(*rec),
                );
                (name.to_string(), sb.add_text_field(name, opts))
            })
            .collect();
        let id = sb.add_i64_field("id", FAST);
        let index = tantivy::Index::create_in_ram(sb.build());
        index.tokenizers().register(
            "ngram2",
            tantivy::tokenizer::TextAnalyzer::builder(
                tantivy::tokenizer::NgramTokenizer::new(2, 2, false).expect("ngram"),
            )
            .filter(tantivy::tokenizer::LowerCaser)
            .build(),
        );
        let mut w: tantivy::IndexWriter = index.writer(50_000_000).expect("writer");
        for (rid, texts) in rows {
            let mut d = tantivy::TantivyDocument::default();
            d.add_i64(id, *rid);
            for ((_, f), t) in fields.iter().zip(texts) {
                if let Some(t) = t {
                    d.add_text(*f, t);
                }
            }
            w.add_document(d).expect("add");
        }
        w.commit().expect("commit");
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .expect("reader");
        MergedIndex {
            index,
            reader,
            fields,
        }
    }

    /// Every row matching `query` that `allow` admits, as (id, score), best first, equal
    /// scores in id order. `boosts` are per column; `conjunction` makes AND the default.
    pub fn search(
        &self,
        query: &str,
        boosts: &[(&str, f32)],
        conjunction: bool,
        allow: &dyn Fn(i64) -> bool,
    ) -> Vec<(i64, f32)> {
        use tantivy::collector::TopDocs;
        let mut p = tantivy::query::QueryParser::for_index(
            &self.index,
            self.fields.iter().map(|(_, f)| *f).collect(),
        );
        if conjunction {
            p.set_conjunction_by_default();
        }
        for (c, b) in boosts {
            if let Some((_, f)) = self.fields.iter().find(|(n, _)| n == c) {
                p.set_field_boost(*f, *b);
            }
        }
        let q = p.parse_query(query).expect("query");
        let searcher = self.reader.searcher();
        let n = searcher.num_docs().max(1) as usize;
        let top = searcher
            .search(&q, &TopDocs::with_limit(n).order_by_score())
            .expect("search");
        let mut out: Vec<(i64, f32)> = top
            .into_iter()
            .map(|(score, addr)| {
                let ids = searcher
                    .segment_reader(addr.segment_ord)
                    .fast_fields()
                    .i64("id")
                    .expect("id");
                (ids.first(addr.doc_id).expect("id value"), score)
            })
            .filter(|(i, _)| allow(*i))
            .collect();
        out.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        out
    }
}

/// How a search result compares with the merged index's ranking.
#[derive(Debug, Default, Clone, Copy)]
pub struct RankCheck {
    /// Largest score difference between a hit and the same row in the merged index.
    pub max_score_diff: f32,
    /// Largest difference between the `i`-th scores of both rankings.
    pub max_rank_diff: f32,
}

/// Checks `got` (id, score, best first) against `want` (the merged index's full ranking): the
/// same number of hits (`k` or fewer), each hit's score equal to its merged score, the `i`-th
/// scores equal, and every row the merged index ranks strictly above the `k`-th score present.
/// Equal scores may be broken differently (moraine: file, then row; merged: id). Scores
/// "equal" means within `tol`.
pub fn check_ranking(
    got: &[(i64, f32)],
    want: &[(i64, f32)],
    k: usize,
    tol: f32,
) -> Result<RankCheck, String> {
    let mut c = RankCheck::default();
    if got.len() != k.min(want.len()) {
        return Err(format!(
            "{} hits, the merged index has {} (k = {k})",
            got.len(),
            want.len()
        ));
    }
    let by_id: std::collections::HashMap<i64, f32> = want.iter().copied().collect();
    for (i, (id, s)) in got.iter().enumerate() {
        let Some(m) = by_id.get(id) else {
            return Err(format!("hit {id} is not a match of the merged index"));
        };
        c.max_score_diff = c.max_score_diff.max((m - s).abs());
        c.max_rank_diff = c.max_rank_diff.max((want[i].1 - s).abs());
    }
    if c.max_score_diff > tol || c.max_rank_diff > tol {
        return Err(format!("scores differ: {c:?}"));
    }
    if let Some(last) = got.last() {
        let mine: std::collections::HashSet<i64> = got.iter().map(|x| x.0).collect();
        for (id, s) in want {
            if *s > last.1 + tol && !mine.contains(id) {
                return Err(format!(
                    "row {id} (score {s}) ranks above the k-th hit ({}) but is missing",
                    last.1
                ));
            }
        }
    }
    Ok(c)
}
