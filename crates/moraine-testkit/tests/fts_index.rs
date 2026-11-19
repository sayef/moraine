//! The `fts` kind end to end: T9 full-text ranking against one merged
//! tantivy index over random corpora, scenario 9 for full text, and cases F1 to F12.
//!
//! More corpora: `MORAINE_FTS_CORPORA=<n>` (default 40).

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use moraine::expr::{Expr, col, lit};
use moraine::index::BuildOptions;
use moraine::index::CacheMode;
use moraine::{Database, Error, IndexSpec, Table, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::text::{MergedIndex, TextRow, batch, check_ranking, document, schema, word};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde_json::json;
use tantivy::schema::IndexRecordOption;

/// Scores of moraine and the merged index agree within this (case F5).
const TOL: f32 = 1e-5;

async fn db(store: &Arc<InMemory>, cache: CacheMode) -> Database {
    Database::builder_for_store(store.clone(), "db")
        .sidecar_cache(cache)
        .sidecar_poll_interval(std::time::Duration::ZERO)
        .open()
        .await
        .unwrap()
}

fn memory() -> CacheMode {
    CacheMode::Memory { max_bytes: 1 << 30 }
}

/// The rows of a corpus, as the merged index takes them.
fn text_rows(docs: &[(Option<String>, Option<String>)], first: i64) -> Vec<TextRow> {
    docs.iter()
        .enumerate()
        .map(|(i, (t, b))| (first + i as i64, vec![t.clone(), b.clone()]))
        .collect()
}

/// (id, score) of a search's hits, from its `id` column.
fn ranked(r: &moraine::index::SearchResult) -> Vec<(i64, f32)> {
    let ids = r
        .batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>();
    ids.values()
        .iter()
        .zip(&r.hits)
        .map(|(i, h)| (*i, h.score))
        .collect()
}

fn hits(r: &moraine::index::SearchResult) -> Vec<(String, u64, f32)> {
    r.hits
        .iter()
        .map(|h| (h.file.clone(), h.offset, h.score))
        .collect()
}

/// A random query over `vocab` words: terms, field terms, phrases, AND, OR, negation, boosts.
fn random_query(rng: &mut SimRng, vocab: usize) -> String {
    let n = rng.range(1, 4) as usize;
    let mut parts: Vec<String> = Vec::new();
    for i in 0..n {
        let w = word(rng, vocab);
        let p = match rng.below(9) {
            0 => format!("title:{w}"),
            1 => format!("body:{w}"),
            2 => format!("\"{w} {}\"", word(rng, vocab)),
            3 => format!("{w}^2.5"),
            4 if i > 0 => format!("-{w}"),
            5 if i > 0 => format!("+{w}"),
            _ => w.to_string(),
        };
        parts.push(p);
        if i + 1 < n && rng.chance(0.2) {
            parts.push(if rng.chance(0.5) { "AND" } else { "OR" }.into());
        }
    }
    // A query of negations alone matches nothing in tantivy; keep one positive term.
    if parts
        .iter()
        .all(|p| p.starts_with('-') || p == "AND" || p == "OR")
    {
        parts.push(word(rng, vocab).to_string());
    }
    parts.join(" ")
}

#[derive(Debug, Default)]
struct T9 {
    corpora: u64,
    queries: u64,
    hits: u64,
    sidecar_files: u64,
    memory_files: u64,
    max_score_diff: f32,
    exact_equal: u64,
}

/// T9 (full text): over random corpora split into random files, some with sidecars and some
/// indexed in memory, every query's ranking equals one tantivy index over all rows (case F5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t9_ranking_equals_merged_index() {
    let corpora: u64 = std::env::var("MORAINE_FTS_CORPORA")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    let mut total = T9::default();
    for seed in 0..corpora {
        corpus(seed, &mut total).await;
    }
    println!("RESULT t9_fts {total:?}");
    assert!(
        total.sidecar_files > 0 && total.memory_files > 0,
        "{total:?}"
    );
}

async fn corpus(seed: u64, total: &mut T9) {
    let mut rng = SimRng::new(seed);
    let vocab = rng.range(8, moraine_testkit::text::WORDS.len() as u64) as usize;
    let files = rng.range(1, 10) as usize;
    let max_body = rng.range(3, 60) as usize;
    let stem = rng.chance(0.3);
    let tok = if stem { "en_stem" } else { "default" };
    let record = if rng.chance(0.2) {
        "freqs"
    } else {
        "positions"
    };
    let store = Arc::new(InMemory::new());
    let d = db(&store, memory()).await;
    let t = d
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let mut all: Vec<TextRow> = Vec::new();
    let mut id = 0i64;
    for _ in 0..files {
        let n = rng.range(1, 400) as usize;
        let docs: Vec<_> = (0..n)
            .map(|_| document(&mut rng, vocab, max_body))
            .collect();
        t.append(vec![batch(id, &docs)]).await.unwrap();
        all.extend(text_rows(&docs, id));
        id += n as i64;
    }
    let min_rows = rng.range(0, 300);
    t.create_index(
        IndexSpec::new("fts", "fts", &["title", "body"])
            .params(json!({"tokenizer": tok, "record": record}))
            .policy(json!({"min_rows_for_sidecar": min_rows})),
    )
    .await
    .unwrap();
    // Sidecars for a random share of the files.
    let builds = rng.range(0, files as u64 + 1) as usize;
    t.build_indexes(BuildOptions::default().max_builds(builds))
        .await
        .unwrap();
    let rec = if record == "freqs" {
        IndexRecordOption::WithFreqs
    } else {
        IndexRecordOption::WithFreqsAndPositions
    };
    let merged = MergedIndex::build(&all, &[("title", tok, rec), ("body", tok, rec)]);
    total.corpora += 1;
    for _ in 0..12 {
        let mut q = random_query(&mut rng, vocab);
        if record == "freqs" {
            q = q.replace('"', "");
        }
        let k = rng.range(1, 40) as usize;
        let conjunction = rng.chance(0.2);
        let boost = rng.chance(0.3).then(|| rng.range(1, 30) as f32 / 10.0);
        let filter: Option<(Expr, i64)> = rng.chance(0.3).then(|| {
            let g = rng.below(7) as i64;
            (col("g").lt_eq(lit(g)), g)
        });
        let mut s = t.search_text("fts", &q).limit(k).select(["id"]);
        if conjunction {
            s = s.param("conjunction", true);
        }
        if let Some(b) = boost {
            s = s.param("boosts", json!({"title": b}));
        }
        if let Some((f, _)) = &filter {
            s = s.filter(f.clone());
        }
        let r = s.collect().await.unwrap();
        let got = ranked(&r);
        let g = filter.as_ref().map(|x| x.1);
        let boosts: Vec<(&str, f32)> = boost.map(|b| ("title", b)).into_iter().collect();
        let want = merged.search(&q, &boosts, conjunction, &|i| g.is_none_or(|g| i % 7 <= g));
        match check_ranking(&got, &want, k, TOL) {
            Ok(c) => {
                total.max_score_diff = total.max_score_diff.max(c.max_score_diff);
                total.exact_equal += u64::from(c.max_score_diff == 0.0 && c.max_rank_diff == 0.0);
            }
            Err(e) => panic!(
                "seed {seed}, query {q:?}, k {k}, filter {g:?}, boost {boost:?}, conj \
                 {conjunction}, tokenizer {tok}, record {record}: {e}\n got {got:?}\n want {:?}",
                &want[..want.len().min(k + 5)]
            ),
        }
        total.queries += 1;
        total.hits += got.len() as u64;
        total.sidecar_files += r.stats.sidecar_files as u64;
        total.memory_files += r.stats.exact_files as u64;
    }
}

/// A table of `files` files of `rows + i` random documents with an `fts` index; the second
/// table handle sees it through a client with the sidecar cache disabled.
async fn env(
    files: usize,
    rows: usize,
    params: serde_json::Value,
) -> (Arc<InMemory>, Table, Table) {
    let store = Arc::new(InMemory::new());
    let d = db(&store, memory()).await;
    let t = d
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let mut rng = SimRng::new(3);
    let mut id = 0;
    for f in 0..files {
        let docs: Vec<_> = (0..rows + f).map(|_| document(&mut rng, 40, 30)).collect();
        t.append(vec![batch(id, &docs)]).await.unwrap();
        id += docs.len() as i64;
    }
    t.create_index(
        IndexSpec::new("fts", "fts", &["title", "body"])
            .params(params)
            .policy(json!({"min_rows_for_sidecar": 10})),
    )
    .await
    .unwrap();
    let exact = db(&store, CacheMode::Disabled)
        .await
        .open_table("t")
        .await
        .unwrap();
    (store, t, exact)
}

const QUERIES: &[&str] = &[
    "object storage",
    "\"delta table\"",
    "title:glacier OR body:moraine",
    "write AND reads",
    "index -cache",
    "+snapshot vacuum^3",
    "segment",
];

async fn answers(t: &Table, filter: Option<Expr>) -> Vec<Vec<(String, u64, f32)>> {
    let mut out = Vec::new();
    for q in QUERIES {
        let mut s = t.search_text("fts", q).limit(15).select(["id"]);
        if let Some(f) = &filter {
            s = s.filter(f.clone());
        }
        out.push(hits(&s.collect().await.unwrap()));
    }
    out
}

/// Scenario 9 (full text): the same queries with the sidecar cache disabled, with every
/// sidecar built, and with a random half of them deleted give identical hits: files, rows and
/// scores (cases F7 and I6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_transparency() {
    let (store, t, exact) = env(8, 300, json!({})).await;
    let none = answers(&t, None).await;
    assert_eq!(none, answers(&exact, None).await);
    let filter = Some(col("g").gt(lit(2i64)));
    let none_f = answers(&t, filter.clone()).await;
    let r = t.build_indexes(BuildOptions::default()).await.unwrap();
    assert!(r.built >= 8, "{r:?}");
    let s = t.search_text("fts", "object").collect().await.unwrap();
    assert_eq!((s.stats.sidecar_files, s.stats.exact_files), (8, 0));
    assert_eq!(answers(&t, None).await, none);
    assert_eq!(answers(&t, filter.clone()).await, none_f);
    assert_eq!(answers(&exact, None).await, none);
    let mut rng = SimRng::new(5);
    let listed: Vec<_> =
        futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&Path::from("db/t/idx"))))
            .await
            .unwrap();
    for m in listed
        .iter()
        .filter(|m| m.location.as_ref().contains("/fts."))
    {
        if rng.chance(0.5) {
            store.delete(&m.location).await.unwrap();
        }
    }
    let half = db(&store, memory()).await.open_table("t").await.unwrap();
    let s = half.search_text("fts", "object").collect().await.unwrap();
    assert!(
        s.stats.sidecar_files > 0 && s.stats.exact_files > 0,
        "{:?}",
        s.stats
    );
    assert_eq!(answers(&half, None).await, none);
    assert_eq!(answers(&half, filter).await, none_f);
}

/// Scenario 10 for full text: corrupt, truncated and swapped sidecars are ignored and the
/// results stay identical (case F10).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_sidecar_is_ignored() {
    let (store, t, exact) = env(4, 200, json!({})).await;
    t.build_indexes(BuildOptions::default()).await.unwrap();
    let want = answers(&exact, None).await;
    let listed: Vec<_> =
        futures::TryStreamExt::try_collect::<Vec<_>>(store.list(Some(&Path::from("db/t/idx"))))
            .await
            .unwrap();
    let fts: Vec<_> = listed
        .iter()
        .filter(|m| m.location.as_ref().contains("/fts."))
        .collect();
    assert_eq!(fts.len(), 4);
    let a = store
        .get(&fts[0].location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let b = store
        .get(&fts[1].location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    // Flip a byte in a section, truncate, and swap two files' sidecars.
    let mut flipped = a.to_vec();
    let at = flipped.len() / 2;
    flipped[at] ^= 0x5a;
    store
        .put(&fts[0].location, PutPayload::from(flipped))
        .await
        .unwrap();
    store
        .put(&fts[1].location, PutPayload::from(a.slice(..a.len() / 3)))
        .await
        .unwrap();
    store
        .put(&fts[2].location, PutPayload::from(b.clone()))
        .await
        .unwrap();
    let fresh = db(&store, memory()).await;
    let ft = fresh.open_table("t").await.unwrap();
    assert_eq!(answers(&ft, None).await, want);
    let bad = fresh
        .index_metrics()
        .bad_sidecars
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(bad >= 3, "bad sidecars counted: {bad}");
}

/// A one-file table holding `docs` with an index over `title` and `body`.
async fn small(docs: &[(Option<String>, Option<String>)], spec: IndexSpec) -> (Database, Table) {
    let store = Arc::new(InMemory::new());
    let d = db(&store, memory()).await;
    let t = d
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    t.append(vec![batch(0, docs)]).await.unwrap();
    t.create_index(spec).await.unwrap();
    (d, t)
}

fn doc(t: &str, b: &str) -> (Option<String>, Option<String>) {
    (Some(t.into()), Some(b.into()))
}

fn ids(r: &moraine::index::SearchResult) -> Vec<i64> {
    ranked(r).into_iter().map(|x| x.0).collect()
}

/// F1, F2, F4, F6: empty queries, syntax errors, phrases without positions, null and empty
/// values counted as documents, and truncation for indexing only.
#[tokio::test]
async fn query_rules() {
    let docs = vec![
        doc("glacier valley", "ice flow"),
        (None, None),
        doc("", ""),
        doc("moraine", &format!("{} hidden", "stone ".repeat(20))),
    ];
    let spec = IndexSpec::new("fts", "fts", &["title", "body"])
        .params(json!({
            "max_indexed_chars": 60,
            "fields": {"body": {"record": "basic"}}
        }))
        .policy(json!({"min_rows_for_sidecar": 0}));
    let (_d, t) = small(&docs, spec).await;
    for built in [false, true] {
        if built {
            t.build_indexes(BuildOptions::default()).await.unwrap();
        }
        // F1: empty, blank, and nothing left after tokenising.
        for q in ["", "   ", ",,, ;;"] {
            let r = t.search_text("fts", q).collect().await.unwrap();
            assert!(r.hits.is_empty(), "{q:?}");
            assert_eq!(r.batch.num_rows(), 0);
        }
        // F2: a syntax error, and a phrase on a field recording no positions.
        assert!(matches!(
            t.search_text("fts", "title:(glacier").collect().await,
            Err(Error::InvalidQuery(_))
        ));
        match t.search_text("fts", "body:\"ice flow\"").collect().await {
            Err(Error::InvalidQuery(m)) => assert!(m.contains("body"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            t.search_text("fts", "nosuch:glacier").collect().await,
            Err(Error::InvalidQuery(_))
        ));
        // F6: "hidden" is past 60 characters of the body.
        let r = t.search_text("fts", "hidden").collect().await.unwrap();
        assert!(r.hits.is_empty());
        assert_eq!(
            ids(&t.search_text("fts", "stone").collect().await.unwrap()),
            [3]
        );
        // A phrase on a positional field works on both routes.
        let r = t
            .search_text("fts", "title:\"glacier valley\"")
            .collect()
            .await
            .unwrap();
        assert_eq!(ids(&r), [0]);
        // F4: the null and empty rows are documents: four in the statistics.
        let only = t.search_text("fts", "glacier").collect().await.unwrap();
        let (_, merged) = (
            0,
            MergedIndex::build(
                &text_rows(&docs, 0),
                &[
                    ("title", "default", IndexRecordOption::WithFreqsAndPositions),
                    ("body", "default", IndexRecordOption::Basic),
                ],
            ),
        );
        let want = merged.search("glacier", &[], false, &|_| true);
        check_ranking(&ranked(&only), &want, 10, TOL).unwrap();
    }
    // The stored value is untouched by truncation (F6).
    let all = t.scan().collect().await.unwrap();
    let body = all[0].column_by_name("body").unwrap().as_string::<i32>();
    assert!(body.value(3).ends_with("hidden"));
    // Searching a vector or full-text index with the other API is refused.
    assert!(matches!(
        t.search_vector("fts", vec![1.0]).collect().await,
        Err(Error::InvalidQuery(_))
    ));
}

/// F3: a custom tokenizer must be registered in every client; an unknown one is an error, not
/// a fallback. Built-in names cannot be replaced.
#[tokio::test]
async fn tokenizer_registration() {
    use tantivy::tokenizer::{LowerCaser, RawTokenizer, TextAnalyzer};
    let store = Arc::new(InMemory::new());
    let words = || {
        TextAnalyzer::builder(tantivy::tokenizer::WhitespaceTokenizer::default())
            .filter(LowerCaser)
            .build()
    };
    let a = Database::builder_for_store(store.clone(), "db")
        .tokenizer("words", words())
        .open()
        .await
        .unwrap();
    let t = a
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    t.append(vec![batch(0, &[doc("Stone-Age ridge", "x")])])
        .await
        .unwrap();
    t.create_index(IndexSpec::new("fts", "fts", &["title"]).params(json!({"tokenizer": "words"})))
        .await
        .unwrap();
    let r = t.search_text("fts", "stone-age").collect().await.unwrap();
    assert_eq!(ids(&r), [0]);
    // A client without the tokenizer cannot search it, or create an index naming one.
    let b = Database::builder_for_store(store.clone(), "db")
        .open()
        .await
        .unwrap();
    let tb = b.open_table("t").await.unwrap();
    match tb.search_text("fts", "ridge").collect().await {
        Err(Error::TokenizerNotRegistered(n)) => assert_eq!(n.name, "words"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        tb.create_index(
            IndexSpec::new("other", "fts", &["body"]).params(json!({"tokenizer": "nope"}))
        )
        .await,
        Err(Error::TokenizerNotRegistered(_))
    ));
    // Built-in names are fixed.
    assert!(matches!(
        Database::builder_for_store(store.clone(), "db")
            .tokenizer("default", TextAnalyzer::from(RawTokenizer::default()))
            .open()
            .await,
        Err(Error::InvalidConfig(_))
    ));
}

/// F11: the bigram tokenizer finds words in text without spaces, on both routes and like the
/// merged index.
#[tokio::test]
async fn cjk_bigrams() {
    let docs = vec![
        doc("東京都の天気", "今日は晴れ"),
        doc("京都の寺", "金閣寺と銀閣寺"),
        doc("大阪", "天気は雨"),
    ];
    let spec = IndexSpec::new("fts", "fts", &["title", "body"])
        .params(json!({"tokenizer": "ngram2"}))
        .policy(json!({"min_rows_for_sidecar": 0}));
    let (_d, t) = small(&docs, spec).await;
    let merged = MergedIndex::build(
        &text_rows(&docs, 0),
        &[
            ("title", "ngram2", IndexRecordOption::WithFreqsAndPositions),
            ("body", "ngram2", IndexRecordOption::WithFreqsAndPositions),
        ],
    );
    for built in [false, true] {
        if built {
            t.build_indexes(BuildOptions::default()).await.unwrap();
        }
        for q in ["京都", "天気", "閣寺", "\"東京\""] {
            let r = t.search_text("fts", q).collect().await.unwrap();
            let want = merged.search(q, &[], false, &|_| true);
            check_ranking(&ranked(&r), &want, 10, TOL).unwrap();
            assert!(!r.hits.is_empty(), "{q}");
        }
    }
    let r = t.search_text("fts", "京都").collect().await.unwrap();
    assert_eq!(
        ids(&r).into_iter().collect::<HashSet<_>>(),
        HashSet::from([0, 1])
    );
}

/// F12: boosts from the spec, and from a query, apply on both routes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boosts_on_both_routes() {
    let docs = vec![doc("ridge", "snow snow"), doc("snow", "ridge ridge")];
    let spec = IndexSpec::new("fts", "fts", &["title", "body"])
        .search(json!({"boosts": {"title": 4.0}}))
        .policy(json!({"min_rows_for_sidecar": 0}));
    let (_d, t) = small(&docs, spec).await;
    let merged = MergedIndex::build(
        &text_rows(&docs, 0),
        &[
            ("title", "default", IndexRecordOption::WithFreqsAndPositions),
            ("body", "default", IndexRecordOption::WithFreqsAndPositions),
        ],
    );
    for built in [false, true] {
        if built {
            t.build_indexes(BuildOptions::default()).await.unwrap();
        }
        let r = t.search_text("fts", "ridge").collect().await.unwrap();
        assert_eq!(ids(&r), [0, 1]);
        check_ranking(
            &ranked(&r),
            &merged.search("ridge", &[("title", 4.0)], false, &|_| true),
            10,
            TOL,
        )
        .unwrap();
        let r = t
            .search_text("fts", "ridge")
            .param("boosts", json!({"title": 0.1}))
            .collect()
            .await
            .unwrap();
        assert_eq!(ids(&r), [1, 0]);
        check_ranking(
            &ranked(&r),
            &merged.search("ridge", &[("title", 0.1)], false, &|_| true),
            10,
            TOL,
        )
        .unwrap();
        assert!(matches!(
            t.search_text("fts", "ridge")
                .param("boosts", json!({"nope": 1.0}))
                .collect()
                .await,
            Err(Error::InvalidQuery(_))
        ));
    }
}

/// F8: with a tiny in-memory budget, indexes built for files without sidecars are dropped, and
/// queries still answer identically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_limit_evicts() {
    let (store, t, _) = env(6, 100, json!({})).await;
    let want = answers(&t, None).await;
    let d = Database::builder_for_store(store.clone(), "db")
        .sidecar_cache(CacheMode::Disabled)
        .fts_memory(64 << 10)
        .open()
        .await
        .unwrap();
    let small = d.open_table("t").await.unwrap();
    assert_eq!(answers(&small, None).await, want);
    let u = d.cache_usage();
    assert!(u.fts_bytes <= 64 << 10, "{u:?}");
    let big = db(&store, CacheMode::Disabled).await;
    let bt = big.open_table("t").await.unwrap();
    assert_eq!(answers(&bt, None).await, want);
    assert_eq!(big.cache_usage().fts_indexes, 6);
}

/// Spec resolution: shorthand defaults, per-column settings, and refusals.
#[tokio::test]
async fn spec_rules() {
    let store = Arc::new(InMemory::new());
    let d = db(&store, memory()).await;
    let t = d
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let s = t
        .create_index(
            IndexSpec::new("fts", "fts", &["title", "body"]).params(json!({
                "tokenizer": "en_stem",
                "fields": {"title": {"tokenizer": "raw", "record": "basic"}}
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        s.params,
        json!({
            "fields": {
                "title": {"tokenizer": "raw", "record": "basic"},
                "body": {"tokenizer": "en_stem", "record": "positions"}
            },
            "max_indexed_chars": 1_000_000
        })
    );
    assert_eq!(s.policy, json!({"min_rows_for_sidecar": 1000}));
    assert_eq!(
        s.search,
        json!({"boosts": {"title": 1.0, "body": 1.0}, "conjunction": false})
    );
    for bad in [
        IndexSpec::new("x", "fts", &["id"]),
        IndexSpec::new("x", "fts", &[]),
        IndexSpec::new("x", "fts", &["title", "title"]),
        IndexSpec::new("x", "fts", &["title"]).params(json!({"stemmer": "en"})),
        IndexSpec::new("x", "fts", &["title"]).params(json!({"record": "all"})),
        IndexSpec::new("x", "fts", &["title"]).params(json!({"fields": {"body": {}}})),
        IndexSpec::new("x", "fts", &["title"]).search(json!({"boosts": {"title": -1}})),
    ] {
        assert!(
            matches!(
                t.create_index(bad.clone()).await,
                Err(Error::InvalidIndexSpec(_))
            ),
            "{bad:?}"
        );
    }
}

/// Equal rows give byte-identical bundles, whatever the batch split, and a build split into
/// parts (segments merged at the end) ranks like a build in one part.
#[tokio::test]
async fn bundles_are_deterministic() {
    use moraine::index::IndexKind;
    use moraine::index::fts::{FtsKind, Tokenizers};
    use moraine::testing::BundleBuild;
    let kind = FtsKind::new(Arc::new(Tokenizers::default()));
    let spec = kind
        .resolve(&IndexSpec::new("fts", "fts", &["title", "body"]), &schema())
        .unwrap();
    let mut rng = SimRng::new(9);
    let docs: Vec<_> = (0..3_000).map(|_| document(&mut rng, 50, 40)).collect();
    let b = batch(0, &docs);
    let cols = b.project(&[2, 3]).unwrap();
    let build = |splits: &[usize], budget: usize| -> Vec<u8> {
        let mut w = BundleBuild::new(
            &kind,
            &spec,
            3_000,
            12_345,
            budget,
            std::io::Cursor::new(Vec::new()),
        )
        .unwrap();
        let mut at = 0;
        for s in splits.iter().copied().chain([3_000]) {
            if s > at {
                w.add(at as u64, &cols.slice(at, s - at)).unwrap();
                at = s;
            }
        }
        w.finish().unwrap().0.into_inner()
    };
    let one = build(&[], 1 << 30);
    assert_eq!(one, build(&[1, 700, 2_999], 1 << 30));
    assert_eq!(one, build(&[], 1 << 30));
    // A small budget seals parts of 1,000 rows (three segments, merged).
    let parts = build(&[], 2_560_000);
    // Its segment files are those of the one-part build; only tantivy's metadata (the
    // operation stamp counts the commits) differs.
    use moraine::index::bundle::{BundleBytes, BundleView};
    let view = |b: &[u8]| BundleView::parse(BundleBytes::copy(b).unwrap()).unwrap();
    let (a, b) = (view(&one), view(&parts));
    let segs = |v: &BundleView| -> Vec<(String, Vec<u8>)> {
        v.sections_with_prefix("seg.")
            .map(|(n, b)| (n.to_string(), b.to_vec()))
            .collect()
    };
    assert_eq!(segs(&a).len(), 6);
    assert_eq!(segs(&a), segs(&b));
    assert!(kind.open(&spec, b).is_ok());
}
