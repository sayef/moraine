//! Full-text search under concurrency (D44): sidecar builds race appends, upserts and
//! deletes (Delta merges that rewrite files, with no compaction involved), and searches run
//! throughout. Under the simulation scheduler, several writers bring a keyed table to a known
//! final state (stale versions of rows corrected by upserts, junk rows appended and deleted),
//! a builder client builds sidecars between their commits, and a reader client compares, at
//! every version it sees, the answers of a client using sidecars with those of a client that
//! uses none (scenario 9 while indexing races writes). At the end, a fresh client's rankings
//! must equal one tantivy index over the final rows (T9).
//!
//! More seeds: `MORAINE_SIM_SEEDS=<n>`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Int64Type;
use moraine::clock::Clock;
use moraine::expr::{ScalarValue, col, lit};
use moraine::index::BuildOptions;
use moraine::index::CacheMode;
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, DuplicatePolicy, IndexSpec, Table, TableOptions};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::text::{MergedIndex, TextRow, check_ranking, document, schema, word};
use moraine_testkit::{FaultRules, SimRng};
use serde_json::json;
use tantivy::schema::IndexRecordOption;

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

const ROOT: &str = "db";
const TOL: f32 = 1e-5;

#[derive(Clone, Debug)]
struct Row {
    id: i64,
    g: i64,
    title: Option<String>,
    body: Option<String>,
}

fn random_row(rng: &mut SimRng, id: i64, vocab: usize) -> Row {
    let (title, body) = document(rng, vocab, 12);
    Row {
        id,
        g: rng.below(7) as i64,
        title,
        body,
    }
}

fn batch(rows: &[Row]) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.id))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| Some(r.g)))),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.title.as_deref()),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.body.as_deref()),
        )),
    ];
    RecordBatch::try_new(schema(), cols).expect("batch")
}

enum Step {
    Append(Vec<Row>),
    Upsert(Vec<Row>),
    DeleteKeys(Vec<i64>),
}

/// Each client's steps: its share of the final rows, as appends, upserts, stale upserts
/// corrected later, and junk rows deleted at the end.
fn plan(rng: &mut SimRng, rows: &[Row], clients: usize, vocab: usize) -> Vec<Vec<Step>> {
    let mut owned: Vec<Vec<Row>> = vec![Vec::new(); clients];
    for r in rows {
        owned[rng.below(clients as u64) as usize].push(r.clone());
    }
    owned
        .into_iter()
        .enumerate()
        .map(|(c, mut mine)| {
            let mut steps = Vec::new();
            let mut junk = Vec::new();
            let mut next_junk = 1_000_000 * (c as i64 + 1);
            while !mine.is_empty() {
                let n = (rng.range(1, 30) as usize).min(mine.len());
                let chunk: Vec<Row> = mine.drain(..n).collect();
                match rng.below(3) {
                    0 => steps.push(Step::Append(chunk)),
                    1 => steps.push(Step::Upsert(chunk)),
                    _ => {
                        let stale: Vec<Row> =
                            chunk.iter().map(|r| random_row(rng, r.id, vocab)).collect();
                        steps.push(Step::Upsert(stale));
                        steps.push(Step::Upsert(chunk));
                    }
                }
                if rng.chance(0.3) {
                    let j: Vec<Row> = (0..rng.range(1, 4))
                        .map(|_| {
                            next_junk += 1;
                            random_row(rng, next_junk, vocab)
                        })
                        .collect();
                    junk.extend(j.iter().map(|r| r.id));
                    steps.push(Step::Append(j));
                }
            }
            if !junk.is_empty() {
                steps.push(Step::DeleteKeys(junk));
            }
            steps
        })
        .collect()
}

async fn run_steps(t: Table, steps: Vec<Step>) -> Result<(), String> {
    for s in steps {
        let r = match s {
            Step::Append(rows) => t.append_unchecked(vec![batch(&rows)]).await,
            Step::Upsert(rows) => {
                t.upsert_with(vec![batch(&rows)], DuplicatePolicy::LastWins)
                    .await
            }
            Step::DeleteKeys(ids) => {
                let keys: Vec<Vec<ScalarValue>> = ids
                    .into_iter()
                    .map(|k| vec![ScalarValue::Int64(Some(k))])
                    .collect();
                t.delete_keys(&keys).await
            }
        };
        r.map_err(|e| format!("write failed: {e}"))?;
    }
    Ok(())
}

fn open(
    env: &SimEnv,
    client: usize,
    cache: CacheMode,
) -> impl Future<Output = Database> + Send + use<> {
    let b = Database::builder_for_store(env.chaos_store(client, FaultRules::default()), ROOT)
        .clock(env.client_clock(client))
        .io_concurrency(1)
        .consistency(Consistency::Manual)
        .sidecar_cache(cache)
        .sidecar_poll_interval(Duration::ZERO)
        .request_retry(RetryPolicy::REQUEST);
    async move { b.open().await.expect("open") }
}

fn memory() -> CacheMode {
    CacheMode::Memory {
        max_bytes: 64 << 20,
    }
}

fn random_query(rng: &mut SimRng, vocab: usize) -> String {
    let a = word(rng, vocab);
    match rng.below(5) {
        0 => format!("{a} {}", word(rng, vocab)),
        1 => format!("title:{a}"),
        2 => format!("\"{a} {}\"", word(rng, vocab)),
        3 => format!("{a} -{}", word(rng, vocab)),
        _ => a.to_string(),
    }
}

/// (id, score) of a search.
async fn search(t: &Table, q: &str, k: usize, g: Option<i64>) -> moraine::Result<Vec<(i64, f32)>> {
    let mut s = t.search_text("fts", q).limit(k).select(["id"]);
    if let Some(g) = g {
        s = s.filter(col("g").lt_eq(lit(g)));
    }
    let r = s.collect().await?;
    let ids = r.batch.column(0).as_primitive::<Int64Type>();
    Ok(ids
        .values()
        .iter()
        .zip(&r.hits)
        .map(|(i, h)| (*i, h.score))
        .collect())
}

#[derive(Debug, Default)]
struct Totals {
    seeds: u64,
    builds: u64,
    compared: u64,
    final_queries: u64,
    sidecar_files: u64,
    memory_files: u64,
}

async fn scenario(mut env: SimEnv, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let vocab = env.rng.range(6, 40) as usize;
    let n = env.rng.range(10, 120) as usize;
    let rows: Vec<Row> = (0..n as i64)
        .map(|i| random_row(&mut env.rng, i, vocab))
        .collect();
    let clients = env.rng.range(2, 4) as usize;
    let plans = plan(&mut env.rng, &rows, clients, vocab);
    let tok = if env.rng.chance(0.3) {
        "en_stem"
    } else {
        "default"
    };
    // Half the seeds give every file a sidecar once built; the others leave small files to the
    // in-memory route.
    let min_rows = if env.rng.chance(0.5) {
        0
    } else {
        env.rng.range(1, 40)
    };
    let opts = TableOptions::default()
        .primary_key(["id"])
        .page_row_limit(env.rng.range(1, 32) as usize);

    let setup = open(&env, clients + 3, memory());
    let setup_fut: ClientFuture = Box::pin(async move {
        let db = setup.await;
        let t = db.create_table("t", schema(), opts).await.expect("create");
        t.create_index(
            IndexSpec::new("fts", "fts", &["title", "body"])
                .params(json!({"tokenizer": tok}))
                .policy(json!({"min_rows_for_sidecar": min_rows})),
        )
        .await
        .expect("index");
    });
    env.run(vec![setup_fut])
        .await
        .map_err(|e| format!("{e:?}"))?;

    let errors: Arc<Mutex<Vec<String>>> = Arc::default();
    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut futs: Vec<ClientFuture> = Vec::new();
    for (c, steps) in plans.into_iter().enumerate() {
        let db = open(&env, c, memory());
        let (errors, done) = (errors.clone(), done.clone());
        futs.push(Box::pin(async move {
            let t = db.await.open_table("t").await.expect("open");
            if let Err(e) = run_steps(t, steps).await {
                errors.lock().unwrap().push(format!("client {c}: {e}"));
            }
            done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }));
    }
    // The builder: builds whatever is missing, between the writers' commits.
    {
        let db = open(&env, clients, memory());
        let (errors, done, totals) = (errors.clone(), done.clone(), totals.clone());
        let clock = env.client_clock(clients);
        let mut rng = env.rng.fork(1);
        futs.push(Box::pin(async move {
            let t = db.await.open_table("t").await.expect("open");
            loop {
                let finished = done.load(std::sync::atomic::Ordering::SeqCst) == clients;
                let o = BuildOptions::default().max_builds(rng.range(1, 4) as usize);
                match t.build_indexes(o).await {
                    Ok(r) => totals.lock().unwrap().builds += r.built as u64,
                    Err(e) => errors.lock().unwrap().push(format!("builder: {e}")),
                }
                if finished {
                    return;
                }
                clock.sleep(Duration::from_millis(rng.range(1, 20))).await;
            }
        }));
    }
    // The reader: at each version it sees, sidecars or not, the same answers.
    {
        let with = open(&env, clients + 1, memory());
        let without = open(&env, clients + 2, CacheMode::Disabled);
        let (errors, done, totals) = (errors.clone(), done.clone(), totals.clone());
        let clock = env.client_clock(clients + 1);
        let mut rng = env.rng.fork(2);
        futs.push(Box::pin(async move {
            let a = with.await.open_table("t").await.expect("open");
            let b = without.await.open_table("t").await.expect("open");
            loop {
                let finished = done.load(std::sync::atomic::Ordering::SeqCst) == clients;
                let r = async {
                    let va = a.refresh().await?;
                    let vb = b.refresh().await?;
                    if va == vb {
                        let q = random_query(&mut rng, vocab);
                        let k = rng.range(1, 20) as usize;
                        let g = rng.chance(0.3).then(|| rng.below(7) as i64);
                        let x = search(&a, &q, k, g).await?;
                        let y = search(&b, &q, k, g).await?;
                        if x != y {
                            return Ok::<_, moraine::Error>(Some(format!(
                                "version {va}, query {q:?}, k {k}: with sidecars {x:?}, \
                                 without {y:?}"
                            )));
                        }
                        totals.lock().unwrap().compared += 1;
                    }
                    Ok(None)
                }
                .await;
                match r {
                    Ok(Some(e)) => errors.lock().unwrap().push(e),
                    Ok(None) => {}
                    Err(e) => errors.lock().unwrap().push(format!("reader: {e}")),
                }
                if finished {
                    return;
                }
                clock.sleep(Duration::from_millis(rng.range(1, 15))).await;
            }
        }));
    }
    env.run(futs).await.map_err(|e| format!("{e:?}"))?;
    if let Some(e) = errors.lock().unwrap().first() {
        return Err(e.clone());
    }

    // A fresh client against one tantivy index over the final rows.
    let cache = if env.rng.chance(0.5) {
        memory()
    } else {
        CacheMode::Disabled
    };
    let reader = open(&env, clients + 4, cache);
    let text: Vec<TextRow> = rows
        .iter()
        .map(|r| (r.id, vec![r.title.clone(), r.body.clone()]))
        .collect();
    let g_of: std::collections::HashMap<i64, i64> = rows.iter().map(|r| (r.id, r.g)).collect();
    let rec = IndexRecordOption::WithFreqsAndPositions;
    let merged = MergedIndex::build(&text, &[("title", tok, rec), ("body", tok, rec)]);
    let mut qrng = env.rng.fork(3);
    let out: Arc<Mutex<Result<(), String>>> = Arc::new(Mutex::new(Ok(())));
    let (o, tot) = (out.clone(), totals.clone());
    let read: ClientFuture = Box::pin(async move {
        let t = reader.await.open_table("t").await.expect("open");
        let r = async {
            let snap = t.snapshot().await.map_err(|e| e.to_string())?;
            if snap.num_rows() != rows.len() as u64 {
                return Err(format!("{} rows, want {}", snap.num_rows(), rows.len()));
            }
            for _ in 0..6 {
                let q = random_query(&mut qrng, vocab);
                let k = qrng.range(1, 30) as usize;
                let g = qrng.chance(0.3).then(|| qrng.below(7) as i64);
                let got = search(&t, &q, k, g).await.map_err(|e| e.to_string())?;
                let want = merged.search(&q, &[], false, &|i| {
                    g.is_none_or(|g| g_of.get(&i).is_some_and(|x| *x <= g))
                });
                check_ranking(&got, &want, k, TOL)
                    .map_err(|e| format!("query {q:?}, k {k}, g {g:?}: {e}"))?;
                tot.lock().unwrap().final_queries += 1;
            }
            let s = t
                .search_text("fts", "x")
                .collect()
                .await
                .map_err(|e| e.to_string())?;
            let mut tt = tot.lock().unwrap();
            tt.sidecar_files += s.stats.sidecar_files as u64;
            tt.memory_files += s.stats.exact_files as u64;
            Ok(())
        }
        .await;
        *o.lock().unwrap() = r;
    });
    env.run(vec![read]).await.map_err(|e| format!("{e:?}"))?;
    totals.lock().unwrap().seeds += 1;
    out.lock().unwrap().clone()
}

/// Seeds that failed during development; they run on every test run.
const REGRESSION_SEEDS: &[u64] = &[];

#[test]
fn fts_indexing_races_writes() {
    let totals = Arc::new(Mutex::new(Totals::default()));
    for s in REGRESSION_SEEDS {
        let t = totals.clone();
        if let Err(f) = run_seeds(*s..*s + 1, move |env| scenario(env, t.clone())) {
            panic!("{f}");
        }
    }
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds_from_env(60), move |env| scenario(env, t.clone())) {
        panic!("{f}");
    }
    let t = totals.lock().unwrap();
    println!("RESULT fts_sim {t:?}");
    assert!(t.compared > 0 && t.builds > 0, "{t:?}");
}
