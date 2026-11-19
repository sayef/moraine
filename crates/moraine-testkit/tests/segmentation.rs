//! Results do not depend on how rows are split into files (D29; strengthens I6 before sidecars
//! rely on it). The same final rows reach two tables: one written by a single append with
//! default options, the other by several clients committing concurrently under the simulation
//! scheduler, in random chunk sizes, with appends, upserts that overwrite stale values, deletes
//! of rows that should never have been there, group commit, and random page, row-group and file
//! size limits. Every query (full scan, filtered scans, projections, key lookups) must return
//! bit-identical Arrow data once rows are put in key order.
//!
//! More seeds: `MORAINE_SIM_SEEDS=<n>`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    UInt32Array,
};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, SchemaRef};
use moraine::expr::{Expr, ScalarValue, col, lit};
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, DuplicatePolicy, Table, TableOptions};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::{FaultRules, SimRng};

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

const ROOT: &str = "db";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("g", DataType::Int64, true),
        Field::new("x", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
    ]))
}

/// One logical row. `x` covers floats whose bits matter: negative zero, subnormals, NaN.
#[derive(Clone, Debug)]
struct Row {
    k: String,
    g: Option<i64>,
    x: Option<f64>,
    s: Option<String>,
    b: Option<bool>,
}

fn random_row(rng: &mut SimRng, k: String) -> Row {
    let x = match rng.below(8) {
        0 => None,
        1 => Some(-0.0),
        2 => Some(f64::MIN_POSITIVE / 2.0),
        3 => Some(f64::NAN),
        4 => Some(f64::INFINITY),
        _ => Some((rng.next_u64() as i64) as f64 / 1e9),
    };
    Row {
        k,
        g: (!rng.chance(0.1)).then(|| rng.below(6) as i64),
        x,
        s: (!rng.chance(0.1)).then(|| "é\u{0}z".repeat(rng.below(4) as usize)),
        b: (!rng.chance(0.2)).then(|| rng.chance(0.5)),
    }
}

fn batch(rows: &[Row]) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.k))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.g))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.x))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.s.as_deref()))),
        Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r.b))),
    ];
    RecordBatch::try_new(schema(), cols).expect("batch")
}

/// `batches` concatenated, put in key order, and encoded value by value: the schema, then per
/// column and row a null flag and the value's exact bytes (floats by their bits, so negative
/// zero and NaN payloads count). Equal encodings mean bit-identical results; bytes under null
/// slots, which Arrow leaves unspecified, do not take part.
fn canonical(batches: &[RecordBatch], schema: &SchemaRef) -> Vec<u8> {
    let all = concat_batches(schema, batches).expect("concat");
    let sorted = match all.column_by_name("k") {
        Some(k) if all.num_rows() > 0 => {
            let idx: UInt32Array = lexsort_to_indices(
                &[SortColumn {
                    values: k.clone(),
                    options: None,
                }],
                None,
            )
            .expect("sort");
            take_record_batch(&all, &idx).expect("take")
        }
        _ => all,
    };
    let mut out = format!("{:?}\n{}\n", sorted.schema(), sorted.num_rows()).into_bytes();
    for c in sorted.columns() {
        for r in 0..c.len() {
            if c.is_null(r) {
                out.push(0);
                continue;
            }
            out.push(1);
            match c.data_type() {
                DataType::Utf8 => {
                    let v = c.as_string::<i32>().value(r);
                    out.extend((v.len() as u64).to_le_bytes());
                    out.extend(v.as_bytes());
                }
                DataType::Int64 => out.extend(c.as_primitive::<Int64Type>().value(r).to_le_bytes()),
                DataType::Float64 => out.extend(
                    c.as_primitive::<Float64Type>()
                        .value(r)
                        .to_bits()
                        .to_le_bytes(),
                ),
                DataType::Boolean => out.push(u8::from(c.as_boolean().value(r))),
                other => panic!("no encoding for {other}"),
            }
        }
    }
    out
}

/// Where two canonical answers differ, for the failure message.
fn describe(a: &[u8], b: &[u8]) -> String {
    let at = a
        .iter()
        .zip(b)
        .position(|(x, y)| x != y)
        .unwrap_or(a.len().min(b.len()));
    let near = |x: &[u8]| {
        let from = at.saturating_sub(40).min(x.len());
        String::from_utf8_lossy(&x[from..x.len().min(from + 120)]).into_owned()
    };
    format!(
        "{} against {} bytes, first difference at byte {at}: {:?} against {:?}",
        a.len(),
        b.len(),
        near(a),
        near(b)
    )
}

/// What a client does, in order: appends of fresh rows, stale versions of rows it later
/// corrects with an upsert, and junk rows it later deletes.
enum Step {
    Append(Vec<Row>),
    Upsert(Vec<Row>),
    DeleteKeys(Vec<String>),
}

fn plan_clients(rng: &mut SimRng, rows: &[Row], clients: usize) -> Vec<Vec<Vec<Step>>> {
    let mut owned: Vec<Vec<Row>> = vec![Vec::new(); clients];
    for r in rows {
        owned[rng.below(clients as u64) as usize].push(r.clone());
    }
    owned
        .into_iter()
        .enumerate()
        .map(|(c, mine)| {
            // Each client runs one or two tasks on its handle (group commit merges them); a
            // key's steps stay in one task, so their order is kept.
            let tasks = rng.range(1, 2) as usize;
            let mut per_task: Vec<Vec<Row>> = vec![Vec::new(); tasks];
            for r in mine {
                per_task[rng.below(tasks as u64) as usize].push(r);
            }
            per_task
                .into_iter()
                .enumerate()
                .map(|(t, mut rows)| {
                    let mut steps = Vec::new();
                    let mut junk = Vec::new();
                    while !rows.is_empty() {
                        let n = (rng.range(1, 40) as usize).min(rows.len());
                        let chunk: Vec<Row> = rows.drain(..n).collect();
                        match rng.below(3) {
                            0 => steps.push(Step::Append(chunk)),
                            1 => steps.push(Step::Upsert(chunk)),
                            _ => {
                                // A stale version first, the final one later.
                                let stale: Vec<Row> =
                                    chunk.iter().map(|r| random_row(rng, r.k.clone())).collect();
                                steps.push(Step::Upsert(stale));
                                steps.push(Step::Upsert(chunk));
                            }
                        }
                        if rng.chance(0.3) {
                            let j: Vec<Row> = (0..rng.range(1, 5))
                                .map(|i| {
                                    random_row(rng, format!("junk-{c}-{t}-{}-{i}", steps.len()))
                                })
                                .collect();
                            junk.extend(j.iter().map(|r| r.k.clone()));
                            steps.push(Step::Append(j));
                        }
                    }
                    if !junk.is_empty() {
                        steps.push(Step::DeleteKeys(junk));
                    }
                    steps
                })
                .collect()
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
            Step::DeleteKeys(keys) => {
                let keys: Vec<Vec<ScalarValue>> = keys
                    .into_iter()
                    .map(|k| vec![ScalarValue::Utf8(Some(k))])
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
    linger: Duration,
) -> impl Future<Output = Database> + Send + use<> {
    let b = Database::builder_for_store(env.chaos_store(client, FaultRules::default()), ROOT)
        .clock(env.client_clock(client))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .group_commit_linger(linger)
        .request_retry(RetryPolicy::REQUEST);
    async move { b.open().await.expect("open") }
}

/// The queries compared between the two tables.
fn queries(rng: &mut SimRng, keys: &[String]) -> Vec<(Option<Expr>, Option<Vec<&'static str>>)> {
    let g = rng.below(6) as i64;
    let k = keys[rng.below(keys.len() as u64) as usize].clone();
    vec![
        (None, None),
        (Some(col("g").eq(lit(g))), None),
        (Some(col("x").gt(lit(0.0f64))), Some(vec!["k", "x"])),
        (
            Some(col("k").lt_eq(lit(k)).and(col("b").is_not_null())),
            None,
        ),
        (
            Some(col("s").is_null().or(col("g").gt(lit(3i64)))),
            Some(vec!["s", "k", "b"]),
        ),
    ]
}

async fn answers(t: &Table, rng: &mut SimRng, keys: &[String]) -> moraine::Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for (filter, columns) in queries(rng, keys) {
        let mut scan = t.scan();
        if let Some(f) = filter {
            scan = scan.filter(f);
        }
        let s = match &columns {
            Some(c) => {
                scan = scan.select(c.iter().copied());
                let full = schema();
                Arc::new(Schema::new(
                    c.iter()
                        .map(|n| full.field_with_name(n).expect("column").clone())
                        .collect::<Vec<_>>(),
                ))
            }
            None => schema(),
        };
        out.push(canonical(&scan.collect().await?, &s));
    }
    for _ in 0..8 {
        let k = &keys[rng.below(keys.len() as u64) as usize];
        let got = t.get(&[ScalarValue::Utf8(Some(k.clone()))]).await?;
        out.push(canonical(&got.into_iter().collect::<Vec<_>>(), &schema()));
    }
    Ok(out)
}

#[derive(Debug, Default)]
struct Totals {
    seeds: u64,
    files: u64,
    commits: u64,
}

async fn scenario(mut env: SimEnv, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let n = env.rng.range(20, 200) as usize;
    let mut keys: Vec<String> = (0..n).map(|i| format!("key-{i:04}")).collect();
    let rows: Vec<Row> = keys
        .iter()
        .map(|k| random_row(&mut env.rng, k.clone()))
        .collect();
    let clients = env.rng.range(2, 5) as usize;
    let plans = plan_clients(&mut env.rng, &rows, clients);
    let segmented = TableOptions::default()
        .primary_key(["k"])
        .cluster_by(if env.rng.chance(0.5) {
            vec!["g"]
        } else {
            Vec::new()
        })
        .page_row_limit(env.rng.range(1, 64) as usize)
        .row_group_rows(env.rng.range(1, 128) as usize)
        .target_file_bytes(env.rng.range(512, 64 << 10));
    let whole = TableOptions::default().primary_key(["k"]);

    // The reference: one append, default options, before anything else runs.
    let setup = open(&env, clients, Duration::ZERO);
    let rows_ref = rows.clone();
    let setup_fut: ClientFuture = Box::pin(async move {
        let db = setup.await;
        let t = db
            .create_table("whole", schema(), whole)
            .await
            .expect("create");
        t.append_unchecked(vec![batch(&rows_ref)])
            .await
            .expect("append");
        db.create_table("split", schema(), segmented)
            .await
            .expect("create");
    });
    env.run(vec![setup_fut])
        .await
        .map_err(|e| format!("{e:?}"))?;

    let errors: Arc<Mutex<Vec<String>>> = Arc::default();
    let mut futs: Vec<ClientFuture> = Vec::new();
    for (c, tasks) in plans.into_iter().enumerate() {
        // Task 0 opens the handle; the others share it, so group commit can merge them. Each
        // task is its own scheduler client.
        let linger = Duration::from_millis(env.rng.below(3) * 5);
        let slot: Arc<Mutex<Option<Table>>> = Arc::default();
        for (i, steps) in tasks.into_iter().enumerate() {
            let db = (i == 0).then(|| open(&env, c, linger));
            let (slot, errors, clock) = (slot.clone(), errors.clone(), env.client_clock(c));
            futs.push(Box::pin(async move {
                use moraine::clock::Clock;
                let t = match db {
                    Some(db) => {
                        let t = db.await.open_table("split").await.expect("open");
                        *slot.lock().unwrap() = Some(t.clone());
                        t
                    }
                    None => loop {
                        let got = slot.lock().unwrap().clone();
                        if let Some(t) = got {
                            break t;
                        }
                        clock.sleep(Duration::from_millis(5)).await;
                    },
                };
                if let Err(e) = run_steps(t, steps).await {
                    errors
                        .lock()
                        .unwrap()
                        .push(format!("client {c} task {i}: {e}"));
                }
            }));
        }
    }
    env.run(futs).await.map_err(|e| format!("{e:?}"))?;
    if let Some(e) = errors.lock().unwrap().first() {
        return Err(e.clone());
    }

    // Read both from a fresh client and compare.
    let reader = open(&env, clients + 1, Duration::ZERO);
    let mut qrng = env.rng.fork(7);
    keys.push("missing".into());
    let out: Arc<Mutex<Result<(), String>>> = Arc::new(Mutex::new(Ok(())));
    let o = out.clone();
    let tot = totals.clone();
    let read: ClientFuture = Box::pin(async move {
        let db = reader.await;
        let split = db.open_table("split").await.expect("open");
        let whole = db.open_table("whole").await.expect("open");
        let r = async {
            let mut q2 = qrng.clone();
            let a = answers(&whole, &mut qrng, &keys).await?;
            let b = answers(&split, &mut q2, &keys).await?;
            let snap = split.snapshot().await?;
            let mut t = tot.lock().unwrap();
            t.files += snap.num_files() as u64;
            t.commits += snap.version();
            Ok::<_, moraine::Error>((a, b))
        }
        .await;
        *o.lock().unwrap() = match r {
            Err(e) => Err(format!("query failed: {e}")),
            Ok((a, b)) => match a.iter().zip(&b).position(|(x, y)| x != y) {
                Some(i) => Err(format!(
                    "query {i} differs between the segmentations: {}",
                    describe(&a[i], &b[i])
                )),
                None => Ok(()),
            },
        };
    });
    env.run(vec![read]).await.map_err(|e| format!("{e:?}"))?;
    totals.lock().unwrap().seeds += 1;
    out.lock().unwrap().clone()
}

#[test]
fn results_do_not_depend_on_segmentation() {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds_from_env(150), move |env| scenario(env, t.clone())) {
        panic!("{f}");
    }
    let t = totals.lock().unwrap();
    println!("RESULT segmentation {t:?}");
    assert!(
        t.files > 2 * t.seeds,
        "the split tables should hold many files: {t:?}"
    );
}
