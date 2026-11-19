//! Table acceptance through the simulation: tables, appends,
//! group commit, snapshot reads, point lookups and refresh, with several clients on one
//! simulated store, faults from `ChaosStore`, and every run checked against the reference model.
//!
//! Named scenarios (docs/design/test-ids.md): 1 `no_lost_appends`, 4 `ambiguous_commit_is_idempotent`,
//! 16 `create_table_race`, 17 `store_without_conditional_put`, 19 `probe_under_faults`.
//! `tables_random` mixes everything
//! with random faults. The committed history is read from the store without moraine
//! (`moraine_testkit::table_log`), so moraine's own log reader is checked too.
//!
//! More seeds: `MORAINE_SIM_SEEDS=10000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use moraine::expr::ScalarValue;
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, Error, Table, TableOptions};
use moraine_testkit::chaos::Conditional;
use moraine_testkit::check::{Outcome, Recorder};
use moraine_testkit::datagen;
use moraine_testkit::model::{ModelOp, Row};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::{read_log, read_parquet};
use moraine_testkit::{FaultRules, SimRng};

const ROOT: &str = "db";
const TABLE: &str = "t";

/// Seeds that failed during development; they run on every test run.
const REGRESSION_SEEDS: &[u64] = &[];

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SimRow {
    k: String,
    op: String,
    i: i64,
    g: Option<i64>,
}

impl Row for SimRow {
    type Key = String;
    type Pred = ();
    type Patch = ();
    fn key(&self) -> Option<String> {
        None
    }
    fn matches(&self, _: &()) -> bool {
        false
    }
    fn patched(&self, _: &()) -> Self {
        self.clone()
    }
}

fn to_rows(batches: &[RecordBatch]) -> Vec<SimRow> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column_by_name("k").expect("k").as_string::<i32>();
        let op = b.column_by_name("op").expect("op").as_string::<i32>();
        let i = b
            .column_by_name("i")
            .expect("i")
            .as_primitive::<Int64Type>();
        let g = b
            .column_by_name("g")
            .expect("g")
            .as_primitive::<Int64Type>();
        for r in 0..b.num_rows() {
            out.push(SimRow {
                k: k.value(r).to_string(),
                op: op.value(r).to_string(),
                i: i.value(r),
                g: (!g.is_null(r)).then(|| g.value(r)),
            });
        }
    }
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Random faults, consistency modes, small-table cache on or off, invalid batches.
    Random,
    /// No faults: every append must commit (scenario 1).
    NoFaults,
    /// Every log put takes effect and loses its response (scenario 4).
    AllAmbiguous,
    /// Every log put takes effect and then fails with an unclassified `Generic` error.
    GenericAfter,
}

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    clients: u64,
    tasks: u64,
    ops: u64,
    acked: u64,
    failed: u64,
    unknown: u64,
    versions: u64,
    grouped_versions: u64,
    reads: u64,
    gets: u64,
    requests: u64,
    faults: u64,
    ambiguous_faults: u64,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
/// A client of the creation race and how its last attempt ended.
type CreateResult = (usize, Result<(), String>);

fn options(keyed: bool) -> TableOptions {
    if keyed {
        TableOptions::default()
            .primary_key(["k"])
            .enforce_primary_key(false)
            .cluster_by(["g"])
            .page_row_limit(2)
    } else {
        TableOptions::default().page_row_limit(2)
    }
}

fn open_db(
    env: &SimEnv,
    client: usize,
    rules: &FaultRules,
    consistency: Consistency,
    small: bool,
    linger: Duration,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    let builder = Database::builder_for_store(env.chaos_store(client, rules.clone()), ROOT)
        .clock(env.client_clock(client))
        .io_concurrency(1)
        .consistency(consistency)
        .small_table_bytes(if small { 64 << 20 } else { 0 })
        .group_commit_linger(linger)
        .request_retry(RetryPolicy::REQUEST);
    builder.open()
}

/// A batch the table must refuse (case C17): a column it does not have.
fn invalid_batch(op: &str) -> RecordBatch {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("nope", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![format!("bad#{op}")])),
            Arc::new(StringArray::from(vec![op.to_string()])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .expect("batch")
}

async fn scenario(mut env: SimEnv, mode: Mode, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let clients = env.rng.range(2, 5) as usize;
    let tasks_per_client = env.rng.range(1, 4) as usize;
    let ops_per_task = env.rng.range(1, 5) as usize;
    let keyed = env.rng.chance(0.6);
    let rules = match mode {
        Mode::Random => FaultRules::random(&mut env.rng),
        Mode::NoFaults => FaultRules::default(),
        Mode::AllAmbiguous => FaultRules {
            ambiguous_commits: 1.0,
            ..Default::default()
        },
        Mode::GenericAfter => FaultRules {
            generic_after_commits: 1.0,
            ..Default::default()
        },
    };
    let recorder = Recorder::<SimRow>::default();
    let violations: Arc<Mutex<Vec<String>>> = Arc::default();
    let counters: Arc<Mutex<(u64, u64)>> = Arc::default();

    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let consistency = match (mode, env.rng.below(3)) {
            (Mode::Random, 0) => Consistency::Strong,
            (Mode::Random, 1) => Consistency::Manual,
            _ => Consistency::Eventual {
                max_staleness: Duration::from_millis(env.rng.range(0, 200)),
            },
        };
        let small = mode == Mode::Random && env.rng.chance(0.5);
        let linger = Duration::from_millis(env.rng.below(3));
        let slot: Arc<Mutex<Option<Option<Table>>>> = Arc::default();
        for t in 0..tasks_per_client {
            let task = c * 10 + t;
            let mut rng = env.rng.fork(task as u64 + 1);
            let (rec, viol, slot, counters) = (
                recorder.clone(),
                violations.clone(),
                slot.clone(),
                counters.clone(),
            );
            let clock = env.client_clock(c);
            let db = if t == 0 {
                Some(open_db(&env, c, &rules, consistency, small, linger))
            } else {
                None
            };
            futs.push(Box::pin(async move {
                use moraine::clock::Clock;
                // Task 0 opens the handle; the others share it (group commit, case C16).
                let table = match db {
                    Some(db) => {
                        let mut table = None;
                        if let Ok(db) = db.await {
                            for _ in 0..10 {
                                if let Ok(t) =
                                    db.create_table(TABLE, datagen::row_schema(), options(keyed)).await
                                {
                                    table = Some(t);
                                    break;
                                }
                            }
                        }
                        *slot.lock().unwrap() = Some(table.clone());
                        table
                    }
                    None => loop {
                        let got = slot.lock().unwrap().clone();
                        if let Some(t) = got {
                            break t;
                        }
                        clock.sleep(Duration::from_millis(1)).await;
                    },
                };
                let Some(table) = table else {
                    return;
                };
                let mut acked_keys: Vec<(String, u64, SimRow)> = Vec::new();
                for j in 0..ops_per_task {
                    let op = format!("c{c}t{t}o{j}");
                    let invalid = mode == Mode::Random && rng.chance(0.1);
                    let n = rng.range(1, 6) as usize;
                    let batch = if invalid {
                        invalid_batch(&op)
                    } else {
                        datagen::rows(&mut rng, &op, n, None, 4)
                    };
                    let mut rows = if invalid { Vec::new() } else { to_rows(std::slice::from_ref(&batch)) };
                    rows.sort();
                    rec.intend(task, &op, ModelOp::Append(rows.clone()));
                    let r = table.append(vec![batch]).await;
                    let outcome = match &r {
                        Ok(c) => Outcome::Acked { version: c.version },
                        Err(Error::OutcomeUnknown { .. }) => Outcome::Unknown,
                        Err(e) => Outcome::Failed(e.to_string()),
                    };
                    if invalid && !matches!(r, Err(Error::InvalidBatch(_))) {
                        viol.lock().unwrap().push(format!(
                            "C17: invalid batch of {op} returned {r:?}, not InvalidBatch"
                        ));
                    }
                    if mode != Mode::Random && r.is_err() {
                        viol.lock()
                            .unwrap()
                            .push(format!("{mode:?}: append {op} failed: {r:?}"));
                    }
                    rec.outcome(task, &op, outcome);
                    if let Ok(v) = r
                        && let Some(row) = rows.first()
                    {
                        acked_keys.push((row.k.clone(), v.version, row.clone()));
                    }
                    // A read after every write: I3 at its version, I5 for this task.
                    let Ok(snap) = table.snapshot().await else {
                        continue;
                    };
                    if let Ok(b) = snap.scan().collect().await {
                        rec.read(task, snap.version(), to_rows(&b));
                        counters.lock().unwrap().0 += 1;
                    }
                    // A point lookup of a key this task wrote at or before that version.
                    if keyed && !acked_keys.is_empty() {
                        let (k, v, row) = acked_keys[rng.below(acked_keys.len() as u64) as usize].clone();
                        if v <= snap.version() {
                            match snap.get(&[ScalarValue::Utf8(Some(k.clone()))]).await {
                                Ok(Some(b)) => {
                                    counters.lock().unwrap().1 += 1;
                                    let got = to_rows(&[b]);
                                    if got != vec![row.clone()] {
                                        viol.lock().unwrap().push(format!(
                                            "I1: get({k:?}) at version {} returned {got:?}, expected {row:?}",
                                            snap.version()
                                        ));
                                    }
                                }
                                Ok(None) => viol.lock().unwrap().push(format!(
                                    "I1: get({k:?}) at version {} found nothing; written at {v}",
                                    snap.version()
                                )),
                                Err(_) => {}
                            }
                        }
                        // A key nobody wrote is absent.
                        if let Ok(Some(b)) = snap
                            .get(&[ScalarValue::Utf8(Some(format!("absent#{op}")))])
                            .await
                        {
                            viol.lock()
                                .unwrap()
                                .push(format!("get of an absent key returned {b:?}"));
                        }
                    }
                }
            }));
        }
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;

    if let Some(v) = violations.lock().unwrap().first() {
        return Err(v.clone());
    }

    // The committed history, read without moraine.
    let root = format!("{ROOT}/{TABLE}/data");
    let entries = read_log(&env.store, &root);
    let mut log = Vec::new();
    let mut grouped = 0;
    for e in entries.iter().skip(1) {
        if e.has_metadata || !e.removes.is_empty() {
            return Err(format!(
                "version {} changes metadata or removes files",
                e.version
            ));
        }
        if e.txns.len() != 1 {
            return Err(format!(
                "version {} has {} txn actions",
                e.version,
                e.txns.len()
            ));
        }
        let mut by_op: BTreeMap<String, Vec<SimRow>> = BTreeMap::new();
        for p in &e.adds {
            for r in to_rows(&read_parquet(&env.store, &format!("{root}/{p}"))) {
                by_op.entry(r.op.clone()).or_default().push(r);
            }
        }
        if by_op.len() > 1 {
            grouped += 1;
        }
        for (op, mut rows) in by_op {
            rows.sort();
            log.push((e.version, op, ModelOp::Append(rows)));
        }
    }
    let history = recorder.history(log);

    // A fresh client without faults sees the final version (I1, I3 through moraine's reader).
    if !entries.is_empty() {
        let fresh = open_db(
            &env,
            99,
            &FaultRules::default(),
            Consistency::Strong,
            false,
            Duration::ZERO,
        )
        .await
        .map_err(|e| e.to_string())?;
        let mut sched_free = Vec::<ClientFuture>::new();
        let rec = recorder.clone();
        sched_free.push(Box::pin(async move {
            let t = fresh.open_table(TABLE).await.expect("open");
            let s = t.snapshot().await.expect("snapshot");
            let b = s.scan().collect().await.expect("scan");
            rec.read(999, s.version(), to_rows(&b));
        }));
        env.run(sched_free)
            .await
            .map_err(|e| format!("final read did not finish: {e:?}"))?;
    }

    let (acked, failed, unknown) = recorder.outcome_counts();
    {
        let mut t = totals.lock().unwrap();
        let c = counters.lock().unwrap();
        t.seeds += 1;
        t.clients += clients as u64;
        t.tasks += (clients * tasks_per_client) as u64;
        t.ops += (clients * tasks_per_client * ops_per_task) as u64;
        t.acked += acked as u64;
        t.failed += failed as u64;
        t.unknown += unknown as u64;
        t.versions += entries.len().saturating_sub(1) as u64;
        t.grouped_versions += grouped;
        t.reads += c.0;
        t.gets += c.1;
        t.requests += env.requests.snapshot().requests();
        t.faults += env.faults.total();
        t.ambiguous_faults += env.faults.ambiguous();
    }
    history.check(false).map_err(|v| v.to_string())
}

fn run(mode: Mode, seeds: std::ops::Range<u64>) -> Totals {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds.clone(), move |env| scenario(env, mode, t.clone())) {
        panic!("{f}");
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT tables_sim {mode:?} seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    t
}

/// Scenario 1: N writers, M appends each, no faults: every append commits, gaplessly, and the
/// final table holds every row once.
#[test]
fn no_lost_appends() {
    let t = run(Mode::NoFaults, seeds_from_env(300));
    assert_eq!(t.acked, t.ops, "every append commits without faults");
    assert!(
        t.grouped_versions > 0,
        "group commit never merged two callers"
    );
}

/// Scenario 4: every log put takes effect and its response is lost. Each write is looked up by
/// its transaction id and acknowledged once; nothing lands twice (I10).
#[test]
fn ambiguous_commit_is_idempotent() {
    let t = run(Mode::AllAmbiguous, seeds_from_env(200));
    assert_eq!(t.acked, t.ops);
    assert!(t.ambiguous_faults >= t.versions);
}

/// Scenario 4 with errors that name no failure class: every log put takes effect and then fails
/// with a plain `Generic` error. Such an error does not say the write was not made, so each
/// write is looked up by its transaction id, acknowledged once, and never reported as failed.
#[test]
fn generic_error_after_commit_is_resolved() {
    let t = run(Mode::GenericAfter, seeds_from_env(200));
    assert_eq!(
        t.acked, t.ops,
        "a commit that landed was reported as an error"
    );
    assert!(t.ambiguous_faults >= t.versions);
}

/// Everything at once, with random faults (T3 and T4 for tables and appends).
#[test]
fn tables_random() {
    let t = run(Mode::Random, seeds_from_env(500));
    assert!(t.failed > 0 && t.acked > 0 && t.ambiguous_faults > 0);
}

#[test]
fn regression_seeds() {
    for &seed in REGRESSION_SEEDS {
        run(Mode::Random, seed..seed + 1);
    }
}

/// Scenario 16: clients create the same table at once. Identical definitions: all succeed on one
/// table. Conflicting definitions: exactly one wins, the rest get
/// `TableExistsWithDifferentSchema` (case C12).
#[test]
fn create_table_race() {
    let seeds = seeds_from_env(300);
    let r = run_seeds(seeds.clone(), |mut env| async move {
        let n = env.rng.range(2, 6) as usize;
        let conflicting = env.rng.chance(0.5);
        let rules = FaultRules {
            conditional: Conditional::Honour,
            ..FaultRules::random(&mut env.rng)
        };
        let results: Arc<Mutex<Vec<CreateResult>>> = Arc::default();
        let mut futs: Vec<ClientFuture> = Vec::new();
        for c in 0..n {
            let db = open_db(&env, c, &rules, Consistency::Strong, false, Duration::ZERO);
            let results = results.clone();
            let opts = if conflicting && c % 2 == 1 {
                options(true)
            } else {
                options(false)
            };
            futs.push(Box::pin(async move {
                let db = db.await.expect("open db");
                // Retry transient failures; the outcome that counts is the last one.
                let mut last = Err("not run".to_string());
                for _ in 0..10 {
                    match db
                        .create_table(TABLE, datagen::row_schema(), opts.clone())
                        .await
                    {
                        Ok(_) => {
                            last = Ok(());
                            break;
                        }
                        Err(Error::TableExistsWithDifferentSchema(_)) => {
                            last = Err("different".into());
                            break;
                        }
                        Err(e) => last = Err(e.to_string()),
                    }
                }
                results.lock().unwrap().push((c, last));
            }));
        }
        env.run(futs).await.map_err(|e| format!("{e:?}"))?;
        let entries = read_log(&env.store, &format!("{ROOT}/{TABLE}/data"));
        let results = results.lock().unwrap().clone();
        if entries.len() > 1 {
            return Err(format!("{} log entries after creation only", entries.len()));
        }
        let ok: Vec<usize> = results
            .iter()
            .filter(|r| r.1.is_ok())
            .map(|r| r.0)
            .collect();
        let different = results
            .iter()
            .filter(|r| r.1.as_ref().err().is_some_and(|e| e == "different"))
            .count();
        let other_errors = results.len() - ok.len() - different;
        if entries.is_empty() {
            return if ok.is_empty() {
                Ok(())
            } else {
                Err(format!("clients {ok:?} succeeded but no table exists"))
            };
        }
        if !conflicting && different > 0 {
            return Err("identical definitions reported as different".into());
        }
        if conflicting && other_errors == 0 {
            // Every client finished: the winner's definition decides which side succeeded.
            let sides: std::collections::BTreeSet<usize> = ok.iter().map(|c| c % 2).collect();
            if sides.len() != 1 {
                return Err(format!("both definitions succeeded: {results:?}"));
            }
        }
        Ok(())
    });
    if let Err(f) = r {
        panic!("{f}");
    }
}

/// Scenario 17: a store that ignores (or rejects) conditional writes. Writing is refused with
/// `ConditionalWritesUnsupported`; reading a table another client created works (case S2).
#[test]
fn store_without_conditional_put() {
    let r = run_seeds(0..50, |mut env| async move {
        let mode = if env.rng.chance(0.5) {
            Conditional::Ignore
        } else {
            Conditional::Reject
        };
        let bad = FaultRules {
            conditional: mode,
            ..Default::default()
        };
        let good = open_db(
            &env,
            0,
            &FaultRules::default(),
            Consistency::Strong,
            false,
            Duration::ZERO,
        );
        let broken = open_db(&env, 1, &bad, Consistency::Strong, false, Duration::ZERO);
        let out: Arc<Mutex<Vec<String>>> = Arc::default();
        let o = out.clone();
        let fut: ClientFuture = Box::pin(async move {
            let good = good.await.expect("db");
            let broken = broken.await.expect("db");
            match broken
                .create_table("other", datagen::row_schema(), options(false))
                .await
            {
                Err(Error::ConditionalWritesUnsupported(_)) => {}
                r => o
                    .lock()
                    .unwrap()
                    .push(format!("create on {mode:?} store: {r:?}")),
            }
            let t = good
                .create_table(TABLE, datagen::row_schema(), options(false))
                .await
                .expect("create");
            let mut rng = SimRng::new(1);
            t.append(vec![datagen::rows(&mut rng, "w", 3, None, 2)])
                .await
                .expect("append");
            let rt = broken.open_table(TABLE).await.expect("read-only open");
            match rt
                .append(vec![datagen::rows(&mut rng, "x", 1, None, 2)])
                .await
            {
                Err(Error::ConditionalWritesUnsupported(_)) => {}
                r => o
                    .lock()
                    .unwrap()
                    .push(format!("append on {mode:?} store: {r:?}")),
            }
            let n = rt.scan().count().await.expect("read");
            if n != 3 {
                o.lock()
                    .unwrap()
                    .push(format!("read-only scan saw {n} rows"));
            }
        });
        env.run(vec![fut]).await.map_err(|e| format!("{e:?}"))?;
        match out.lock().unwrap().first() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    });
    if let Err(f) = r {
        panic!("{f}");
    }
}

/// Scenario 19: the capability probe under faults (D92). Clients open a database and write while
/// random faults hit every request, probe writes are resent (the first send takes effect, the
/// client sees the second answer) and lose their responses. On a store that honours conditional
/// writes no client is ever refused with `ConditionalWritesUnsupported` and each one writes in
/// the end; on a store that ignores or rejects them every write is refused or fails, and none
/// succeeds.
#[test]
fn probe_under_faults() {
    let resent = Arc::new(Mutex::new((0u64, 0u64, 0u64)));
    let tally = resent.clone();
    let seeds = seeds_from_env(300);
    let r = run_seeds(seeds.clone(), move |mut env| {
        let tally = tally.clone();
        async move {
            let mode = match env.rng.below(10) {
                0 => Conditional::Ignore,
                1 => Conditional::Reject,
                _ => Conditional::Honour,
            };
            let mut rules = FaultRules::random(&mut env.rng);
            rules.conditional = mode;
            rules.resend_probes = env.rng.below(700) as f64 / 1000.0;
            let clients = env.rng.range(1, 4) as usize;
            let out: Arc<Mutex<Vec<String>>> = Arc::default();
            let wrote = Arc::new(Mutex::new(0u64));
            let mut futs: Vec<ClientFuture> = Vec::new();
            for c in 0..clients {
                let db = open_db(&env, c, &rules, Consistency::Strong, false, Duration::ZERO);
                let (o, w) = (out.clone(), wrote.clone());
                let mut rng = SimRng::new(env.seed ^ c as u64);
                futs.push(Box::pin(async move {
                    let fail = |m: String| o.lock().unwrap().push(format!("client {c}: {m}"));
                    let db = match db.await {
                        Ok(db) => db,
                        // Opening reads the store; a fault may end it, but never with a verdict.
                        Err(Error::ConditionalWritesUnsupported(m)) => {
                            return fail(format!("refused at open on {mode:?}: {m}"));
                        }
                        Err(_) => return,
                    };
                    let name = format!("t{c}");
                    for _ in 0..20 {
                        let r = match db.open_table(&name).await {
                            Ok(t) => t
                                .append(vec![datagen::rows(&mut rng, "p", 2, None, 2)])
                                .await
                                .map(|_| ()),
                            Err(_) => db
                                .create_table(&name, datagen::row_schema(), options(false))
                                .await
                                .map(|_| ()),
                        };
                        match (mode, r) {
                            (Conditional::Honour, Ok(())) => {
                                *w.lock().unwrap() += 1;
                                return;
                            }
                            (Conditional::Honour, Err(Error::ConditionalWritesUnsupported(m))) => {
                                return fail(format!("an honouring store was refused: {m}"));
                            }
                            (Conditional::Honour, Err(_)) => {}
                            (_, Ok(())) => {
                                return fail(format!("a write succeeded on a {mode:?} store"));
                            }
                            (_, Err(_)) => {}
                        }
                    }
                    if mode == Conditional::Honour {
                        fail("no write succeeded in 20 attempts".into());
                    }
                }));
            }
            env.run(futs).await.map_err(|e| format!("{e:?}"))?;
            let resends = env.faults.resent.load(std::sync::atomic::Ordering::Relaxed);
            let lost = env.faults.ambiguous();
            {
                let mut t = tally.lock().unwrap();
                t.0 += resends;
                t.1 += lost;
                t.2 += *wrote.lock().unwrap();
            }
            match out.lock().unwrap().first() {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }
    });
    if let Err(f) = r {
        panic!("{f}");
    }
    let (resends, lost, wrote) = *resent.lock().unwrap();
    println!(
        "RESULT probe_under_faults seeds={}..{} resent={resends} ambiguous={lost} wrote={wrote}",
        seeds.start, seeds.end
    );
    assert!(resends > 0 && lost > 0 && wrote > 0);
}
