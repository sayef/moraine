//! Cancelled writes in the simulation (D56): several clients, each with several tasks sharing
//! one table handle (so writes group), cancel appends at random points. A cancelled write is
//! either dropped after a random number of polls (wherever it is: waiting in the group-commit
//! queue, while the commit task lingers, uploads or puts the log entry) or aborted when a random
//! simulated deadline passes, as `JoinHandle::abort` drops a task at its next await. Every task
//! ends with one plain append.
//!
//! Checked: the run finishes (a cancelled write never stalls the queue), a write that was not
//! cancelled commits when there are no faults, whatever happened to the other writes of its
//! group (D64: the group commits on the handle's task), every acknowledged write is in the
//! log at its version and every failed one is not (I1, I2), no write lands twice (I4), and
//! reads agree with the model (I3, I5). A cancelled write's outcome is unknown,
//! so it may land once or not at all.
//!
//! More seeds: `MORAINE_SIM_SEEDS=10000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, Error, Table, TableOptions};
use moraine_testkit::check::{Outcome, Recorder};
use moraine_testkit::datagen;
use moraine_testkit::model::{ModelOp, Row};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::{read_log, read_parquet};
use moraine_testkit::{FaultRules, SimClock, SimRng};

const ROOT: &str = "db";
const TABLE: &str = "t";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SimRow {
    k: String,
    op: String,
    i: i64,
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
        for r in 0..b.num_rows() {
            if k.is_null(r) {
                continue;
            }
            out.push(SimRow {
                k: k.value(r).to_string(),
                op: op.value(r).to_string(),
                i: i.value(r),
            });
        }
    }
    out
}

/// Polls `fut` at most `polls` times, then drops it: a caller that gives up at an arbitrary
/// await point. `None` when it was dropped unfinished.
struct DropAfter<F> {
    fut: Option<Pin<Box<F>>>,
    polls: u32,
}

impl<F: Future> Future for DropAfter<F> {
    type Output = Option<F::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Dropped at the start of a poll, when the write was woken: a future that just stopped
        // at a scheduler gate is not running, so dropping it there would let this client run
        // on without being scheduled.
        if self.polls == 0 {
            self.fut = None;
        }
        self.polls = self.polls.saturating_sub(1);
        let Some(f) = self.fut.as_mut() else {
            return Poll::Ready(None);
        };
        f.as_mut().poll(cx).map(Some)
    }
}

/// Polls `fut` until simulated time passes `deadline`, then drops it at its next poll, as
/// aborting a task does. `None` when it was dropped unfinished.
struct DropAt<F> {
    fut: Option<Pin<Box<F>>>,
    deadline: Duration,
    clock: SimClock,
}

impl<F: Future> Future for DropAt<F> {
    type Output = Option<F::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.clock.now() >= self.deadline {
            self.fut = None;
        }
        let Some(f) = self.fut.as_mut() else {
            return Poll::Ready(None);
        };
        f.as_mut().poll(cx).map(Some)
    }
}

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    ops: u64,
    dropped: u64,
    aborted: u64,
    acked: u64,
    failed: u64,
    unknown: u64,
    cancelled_landed: u64,
    versions: u64,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn open_db(
    env: &SimEnv,
    client: usize,
    rules: &FaultRules,
    linger: Duration,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    Database::builder_for_store(env.chaos_store(client, rules.clone()), ROOT)
        .clock(env.client_clock(client))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .group_commit_linger(linger)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

/// How a write is cancelled, if at all.
#[derive(Clone, Copy, Debug)]
enum Cancel {
    None,
    /// Dropped after this many polls.
    Drop(u32),
    /// Aborted after this many simulated milliseconds.
    Abort(u64),
}

async fn scenario(mut env: SimEnv, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let clients = env.rng.range(1, 3) as usize;
    let tasks_per_client = env.rng.range(2, 5) as usize;
    let ops_per_task = env.rng.range(1, 4) as usize;
    let faults = env.rng.chance(0.3);
    let rules = if faults {
        FaultRules::random(&mut env.rng)
    } else {
        FaultRules::default()
    };
    let recorder = Recorder::<SimRow>::default();
    let violations: Arc<Mutex<Vec<String>>> = Arc::default();
    let counts: Arc<Mutex<(u64, u64)>> = Arc::default();

    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        // A linger makes the leader wait before taking its group, so cancellations land there too.
        let linger = Duration::from_millis(env.rng.below(4));
        let slot: Arc<Mutex<Option<Option<Table>>>> = Arc::default();
        for t in 0..tasks_per_client {
            let task = c * 10 + t;
            let mut rng: SimRng = env.rng.fork(task as u64 + 1);
            let (rec, viol, slot, counts) = (
                recorder.clone(),
                violations.clone(),
                slot.clone(),
                counts.clone(),
            );
            let clock = env.client_clock(c);
            let sim = env.clock.clone();
            let db = (t == 0).then(|| open_db(&env, c, &rules, linger));
            futs.push(Box::pin(async move {
                use moraine::clock::Clock;
                let table = match db {
                    Some(db) => {
                        let mut table = None;
                        if let Ok(db) = db.await {
                            for _ in 0..10 {
                                if let Ok(t) = db
                                    .create_table(
                                        TABLE,
                                        datagen::row_schema(),
                                        TableOptions::default(),
                                    )
                                    .await
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
                // The last op of every task is never cancelled: it must finish.
                for j in 0..=ops_per_task {
                    let op = format!("c{c}t{t}o{j}");
                    let cancel = if j == ops_per_task {
                        Cancel::None
                    } else {
                        match rng.below(3) {
                            0 => Cancel::None,
                            1 => Cancel::Drop(rng.range(1, 12) as u32),
                            _ => Cancel::Abort(rng.below(5)),
                        }
                    };
                    let n = rng.range(1, 4) as usize;
                    let batch = datagen::rows(&mut rng, &op, n, None, 4);
                    let mut rows = to_rows(std::slice::from_ref(&batch));
                    rows.sort();
                    rec.intend(task, &op, ModelOp::Append(rows));
                    let r: Option<moraine::Result<moraine::Committed>> = match cancel {
                        Cancel::None => Some(table.append(vec![batch]).await),
                        Cancel::Drop(polls) => {
                            let r = DropAfter {
                                fut: Some(Box::pin(table.append(vec![batch]))),
                                polls,
                            }
                            .await;
                            if r.is_none() {
                                counts.lock().unwrap().0 += 1;
                            }
                            r
                        }
                        Cancel::Abort(ms) => {
                            // What `JoinHandle::abort` does to a task: the write is dropped when
                            // it is next polled after the deadline. (The scheduler runs one gate
                            // per client at a time, so the deadline is checked, not awaited.)
                            let r = DropAt {
                                fut: Some(Box::pin(table.append(vec![batch]))),
                                deadline: sim.now() + Duration::from_millis(ms),
                                clock: sim.clone(),
                            }
                            .await;
                            if r.is_none() {
                                counts.lock().unwrap().1 += 1;
                            }
                            r
                        }
                    };
                    let outcome = match &r {
                        None | Some(Err(Error::OutcomeUnknown { .. })) => Outcome::Unknown,
                        Some(Ok(c)) => Outcome::Acked { version: c.version },
                        Some(Err(e)) => Outcome::Failed(e.to_string()),
                    };
                    if matches!(cancel, Cancel::None) && !faults && !matches!(r, Some(Ok(_))) {
                        // Without faults, a write that was not cancelled commits, whatever
                        // happened to the other writes of its group: the group commits on the
                        // handle's task, which no caller's cancellation stops (D64).
                        viol.lock()
                            .unwrap()
                            .push(format!("append {op} failed without faults: {r:?}"));
                    }
                    rec.outcome(task, &op, outcome);
                    if let Ok(snap) = table.snapshot().await
                        && let Ok(b) = snap.scan().collect().await
                    {
                        rec.read(task, snap.version(), to_rows(&b));
                    }
                }
            }));
        }
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish (a cancelled write stalled the queue?): {e:?}"))?;
    if let Some(v) = violations.lock().unwrap().first() {
        return Err(v.clone());
    }

    let root = format!("{ROOT}/{TABLE}/data");
    let entries = read_log(&env.store, &root);
    let mut log = Vec::new();
    for e in entries.iter().skip(1) {
        let mut by_op: BTreeMap<String, Vec<SimRow>> = BTreeMap::new();
        for p in &e.adds {
            for r in to_rows(&read_parquet(&env.store, &format!("{root}/{p}"))) {
                by_op.entry(r.op.clone()).or_default().push(r);
            }
        }
        for (op, mut rows) in by_op {
            rows.sort();
            log.push((e.version, op, ModelOp::Append(rows)));
        }
    }
    let landed: std::collections::BTreeSet<String> =
        log.iter().map(|(_, op, _)| op.clone()).collect();
    let outcomes = recorder.outcomes();
    let history = recorder.history(log);
    let (acked, failed, unknown) = recorder.outcome_counts();
    {
        let mut tot = totals.lock().unwrap();
        let k = counts.lock().unwrap();
        tot.seeds += 1;
        tot.ops += (clients * tasks_per_client * (ops_per_task + 1)) as u64;
        tot.dropped += k.0;
        tot.aborted += k.1;
        tot.acked += acked as u64;
        tot.failed += failed as u64;
        tot.unknown += unknown as u64;
        tot.cancelled_landed += outcomes
            .iter()
            .filter(|(op, o)| matches!(o, Outcome::Unknown) && landed.contains(*op))
            .count() as u64;
        tot.versions += entries.len().saturating_sub(1) as u64;
    }
    history.check(false).map_err(|v| v.to_string())
}

/// Seeds that failed during development; they run on every test run.
const REGRESSION_SEEDS: &[u64] = &[];

#[test]
fn cancelled_writes() {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let seeds = seeds_from_env(300);
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds.clone(), move |env| scenario(env, t.clone())) {
        panic!("{f}");
    }
    for &s in REGRESSION_SEEDS {
        let t = totals.clone();
        if let Err(f) = run_seeds(s..s + 1, move |env| scenario(env, t.clone())) {
            panic!("{f}");
        }
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT cancel_sim seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    assert!(t.dropped > 0 && t.aborted > 0, "nothing was cancelled");
    assert!(t.cancelled_landed > 0, "no cancelled write ever committed");
}
