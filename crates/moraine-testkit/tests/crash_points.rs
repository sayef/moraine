//! Case C8 for the append path, with the `fail` crate's failpoints: a writer crashes (panics) at
//! each step boundary of a commit, its "process" restarts with a new handle and carries on, and
//! the run is checked against the model. A crash before the log put leaves nothing visible
//! (I2, checked by recording it as failed); a crash after it may have committed (unknown).
//!
//! Failpoints are global to the process, so this file is its own test binary with one test.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::array::{AsArray, RecordBatch};
use futures::FutureExt;
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, Error, Table, TableOptions};
use moraine_testkit::FaultRules;
use moraine_testkit::check::{Outcome, Recorder};
use moraine_testkit::datagen;
use moraine_testkit::model::{ModelOp, Row};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::{read_log, read_parquet};

const POINTS: &[(&str, bool)] = &[
    ("moraine::append::before_upload", false),
    ("moraine::append::after_upload", false),
    ("moraine::append::before_commit", false),
    ("moraine::append::after_commit", true),
];

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct R(String, String);

impl Row for R {
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

fn rows(b: &[RecordBatch]) -> Vec<R> {
    let mut out = Vec::new();
    for b in b {
        let k = b.column_by_name("k").unwrap().as_string::<i32>();
        let op = b.column_by_name("op").unwrap().as_string::<i32>();
        for i in 0..b.num_rows() {
            out.push(R(k.value(i).into(), op.value(i).into()));
        }
    }
    out.sort();
    out
}

/// Client `c`'s `life`-th process: a restarted process draws fresh randomness (writer id, file
/// names), as a real one does.
fn open(
    env: &SimEnv,
    c: usize,
    life: u64,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    let clock = moraine_testkit::ClientClock::new(
        env.clock.clone(),
        Some(env.sched.clone()),
        c,
        // Hashed, not `seed ^ life * GOLDEN`: SplitMix64 steps its state by GOLDEN, so such
        // seeds can give two processes the same stream shifted by one draw, and with it the
        // same data file names (D32).
        moraine::testing::splitmix64(
            env.seed ^ moraine::testing::splitmix64((c as u64) << 32 | life),
        ),
    );
    Database::builder_for_store(env.chaos_store(c, FaultRules::default()), "db")
        .clock(Arc::new(clock))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

async fn scenario(mut env: SimEnv, point: &'static str, may_commit: bool) -> Result<(), String> {
    fail::cfg(point, "1*panic").map_err(|e| e.to_string())?;
    let clients = env.rng.range(2, 4) as usize;
    let rec = Recorder::<R>::default();
    let crashes: Arc<Mutex<u64>> = Arc::default();
    // Client 0 creates the table first so every client can open it.
    let setup = open(&env, 0, 99);
    env.run(vec![Box::pin(async move {
        let db = setup.await.unwrap();
        db.create_table("t", datagen::row_schema(), TableOptions::default())
            .await
            .unwrap();
    }) as ClientFuture])
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let (rec, crashes) = (rec.clone(), crashes.clone());
        let mut rng = env.rng.fork(c as u64);
        let restarts: Vec<_> = (0..4).map(|life| open(&env, c, life)).collect();
        futs.push(Box::pin(async move {
            let mut restarts = restarts.into_iter();
            let mut table: Option<Table> = None;
            for j in 0..3 {
                if table.is_none() {
                    let Some(db) = restarts.next() else { return };
                    table = Some(db.await.unwrap().open_table("t").await.unwrap());
                }
                let t = table.clone().unwrap();
                let op = format!("c{c}o{j}");
                let b = datagen::rows(&mut rng, &op, 2, None, 2);
                rec.intend(c, &op, ModelOp::Append(rows(std::slice::from_ref(&b))));
                match AssertUnwindSafe(t.append(vec![b])).catch_unwind().await {
                    Ok(Ok(w)) => rec.outcome(c, &op, Outcome::Acked { version: w.version }),
                    Ok(Err(Error::OutcomeUnknown { .. })) => rec.outcome(c, &op, Outcome::Unknown),
                    Ok(Err(e)) => rec.outcome(c, &op, Outcome::Failed(e.to_string())),
                    Err(_) => {
                        // The process died here; a crash before the log put must leave
                        // nothing visible.
                        *crashes.lock().unwrap() += 1;
                        let o = if may_commit {
                            Outcome::Unknown
                        } else {
                            Outcome::Failed("crashed before the commit".into())
                        };
                        rec.outcome(c, &op, o);
                        table = None;
                        continue;
                    }
                }
                if let Ok(s) = t.snapshot().await
                    && let Ok(b) = s.scan().collect().await
                {
                    rec.read(c, s.version(), rows(&b));
                }
            }
        }));
    }
    let r = env.run(futs).await;
    fail::remove(point);
    r.map_err(|e| format!("{e:?}"))?;
    let root = "db/t/data";
    let mut log = Vec::new();
    for e in read_log(&env.store, root).iter().skip(1) {
        if std::env::var("CRASH_DEBUG").is_ok() {
            eprintln!("v{} adds={:?} txns={:?}", e.version, e.adds, e.txns);
        }
        let mut by_op: std::collections::BTreeMap<String, Vec<R>> = Default::default();
        for p in &e.adds {
            for r in rows(&read_parquet(&env.store, &format!("{root}/{p}"))) {
                by_op.entry(r.1.clone()).or_default().push(r);
            }
        }
        for (op, mut v) in by_op {
            v.sort();
            log.push((e.version, op, ModelOp::Append(v)));
        }
    }
    if *crashes.lock().unwrap() == 0 {
        return Err(format!("{point} never fired"));
    }
    rec.history(log).check(false).map_err(|v| v.to_string())
}

#[test]
fn crash_at_every_append_failpoint() {
    let _scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| {}));
    let seeds = seeds_from_env(100);
    for &(point, may_commit) in POINTS {
        if let Err(f) = run_seeds(seeds.clone(), |env| scenario(env, point, may_commit)) {
            let _ = std::panic::take_hook();
            panic!("{point}: {f}");
        }
        println!(
            "RESULT crash_points {point}: {} seeds clean",
            seeds.end - seeds.start
        );
    }
    let _ = std::panic::take_hook();
}
