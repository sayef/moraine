//! Scenario 5 (`crash_everywhere`) for rewrites (deferred from upserts and deletes) and every
//! maintenance job: for each failpoint, a client crashes (panics) there, restarts as a new
//! process with a new handle and carries on; afterwards a fresh client runs maintenance past the
//! grace period, and the store's history is checked as in `tests/maintenance_sim.rs` (rows,
//! collectors, and `verify`). Queued writes crash at their own points with a keyless table.
//!
//! Failpoints are global to the process, so this file is its own test binary with one test.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::AsArray;
use futures::FutureExt;
use moraine::expr::ScalarValue;
use moraine::store::RetryPolicy;
use moraine::{CompactionPolicy, Consistency, Database, MaintainOptions, Table, TableOptions};
use moraine_testkit::maintcheck::{
    GRACE, Outcome, ROOT, Record, Row, TABLE, batch, check, read, record_write,
};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::{FaultRules, datagen};

/// Rewrites, compaction (its commit is a replace), checkpoints, log cleanup, vacuum and purges
/// (D66).
const POINTS: &[&str] = &[
    "moraine::rewrite::after_upload",
    "moraine::rewrite::before_commit",
    "moraine::rewrite::after_commit",
    "moraine::compact::after_read",
    "moraine::compact::after_upload",
    "moraine::compact::before_commit",
    "moraine::replace::after_upload",
    "moraine::replace::before_commit",
    "moraine::replace::after_commit",
    "moraine::checkpoint::before_write",
    "moraine::cleanup::before_delete",
    "moraine::vacuum::before_delete",
    "moraine::purge::after_marker",
    "moraine::purge::before_delete",
    "moraine::purge::after_delete",
    "moraine::purge::after_checkpoint",
    "moraine::purge::after_scrub",
];

/// Queued writes: the submitter's put and the ingestor's commit.
const QUEUED_POINTS: &[&str] = &[
    "moraine::submit::before_put",
    "moraine::ingest::before_commit",
    "moraine::ingest::after_commit",
];

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn open(
    env: &SimEnv,
    c: usize,
    life: u64,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    let clock = moraine_testkit::ClientClock::new(
        env.clock.clone(),
        Some(env.sched.clone()),
        c,
        moraine::testing::splitmix64(
            env.seed ^ moraine::testing::splitmix64((c as u64) << 32 | life),
        ),
    );
    Database::builder_for_store(env.chaos_store(c, FaultRules::default()), ROOT)
        .clock(Arc::new(clock))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .max_query_duration(Duration::from_secs(60))
        .small_table_bytes(0)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

fn maintain_options() -> MaintainOptions {
    MaintainOptions::default()
        .leases(false)
        .compaction(Some(
            CompactionPolicy::default()
                .fanout(2)
                .max_files(2)
                .min_file_age(Duration::ZERO),
        ))
        .checkpoint_interval(Some(2))
}

fn purge_options(sim: &moraine_testkit::SimClock) -> moraine::Purge {
    let grace = chrono::Duration::from_std(GRACE).unwrap_or_default();
    moraine::Purge::new()
        .removed_before(sim.server_time() - grace - chrono::Duration::seconds(60))
        .erase()
}

type Life = Pin<Box<dyn Future<Output = moraine::Result<Database>> + Send>>;

/// Opens client `c`'s next life, if any is left.
async fn reopen(lives: &mut impl Iterator<Item = Life>) -> Option<Table> {
    let db = lives.next()?.await.ok()?;
    db.open_table(TABLE).await.ok()
}

/// Seeds in which the failpoint fired (it need not fire in every one).
static FIRED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

async fn scenario(env: SimEnv, point: &'static str) -> Result<(), String> {
    fail::cfg(point, "1*panic").map_err(|e| e.to_string())?;
    let rec: Arc<Mutex<Record>> = Arc::default();
    let crashes: Arc<Mutex<u64>> = Arc::default();
    let setup = open(&env, 0, 99);
    env.run(vec![Box::pin(async move {
        let db = setup.await.unwrap();
        db.create_table(
            TABLE,
            datagen::row_schema(),
            TableOptions::default()
                .primary_key(["k"])
                .cluster_by(["g"])
                .grace_period(GRACE),
        )
        .await
        .unwrap();
    }) as ClientFuture])
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..3usize {
        let (rec, crashes) = (rec.clone(), crashes.clone());
        let mut lives = (0..4)
            .map(|life| Box::pin(open(&env, c, life)) as Life)
            .collect::<Vec<_>>()
            .into_iter();
        let clock = env.client_clock(c);
        let sim = env.clock.clone();
        let key = format!("c{c}");
        futs.push(Box::pin(async move {
            use moraine::clock::Clock;
            let mut table: Option<Table> = None;
            for j in 0..16usize {
                if table.is_none() {
                    table = reopen(&mut lives).await;
                }
                let Some(t) = table.clone() else { return };
                let op = format!("c{c}o{j}");
                let key = format!("{key}k{}", j % 3);
                let delete = j % 8 == 3;
                let step = AssertUnwindSafe(async {
                    match j % 4 {
                        0 | 1 => {
                            let row = Row {
                                k: key.clone(),
                                op: op.clone(),
                                j: j as i64,
                                g: Some((j % 3) as i64),
                            };
                            let r = t.upsert(vec![batch(&[row])]).await;
                            record_write(&rec, &op, &key, j, false, r);
                        }
                        3 if delete => {
                            let r = t
                                .delete_keys(&[vec![ScalarValue::Utf8(Some(key.clone()))]])
                                .await;
                            record_write(&rec, &op, &key, j, true, r);
                        }
                        2 if j % 8 == 6 => {
                            // A purge with erasure that collects what vacuum would: removals
                            // older than the grace period (the checks hold readers to it).
                            let _ = t.purge(&purge_options(&sim)).await;
                        }
                        2 => {
                            let _ = t.maintain(&maintain_options()).await;
                        }
                        _ => clock.sleep(Duration::from_secs(700)).await,
                    }
                })
                .catch_unwind()
                .await;
                if step.is_err() {
                    // The process died here. A write that crashed after its commit may have
                    // committed; before it, it must be invisible.
                    *crashes.lock().unwrap() += 1;
                    if let Some(w) = rec.lock().unwrap().writes.get_mut(&op) {
                        w.outcome = Outcome::Unknown;
                    }
                    let wrote = j % 4 < 2 || delete;
                    if !rec.lock().unwrap().writes.contains_key(&op) && wrote {
                        let o = if point.ends_with("after_commit") {
                            Outcome::Unknown
                        } else {
                            Outcome::Failed
                        };
                        rec.lock().unwrap().writes.insert(
                            op.clone(),
                            moraine_testkit::maintcheck::Write {
                                key: key.clone(),
                                j: j as i64,
                                delete,
                                outcome: o,
                            },
                        );
                    }
                    table = None;
                    continue;
                }
                read(&t, &format!("c{c}"), &rec).await;
            }
        }));
    }
    let r = env.run(futs).await;
    fail::remove(point);
    r.map_err(|e| format!("{e:?}"))?;
    if *crashes.lock().unwrap() > 0 {
        FIRED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let fresh = open(&env, 9, 0);
    let clock = env.client_clock(9);
    let sim2 = env.clock.clone();
    let r2 = rec.clone();
    let out: Arc<Mutex<Option<(u64, bool)>>> = Arc::default();
    let o2 = out.clone();
    env.run(vec![Box::pin(async move {
        use moraine::clock::Clock;
        let t = fresh.await.unwrap().open_table(TABLE).await.unwrap();
        let full = maintain_options()
            .compaction(Some(CompactionPolicy::full()))
            .checkpoint_interval(Some(1));
        t.maintain(&full).await.expect("final maintenance");
        for _ in 0..2 {
            clock.sleep(GRACE + Duration::from_secs(60)).await;
            t.maintain(&full.clone().compaction(None))
                .await
                .expect("final maintenance");
        }
        t.purge(&purge_options(&sim2)).await.expect("final purge");
        read(&t, "final", &r2).await;
        let ok = t.verify().await.expect("verify").is_ok();
        *o2.lock().unwrap() = Some((t.version(), ok));
    }) as ClientFuture])
        .await
        .map_err(|e| format!("final phase: {e:?}"))?;
    let (v, ok) = out.lock().unwrap().take().ok_or("no final state")?;
    if !ok {
        return Err("verify failed after the final phase".into());
    }
    let rec = std::mem::take(&mut *rec.lock().unwrap());
    check(&env, &rec, v).map(|_| ())
}

/// Queued writes on a keyless table: every acknowledged submission lands exactly once, a
/// crashed one at most once, whoever ingests and wherever it crashes.
async fn queued(env: SimEnv, point: &'static str) -> Result<(), String> {
    fail::cfg(point, "1*panic").map_err(|e| e.to_string())?;
    let acked: Arc<Mutex<BTreeSet<String>>> = Arc::default();
    let crashes: Arc<Mutex<u64>> = Arc::default();
    let setup = open(&env, 0, 99);
    env.run(vec![Box::pin(async move {
        let db = setup.await.unwrap();
        db.create_table(TABLE, datagen::row_schema(), TableOptions::default())
            .await
            .unwrap();
    }) as ClientFuture])
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..3usize {
        let (acked, crashes) = (acked.clone(), crashes.clone());
        let mut lives = (0..4)
            .map(|life| Box::pin(open(&env, c, life)) as Life)
            .collect::<Vec<_>>()
            .into_iter();
        futs.push(Box::pin(async move {
            let mut table: Option<Table> = None;
            for j in 0..8usize {
                if table.is_none() {
                    table = reopen(&mut lives).await;
                }
                let Some(t) = table.clone() else { return };
                let op = format!("c{c}o{j}");
                let step = AssertUnwindSafe(async {
                    if j % 3 == 2 {
                        let _ = t.ingest(j % 2 == 0).await;
                    } else {
                        let row = Row {
                            k: op.clone(),
                            op: op.clone(),
                            j: j as i64,
                            g: None,
                        };
                        if t.submit(vec![batch(&[row])]).await.is_ok() {
                            acked.lock().unwrap().insert(op.clone());
                        }
                    }
                })
                .catch_unwind()
                .await;
                if step.is_err() {
                    *crashes.lock().unwrap() += 1;
                    table = None;
                }
            }
        }));
    }
    let r = env.run(futs).await;
    fail::remove(point);
    r.map_err(|e| format!("{e:?}"))?;
    if *crashes.lock().unwrap() > 0 {
        FIRED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let fresh = open(&env, 9, 0);
    let rows: Arc<Mutex<Vec<String>>> = Arc::default();
    let r2 = rows.clone();
    env.run(vec![Box::pin(async move {
        let t = fresh.await.unwrap().open_table(TABLE).await.unwrap();
        for _ in 0..3 {
            t.ingest(false).await.expect("final ingest");
        }
        let b = t.scan().collect().await.unwrap();
        let mut out = Vec::new();
        for b in &b {
            let k = b.column_by_name("k").unwrap().as_string::<i32>();
            for i in 0..b.num_rows() {
                out.push(k.value(i).to_string());
            }
        }
        *r2.lock().unwrap() = out;
    }) as ClientFuture])
        .await
        .map_err(|e| format!("final phase: {e:?}"))?;
    let rows = rows.lock().unwrap().clone();
    let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
    for r in &rows {
        *seen.entry(r.as_str()).or_default() += 1;
    }
    if let Some((r, n)) = seen.iter().find(|(_, n)| **n > 1) {
        return Err(format!("queued row {r} landed {n} times"));
    }
    for a in acked.lock().unwrap().iter() {
        if !seen.contains_key(a.as_str()) {
            return Err(format!("acknowledged submission {a} never landed"));
        }
    }
    Ok(())
}

#[test]
fn crash_everywhere() {
    let _scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| {}));
    let seeds = seeds_from_env(30);
    let report = |point: &str| {
        let fired = FIRED.swap(0, std::sync::atomic::Ordering::Relaxed);
        println!(
            "RESULT crash_maintenance {point}: {} seeds clean, crashed in {fired}",
            seeds.end - seeds.start
        );
        if fired == 0 {
            let _ = std::panic::take_hook();
            panic!("{point} never fired");
        }
    };
    for &point in POINTS {
        if let Err(f) = run_seeds(seeds.clone(), |env| scenario(env, point)) {
            let _ = std::panic::take_hook();
            panic!("{point}: {f}");
        }
        report(point);
    }
    for &point in QUEUED_POINTS {
        if let Err(f) = run_seeds(seeds.clone(), |env| queued(env, point)) {
            let _ = std::panic::take_hook();
            panic!("{point}: {f}");
        }
        report(point);
    }
    let _ = std::panic::take_hook();
}
