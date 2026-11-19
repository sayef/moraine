//! Maintenance acceptance through the simulation: maintenance racing writes, readers and
//! itself, with faults and with simulated time passing the grace period.
//!
//! Named scenarios (docs/design/test-ids.md): 3 `delete_never_resurrects`, 6 `reader_survives_vacuum`,
//! 12 `maintenance_everywhere`, 13 `collector_never_deletes_live` (checked in every mode),
//! 14 `collector_eventually_cleans` (every mode ends with it), 18 `partial_listing`.
//! `maintenance_random` mixes everything with random faults and leases on or off.
//!
//! Each client owns a few keys of a keyed table and upserts and deletes them; maintenance
//! passes (index build, compaction, checkpoints and log cleanup, vacuum and sidecar
//! collection) run between writes, clients sleep for minutes of simulated time, and some pin
//! and unpin versions. The checks read the store's full history, deleted objects included,
//! without moraine:
//!
//! - every version holds each key at most once (I4), and a compaction changes no row (I10);
//! - a key changes only by its owner's operations, in their order: an acknowledged upsert is
//!   at its version, an acknowledged delete leaves the key absent at its version and the key
//!   never comes back with an older value (scenario 3), a failed write never shows;
//! - every read equals the rows stored at the version it reports (I3), pinned reads included;
//! - no collector deleted a data file, sidecar or log entry that a snapshot younger than the
//!   grace period, or a pin, could still need (scenario 13);
//! - after writes stop and two passes beyond the grace period, `data/` holds exactly the live
//!   files and `idx/` only their sidecars (scenario 14), and `verify` finds nothing wrong.
//!
//! More seeds: `MORAINE_SIM_SEEDS=10000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moraine::expr::ScalarValue;
use moraine::store::RetryPolicy;
use moraine::{
    CompactionPolicy, Consistency, Database, Error, MaintainOptions, Table, TableOptions,
};
use moraine_testkit::maintcheck::{
    Done, GRACE, Outcome, ROOT, Record, Row, TABLE, batch, check, pass, pin_or_unpin, read,
    record_write, to_rows,
};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::{FaultRules, SimRng, datagen};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Everything, random faults, leases on or off per pass.
    Random,
    /// Scenario 12: every client runs every job after every write, leases off.
    Everywhere,
    /// Scenario 3: deletes and upserts of other keys race compaction; no faults.
    Resurrect,
    /// Scenario 18: listings are cut short often.
    PartialListing,
    /// Scenario 6: a reader holds a snapshot past the grace period while others vacuum.
    Reader,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn options() -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .cluster_by(["g"])
        .page_row_limit(2)
        .grace_period(GRACE)
}

fn open_db(
    env: &SimEnv,
    client: usize,
    rules: &FaultRules,
    consistency: Consistency,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    Database::builder_for_store(env.chaos_store(client, rules.clone()), ROOT)
        .clock(env.client_clock(client))
        .io_concurrency(1)
        .consistency(consistency)
        .max_query_duration(Duration::from_secs(60))
        .small_table_bytes(0)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

fn maintain_options(rng: &mut SimRng, mode: Mode) -> MaintainOptions {
    MaintainOptions::default()
        .leases(match mode {
            Mode::Everywhere => false,
            _ => rng.chance(0.6),
        })
        .lease_ttl(Duration::from_secs(rng.range(30, 300)))
        .build_indexes(rng.chance(0.5).then(Default::default))
        .compaction(Some(
            CompactionPolicy::default()
                .fanout(2)
                .max_files(3)
                .min_file_age(Duration::from_secs(rng.range(0, 30))),
        ))
        .checkpoint_interval(Some(rng.range(1, 5)))
}

async fn scenario(mut env: SimEnv, mode: Mode, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let clients = env.rng.range(2, 5) as usize;
    let ops = env.rng.range(4, 12) as usize;
    let rules = match mode {
        Mode::Random | Mode::Reader => FaultRules::random(&mut env.rng),
        Mode::PartialListing => FaultRules {
            cut_listing: 0.5,
            ..Default::default()
        },
        Mode::Everywhere | Mode::Resurrect => FaultRules::default(),
    };
    let rec: Arc<Mutex<Record>> = Arc::default();
    // Writers that finished (scenario 6).
    let done: Arc<std::sync::atomic::AtomicUsize> = Arc::default();

    let setup = open_db(&env, 0, &FaultRules::default(), Consistency::Strong);
    env.run(vec![Box::pin(async move {
        let db = setup.await.expect("open");
        db.create_table(TABLE, datagen::row_schema(), options())
            .await
            .expect("create");
    }) as ClientFuture])
        .await
        .map_err(|e| format!("setup: {e:?}"))?;

    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let mut rng = env.rng.fork(c as u64 + 1);
        let reader = mode == Mode::Reader && c == 0;
        let consistency = if reader {
            Consistency::Manual
        } else {
            match rng.below(3) {
                0 => Consistency::Strong,
                1 => Consistency::Manual,
                _ => Consistency::Eventual {
                    max_staleness: Duration::from_millis(rng.range(0, 200)),
                },
            }
        };
        let db = open_db(&env, c, &rules, consistency);
        let clock = env.client_clock(c);
        let rec = rec.clone();
        let done = done.clone();
        let keys: Vec<String> = (0..rng.range(1, 4))
            .map(|n| format!("{}#c{c}k{n}", datagen::hostile_string(&mut rng)))
            .collect();
        futs.push(Box::pin(async move {
            use moraine::clock::Clock;
            // Counts this writer as finished however it ends (scenario 6's reader waits).
            let _done = (!reader).then(|| Done(done.clone()));
            let Ok(db) = db.await else { return };
            let mut table = None;
            for _ in 0..10 {
                if let Ok(t) = db.open_table(TABLE).await {
                    table = Some(t);
                    break;
                }
            }
            let Some(t) = table else { return };
            if reader {
                read_past_grace(&t, &clock, &rec, &done, clients - 1).await;
                return;
            }
            for j in 0..ops {
                let op = format!("c{c}o{j}");
                let key = keys[rng.below(keys.len() as u64) as usize].clone();
                // Scenario 3 deletes more often, and runs maintenance between the deletes and
                // upserts, so compaction races both.
                let (up, del, maint) = match mode {
                    Mode::Resurrect => (35, 65, 90),
                    _ => (40, 60, 80),
                };
                let roll = rng.below(100);
                if roll < up {
                    let row = Row {
                        k: key.clone(),
                        op: op.clone(),
                        j: j as i64,
                        g: (!rng.chance(0.2)).then(|| rng.below(3) as i64),
                    };
                    let r = t.upsert(vec![batch(&[row])]).await;
                    record_write(&rec, &op, &key, j, false, r);
                } else if roll < del {
                    let r = t
                        .delete_keys(&[vec![ScalarValue::Utf8(Some(key.clone()))]])
                        .await;
                    record_write(&rec, &op, &key, j, true, r);
                } else if roll < maint {
                    let o = maintain_options(&mut rng, mode);
                    pass(&t, &o, &rec).await;
                } else if roll < maint + 12 {
                    clock.sleep(Duration::from_secs(rng.range(10, 400))).await;
                } else {
                    pin_or_unpin(&t, c, &mut rng, &rec).await;
                }
                if mode == Mode::Everywhere {
                    let o = maintain_options(&mut rng, mode);
                    pass(&t, &o, &rec).await;
                }
                read(&t, &format!("c{c}"), &rec).await;
            }
            if mode == Mode::Reader {
                // Every key rewritten, so every file the reader's snapshot lists is replaced;
                // then past the grace period, vacuum, while the reader still sleeps.
                for (n, key) in keys.iter().enumerate() {
                    let row = Row {
                        k: key.clone(),
                        op: format!("c{c}r{n}"),
                        j: (ops + n) as i64,
                        g: None,
                    };
                    let r = t.upsert(vec![batch(&[row])]).await;
                    record_write(&rec, &format!("c{c}r{n}"), key, ops + n, false, r);
                }
                clock.sleep(GRACE + Duration::from_secs(120)).await;
                let o = MaintainOptions::default().leases(false);
                pass(&t, &o, &rec).await;
            }
        }));
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;

    // Scenario 14: writes stop; pins go; two passes beyond the grace period.
    let fresh = open_db(&env, 99, &FaultRules::default(), Consistency::Strong);
    let clock = env.client_clock(99);
    let r2 = rec.clone();
    let final_state: Arc<Mutex<Option<(u64, moraine::VerifyReport)>>> = Arc::default();
    let fs = final_state.clone();
    let ran = env
        .run(vec![Box::pin(async move {
            use moraine::clock::Clock;
            let db = fresh.await.expect("open");
            let t = db.open_table(TABLE).await.expect("open table");
            t.refresh().await.expect("refresh");
            for name in t.pins().expect("pins").into_keys() {
                t.unpin(&name).await.expect("unpin");
            }
            let full = MaintainOptions::default()
                .leases(false)
                .compaction(Some(CompactionPolicy::full()))
                .checkpoint_interval(Some(1));
            t.maintain(&full).await.expect("final maintenance");
            let quiet = full.clone().compaction(None);
            for _ in 0..2 {
                clock.sleep(GRACE + Duration::from_secs(60)).await;
                t.maintain(&quiet).await.expect("final maintenance");
            }
            read(&t, "final", &r2).await;
            let v = t.verify().await.expect("verify");
            *fs.lock().unwrap() = Some((t.version(), v));
        }) as ClientFuture])
        .await;
    if let Err(e) = ran {
        // The history usually says why: check it first, for the more precise message.
        let rec = std::mem::take(&mut *rec.lock().unwrap());
        let last = moraine_testkit::table_log::read_log_all(&env.store, "db/t/data")
            .last()
            .map_or(0, |e| e.version);
        check(&env, &rec, last)?;
        return Err(format!("final phase did not finish: {e:?}"));
    }
    let (final_version, verify) = final_state
        .lock()
        .unwrap()
        .take()
        .ok_or("the final phase did not report")?;
    if !verify.is_ok() {
        return Err(format!("verify after the final phase: {verify:?}"));
    }
    let rec = std::mem::take(&mut *rec.lock().unwrap());
    let stats = check(&env, &rec, final_version)?;
    let mut t = totals.lock().unwrap();
    t.seeds += 1;
    t.versions += stats.versions;
    t.compactions += stats.compactions;
    t.deleted_files += stats.deleted_files;
    t.deleted_sidecars += stats.deleted_sidecars;
    t.deleted_entries += stats.deleted_entries;
    t.checkpoints += stats.checkpoints;
    t.reads += rec.reads.len() as u64;
    t.passes += rec.maintenance_passes;
    t.pass_errors += rec.maintenance_errors;
    t.expired += rec.expired;
    t.restarted += rec.restarted_reads;
    t.pins += rec.pins;
    t.faults += env.faults.total();
    for w in rec.writes.values() {
        match w.outcome {
            Outcome::Acked(_) => t.acked += 1,
            Outcome::Failed => t.failed += 1,
            Outcome::Unknown => t.unknown += 1,
        }
    }
    Ok(())
}

/// Scenario 6: a reader takes a snapshot, sleeps past the grace period while the others write
/// and vacuum, then reads through the handle (which restarts once at the newest version) and
/// through the held snapshot (its rows, or `SnapshotExpired`).
async fn read_past_grace(
    t: &Table,
    clock: &Arc<moraine_testkit::ClientClock>,
    rec: &Mutex<Record>,
    done: &std::sync::atomic::AtomicUsize,
    writers: usize,
) {
    use moraine::clock::Clock;
    // Timers may fire before other clients run: wait for some commits first.
    for _ in 0..200 {
        if t.refresh().await.is_ok_and(|v| v >= 3) {
            break;
        }
        clock.sleep(Duration::from_secs(1)).await;
    }
    let held = t.snapshot_in_hand();
    // Until every writer has rewritten its keys and vacuumed past the grace period.
    while done.load(std::sync::atomic::Ordering::SeqCst) < writers {
        clock.sleep(Duration::from_secs(60)).await;
    }
    let before = t.version();
    if let Ok(b) = t.scan().collect().await {
        let mut g = rec.lock().unwrap();
        g.reads.push(("reader".into(), t.version(), to_rows(&b)));
        if t.version() > before {
            g.restarted_reads += 1;
        }
    }
    match held.scan().collect().await {
        Ok(b) => rec
            .lock()
            .unwrap()
            .reads
            .push(("held".into(), held.version(), to_rows(&b))),
        Err(Error::SnapshotExpired) => rec.lock().unwrap().expired += 1,
        Err(_) => {}
    }
}

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    versions: u64,
    compactions: u64,
    checkpoints: u64,
    deleted_files: u64,
    deleted_sidecars: u64,
    deleted_entries: u64,
    reads: u64,
    passes: u64,
    pass_errors: u64,
    expired: u64,
    restarted: u64,
    pins: u64,
    acked: u64,
    failed: u64,
    unknown: u64,
    faults: u64,
}

fn run(mode: Mode, seeds: std::ops::Range<u64>) -> Totals {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds.clone(), move |env| scenario(env, mode, t.clone())) {
        panic!("{f}");
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT maintenance_sim {mode:?} seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    t
}

/// Everything at once with random faults (T3 and T4 for maintenance).
#[test]
fn maintenance_random() {
    let t = run(Mode::Random, seeds_from_env(200));
    assert!(t.compactions > 0 && t.deleted_files > 0 && t.checkpoints > 0);
}

/// Scenario 12: every client runs every maintenance job after every write, leases off.
#[test]
fn maintenance_everywhere() {
    let t = run(Mode::Everywhere, seeds_from_env(200));
    assert!(t.compactions > 0 && t.deleted_entries > 0);
}

/// Scenario 3: deletes race compaction and upserts of other keys; a deleted key never comes
/// back.
#[test]
fn delete_never_resurrects() {
    let t = run(Mode::Resurrect, seeds_from_env(200));
    assert!(t.compactions > 0 && t.acked > 0 && t.passes > 0);
}

/// Scenario 18: listings are cut short at random; collectors delete nothing they should not.
#[test]
fn partial_listing() {
    let t = run(Mode::PartialListing, seeds_from_env(200));
    assert!(t.faults > 0 && t.deleted_files > 0);
}

/// Scenario 6: a reader on a snapshot older than the grace period restarts once and succeeds.
#[test]
fn reader_survives_vacuum() {
    let t = run(Mode::Reader, seeds_from_env(200));
    assert!(t.restarted > 0 && t.expired > 0, "{t:?}");
}

/// Queued writes (D51) from several submitters with random faults, ingested by several clients
/// with leases on or off: every acknowledged submission lands exactly once, one whose outcome
/// was unknown at most once, and the inbox ends empty.
async fn queued(mut env: SimEnv, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let rules = FaultRules::random(&mut env.rng);
    let clients = env.rng.range(2, 5) as usize;
    let setup = open_db(&env, 0, &FaultRules::default(), Consistency::Strong);
    env.run(vec![Box::pin(async move {
        let db = setup.await.expect("open");
        db.create_table(TABLE, datagen::row_schema(), TableOptions::default())
            .await
            .expect("create");
    }) as ClientFuture])
        .await
        .map_err(|e| format!("setup: {e:?}"))?;
    let acked: Arc<Mutex<Vec<String>>> = Arc::default();
    let unknown: Arc<Mutex<u64>> = Arc::default();
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let mut rng = env.rng.fork(c as u64 + 1);
        let db = open_db(&env, c, &rules, Consistency::Strong);
        let (acked, unknown) = (acked.clone(), unknown.clone());
        futs.push(Box::pin(async move {
            let Ok(db) = db.await else { return };
            let Ok(t) = db.open_table(TABLE).await else {
                return;
            };
            for j in 0..rng.range(3, 10) {
                if rng.chance(0.3) {
                    let _ = t.ingest(rng.chance(0.5)).await;
                    continue;
                }
                let op = format!("c{c}o{j}");
                let rows: Vec<Row> = (0..rng.range(1, 4))
                    .map(|i| Row {
                        k: format!("{op}r{i}"),
                        op: op.clone(),
                        j: i as i64,
                        g: None,
                    })
                    .collect();
                match t.submit(vec![batch(&rows)]).await {
                    Ok(_) => acked.lock().unwrap().extend(rows.into_iter().map(|r| r.k)),
                    Err(Error::OutcomeUnknown { .. }) => *unknown.lock().unwrap() += 1,
                    Err(_) => {}
                }
            }
        }));
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;
    let fresh = open_db(&env, 99, &FaultRules::default(), Consistency::Strong);
    // The final rows, and the inbox objects left.
    type Final = Option<(Vec<Row>, usize)>;
    let got: Arc<Mutex<Final>> = Arc::default();
    let g2 = got.clone();
    let store = env.store.clone();
    env.run(vec![Box::pin(async move {
        let t = fresh
            .await
            .expect("open")
            .open_table(TABLE)
            .await
            .expect("table");
        for _ in 0..2 {
            t.ingest(false).await.expect("final ingest");
        }
        let rows = to_rows(&t.scan().collect().await.expect("scan"));
        let left = store
            .snapshot()
            .keys()
            .filter(|k| k.starts_with("db/t/meta/inbox/"))
            .count();
        *g2.lock().unwrap() = Some((rows, left));
    }) as ClientFuture])
        .await
        .map_err(|e| format!("final ingest: {e:?}"))?;
    let (rows, left) = got.lock().unwrap().take().ok_or("no final state")?;
    if left > 0 {
        return Err(format!("{left} inbox objects left after the final ingest"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for r in &rows {
        if !seen.insert(r.k.clone()) {
            return Err(format!("queued row {} landed twice", r.k));
        }
    }
    let acked = acked.lock().unwrap().clone();
    for k in &acked {
        if !seen.contains(k) {
            return Err(format!("acknowledged queued row {k} is missing"));
        }
    }
    let mut t = totals.lock().unwrap();
    t.seeds += 1;
    t.acked += acked.len() as u64;
    t.unknown += *unknown.lock().unwrap();
    t.reads += rows.len() as u64;
    t.faults += env.faults.total();
    Ok(())
}

/// Queued writes land exactly once under faults (an acceptance check of maintenance).
#[test]
fn queued_writes_exactly_once() {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    let seeds = seeds_from_env(200);
    if let Err(f) = run_seeds(seeds.clone(), move |env| queued(env, t.clone())) {
        panic!("{f}");
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT maintenance_sim Queued seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    assert!(t.acked > 0 && t.faults > 0);
}
