//! Uploads of writes that do not commit, in the simulation (D65). Clients share a keyed table
//! and upsert, delete and append over a small key space, replace random files (which loses to
//! other writers' rewrites), and compact under a short lease (so commits are fenced), with
//! random faults on some seeds and simulated minutes passing between operations. Nothing runs
//! vacuum, so every object a write uploaded is still in the store unless the write deleted it.
//!
//! Checked from the store's full history, without moraine:
//!
//! - nothing a commit references is ever deleted: every file a log entry adds is in the store;
//! - no upload of a write that definitely failed survives: a data file no log entry adds is
//!   left only when some write's outcome was unknown, a delete failed (both counted in
//!   [`moraine::WriteMetrics::uploads_left`]) or a compaction step failed before its commit,
//!   and never on a seed without faults.
//!
//! More seeds: `MORAINE_SIM_SEEDS=10000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moraine::expr::ScalarValue;
use moraine::store::RetryPolicy;
use moraine::{
    CompactionPolicy, Consistency, Database, Error, MaintainOptions, Replace, Table, TableOptions,
};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::read_log_all;
use moraine_testkit::{FaultRules, datagen};

const ROOT: &str = "db";

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    ops: u64,
    failed: u64,
    unknown: u64,
    deleted: u64,
    left: u64,
    orphans: u64,
    fenced: u64,
    files_changed: u64,
    yielded: u64,
}

#[derive(Default)]
struct Seen {
    failed: u64,
    unknown: u64,
    fenced: u64,
    files_changed: u64,
    yielded: u64,
    /// Compaction steps that failed before their commit (their merged files are not a
    /// commit's uploads).
    compaction_errors: u64,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn options() -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
        .cluster_by(["g"])
        .grace_period(Duration::from_secs(600))
}

fn note(seen: &Mutex<Seen>, e: &Error) {
    let mut s = seen.lock().unwrap();
    match e {
        Error::OutcomeUnknown { .. } => s.unknown += 1,
        Error::Fenced { .. } => s.fenced += 1,
        Error::FilesChanged(_) => s.files_changed += 1,
        _ => s.failed += 1,
    }
}

async fn scenario(mut env: SimEnv, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let clients = env.rng.range(2, 4) as usize;
    let ops = env.rng.range(3, 9) as usize;
    let faults = env.rng.chance(0.5);
    let rules = if faults {
        FaultRules::random(&mut env.rng)
    } else {
        FaultRules::default()
    };
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let handles: Arc<Mutex<Vec<Table>>> = Arc::default();
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let mut rng = env.rng.fork(c as u64 + 1);
        let db = Database::builder_for_store(env.chaos_store(c, rules.clone()), ROOT)
            .clock(env.client_clock(c))
            .io_concurrency(1)
            .consistency(Consistency::Strong)
            .request_retry(RetryPolicy::REQUEST)
            .open();
        let clock = env.client_clock(c);
        let (seen, handles) = (seen.clone(), handles.clone());
        futs.push(Box::pin(async move {
            use moraine::clock::Clock;
            let Ok(db) = db.await else {
                return;
            };
            let mut table = None;
            for _ in 0..10 {
                if let Ok(t) = db.create_table("t", datagen::row_schema(), options()).await {
                    table = Some(t);
                    break;
                }
            }
            let Some(t) = table else {
                return;
            };
            handles.lock().unwrap().push(t.clone());
            for j in 0..ops {
                let op = format!("c{c}o{j}");
                let r: Result<(), Error> = match rng.below(6) {
                    0 => t
                        .append(vec![datagen::rows(&mut rng, &op, 3, None, 2)])
                        .await
                        .map(|_| ()),
                    1 | 2 => t
                        .upsert(vec![datagen::rows(&mut rng, &op, 2, Some(6), 2)])
                        .await
                        .map(|_| ()),
                    3 => {
                        let keys: Vec<Vec<ScalarValue>> = (0..2)
                            .map(|_| {
                                let b = datagen::rows(&mut rng, &op, 1, Some(6), 2);
                                let k = b
                                    .column_by_name("k")
                                    .and_then(|a| ScalarValue::try_from_array(a, 0).ok())
                                    .unwrap_or(ScalarValue::Null);
                                vec![k]
                            })
                            .collect();
                        t.delete_keys(&keys).await.map(|_| ())
                    }
                    4 => {
                        // A fold of one or two files into one, planned on the version in hand.
                        let snap = t.snapshot_in_hand();
                        let files: Vec<String> = snap.files().into_iter().map(|f| f.path).collect();
                        if files.is_empty() {
                            Ok(())
                        } else {
                            let n = (rng.below(2) as usize + 1).min(files.len());
                            let start = rng.below((files.len() - n + 1) as u64) as usize;
                            let chosen = files[start..start + n].to_vec();
                            match snap.scan().files(chosen.clone()).collect().await {
                                Ok(rows) => t
                                    .replace_files(
                                        Replace::new()
                                            .remove_all(chosen)
                                            .add_rows(rows)
                                            .rows_unchanged(),
                                    )
                                    .await
                                    .map(|_| ()),
                                Err(e) => Err(e),
                            }
                        }
                    }
                    _ => {
                        let o = MaintainOptions::default()
                            .build_indexes(None)
                            .ingest(false)
                            .checkpoint(false)
                            .vacuum(false)
                            .lease_ttl(Duration::from_secs(20))
                            .compaction(Some(
                                CompactionPolicy::full().min_file_age(Duration::ZERO),
                            ));
                        match t.maintain(&o).await {
                            Ok(rep) => {
                                if let Some(c) = rep.compaction {
                                    seen.lock().unwrap().yielded += c.yielded as u64;
                                }
                                Ok(())
                            }
                            Err(e) => {
                                seen.lock().unwrap().compaction_errors += 1;
                                Err(e)
                            }
                        }
                    }
                };
                if let Err(e) = &r {
                    note(&seen, e);
                }
                if !faults
                    && let Err(e) = &r
                    && !matches!(
                        e,
                        Error::FilesChanged(_)
                            | Error::DuplicateKeyInBatch(_)
                            | Error::Fenced { .. }
                            | Error::TooManyConflicts { .. }
                            | Error::CommitDeadlineExceeded { .. }
                            | Error::SnapshotExpired
                    )
                {
                    panic!("{op} failed without faults: {e:?}");
                }
                clock.sleep(Duration::from_millis(rng.below(30_000))).await;
            }
        }));
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;

    let root = format!("{ROOT}/t/data");
    let added: BTreeSet<String> = read_log_all(&env.store, &root)
        .into_iter()
        .flat_map(|e| e.adds)
        .collect();
    let stored: BTreeSet<String> = env
        .store
        .snapshot()
        .into_keys()
        .filter_map(|k| k.strip_prefix(&format!("{root}/")).map(str::to_string))
        .filter(|k| !k.starts_with('_') && k.ends_with(".parquet"))
        .collect();
    let missing: Vec<&String> = added.difference(&stored).collect();
    if !missing.is_empty() {
        return Err(format!("committed files deleted: {missing:?}"));
    }
    let orphans: Vec<&String> = stored.difference(&added).collect();
    let (deleted, left) = handles.lock().unwrap().iter().fold((0, 0), |(d, l), t| {
        let m = t.write_metrics();
        (
            d + m.uploads_deleted.load(Ordering::Relaxed),
            l + m.uploads_left.load(Ordering::Relaxed),
        )
    });
    let s = seen.lock().unwrap();
    if !orphans.is_empty() {
        if !faults {
            return Err(format!("uploads left without faults: {orphans:?}"));
        }
        if left == 0 && s.compaction_errors == 0 {
            return Err(format!(
                "uploads left although every write settled and every delete succeeded: {orphans:?}"
            ));
        }
        if s.compaction_errors == 0 && orphans.len() as u64 > left {
            return Err(format!(
                "{} uploads left, {left} counted: {orphans:?}",
                orphans.len()
            ));
        }
    }
    let mut tot = totals.lock().unwrap();
    tot.seeds += 1;
    tot.ops += (clients * ops) as u64;
    tot.failed += s.failed;
    tot.unknown += s.unknown;
    tot.fenced += s.fenced;
    tot.files_changed += s.files_changed;
    tot.yielded += s.yielded;
    tot.deleted += deleted;
    tot.left += left;
    tot.orphans += orphans.len() as u64;
    Ok(())
}

#[test]
fn failed_uploads() {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let seeds = seeds_from_env(200);
    let t = totals.clone();
    if let Err(f) = run_seeds(seeds.clone(), move |env| scenario(env, t.clone())) {
        panic!("{f}");
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT failed_uploads_sim seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    assert!(t.deleted > 0, "no write deleted an upload");
}
