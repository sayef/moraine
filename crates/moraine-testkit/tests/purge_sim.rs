//! Purge and erasure in the simulation (D66). Clients append, upsert and delete, pin and unpin
//! versions, read, run maintenance passes (compaction, checkpoints every two versions, log
//! cleanup and vacuum, leases on or off), and purge, with or without erasure and with a cutoff
//! now or a minute back, all at once, with random faults on some seeds and simulated minutes
//! passing between operations. Once they stop, one more client purges, waits under two
//! seconds, refreshes, deletes the rows of one group, and at once purges with erasure. On half the seeds the store keeps last-modified times in whole seconds, as S3
//! does, so a commit and the purge after it often share a timestamp (D93).
//!
//! Checked from the store's full history, without moraine:
//!
//! - no data file was deleted while the newest version committed by then, or a version a pin
//!   current then names, held it; no commit added a file deleted before it, and no pin
//!   committed for a version whose files were already deleted (a purge refuses older pins);
//! - after the last purge: no data file removed before it is left, no log entry in the store
//!   carries the minimum or maximum of such a file, no checkpoint is older than the purge, and
//!   no pin manifest of an older version is left unless its pin is current;
//! - a new handle reads the newest version as its files hold it, and every current pin.
//!
//! More seeds: `MORAINE_SIM_SEEDS=10000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moraine::expr::ScalarValue;
use moraine::store::RetryPolicy;
use moraine::{
    CompactionPolicy, Consistency, Database, Error, MaintainOptions, Purge, PurgeReport, Table,
    TableOptions,
};
use moraine_testkit::maintcheck::{Done, Row, to_rows};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::{CommittedEntry, read_log_all, read_parquet_archived};
use moraine_testkit::{FaultRules, datagen};
use serde_json::Value;

const ROOT: &str = "db";
const GRACE: Duration = Duration::from_secs(600);

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    purges: u64,
    purge_errors: u64,
    files_purged: u64,
    entries_scrubbed: u64,
    expired_reads: u64,
    pins_refused: u64,
}

#[derive(Default)]
struct Seen {
    purges: u64,
    purge_errors: u64,
    files_purged: u64,
    entries_scrubbed: u64,
    expired_reads: u64,
    pins_refused: u64,
    last: Option<PurgeReport>,
    newest: Option<(u64, Vec<Row>)>,
    pinned: Vec<(String, u64, Vec<Row>)>,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn options() -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
        .cluster_by(["g"])
        .grace_period(GRACE)
}

fn open(
    env: &SimEnv,
    c: usize,
    rules: &FaultRules,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    Database::builder_for_store(env.chaos_store(c, rules.clone()), ROOT)
        .clock(env.client_clock(c))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

async fn table(db: moraine::Result<Database>) -> Option<Table> {
    let db = db.ok()?;
    for _ in 0..10 {
        if let Ok(t) = db.create_table("t", datagen::row_schema(), options()).await {
            return Some(t);
        }
    }
    None
}

async fn client(
    t: Table,
    c: usize,
    ops: usize,
    mut rng: moraine_testkit::SimRng,
    clock: Arc<moraine_testkit::ClientClock>,
    sim: moraine_testkit::SimClock,
    seen: Arc<Mutex<Seen>>,
) {
    use moraine::clock::Clock;
    for j in 0..ops {
        let op = format!("c{c}o{j}");
        match rng.below(9) {
            0 | 1 => {
                let _ = t
                    .upsert(vec![datagen::rows(&mut rng, &op, 2, Some(8), 2)])
                    .await;
            }
            2 => {
                let b = datagen::rows(&mut rng, &op, 1, Some(8), 2);
                if let Some(k) = b
                    .column_by_name("k")
                    .and_then(|a| ScalarValue::try_from_array(a, 0).ok())
                {
                    let _ = t.delete_keys(&[vec![k]]).await;
                }
            }
            3 => {
                let _ = t
                    .append(vec![datagen::rows(&mut rng, &op, 2, None, 2)])
                    .await;
            }
            4 => {
                let name = format!("p{}", rng.below(2));
                if rng.chance(0.3) {
                    let _ = t.unpin(&name).await;
                } else if let Ok(v) = t.refresh().await {
                    // Sometimes an older version, which a purge since may make unavailable.
                    let v = v.saturating_sub(rng.below(3));
                    if let Err(Error::VersionNotAvailable(_)) = t.pin(&name, v).await {
                        seen.lock().unwrap().pins_refused += 1;
                    }
                }
            }
            5 => {
                if let Err(Error::SnapshotExpired) = t.scan().collect().await {
                    seen.lock().unwrap().expired_reads += 1;
                }
            }
            6 => {
                let o = MaintainOptions::default()
                    .build_indexes(None)
                    .ingest(false)
                    .leases(rng.chance(0.5))
                    .checkpoint_interval(Some(2))
                    .compaction(Some(CompactionPolicy::full().min_file_age(Duration::ZERO)));
                let _ = t.maintain(&o).await;
            }
            _ => {
                let mut p = Purge::new();
                if rng.chance(0.5) {
                    p = p.erase();
                }
                if rng.chance(0.5) {
                    let back = chrono::Duration::seconds(60);
                    p = p.removed_before(sim.server_time() - back);
                }
                let r = t.purge(&p).await;
                let mut s = seen.lock().unwrap();
                match r {
                    Ok(r) => {
                        s.purges += 1;
                        s.files_purged += r.files_deleted as u64;
                        s.entries_scrubbed += r.entries_scrubbed as u64;
                    }
                    Err(_) => s.purge_errors += 1,
                }
            }
        }
        clock.sleep(Duration::from_millis(rng.below(60_000))).await;
    }
}

/// The client that purges with erasure once the others are done, then reads.
async fn last(
    t: Table,
    seed: u64,
    done: Arc<AtomicUsize>,
    clients: usize,
    clock: Arc<moraine_testkit::ClientClock>,
    seen: Arc<Mutex<Seen>>,
) -> Result<(), String> {
    use moraine::clock::Clock;
    while done.load(Ordering::SeqCst) < clients {
        clock.sleep(Duration::from_secs(1)).await;
    }
    // A purge first, so the handle's server clock is a cached probe that lags by up to the
    // store's timestamp resolution, as it often is in a long-running process; then a pause
    // that ends anywhere within the next two seconds.
    let _ = t.purge(&Purge::new()).await;
    clock
        .sleep(Duration::from_millis(
            moraine::testing::splitmix64(seed ^ 0x1A57) % 2000,
        ))
        .await;
    // No pause before the purge: what the refresh loaded and the delete committed was
    // committed before it starts, whatever the store's timestamps say (D93).
    t.refresh()
        .await
        .map_err(|e| format!("the last refresh failed: {e:?}"))?;
    t.delete(moraine::expr::col("g").eq(moraine::expr::lit(0i64)))
        .await
        .map_err(|e| format!("the last delete failed: {e:?}"))?;
    // A purge whose lease another (crashed or slow) client still holds waits it out.
    let mut report = None;
    for _ in 0..20 {
        match t.purge(&Purge::new().erase()).await {
            Ok(r) => {
                report = Some(r);
                break;
            }
            Err(Error::LeaseHeld { .. }) => clock.sleep(Duration::from_secs(60)).await,
            Err(e) => return Err(format!("the last purge failed: {e:?}")),
        }
    }
    let report = report.ok_or("the purge lease never came free")?;
    let snap = t.snapshot().await.map_err(|e| e.to_string())?;
    let rows = to_rows(&snap.scan().collect().await.map_err(|e| e.to_string())?);
    let mut pinned = Vec::new();
    for (name, v) in t.pins().map_err(|e| e.to_string())? {
        let p = t.snapshot_at_pin(&name).await.map_err(|e| e.to_string())?;
        let r = to_rows(&p.scan().collect().await.map_err(|e| e.to_string())?);
        pinned.push((name, v, r));
    }
    let mut s = seen.lock().unwrap();
    s.last = Some(report);
    s.newest = Some((snap.version(), rows));
    s.pinned = pinned;
    Ok(())
}

/// Live files per version, replayed from the log.
fn live_sets(log: &[CommittedEntry]) -> Vec<BTreeSet<String>> {
    let mut live = BTreeSet::new();
    let mut out = Vec::with_capacity(log.len());
    for e in log {
        for r in &e.removes {
            live.remove(r);
        }
        live.extend(e.adds.iter().cloned());
        out.push(live.clone());
    }
    out
}

/// The pins per version, from the table properties.
fn pins_at(log: &[CommittedEntry]) -> Vec<BTreeMap<String, u64>> {
    let mut pins = BTreeMap::new();
    let mut out = Vec::with_capacity(log.len());
    for e in log {
        if let Some(c) = &e.configuration {
            pins = c
                .get("moraine.pins")
                .and_then(|v| serde_json::from_str(v).ok())
                .unwrap_or_default();
        }
        out.push(pins.clone());
    }
    out
}

fn rows_of(env: &SimEnv, root: &str, files: &BTreeSet<String>) -> Vec<Row> {
    let mut out = Vec::new();
    for f in files {
        out.extend(to_rows(&read_parquet_archived(
            &env.store,
            &format!("{root}/{f}"),
        )));
    }
    out.sort();
    out
}

fn check(env: &SimEnv, seen: &Seen) -> Result<(), String> {
    let root = format!("{ROOT}/t/data");
    let log = read_log_all(&env.store, &root);
    let live = live_sets(&log);
    let pins = pins_at(&log);
    // The version in force at server time `t`.
    let at = |t: chrono::DateTime<chrono::Utc>| log.iter().rposition(|e| e.committed_at <= t);
    let archive = env.store.archive();
    let mut deleted_at: BTreeMap<String, chrono::DateTime<chrono::Utc>> = BTreeMap::new();
    for (path, writes) in &archive {
        let Some(rel) = path.strip_prefix(&format!("{root}/")) else {
            continue;
        };
        if rel.starts_with('_') || !rel.ends_with(".parquet") {
            continue;
        }
        for w in writes {
            if w.deleted
                && let Some(gone) = w.gone
            {
                deleted_at.entry(rel.to_string()).or_insert(gone);
                if let Some(v) = at(gone) {
                    if live[v].contains(rel) {
                        return Err(format!("{rel} deleted at {gone} while live at version {v}"));
                    }
                    for (name, pv) in &pins[v] {
                        if live.get(*pv as usize).is_some_and(|s| s.contains(rel)) {
                            return Err(format!(
                                "{rel} deleted at {gone} while pin {name} (version {pv}) names it"
                            ));
                        }
                    }
                }
            }
        }
    }
    for (v, e) in log.iter().enumerate() {
        for a in &e.adds {
            if deleted_at.get(a).is_some_and(|d| *d < e.committed_at) {
                return Err(format!("version {v} added {a}, deleted before"));
            }
        }
        let before = if v == 0 {
            BTreeMap::new()
        } else {
            pins[v - 1].clone()
        };
        for (name, pv) in &pins[v] {
            if before.get(name) != Some(pv)
                && let Some(gone) = live
                    .get(*pv as usize)
                    .into_iter()
                    .flatten()
                    .find(|f| deleted_at.get(*f).is_some_and(|d| *d < e.committed_at))
            {
                return Err(format!(
                    "pin {name} of version {pv} committed at version {v}, its file {gone} \
                     already deleted"
                ));
            }
        }
    }

    // After the last purge.
    let report = seen.last.as_ref().ok_or("no last purge")?;
    let p = report.version;
    let newest = live.last().cloned().unwrap_or_default();
    let pinned: BTreeSet<String> = pins
        .last()
        .into_iter()
        .flatten()
        .flat_map(|(_, v)| live[*v as usize].iter().cloned())
        .collect();
    let removed: BTreeSet<String> = log
        .iter()
        .take(p as usize + 1)
        .flat_map(|e| e.removes.iter().cloned())
        .filter(|f| !newest.contains(f) && !pinned.contains(f))
        .collect();
    let stored = env.store.snapshot();
    for f in &removed {
        if stored.contains_key(&format!("{root}/{f}")) {
            return Err(format!("{f}, removed before the last purge, is left"));
        }
    }
    for (path, body) in &stored {
        let Some(name) = path.strip_prefix(&format!("{root}/_delta_log/")) else {
            continue;
        };
        if let Some(v) = name.strip_suffix(".checkpoint.parquet") {
            let v: u64 = v.parse().map_err(|e| format!("{name}: {e}"))?;
            if v < p {
                return Err(format!("checkpoint {v} is older than the last purge ({p})"));
            }
        }
        if name.ends_with(".json") && !name.starts_with('_') {
            for line in String::from_utf8_lossy(body).lines() {
                let Ok(doc) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                let Some(add) = doc.get("add") else {
                    continue;
                };
                let path = add["path"].as_str().unwrap_or_default();
                if !removed.contains(path) {
                    continue;
                }
                let stats: Value = add["stats"]
                    .as_str()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(Value::Null);
                if stats.get("minValues").is_some() || stats.get("maxValues").is_some() {
                    return Err(format!("{name} still holds the statistics of {path}"));
                }
            }
        }
    }
    let live_manifests: BTreeSet<String> = pins
        .last()
        .into_iter()
        .flatten()
        .map(|(n, v)| format!("{ROOT}/t/meta/pins/{n}/{v:020}.json"))
        .collect();
    for path in stored.keys() {
        if let Some(rest) = path.strip_prefix(&format!("{ROOT}/t/meta/pins/"))
            && !live_manifests.contains(path)
            && let Some(v) = rest
                .rsplit('/')
                .next()
                .and_then(|f| f.strip_suffix(".json"))
                .and_then(|v| v.parse::<u64>().ok())
            && v < p
        {
            return Err(format!(
                "manifest {path} of a version before the purge is left"
            ));
        }
    }
    let (version, rows) = seen.newest.as_ref().ok_or("no read")?;
    if *version as usize >= live.len() || *rows != rows_of(env, &root, &live[*version as usize]) {
        return Err(format!(
            "the read of version {version} differs from its files"
        ));
    }
    for (name, v, rows) in &seen.pinned {
        if *rows != rows_of(env, &root, &live[*v as usize]) {
            return Err(format!("pin {name} (version {v}) reads differently"));
        }
    }
    Ok(())
}

async fn scenario(mut env: SimEnv, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    // Chosen from the seed, not drawn, so every other choice of a seed stays as it was.
    if moraine::testing::splitmix64(env.seed).is_multiple_of(2) {
        env.store = moraine_testkit::SimStore::new(env.clock.clone())
            .with_mtime_resolution(Duration::from_secs(1));
    }
    let clients = env.rng.range(2, 4) as usize;
    let ops = env.rng.range(4, 12) as usize;
    let faults = env.rng.chance(0.4);
    let rules = if faults {
        FaultRules::random(&mut env.rng)
    } else {
        FaultRules::default()
    };
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let done = Arc::new(AtomicUsize::new(0));
    let failure: Arc<Mutex<Option<String>>> = Arc::default();
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let rng = env.rng.fork(c as u64 + 1);
        let db = open(&env, c, &rules);
        let (clock, sim, seen) = (env.client_clock(c), env.clock.clone(), seen.clone());
        let d = Done(done.clone());
        futs.push(Box::pin(async move {
            let _done = d;
            if let Some(t) = table(db.await).await {
                client(t, c, ops, rng, clock, sim, seen).await;
            }
        }));
    }
    let db = open(&env, clients, &FaultRules::default());
    let (clock, seen2, f2) = (env.client_clock(clients), seen.clone(), failure.clone());
    let seed = env.seed;
    futs.push(Box::pin(async move {
        let r = match table(db.await).await {
            Some(t) => last(t, seed, done, clients, clock, seen2).await,
            None => Err("the last client could not open the table".into()),
        };
        if let Err(e) = r {
            *f2.lock().unwrap() = Some(e);
        }
    }));
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;
    if let Some(e) = failure.lock().unwrap().take() {
        return Err(e);
    }
    let s = seen.lock().unwrap();
    check(&env, &s)?;
    let mut tot = totals.lock().unwrap();
    tot.seeds += 1;
    tot.purges += s.purges + 1;
    tot.purge_errors += s.purge_errors;
    tot.files_purged += s.files_purged + s.last.as_ref().map_or(0, |r| r.files_deleted as u64);
    tot.entries_scrubbed +=
        s.entries_scrubbed + s.last.as_ref().map_or(0, |r| r.entries_scrubbed as u64);
    tot.expired_reads += s.expired_reads;
    tot.pins_refused += s.pins_refused;
    Ok(())
}

/// Seeds that failed during development; they run on every test run. Each had a pin refused by
/// a purge delete the manifest of the same pin committed earlier (D66).
const REGRESSION_SEEDS: &[u64] = &[160, 1201, 2286, 3029, 4622, 5967, 6304, 7218, 9276];

#[test]
fn purges_everywhere() {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let seeds = seeds_from_env(100);
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
        "RESULT purge_sim seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    assert!(t.files_purged > 0, "no purge collected a file");
    assert!(t.entries_scrubbed > 0, "no purge erased statistics");
}
