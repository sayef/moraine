//! Blobs through the simulation (D57): uploads, pass-through copies and
//! staged uploads race vacuum's blob collection, pins, clients that crash mid-write, and
//! simulated time running past the grace period, with random store faults.
//!
//! Modes: `Upload` (values written as bytes), `Copy` (rows take other rows' references, the
//! sources are deleted), `Pins` (both, with pins), and scenario 7 `slow_writer_vs_collectors`
//! (a staged upload committed just under, and just over, the commit deadline while others
//! collect). A crash is a write dropped at a random instant, after which the client opens a new
//! handle, as a restarted process would.
//!
//! The checks read the store's full history, deleted objects included, without moraine:
//!
//! - no blob collector deleted an object that a version younger than the grace period, or a
//!   pinned version, references (scenario 13 for blobs);
//! - every read through a fresh snapshot gets every blob of every row, with the bytes its row
//!   was written with (I3 for blob values);
//! - after writes stop and two passes beyond the grace period, `blobs/` holds exactly the
//!   objects the live rows reference (scenario 14 for blobs), and `verify` is clean.
//!
//! More seeds: `MORAINE_SIM_SEEDS=10000`; one seed: `MORAINE_SIM_SEED=<n>`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::AsArray;
use futures::FutureExt;
use moraine::blob::{self, BlobRef};
use moraine::clock::Clock;
use moraine::expr::ScalarValue;
use moraine::store::RetryPolicy;
use moraine::{Consistency, Database, Error, MaintainOptions, Table, TableOptions};
use moraine_testkit::blobgen::{Body, content, row, schema};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};
use moraine_testkit::table_log::{read_log_all, read_parquet_archived};
use moraine_testkit::{FaultRules, SimRng};

const GRACE: Duration = Duration::from_secs(600);
const DEADLINE: Duration = Duration::from_secs(150);
const INLINE: u64 = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Upload,
    Copy,
    Pins,
    SlowWriter,
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn open_db(
    env: &SimEnv,
    client: usize,
    rules: &FaultRules,
) -> impl Future<Output = moraine::Result<Database>> + Send + 'static {
    Database::builder_for_store(env.chaos_store(client, rules.clone()), "db")
        .clock(env.client_clock(client))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .small_table_bytes(0)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

async fn open_table(db: impl Future<Output = moraine::Result<Database>>) -> Option<Table> {
    let db = db.await.ok()?;
    for _ in 0..10 {
        if let Ok(t) = db.open_table("t").await {
            return Some(t);
        }
    }
    None
}

#[derive(Default)]
struct Shared {
    violations: Vec<String>,
    reads: u64,
    blobs_read: u64,
    copies: u64,
    staged: u64,
    crashes: u64,
    passes: u64,
    deleted: u64,
    slow: Vec<(bool, bool)>,
}

/// Reads every row and every blob through a fresh snapshot.
async fn check_read(t: &Table, sh: &Mutex<Shared>) {
    let Ok(batches) = t.scan().collect().await else {
        return;
    };
    sh.lock().unwrap().reads += 1;
    for b in &batches {
        let src = b.column(1).as_string::<i32>();
        let Ok(refs) = blob::refs(b, "body") else {
            sh.lock()
                .unwrap()
                .violations
                .push("unreadable blob column".into());
            return;
        };
        for (i, r) in refs.iter().enumerate() {
            let Some(r) = r else { continue };
            let Ok(h) = t.blob(r) else { continue };
            match h.bytes().await {
                Ok(got) => {
                    let mut g = sh.lock().unwrap();
                    g.blobs_read += 1;
                    if got != content(src.value(i), r.size() as usize) {
                        g.violations
                            .push(format!("blob of {} has other bytes", src.value(i)));
                    }
                }
                Err(e @ (Error::BlobNotFound(_) | Error::BlobCorrupt(_))) => {
                    sh.lock().unwrap().violations.push(format!(
                        "read at version {}: blob {:?} of {}: {e}",
                        t.version(),
                        r.key(),
                        src.value(i)
                    ));
                }
                Err(_) => {}
            }
        }
    }
}

/// A random other row's reference, for a pass-through copy.
async fn some_ref(t: &Table, rng: &mut SimRng) -> Option<(String, BlobRef)> {
    let batches = t.scan().collect().await.ok()?;
    let mut all = Vec::new();
    for b in &batches {
        let src = b.column(1).as_string::<i32>();
        for (i, r) in blob::refs(b, "body").ok()?.into_iter().enumerate() {
            if let Some(r) = r {
                all.push((src.value(i).to_string(), r));
            }
        }
    }
    if all.is_empty() {
        return None;
    }
    Some(all.swap_remove(rng.below(all.len() as u64) as usize))
}

/// Runs `w`, or drops it at a random suspension point (just before a store call or a timer
/// fires) to model a crash mid-write; true when dropped.
async fn maybe_crash<F: Future<Output = ()> + Send>(w: F, rng: &mut SimRng) -> bool {
    if !rng.chance(0.1) {
        w.await;
        return false;
    }
    CrashAt {
        f: Some(Box::pin(w)),
        left: rng.below(12) as u32,
        armed: false,
    }
    .await
}

/// See [`maybe_crash`].
struct CrashAt<F> {
    f: Option<Pin<Box<F>>>,
    left: u32,
    armed: bool,
}

impl<F: Future<Output = ()>> Future for CrashAt<F> {
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<bool> {
        // Dropped at the next poll. A gate that has not opened yet is withdrawn and the client
        // counted as running again (the scheduler's `Parked` guard), so the count stays right
        // either way. Asking whether the client waits at a gate cannot tell the write's gate from
        // the gate of the handle's commit task (D64), which shares the client's store.
        if self.armed {
            self.f = None;
            return std::task::Poll::Ready(true);
        }
        let Some(f) = self.f.as_mut() else {
            return std::task::Poll::Ready(false);
        };
        match f.as_mut().poll(cx) {
            std::task::Poll::Ready(()) => std::task::Poll::Ready(false),
            std::task::Poll::Pending => {
                if self.left == 0 {
                    self.armed = true;
                } else {
                    self.left -= 1;
                }
                std::task::Poll::Pending
            }
        }
    }
}

async fn scenario(mut env: SimEnv, mode: Mode, totals: Arc<Mutex<Totals>>) -> Result<(), String> {
    let clients = env.rng.range(2, 5) as usize;
    let ops = env.rng.range(4, 12) as usize;
    let rules = match mode {
        Mode::SlowWriter => FaultRules::default(),
        _ => FaultRules::random(&mut env.rng),
    };
    let setup = open_db(&env, 0, &FaultRules::default());
    env.run(vec![Box::pin(async move {
        let db = setup.await.expect("open");
        db.create_table(
            "t",
            schema(),
            TableOptions::default()
                .primary_key(["id"])
                .grace_period(GRACE)
                .blob_inline_bytes(INLINE),
        )
        .await
        .expect("create");
    }) as ClientFuture])
        .await
        .map_err(|e| format!("setup: {e:?}"))?;

    let sh: Arc<Mutex<Shared>> = Arc::default();
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let mut rng = env.rng.fork(c as u64 + 1);
        let sh = sh.clone();
        // A crashed client comes back as a new process: a new client id, so its clock draws
        // other random names than the crashed one did.
        let reopen = {
            let rules = rules.clone();
            let mut v: Vec<_> = (0..4)
                .map(|k| {
                    let id = c + 16 * k;
                    (id, env.client_clock(id), open_db(&env, id, &rules).boxed())
                })
                .collect();
            v.reverse();
            Arc::new(Mutex::new(v))
        };
        let keys: Vec<String> = (0..rng.range(1, 4)).map(|n| format!("c{c}k{n}")).collect();
        futs.push(Box::pin(async move {
            let next = || reopen.lock().unwrap().pop();
            let Some((_, mut clock, db)) = next() else {
                return;
            };
            let Some(mut t) = open_table(db).await else {
                return;
            };
            if mode == Mode::SlowWriter && c == 0 {
                slow_writer(&t, &clock, &mut rng, &sh).await;
                return;
            }
            for j in 0..ops {
                let op = format!("c{c}o{j}");
                let key = keys[rng.below(keys.len() as u64) as usize].clone();
                let roll = rng.below(100);
                let crashed = if roll < 30 {
                    let n = match rng.below(3) {
                        0 => rng.below(INLINE + 1),
                        1 => INLINE + 1 + rng.below(200),
                        _ => 0,
                    } as usize;
                    let b = row(&key, &op, &Body::Bytes(content(&op, n)));
                    maybe_crash(t.upsert(vec![b]).map(drop), &mut rng).await
                } else if roll < 45 && mode != Mode::Upload {
                    match some_ref(&t, &mut rng).await {
                        Some((src, r)) => {
                            sh.lock().unwrap().copies += 1;
                            let b = row(&key, &src, &Body::Ref(r));
                            maybe_crash(t.upsert(vec![b]).map(drop), &mut rng).await
                        }
                        None => false,
                    }
                } else if roll < 55 {
                    // A staged upload, committed after a pause around the commit deadline.
                    let n = (INLINE + 1 + rng.below(100)) as usize;
                    let body = content(&op, n);
                    let staged = t
                        .upload_blob(futures::stream::iter([Ok::<_, std::io::Error>(body)]), None)
                        .await;
                    if let Ok(r) = staged {
                        sh.lock().unwrap().staged += 1;
                        let pause = Duration::from_secs(rng.range(0, 200));
                        clock.sleep(pause).await;
                        let b = row(&key, &op, &Body::Ref(r));
                        maybe_crash(t.upsert(vec![b]).map(drop), &mut rng).await
                    } else {
                        false
                    }
                } else if roll < 70 {
                    let k = vec![ScalarValue::Utf8(Some(key.clone()))];
                    maybe_crash(t.delete_keys(&[k]).map(drop), &mut rng).await
                } else if roll < 85 {
                    let o = MaintainOptions::default()
                        .leases(rng.chance(0.5))
                        .compaction(Some(
                            moraine::CompactionPolicy::default()
                                .fanout(2)
                                .max_files(3)
                                .min_file_age(Duration::ZERO),
                        ))
                        .checkpoint_interval(Some(rng.range(1, 5)));
                    if let Ok(r) = t.maintain(&o).await {
                        let mut g = sh.lock().unwrap();
                        g.passes += 1;
                        g.deleted += r.vacuum.map_or(0, |v| v.blobs_deleted as u64);
                    }
                    false
                } else if roll < 93 || mode != Mode::Pins {
                    clock.sleep(Duration::from_secs(rng.range(10, 400))).await;
                    false
                } else {
                    let name = format!("p{}", rng.below(2));
                    if rng.chance(0.6) {
                        let _ = t.pin(&name, t.version()).await;
                    } else {
                        let _ = t.unpin(&name).await;
                    }
                    false
                };
                if crashed {
                    sh.lock().unwrap().crashes += 1;
                    let Some((_, nclock, db)) = next() else {
                        return;
                    };
                    let Some(nt) = open_table(db).await else {
                        return;
                    };
                    (clock, t) = (nclock, nt);
                }
                check_read(&t, &sh).await;
            }
        }));
    }
    env.run(futs)
        .await
        .map_err(|e| format!("run did not finish: {e:?}"))?;

    // Scenario 14 for blobs: writes stop, pins go, two passes beyond the grace period.
    let fresh = open_db(&env, 99, &FaultRules::default());
    let clock = env.client_clock(99);
    let fin: Arc<Mutex<Option<moraine::VerifyReport>>> = Arc::default();
    let (f2, sh2) = (fin.clone(), sh.clone());
    env.run(vec![Box::pin(async move {
        let db = fresh.await.expect("open");
        let t = db.open_table("t").await.expect("open table");
        for name in t.pins().expect("pins").into_keys() {
            t.unpin(&name).await.expect("unpin");
        }
        let quiet = MaintainOptions::default()
            .leases(false)
            .compaction(None)
            .checkpoint_interval(Some(1));
        for _ in 0..2 {
            clock.sleep(GRACE + Duration::from_secs(60)).await;
            t.maintain(&quiet).await.expect("final maintenance");
        }
        check_read(&t, &sh2).await;
        *f2.lock().unwrap() = Some(t.verify().await.expect("verify"));
    }) as ClientFuture])
        .await
        .map_err(|e| format!("final phase: {e:?}"))?;
    let verify = fin.lock().unwrap().take().ok_or("no final verify")?;
    if !verify.is_ok() {
        return Err(format!("verify after the final phase: {verify:?}"));
    }
    let sh = std::mem::take(&mut *sh.lock().unwrap());
    if let Some(v) = sh.violations.first() {
        return Err(v.clone());
    }
    check_history(&env)?;
    if mode == Mode::SlowWriter {
        // Nothing staged longer than the commit deadline before its write is committed as it
        // is. Under the deadline it is kept unless the write's own requests (each may wait up
        // to 30 s of simulated time for its turn) carried it past.
        if sh.slow.iter().any(|(under, copied)| !under && !copied) {
            return Err("a blob staged past the commit deadline was committed uncopied".into());
        }
    }
    let mut t = totals.lock().unwrap();
    t.seeds += 1;
    t.reads += sh.reads;
    t.blobs_read += sh.blobs_read;
    t.copies += sh.copies;
    t.staged += sh.staged;
    t.crashes += sh.crashes;
    t.passes += sh.passes;
    t.blobs_deleted += sh.deleted;
    t.slow += sh.slow.len() as u64;
    t.slow_kept += sh.slow.iter().filter(|(u, c)| *u && !c).count() as u64;
    t.slow_copied += sh.slow.iter().filter(|(u, c)| !u && *c).count() as u64;
    t.faults += env.faults.total();
    Ok(())
}

/// Scenario 7: a staged upload, then a pause of just under or just over the commit deadline,
/// then the commit, while the other clients collect. Under: committed as uploaded. Over:
/// committed as a fresh copy (nothing late is committed). The row's blob reads either way.
async fn slow_writer(
    t: &Table,
    clock: &Arc<moraine_testkit::ClientClock>,
    rng: &mut SimRng,
    sh: &Mutex<Shared>,
) {
    for (n, under) in [true, false, rng.chance(0.5)].into_iter().enumerate() {
        let op = format!("slow{n}");
        let body = content(&op, 500);
        let Ok(r) = t
            .upload_blob(futures::stream::iter([Ok::<_, std::io::Error>(body)]), None)
            .await
        else {
            continue;
        };
        let pause = if under {
            DEADLINE - Duration::from_secs(5)
        } else {
            DEADLINE + Duration::from_secs(5)
        };
        clock.sleep(pause).await;
        if t.upsert(vec![row(&op, &op, &Body::Ref(r.clone()))])
            .await
            .is_err()
        {
            continue;
        }
        let Ok(Some(b)) = t.get(&[ScalarValue::Utf8(Some(op.clone()))]).await else {
            sh.lock().unwrap().violations.push(format!("{op} missing"));
            continue;
        };
        let got = blob::refs(&b, "body").ok().and_then(|mut v| v.remove(0));
        let copied = got.as_ref().and_then(|g| g.key()) != r.key();
        sh.lock().unwrap().slow.push((under, copied));
        // Wait past the grace period with the others collecting; the blob stays.
        clock.sleep(GRACE + Duration::from_secs(30)).await;
        check_read(t, sh).await;
    }
}

/// From the store's history: blob objects a version needed were never deleted while it was
/// needed (younger than the grace period since a later version replaced it, or pinned), and at
/// the end `blobs/` holds exactly what the newest version references.
fn check_history(env: &SimEnv) -> Result<(), String> {
    let log = read_log_all(&env.store, "db/t/data");
    let archive = env.store.archive();
    let mut deleted: HashMap<String, chrono::DateTime<chrono::Utc>> = HashMap::new();
    let mut existing: BTreeSet<String> = BTreeSet::new();
    for (path, writes) in &archive {
        let Some(key) = path.strip_prefix("db/t/blobs/") else {
            continue;
        };
        for w in writes {
            if w.deleted
                && let Some(at) = w.gone
            {
                deleted.insert(key.to_string(), at);
            }
        }
        if writes.last().is_some_and(|w| w.gone.is_none()) {
            existing.insert(key.to_string());
        }
    }
    let grace = chrono::Duration::from_std(GRACE).unwrap_or_default();
    let mut file_keys: HashMap<String, Arc<HashSet<String>>> = HashMap::new();
    let mut keys_of = |path: &str| -> Arc<HashSet<String>> {
        file_keys
            .entry(path.to_string())
            .or_insert_with(|| {
                let mut s = HashSet::new();
                for b in read_parquet_archived(&env.store, &format!("db/t/data/{path}")) {
                    for r in blob::refs(&b, "body")
                        .unwrap_or_default()
                        .into_iter()
                        .flatten()
                    {
                        if let Some(k) = r.key() {
                            s.insert(k.to_string());
                        }
                    }
                }
                Arc::new(s)
            })
            .clone()
    };
    let mut files: BTreeSet<String> = BTreeSet::new();
    let mut versions: Vec<(u64, chrono::DateTime<chrono::Utc>, HashSet<String>)> = Vec::new();
    let mut pins: BTreeMap<String, u64> = BTreeMap::new();
    let mut pin_spans: Vec<(
        u64,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = Vec::new();
    for e in &log {
        for r in &e.removes {
            files.remove(r);
        }
        for a in &e.adds {
            files.insert(a.clone());
        }
        let mut keys = HashSet::new();
        for f in &files {
            keys.extend(keys_of(f).iter().cloned());
        }
        if let Some(cfg) = &e.configuration {
            let now: BTreeMap<String, u64> = cfg
                .get("moraine.pins")
                .and_then(|v| serde_json::from_str(v).ok())
                .unwrap_or_default();
            if now != pins {
                for s in pin_spans.iter_mut().filter(|s| s.2.is_none()) {
                    if !now.values().any(|v| *v == s.0) {
                        s.2 = Some(e.committed_at);
                    }
                }
                for v in now.values() {
                    if !pins.values().any(|p| p == v) {
                        pin_spans.push((*v, e.committed_at, None));
                    }
                }
                pins = now;
            }
        }
        versions.push((e.version, e.committed_at, keys));
    }
    for (i, (v, _, keys)) in versions.iter().enumerate() {
        // Needed until the next version is older than the grace period.
        let until = versions.get(i + 1).map(|n| n.1 + grace);
        for k in keys {
            if let Some(at) = deleted.get(k)
                && until.is_none_or(|u| *at < u)
            {
                return Err(format!(
                    "blob {k} of version {v} deleted at {at} while the version was needed"
                ));
            }
        }
    }
    for (pv, from, to) in &pin_spans {
        let Some((_, _, keys)) = versions.iter().find(|x| x.0 == *pv) else {
            continue;
        };
        for k in keys {
            if let Some(at) = deleted.get(k)
                && *at >= *from
                && to.is_none_or(|t| *at < t)
            {
                return Err(format!("blob {k} of pinned version {pv} deleted at {at}"));
            }
        }
    }
    let live: BTreeSet<String> = versions
        .last()
        .map(|v| v.2.iter().cloned().collect())
        .unwrap_or_default();
    if existing != live {
        let extra: Vec<_> = existing.difference(&live).collect();
        let missing: Vec<_> = live.difference(&existing).collect();
        return Err(format!(
            "after the final passes blobs/ differs from the live references: extra {extra:?}, \
             missing {missing:?}"
        ));
    }
    Ok(())
}

#[derive(Default, Debug)]
struct Totals {
    seeds: u64,
    reads: u64,
    blobs_read: u64,
    copies: u64,
    staged: u64,
    crashes: u64,
    passes: u64,
    blobs_deleted: u64,
    slow: u64,
    slow_kept: u64,
    slow_copied: u64,
    faults: u64,
}

fn run(mode: Mode) -> Totals {
    let totals = Arc::new(Mutex::new(Totals::default()));
    let t = totals.clone();
    let seeds = seeds_from_env(200);
    if let Err(f) = run_seeds(seeds.clone(), move |env| scenario(env, mode, t.clone())) {
        panic!("{f}");
    }
    let t = std::mem::take(&mut *totals.lock().unwrap());
    println!(
        "RESULT blobs_sim {mode:?} seeds={}..{} {t:?}",
        seeds.start, seeds.end
    );
    t
}

/// Uploads racing blob collection, with crashes and faults.
#[test]
fn blob_upload_vs_collection() {
    let t = run(Mode::Upload);
    assert!(
        t.blobs_read > 0 && t.blobs_deleted > 0 && t.crashes > 0,
        "{t:?}"
    );
}

/// Pass-through copies of references whose rows are deleted while collection runs.
#[test]
fn blob_copy_vs_collection() {
    let t = run(Mode::Copy);
    assert!(t.copies > 0 && t.blobs_deleted > 0, "{t:?}");
}

/// Pins keep their versions' blobs while collection runs.
#[test]
fn blob_collection_respects_pins() {
    let t = run(Mode::Pins);
    assert!(t.passes > 0 && t.blobs_read > 0, "{t:?}");
}

/// Scenario 7: a writer pauses between its blob upload and its commit for just under, and just
/// over, the commit deadline, while collectors run.
#[test]
fn slow_writer_vs_collectors() {
    let t = run(Mode::SlowWriter);
    assert!(
        t.slow >= 2 * t.seeds && t.slow_kept > 0 && t.slow_copied > 0,
        "{t:?}"
    );
}
