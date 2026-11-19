//! Case C8 for blobs and streaming compaction, with the `fail` crate's failpoints: a client
//! crashes (panics) right after uploading blob values, right after copying a reference, right
//! after staging an upload, in blob collection between its mark and its sweep, and in streaming
//! compaction before a merged file goes up. Its process restarts with a new handle and carries
//! on. A crash before the commit leaves nothing visible; whatever it uploaded is an orphan that
//! collection removes after the grace period. At the end every live row's blob reads with the
//! bytes it was written with, `blobs/` holds exactly what the live rows reference, and `verify`
//! is clean.
//!
//! Failpoints are global to the process, so this file is its own test binary with one test.

use std::collections::BTreeSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::AsArray;
use futures::FutureExt;
use moraine::blob;
use moraine::clock::Clock;
use moraine::store::RetryPolicy;
use moraine::{CompactionPolicy, Consistency, Database, MaintainOptions, Table, TableOptions};
use moraine_testkit::FaultRules;
use moraine_testkit::blobgen::{Body, content, row, schema};
use moraine_testkit::sim::{SimEnv, run_seeds, seeds_from_env};

const POINTS: &[&str] = &[
    "moraine::blob::after_upload",
    "moraine::blob::after_copy",
    "moraine::blob::after_stage",
    "moraine::blobs::after_mark",
    "moraine::compact::stream_before_upload",
];

/// Crashes across the seeds of the current point.
static FIRED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

const GRACE: Duration = Duration::from_secs(600);

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
    Database::builder_for_store(env.chaos_store(c, FaultRules::default()), "db")
        .clock(Arc::new(clock))
        .io_concurrency(1)
        .consistency(Consistency::Strong)
        .small_table_bytes(0)
        .request_retry(RetryPolicy::REQUEST)
        .open()
}

type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

fn maintain() -> MaintainOptions {
    MaintainOptions::default()
        .leases(false)
        .compaction(Some(CompactionPolicy::full()))
}

/// Operation `j` of client `c`: an upload, a copy, a staged upload, or a maintenance pass.
async fn op(t: &Table, c: usize, j: usize, kind: u64) -> moraine::Result<()> {
    let key = format!("c{c}k{}", j % 2);
    let name = format!("c{c}o{j}");
    match kind {
        0 => {
            t.upsert(vec![row(&key, &name, &Body::Bytes(content(&name, 300)))])
                .await?;
        }
        1 => {
            // A copy of one of this client's earlier values, when it has one.
            let rows = t.scan().collect().await?;
            let mut found = None;
            for b in &rows {
                let src = b.column(1).as_string::<i32>();
                for (i, r) in blob::refs(b, "body")?.into_iter().enumerate() {
                    if let Some(r) = r
                        && r.key().is_some()
                    {
                        found = Some((src.value(i).to_string(), r));
                    }
                }
            }
            if let Some((src, r)) = found {
                t.upsert(vec![row(&key, &src, &Body::Ref(r))]).await?;
            }
        }
        2 => {
            let body = content(&name, 400);
            let r = t
                .upload_blob(futures::stream::iter([Ok::<_, std::io::Error>(body)]), None)
                .await?;
            t.upsert(vec![row(&key, &name, &Body::Ref(r))]).await?;
        }
        _ => {
            t.maintain(&maintain()).await?;
        }
    }
    Ok(())
}

async fn scenario(mut env: SimEnv, point: &'static str) -> Result<(), String> {
    fail::cfg(point, "1*panic").map_err(|e| e.to_string())?;
    let clients = env.rng.range(2, 4) as usize;
    let crashes: Arc<Mutex<u64>> = Arc::default();
    let setup = open(&env, 0, 99);
    env.run(vec![Box::pin(async move {
        let db = setup.await.unwrap();
        db.create_table(
            "t",
            schema(),
            TableOptions::default()
                .primary_key(["id"])
                .grace_period(GRACE)
                .blob_inline_bytes(64),
        )
        .await
        .unwrap();
    }) as ClientFuture])
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut futs: Vec<ClientFuture> = Vec::new();
    for c in 0..clients {
        let crashes = crashes.clone();
        let mut rng = env.rng.fork(c as u64);
        let restarts: Vec<_> = (0..6).map(|life| open(&env, c, life)).collect();
        futs.push(Box::pin(async move {
            let mut restarts = restarts.into_iter();
            let mut table: Option<Table> = None;
            for j in 0..6 {
                if table.is_none() {
                    let Some(db) = restarts.next() else { return };
                    table = Some(db.await.unwrap().open_table("t").await.unwrap());
                }
                let t = table.clone().unwrap();
                let kind = rng.below(4);
                if AssertUnwindSafe(op(&t, c, j, kind))
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    *crashes.lock().unwrap() += 1;
                    table = None;
                }
            }
        }));
    }
    let r = env.run(futs).await;
    fail::remove(point);
    r.map_err(|e| format!("{e:?}"))?;
    FIRED.fetch_add(
        *crashes.lock().unwrap(),
        std::sync::atomic::Ordering::SeqCst,
    );

    // Quiet passes beyond the grace period, then the checks.
    let fresh = open(&env, 50, 0);
    let clock = env.client_clock(50);
    let store = env.store.clone();
    let out: Arc<Mutex<Option<Result<(), String>>>> = Arc::default();
    let o2 = out.clone();
    env.run(vec![Box::pin(async move {
        let t = fresh.await.unwrap().open_table("t").await.unwrap();
        for _ in 0..2 {
            clock.sleep(GRACE + Duration::from_secs(60)).await;
            t.maintain(&maintain()).await.unwrap();
        }
        let r = async {
            let rows = t.scan().collect().await.map_err(|e| e.to_string())?;
            let mut live = BTreeSet::new();
            for b in &rows {
                let src = b.column(1).as_string::<i32>();
                for (i, r) in blob::refs(b, "body")
                    .map_err(|e| e.to_string())?
                    .into_iter()
                    .enumerate()
                {
                    let Some(r) = r else { continue };
                    let got = t
                        .blob(&r)
                        .map_err(|e| e.to_string())?
                        .bytes()
                        .await
                        .map_err(|e| format!("{}: {e}", src.value(i)))?;
                    if got != content(src.value(i), r.size() as usize) {
                        return Err(format!("{}: other bytes", src.value(i)));
                    }
                    if let Some(k) = r.key() {
                        live.insert(format!("db/t/blobs/{k}"));
                    }
                }
            }
            let stored: BTreeSet<String> = store
                .snapshot()
                .into_keys()
                .filter(|k| k.starts_with("db/t/blobs/"))
                .collect();
            if stored != live {
                return Err(format!(
                    "blobs/ {stored:?} differs from the live references {live:?}"
                ));
            }
            let v = t.verify().await.map_err(|e| e.to_string())?;
            if !v.is_ok() {
                return Err(format!("verify: {v:?}"));
            }
            Ok(())
        }
        .await;
        *o2.lock().unwrap() = Some(r);
    }) as ClientFuture])
        .await
        .map_err(|e| format!("final phase: {e:?}"))?;
    out.lock()
        .unwrap()
        .take()
        .unwrap_or(Err("no result".into()))
}

#[test]
fn crash_at_every_blob_failpoint() {
    let _scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| {}));
    let seeds = seeds_from_env(100);
    for &point in POINTS {
        FIRED.store(0, std::sync::atomic::Ordering::SeqCst);
        if let Err(f) = run_seeds(seeds.clone(), |env| scenario(env, point)) {
            let _ = std::panic::take_hook();
            panic!("{point}: {f}");
        }
        let fired = FIRED.load(std::sync::atomic::Ordering::SeqCst);
        println!(
            "RESULT crash_blobs {point}: {} seeds clean, {fired} crashes",
            seeds.end - seeds.start
        );
        assert!(fired > 0, "{point} never fired");
    }
    let _ = std::panic::take_hook();
}
