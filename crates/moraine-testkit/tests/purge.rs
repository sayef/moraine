//! Purge and erasure (D66), case by case, on a store with simulated server time: removed
//! files collected at once with their sidecars and blobs, the cutoff, uploads of commits in
//! flight left alone, statistics erased from the log, stale checkpoints and pin manifests
//! deleted, pins of older versions refused, a checkpoint written by a pass that read the log
//! before the purge, and the lease.
//!
//! Failpoints are global to the process, so the tests take turns.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use moraine::expr::{col, lit};
use moraine::{Consistency, Database, Error, Purge, Table, TableOptions};
use moraine_testkit::maintcheck::{Row, batch, to_rows};
use moraine_testkit::{ClientClock, SimClock, SimStore};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SECRET: &str = "secret-value-7f3a";

struct Env {
    sim: SimClock,
    store: Arc<SimStore>,
    seed: u64,
}

impl Env {
    fn new(seed: u64) -> Env {
        let sim = SimClock::new();
        Env {
            store: Arc::new(SimStore::new(sim.clone())),
            sim,
            seed,
        }
    }

    async fn db(&mut self) -> Database {
        self.seed += 1;
        let clock = ClientClock::new(self.sim.clone(), None, self.seed as usize, self.seed);
        let store: Arc<dyn ObjectStore> = self.store.clone();
        Database::builder_for_store(store, "db")
            .clock(Arc::new(clock))
            .consistency(Consistency::Strong)
            .small_table_bytes(0)
            .open()
            .await
            .unwrap()
    }

    fn keys(&self, prefix: &str) -> BTreeSet<String> {
        self.store
            .snapshot()
            .into_keys()
            .filter(|k| k.starts_with(prefix))
            .collect()
    }

    /// Data files in the store.
    fn data(&self) -> BTreeSet<String> {
        self.keys("db/t/data/")
            .into_iter()
            .filter(|k| !k.contains("/_delta_log/") && k.ends_with(".parquet"))
            .map(|k| k["db/t/data/".len()..].to_string())
            .collect()
    }

    fn checkpoints(&self) -> Vec<u64> {
        self.keys("db/t/data/_delta_log/")
            .into_iter()
            .filter_map(|k| {
                k.rsplit('/')
                    .next()?
                    .strip_suffix(".checkpoint.parquet")?
                    .parse()
                    .ok()
            })
            .collect()
    }

    /// Whether any log entry or pin manifest in the store holds `text`.
    fn log_holds(&self, text: &str) -> bool {
        self.store.snapshot().into_iter().any(|(k, v)| {
            (k.contains("/_delta_log/") && k.ends_with(".json") || k.contains("/meta/pins/"))
                && String::from_utf8_lossy(&v).contains(text)
        })
    }
}

fn row(k: &str, op: &str) -> Row {
    Row {
        k: k.into(),
        op: op.into(),
        j: 0,
        g: Some(1),
    }
}

fn files(t: &Table) -> BTreeSet<String> {
    t.snapshot_in_hand()
        .files()
        .into_iter()
        .map(|f| f.path)
        .collect()
}

/// A table with one file holding the secret, rewritten away by a delete.
async fn erased_table(env: &mut Env) -> (Table, BTreeSet<String>, u64) {
    let t = env
        .db()
        .await
        .create_table(
            "t",
            moraine_testkit::datagen::row_schema(),
            TableOptions::default(),
        )
        .await
        .unwrap();
    t.append(vec![batch(&[row("a", "keep"), row(SECRET, "gone")])])
        .await
        .unwrap();
    t.append(vec![batch(&[row("b", "keep")])]).await.unwrap();
    let before = files(&t);
    let v = t.refresh().await.unwrap();
    t.delete(col("k").eq(lit(SECRET))).await.unwrap();
    let removed: BTreeSet<String> = before.difference(&files(&t)).cloned().collect();
    assert_eq!(removed.len(), 1);
    // Store requests take no simulated time: the purge starts strictly after the removal.
    env.sim.advance(Duration::from_secs(1));
    (t, removed, v)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collect_now_takes_removed_files_and_nothing_else() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(10);
    let (t, removed, _) = erased_table(&mut env).await;
    // An upload of a commit in flight: young, in no log entry.
    let in_flight = "part-in-flight.parquet";
    env.store
        .put(
            &Path::from(format!("db/t/data/{in_flight}")),
            PutPayload::from(Bytes::from_static(b"PAR1")),
        )
        .await
        .unwrap();
    let r = t.purge(&Purge::new()).await.unwrap();
    assert_eq!(r.files_deleted, 1, "{r:?}");
    let data = env.data();
    assert!(removed.iter().all(|f| !data.contains(f)));
    assert!(files(&t).iter().all(|f| data.contains(f)));
    assert!(data.contains(in_flight));
    let rows = to_rows(&t.scan().collect().await.unwrap());
    assert_eq!(
        rows.iter().map(|r| r.k.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    // Without erasure the log keeps the statistics.
    assert!(env.log_holds(SECRET));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cutoff_keeps_later_removals() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(20);
    let (t, removed, _) = erased_table(&mut env).await;
    // Removed before the cutoff: the delete above. After it: this one.
    env.sim.advance(Duration::from_secs(60));
    let cutoff = env.sim.server_time();
    env.sim.advance(Duration::from_secs(60));
    let before = files(&t);
    t.delete(col("k").eq(lit("b"))).await.unwrap();
    let later: BTreeSet<String> = before.difference(&files(&t)).cloned().collect();
    let r = t.purge(&Purge::new().removed_before(cutoff)).await.unwrap();
    assert_eq!(r.files_deleted, 1, "{r:?}");
    let data = env.data();
    assert!(removed.iter().all(|f| !data.contains(f)));
    assert!(later.iter().all(|f| data.contains(f)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn erasure_takes_values_out_of_the_log() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(30);
    let (t, removed, v) = erased_table(&mut env).await;
    // A pin of the version holding the secret, removed again: its manifest names it.
    t.pin("old", v).await.unwrap();
    t.unpin("old").await.unwrap();
    // A checkpoint from before the purge holds the file too.
    t.checkpoint().await.unwrap();
    let older = env.checkpoints();
    for n in 0..3 {
        t.append(vec![batch(&[row(&format!("c{n}"), "keep")])])
            .await
            .unwrap();
    }
    assert!(env.log_holds(SECRET));
    let r = t.purge(&Purge::new().erase()).await.unwrap();
    assert!(r.entries_scrubbed >= 1, "{r:?}");
    assert_eq!(r.files_erased, 1, "{r:?}");
    assert!(r.checkpoints_deleted >= older.len(), "{r:?}");
    assert_eq!(r.manifests_deleted, 1, "{r:?}");
    assert!(
        !env.log_holds(SECRET),
        "a log entry or manifest still holds the value"
    );
    assert!(env.checkpoints().iter().all(|c| *c >= r.version));
    assert!(removed.iter().all(|f| !env.data().contains(f)));
    // The table reads as before, from a new handle too, and a pin before the purge is refused.
    let fresh = env.db().await.open_table("t").await.unwrap();
    let rows = to_rows(&fresh.scan().collect().await.unwrap());
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|r| r.k != SECRET));
    let e = t.pin("late", v).await.unwrap_err();
    assert!(matches!(e, Error::VersionNotAvailable(_)), "{e:?}");
    assert!(!env.keys("db/t/meta/pins/late/").iter().any(|_| true));
}

/// A pin current at the purge keeps its files: they are neither collected nor erased.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_current_pin_keeps_its_files() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(40);
    let t = env
        .db()
        .await
        .create_table(
            "t",
            moraine_testkit::datagen::row_schema(),
            TableOptions::default(),
        )
        .await
        .unwrap();
    t.append(vec![batch(&[row(SECRET, "pinned")])])
        .await
        .unwrap();
    let v = t.refresh().await.unwrap();
    t.pin("p", v).await.unwrap();
    t.delete(col("k").eq(lit(SECRET))).await.unwrap();
    let r = t.purge(&Purge::new().erase()).await.unwrap();
    assert_eq!(r.files_deleted, 0, "{r:?}");
    let pinned = t.snapshot_at_pin("p").await.unwrap();
    let rows = to_rows(&pinned.scan().collect().await.unwrap());
    assert_eq!(rows.len(), 1);
    assert!(env.log_holds(SECRET), "the pinned file's statistics stay");
}

/// A checkpoint pass that read the log before the purge's marker and writes after the purge
/// finished deletes its own checkpoint: it would hold erased values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_checkpoint_from_before_the_purge_deletes_itself() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(50);
    let (t, _, _) = erased_table(&mut env).await;
    let other = env.db().await.open_table("t").await.unwrap();
    fail::cfg("moraine::checkpoint::before_write", "1*pause").unwrap();
    let late = tokio::spawn(async move { other.checkpoint().await });
    // On the test thread: the paused failpoint blocks a worker, which can hold up timers.
    std::thread::sleep(Duration::from_millis(300));
    t.append(vec![batch(&[row("c", "keep")])]).await.unwrap();
    let r = t.purge(&Purge::new().erase()).await.unwrap();
    fail::remove("moraine::checkpoint::before_write");
    assert_eq!(
        late.await.unwrap().unwrap(),
        None,
        "the stale checkpoint stayed"
    );
    let cps = env.checkpoints();
    assert!(
        !cps.is_empty() && cps.iter().all(|c| *c >= r.version),
        "{cps:?} {r:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_purge_at_a_time() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(60);
    let (t, _, _) = erased_table(&mut env).await;
    let other = env.db().await.open_table("t").await.unwrap();
    let held = other
        .try_lease("purge", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("free");
    let e = t.purge(&Purge::new()).await.unwrap_err();
    assert!(matches!(e, Error::LeaseHeld { .. }), "{e:?}");
    other.release_lease(&held).await.unwrap();
    t.purge(&Purge::new()).await.unwrap();
}
