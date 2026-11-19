//! A write that definitely fails deletes what it uploaded, and a write that commits deletes
//! what it uploaded and did not commit (D65): a lost file replace, a fenced one, a rewrite
//! planned again after a conflict, compaction yielding, and a commit past its deadline. After
//! each, the table's `data/` holds exactly the files some log entry adds.
//!
//! Failpoints are global to the process, so the tests take turns.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use moraine::expr::ScalarValue;
use moraine::{CompactionPolicy, Consistency, Database, Error, Replace, Table, TableOptions};
use moraine_testkit::maintcheck::{Row, batch};
use moraine_testkit::table_log::read_log_all;
use moraine_testkit::{ClientClock, SimClock, SimRng, SimStore, datagen};
use object_store::ObjectStore;

static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

    /// Data files in the store that no log entry adds, and log-entry files missing from it.
    fn orphans_and_missing(&self) -> (BTreeSet<String>, BTreeSet<String>) {
        let root = "db/t/data";
        let added: BTreeSet<String> = read_log_all(&self.store, root)
            .into_iter()
            .flat_map(|e| e.adds)
            .collect();
        let stored: BTreeSet<String> = self
            .store
            .snapshot()
            .into_keys()
            .filter_map(|k| k.strip_prefix(&format!("{root}/")).map(str::to_string))
            .filter(|k| !k.starts_with('_') && k.ends_with(".parquet"))
            .collect();
        (
            stored.difference(&added).cloned().collect(),
            added.difference(&stored).cloned().collect(),
        )
    }

    fn assert_clean(&self) {
        let (orphans, missing) = self.orphans_and_missing();
        assert!(orphans.is_empty(), "uploads left behind: {orphans:?}");
        assert!(missing.is_empty(), "committed files deleted: {missing:?}");
    }
}

fn keyed() -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
        .cluster_by(["g"])
}

fn row(k: &str, op: &str) -> Row {
    Row {
        k: k.into(),
        op: op.into(),
        j: 0,
        g: Some(1),
    }
}

fn paths(t: &Table) -> Vec<String> {
    t.snapshot_in_hand()
        .files()
        .into_iter()
        .map(|f| f.path)
        .collect()
}

async fn table(env: &mut Env) -> (Table, Table) {
    let a = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let b = env.db().await.open_table("t").await.unwrap();
    let mut rng = SimRng::new(env.seed);
    for n in 0..3 {
        a.append(vec![datagen::rows(&mut rng, &format!("w{n}"), 3, None, 2)])
            .await
            .unwrap();
    }
    a.refresh().await.unwrap();
    b.refresh().await.unwrap();
    (a, b)
}

fn deleted(t: &Table) -> u64 {
    t.write_metrics().uploads_deleted.load(Ordering::Relaxed)
}

/// Runs `replace` on `t` held before its log put, while `other` commits; returns its result.
async fn held_replace<F>(t: &Table, replace: Replace, other: F) -> moraine::Result<u64>
where
    F: std::future::Future<Output = ()>,
{
    fail::cfg("moraine::replace::before_commit", "1*pause").unwrap();
    let held = t.clone();
    let h = tokio::spawn(async move { held.replace_files(replace).await });
    // On the test thread: the paused failpoint blocks a worker, which can hold up timers.
    std::thread::sleep(Duration::from_millis(300));
    other.await;
    fail::remove("moraine::replace::before_commit");
    h.await.unwrap().map(|c| c.version)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_replace_deletes_its_files() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(100);
    let (a, b) = table(&mut env).await;
    let files = paths(&a);
    let r = Replace::new()
        .remove(files[1].clone())
        .add_rows(vec![batch(&[row("x", "lost")])]);
    let e = held_replace(&b, r, async {
        a.replace_files(Replace::new().remove_all(files[..2].to_vec()))
            .await
            .unwrap();
    })
    .await
    .unwrap_err();
    assert!(matches!(e, Error::FilesChanged(_)), "{e:?}");
    assert_eq!(deleted(&b), 1);
    env.assert_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_replace_deletes_its_files() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(200);
    let (a, b) = table(&mut env).await;
    let ttl = Duration::from_secs(60);
    let la = a.try_lease("fold", ttl).await.unwrap().expect("free");
    env.sim.advance(Duration::from_secs(120));
    let lb = b.try_lease("fold", ttl).await.unwrap().expect("expired");
    let files = paths(&a);
    let r = Replace::new()
        .remove(files[1].clone())
        .add_rows(vec![batch(&[row("x", "fenced")])])
        .fenced(&la);
    let e = held_replace(&a, r, async {
        b.replace_files(Replace::new().remove(files[0].clone()).fenced(&lb))
            .await
            .unwrap();
    })
    .await
    .unwrap_err();
    assert!(matches!(e, Error::Fenced { .. }), "{e:?}");
    assert_eq!(deleted(&a), 1);
    env.assert_clean();
}

/// An upsert rewrites the file holding its key, then loses to a delete that removed that file:
/// it plans again on the new version, and the first plan's rewritten file goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rewrite_planned_again_deletes_the_first_plan() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(300);
    let (a, b) = table(&mut env).await;
    a.append(vec![batch(&[row("key", "first"), row("other", "first")])])
        .await
        .unwrap();
    b.refresh().await.unwrap();
    fail::cfg("moraine::rewrite::before_commit", "1*pause").unwrap();
    let ta = a.clone();
    let upsert =
        tokio::spawn(async move { ta.upsert(vec![batch(&[row("key", "upserted")])]).await });
    // The upsert has rewritten the file and waits before its commit.
    // On the test thread: the paused failpoint blocks a worker, which can hold up timers.
    std::thread::sleep(Duration::from_millis(300));
    b.delete_keys(&[vec![ScalarValue::from("other")]])
        .await
        .unwrap();
    fail::remove("moraine::rewrite::before_commit");
    upsert.await.unwrap().unwrap();
    assert!(deleted(&a) >= 1, "the first plan's file was not deleted");
    env.assert_clean();
    let ops: Vec<String> = moraine_testkit::maintcheck::to_rows(&a.scan().collect().await.unwrap())
        .into_iter()
        .filter(|r| r.k == "key" || r.k == "other")
        .map(|r| format!("{}={}", r.k, r.op))
        .collect();
    assert_eq!(ops, vec!["key=upserted"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_that_yields_deletes_its_merged_files() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(400);
    let (a, b) = table(&mut env).await;
    fail::cfg("moraine::compact::before_commit", "1*pause").unwrap();
    let ta = a.clone();
    let compact = tokio::spawn(async move { ta.compact(&CompactionPolicy::full()).await });
    // On the test thread: the paused failpoint blocks a worker, which can hold up timers.
    std::thread::sleep(Duration::from_millis(300));
    // Removes one of the inputs before the merged file commits.
    let first = paths(&b)[0].clone();
    b.replace_files(Replace::new().remove(first)).await.unwrap();
    fail::remove("moraine::compact::before_commit");
    let report = compact.await.unwrap().unwrap();
    assert_eq!(report.yielded, 1, "{report:?}");
    assert!(deleted(&a) >= 1);
    env.assert_clean();
}

/// A commit that finds its deadline passed: a write starts again with fresh uploads (case C6)
/// and the first round's go once it commits; a swap of files uploaded ahead (a streamed
/// replace) cannot start again and deletes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_past_the_deadline_delete_their_uploads() {
    let _turn = TURN.lock().await;
    let mut env = Env::new(500);
    let (a, _) = table(&mut env).await;
    let late = |point: &'static str, sim: SimClock| {
        let once = Arc::new(AtomicBool::new(false));
        fail::cfg_callback(point, move || {
            if !once.swap(true, Ordering::SeqCst) {
                sim.advance(Duration::from_secs(7200));
            }
        })
        .unwrap();
    };
    late("moraine::append::before_commit", env.sim.clone());
    a.append(vec![batch(&[row("late", "append")])])
        .await
        .unwrap();
    fail::remove("moraine::append::before_commit");
    assert_eq!(deleted(&a), 1);
    env.assert_clean();

    late("moraine::replace::before_commit", env.sim.clone());
    let e = a
        .replace_files(Replace::new().add_batches(vec![batch(&[row("late", "stream")])]))
        .await
        .unwrap_err();
    fail::remove("moraine::replace::before_commit");
    assert!(matches!(e, Error::CommitDeadlineExceeded { .. }), "{e:?}");
    assert_eq!(deleted(&a), 2);
    env.assert_clean();
}
