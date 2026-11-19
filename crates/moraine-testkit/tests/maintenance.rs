//! Maintenance, job by job, on a store with simulated server time: checkpoints and
//! log cleanup (cases L1, L2), tiered compaction (P1, P3, D48), leases and fencing (D29),
//! pins (D39), vacuum and sidecar collection (G1, G2, G5; scenario 6), file replaces, queued
//! writes (D51), and verify and repair.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use moraine::expr::ScalarValue;
use moraine::store::Op;
use moraine::{
    CompactionPolicy, Consistency, Database, Error, MaintainOptions, Replace, Table, TableOptions,
};
use moraine_testkit::{ClientClock, SimClock, SimRng, SimStore, datagen};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

const HOUR: Duration = Duration::from_secs(3600);

struct Env {
    sim: SimClock,
    store: Arc<SimStore>,
    seed: u64,
}

impl Env {
    fn new() -> Env {
        // Distinct seeds per test: tests run in parallel and share the build directory, where
        // names come from the clients' random numbers.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let sim = SimClock::new();
        Env {
            store: Arc::new(SimStore::new(sim.clone())),
            sim,
            seed: NEXT.fetch_add(1000, std::sync::atomic::Ordering::Relaxed),
        }
    }

    async fn db(&mut self) -> Database {
        self.db_with(Consistency::Strong, 0).await
    }

    async fn db_with(&mut self, c: Consistency, skew_ms: i64) -> Database {
        self.seed += 1;
        let clock = ClientClock::new(self.sim.clone(), None, self.seed as usize, self.seed)
            .with_skew(skew_ms);
        let store: Arc<dyn ObjectStore> = self.store.clone();
        Database::builder_for_store(store, "db")
            .clock(Arc::new(clock))
            .consistency(c)
            .small_table_bytes(0)
            .open()
            .await
            .unwrap()
    }

    fn objects(&self, prefix: &str) -> Vec<String> {
        self.store
            .snapshot()
            .into_keys()
            .filter(|k| k.starts_with(prefix))
            .collect()
    }
}

fn keyed() -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
        .cluster_by(["g"])
}

/// Every row as (k, op, i, g), sorted.
async fn rows_of(batches: &[RecordBatch]) -> Vec<(String, String, i64, Option<i64>)> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column_by_name("k").unwrap().as_string::<i32>();
        let op = b.column_by_name("op").unwrap().as_string::<i32>();
        let i = b.column_by_name("i").unwrap().as_primitive::<Int64Type>();
        let g = b.column_by_name("g").unwrap().as_primitive::<Int64Type>();
        for r in 0..b.num_rows() {
            out.push((
                k.value(r).to_string(),
                op.value(r).to_string(),
                i.value(r),
                (!arrow::array::Array::is_null(g, r)).then(|| g.value(r)),
            ));
        }
    }
    out.sort();
    out
}

async fn all_rows(t: &Table) -> Vec<(String, String, i64, Option<i64>)> {
    rows_of(&t.scan().collect().await.unwrap()).await
}

fn no_age() -> CompactionPolicy {
    CompactionPolicy::default().min_file_age(Duration::ZERO)
}

/// Case L1/L2: a checkpointed table opens from the checkpoint and the entries after it, and the
/// file levels and tags written before the checkpoint survive it.
#[tokio::test]
async fn opening_a_checkpointed_table_reads_only_what_follows() {
    let mut env = Env::new();
    let db = env.db().await;
    let t = db
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(7);
    for n in 0..24 {
        t.append(vec![datagen::rows(&mut rng, &format!("a{n}"), 3, None, 3)])
            .await
            .unwrap();
    }
    t.compact(&CompactionPolicy::full()).await.unwrap();
    let levels: Vec<u32> = t
        .snapshot()
        .await
        .unwrap()
        .files()
        .iter()
        .map(|f| f.level)
        .collect();
    assert!(levels.iter().all(|l| *l >= 1), "{levels:?}");
    let cp = t.checkpoint().await.unwrap().expect("a checkpoint");
    assert_eq!(cp, t.version());
    assert_eq!(t.checkpoint().await.unwrap(), None, "one per version");
    for n in 0..2 {
        t.append(vec![datagen::rows(&mut rng, &format!("b{n}"), 3, None, 3)])
            .await
            .unwrap();
    }
    let want = all_rows(&t).await;
    let want_files = t.snapshot().await.unwrap().files();

    let fresh = env.db().await;
    let before = fresh.store_metrics().snapshot();
    let t2 = fresh.open_table("t").await.unwrap();
    let used = fresh.store_metrics().snapshot().since(&before);
    // `_last_checkpoint`, the checkpoint, and the two entries after it; one listing that starts
    // at the checkpoint.
    assert_eq!(used.count(Op::Get), 4, "{used:?}");
    assert_eq!(used.listings(), 1, "{used:?}");
    assert_eq!(t2.version(), cp + 2);
    let files = t2.snapshot_in_hand().files();
    let strip = |f: &moraine::SnapshotFile| (f.path.clone(), f.level, f.tags.clone(), f.rows);
    assert_eq!(
        files.iter().map(strip).collect::<Vec<_>>(),
        want_files.iter().map(strip).collect::<Vec<_>>()
    );
    assert_eq!(all_rows(&t2).await, want);
}

/// Compaction picks adjacent files by level, keeps every row and the order of equal keys, and
/// the levels survive a reload (an acceptance check of maintenance).
#[tokio::test]
async fn compaction_is_tiered_and_keeps_rows_and_order() {
    let mut env = Env::new();
    let db = env.db().await;
    let t = db
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(3);
    let policy = no_age();
    // Keys repeat across appends; the newest append of a key must stay the one `get` returns.
    let mut last: BTreeMap<String, String> = BTreeMap::new();
    for round in 0..4 {
        for n in 0..4 {
            let op = format!("r{round}n{n}");
            let b = datagen::rows(&mut rng, &op, 4, Some(5), 2);
            let k = b.column_by_name("k").unwrap().as_string::<i32>();
            for r in 0..b.num_rows() {
                last.insert(k.value(r).to_string(), op.clone());
            }
            t.append(vec![b]).await.unwrap();
        }
        let before = all_rows(&t).await;
        let r = t.compact(&policy).await.unwrap();
        assert!(r.steps >= 1, "round {round}: {r:?}");
        assert_eq!(all_rows(&t).await, before, "P1: rows unchanged");
        let levels: Vec<u32> = t
            .snapshot()
            .await
            .unwrap()
            .files()
            .iter()
            .map(|f| f.level)
            .collect();
        match round {
            0 => assert_eq!(levels, vec![1]),
            1 => assert_eq!(levels, vec![1, 1]),
            2 => assert_eq!(levels, vec![1, 1, 1]),
            // Four level-1 files make a run: one level-2 file.
            _ => assert_eq!(levels, vec![2]),
        }
    }
    for (k, op) in &last {
        let row = t
            .get(&[ScalarValue::Utf8(Some(k.clone()))])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.column_by_name("op")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            op
        );
    }
    // Nothing more is due.
    assert_eq!(t.compact(&policy).await.unwrap().steps, 0);
    let fresh = env.db().await.open_table("t").await.unwrap();
    let levels: Vec<u32> = fresh
        .snapshot_in_hand()
        .files()
        .iter()
        .map(|f| f.level)
        .collect();
    assert_eq!(levels, vec![2], "levels survive a reload");
    // Files younger than min_file_age are left alone (case P2).
    t.append(vec![datagen::rows(&mut rng, "young", 2, None, 2)])
        .await
        .unwrap();
    t.append(vec![datagen::rows(&mut rng, "young2", 2, None, 2)])
        .await
        .unwrap();
    let r = t
        .compact(&CompactionPolicy::full().min_file_age(HOUR))
        .await
        .unwrap();
    assert_eq!(r.steps, 0);
}

/// A full compaction merges files whose writer's wall clock runs ahead of the store's (D91):
/// with no minimum age, a file stamped after server time is not too young to merge.
#[tokio::test]
async fn full_compaction_merges_files_stamped_ahead_of_server_time() {
    let mut env = Env::new();
    let db = env.db_with(Consistency::Strong, 5_000).await;
    let t = db
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(11);
    for n in 0..5 {
        t.append(vec![datagen::rows(&mut rng, &format!("a{n}"), 3, None, 3)])
            .await
            .unwrap();
    }
    let before = all_rows(&t).await;
    let r = t.compact(&CompactionPolicy::full()).await.unwrap();
    assert!(r.steps >= 1, "{r:?}");
    let levels: Vec<u32> = t
        .snapshot_in_hand()
        .files()
        .iter()
        .map(|f| f.level)
        .collect();
    assert_eq!(levels, vec![1]);
    assert_eq!(all_rows(&t).await, before);
    // A minimum age still holds the same files back.
    t.append(vec![datagen::rows(&mut rng, "b0", 3, None, 3)])
        .await
        .unwrap();
    t.append(vec![datagen::rows(&mut rng, "b1", 3, None, 3)])
        .await
        .unwrap();
    let r = t
        .compact(&CompactionPolicy::full().min_file_age(Duration::from_millis(1)))
        .await
        .unwrap();
    assert_eq!(r.steps, 0);
}

/// A maintenance commit from a holder whose lease generation was superseded is refused.
#[tokio::test]
async fn fencing_refuses_a_superseded_generation() {
    let mut env = Env::new();
    let a = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let b = env.db().await.open_table("t").await.unwrap();
    let mut rng = SimRng::new(5);
    for n in 0..4 {
        a.append(vec![datagen::rows(&mut rng, &format!("w{n}"), 2, None, 2)])
            .await
            .unwrap();
    }
    let ttl = Duration::from_secs(60);
    let la = a.try_lease("fold", ttl).await.unwrap().expect("free");
    assert!(b.try_lease("fold", ttl).await.unwrap().is_none(), "held");
    // A's lease runs out without A noticing; B takes it over.
    env.sim.advance(Duration::from_secs(120));
    let lb = b.try_lease("fold", ttl).await.unwrap().expect("expired");
    assert!(lb.generation > la.generation);
    // Before B commits anything, A's commit is still accepted (the lease is a hint).
    let files: Vec<String> = a
        .snapshot()
        .await
        .unwrap()
        .files()
        .into_iter()
        .map(|f| f.path)
        .collect();
    a.replace_files(Replace::new().remove(files[0].clone()).fenced(&la))
        .await
        .unwrap();
    b.replace_files(Replace::new().remove(files[1].clone()).fenced(&lb))
        .await
        .unwrap();
    let e = a
        .replace_files(Replace::new().remove(files[2].clone()).fenced(&la))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Fenced { .. }), "{e:?}");
    // The refused commit wrote nothing.
    assert!(
        a.snapshot()
            .await
            .unwrap()
            .files()
            .iter()
            .any(|f| f.path == files[2])
    );
    let mut la = la;
    assert!(!a.renew_lease(&mut la, ttl).await.unwrap(), "lost");
    b.release_lease(&lb).await.unwrap();
    assert!(
        a.try_lease("fold", ttl).await.unwrap().is_some(),
        "released"
    );
}

/// Case G1: leases expire by server time, whatever the clients' clocks say.
#[tokio::test]
async fn leases_expire_on_server_time() {
    let mut env = Env::new();
    let a = env
        .db_with(Consistency::Strong, -3_600_000)
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let b = env
        .db_with(Consistency::Strong, 7_200_000)
        .await
        .open_table("t")
        .await
        .unwrap();
    let ttl = Duration::from_secs(300);
    let mut la = a.try_lease("compact", ttl).await.unwrap().unwrap();
    assert!(b.try_lease("compact", ttl).await.unwrap().is_none());
    env.sim.advance(Duration::from_secs(200));
    assert!(a.renew_lease(&mut la, ttl).await.unwrap());
    env.sim.advance(Duration::from_secs(200));
    assert!(
        b.try_lease("compact", ttl).await.unwrap().is_none(),
        "renewed"
    );
    env.sim.advance(Duration::from_secs(200));
    assert!(
        b.try_lease("compact", ttl).await.unwrap().is_some(),
        "expired"
    );
    // Old generations are removed by the next holder.
    assert_eq!(env.objects("db/t/meta/leases/compact/").len(), 1);
}

/// Pins (D39): a pinned version stays readable after vacuum; after `unpin` vacuum collects
/// its files.
#[tokio::test]
async fn pinned_versions_survive_vacuum() {
    let mut env = Env::new();
    let t = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(9);
    for n in 0..3 {
        t.append(vec![datagen::rows(&mut rng, &format!("p{n}"), 3, None, 2)])
            .await
            .unwrap();
    }
    let pinned_rows = all_rows(&t).await;
    let v = t.version();
    t.pin("v1", v).await.unwrap();
    assert_eq!(t.pins().unwrap(), BTreeMap::from([("v1".to_string(), v)]));
    t.compact(&CompactionPolicy::full()).await.unwrap();
    t.delete(moraine::expr::col("g").eq(moraine::expr::lit(0i64)))
        .await
        .unwrap();
    env.sim.advance(2 * HOUR);
    let r = t.vacuum().await.unwrap();
    assert_eq!(
        r.files_deleted, 1,
        "the compacted file the delete replaced: {r:?}"
    );
    assert_eq!(r.kept_for_pins, 3);
    let old = t.snapshot_at_pin("v1").await.unwrap();
    assert_eq!(old.version(), v);
    assert_eq!(
        rows_of(&old.scan().collect().await.unwrap()).await,
        pinned_rows
    );
    // Another handle reads the pin too.
    let other = env.db().await.open_table("t").await.unwrap();
    assert_eq!(
        rows_of(
            &other
                .snapshot_at_pin("v1")
                .await
                .unwrap()
                .scan()
                .collect()
                .await
                .unwrap()
        )
        .await,
        pinned_rows
    );
    t.unpin("v1").await.unwrap();
    assert!(matches!(
        t.snapshot_at_pin("v1").await,
        Err(Error::PinNotFound(_))
    ));
    let r = t.vacuum().await.unwrap();
    assert_eq!(
        (r.files_deleted, r.meta_objects_deleted),
        (3, 1),
        "files and manifest: {r:?}"
    );
    // Versions too old to pin are refused.
    assert!(matches!(
        t.pin("old", 1).await,
        Err(Error::VersionNotAvailable(1))
    ));
}

/// Scenario 6 (`reader_survives_vacuum`, case G2): a reader on a snapshot older than the grace
/// period restarts once at the newest version; an explicit snapshot reports SnapshotExpired.
#[tokio::test]
async fn reader_survives_vacuum() {
    let mut env = Env::new();
    let w = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(11);
    for n in 0..3 {
        w.append(vec![datagen::rows(&mut rng, &format!("r{n}"), 3, None, 2)])
            .await
            .unwrap();
    }
    let reader = env
        .db_with(Consistency::Manual, 0)
        .await
        .open_table("t")
        .await
        .unwrap();
    let held = reader.snapshot().await.unwrap();
    let want = all_rows(&w).await;
    w.compact(&CompactionPolicy::full()).await.unwrap();
    env.sim.advance(2 * HOUR);
    let r = w.vacuum().await.unwrap();
    assert_eq!(r.files_deleted, 3);
    // The handle's query restarts at the newest version once and succeeds.
    assert_eq!(reader.version(), held.version());
    assert_eq!(all_rows(&reader).await, want);
    assert!(reader.version() > held.version());
    let e = held.scan().collect().await.unwrap_err();
    assert!(matches!(e, Error::SnapshotExpired), "{e:?}");
}

/// Vacuum ages orphans by server time and keeps what recent snapshots need; sidecars of
/// removed files go after the grace period too.
#[tokio::test]
async fn vacuum_and_sidecar_collection_follow_the_grace_period() {
    let mut env = Env::new();
    let db = env.db().await;
    let t = db
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(13);
    for n in 0..4 {
        t.append(vec![datagen::rows(&mut rng, &format!("s{n}"), 50, None, 3)])
            .await
            .unwrap();
    }
    // Key-index sidecars for the small files, from the builder.
    t.build_indexes(Default::default()).await.unwrap();
    let sidecars = env.objects("db/t/idx/").len();
    assert_eq!(sidecars, 4);
    // An upload that never committed.
    let orphan = Path::from("db/t/data/part-orphan.parquet");
    env.store
        .put(&orphan, PutPayload::from_static(b"PAR1"))
        .await
        .unwrap();
    t.compact(&no_age()).await.unwrap();
    // The compacted file got its sidecar before its commit (case P3, D48).
    assert_eq!(env.objects("db/t/idx/").len(), 5);
    let r = t.vacuum().await.unwrap();
    assert_eq!(
        (r.files_deleted, r.sidecars_deleted),
        (0, 0),
        "all young: {r:?}"
    );
    env.sim.advance(HOUR + Duration::from_secs(1));
    let r = t.vacuum().await.unwrap();
    assert_eq!(r.files_deleted, 5, "four compacted away, one orphan: {r:?}");
    assert_eq!(r.sidecars_deleted, 4, "{r:?}");
    assert_eq!(
        env.objects("db/t/data/")
            .iter()
            .filter(|p| p.ends_with(".parquet"))
            .count(),
        1
    );
    assert_eq!(env.objects("db/t/idx/").len(), 1);
    // Collectors racing: a second pass finds nothing and fails on nothing (case G3).
    let r = t.vacuum().await.unwrap();
    assert_eq!(r.files_deleted + r.sidecars_deleted, 0);
    assert_eq!(all_rows(&t).await.len(), 200);
}

/// Log cleanup removes entries older than the grace period behind a checkpoint, and the
/// table still opens.
#[tokio::test]
async fn log_cleanup_keeps_what_the_grace_period_needs() {
    let mut env = Env::new();
    let t = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(17);
    for n in 0..45 {
        t.append(vec![datagen::rows(&mut rng, &format!("c{n}"), 2, None, 2)])
            .await
            .unwrap();
    }
    let opts = MaintainOptions::default()
        .compaction(None)
        .build_indexes(None)
        .vacuum(false);
    let r = t.maintain(&opts).await.unwrap();
    assert_eq!(r.log.as_ref().unwrap().checkpoint, Some(45));
    assert_eq!(r.log.unwrap().objects_removed, 0, "nothing old enough");
    env.sim.advance(2 * HOUR);
    t.append(vec![datagen::rows(&mut rng, "late", 2, None, 2)])
        .await
        .unwrap();
    let r = t.maintain(&opts).await.unwrap();
    assert_eq!(r.log.unwrap().objects_removed, 45, "entries 0 to 44");
    let log = env.objects("db/t/data/_delta_log/");
    assert!(
        log.iter()
            .any(|p| p.ends_with("00000000000000000045.checkpoint.parquet"))
    );
    assert!(!log.iter().any(|p| p.ends_with("00000000000000000044.json")));
    let fresh = env.db().await.open_table("t").await.unwrap();
    assert_eq!(fresh.version(), 46);
    assert_eq!(all_rows(&fresh).await, all_rows(&t).await);
    // A handle that slept through the cleanup lists the log and reloads (D25).
    assert_eq!(t.refresh().await.unwrap(), 46);
}

/// An atomic replace conflicts only on the files it removes; tags come back on the files.
#[tokio::test]
async fn replace_files_is_atomic_and_conflict_checked() {
    let mut env = Env::new();
    let a = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let b = env.db().await.open_table("t").await.unwrap();
    let mut rng = SimRng::new(19);
    for n in 0..3 {
        a.append(vec![datagen::rows(&mut rng, &format!("x{n}"), 3, None, 2)])
            .await
            .unwrap();
    }
    let files: Vec<String> = a
        .snapshot()
        .await
        .unwrap()
        .files()
        .into_iter()
        .map(|f| f.path)
        .collect();
    let folded = datagen::rows(&mut rng, "folded", 2, None, 2);
    a.replace_files(
        Replace::new()
            .remove_all(files[..2].to_vec())
            .add_rows(vec![folded])
            .tag("kind", "stats"),
    )
    .await
    .unwrap();
    // B planned against the older version: one of its files is gone.
    let e = b
        .replace_files(Replace::new().remove(files[1].clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::FilesChanged(ref p) if *p == files[1]),
        "{e:?}"
    );
    // A replace of other files commits after other writers' appends.
    b.append(vec![datagen::rows(&mut rng, "late", 1, None, 2)])
        .await
        .unwrap();
    a.replace_files(Replace::new().remove(files[2].clone()))
        .await
        .unwrap();
    let snap = a.snapshot().await.unwrap();
    let tagged: Vec<_> = snap
        .files()
        .into_iter()
        .filter(|f| f.tags.contains_key("kind"))
        .collect();
    assert_eq!(tagged.len(), 1);
    let ops: Vec<String> = all_rows(&a).await.into_iter().map(|r| r.1).collect();
    assert_eq!(ops, vec!["folded", "folded", "late"]);
    assert!(matches!(
        a.replace_files(Replace::new().tag("moraine.level", "3"))
            .await,
        Err(Error::InvalidQuery(_))
    ));
}

/// Queued writes from several handles land exactly once, in each handle's order, whoever
/// ingests and however often (D51).
#[tokio::test]
async fn queued_writes_land_exactly_once() {
    let mut env = Env::new();
    let t0 = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut handles = vec![t0.clone()];
    for _ in 0..2 {
        handles.push(env.db().await.open_table("t").await.unwrap());
    }
    let mut rng = SimRng::new(23);
    let mut tickets = Vec::new();
    for round in 0..4 {
        for (h, t) in handles.iter().enumerate() {
            let b = datagen::rows(&mut rng, &format!("h{h}r{round}"), 2, None, 2);
            tickets.push(t.submit(vec![b]).await.unwrap());
        }
        if round == 1 {
            // Two ingestors without leases at once: each batch still lands once.
            let (x, y) = futures::join!(handles[1].ingest(false), handles[2].ingest(false));
            let (x, y) = (x.unwrap(), y.unwrap());
            assert_eq!(x.batches + y.batches, 6, "{x:?} {y:?}");
        }
    }
    assert!(!t0.is_ingested(tickets.last().unwrap()).await.unwrap());
    let r = t0.ingest(true).await.unwrap();
    assert_eq!(r.batches, 6);
    for s in &tickets {
        assert!(t0.is_ingested(s).await.unwrap());
    }
    let r = t0.ingest(true).await.unwrap();
    assert_eq!((r.batches, r.version), (0, None));
    let rows = all_rows(&t0).await;
    assert_eq!(rows.len(), 24);
    assert!(
        env.objects("db/t/meta/inbox/").is_empty(),
        "committed batches are deleted"
    );
}

/// Verify reports a missing file; repair removes it and reports the rows lost.
#[tokio::test]
async fn verify_and_repair() {
    let mut env = Env::new();
    let t = env
        .db()
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(29);
    for n in 0..2 {
        t.append(vec![datagen::rows(&mut rng, &format!("v{n}"), 5, None, 2)])
            .await
            .unwrap();
    }
    t.build_indexes(Default::default()).await.unwrap();
    let r = t.verify().await.unwrap();
    assert!(r.is_ok(), "{r:?}");
    assert_eq!((r.files_checked, r.sidecars_checked), (2, 2));
    let lost = t.snapshot().await.unwrap().files()[0].path.clone();
    env.store
        .delete(&Path::from(format!("db/t/data/{lost}")))
        .await
        .unwrap();
    let r = t.verify().await.unwrap();
    assert_eq!(r.missing_files, vec![lost.clone()]);
    let r = t.repair().await.unwrap();
    assert_eq!((r.files_removed, r.rows_lost), (vec![lost], 5));
    assert!(t.verify().await.unwrap().is_ok());
    assert_eq!(all_rows(&t).await.len(), 5);
}

/// A step of a table whose merged files get sidecars rebuilt from rows takes no more rows than
/// its builds can finish in half the commit deadline (D81): with no build measured yet, 1,000
/// rows a second over 75 s of the 150 s deadline of a 600 s grace period, so 75,000 rows; a
/// table without such an index merges the whole run in one step.
#[tokio::test]
async fn compaction_steps_fit_the_build_into_the_deadline() {
    use moraine::IndexSpec;
    use moraine_testkit::vectors;
    let mut env = Env::new();
    let db = env.db().await;
    let opts = TableOptions::default().grace_period(Duration::from_secs(600));
    let c = vectors::centres(1, 4, 4);
    let mut rng = SimRng::new(2);
    for (name, indexed) in [("plain", false), ("indexed", true)] {
        let t = db
            .create_table(name, vectors::schema(), opts.clone())
            .await
            .unwrap();
        if indexed {
            t.create_index(
                IndexSpec::new("emb", "vec-hnsw", &["v"])
                    .params(serde_json::json!({"dim": 4}))
                    .policy(serde_json::json!({"min_rows_for_ann": 1_000_000})),
            )
            .await
            .unwrap();
        }
        for i in 0..4 {
            let v: Vec<Option<Vec<f32>>> = vectors::clustered(&mut rng, &c, 40_000, 0.4)
                .into_iter()
                .map(Some)
                .collect();
            t.append(vec![vectors::batch(i * 40_000, &v)])
                .await
                .unwrap();
        }
        let r = t
            .compact(&CompactionPolicy::full().max_steps(1))
            .await
            .unwrap();
        assert_eq!(r.steps, 1, "{name}: {r:?}");
        assert_eq!(
            r.files_removed,
            if indexed { 2 } else { 4 },
            "{name}: {r:?}"
        );
    }
}
