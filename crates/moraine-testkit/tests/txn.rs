//! Caller transaction ids (D32): a write carrying `(app_id, version)` applies at most once,
//! whichever handle, process or retry sends it, and a replay is told "already applied".

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{AsArray, RecordBatch};
use moraine::expr::{ScalarValue, col, lit};
use moraine::{Committed, Consistency, Database, Error, Table, TableOptions, WriteOutcome};
use moraine_testkit::SimRng;
use moraine_testkit::datagen;
use object_store::memory::InMemory;

async fn db(store: &Arc<InMemory>, consistency: Consistency, linger: Duration) -> Database {
    Database::builder_for_store(store.clone(), "db")
        .consistency(consistency)
        .group_commit_linger(linger)
        .open()
        .await
        .unwrap()
}

fn keyed() -> TableOptions {
    TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
}

async fn table(store: &Arc<InMemory>) -> Table {
    db(store, Consistency::Manual, Duration::ZERO)
        .await
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap()
}

fn ops(batches: &[RecordBatch]) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let op = b.column_by_name("op").unwrap().as_string::<i32>();
            (0..b.num_rows())
                .map(|i| op.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

fn already(o: WriteOutcome) -> i64 {
    match o {
        WriteOutcome::AlreadyApplied { txn_version, .. } => txn_version,
        other => panic!("expected already applied, got {other:?}"),
    }
}

#[tokio::test]
async fn a_replay_is_already_applied() {
    let store = Arc::new(InMemory::new());
    let t = table(&store).await;
    let mut rng = SimRng::new(1);
    let b = datagen::rows(&mut rng, "w1", 3, None, 2);

    assert_eq!(t.last_txn_version("wal").await.unwrap(), None);
    let o = t.txn("wal", 1).append(vec![b.clone()]).await.unwrap();
    assert_eq!(o, WriteOutcome::Committed(Committed::new(1)));
    assert_eq!(t.last_txn_version("wal").await.unwrap(), Some(1));

    // The same id again, and a lower one, write nothing.
    assert_eq!(
        t.txn("wal", 1).append(vec![b.clone()]).await.unwrap(),
        WriteOutcome::AlreadyApplied {
            version: 1,
            txn_version: 1
        }
    );
    assert_eq!(already(t.txn("wal", 0).append(vec![b]).await.unwrap()), 1);
    assert_eq!(t.scan().count().await.unwrap(), 3);
    assert_eq!(t.version(), 1);

    // Ids of different applications are independent; versions may skip.
    let b = datagen::rows(&mut rng, "x5", 1, None, 2);
    assert!(
        t.txn("other", 5)
            .append(vec![b])
            .await
            .unwrap()
            .is_committed()
    );
    let b = datagen::rows(&mut rng, "w3", 1, None, 2);
    assert!(
        t.txn("wal", 3)
            .append(vec![b])
            .await
            .unwrap()
            .is_committed()
    );
    let snap = t.snapshot().await.unwrap();
    assert_eq!(snap.last_txn_version("wal"), Some(3));
    assert_eq!(snap.last_txn_version("other"), Some(5));
    assert_eq!(snap.last_txn_version("none"), None);

    // The log carries one `txn` action per caller id, beside the handle's own.
    let entries = read_log_mem(&store).await;
    let callers: Vec<Vec<(String, i64)>> = entries
        .iter()
        .map(|e| {
            e.iter()
                .filter(|(a, _)| a == "wal" || a == "other")
                .cloned()
                .collect()
        })
        .collect();
    assert_eq!(
        callers,
        vec![
            vec![],
            vec![("wal".to_string(), 1)],
            vec![("other".to_string(), 5)],
            vec![("wal".to_string(), 3)],
        ]
    );
    assert!(entries[1..].iter().all(|e| e.len() == 2));

    // An empty application id is refused.
    assert!(matches!(
        t.txn("", 1).append(vec![]).await,
        Err(Error::InvalidQuery(_))
    ));
}

/// The `txn` actions of every log entry in an in-memory store (no Parquet needed).
async fn read_log_mem(store: &Arc<InMemory>) -> Vec<Vec<(String, i64)>> {
    use futures::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};
    let prefix = object_store::path::Path::from("db/t/data/_delta_log");
    let mut metas: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
    metas.sort_by(|a, b| a.location.cmp(&b.location));
    let mut out = Vec::new();
    for m in metas {
        if !m.location.as_ref().ends_with(".json") {
            continue;
        }
        let body = store.get(&m.location).await.unwrap().bytes().await.unwrap();
        let mut txns = Vec::new();
        for line in std::str::from_utf8(&body).unwrap().lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(t) = v.get("txn") {
                txns.push((
                    t["appId"].as_str().unwrap().to_string(),
                    t["version"].as_i64().unwrap(),
                ));
                // Caller ids carry no lastUpdated, so retention never expires them.
                let caller = t["appId"] == "wal" || t["appId"] == "other";
                assert_eq!(t.get("lastUpdated").is_none_or(|x| x.is_null()), caller);
            }
        }
        out.push(txns);
    }
    out
}

/// Another process, and a handle whose snapshot is older than the commit, both find the id.
#[tokio::test]
async fn other_handles_and_stale_snapshots_see_the_id() {
    let store = Arc::new(InMemory::new());
    let a = table(&store).await;
    // b opens before a commits and never refreshes on its own (Manual).
    let b = db(&store, Consistency::Manual, Duration::ZERO)
        .await
        .open_table("t")
        .await
        .unwrap();
    let mut rng = SimRng::new(2);
    let batch = datagen::rows(&mut rng, "w7", 2, None, 2);
    assert!(
        a.txn("wal", 7)
            .append(vec![batch.clone()])
            .await
            .unwrap()
            .is_committed()
    );
    assert_eq!(b.version(), 0);

    // An append does not refresh first: it loses version 1, reads it, finds the id there.
    let o = b.txn("wal", 7).append(vec![batch.clone()]).await.unwrap();
    assert_eq!(
        o,
        WriteOutcome::AlreadyApplied {
            version: 1,
            txn_version: 7
        }
    );
    // An upsert refreshes first and finds it in the snapshot.
    let c = db(&store, Consistency::Manual, Duration::ZERO)
        .await
        .open_table("t")
        .await
        .unwrap();
    assert_eq!(
        already(c.txn("wal", 6).upsert(vec![batch]).await.unwrap()),
        7
    );
    assert_eq!(a.scan().count().await.unwrap(), 2);

    // A fresh process (a restart) reads the id from the log.
    let d = db(&store, Consistency::Strong, Duration::ZERO)
        .await
        .open_table("t")
        .await
        .unwrap();
    assert_eq!(d.last_txn_version("wal").await.unwrap(), Some(7));
}

/// Every kind of write takes an id, and a write that changes no row still records it.
#[tokio::test]
async fn every_write_kind_records_its_id() {
    let store = Arc::new(InMemory::new());
    let t = table(&store).await;
    let mut rng = SimRng::new(3);
    let rows = datagen::rows(&mut rng, "u1", 4, None, 2);
    let key = |k: &str| vec![ScalarValue::Utf8(Some(k.to_string()))];
    let keys: Vec<String> = {
        let k = rows.column_by_name("k").unwrap().as_string::<i32>();
        (0..rows.num_rows())
            .map(|i| k.value(i).to_string())
            .collect()
    };

    let w = |v| t.txn("wal", v);
    assert_eq!(
        w(1).upsert(vec![rows.clone()]).await.unwrap(),
        WriteOutcome::Committed(Committed::new(1))
    );
    assert_eq!(
        w(2).update(
            col("k").eq(lit(keys[0].as_str())),
            vec![("op".into(), lit("u2"))]
        )
        .await
        .unwrap(),
        WriteOutcome::Committed(Committed::new(2))
    );
    assert_eq!(
        w(3).delete_keys(&[key(&keys[1])]).await.unwrap(),
        WriteOutcome::Committed(Committed::new(3))
    );
    assert_eq!(
        w(4).delete(col("k").eq(lit(keys[2].as_str())))
            .await
            .unwrap(),
        WriteOutcome::Committed(Committed::new(4))
    );
    // Nothing matches, nothing to write: each still commits, so a replay after a later write
    // of those rows cannot apply it.
    assert_eq!(
        w(5).delete(col("k").eq(lit("absent"))).await.unwrap(),
        WriteOutcome::Committed(Committed::new(5))
    );
    assert_eq!(
        w(6).update(col("k").eq(lit("absent")), vec![("op".into(), lit("x"))])
            .await
            .unwrap(),
        WriteOutcome::Committed(Committed::new(6))
    );
    assert_eq!(
        w(7).append(vec![]).await.unwrap(),
        WriteOutcome::Committed(Committed::new(7))
    );
    assert_eq!(
        w(8).delete_keys(&[]).await.unwrap(),
        WriteOutcome::Committed(Committed::new(8))
    );
    assert_eq!(t.last_txn_version("wal").await.unwrap(), Some(8));

    // Replays of each change nothing.
    let before = ops(&t.scan().collect().await.unwrap());
    assert_eq!(already(w(1).upsert(vec![rows]).await.unwrap()), 8);
    assert_eq!(
        already(
            w(2).update(
                col("k").eq(lit(keys[0].as_str())),
                vec![("op".into(), lit("u9"))]
            )
            .await
            .unwrap()
        ),
        8
    );
    assert_eq!(
        already(w(3).delete_keys(&[key(&keys[0])]).await.unwrap()),
        8
    );
    assert_eq!(already(w(4).delete(lit(true)).await.unwrap()), 8);
    assert_eq!(ops(&t.scan().collect().await.unwrap()), before);
    assert_eq!(before.len(), 2);
    assert_eq!(t.version(), 8);
}

/// Several application ids share one group commit; a second write of an application already in
/// the group commits after it, in queue order; the same id twice commits once.
#[tokio::test]
async fn group_commit_with_ids() {
    let store = Arc::new(InMemory::new());
    let a = db(&store, Consistency::Manual, Duration::from_millis(20)).await;
    let t = a
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(4);
    let jobs: Vec<(String, i64, RecordBatch)> = vec![
        ("a".into(), 1, datagen::rows(&mut rng, "a1", 1, None, 2)),
        ("b".into(), 1, datagen::rows(&mut rng, "b1", 1, None, 2)),
        ("a".into(), 2, datagen::rows(&mut rng, "a2", 1, None, 2)),
        ("c".into(), 1, datagen::rows(&mut rng, "c1", 1, None, 2)),
        ("b".into(), 1, datagen::rows(&mut rng, "b1", 1, None, 2)),
    ];
    let results = futures::future::join_all(jobs.into_iter().map(|(app, v, b)| {
        let t = t.clone();
        async move { t.txn(app, v).append(vec![b]).await.unwrap() }
    }))
    .await;
    // Group 1: a1, b1 (a2 has an application already in it). Group 2: a2, c1, and the second
    // b1, found applied before the group runs.
    assert_eq!(
        results,
        vec![
            WriteOutcome::Committed(Committed::new(1)),
            WriteOutcome::Committed(Committed::new(1)),
            WriteOutcome::Committed(Committed::new(2)),
            WriteOutcome::Committed(Committed::new(2)),
            WriteOutcome::AlreadyApplied {
                version: 1,
                txn_version: 1
            },
        ]
    );
    assert_eq!(
        ops(&t.scan().collect().await.unwrap()),
        vec!["a1", "a2", "b1", "c1"]
    );
}

/// An append that loses its version to an entry holding the same id stops, and the rest of its
/// group commits without it.
#[tokio::test]
async fn a_superseded_member_leaves_its_group() {
    let store = Arc::new(InMemory::new());
    let first = table(&store).await;
    let late = db(&store, Consistency::Manual, Duration::from_millis(20))
        .await
        .open_table("t")
        .await
        .unwrap();
    let mut rng = SimRng::new(5);
    let b = datagen::rows(&mut rng, "a1", 1, None, 2);
    let o = first.txn("a", 1).append(vec![b.clone()]).await.unwrap();
    assert_eq!(o, WriteOutcome::Committed(Committed::new(1)));
    // `late` still holds version 0: both its members are grouped, the commit loses version 1,
    // finds a:1 there, and the group commits again with b:1 alone.
    let other = datagen::rows(&mut rng, "b1", 1, None, 2);
    let (x, y) = futures::future::join(
        late.txn("a", 1).append(vec![b]),
        late.txn("b", 1).append(vec![other]),
    )
    .await;
    assert_eq!(
        x.unwrap(),
        WriteOutcome::AlreadyApplied {
            version: 1,
            txn_version: 1
        }
    );
    assert_eq!(y.unwrap(), WriteOutcome::Committed(Committed::new(2)));
    assert_eq!(ops(&late.scan().collect().await.unwrap()), vec!["a1", "b1"]);
}

/// Without ids nothing changes: a write with nothing to do commits nothing.
#[tokio::test]
async fn plain_writes_are_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let d = Database::builder(dir.path().to_str().unwrap())
        .open()
        .await
        .unwrap();
    let t = d
        .create_table("t", datagen::row_schema(), keyed())
        .await
        .unwrap();
    let mut rng = SimRng::new(6);
    assert_eq!(t.append(vec![]).await.unwrap().version, 0);
    let b = datagen::rows(&mut rng, "p", 2, None, 2);
    assert_eq!(t.append(vec![b]).await.unwrap().version, 1);
    assert_eq!(
        t.delete(col("k").eq(lit("absent"))).await.unwrap().version,
        1
    );
}
