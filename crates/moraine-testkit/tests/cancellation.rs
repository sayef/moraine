//! Cancellation safety of the group-commit queue (D56): a write future dropped while it leads
//! (during the linger, before it takes its group) or while it waits in the queue used to leave
//! the queue marked busy with nobody to lead, so every later write on the handle hung. The
//! simulation scenario `cancelled_writes` in `tests/cancel_sim.rs` covers the same at random
//! points. Since D64 the group commits on a task owned by the handle, so cancelling the caller
//! whose write started it fails nobody else.

use std::sync::Arc;
use std::time::Duration;

use moraine::{Database, TableOptions};
use object_store::memory::InMemory;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_dropped_during_linger() {
    let db = Database::builder_for_store(Arc::new(InMemory::new()), "db")
        .group_commit_linger(Duration::from_millis(500))
        .open()
        .await
        .unwrap();
    let t = db
        .create_table(
            "t",
            moraine_testkit::datagen::row_schema(),
            TableOptions::default(),
        )
        .await
        .unwrap();
    let rows = moraine_testkit::golden::table_rows("a", 0..3);
    // The leader's future is dropped while it lingers, before it takes its group.
    let r = tokio::time::timeout(Duration::from_millis(20), t.append(vec![rows.clone()])).await;
    assert!(r.is_err());
    let r = tokio::time::timeout(Duration::from_secs(5), t.append(vec![rows])).await;
    assert!(r.is_ok(), "the next write on the handle hangs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiter_dropped_in_queue() {
    let db = Database::builder_for_store(Arc::new(InMemory::new()), "db")
        .group_commit_linger(Duration::from_millis(300))
        .open()
        .await
        .unwrap();
    let t = db
        .create_table(
            "t",
            moraine_testkit::datagen::row_schema(),
            TableOptions::default(),
        )
        .await
        .unwrap();
    let a = moraine_testkit::golden::table_rows("a", 0..3);
    // An update is a commit of its own, so it waits behind the leader's append group.
    let t1 = t.clone();
    let a1 = a.clone();
    let leader = tokio::spawn(async move { t1.append(vec![a1]).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let r = tokio::time::timeout(
        Duration::from_millis(20),
        t.update(
            moraine::expr::lit(true),
            vec![("op".into(), moraine::expr::lit("x"))],
        ),
    )
    .await;
    assert!(r.is_err());
    leader.await.unwrap().unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), t.append(vec![a])).await;
    assert!(r.is_ok(), "the next write on the handle hangs");
}

/// D56's remaining limit, removed by D64: the caller whose write started the group commit is
/// aborted after the commit took the group. The commit runs on the handle's task, so it goes
/// on, and every member of the group, the aborted caller's write included, commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_caller_aborted_after_the_group_was_taken() {
    let db = Database::builder_for_store(Arc::new(InMemory::new()), "db")
        .group_commit_linger(Duration::from_millis(50))
        .open()
        .await
        .unwrap();
    let t = db
        .create_table(
            "first-caller",
            moraine_testkit::datagen::row_schema(),
            TableOptions::default(),
        )
        .await
        .unwrap();
    // Holds the group between upload and log put long enough to abort its first caller.
    fail::cfg("moraine::append::before_commit", "sleep(400)").unwrap();
    let (ta, tb) = (t.clone(), t.clone());
    let a = tokio::spawn(async move {
        ta.append(vec![moraine_testkit::golden::table_rows("a", 0..3)])
            .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let b = tokio::spawn(async move {
        tb.append(vec![moraine_testkit::golden::table_rows("b", 0..4)])
            .await
    });
    // Both are queued within the linger; the group is taken at 50 ms and held at the put. The
    // failpoint blocks a worker thread, which can hold up the runtime's timers, so this waits
    // on the test's own thread.
    std::thread::sleep(Duration::from_millis(200));
    a.abort();
    let rb = tokio::time::timeout(Duration::from_secs(10), b).await;
    fail::remove("moraine::append::before_commit");
    let v = rb
        .expect("the group's other caller hangs")
        .unwrap()
        .unwrap()
        .version;
    assert!(a.await.unwrap_err().is_cancelled());
    let snap = t.snapshot().await.unwrap();
    assert_eq!(snap.version(), v);
    assert_eq!(snap.num_rows(), 7, "both writes of the group commit");
}
