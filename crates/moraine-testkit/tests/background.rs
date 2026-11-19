//! Background snapshot refresh (D29, D43): with `Consistency::Background`, a handle
//! picks up other writers' commits by itself and prewarms its caches, so the first query after
//! a commit makes no snapshot request; for a small table, and for full-text and vector
//! searches whose sidecars and footers were prewarmed, it makes no request at all.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use moraine::index::BuildOptions;
use moraine::store::Op;
use moraine::{Consistency, Database, IndexSpec, Table, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::text::{batch, document, schema};
use object_store::memory::InMemory;
use serde_json::json;

const INTERVAL: Duration = Duration::from_millis(20);

async fn open(store: &Arc<InMemory>, c: Consistency, small: u64) -> Database {
    Database::builder_for_store(store.clone(), "db")
        .consistency(c)
        .small_table_bytes(small)
        .open()
        .await
        .unwrap()
}

/// Lets the background task of `t` finish its current round and then sleep for an hour, so
/// the store metrics that follow count the queries' requests alone.
async fn quiet(t: &Table) {
    t.set_consistency(Consistency::Background {
        interval: Duration::from_secs(3600),
    });
    let n = t.refresh_metrics().refreshes.load(Ordering::SeqCst);
    while t.refresh_metrics().refreshes.load(Ordering::SeqCst) == n {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Waits (up to 10 s) until `t` holds version `v` and its prewarm has finished.
async fn until_version(t: &Table, v: u64) {
    for _ in 0..2000 {
        if t.refresh_metrics().prewarmed_version.load(Ordering::SeqCst) >= v {
            assert!(t.snapshot_in_hand().version() >= v);
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("version {v} never arrived");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_query_after_a_commit_makes_no_request() {
    let store = Arc::new(InMemory::new());
    let w = open(&store, Consistency::Strong, 64 << 20).await;
    let tw = w
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let mut rng = SimRng::new(1);
    let docs: Vec<_> = (0..50).map(|_| document(&mut rng, 30, 20)).collect();
    tw.append(vec![batch(0, &docs)]).await.unwrap();
    tw.create_index(
        IndexSpec::new("fts", "fts", &["title", "body"]).policy(json!({"min_rows_for_sidecar": 0})),
    )
    .await
    .unwrap();
    tw.build_indexes(BuildOptions::default()).await.unwrap();

    let r = open(
        &store,
        Consistency::Background { interval: INTERVAL },
        64 << 20,
    )
    .await;
    let tr = r.open_table("t").await.unwrap();
    // Warm everything for the current version.
    assert_eq!(tr.scan().count().await.unwrap(), 50);
    tr.search_text("fts", "glacier").collect().await.unwrap();

    // Another writer appends and builds the new file's sidecar.
    let docs: Vec<_> = (0..30).map(|_| document(&mut rng, 30, 20)).collect();
    tw.build_indexes(BuildOptions::default()).await.unwrap();
    let v = tw.append(vec![batch(50, &docs)]).await.unwrap().version;
    tw.build_indexes(BuildOptions::default()).await.unwrap();
    until_version(&tr, v).await;
    quiet(&tr).await;

    // The first queries at the new version: no request of any kind.
    let before = r.store_metrics().snapshot();
    let snap = tr.snapshot().await.unwrap();
    assert_eq!(snap.version(), v);
    assert_eq!(tr.scan().count().await.unwrap(), 80);
    let hits = tr
        .search_text("fts", "glacier river")
        .limit(5)
        .collect()
        .await
        .unwrap();
    let d = r.store_metrics().snapshot().since(&before);
    assert_eq!(
        d.count(Op::Get) + d.listings() + d.count(Op::Head),
        0,
        "{d}"
    );
    assert_eq!(hits.stats.files, 2);
    // The new file's sidecar may have been uploaded after the background listing; either
    // way its searcher was ready: nothing was indexed during the query.
    assert_eq!(hits.stats.sidecar_files + hits.stats.exact_files, 2);
    assert_eq!(hits.stats.exact_rows, 0, "{:?}", hits.stats);
    let m = tr.refresh_metrics();
    assert!(m.prewarmed_files.load(Ordering::SeqCst) >= 1);
    assert_eq!(m.failures.load(Ordering::SeqCst), 0);
}

/// A large table (no in-memory copy): the first scan after a commit reads only data, never the
/// log or a footer, and full-text files without a sidecar were indexed in the background.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prewarm_fetches_footers_and_builds_text_indexes() {
    let store = Arc::new(InMemory::new());
    let w = open(&store, Consistency::Strong, 0).await;
    let tw = w
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let mut rng = SimRng::new(2);
    let docs: Vec<_> = (0..40).map(|_| document(&mut rng, 30, 20)).collect();
    tw.append(vec![batch(0, &docs)]).await.unwrap();
    tw.create_index(IndexSpec::new("fts", "fts", &["title", "body"]))
        .await
        .unwrap();
    let r = open(&store, Consistency::Background { interval: INTERVAL }, 0).await;
    let tr = r.open_table("t").await.unwrap();
    tr.search_text("fts", "glacier").collect().await.unwrap();
    let docs: Vec<_> = (0..40).map(|_| document(&mut rng, 30, 20)).collect();
    let v = tw.append(vec![batch(40, &docs)]).await.unwrap().version;
    until_version(&tr, v).await;
    quiet(&tr).await;
    assert_eq!(r.cache_usage().fts_indexes, 2);
    let before = r.store_metrics().snapshot();
    let hits = tr
        .search_text("fts", "glacier")
        .limit(100)
        .collect()
        .await
        .unwrap();
    let text = r.store_metrics().snapshot().since(&before);
    assert_eq!(hits.stats.exact_rows, 0, "{:?}", hits.stats);
    let before = r.store_metrics().snapshot();
    assert_eq!(tr.scan().count().await.unwrap(), 80);
    let scan = r.store_metrics().snapshot().since(&before);
    // The text search fetched only its hits' rows; the scan only data pages.
    assert_eq!(text.listings() + scan.listings(), 0, "{text} / {scan}");
    let files_read = scan.count(Op::GetRange) + scan.count(Op::Get);
    assert!(files_read <= 2 * 2, "{scan}");
}

/// Switching the mode stops the task; handles dropped end it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mode_changes_stop_the_task() {
    let store = Arc::new(InMemory::new());
    let w = open(&store, Consistency::Strong, 64 << 20).await;
    let tw = w
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap();
    let r = open(&store, Consistency::Manual, 64 << 20).await;
    let tr = r.open_table("t").await.unwrap();
    tr.set_consistency(Consistency::Background { interval: INTERVAL });
    let mut rng = SimRng::new(3);
    let v = tw
        .append(vec![batch(0, &[document(&mut rng, 10, 5)])])
        .await
        .unwrap()
        .version;
    until_version(&tr, v).await;
    tr.set_consistency(Consistency::Manual);
    tokio::time::sleep(INTERVAL * 5).await;
    let n = tr.refresh_metrics().refreshes.load(Ordering::SeqCst);
    let v2 = tw
        .append(vec![batch(1, &[document(&mut rng, 10, 5)])])
        .await
        .unwrap()
        .version;
    tokio::time::sleep(INTERVAL * 5).await;
    assert_eq!(tr.refresh_metrics().refreshes.load(Ordering::SeqCst), n);
    assert_eq!(tr.snapshot_in_hand().version(), v);
    assert_eq!(tr.refresh().await.unwrap(), v2);
}
