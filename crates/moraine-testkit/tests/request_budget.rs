//! D18's request budget, asserted exactly on small fixed scenarios: an append commit, a commit
//! that lost its version to another writer, a group commit, and snapshot refreshes. A change that
//! adds a request to any of these paths fails here. Each scenario runs with and without object
//! tags (D29), which must add no request, and the tagged runs check that every write carried them.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, TagSet,
};

use moraine::store::{MetricsSnapshot, Op};
use moraine::{Consistency, Database, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::datagen;
use object_store::memory::InMemory;

const TAGS: [(&str, &str); 2] = [("team", "skills"), ("env", "test")];

/// A database on `store`, recording every write's tags in `seen` when `tags` is set.
async fn db(store: &Arc<InMemory>, linger: Duration, seen: Option<&Arc<TagLog>>) -> Database {
    let backend: Arc<dyn ObjectStore> = match seen {
        Some(log) => Arc::new(TagRecorder {
            inner: store.clone(),
            log: log.clone(),
        }),
        None => store.clone(),
    };
    let b = Database::builder_for_store(backend, "db")
        .consistency(Consistency::Manual)
        .group_commit_linger(linger);
    let b = if seen.is_some() {
        b.object_tags(TAGS)
    } else {
        b
    };
    b.open().await.unwrap()
}

type TagLog = Mutex<Vec<(String, String)>>;

/// Records the encoded tags of every put and multipart upload that reaches the store.
#[derive(Debug)]
struct TagRecorder {
    inner: Arc<InMemory>,
    log: Arc<TagLog>,
}

impl std::fmt::Display for TagRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TagRecorder")
    }
}

impl TagRecorder {
    fn record(&self, location: &Path, tags: &TagSet) {
        let mut g = self.log.lock().unwrap();
        g.push((location.to_string(), tags.encoded().to_string()));
    }
}

#[async_trait]
impl ObjectStore for TagRecorder {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.record(location, &opts.tags);
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.record(location, &opts.tags);
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Every write recorded carried exactly the configured tags.
fn assert_all_tagged(log: &TagLog) {
    let mut want = TagSet::default();
    for (k, v) in TAGS {
        want.push(k, v);
    }
    let g = log.lock().unwrap();
    assert!(!g.is_empty());
    for (path, tags) in g.iter() {
        assert_eq!(tags, want.encoded(), "write to {path} without the tags");
    }
}

fn counts(m: &MetricsSnapshot) -> Vec<(&'static str, u64)> {
    Op::all()
        .iter()
        .filter(|o| m.count(**o) > 0)
        .map(|o| (o.name(), m.count(*o)))
        .collect()
}

#[tokio::test]
async fn append_refresh_and_conflict_request_counts() {
    append_refresh_and_conflict(None).await;
}

#[tokio::test]
async fn append_refresh_and_conflict_request_counts_with_tags() {
    let log = Arc::new(TagLog::default());
    append_refresh_and_conflict(Some(&log)).await;
    assert_all_tagged(&log);
}

async fn append_refresh_and_conflict(tags: Option<&Arc<TagLog>>) {
    let store = Arc::new(InMemory::new());
    let a = db(&store, Duration::ZERO, tags).await;
    let b = db(&store, Duration::ZERO, tags).await;
    let opts = TableOptions::default()
        .primary_key(["k"])
        .enforce_primary_key(false)
        .cluster_by(["g"]);
    let ta = a
        .create_table("t", datagen::row_schema(), opts.clone())
        .await
        .unwrap();
    let tb = b.open_table("t").await.unwrap();
    let mut rng = SimRng::new(7);
    // Warm b's capability probe, which happens once per handle.
    tb.append(vec![datagen::rows(&mut rng, "warm", 2, None, 3)])
        .await
        .unwrap();
    ta.refresh().await.unwrap();

    // An append with a current snapshot: one data PUT, one conditional log PUT.
    let before = a.store_metrics().snapshot();
    let v = ta
        .append(vec![datagen::rows(&mut rng, "a1", 5, None, 3)])
        .await
        .unwrap()
        .version;
    let d = a.store_metrics().snapshot().since(&before);
    assert_eq!(
        counts(&d),
        vec![("put", 1), ("put_create", 1)],
        "append: {d}"
    );
    assert_eq!(v, 2);

    // A refresh with nothing new: one GET that finds no entry.
    let before = a.store_metrics().snapshot();
    ta.refresh().await.unwrap();
    let d = a.store_metrics().snapshot().since(&before);
    assert_eq!(counts(&d), vec![("get", 1)], "refresh, nothing new: {d}");

    // b commits; a's next append loses version 3, reads it, and commits as 4.
    tb.refresh().await.unwrap();
    tb.append(vec![datagen::rows(&mut rng, "b1", 1, None, 3)])
        .await
        .unwrap();
    let before = a.store_metrics().snapshot();
    let v = ta
        .append(vec![datagen::rows(&mut rng, "a2", 1, None, 3)])
        .await
        .unwrap()
        .version;
    let d = a.store_metrics().snapshot().since(&before);
    assert_eq!(v, 4);
    assert_eq!(
        counts(&d),
        vec![("get", 1), ("put", 1), ("put_create", 2)],
        "conflicting append: {d}"
    );

    // A refresh with one new entry: that entry and one missing one.
    tb.append(vec![datagen::rows(&mut rng, "b2", 1, None, 3)])
        .await
        .unwrap();
    let before = a.store_metrics().snapshot();
    assert_eq!(ta.refresh().await.unwrap(), 5);
    let d = a.store_metrics().snapshot().since(&before);
    assert_eq!(counts(&d), vec![("get", 2)], "refresh, one new entry: {d}");

    // No listing and no HEAD anywhere after opening.
    let all = a.store_metrics().snapshot();
    assert_eq!(all.listings(), 0, "listings: {all}");
    assert_eq!(all.count(Op::Head), 0, "heads: {all}");
}

/// Concurrent appends on one handle become one commit: one data PUT and one log PUT for all of
/// them, and every caller gets the same version (D18, case C16).
#[tokio::test]
async fn group_commit_is_one_commit() {
    group_commit(None).await;
}

#[tokio::test]
async fn group_commit_is_one_commit_with_tags() {
    let log = Arc::new(TagLog::default());
    group_commit(Some(&log)).await;
    assert_all_tagged(&log);
}

async fn group_commit(tags: Option<&Arc<TagLog>>) {
    let store = Arc::new(InMemory::new());
    let a = db(&store, Duration::from_millis(20), tags).await;
    let t = a
        .create_table("t", datagen::row_schema(), TableOptions::default())
        .await
        .unwrap();
    let mut rng = SimRng::new(3);
    let batches: Vec<_> = (0..4)
        .map(|i| datagen::rows(&mut rng, &format!("c{i}"), 3, None, 2))
        .collect();
    let before = a.store_metrics().snapshot();
    let results = futures::future::join_all(batches.into_iter().map(|b| {
        let t = t.clone();
        async move { t.append(vec![b]).await }
    }))
    .await;
    let d = a.store_metrics().snapshot().since(&before);
    let versions: Vec<u64> = results.into_iter().map(|r| r.unwrap().version).collect();
    assert_eq!(versions, vec![1, 1, 1, 1]);
    assert_eq!(
        counts(&d),
        vec![("put", 1), ("put_create", 1)],
        "group: {d}"
    );
    assert_eq!(t.scan().count().await.unwrap(), 12);
}

/// D39: a handle's own commit advances its snapshot, so the snapshot after a write costs no
/// request, even under strong consistency, and its file list costs none either. The same holds
/// for a write that lost its version to another writer and committed after it.
#[tokio::test]
async fn own_commit_snapshot_and_file_list_cost_nothing() {
    let store = Arc::new(InMemory::new());
    let open = |c: Consistency| {
        Database::builder_for_store(store.clone(), "db")
            .consistency(c)
            .group_commit_linger(Duration::ZERO)
            .open()
    };
    let a = open(Consistency::Strong).await.unwrap();
    let b = open(Consistency::Manual).await.unwrap();
    let ta = a
        .create_table("t", datagen::row_schema(), TableOptions::default())
        .await
        .unwrap();
    let tb = b.open_table("t").await.unwrap();
    let mut rng = SimRng::new(11);
    tb.append(vec![datagen::rows(&mut rng, "warm", 2, None, 3)])
        .await
        .unwrap();
    ta.refresh().await.unwrap();

    // Plain append, then the snapshot and its files: two PUTs and nothing else.
    let before = a.store_metrics().snapshot();
    let v = ta
        .append(vec![datagen::rows(&mut rng, "a1", 4, None, 3)])
        .await
        .unwrap()
        .version;
    let snap = ta.snapshot_in_hand();
    let files = snap.files();
    let d = a.store_metrics().snapshot().since(&before);
    assert_eq!(
        counts(&d),
        vec![("put", 1), ("put_create", 1)],
        "own commit: {d}"
    );
    assert_eq!(snap.version(), v);
    assert_eq!(files.len(), 2);
    assert_eq!(files.iter().filter_map(|f| f.rows).sum::<u64>(), 6);
    assert_eq!(files.last().unwrap().added_in, Some(v));
    assert!(files.iter().all(|f| f.size > 0 && !f.path.is_empty()));

    // A lost race: the GET of the winner's entry is the only read, and the snapshot after it
    // holds both commits without another GET.
    tb.append(vec![datagen::rows(&mut rng, "b1", 1, None, 3)])
        .await
        .unwrap();
    let before = a.store_metrics().snapshot();
    let v = ta
        .append(vec![datagen::rows(&mut rng, "a2", 1, None, 3)])
        .await
        .unwrap()
        .version;
    let snap = ta.snapshot_in_hand();
    let d = a.store_metrics().snapshot().since(&before);
    assert_eq!(
        counts(&d),
        vec![("get", 1), ("put", 1), ("put_create", 2)],
        "own commit after a lost race: {d}"
    );
    assert_eq!(snap.version(), v);
    assert_eq!(snap.num_rows(), 8);
    assert_eq!(snap.files().len(), 4);

    // Under strong consistency a query still looks for newer commits: one GET.
    let before = a.store_metrics().snapshot();
    let s = ta.snapshot().await.unwrap();
    let d = a.store_metrics().snapshot().since(&before);
    assert_eq!(counts(&d), vec![("get", 1)], "strong snapshot: {d}");
    assert_eq!(s.version(), v);

    // A manual handle's snapshot after its own write costs nothing either way.
    let before = b.store_metrics().snapshot();
    let v = tb
        .append(vec![datagen::rows(&mut rng, "b2", 1, None, 3)])
        .await
        .unwrap()
        .version;
    let s = tb.snapshot().await.unwrap();
    let d = b.store_metrics().snapshot().since(&before);
    assert_eq!(s.version(), v);
    assert_eq!(s.files().len(), 5);
    assert_eq!(
        counts(&d),
        vec![("get", 1), ("put", 1), ("put_create", 2)],
        "manual: {d}"
    );
}

/// D32: a transaction id adds no request. An append with one costs what a plain append does, a
/// replay the handle's snapshot already answers costs nothing, and writes of several
/// applications still become one commit.
#[tokio::test]
async fn transaction_ids_add_no_request() {
    let store = Arc::new(InMemory::new());
    let a = db(&store, Duration::from_millis(20), None).await;
    let t = a
        .create_table("t", datagen::row_schema(), TableOptions::default())
        .await
        .unwrap();
    let mut rng = SimRng::new(11);
    // Warm the capability probe.
    t.append(vec![datagen::rows(&mut rng, "warm", 1, None, 2)])
        .await
        .unwrap();

    let before = a.store_metrics().snapshot();
    let o = t
        .txn("wal", 1)
        .append(vec![datagen::rows(&mut rng, "w1", 3, None, 2)])
        .await
        .unwrap();
    let d = a.store_metrics().snapshot().since(&before);
    assert!(o.is_committed());
    assert_eq!(
        counts(&d),
        vec![("put", 1), ("put_create", 1)],
        "append with an id: {d}"
    );

    let before = a.store_metrics().snapshot();
    let o = t
        .txn("wal", 1)
        .append(vec![datagen::rows(&mut rng, "w1", 3, None, 2)])
        .await
        .unwrap();
    let d = a.store_metrics().snapshot().since(&before);
    assert!(!o.is_committed());
    assert_eq!(counts(&d), vec![], "replay: {d}");

    let before = a.store_metrics().snapshot();
    let results = futures::future::join_all((0..4).map(|i| {
        let t = t.clone();
        let b = datagen::rows(&mut rng, &format!("app{i}"), 2, None, 2);
        async move { t.txn(format!("app{i}"), 1).append(vec![b]).await }
    }))
    .await;
    let d = a.store_metrics().snapshot().since(&before);
    assert!(results.into_iter().all(|r| r.unwrap().is_committed()));
    assert_eq!(
        counts(&d),
        vec![("put", 1), ("put_create", 1)],
        "group with ids: {d}"
    );
}

/// Tags beyond S3's limits are refused when the handle opens.
#[tokio::test]
async fn object_tags_are_checked_at_open() {
    let open = |tags: Vec<(String, String)>| async move {
        Database::builder_for_store(Arc::new(InMemory::new()), "db")
            .object_tags(tags)
            .open()
            .await
    };
    let many = (0..11)
        .map(|i| (format!("k{i}"), "v".to_string()))
        .collect();
    assert!(matches!(
        open(many).await,
        Err(moraine::Error::InvalidConfig(_))
    ));
    let empty_key = vec![(String::new(), "v".to_string())];
    assert!(matches!(
        open(empty_key).await,
        Err(moraine::Error::InvalidConfig(_))
    ));
    let twice = vec![
        ("k".to_string(), "a".to_string()),
        ("k".to_string(), "b".to_string()),
    ];
    assert!(matches!(
        open(twice).await,
        Err(moraine::Error::InvalidConfig(_))
    ));
    let long = vec![("k".to_string(), "v".repeat(257))];
    assert!(matches!(
        open(long).await,
        Err(moraine::Error::InvalidConfig(_))
    ));
    assert!(open(vec![("k".into(), "v".repeat(256))]).await.is_ok());
}
