//! T10: the local sidecar cache (cases K1 to K8 and V12).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use moraine::index::bundle::{BundleError, BundleMeta, BundleView, write_to_vec};
use moraine::index::{FileIndex, Hit, Query};
use moraine::testing::Listed;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use {
    moraine::index::CacheMode,
    moraine::testing::{HandleCache, SidecarCache},
};

const URL: &str = "memory:///test";

/// The disk-full test arms a process-wide failpoint; the tests take turns.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn bundle(fill: u8, len: usize) -> Vec<u8> {
    write_to_vec(
        BundleMeta {
            kind: "test-sum".into(),
            kind_format: 1,
            spec_hash: "0123456789abcdef".into(),
            source_rows: 10,
            source_size: 100,
        },
        &[("a", &vec![fill; len]), ("b", b"tail")],
        10,
    )
    .unwrap()
}

async fn put(store: &Arc<dyn ObjectStore>, key: &str, body: Vec<u8>) -> Listed {
    let location = Path::from(key);
    store.put(&location, PutPayload::from(body)).await.unwrap();
    let m = store.head(&location).await.unwrap();
    Listed {
        location,
        spec_hash: "0123456789abcdef".into(),
        size: m.size,
        etag: m.e_tag,
    }
}

fn ok(_: &BundleView) -> Result<(), BundleError> {
    Ok(())
}

fn disk(dir: &std::path::Path, max: u64) -> SidecarCache {
    SidecarCache::open(CacheMode::Disk {
        dir: dir.to_path_buf(),
        max_bytes: max,
    })
    .unwrap()
}

fn entries(dir: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir.join("v1"))
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// K1 (threads): concurrent downloads of one sidecar all succeed; one copy is installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn k1_concurrent_downloads_threads() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let l = put(&store, "idx/p/a.0123456789abcdef.osix", bundle(1, 200_000)).await;
    let cache = Arc::new(disk(dir.path(), 1 << 30));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (c, s, l) = (cache.clone(), store.clone(), l.clone());
        tasks.push(tokio::spawn(async move {
            c.get(&s, URL, &l, &ok)
                .await
                .unwrap()
                .unwrap()
                .view
                .section("b")
                .map(<[u8]>::to_vec)
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap().as_deref(), Some(&b"tail"[..]));
    }
    assert_eq!(entries(dir.path()).len(), 1);
    assert_eq!(
        std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
        0
    );
}

/// K1 (processes): two processes fill the same cache directory at once.
#[tokio::test]
async fn k1_concurrent_downloads_processes() {
    let _serial = SERIAL.lock().await;
    let cache = tempfile::tempdir().unwrap();
    let store_dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(store_dir.path()).unwrap());
    put(&store, "idx/p/a.0123456789abcdef.osix", bundle(2, 4 << 20)).await;
    let exe = env!("CARGO_BIN_EXE_cache_child");
    let spawn = || {
        std::process::Command::new(exe)
            .args([
                cache.path().to_str().unwrap(),
                store_dir.path().to_str().unwrap(),
                "idx/p/a.0123456789abcdef.osix",
            ])
            .output()
    };
    let (a, b) = std::thread::scope(|s| {
        let a = s.spawn(spawn);
        let b = s.spawn(spawn);
        (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
    });
    for o in [&a, &b] {
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        assert!(String::from_utf8_lossy(&o.stdout).starts_with("ok sections=2"));
    }
    assert_eq!(entries(cache.path()).len(), 1);
}

/// K2: a crash mid-download leaves a temporary directory and maybe an entry without `.ok`;
/// the next start removes both, and no entry without `.ok` is used.
#[tokio::test]
async fn k2_crash_during_download() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    drop(disk(dir.path(), 1 << 30));
    let stale = dir.path().join("tmp").join("dead");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("bundle.osix"), b"partial").unwrap();
    let old = SystemTime::now() - Duration::from_secs(7200);
    std::fs::File::open(&stale)
        .unwrap()
        .set_modified(old)
        .unwrap();
    let fresh = dir.path().join("tmp").join("live");
    std::fs::create_dir_all(&fresh).unwrap();
    let half = dir.path().join("v1").join("half-entry");
    std::fs::create_dir_all(&half).unwrap();
    std::fs::write(half.join("bundle.osix"), b"partial").unwrap();
    let c = disk(dir.path(), 1 << 30);
    assert!(!stale.exists(), "stale temporary removed");
    assert!(
        fresh.exists(),
        "a recent temporary may belong to a live download"
    );
    assert!(!half.exists(), "entry without .ok removed");
    assert_eq!(c.bytes().0, 0);
}

/// K3: the disk write fails (a full disk) or the cache is smaller than one sidecar: the
/// sidecar is served from memory when it fits, otherwise the query takes the exact route.
#[tokio::test]
async fn k3_disk_full_and_tiny_cache() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let l = put(&store, "idx/p/a.0123456789abcdef.osix", bundle(3, 100_000)).await;

    let scenario = fail::FailScenario::setup();
    fail::cfg("moraine::cache::write", "return").unwrap();
    let c = disk(dir.path(), 1 << 30);
    let got = c.get(&store, URL, &l, &ok).await.unwrap();
    assert!(got.is_some(), "served from memory");
    assert_eq!(c.stats().disk_failures, 1);
    assert!(entries(dir.path()).is_empty());
    assert_eq!(
        std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
        0
    );
    fail::remove("moraine::cache::write");
    scenario.teardown();

    // A cache smaller than the sidecar: memory.
    let small = disk(dir.path(), 1_000);
    assert!(small.get(&store, URL, &l, &ok).await.unwrap().is_some());
    assert!(entries(dir.path()).is_empty());
    // Memory smaller than the sidecar too: the exact route.
    let none = SidecarCache::open(CacheMode::Memory { max_bytes: 1_000 }).unwrap();
    assert!(none.get(&store, URL, &l, &ok).await.unwrap().is_none());
    let off = SidecarCache::open(CacheMode::Disabled).unwrap();
    assert!(off.get(&store, URL, &l, &ok).await.unwrap().is_none());
}

/// K4: the cache directory is removed while running; it is recreated and refilled.
#[tokio::test]
async fn k4_directory_removed() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("cache");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let l = put(&store, "idx/p/a.0123456789abcdef.osix", bundle(4, 10_000)).await;
    let c = disk(&root, 1 << 30);
    c.get(&store, URL, &l, &ok).await.unwrap().unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    let v = c.get(&store, URL, &l, &ok).await.unwrap().unwrap();
    assert_eq!(v.view.section("b").unwrap(), b"tail");
    assert_eq!(entries(&root).len(), 1);
    assert_eq!(c.stats().downloads, 2);
}

/// K5: a cached file corrupted on disk is detected at open, deleted and downloaded once more;
/// when the store's copy is bad too, the error reaches the caller (which marks it bad).
#[tokio::test]
async fn k5_corrupt_entry() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let l = put(&store, "idx/p/a.0123456789abcdef.osix", bundle(5, 10_000)).await;
    let c = disk(dir.path(), 1 << 30);
    c.get(&store, URL, &l, &ok).await.unwrap().unwrap();
    let name = entries(dir.path()).remove(0);
    let file = dir.path().join("v1").join(&name).join("bundle.osix");
    let mut b = std::fs::read(&file).unwrap();
    let n = b.len();
    b[n - 30] ^= 0xff; // inside the footer checksum
    std::fs::write(&file, &b).unwrap();
    let v = c.get(&store, URL, &l, &ok).await.unwrap().unwrap();
    assert_eq!(v.view.section("b").unwrap(), b"tail");
    assert_eq!(c.stats().corrupt, 1);
    assert_eq!(c.stats().downloads, 2);

    let bad = put(
        &store,
        "idx/p/b.0123456789abcdef.osix",
        b"not a bundle".to_vec(),
    )
    .await;
    assert!(c.get(&store, URL, &bad, &ok).await.is_err());
    // A header that does not match what the snapshot expects is an error too (V5).
    let reject = |_: &BundleView| Err(BundleError::SourceMismatch("rows".into()));
    let other = put(&store, "idx/q/a.0123456789abcdef.osix", bundle(6, 100)).await;
    assert!(c.get(&store, URL, &other, &reject).await.is_err());
}

/// K6: eviction skips entries in use; K7: a new ETag for the same key is a separate entry.
#[tokio::test]
async fn k6_k7_eviction_and_etags() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let c = disk(dir.path(), 250_000);
    let a = put(&store, "idx/p/a.0123456789abcdef.osix", bundle(7, 100_000)).await;
    let held = c.get(&store, URL, &a, &ok).await.unwrap().unwrap();
    let b = put(&store, "idx/q/a.0123456789abcdef.osix", bundle(8, 100_000)).await;
    drop(c.get(&store, URL, &b, &ok).await.unwrap().unwrap());
    let d = put(&store, "idx/r/a.0123456789abcdef.osix", bundle(9, 100_000)).await;
    drop(c.get(&store, URL, &d, &ok).await.unwrap().unwrap());
    // Three entries do not fit: the unpinned, least recently used one (b) went.
    assert_eq!(entries(dir.path()).len(), 2);
    assert_eq!(c.stats().evictions, 1);
    assert_eq!(
        held.view.section("b").unwrap(),
        b"tail",
        "the pinned entry is intact"
    );
    let names = entries(dir.path());
    assert!(
        names
            .iter()
            .any(|n| *n == moraine::testing::entry_name(URL, &a))
    );

    // K7: the same key uploaded again (a duplicate build) has a new ETag.
    let a2 = put(&store, "idx/p/a.0123456789abcdef.osix", bundle(7, 100_000)).await;
    assert_ne!(a.etag, a2.etag);
    assert_ne!(
        moraine::testing::entry_name(URL, &a),
        moraine::testing::entry_name(URL, &a2)
    );
    drop(held);
    c.get(&store, URL, &a2, &ok).await.unwrap().unwrap();
    assert!(entries(dir.path()).contains(&moraine::testing::entry_name(URL, &a2)));
}

/// K8: a read-only directory fails at open with a clear error; the memory mode works there.
#[tokio::test]
async fn k8_read_only_directory() {
    let _serial = SERIAL.lock().await;
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let ro = dir.path().join("ro");
    std::fs::create_dir(&ro).unwrap();
    std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
    if std::fs::write(ro.join("x"), b"").is_ok() {
        eprintln!("running with privileges that ignore permissions; skipping");
        return;
    }
    let err = SidecarCache::open(CacheMode::Disk {
        dir: ro.join("cache"),
        max_bytes: 1 << 30,
    })
    .unwrap_err();
    assert!(matches!(err, moraine::Error::InvalidConfig(ref m) if m.contains("CacheMode::Memory")));
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let l = put(&store, "idx/p/a.0123456789abcdef.osix", bundle(1, 1000)).await;
    let mem = SidecarCache::open(CacheMode::Memory { max_bytes: 1 << 20 }).unwrap();
    assert!(mem.get(&store, URL, &l, &ok).await.unwrap().is_some());
    std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(unix)]
#[test]
fn directory_mode_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("c");
    disk(&root, 1);
    let mode = std::fs::metadata(&root).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
}

struct Toy(usize);

impl FileIndex for Toy {
    fn resident_bytes(&self) -> usize {
        self.0
    }
    fn search(
        &self,
        _: &Query<'_>,
        _: Option<&roaring::RoaringBitmap>,
        _: usize,
    ) -> moraine::Result<Vec<Hit>> {
        Ok(Vec::new())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// V12: hundreds of sidecars opened in turn stay within the handle limits (count and resident
/// bytes), so maps and descriptors are bounded; evicted handles release their cache pins.
#[tokio::test]
async fn v12_open_handle_limit() {
    let _serial = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let c = disk(dir.path(), 1 << 30);
    let handles = HandleCache::new(64, 10_000);
    for i in 0..600 {
        let l = put(
            &store,
            &format!("idx/f{i}/a.0123456789abcdef.osix"),
            bundle(1, 10),
        )
        .await;
        let p = c.get(&store, URL, &l, &ok).await.unwrap().unwrap();
        handles.insert(l.location.to_string(), Arc::new(Toy(100)), p);
        let (n, bytes) = handles.usage();
        assert!(n <= 64 && bytes <= 10_000, "{n} handles, {bytes} bytes");
    }
    // A resident size beyond the byte limit keeps only that one.
    let l = put(&store, "idx/big/a.0123456789abcdef.osix", bundle(1, 10)).await;
    let p = c.get(&store, URL, &l, &ok).await.unwrap().unwrap();
    handles.insert("big".into(), Arc::new(Toy(50_000)), p);
    assert_eq!(handles.usage().0, 1);
    assert!(handles.get("big").is_some());
}

/// Scenario 18 for discovery: listings cut short at random never make known sidecars
/// disappear; the first complete listing is the reference.
#[tokio::test]
async fn partial_listings_keep_known_sidecars() {
    use moraine::testing::{Discovery, sidecar_location};
    use moraine_testkit::chaos::{ChaosStore, FaultRules};
    let _serial = SERIAL.lock().await;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let idx = Path::from("db/t/idx");
    for i in 0..20 {
        let loc = sidecar_location(
            &idx,
            &format!("part-{i}.parquet"),
            "emb",
            "0123456789abcdef",
        );
        inner
            .put(&loc, PutPayload::from(vec![0u8; 8]))
            .await
            .unwrap();
    }
    let rules = FaultRules {
        cut_listing: 0.5,
        ..Default::default()
    };
    let concrete = Arc::new(ChaosStore::new(inner.clone(), rules, 7));
    let chaos: Arc<dyn ObjectStore> = concrete.clone();
    let d = Discovery::default();
    let poll = Duration::ZERO;
    let mut known = None;
    let mut cut = 0;
    for v in 0..200u64 {
        match d.map(&chaos, &idx, v, Duration::from_secs(v), poll).await {
            Ok(m) => {
                let n: usize = m.values().map(Vec::len).sum();
                if let Some(k) = known {
                    assert_eq!(n, k, "a cut listing dropped or added sidecars");
                } else {
                    known = Some(n);
                }
            }
            Err(_) => {
                assert!(known.is_none(), "an error after a good listing");
                cut += 1;
            }
        }
    }
    assert_eq!(known, Some(20));
    let injected = concrete
        .faults()
        .cut_listing
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        injected > 50,
        "only {injected} listings were cut ({cut} before the first good one)"
    );
}
