//! The local sidecar cache (cases K1 to K8).
//!
//! A sidecar never changes for a given key and ETag, so entries are never invalidated, only
//! evicted. Three modes:
//!
//! - [`CacheMode::Disk`]: `<dir>/v1/<32 hex of SHA-256(store URL + key)>-<ETag>/bundle.osix`
//!   plus `.ok`, downloaded into `<dir>/tmp/<uuid>/`, checked, and renamed into place. Several
//!   threads or processes may share the directory; nothing but atomic rename is relied on, and
//!   any entry may vanish at any time (a missing file means "download again"). Least recently
//!   used entries are evicted by total bytes, skipping entries in use.
//! - [`CacheMode::Memory`]: bundles held in page-aligned memory under a byte limit, for
//!   read-only file systems and small disks.
//! - [`CacheMode::Disabled`]: no sidecar is used; every query takes the exact route (tests).
//!
//! A disk problem never fails a query: a sidecar that cannot be written falls back to memory
//! when the memory limit allows, otherwise its file takes the exact route.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};

use super::bundle::{BundleBytes, BundleError, BundleView};
use super::discover::Listed;
use crate::error::{Error, Result};

/// Where sidecars are kept locally.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheMode {
    /// On disk under `dir`, at most `max_bytes` in total (the original design's default was 10 GiB).
    Disk {
        /// The cache directory; several processes may share it (K1).
        dir: PathBuf,
        /// Bytes of bundles kept at most.
        max_bytes: u64,
    },
    /// In memory, at most `max_bytes`.
    Memory {
        /// Bytes of bundles kept at most.
        max_bytes: u64,
    },
    /// No sidecars: every file takes the exact route.
    Disabled,
}

impl Default for CacheMode {
    fn default() -> Self {
        CacheMode::Memory { max_bytes: 1 << 30 }
    }
}

/// Temporary download directories older than this are removed at start-up (K2).
const STALE_TMP: Duration = Duration::from_secs(3600);
const OK: &str = ".ok";
const BUNDLE: &str = "bundle.osix";

/// A cached bundle in use. Entries with a live handle are not evicted (K6).
#[derive(Clone, Debug)]
pub struct Pinned {
    /// The bundle's bytes.
    pub view: BundleView,
    _pin: Arc<()>,
}

struct Entry {
    bytes: u64,
    tick: u64,
    pin: Arc<()>,
    /// Memory mode (or memory fallback): the bundle itself.
    view: Option<BundleView>,
}

#[derive(Default)]
struct Lru {
    entries: HashMap<String, Entry>,
    tick: u64,
    disk_bytes: u64,
    mem_bytes: u64,
}

/// Counters, for tests and metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CacheStats {
    /// Bundles downloaded from the store.
    pub downloads: u64,
    /// Lookups answered from the cache.
    pub hits: u64,
    /// Entries removed to stay within the size limit.
    pub evictions: u64,
    /// Installs that lost the rename race to another thread or process (K1).
    pub lost_races: u64,
    /// Disk writes that failed and fell back to memory or the exact route (K3).
    pub disk_failures: u64,
    /// Entries deleted because they failed to open (K5).
    pub corrupt: u64,
}

/// See the module documentation.
pub struct SidecarCache {
    mode: CacheMode,
    /// Memory for the memory mode, and for the disk mode's fallback (K3).
    mem_limit: u64,
    lru: Mutex<Lru>,
    stats: Mutex<CacheStats>,
}

impl std::fmt::Debug for SidecarCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidecarCache")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

fn sanitise(etag: &str) -> String {
    let s: String = etag
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(64)
        .collect();
    if s.is_empty() { "none".into() } else { s }
}

/// The entry name of a sidecar: hash of the store URL and key, plus its ETag (K7: a new ETag
/// is a separate entry).
pub fn entry_name(store_url: &str, l: &Listed) -> String {
    let mut h = Sha256::new();
    h.update(store_url.as_bytes());
    h.update(b"\n");
    h.update(l.location.as_ref().as_bytes());
    let d = h.finalize();
    let hex: String = d[..16].iter().map(|b| format!("{b:02x}")).collect();
    let tag = match &l.etag {
        Some(e) => sanitise(e),
        None => format!("s{}", l.size),
    };
    format!("{hex}-{tag}")
}

#[cfg(unix)]
fn private_dir(p: &FsPath) -> io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(p)?;
    fs::set_permissions(p, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn private_dir(p: &FsPath) -> io::Result<()> {
    fs::create_dir_all(p)
}

fn age(p: &FsPath) -> Duration {
    fs::metadata(p)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| SystemTime::now().duration_since(t).ok())
        .unwrap_or_default()
}

impl SidecarCache {
    /// Opens the cache. In disk mode the directory is created with mode 0700 and scanned:
    /// entries without `.ok` and stale temporary downloads are removed (K2). A directory that
    /// cannot be written is [`Error::InvalidConfig`] (K8: use the memory mode there).
    pub fn open(mode: CacheMode) -> Result<SidecarCache> {
        let mem_limit = match &mode {
            CacheMode::Memory { max_bytes } => *max_bytes,
            // The disk mode's fallback for sidecars that cannot be written (K3).
            CacheMode::Disk { .. } => 256 << 20,
            CacheMode::Disabled => 0,
        };
        let c = SidecarCache {
            mode,
            mem_limit,
            lru: Mutex::new(Lru::default()),
            stats: Mutex::new(CacheStats::default()),
        };
        if let CacheMode::Disk { dir, .. } = &c.mode {
            let bad = |e: io::Error| {
                Error::InvalidConfig(format!(
                    "sidecar cache directory {}: {e} (use CacheMode::Memory on a read-only file \
                     system)",
                    dir.display()
                ))
            };
            private_dir(dir).map_err(bad)?;
            private_dir(&dir.join("v1")).map_err(bad)?;
            private_dir(&dir.join("tmp")).map_err(bad)?;
            let probe = dir
                .join("tmp")
                .join(format!(".probe-{}", std::process::id()));
            fs::write(&probe, b"").map_err(bad)?;
            let _ = fs::remove_file(&probe);
            c.scan(dir);
        }
        Ok(c)
    }

    /// Where this cache keeps sidecars.
    pub fn mode(&self) -> &CacheMode {
        &self.mode
    }

    /// The counters so far.
    pub fn stats(&self) -> CacheStats {
        self.stats.lock().map(|g| *g).unwrap_or_default()
    }

    fn count(&self, f: impl FnOnce(&mut CacheStats)) {
        if let Ok(mut g) = self.stats.lock() {
            f(&mut g);
        }
    }

    /// Bytes held: (disk, memory).
    pub fn bytes(&self) -> (u64, u64) {
        self.lru
            .lock()
            .map(|g| (g.disk_bytes, g.mem_bytes))
            .unwrap_or_default()
    }

    /// Start-up scan: rebuild sizes, drop entries without `.ok`, drop stale temporaries.
    fn scan(&self, dir: &FsPath) {
        if let Ok(rd) = fs::read_dir(dir.join("tmp")) {
            for e in rd.flatten() {
                if age(&e.path()) > STALE_TMP {
                    let _ = fs::remove_dir_all(e.path());
                }
            }
        }
        let Ok(rd) = fs::read_dir(dir.join("v1")) else {
            return;
        };
        let mut g = self.lru.lock().unwrap_or_else(|p| p.into_inner());
        for e in rd.flatten() {
            let p = e.path();
            if !p.join(OK).exists() {
                let _ = fs::remove_dir_all(&p);
                continue;
            }
            let bytes = fs::metadata(p.join(BUNDLE)).map(|m| m.len()).unwrap_or(0);
            let name = e.file_name().to_string_lossy().into_owned();
            g.disk_bytes += bytes;
            g.entries.insert(
                name,
                Entry {
                    bytes,
                    tick: 0,
                    pin: Arc::new(()),
                    view: None,
                },
            );
        }
    }

    /// The bundle of `l`, validated with `check` (header expectations), downloading it on a
    /// miss. `Ok(None)` means "take the exact route": the cache is disabled or full. An error
    /// means the sidecar is unusable even after one fresh download (K5); the caller marks it
    /// bad.
    pub async fn get(
        &self,
        store: &Arc<dyn ObjectStore>,
        store_url: &str,
        l: &Listed,
        check: &(dyn Fn(&BundleView) -> std::result::Result<(), BundleError> + Sync),
    ) -> Result<Option<Pinned>> {
        let name = entry_name(store_url, l);
        match self.mode.clone() {
            CacheMode::Disabled => Ok(None),
            CacheMode::Memory { .. } => self.get_memory(store, &name, l, check).await,
            CacheMode::Disk { dir, max_bytes } => {
                if l.size > max_bytes {
                    // Smaller than one sidecar (K3): memory if it fits, else exact.
                    return self.get_memory(store, &name, l, check).await;
                }
                if let Some(p) = self.memory_hit(&name) {
                    return Ok(Some(p));
                }
                let entry = dir.join("v1").join(&name);
                if entry.join(OK).exists() {
                    match self.open_disk(&entry, check) {
                        Ok(view) => {
                            self.count(|s| s.hits += 1);
                            return Ok(Some(self.touch_disk(&name, view)));
                        }
                        Err(_) => {
                            // K5: delete and download once more.
                            self.count(|s| s.corrupt += 1);
                            self.forget(&name);
                            let _ = fs::remove_dir_all(&entry);
                        }
                    }
                }
                match self.download_disk(store, &dir, &name, l, check).await {
                    Ok(view) => {
                        let p = self.touch_disk(&name, view);
                        self.evict(&dir, max_bytes, &name);
                        Ok(Some(p))
                    }
                    Err(DiskError::Io) => {
                        // K3: a disk problem never fails the query.
                        self.count(|s| s.disk_failures += 1);
                        self.get_memory(store, &name, l, check).await
                    }
                    Err(DiskError::Other(e)) => Err(e),
                }
            }
        }
    }

    fn memory_hit(&self, name: &str) -> Option<Pinned> {
        let mut g = self.lru.lock().ok()?;
        g.tick += 1;
        let t = g.tick;
        let e = g.entries.get_mut(name)?;
        let view = e.view.clone()?;
        e.tick = t;
        let pin = e.pin.clone();
        drop(g);
        self.count(|s| s.hits += 1);
        Some(Pinned { view, _pin: pin })
    }

    async fn get_memory(
        &self,
        store: &Arc<dyn ObjectStore>,
        name: &str,
        l: &Listed,
        check: &(dyn Fn(&BundleView) -> std::result::Result<(), BundleError> + Sync),
    ) -> Result<Option<Pinned>> {
        if let Some(p) = self.memory_hit(name) {
            return Ok(Some(p));
        }
        if l.size > self.mem_limit {
            return Ok(None);
        }
        self.count(|s| s.downloads += 1);
        let body = store.get(&l.location).await?.bytes().await?;
        let view = BundleView::parse(BundleBytes::copy(&body)?)?;
        check(&view)?;
        view.verify_sections()?;
        let bytes = view.len();
        let mut g = self.lru.lock().unwrap_or_else(|p| p.into_inner());
        // Evict memory entries, least recently used first, until this one fits.
        while g.mem_bytes + bytes > self.mem_limit {
            let victim = g
                .entries
                .iter()
                .filter(|(_, e)| e.view.is_some())
                .min_by_key(|(_, e)| e.tick)
                .map(|(k, _)| k.clone());
            let Some(k) = victim else { break };
            if let Some(e) = g.entries.remove(&k) {
                g.mem_bytes -= e.bytes;
            }
            self.count(|s| s.evictions += 1);
        }
        if g.mem_bytes + bytes > self.mem_limit {
            return Ok(None);
        }
        g.tick += 1;
        let tick = g.tick;
        let pin = Arc::new(());
        g.mem_bytes += bytes;
        g.entries.insert(
            name.to_string(),
            Entry {
                bytes,
                tick,
                pin: pin.clone(),
                view: Some(view.clone()),
            },
        );
        Ok(Some(Pinned { view, _pin: pin }))
    }

    fn open_disk(
        &self,
        entry: &FsPath,
        check: &(dyn Fn(&BundleView) -> std::result::Result<(), BundleError> + Sync),
    ) -> std::result::Result<BundleView, BundleError> {
        let f = fs::File::open(entry.join(BUNDLE))?;
        let view = BundleView::parse(BundleBytes::map(&f)?)?;
        check(&view)?;
        Ok(view)
    }

    async fn download_disk(
        &self,
        store: &Arc<dyn ObjectStore>,
        dir: &FsPath,
        name: &str,
        l: &Listed,
        check: &(dyn Fn(&BundleView) -> std::result::Result<(), BundleError> + Sync),
    ) -> std::result::Result<BundleView, DiskError> {
        // K4: the directory may have been removed while running.
        private_dir(&dir.join("tmp"))?;
        private_dir(&dir.join("v1"))?;
        let tmp = dir
            .join("tmp")
            .join(crate::clock::random_name(&crate::clock::SystemClock::new()));
        private_dir(&tmp)?;
        let cleanup = TmpGuard(tmp.clone());
        self.count(|s| s.downloads += 1);
        {
            let mut f = fs::File::create(tmp.join(BUNDLE))?;
            let mut stream = store
                .get(&l.location)
                .await
                .map_err(|e| DiskError::Other(e.into()))?
                .into_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| DiskError::Other(e.into()))?;
                write_chunk(&mut f, &chunk)?;
            }
            f.sync_all()?;
        }
        let view = {
            let f = fs::File::open(tmp.join(BUNDLE))?;
            let view = BundleView::parse(BundleBytes::map(&f)?).map_err(DiskError::bundle)?;
            check(&view).map_err(DiskError::bundle)?;
            view.verify_sections().map_err(DiskError::bundle)?;
            view
        };
        fs::write(tmp.join(OK), b"")?;
        let target = dir.join("v1").join(name);
        match fs::rename(&tmp, &target) {
            Ok(()) => {
                std::mem::forget(cleanup);
                // The mapping stays valid after the rename (same inode).
                Ok(view)
            }
            Err(_) if target.join(OK).exists() => {
                // K1: another thread or process installed it first; use theirs.
                self.count(|s| s.lost_races += 1);
                drop(cleanup);
                self.open_disk(&target, check).map_err(DiskError::bundle)
            }
            Err(_) => Err(DiskError::Io),
        }
    }

    fn touch_disk(&self, name: &str, view: BundleView) -> Pinned {
        let mut g = self.lru.lock().unwrap_or_else(|p| p.into_inner());
        g.tick += 1;
        let tick = g.tick;
        let bytes = view.len();
        let pin = match g.entries.get_mut(name) {
            Some(e) => {
                e.tick = tick;
                e.pin.clone()
            }
            None => {
                g.disk_bytes += bytes;
                let pin = Arc::new(());
                g.entries.insert(
                    name.to_string(),
                    Entry {
                        bytes,
                        tick,
                        pin: pin.clone(),
                        view: None,
                    },
                );
                pin
            }
        };
        Pinned { view, _pin: pin }
    }

    fn forget(&self, name: &str) {
        let mut g = self.lru.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(e) = g.entries.remove(name) {
            if e.view.is_some() {
                g.mem_bytes -= e.bytes;
            } else {
                g.disk_bytes -= e.bytes;
            }
        }
    }

    /// Evicts disk entries, least recently used first, until the total fits; entries in use
    /// (a handle holds their pin) and `keep` are skipped (K6).
    fn evict(&self, dir: &FsPath, max_bytes: u64, keep: &str) {
        let mut g = self.lru.lock().unwrap_or_else(|p| p.into_inner());
        if g.disk_bytes <= max_bytes {
            return;
        }
        let mut order: Vec<(u64, String)> = g
            .entries
            .iter()
            .filter(|(k, e)| {
                e.view.is_none() && k.as_str() != keep && Arc::strong_count(&e.pin) == 1
            })
            .map(|(k, e)| (e.tick, k.clone()))
            .collect();
        order.sort();
        for (_, k) in order {
            if g.disk_bytes <= max_bytes {
                break;
            }
            if let Some(e) = g.entries.remove(&k) {
                g.disk_bytes -= e.bytes;
                // On Unix a file another process has mapped stays valid until unmapped.
                let _ = fs::remove_dir_all(dir.join("v1").join(&k));
                self.count(|s| s.evictions += 1);
            }
        }
    }
}

fn write_chunk(f: &mut fs::File, chunk: &[u8]) -> io::Result<()> {
    fail::fail_point!("moraine::cache::write", |_| Err(io::Error::new(
        io::ErrorKind::StorageFull,
        "injected: no space left on device"
    )));
    f.write_all(chunk)
}

/// Removes a temporary download directory unless forgotten.
struct TmpGuard(PathBuf);

impl Drop for TmpGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

enum DiskError {
    /// A local file-system problem: fall back (K3).
    Io,
    /// The store or the bundle: not a cache problem.
    Other(Error),
}

impl DiskError {
    fn bundle(e: BundleError) -> DiskError {
        match e {
            BundleError::Io(_) => DiskError::Io,
            other => DiskError::Other(Error::Sidecar(other)),
        }
    }
}

impl From<io::Error> for DiskError {
    fn from(_: io::Error) -> Self {
        DiskError::Io
    }
}

/// Opened sidecars kept for reuse (case V12), bounded by
/// count and by the resident bytes the kinds report. Each holds its cache pin, so an entry in
/// use is never evicted from the disk cache.
pub struct HandleCache {
    max_count: usize,
    max_bytes: usize,
    inner: Mutex<HandleLru>,
}

#[derive(Default)]
struct HandleLru {
    map: HashMap<String, (Arc<dyn super::FileIndex>, Pinned, usize, u64)>,
    tick: u64,
    bytes: usize,
}

impl HandleCache {
    /// A cache of at most `max_count` opened sidecars holding at most `max_bytes` in all.
    pub fn new(max_count: usize, max_bytes: usize) -> Self {
        HandleCache {
            max_count: max_count.max(1),
            max_bytes,
            inner: Mutex::new(HandleLru::default()),
        }
    }

    /// The opened sidecar under `key`, marked as used now.
    pub fn get(&self, key: &str) -> Option<Arc<dyn super::FileIndex>> {
        let mut g = self.inner.lock().ok()?;
        g.tick += 1;
        let t = g.tick;
        let e = g.map.get_mut(key)?;
        e.3 = t;
        Some(e.0.clone())
    }

    /// Keeps `index` (and the cache entry `pin` it reads from) under `key`, evicting the least
    /// recently used entries beyond the limits.
    pub fn insert(&self, key: String, index: Arc<dyn super::FileIndex>, pin: Pinned) {
        let bytes = index.resident_bytes();
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.tick += 1;
        let t = g.tick;
        if let Some(old) = g.map.insert(key.clone(), (index, pin, bytes, t)) {
            g.bytes -= old.2;
        }
        g.bytes += bytes;
        while g.map.len() > self.max_count || (g.bytes > self.max_bytes && g.map.len() > 1) {
            let victim = g
                .map
                .iter()
                .filter(|(k, _)| **k != key)
                .min_by_key(|(_, e)| e.3)
                .map(|(k, _)| k.clone());
            let Some(k) = victim else { break };
            if let Some(e) = g.map.remove(&k) {
                g.bytes -= e.2;
            }
        }
    }

    /// Open handles and their resident bytes.
    pub fn usage(&self) -> (usize, usize) {
        self.inner
            .lock()
            .map(|g| (g.map.len(), g.bytes))
            .unwrap_or_default()
    }
}
