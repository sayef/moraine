//! Indexes: per-data-file sidecars in one bundle format (D19, D24).
//!
//! Every index kind writes its sidecar as a [`bundle`] (`.osix`) next to the table's data,
//! under `idx/<data file path>/<index name>.<spec hash>.osix`. Nothing there is needed to read
//! rows correctly: a file without a valid sidecar is answered by the kind's exact route.

pub(crate) mod build;
pub mod bundle;
pub(crate) mod cache;
pub(crate) mod discover;
#[cfg(feature = "fts")]
#[cfg_attr(docsrs, doc(cfg(feature = "fts")))]
pub mod fts;
#[cfg(feature = "fts")]
#[cfg_attr(docsrs, doc(cfg(feature = "fts")))]
pub mod hybrid;
pub(crate) mod inline;
pub(crate) mod key;
pub mod kind;
pub(crate) mod query;
pub(crate) mod spec;
pub(crate) mod topk;
pub mod vector;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use build::{BuildOptions, BuildReport, IndexStatus};
pub use cache::CacheMode;
pub use kind::{
    BuildCtx, BuildSummary, ExactScorer, FileIndex, Hit, IndexBuilder, IndexKind, MergeInput,
    Order, Query, RowMap,
};
pub use query::{SCORE_COLUMN, Search, SearchHit, SearchResult, SearchStats};
pub use spec::IndexSpec;

use crate::error::{Error, Result};

/// What a query does when its exact-route work exceeds `max_exact_scan_rows` (case V13).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExactScanPolicy {
    /// Scan anyway and count it in [`IndexMetrics::exact_limit_exceeded`].
    #[default]
    Allow,
    /// Fail with [`Error::ExactScanLimit`].
    Error,
}

/// Settings of the index machinery of one database (see [`crate::DatabaseBuilder`]).
#[derive(Clone)]
pub(crate) struct IndexConfig {
    pub kinds: Vec<Arc<dyn IndexKind>>,
    pub cache: cache::CacheMode,
    pub poll: Duration,
    pub max_open: usize,
    pub max_open_bytes: usize,
    pub exact_filter_threshold: u64,
    pub max_exact_scan_rows: u64,
    pub exact_policy: ExactScanPolicy,
    /// Bytes one build may hold (D35).
    pub build_budget: usize,
    /// Bytes all builds of the database may hold at once.
    pub max_build_memory: usize,
    /// Threads one build may use (D80).
    pub build_threads: usize,
    /// Files written with at least this many rows get their key index from the writer.
    pub key_inline_min_rows: u64,
    /// Bytes of full-text indexes built in memory for files without a sidecar (case F8).
    pub fts_fallback_memory: usize,
    /// The tokenizers full-text indexes may name.
    #[cfg(feature = "fts")]
    pub tokenizers: Arc<fts::Tokenizers>,
    /// A configuration error found while building, reported at open.
    pub invalid: Option<String>,
}

impl std::fmt::Debug for IndexConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.kinds.iter().map(|k| k.name()).collect();
        f.debug_struct("IndexConfig")
            .field("kinds", &names)
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

impl Default for IndexConfig {
    fn default() -> Self {
        #[cfg(feature = "fts")]
        let tokenizers = Arc::new(fts::Tokenizers::default());
        IndexConfig {
            #[cfg(feature = "fts")]
            kinds: builtin_kinds(&tokenizers),
            #[cfg(not(feature = "fts"))]
            kinds: builtin_kinds(),
            cache: cache::CacheMode::default(),
            poll: Duration::from_secs(10),
            max_open: 256,
            max_open_bytes: 2 << 30,
            exact_filter_threshold: 10_000,
            max_exact_scan_rows: 5_000_000,
            exact_policy: ExactScanPolicy::Allow,
            build_budget: 256 << 20,
            max_build_memory: 1 << 30,
            build_threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            key_inline_min_rows: key::INLINE_MIN_ROWS,
            fts_fallback_memory: 512 << 20,
            #[cfg(feature = "fts")]
            tokenizers,
            invalid: None,
        }
    }
}

/// Scratch memory a build thread is assumed to hold (search lists, visited sets, a converted
/// vector); a build gives at most an eighth of its budget to it.
const BUILD_THREAD_BYTES: usize = 1 << 20;
/// Rows a build thread is worth starting for.
const BUILD_ROWS_PER_THREAD: u64 = 1024;

impl IndexConfig {
    /// Threads a build of `rows` rows under `budget` bytes may use (D80): the configured
    /// number, fewer for a small input, and no more than an eighth of the budget pays for.
    pub(crate) fn threads_for(&self, budget: usize, rows: u64) -> usize {
        let by_rows = (rows / BUILD_ROWS_PER_THREAD).max(1) as usize;
        let by_memory = (budget / 8 / BUILD_THREAD_BYTES).max(1);
        self.build_threads.min(by_rows).min(by_memory).max(1)
    }
}

/// The kinds the enabled features provide.
fn builtin_kinds(
    #[cfg(feature = "fts")] tokenizers: &Arc<fts::Tokenizers>,
) -> Vec<Arc<dyn IndexKind>> {
    let kinds: [Option<Arc<dyn IndexKind>>; 4] = [
        #[cfg(feature = "fts")]
        Some(Arc::new(fts::FtsKind::new(tokenizers.clone()))),
        #[cfg(not(feature = "fts"))]
        None,
        Some(Arc::new(key::KeyKind)),
        #[cfg(feature = "vector")]
        Some(Arc::new(vector::usearch::HnswKind)),
        #[cfg(not(feature = "vector"))]
        None,
        #[cfg(feature = "turbovec")]
        Some(Arc::new(vector::turbo::TurboKind)),
        #[cfg(not(feature = "turbovec"))]
        None,
    ];
    kinds.into_iter().flatten().collect()
}

impl crate::DatabaseBuilder {
    /// Where sidecars are kept locally. Default: memory, 1 GiB.
    pub fn sidecar_cache(mut self, mode: cache::CacheMode) -> Self {
        self.index.cache = mode;
        self
    }

    /// How often a table lists its sidecars when its snapshot has not moved (default 10 s).
    pub fn sidecar_poll_interval(mut self, d: Duration) -> Self {
        self.index.poll = d;
        self
    }

    /// Opened sidecars kept for reuse: at most `count`, holding at most `resident_bytes` of
    /// heap (defaults 256 and 2 GiB; case V12).
    pub fn max_open_sidecars(mut self, count: usize, resident_bytes: usize) -> Self {
        self.index.max_open = count;
        self.index.max_open_bytes = resident_bytes;
        self
    }

    /// A file whose filter admits fewer rows than this is searched exactly even when it has a
    /// sidecar (default 10,000; case V8).
    pub fn exact_filter_threshold(mut self, rows: u64) -> Self {
        self.index.exact_filter_threshold = rows;
        self
    }

    /// Rows one query may score on the exact route, and what happens beyond (default
    /// 5,000,000 and [`ExactScanPolicy::Allow`]; case V13).
    pub fn max_exact_scan_rows(mut self, rows: u64, policy: ExactScanPolicy) -> Self {
        self.index.max_exact_scan_rows = rows;
        self.index.exact_policy = policy;
        self
    }

    /// Files written with at least this many rows get their key index from the writer, in the
    /// same round trip as the data (default 100,000; D19). Smaller files get theirs from
    /// [`crate::Table::build_indexes`], and are still found through bloom filters and page
    /// statistics until then.
    pub fn key_index_inline_min_rows(mut self, rows: u64) -> Self {
        self.index.key_inline_min_rows = rows;
        self
    }

    /// Memory held by full-text indexes built in memory for files that have no sidecar
    /// (default 512 MiB; case F8). Beyond it the least recently used are dropped; a query
    /// never fails for it.
    pub fn fts_memory(mut self, bytes: usize) -> Self {
        self.index.fts_fallback_memory = bytes;
        self
    }

    /// Registers a tokenizer under `name` for full-text indexes. Every client
    /// that builds or searches an index naming it must register the same tokenizer under the
    /// same name; built-in names (`default`, `en_stem`, `ngram2`, ...) cannot be replaced.
    #[cfg(feature = "fts")]
    #[cfg_attr(docsrs, doc(cfg(feature = "fts")))]
    pub fn tokenizer(mut self, name: &str, analyzer: tantivy::tokenizer::TextAnalyzer) -> Self {
        if let Err(e) = self.index.tokenizers.register(name, analyzer) {
            self.index.invalid = Some(e.to_string());
        }
        self
    }

    /// Memory one index build may hold (default 256 MiB), and all builds of the database at
    /// once (default 1 GiB). A build over a larger file splits into parts.
    pub fn build_memory(mut self, per_build: usize, total: usize) -> Self {
        self.index.build_budget = per_build.max(1 << 20);
        self.index.max_build_memory = total.max(self.index.build_budget);
        self
    }

    /// Threads one index build may use (default: the machine's parallelism). usearch graphs
    /// are built on that many threads when a file has enough rows; a graph built on more than
    /// one is not byte-identical to another build of the same rows (D80). Builds of other
    /// kinds use one thread each.
    pub fn build_threads(mut self, threads: usize) -> Self {
        self.index.build_threads = threads.max(1);
        self
    }
}

/// Counters of the index machinery of one database.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct IndexMetrics {
    /// Per-file searches answered from a sidecar.
    pub sidecar_searches: AtomicU64,
    /// Per-file searches answered by the exact route.
    pub exact_searches: AtomicU64,
    /// Rows scored on the exact route.
    pub exact_rows: AtomicU64,
    /// Sidecars found unusable (corrupt, mismatched) and marked bad (case V5).
    pub bad_sidecars: AtomicU64,
    /// Queries whose exact work exceeded `max_exact_scan_rows` under the `Allow` policy.
    pub exact_limit_exceeded: AtomicU64,
    /// The most build memory reserved at once, in bytes (never above `max_build_memory`).
    pub build_memory_peak: AtomicU64,
}

/// Cached table-wide full-text statistics, by (version, spec hash).
pub(crate) type TextStatsEntry = ((u64, String), Arc<dyn std::any::Any + Send + Sync>);

/// Index state of one table, shared by its handles.
#[derive(Debug, Default)]
pub(crate) struct TableIndexState {
    pub discovery: discover::Discovery,
    pub failures: build::Failures,
    /// Table-wide full-text statistics of recent (version, spec hash) pairs (D53): every query
    /// at one version sums the same files, so the sums and the document frequencies of terms
    /// already asked for are kept. The newest two are kept.
    #[cfg_attr(not(feature = "fts"), allow(dead_code))]
    pub text_stats: Mutex<Vec<TextStatsEntry>>,
    /// Set by the first full-text search on the table: background prewarm builds in-memory
    /// text indexes only from then on (D53), since building them is the costly part of a
    /// prewarm and a table that is never searched by text does not need them.
    #[cfg_attr(not(feature = "fts"), allow(dead_code))]
    pub text_searched: std::sync::atomic::AtomicBool,
    /// Rows a second this handle's compaction builds of the table's sidecars have run at
    /// (a moving average); `None` before the first measured build (D81).
    pub build_rate: Mutex<Option<f64>>,
}

impl TableIndexState {
    /// Folds a build of `rows` rows that took `elapsed` into [`TableIndexState::build_rate`].
    /// Builds too small or too quick to time are left out.
    pub fn record_build(&self, rows: u64, elapsed: Duration) {
        if rows < 1024 || elapsed < Duration::from_millis(10) {
            return;
        }
        let rate = rows as f64 / elapsed.as_secs_f64();
        let mut g = self.build_rate.lock().unwrap_or_else(|p| p.into_inner());
        *g = Some(g.map_or(rate, |r| r * 0.7 + rate * 0.3));
    }
}

/// The index state a database's tables share.
pub(crate) struct IndexRuntime {
    kinds: HashMap<String, Arc<dyn IndexKind>>,
    pub cfg: IndexConfig,
    pub cache: cache::SidecarCache,
    pub handles: cache::HandleCache,
    pub metrics: Arc<IndexMetrics>,
    tables: Mutex<HashMap<String, Arc<TableIndexState>>>,
    bitmaps: Mutex<HashMap<(String, u64, String), Arc<roaring::RoaringBitmap>>>,
    /// Full-text indexes built in memory for files without a sidecar.
    pub fallback: ByteLru,
    /// Build memory, in KiB permits.
    builds: Arc<tokio::sync::Semaphore>,
    build_reserved_kib: AtomicU64,
}

impl std::fmt::Debug for IndexRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexRuntime").finish_non_exhaustive()
    }
}

/// Filter bitmaps kept per database before the cache is cleared.
const MAX_BITMAPS: usize = 4096;

impl IndexRuntime {
    pub fn new(config: IndexConfig) -> Result<IndexRuntime> {
        if let Some(e) = &config.invalid {
            return Err(Error::InvalidConfig(e.clone()));
        }
        let mut kinds = HashMap::new();
        for k in &config.kinds {
            let name = k.name();
            if name.is_empty()
                || name.len() > bundle::MAX_KIND_NAME
                || !name.bytes().all(|c| c.is_ascii_graphic())
            {
                return Err(Error::InvalidConfig(format!(
                    "index kind name {name:?} must be 1 to 8 printable ASCII bytes"
                )));
            }
            // A later registration of the same name replaces the built-in one.
            kinds.insert(name.to_string(), k.clone());
        }
        Ok(IndexRuntime {
            kinds,
            cache: cache::SidecarCache::open(config.cache.clone())?,
            handles: cache::HandleCache::new(config.max_open, config.max_open_bytes),
            cfg: config.clone(),
            metrics: Arc::new(IndexMetrics::default()),
            tables: Mutex::new(HashMap::new()),
            bitmaps: Mutex::new(HashMap::new()),
            fallback: ByteLru::new(config.fts_fallback_memory),
            builds: Arc::new(tokio::sync::Semaphore::new(
                (config.max_build_memory >> 10).max(1),
            )),
            build_reserved_kib: AtomicU64::new(0),
        })
    }

    /// The kind registered under `name`.
    pub fn kind(&self, name: &str) -> Result<&Arc<dyn IndexKind>> {
        self.kinds.get(name).ok_or_else(|| {
            Error::IndexKindNotRegistered(crate::names::NotFound::among(
                name,
                self.kinds.keys().cloned(),
            ))
        })
    }

    /// The shared state of the table rooted at `root`.
    pub fn table(&self, root: &str) -> Arc<TableIndexState> {
        let mut g = self.tables.lock().unwrap_or_else(|p| p.into_inner());
        g.entry(root.to_string()).or_default().clone()
    }

    /// Reserves `bytes` of the database's build memory until the guard drops.
    pub async fn reserve_build(&self, bytes: usize) -> Result<BuildPermit<'_>> {
        let kib = ((bytes >> 10).max(1) as u32).min(self.builds_total());
        let p = self
            .builds
            .clone()
            .acquire_many_owned(kib)
            .await
            .map_err(|e| Error::Internal(format!("build semaphore: {e}")))?;
        let now = self
            .build_reserved_kib
            .fetch_add(u64::from(kib), std::sync::atomic::Ordering::SeqCst)
            + u64::from(kib);
        self.metrics
            .build_memory_peak
            .fetch_max(now << 10, std::sync::atomic::Ordering::SeqCst);
        Ok(BuildPermit {
            _p: p,
            kib: u64::from(kib),
            rt: self,
        })
    }

    fn builds_total(&self) -> u32 {
        (self.cfg.max_build_memory >> 10).clamp(1, u32::MAX as usize) as u32
    }

    /// Where builds write their bundles before uploading.
    pub fn build_dir(&self) -> std::path::PathBuf {
        let d = match &self.cfg.cache {
            cache::CacheMode::Disk { dir, .. } => dir.join("tmp"),
            _ => std::env::temp_dir(),
        };
        let _ = std::fs::create_dir_all(&d);
        d
    }

    pub fn bitmap(&self, key: &(String, u64, String)) -> Option<Arc<roaring::RoaringBitmap>> {
        self.bitmaps.lock().ok()?.get(key).cloned()
    }

    /// Filter bitmaps held.
    pub fn bitmaps(&self) -> usize {
        self.bitmaps.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn put_bitmap(&self, key: (String, u64, String), b: Arc<roaring::RoaringBitmap>) {
        if let Ok(mut g) = self.bitmaps.lock() {
            if g.len() >= MAX_BITMAPS {
                g.clear();
            }
            g.insert(key, b);
        }
    }
}

/// Opened indexes keyed by (data file path, size, spec hash), least recently used dropped
/// beyond a byte limit. An entry larger than the limit is not kept.
#[cfg_attr(not(feature = "fts"), allow(dead_code))]
pub(crate) struct ByteLru {
    max_bytes: usize,
    inner: Mutex<ByteLruInner>,
}

type FallbackKey = (String, u64, String);

#[derive(Default)]
#[cfg_attr(not(feature = "fts"), allow(dead_code))]
struct ByteLruInner {
    map: HashMap<FallbackKey, (Arc<dyn FileIndex>, usize, u64)>,
    tick: u64,
    bytes: usize,
}

#[cfg_attr(not(feature = "fts"), allow(dead_code))]
impl ByteLru {
    pub fn new(max_bytes: usize) -> ByteLru {
        ByteLru {
            max_bytes,
            inner: Mutex::new(ByteLruInner::default()),
        }
    }

    pub fn get(&self, key: &FallbackKey) -> Option<Arc<dyn FileIndex>> {
        let mut g = self.inner.lock().ok()?;
        g.tick += 1;
        let t = g.tick;
        let e = g.map.get_mut(key)?;
        e.2 = t;
        Some(e.0.clone())
    }

    pub fn insert(&self, key: FallbackKey, index: Arc<dyn FileIndex>, bytes: usize) {
        if bytes > self.max_bytes {
            return;
        }
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.tick += 1;
        let t = g.tick;
        if let Some(old) = g.map.insert(key, (index, bytes, t)) {
            g.bytes -= old.1;
        }
        g.bytes += bytes;
        while g.bytes > self.max_bytes {
            let victim = g
                .map
                .iter()
                .min_by_key(|(_, e)| e.2)
                .map(|(k, _)| k.clone());
            let Some(k) = victim else { break };
            if let Some(e) = g.map.remove(&k) {
                g.bytes -= e.1;
            }
        }
    }

    /// Entries and their bytes.
    pub fn usage(&self) -> (usize, usize) {
        self.inner
            .lock()
            .map(|g| (g.map.len(), g.bytes))
            .unwrap_or_default()
    }
}

/// Build memory held by one build.
pub(crate) struct BuildPermit<'a> {
    _p: tokio::sync::OwnedSemaphorePermit,
    kib: u64,
    rt: &'a IndexRuntime,
}

impl Drop for BuildPermit<'_> {
    fn drop(&mut self) {
        self.rt
            .build_reserved_kib
            .fetch_sub(self.kib, std::sync::atomic::Ordering::SeqCst);
    }
}
