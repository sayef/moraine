//! Background snapshot refresh (D29, D43): under [`Consistency::Background`] a task
//! per handle reads new log entries every interval, so queries use the snapshot in hand and
//! make no snapshot request, and it prewarms the caches for each new version.
//!
//! **Prewarm.** For the files a new version adds: their Parquet footers and page indexes (the
//! file cache), the sidecar listing at the new version and their sidecars (downloaded, checked
//! and opened), the in-memory full-text indexes of files without one, and the materialised
//! copy of a small table (D20). **Carry-over.** Every cache is keyed by an immutable data file
//! (path and size) or by a sidecar's key and ETag, so entries of files the new version still
//! holds stay valid and are not fetched again; the small table is extended with the new files
//! when the version moved by appends only.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::StreamExt;

use super::TableInner;
use crate::index::spec::{parse_specs, spec_hash};
use crate::log::DataFile;
use crate::options::Consistency;
use crate::snapshot::Snapshot;

/// A query refreshes by itself when the background task has not confirmed the snapshot for
/// this many intervals (plus one second), as when the store is unreachable.
pub(crate) const STALE_INTERVALS: u32 = 4;

/// Counters of one handle's background refresh.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct RefreshMetrics {
    /// Refreshes run.
    pub refreshes: std::sync::atomic::AtomicU64,
    /// Refreshes that found a new version.
    pub new_versions: std::sync::atomic::AtomicU64,
    /// Refreshes that failed (the next interval tries again).
    pub failures: std::sync::atomic::AtomicU64,
    /// Data files whose caches were prewarmed.
    pub prewarmed_files: std::sync::atomic::AtomicU64,
    /// The newest version whose prewarm has finished.
    pub prewarmed_version: std::sync::atomic::AtomicU64,
}

impl TableInner {
    /// Starts the background task when the consistency mode asks for one and none runs.
    pub(crate) fn start_background(self: &Arc<Self>) {
        let Consistency::Background { interval } = self.consistency() else {
            return;
        };
        if self.background.swap(true, Ordering::SeqCst) {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            // No runtime to run on: queries refresh by themselves once the snapshot is stale.
            self.background.store(false, Ordering::SeqCst);
            return;
        };
        let weak = Arc::downgrade(self);
        let clock = self.db.clock().clone();
        rt.spawn(async move {
            let mut interval = interval;
            loop {
                clock.sleep(interval).await;
                let Some(t) = Weak::upgrade(&weak) else {
                    return;
                };
                match t.consistency() {
                    Consistency::Background { interval: i } => interval = i,
                    _ => {
                        t.background.store(false, Ordering::SeqCst);
                        return;
                    }
                }
                t.background_step().await;
            }
        });
    }

    /// One refresh, and the prewarm of what it brought.
    async fn background_step(self: &Arc<Self>) {
        let old = self.current();
        let m = &self.refresh_metrics;
        m.refreshes.fetch_add(1, Ordering::Relaxed);
        let new = match self.refresh().await {
            Ok(s) => s,
            Err(_) => {
                m.failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if new.version == old.version {
            return;
        }
        m.new_versions.fetch_add(1, Ordering::Relaxed);
        self.prewarm(&old, &new).await;
        m.prewarmed_version.fetch_max(new.version, Ordering::SeqCst);
    }

    /// Fetches into the caches what queries at `new` need beyond what `old` had. Failures are
    /// ignored: a query fetches whatever is missing.
    pub(crate) async fn prewarm(self: &Arc<Self>, old: &Snapshot, new: &Arc<Snapshot>) {
        let had: HashSet<&str> = old.files.iter().map(|f| f.path.as_str()).collect();
        let fresh: Vec<Arc<DataFile>> = new
            .files
            .iter()
            .filter(|f| !had.contains(f.path.as_str()))
            .cloned()
            .collect();
        let n = self.db.opts.io_concurrency.max(1);
        let store = self.db.store.object_store().clone();
        // Footers and page indexes.
        let jobs: Vec<(object_store::path::Path, u64)> = fresh
            .iter()
            .map(|f| {
                (
                    self.layer.data_prefix().clone().join(f.path.as_str()),
                    f.size,
                )
            })
            .collect();
        let me = self.clone();
        let fetched: Vec<bool> = futures::stream::iter(jobs)
            .map(move |(path, size)| {
                let (me, store) = (me.clone(), store.clone());
                async move { me.db.cache.get(&store, &path, size).await.is_ok() }
            })
            .buffer_unordered(n)
            .collect()
            .await;
        let fetched = fetched.into_iter().filter(|ok| *ok).count();
        self.refresh_metrics
            .prewarmed_files
            .fetch_add(fetched as u64, Ordering::Relaxed);
        // A small table's copy, extended rather than rebuilt when only appends happened.
        let _ = self.small_table(new).await;
        if fresh.is_empty() {
            return;
        }
        self.prewarm_indexes(new, &fresh).await;
    }

    /// The sidecar listing at `new`, and every index's searcher for the `fresh` files.
    async fn prewarm_indexes(self: &Arc<Self>, new: &Arc<Snapshot>, fresh: &[Arc<DataFile>]) {
        let rt = &self.db.index;
        let Ok(specs) = parse_specs(&new.meta.configuration) else {
            return;
        };
        if specs.is_empty()
            || matches!(rt.cache.mode(), crate::index::cache::CacheMode::Disabled)
                && specs.iter().all(|s| s.kind != "fts")
        {
            return;
        }
        let state = rt.table(self.idx_prefix().as_ref());
        let clock = self.db.clock();
        let map = if matches!(rt.cache.mode(), crate::index::cache::CacheMode::Disabled) {
            None
        } else {
            state
                .discovery
                .map(
                    self.db.store.object_store(),
                    &self.idx_prefix(),
                    new.version,
                    clock.now(),
                    rt.cfg.poll,
                )
                .await
                .ok()
        };
        for spec in specs {
            #[cfg(feature = "fts")]
            if spec.kind == crate::index::fts::KIND {
                if state
                    .text_searched
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    crate::index::fts::search::prewarm(self, new, &spec, fresh, map.clone()).await;
                }
                continue;
            }
            let (Some(map), Ok(kind)) = (&map, rt.kind(&spec.kind)) else {
                continue;
            };
            let hash = spec_hash(kind.as_ref(), &spec);
            for f in fresh {
                if f.rows().is_some_and(|r| !kind.wants_sidecar(&spec, r)) {
                    continue;
                }
                let key = (
                    crate::index::discover::data_segment(&f.path),
                    spec.name.clone(),
                );
                let Some(listed) = map
                    .get(&key)
                    .and_then(|v| v.iter().find(|l| l.spec_hash == hash).cloned())
                else {
                    continue;
                };
                let _ = crate::index::query::open_listed(
                    self,
                    kind.as_ref(),
                    &spec,
                    &hash,
                    f,
                    &listed,
                    &state,
                )
                .await;
            }
        }
    }

    /// Whether a query under [`Consistency::Background`] should refresh by itself: the task
    /// has not confirmed the snapshot for [`STALE_INTERVALS`] intervals and a second.
    pub(crate) fn background_stale(&self, interval: Duration, snap: &Snapshot) -> bool {
        let limit = interval * STALE_INTERVALS + Duration::from_secs(1);
        self.db.clock().now().saturating_sub(snap.confirmed_at) > limit
    }
}
