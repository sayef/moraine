//! Vacuum and sidecar collection on server time (D11, D14, D50).
//!
//! Vacuum keeps a data file when the current snapshot holds it, when a pin's manifest names it,
//! when a log entry younger than the grace period removed it (a reader of an older snapshot may
//! still need it), or when the object itself is younger than the grace period (a write that has
//! not committed yet). Everything else in `data/` goes. Ages are server modification times
//! compared with server time, so client clocks play no part (D11).
//!
//! The recently removed files come from the log entries themselves, read backwards from the
//! version in hand until one is older than the grace period, never from a listing, so a listing
//! that is cut short can only make vacuum delete less (scenario 18). Entries already read are
//! kept per handle, so a later pass reads only the new ones.
//!
//! Sidecar collection deletes a sidecar older than the grace period when its data file is not
//! kept by the same rules, or its index no longer exists, or its spec hash is not the index's
//! current one. Pin manifests of pins that no longer exist, and server-time probes a crashed
//! client left behind, go after the grace period too.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;

use super::logjob::grace;
use crate::error::Result;
use crate::index::discover::{data_segment, parse};
use crate::index::spec::{parse_specs, spec_hash};
use crate::snapshot::Snapshot;
use crate::table::TableInner;

/// What vacuum and the collectors did in one pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct VacuumReport {
    /// Data files deleted.
    pub files_deleted: usize,
    /// Bytes of those files.
    pub bytes_deleted: u64,
    /// Sidecars deleted: of deleted files, or of specs no longer current.
    pub sidecars_deleted: usize,
    /// Pin manifests and server-time probes.
    pub meta_objects_deleted: usize,
    /// Files kept only because a pin names them.
    pub kept_for_pins: usize,
    /// Blob objects no kept row references, older than the grace period.
    pub blobs_deleted: usize,
    /// Bytes of those blob objects.
    pub blob_bytes_deleted: u64,
    /// Blob references found in the kept files (the mark set).
    pub blobs_referenced: usize,
    /// Blob collection stopped before deleting anything more: the pass ran past half the
    /// grace period, or a kept file could not be read (case B11).
    pub blob_collection_aborted: bool,
}

impl TableInner {
    /// Data file locations vacuum must keep at `snap` and server time `now`.
    async fn keep_set(
        self: &Arc<Self>,
        snap: &Snapshot,
        now: DateTime<Utc>,
    ) -> Result<(HashSet<String>, usize)> {
        let mut keep: HashSet<String> = snap.files.iter().map(|f| f.path.clone()).collect();
        for p in self.recent_removals(snap, now).await? {
            keep.insert(p);
        }
        let mut pinned_only = 0;
        for (name, version) in crate::table::parse_pins(&snap.meta.configuration)? {
            let entry = self.read_pin_manifest(&name, version).await?;
            for f in entry.adds {
                if keep.insert(f.path) {
                    pinned_only += 1;
                }
            }
        }
        Ok((keep, pinned_only))
    }

    /// Paths removed by log entries younger than the grace period, up to `snap`'s version.
    async fn recent_removals(
        self: &Arc<Self>,
        snap: &Snapshot,
        now: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        let cutoff = now - grace(&snap.def);
        let mut out = Vec::new();
        let mut v = snap.version;
        while v > 0 {
            let cached = self
                .maint
                .lock()
                .ok()
                .and_then(|g| g.removals.get(&v).cloned());
            let (at, removes) = match cached {
                Some(c) => c,
                None => match self.layer.read_entry_at(v, None).await? {
                    Some((e, at)) => {
                        let c = (at, e.removes);
                        if let Ok(mut g) = self.maint.lock() {
                            g.removals.insert(v, c.clone());
                        }
                        c
                    }
                    // Cleaned up: log cleanup removes only entries older than the grace period.
                    None => break,
                },
            };
            if at < cutoff {
                break;
            }
            out.extend(removes);
            v -= 1;
        }
        if let Ok(mut g) = self.maint.lock() {
            // Entries below the one that ended the walk are older still: never needed again.
            g.removals = g.removals.split_off(&v);
        }
        Ok(out)
    }

    pub(crate) async fn vacuum(
        self: &Arc<Self>,
        now: DateTime<Utc>,
        sidecars: bool,
    ) -> Result<VacuumReport> {
        let mut report = VacuumReport::default();
        let started = self.db.clock().now();
        let snap = self.refresh().await?;
        let cutoff = now - grace(&snap.def);
        let (keep, pinned_only) = self.keep_set(&snap, now).await?;
        report.kept_for_pins = pinned_only;
        let store = self.db.store.object_store().clone();
        let data = self.layer.data_prefix().clone();
        let keep_locations: HashSet<String> = keep
            .iter()
            .map(|p| data.clone().join(p.as_str()).as_ref().to_string())
            .collect();
        let listed: Vec<_> = store.list(Some(&data)).try_collect().await?;
        let mut doomed: Vec<(Path, u64)> = Vec::new();
        for m in listed {
            let Some(rel) = m.location.prefix_match(&data) else {
                continue;
            };
            let parts: Vec<String> = rel.map(|p| p.as_ref().to_string()).collect();
            // Only data files: nothing under `_delta_log/` or another hidden directory.
            if parts
                .iter()
                .any(|p| p.starts_with('_') || p.starts_with('.'))
                || !parts.last().is_some_and(|n| n.ends_with(".parquet"))
            {
                continue;
            }
            if keep_locations.contains(m.location.as_ref()) || m.last_modified >= cutoff {
                continue;
            }
            doomed.push((m.location, m.size));
        }
        fail::fail_point!("moraine::vacuum::before_delete");
        for (loc, size) in doomed {
            match store.delete(&loc).await {
                Ok(()) => {
                    report.files_deleted += 1;
                    report.bytes_deleted += size;
                }
                // Another collector got there first (case G3).
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        if sidecars {
            report.sidecars_deleted = self
                .collect_sidecars(&snap, &keep, cutoff, &HashSet::new())
                .await?;
        }
        report.meta_objects_deleted = self.collect_meta(&snap, cutoff).await?;
        if sidecars {
            self.collect_blobs(&snap, &keep, cutoff, started, &mut report)
                .await?;
        }
        Ok(report)
    }

    /// The external blob keys of data file `path` (relative to `data/`), cached per handle:
    /// data files never change.
    pub(crate) async fn blob_keys_of(
        self: &Arc<Self>,
        snap: &Snapshot,
        path: &str,
    ) -> Result<Arc<HashSet<String>>> {
        if let Some(k) = self
            .blob_keys
            .lock()
            .ok()
            .and_then(|g| g.get(path).cloned())
        {
            return Ok(k);
        }
        let schema = snap.schema();
        let cols: Vec<String> = crate::blob::columns(schema).into_iter().collect();
        let mut keys = HashSet::new();
        if !cols.is_empty() {
            let at = self.layer.data_prefix().clone().join(path);
            let store = self.db.store.object_store();
            let size = match snap.files.iter().find(|f| f.path == path) {
                Some(f) => f.size,
                None => store.head(&at).await?.size,
            };
            let q = crate::read::FileQuery {
                table_schema: schema.clone(),
                output: cols.clone(),
                needed: crate::schema::project_schema(schema, &cols)?,
                filter: None,
                physical: None,
            };
            for b in crate::read::read_file(&self.db.cache, store, &at, size, &q).await? {
                crate::blob::keys_of(&b, &mut keys)?;
            }
        }
        let keys = Arc::new(keys);
        if let Ok(mut g) = self.blob_keys.lock() {
            g.insert(path.to_string(), keys.clone());
        }
        Ok(keys)
    }

    /// Blob collection: mark the keys every kept file references, list
    /// `blobs/`, delete what is unmarked and older than the grace period. A table without blob
    /// columns makes no request. The pass deletes nothing once it has run for half the grace
    /// period, and a kept file that cannot be read aborts it (case B11).
    async fn collect_blobs(
        self: &Arc<Self>,
        snap: &Snapshot,
        keep: &HashSet<String>,
        cutoff: DateTime<Utc>,
        started: std::time::Duration,
        report: &mut VacuumReport,
    ) -> Result<()> {
        if crate::blob::columns(snap.schema()).is_empty() {
            return Ok(());
        }
        let clock = self.db.clock().clone();
        let half = (grace(&snap.def) / 2).to_std().unwrap_or_default();
        let overrun = || clock.now().saturating_sub(started) >= half;
        let mut marked: HashSet<String> = HashSet::new();
        for p in keep {
            match self.blob_keys_of(snap, p).await {
                Ok(k) => marked.extend(k.iter().cloned()),
                Err(e) if e.is_missing_object() => {
                    report.blob_collection_aborted = true;
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
        if let Ok(mut g) = self.blob_keys.lock() {
            g.retain(|p, _| keep.contains(p));
        }
        report.blobs_referenced = marked.len();
        fail::fail_point!("moraine::blobs::after_mark");
        let store = self.db.store.object_store().clone();
        let prefix = self.blob_prefix();
        let listed: Vec<_> = store.list(Some(&prefix)).try_collect().await?;
        for m in listed {
            if m.last_modified >= cutoff {
                continue;
            }
            let Some(rel) = m.location.prefix_match(&prefix) else {
                continue;
            };
            let key = rel
                .map(|p| p.as_ref().to_string())
                .collect::<Vec<_>>()
                .join("/");
            if marked.contains(&key) {
                continue;
            }
            if overrun() {
                report.blob_collection_aborted = true;
                return Ok(());
            }
            match store.delete(&m.location).await {
                Ok(()) => {
                    report.blobs_deleted += 1;
                    report.blob_bytes_deleted += m.size;
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Sidecars of files not in `keep` older than `cutoff`, of stale specs, and of the files in
    /// `doomed` (data files a purge deletes, D66) whatever their age.
    pub(crate) async fn collect_sidecars(
        self: &Arc<Self>,
        snap: &Snapshot,
        keep: &HashSet<String>,
        cutoff: DateTime<Utc>,
        doomed: &HashSet<String>,
    ) -> Result<usize> {
        let doomed: HashSet<String> = doomed.iter().map(|p| data_segment(p)).collect();
        let segments: HashSet<String> = keep.iter().map(|p| data_segment(p)).collect();
        // Index name to its current spec hash, when this client knows the kind.
        let mut current: BTreeMap<String, Option<String>> = BTreeMap::new();
        let specs = parse_specs(&snap.meta.configuration)?;
        for spec in specs
            .into_iter()
            .chain(crate::index::key::key_spec(&snap.def))
        {
            let hash = self
                .db
                .index
                .kind(&spec.kind)
                .ok()
                .map(|k| spec_hash(k.as_ref(), &spec));
            current.insert(spec.name.clone(), hash);
        }
        let store = self.db.store.object_store().clone();
        let idx = self.idx_prefix();
        let listed: Vec<_> = store.list(Some(&idx)).try_collect().await?;
        let mut deleted = 0;
        for m in listed {
            let Some((segment, index, hash)) = parse(&idx, &m.location) else {
                continue;
            };
            if m.last_modified >= cutoff && !doomed.contains(&segment) {
                continue;
            }
            let live_file = segments.contains(&segment);
            let live_spec = match current.get(&index) {
                None => false,
                Some(None) => true,
                Some(Some(h)) => *h == hash,
            };
            if live_file && live_spec {
                continue;
            }
            match store.delete(&m.location).await {
                Ok(()) => deleted += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(deleted)
    }

    /// Pin manifests of removed pins, and abandoned server-time probes.
    async fn collect_meta(
        self: &Arc<Self>,
        snap: &Snapshot,
        cutoff: DateTime<Utc>,
    ) -> Result<usize> {
        let pins = crate::table::parse_pins(&snap.meta.configuration)?;
        let live: HashSet<String> = pins
            .iter()
            .map(|(n, v)| self.pin_manifest(n, *v).as_ref().to_string())
            .collect();
        let purged = crate::table::purged(&snap.meta.configuration)?;
        let store = self.db.store.object_store().clone();
        let mut deleted = 0;
        for dir in ["pins", "clock"] {
            let prefix = self.meta_prefix().join(dir);
            let listed: Vec<_> = store.list(Some(&prefix)).try_collect().await?;
            for m in listed {
                if live.contains(m.location.as_ref()) {
                    continue;
                }
                // A manifest of a version older than the newest purge: no pin of it can
                // commit any more, and it may name erased values (D66).
                let stale = dir == "pins"
                    && purged.is_some_and(|p| manifest_version(&m.location).is_some_and(|v| v < p));
                if m.last_modified >= cutoff && !stale {
                    continue;
                }
                match store.delete(&m.location).await {
                    Ok(()) => deleted += 1,
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(deleted)
    }
}

/// The version a pin manifest (`meta/pins/<name>/<version>.json`) is of.
pub(crate) fn manifest_version(location: &Path) -> Option<u64> {
    location.filename()?.strip_suffix(".json")?.parse().ok()
}
