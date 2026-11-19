//! Purge and erasure (D66): collect removed files now instead of after the grace period, and
//! take the values of erased rows out of the log, its checkpoints and pin manifests.
//!
//! A purge runs under the `purge` lease and starts by committing its marker: the table property
//! `moraine.purged`, set to the version the marker follows (P), fenced by the lease. From then
//! on no version before P can be pinned, every checkpoint older than P is stale, and so is
//! every pin manifest of a version before P. Then, at the version in hand (S, at or after the
//! marker):
//!
//! 1. **Keep set.** The files live at S, the files named by the manifests of S's pins, and the
//!    files removed by entries after P (they were live at P, which stays pinnable).
//! 2. **Collect now.** A listed data file outside the keep set goes when the entry that removed
//!    it (at or before P) was committed before the cutoff: the caller's statement that no
//!    reader holds a snapshot loaded before it. When the cutoff is the purge's start, an entry
//!    this handle had loaded or committed before the purge started was committed before it,
//!    whatever its timestamp says (D93). A file no walked entry removed is an orphan or
//!    a removal older than the walk, and goes by vacuum's rule (older than the grace period),
//!    so an upload of a commit in flight is never taken. The blobs only collected files
//!    reference and the collected files' sidecars go with them, whatever their age.
//! 3. **Erase** (optional). A checkpoint at S; then every entry up to P is rewritten without the
//!    minimum and maximum of the erased files (those collected, and those removed before the
//!    cutoff that are gone already) and without commit information that may hold values;
//!    then the checkpoints older than P and the manifests of versions before P are deleted.
//!
//! Nothing a commit references is deleted: the keep set holds every file a version from P on
//! names, and pins of older versions are refused from the marker on. A maintenance pass that
//! read the log before the marker and writes a checkpoint older than P afterwards deletes it
//! once it sees the marker and a checkpoint at or after P; until that checkpoint exists, the
//! purge's own deletion of checkpoints older than P takes it. Log cleanup deletes checkpoints
//! older than the newest purge, and vacuum such manifests, whatever their age, so an object a
//! crashed client leaves behind goes on the next pass.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;

use super::logjob::grace;
use super::vacuum::manifest_version;
use crate::error::{Error, Result};
use crate::log::LogObjectKind;
use crate::table::{Table, TableInner};

/// What [`Table::purge`] does.
///
/// ```no_run
/// # async fn f(t: moraine::Table, before: chrono::DateTime<chrono::Utc>) -> moraine::Result<()> {
/// use moraine::Purge;
/// // Collect at once every file removed by a commit older than `before` (no reader holds a
/// // snapshot loaded earlier), and erase their values from the log.
/// let report = t.purge(&Purge::new().removed_before(before).erase()).await?;
/// println!("{} files collected", report.files_deleted);
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct Purge {
    removed_before: Option<DateTime<Utc>>,
    erase: bool,
    lease_ttl: Duration,
}

impl Default for Purge {
    fn default() -> Self {
        Purge {
            removed_before: None,
            erase: false,
            lease_ttl: Duration::from_secs(300),
        }
    }
}

impl Purge {
    /// Collects every file removed by a commit before the purge starts: the caller states
    /// that no reader holds a snapshot loaded before then. A commit this handle made or loaded
    /// before calling the purge counts, so a purge right after the caller's own rewrite collects
    /// what it removed, without waiting for the store's clock.
    pub fn new() -> Purge {
        Purge::default()
    }

    /// Collects only files removed by commits before `t` (server time): the caller states that
    /// no reader holds a snapshot loaded before `t`. A later `t` than the purge's start counts
    /// as its start.
    pub fn removed_before(mut self, t: DateTime<Utc>) -> Purge {
        self.removed_before = Some(t);
        self
    }

    /// Also erases the collected files' values from the log: statistics of their columns in
    /// log entries, checkpoints older than the purge, and manifests of pins of older versions.
    pub fn erase(mut self) -> Purge {
        self.erase = true;
        self
    }

    /// How long the `purge` lease lasts (default 5 minutes).
    pub fn lease_ttl(mut self, ttl: Duration) -> Purge {
        self.lease_ttl = ttl;
        self
    }
}

/// What one purge did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PurgeReport {
    /// The version the purge followed (the table property `moraine.purged`): versions before
    /// it can no longer be pinned.
    pub version: u64,
    /// Data files deleted.
    pub files_deleted: usize,
    /// Bytes of the data files deleted.
    pub bytes_deleted: u64,
    /// Sidecars deleted: those of the files deleted, and those vacuum would collect.
    pub sidecars_deleted: usize,
    /// Blob objects only the deleted files referenced.
    pub blobs_deleted: usize,
    /// Files whose statistics were erased from the log ([`Purge::erase`]).
    pub files_erased: usize,
    /// Log entries rewritten without erased values.
    pub entries_scrubbed: usize,
    /// The checkpoint written for the erasure, when a new one was needed.
    pub checkpoint: Option<u64>,
    /// Checkpoints older than the purge, deleted.
    pub checkpoints_deleted: usize,
    /// Pin manifests of versions older than the purge, deleted.
    pub manifests_deleted: usize,
}

impl Table {
    /// Collects removed data files now rather than after the grace period, and with
    /// [`Purge::erase`] takes their values out of the log (D66). Runs under the `purge` lease:
    /// [`Error::LeaseHeld`] while another client purges, [`Error::Fenced`] when another
    /// purge took the lease over first. Safe alongside writers and maintenance; readers of
    /// snapshots loaded before the cutoff may find files gone ([`Error::SnapshotExpired`]).
    pub async fn purge(&self, p: &Purge) -> Result<PurgeReport> {
        self.inner.purge(p).await
    }
}

impl TableInner {
    pub(crate) async fn purge(self: &Arc<Self>, p: &Purge) -> Result<PurgeReport> {
        // Every version up to this one was committed before the purge started (D93).
        let seen = self.current().version;
        let now = self.server_time().await?;
        let leases = self.leases(&self.db.writer_id);
        let Some(lease) = leases.acquire("purge", p.lease_ttl, now).await? else {
            return Err(Error::LeaseHeld {
                job: "purge".into(),
            });
        };
        let r = self.purge_leased(p, now, seen, lease.fence()).await;
        let at = self.server_time().await.unwrap_or(now);
        let _ = self.leases(&self.db.writer_id).release(&lease, at).await;
        r
    }

    async fn purge_leased(
        self: &Arc<Self>,
        p: &Purge,
        now: DateTime<Utc>,
        seen: u64,
        fence: crate::log::TxnId,
    ) -> Result<PurgeReport> {
        let mut report = PurgeReport::default();
        let cutoff = p.removed_before.map_or(now, |t| t.min(now));
        // Server time comes from last-modified times, which lag by up to their resolution (a
        // second on S3), so an entry committed just before the purge can look as old as its
        // start. When the cutoff is the start, the entries this handle had in hand then were
        // committed before it: their order is known, their timestamps are not needed (D93).
        let from_start = cutoff == now;
        let before_cutoff = |at: DateTime<Utc>, v: u64| at < cutoff || (from_start && v <= seen);
        self.change_indexes(crate::table::Change::Purge(Some(fence)))
            .await?;
        fail::fail_point!("moraine::purge::after_marker");
        let snap = self.current();
        let pv = crate::table::purged(&snap.meta.configuration)?
            .ok_or_else(|| Error::Internal("the purge marker is missing".into()))?;
        report.version = pv;
        let grace_cut = now - grace(&snap.def);

        // The keep set: S's files, its pins' files, and what entries after P removed.
        let mut keep: HashSet<String> = snap.files.iter().map(|f| f.path.clone()).collect();
        for (name, version) in crate::table::parse_pins(&snap.meta.configuration)? {
            let entry = self.read_pin_manifest(&name, version).await?;
            keep.extend(entry.adds.into_iter().map(|f| f.path));
        }
        // Entries newest first: removals with the entry's server time, and the versions an
        // erasure rewrites. Without erasure the walk ends at the grace period, as vacuum's.
        let mut removed: HashMap<String, (DateTime<Utc>, u64)> = HashMap::new();
        let mut walked: Vec<u64> = Vec::new();
        let mut v = snap.version;
        while v > 0 {
            let Some((e, at)) = self.layer.read_entry_at(v, None).await? else {
                break;
            };
            if v > pv {
                keep.extend(e.removes);
            } else {
                if !p.erase && at < grace_cut {
                    break;
                }
                walked.push(v);
                for r in e.removes {
                    removed.entry(r).or_insert((at, v));
                }
            }
            v -= 1;
        }

        // Collect now.
        let store = self.db.store.object_store().clone();
        let data = self.layer.data_prefix().clone();
        let listed: Vec<_> = store.list(Some(&data)).try_collect().await?;
        let mut doomed: Vec<(String, u64)> = Vec::new();
        // Every data file that stays, kept or not yet collectable: its sidecars and blobs stay.
        let mut stays: HashSet<String> = keep.clone();
        for m in listed {
            let Some(rel) = m.location.prefix_match(&data) else {
                continue;
            };
            let parts: Vec<String> = rel.map(|p| p.as_ref().to_string()).collect();
            if parts
                .iter()
                .any(|p| p.starts_with('_') || p.starts_with('.'))
                || !parts.last().is_some_and(|n| n.ends_with(".parquet"))
            {
                continue;
            }
            let path = parts.join("/");
            if keep.contains(&path) {
                continue;
            }
            let goes = match removed.get(&path) {
                Some(&(at, v)) => before_cutoff(at, v),
                None => m.last_modified < grace_cut,
            };
            if goes {
                doomed.push((path, m.size));
            } else {
                stays.insert(path);
            }
        }
        let doomed_paths: HashSet<String> = doomed.iter().map(|(p, _)| p.clone()).collect();
        report.blobs_deleted = self.purge_blobs(&snap, &stays, &doomed_paths).await?;
        report.sidecars_deleted = self
            .collect_sidecars(&snap, &stays, grace_cut, &doomed_paths)
            .await?;
        fail::fail_point!("moraine::purge::before_delete");
        for (path, size) in &doomed {
            match store.delete(&data.clone().join(path.as_str())).await {
                Ok(()) => {
                    report.files_deleted += 1;
                    report.bytes_deleted += size;
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        if !p.erase {
            return Ok(report);
        }
        fail::fail_point!("moraine::purge::after_delete");

        // Erase: what was removed before the cutoff and is not kept, collected now or before.
        let erased: HashSet<String> = removed
            .iter()
            .filter(|&(path, &(at, v))| before_cutoff(at, v) && !keep.contains(path))
            .map(|(path, _)| path.clone())
            .chain(doomed_paths)
            .collect();
        report.files_erased = erased.len();
        // A checkpoint from the marker on, so older ones can go; its files are all kept.
        report.checkpoint = self
            .checkpoint_and_clean(Some(1), now, false)
            .await?
            .checkpoint;
        fail::fail_point!("moraine::purge::after_checkpoint");
        for v in &walked {
            if self.layer.scrub_entry(*v, &erased).await? {
                report.entries_scrubbed += 1;
            }
        }
        fail::fail_point!("moraine::purge::after_scrub");
        let objects = self.layer.list_log_objects().await?;
        if objects
            .iter()
            .any(|o| o.kind == LogObjectKind::Checkpoint && o.version >= pv)
        {
            let stale: Vec<_> = objects
                .into_iter()
                .filter(|o| o.kind == LogObjectKind::Checkpoint && o.version < pv)
                .map(|o| o.location)
                .collect();
            report.checkpoints_deleted = self.layer.delete_log_objects(stale).await?;
        }
        let live: HashSet<String> = crate::table::parse_pins(&self.current().meta.configuration)?
            .iter()
            .map(|(n, v)| self.pin_manifest(n, *v).as_ref().to_string())
            .collect();
        let pins = self.meta_prefix().join("pins");
        let listed: Vec<_> = store.list(Some(&pins)).try_collect().await?;
        for m in listed {
            if live.contains(m.location.as_ref())
                || manifest_version(&m.location).is_none_or(|v| v >= pv)
            {
                continue;
            }
            match store.delete(&m.location).await {
                Ok(()) => report.manifests_deleted += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(report)
    }

    /// Deletes the blob objects that files in `doomed` reference and no file in `keep` (every
    /// data file that stays) does.
    /// A table without blob columns makes no request.
    async fn purge_blobs(
        self: &Arc<Self>,
        snap: &crate::snapshot::Snapshot,
        keep: &HashSet<String>,
        doomed: &HashSet<String>,
    ) -> Result<usize> {
        if doomed.is_empty() || crate::blob::columns(snap.schema()).is_empty() {
            return Ok(0);
        }
        let mut marked: HashSet<String> = HashSet::new();
        for p in keep {
            match self.blob_keys_of(snap, p).await {
                Ok(k) => marked.extend(k.iter().cloned()),
                // A kept file that cannot be read: delete no blob (case B11).
                Err(e) if e.is_missing_object() => return Ok(0),
                Err(e) => return Err(e),
            }
        }
        let store = self.db.store.object_store().clone();
        let prefix = self.blob_prefix();
        let mut deleted = 0;
        for p in doomed {
            let keys = match self.blob_keys_of(snap, p).await {
                Ok(k) => k,
                Err(e) if e.is_missing_object() => continue,
                Err(e) => return Err(e),
            };
            for k in keys.iter().filter(|k| !marked.contains(*k)) {
                match store.delete(&crate::blob::join_key(&prefix, k)).await {
                    Ok(()) => deleted += 1,
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(deleted)
    }
}
