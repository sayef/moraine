//! One commit of a group: prepare it against a snapshot,
//! put its log entry, and on a lost race decide whether to rebase or to re-execute.
//!
//! Every kind of write runs through [`TableInner::run_job`]:
//!
//! - **Append**: data files are uploaded once and rebased over anything except a change of
//!   schema. A schema that only gained nullable columns keeps the uploaded files (they read the
//!   new columns as null) and re-encodes the entry; any other change conforms the rows to the new
//!   schema and uploads them again (D9).
//! - **Rewrite** (upsert, delete, delete by key, update): the files that may hold matching rows
//!   are read, those that do are rewritten without (or with updated) matching rows, and the new
//!   rows of an upsert are written. When the version is taken, the entry there conflicts if it
//!   changed the metadata, removed a file this write removes, or added a file that holds a
//!   matching row (checked exactly, by reading that file's key or filter columns); otherwise the
//!   same entry is put at the next version. A conflict re-executes against the newer snapshot,
//!   reusing every file already rewritten or uploaded whose source is still live.
//! - **Add column**: a metadata entry; it rebases over data changes and re-executes over another
//!   metadata change.
//!
//! Lost races are taken in no fixed order, except that a job that has lost twice its fair share
//! (two races per contending writer) gets a head start of half a round trip over the others
//! ([`TableInner::turn_wait`], D28).
//!
//! Callers' transaction ids (D32) ride along in the entry as extra `txn` actions. A job stops
//! as superseded when the snapshot it prepares against, or an entry it loses a race to, already
//! holds one of them at the same or a later version; the group then answers that member
//! "already applied" and commits the rest again ([`super::write`]). Every entry between the
//! job's snapshot and the version it commits is read on the way (a lost race reads the winner's
//! entry), so a transaction id committed by any other writer is always seen before the put
//! succeeds. A job with caller ids commits even when it changes no row, so that the id is
//! recorded.
//!
//! The commit clock starts when the job starts. If an attempt finds it past the commit deadline,
//! the job starts once more with a fresh clock and fresh uploads (case C6); a second overrun is
//! returned as [`Error::CommitDeadlineExceeded`] (case C20). Nothing late is ever committed.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, concat_batches, filter_record_batch, not, prep_null_mask_filter};
use arrow::datatypes::{Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::{col, lit};
use futures::FutureExt;
use object_store::{ObjectStoreExt, PutPayload};

use super::{TableInner, transient};
use crate::commit::{AttemptError, ConflictKind, commit_with_retries};
use crate::error::{Error, Result};
use crate::log::{DataFile, LogEntry, NewEntry, TableMeta, TxnId};
use crate::read::prune::{Bounds, may_match};
use crate::read::{FileQuery, bloom_excludes, read_file, to_schema};
use crate::schema::{column_not_found, conform, key_rows, project_schema, validate_new_schema};
use crate::snapshot::Snapshot;
use crate::stats::ColRange;
use crate::write as files;

/// Lost races handled inside one attempt before the retry loop backs off.
const MAX_REBASES: u32 = 1_000;

/// At most this many distinct values per key column go into a pruning `IN` list.
const MAX_PRUNE_VALUES: usize = 1_024;

/// Rows a rewrite looks for.
pub(crate) enum Matcher {
    /// Rows whose primary key is in the set.
    Keys(Keys),
    /// Rows for which the filter is true.
    Predicate(Expr),
}

/// A set of primary keys.
#[derive(Clone)]
pub(crate) struct Keys {
    pub cols: Vec<String>,
    /// Order-preserving encodings of the keys ([`key_rows`]).
    pub set: HashSet<Vec<u8>>,
    /// A conservative filter for pruning files and pages: `IN` lists of the distinct values of
    /// each key column, when they are few enough.
    pub prune: Option<Expr>,
}

impl Keys {
    /// The keys of `batches` (which hold at least the key columns, with the table's types).
    pub fn of(cols: &[String], batches: &[RecordBatch]) -> Result<Keys> {
        let mut set = HashSet::new();
        let mut distinct: Vec<HashSet<ScalarValue>> = vec![HashSet::new(); cols.len()];
        for b in batches {
            for r in key_rows(b, cols)?.iter() {
                set.insert(r.as_ref().to_vec());
            }
            for (i, c) in cols.iter().enumerate() {
                if distinct[i].len() > MAX_PRUNE_VALUES {
                    continue;
                }
                let a = b
                    .column_by_name(c)
                    .ok_or_else(|| Error::Internal(format!("key column {c} missing")))?;
                for row in 0..a.len() {
                    distinct[i].insert(ScalarValue::try_from_array(a, row)?);
                }
            }
        }
        let prune = cols
            .iter()
            .zip(distinct)
            .filter(|(_, d)| d.len() <= MAX_PRUNE_VALUES)
            .map(|(c, d)| col(c.as_str()).in_list(d.into_iter().map(lit).collect(), false))
            .reduce(Expr::and);
        Ok(Keys {
            cols: cols.to_vec(),
            set,
            prune,
        })
    }

    /// Merges `other` into this set.
    pub fn extend(&mut self, other: Keys) {
        self.set.extend(other.set);
        self.prune = match (self.prune.take(), other.prune) {
            (Some(a), Some(b)) => Some(a.or(b)),
            _ => None,
        };
    }

    pub fn overlaps(&self, other: &Keys) -> bool {
        let (small, large) = if self.set.len() <= other.set.len() {
            (&self.set, &other.set)
        } else {
            (&other.set, &self.set)
        };
        small.iter().any(|k| large.contains(k))
    }
}

/// What a group commits.
pub(crate) enum Work {
    Append(Vec<RecordBatch>),
    Rewrite(Rewrite),
    AddColumn(Field),
    Replace(ReplaceWork),
}

/// An atomic swap of files (D48): compaction, and [`crate::Table::replace_files`].
#[derive(Clone)]
pub(crate) struct ReplaceWork {
    /// Delta's operation name: "OPTIMIZE" for compaction, "REPLACE" otherwise.
    pub operation: &'static str,
    /// Paths of the files to remove, as the snapshot names them.
    pub remove: Vec<String>,
    /// New rows to write as files of the table's layout.
    pub batches: Vec<RecordBatch>,
    /// Files already uploaded (compaction writes them, and their sidecars, before it commits).
    pub ready: Vec<DataFile>,
    /// Tags of the files written from `batches`.
    pub tags: std::collections::BTreeMap<String, String>,
    /// Whether rows change. A swap that keeps every row (compaction) takes the place of the
    /// first file it removes in the snapshot's file order.
    pub data_change: bool,
    /// The lease generation that fences the commit (D29): `app_id` names the job.
    pub fence: Option<TxnId>,
    /// When the uploads of `ready` began (client monotonic time): the commit clock starts
    /// there, not when the commit does, so the files are committed within the commit deadline
    /// of their upload or not at all, as a writer's own are.
    pub started: Option<Duration>,
    /// Transaction ids recorded with the swap, each with the first version it covers (queued
    /// writes: per submitter its last sequence, and its first one in this swap, D51). The
    /// commit stops with [`Error::AlreadyIngested`] once the log holds any version it covers.
    pub txns: Vec<(TxnId, i64)>,
}

/// A merge, delete or update.
pub(crate) struct Rewrite {
    pub operation: &'static str,
    pub matcher: Matcher,
    /// Assignments of an update; `None` removes the matching rows.
    pub assignments: Option<Vec<(String, Expr)>>,
    /// New rows of an upsert, conformed to the schema they were validated against.
    pub insert: Vec<RecordBatch>,
}

/// What one source file turned into, for reuse by a later attempt.
#[derive(Clone)]
enum FileOutcome {
    Untouched,
    Rewritten(Vec<DataFile>),
}

/// Everything prepared for one log entry.
struct Prepared {
    schema: SchemaRef,
    meta: Option<TableMeta>,
    adds: Vec<DataFile>,
    removes: Vec<DataFile>,
    body: Bytes,
}

/// State a job keeps across attempts.
#[derive(Default)]
struct JobState {
    /// Uploaded new rows (appends, upsert inserts) and the schema they were written with.
    uploaded: Option<(SchemaRef, Vec<DataFile>)>,
    /// Rewrites of source files, valid while the schema extends `reuse_schema`.
    reuse: HashMap<String, FileOutcome>,
    reuse_schema: Option<SchemaRef>,
    /// New rows with their blob columns in stored form (values uploaded, references copied),
    /// so a re-executed attempt does not upload them again.
    blobs: Option<Vec<RecordBatch>>,
}

/// How a prepared attempt ends before any put.
enum Ready {
    Commit(Prepared),
    /// Nothing to change at this version (case C19).
    Nothing(u64),
}

/// How this handle's recent log puts went, for the starvation guard (D28).
#[derive(Debug, Default)]
pub(crate) struct Contention {
    /// Moving average of a log put's round trip.
    rtt: Duration,
    /// Races the last committed job lost, and when it committed (client monotonic time).
    last_losses: u32,
    last_at: Duration,
    /// Writers of the other writers' log entries read most recently, newest last.
    writers: std::collections::VecDeque<String>,
}

/// Round trips below this are not staggered: timers cannot express a fraction of them.
const MIN_STAGGER_RTT: Duration = Duration::from_millis(2);
/// Entries remembered for counting the writers that contend.
const WRITER_WINDOW: usize = 64;
/// A job is starving once it has lost this many times the races a fair turn would cost
/// (one per contending writer).
const STARVING_TURNS: u32 = 2;
/// The head start of a starving job: this share of a log put's round trip.
const HEAD_START: f64 = 0.5;
/// How long after a contended commit a handle still treats the log as contended.
const CONTENDED_FOR: Duration = Duration::from_secs(1);

/// The objects a job uploaded, so those no commit references can be deleted once the job is
/// over (D65).
#[derive(Debug, Default)]
pub(crate) struct Uploads {
    /// One entry per round (a restart past the commit deadline starts a new one): data files
    /// (paths relative to the data root) and blob keys uploaded or copied in that round. Blob
    /// keys belong to the rows of the round's files, so a round whose files did not commit
    /// left its blobs unreferenced too.
    rounds: Vec<Round>,
    /// Versions a log put of the job targeted whose outcome was unknown.
    open: Vec<u64>,
    /// The data files of the entry the job committed, read from that entry.
    committed: Option<HashSet<String>>,
}

#[derive(Debug, Default)]
struct Round {
    files: Vec<String>,
    blobs: Vec<String>,
}

impl Uploads {
    fn round(&mut self) -> &mut Round {
        if self.rounds.is_empty() {
            self.rounds.push(Round::default());
        }
        let n = self.rounds.len();
        &mut self.rounds[n - 1]
    }

    pub(crate) fn file(&mut self, path: &str) {
        self.round().files.push(path.to_string());
    }

    /// Every data file recorded, all rounds.
    pub(crate) fn files(self) -> Vec<String> {
        self.rounds.into_iter().flat_map(|r| r.files).collect()
    }

    fn blobs(&mut self, keys: impl IntoIterator<Item = String>) {
        self.round().blobs.extend(keys);
    }
}

struct Job {
    work: Work,
    /// What the job uploaded (D65).
    uploads: Mutex<Uploads>,
    /// Races lost so far, over all attempts.
    losses: std::sync::atomic::AtomicU32,
    txn: TxnId,
    /// Callers' transaction ids (D32).
    caller: Vec<TxnId>,
    /// Set when one of `caller` turned out to be applied already.
    superseded: Arc<AtomicBool>,
    /// Commit clock start (client monotonic time).
    started: Mutex<Duration>,
    /// Set when the job started again past the commit deadline (case C6): blobs staged ahead
    /// of the write are copied then, as they can no longer be committed in time.
    restarted: AtomicBool,
    state: Mutex<JobState>,
}

impl Job {
    /// Records the data files of the entry the job committed (none: nothing to commit).
    fn committed(&self, files: HashSet<String>) {
        self.uploads
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .committed = Some(files);
    }

    fn fence(&self) -> Option<TxnId> {
        match &self.work {
            Work::Replace(r) => r.fence.clone(),
            _ => None,
        }
    }

    /// Ids written with the entry besides the job's own: callers', a replace's, the fence.
    fn entry_txns(&self) -> Vec<TxnId> {
        let mut t = self.caller.clone();
        if let Work::Replace(r) = &self.work {
            t.extend(r.txns.iter().map(|(t, _)| t.clone()));
        }
        t.extend(self.fence());
        t
    }

    /// Stops a replace when a commit already recorded a version its transaction ids cover:
    /// `committed(app_id)` is the newest version the log holds for it (D51).
    fn check_replace_txns(
        &self,
        committed: impl Fn(&str) -> Option<i64>,
    ) -> Result<(), AttemptError> {
        if let Work::Replace(r) = &self.work
            && let Some((t, _)) = r
                .txns
                .iter()
                .find(|(t, first)| committed(&t.app_id).is_some_and(|v| v >= *first))
        {
            return Err(AttemptError::Fatal(Error::AlreadyIngested(t.to_string())));
        }
        Ok(())
    }

    fn data_change(&self) -> bool {
        match &self.work {
            Work::Replace(r) => r.data_change,
            _ => true,
        }
    }

    /// Refuses the commit when `committed` (the newest version the log holds for the fence's
    /// job) is a newer lease generation than the job's own (D29).
    fn check_fence(&self, committed: Option<i64>) -> Result<(), AttemptError> {
        if let (Some(f), Some(current)) = (self.fence(), committed)
            && current > f.version
        {
            return Err(AttemptError::Fatal(Error::Fenced {
                job: f.app_id,
                generation: f.version,
                current,
            }));
        }
        Ok(())
    }
}

/// Whether `new` is `old` plus nullable columns at the end.
pub(crate) fn extends(old: &Schema, new: &Schema) -> bool {
    old.fields().len() <= new.fields().len()
        && old.fields().iter().zip(new.fields()).all(|(a, b)| a == b)
        && new.fields()[old.fields().len()..]
            .iter()
            .all(|f| f.is_nullable())
}

struct FileBounds<'a>(&'a DataFile);

impl Bounds for FileBounds<'_> {
    fn bounds(&self, column: &str) -> Option<ColRange> {
        self.0.stats.column(column).cloned()
    }
}

impl TableInner {
    /// Runs one group's write to its end: commits it, finds it has nothing to do, or fails.
    ///
    /// `superseded` is set when the job stopped because one of the `caller` transaction ids is
    /// applied already (the error returned then is only a placeholder).
    pub(super) async fn run_job(
        self: &Arc<Self>,
        work: Work,
        caller: Vec<TxnId>,
        put_started: Arc<AtomicBool>,
        superseded: Arc<AtomicBool>,
    ) -> Result<u64> {
        let clock = self.db.clock().clone();
        // Files uploaded before the job started cannot be uploaded again: such a job never
        // restarts, and its clock starts at their upload.
        let (started, may_restart) = match &work {
            Work::Replace(r) if !r.ready.is_empty() => {
                (r.started.unwrap_or(clock.now()).min(clock.now()), false)
            }
            _ => (clock.now(), true),
        };
        let mut uploads = Uploads::default();
        if let Work::Replace(r) = &work {
            // Compaction uploaded these for this commit alone.
            for f in &r.ready {
                uploads.file(&f.path);
            }
        }
        let job = Arc::new(Job {
            uploads: Mutex::new(uploads),
            work,
            txn: TxnId {
                app_id: self.db.writer_id.clone(),
                version: self.db.next_txn(),
            },
            caller,
            superseded,
            started: Mutex::new(started),
            restarted: AtomicBool::new(false),
            state: Mutex::new(JobState::default()),
            losses: std::sync::atomic::AtomicU32::new(0),
        });
        let mut restarted = false;
        let r = loop {
            match self.commit_job(job.clone(), put_started.clone()).await {
                Err(Error::CommitDeadlineExceeded { .. }) if !restarted && may_restart => {
                    // Case C6: start again from step 2, with fresh uploads.
                    restarted = true;
                    job.restarted.store(true, Ordering::SeqCst);
                    *job.started.lock().unwrap_or_else(|p| p.into_inner()) = clock.now();
                    *job.state.lock().unwrap_or_else(|p| p.into_inner()) = JobState::default();
                    job.uploads
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .rounds
                        .push(Round::default());
                }
                other => break other,
            }
        };
        let uploads = std::mem::take(&mut *job.uploads.lock().unwrap_or_else(|p| p.into_inner()));
        self.delete_uploads(uploads, &r, &job.txn).await;
        r
    }

    /// Deletes what a finished job uploaded that no commit references (D65): everything after a
    /// definite failure, and after a commit what the committed entry does not hold (files of a
    /// rewrite planned again, of a round restarted past the commit deadline, and their blobs).
    /// Nothing is deleted while the outcome is unknown, nor while a log put of the job whose
    /// outcome was unknown could still land: that needs its version taken by another writer's
    /// entry. Deletes are best effort; what is left, vacuum collects after the grace period.
    async fn delete_uploads(self: &Arc<Self>, u: Uploads, r: &Result<u64>, txn: &TxnId) {
        let total: usize = u.rounds.iter().map(|x| x.files.len() + x.blobs.len()).sum();
        if total == 0 {
            return;
        }
        let metrics = &self.write_metrics;
        let left = |n: usize| {
            metrics
                .uploads_left
                .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        };
        // Once an entry of the job is in the log, its files stay whatever the job returned.
        let committed: HashSet<String> = match (u.committed, r) {
            (Some(c), _) => c,
            (None, Err(Error::OutcomeUnknown { .. })) => return left(total),
            // Committed through an outcome looked up after the fact: read what it holds.
            (None, Ok(v)) => match self.layer.read_entry(*v, None).await {
                Ok(Some(e)) if e.has_txn(txn) => e.adds.into_iter().map(|f| f.path).collect(),
                _ => return left(total),
            },
            (None, Err(_)) => {
                // A put whose outcome was unknown can no longer land once another entry holds
                // its version; until then it might.
                for v in &u.open {
                    match self.layer.read_entry(*v, None).await {
                        Ok(Some(e)) if !e.has_txn(txn) => {}
                        _ => return left(total),
                    }
                }
                HashSet::new()
            }
        };
        let mut files: Vec<String> = Vec::new();
        let mut blobs: Vec<String> = Vec::new();
        for round in u.rounds {
            if round.files.iter().any(|f| committed.contains(f)) {
                files.extend(round.files.into_iter().filter(|f| !committed.contains(f)));
            } else {
                files.extend(round.files);
                blobs.extend(round.blobs);
            }
        }
        let (deleted, failed) = self.delete_objects(&files, &blobs).await;
        metrics
            .uploads_deleted
            .fetch_add(deleted as u64, std::sync::atomic::Ordering::Relaxed);
        left(failed);
    }

    /// Deletes data files (with their sidecars) and blob objects nothing references.
    /// Returns the objects deleted and those that could not be.
    pub(crate) async fn delete_objects(
        self: &Arc<Self>,
        files: &[String],
        blobs: &[String],
    ) -> (usize, usize) {
        use futures::{StreamExt, TryStreamExt};
        let store = self.db.store.object_store().clone();
        let data = self.layer.data_prefix().clone();
        let idx = self.idx_prefix();
        let snap = self.current();
        let indexed = !crate::index::spec::parse_specs(&snap.meta.configuration)
            .unwrap_or_default()
            .is_empty()
            || crate::index::key::key_spec(&snap.def).is_some();
        let mut locations: Vec<object_store::path::Path> = Vec::new();
        let mut failed = 0;
        for f in files {
            if indexed {
                // A data file's sidecars sit under `idx/<file>/`; the name is unique to it.
                let dir = idx.clone().join(f.as_str());
                match store.list(Some(&dir)).try_collect::<Vec<_>>().await {
                    Ok(listed) => locations.extend(listed.into_iter().map(|m| m.location)),
                    Err(_) => failed += 1,
                }
            }
            locations.push(data.clone().join(f.as_str()));
        }
        let prefix = self.blob_prefix();
        locations.extend(blobs.iter().map(|k| crate::blob::join_key(&prefix, k)));
        let n = self.db.opts.io_concurrency.max(1);
        let results: Vec<bool> = futures::stream::iter(locations)
            .map(|l| {
                let store = store.clone();
                async move {
                    matches!(
                        store.delete(&l).await,
                        Ok(()) | Err(object_store::Error::NotFound { .. })
                    )
                }
            })
            .buffered(n)
            .collect()
            .await;
        let deleted = results.iter().filter(|ok| **ok).count();
        (deleted, failed + results.len() - deleted)
    }

    async fn commit_job(
        self: &Arc<Self>,
        job: Arc<Job>,
        put_started: Arc<AtomicBool>,
    ) -> Result<u64> {
        let clock = self.db.clock().clone();
        // The snapshot just before the last put whose outcome was unknown.
        let open: Arc<Mutex<Option<Arc<Snapshot>>>> = Arc::new(Mutex::new(None));
        let attempt = {
            let (me, job, open) = (self.clone(), job.clone(), open.clone());
            move |_| {
                let (me, job, open, ps) =
                    (me.clone(), job.clone(), open.clone(), put_started.clone());
                async move { me.attempt(job, open, ps).await }.boxed()
            }
        };
        let resolve = {
            let (me, txn) = (self.clone(), job.txn.clone());
            move || {
                let (me, open, txn) = (me.clone(), open.clone(), txn.clone());
                async move { me.resolve(open, txn).await }.boxed()
            }
        };
        let (r, _stats) = commit_with_retries(
            &self.db.opts.commit_policy,
            clock.as_ref(),
            &job.txn.to_string(),
            attempt,
            resolve,
        )
        .await;
        r
    }

    /// How long to wait before a log put (D28). While the log is contended, every job that is
    /// not starving waits half a round trip, so a job that has lost twice its fair share of
    /// races puts first and wins the next version. Uncontended puts, and stores whose round
    /// trip is too short to divide, never wait.
    fn turn_wait(&self, losses: u32) -> Duration {
        let now = self.db.clock().now();
        let c = self.contention.lock().unwrap_or_else(|p| p.into_inner());
        let contended =
            losses > 0 || (c.last_losses > 0 && now.saturating_sub(c.last_at) < CONTENDED_FOR);
        if !contended || c.rtt < MIN_STAGGER_RTT {
            return Duration::ZERO;
        }
        let distinct: HashSet<&str> = c.writers.iter().map(String::as_str).collect();
        let writers = distinct.len() as u32 + 1;
        if losses >= STARVING_TURNS * writers {
            Duration::ZERO
        } else {
            c.rtt.mul_f64(HEAD_START)
        }
    }

    fn note_put(&self, took: Duration, r: &Result<(), AttemptError>, losses: u32) {
        let mut c = self.contention.lock().unwrap_or_else(|p| p.into_inner());
        c.rtt = if c.rtt.is_zero() {
            took
        } else {
            (c.rtt * 7 + took) / 8
        };
        if r.is_ok() {
            c.last_losses = losses;
            c.last_at = self.db.clock().now();
        }
    }

    /// Remembers who wrote an entry this handle lost a race to.
    fn note_writer(&self, e: &LogEntry) {
        let mut c = self.contention.lock().unwrap_or_else(|p| p.into_inner());
        for t in &e.txns {
            if t.app_id != self.db.writer_id {
                c.writers.push_back(t.app_id.clone());
                if c.writers.len() > WRITER_WINDOW {
                    c.writers.pop_front();
                }
            }
        }
    }

    /// Stops the job when `applied` names one of its callers' transaction ids (D32).
    fn check_superseded(job: &Job, applied: bool) -> Result<(), AttemptError> {
        if applied {
            job.superseded.store(true, Ordering::SeqCst);
            return Err(AttemptError::Fatal(Error::Internal(
                "a transaction id of the group is already applied".into(),
            )));
        }
        Ok(())
    }

    fn check_deadline(&self, job: &Job, deadline: Duration) -> Result<(), AttemptError> {
        let started = *job.started.lock().unwrap_or_else(|p| p.into_inner());
        let elapsed = self.db.clock().now().saturating_sub(started);
        if elapsed > deadline {
            return Err(AttemptError::Fatal(Error::CommitDeadlineExceeded {
                elapsed,
            }));
        }
        Ok(())
    }

    async fn attempt(
        self: Arc<Self>,
        job: Arc<Job>,
        open: Arc<Mutex<Option<Arc<Snapshot>>>>,
        put_started: Arc<AtomicBool>,
    ) -> Result<u64, AttemptError> {
        let clock = self.db.clock().clone();
        let deadline = self.current().def.commit_deadline();
        self.check_deadline(&job, deadline)?;
        // Appends read nothing and need a fresh snapshot only to avoid a cleaned-up version;
        // every other write executes against the newest version (D25's stale-snapshot guard).
        let mut snap = self.current();
        let stale = clock.now().saturating_sub(snap.confirmed_at) > deadline;
        if stale || !matches!(job.work, Work::Append(_)) {
            snap = self.refresh().await.map_err(read_failure)?;
        }
        Self::check_superseded(&job, snap.txns.applied(&job.caller).is_some())?;
        if let Some(f) = job.fence() {
            job.check_fence(snap.txns.get(&f.app_id))?;
        }
        job.check_replace_txns(|app| snap.txns.get(app))?;
        let mut prepared = match self.prepare(&job, &snap).await.map_err(read_failure)? {
            Ready::Commit(p) => p,
            Ready::Nothing(v) => {
                job.committed(HashSet::new());
                return Ok(v);
            }
        };
        self.check_deadline(&job, deadline)?;
        match job.work {
            Work::Append(_) => fail::fail_point!("moraine::append::before_commit"),
            Work::Replace(_) => fail::fail_point!("moraine::replace::before_commit"),
            _ => fail::fail_point!("moraine::rewrite::before_commit"),
        }
        for _ in 0..MAX_REBASES {
            self.check_deadline(&job, deadline)?;
            let v = snap.version + 1;
            let losses = job.losses.load(Ordering::SeqCst);
            let wait = self.turn_wait(losses);
            if !wait.is_zero() {
                clock.sleep(wait).await;
            }
            put_started.store(true, Ordering::SeqCst);
            let t0 = clock.now();
            let put = self.layer.put_entry(v, prepared.body.clone()).await;
            self.note_put(clock.now().saturating_sub(t0), &put, losses);
            match put {
                Ok(()) => {
                    job.committed(prepared.adds.iter().map(|f| f.path.clone()).collect());
                    let txns = job.entry_txns();
                    let own = own_entry(v, &prepared, &job.txn, &txns, job.data_change());
                    let mut s = snap.apply(&[own]).map_err(AttemptError::Fatal)?;
                    s.confirmed_at = clock.now();
                    self.install(s);
                    match job.work {
                        Work::Append(_) => fail::fail_point!("moraine::append::after_commit"),
                        Work::Replace(_) => fail::fail_point!("moraine::replace::after_commit"),
                        _ => fail::fail_point!("moraine::rewrite::after_commit"),
                    }
                    return Ok(v);
                }
                Err(AttemptError::Conflict(ConflictKind::VersionTaken)) => {
                    job.losses.fetch_add(1, Ordering::SeqCst);
                    let e = self
                        .layer
                        .read_entry(v, Some(snap.schema()))
                        .await
                        .map_err(read_failure)?;
                    let Some(e) = e else {
                        // Taken but unreadable: cleaned up, or a 409 race. Start over.
                        return Err(AttemptError::Conflict(ConflictKind::VersionTaken));
                    };
                    let mine = e.has_txn(&job.txn);
                    self.note_writer(&e);
                    // The attempt keeps its own snapshot: the handle's may have moved further
                    // (another task refreshed it), and every entry after the one prepared
                    // against must pass the conflict check below.
                    let next = Arc::new(
                        snap.apply(std::slice::from_ref(&e))
                            .map_err(AttemptError::Fatal)?,
                    );
                    self.install(next.as_ref().clone());
                    snap = next;
                    if mine {
                        job.committed(e.adds.iter().map(|f| f.path.clone()).collect());
                        return Ok(v);
                    }
                    Self::check_superseded(
                        &job,
                        job.caller.iter().any(|c| {
                            e.txns
                                .iter()
                                .any(|t| t.app_id == c.app_id && t.version >= c.version)
                        }),
                    )?;
                    self.rebase(&job, &mut prepared, &e, &snap).await?;
                }
                Err(AttemptError::Ambiguous(m)) => {
                    if let Ok(mut g) = open.lock() {
                        *g = Some(snap.clone());
                    }
                    job.uploads
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .open
                        .push(v);
                    return Err(AttemptError::Ambiguous(m));
                }
                Err(other) => return Err(other),
            }
        }
        Err(AttemptError::Conflict(ConflictKind::VersionTaken))
    }

    /// Decides whether the prepared entry may follow `e` (another writer's entry, already
    /// applied to `snap`); re-encodes it when only the schema grew under an append.
    async fn rebase(
        &self,
        job: &Job,
        prepared: &mut Prepared,
        e: &LogEntry,
        snap: &Snapshot,
    ) -> Result<(), AttemptError> {
        match &job.work {
            Work::Append(_) => {
                if let Some(m) = &e.meta {
                    if !extends(&prepared.schema, &m.schema) {
                        return Err(AttemptError::Conflict(ConflictKind::MetadataChanged));
                    }
                    // D9: the files stay valid; the new columns read as null.
                    prepared.schema = m.schema.clone();
                    prepared.body = self
                        .encode(
                            job,
                            &prepared.schema,
                            None,
                            &prepared.adds,
                            &prepared.removes,
                        )
                        .map_err(AttemptError::Fatal)?;
                }
                Ok(())
            }
            Work::AddColumn(_) => {
                if e.meta.is_some() {
                    return Err(AttemptError::Conflict(ConflictKind::MetadataChanged));
                }
                Ok(())
            }
            Work::Replace(r) => {
                if let Some(f) = &r.fence {
                    job.check_fence(
                        e.txns
                            .iter()
                            .filter(|t| t.app_id == f.app_id)
                            .map(|t| t.version)
                            .max(),
                    )?;
                }
                job.check_replace_txns(|app| {
                    e.txns
                        .iter()
                        .filter(|x| x.app_id == app)
                        .map(|x| x.version)
                        .max()
                })?;
                let mine: HashSet<&str> = r.remove.iter().map(String::as_str).collect();
                if let Some(p) = e.removes.iter().find(|p| mine.contains(p.as_str())) {
                    return Err(AttemptError::Fatal(Error::FilesChanged(p.clone())));
                }
                if let Some(m) = &e.meta {
                    if !extends(&prepared.schema, &m.schema) {
                        return Err(AttemptError::Conflict(ConflictKind::MetadataChanged));
                    }
                    prepared.schema = m.schema.clone();
                    prepared.body = self
                        .encode(
                            job,
                            &prepared.schema,
                            None,
                            &prepared.adds,
                            &prepared.removes,
                        )
                        .map_err(AttemptError::Fatal)?;
                }
                Ok(())
            }
            Work::Rewrite(rw) => {
                if e.meta.is_some() {
                    return Err(AttemptError::Conflict(ConflictKind::MetadataChanged));
                }
                let mine: HashSet<&str> =
                    prepared.removes.iter().map(|f| f.path.as_str()).collect();
                if e.removes.iter().any(|p| mine.contains(p.as_str())) {
                    return Err(AttemptError::Conflict(ConflictKind::ConcurrentModification));
                }
                // A swap that keeps every row (compaction) adds rows only from files it removes,
                // and those did not overlap this write's removes, so it adds no match.
                let adds = if e.data_change {
                    e.adds.as_slice()
                } else {
                    &[]
                };
                for f in adds {
                    if self
                        .holds_match(&rw.matcher, f, snap.schema())
                        .await
                        .map_err(read_failure)?
                    {
                        return Err(AttemptError::Conflict(ConflictKind::ConcurrentModification));
                    }
                }
                Ok(())
            }
        }
    }

    fn encode(
        &self,
        job: &Job,
        schema: &SchemaRef,
        meta: Option<TableMeta>,
        adds: &[DataFile],
        removes: &[DataFile],
    ) -> Result<Bytes> {
        let operation = match &job.work {
            Work::Append(_) => "WRITE",
            Work::Rewrite(rw) => rw.operation,
            Work::AddColumn(_) => "ADD COLUMNS",
            Work::Replace(r) => r.operation,
        };
        let caller_txns = job.entry_txns();
        self.layer.encode(
            &NewEntry {
                meta,
                protocol: false,
                adds: adds.to_vec(),
                removes: removes.to_vec(),
                txn: Some(job.txn.clone()),
                caller_txns,
                operation,
                timestamp: self.db.clock().wall_now().timestamp_millis(),
                data_change: job.data_change(),
            },
            schema,
        )
    }

    async fn prepare(&self, job: &Job, snap: &Arc<Snapshot>) -> Result<Ready> {
        match &job.work {
            Work::Append(batches) => {
                let adds = self
                    .upload_rows(job, snap, batches, "moraine::append")
                    .await?;
                let body = self.encode(job, snap.schema(), None, &adds, &[])?;
                Ok(Ready::Commit(Prepared {
                    schema: snap.schema().clone(),
                    meta: None,
                    adds,
                    removes: Vec::new(),
                    body,
                }))
            }
            Work::AddColumn(field) => {
                let schema = snap.schema();
                if let Ok(existing) = schema.field_with_name(field.name()) {
                    return if existing.data_type() == field.data_type() && existing.is_nullable() {
                        Ok(Ready::Nothing(snap.version))
                    } else {
                        Err(Error::InvalidSchema(format!(
                            "column {:?} exists with type {}",
                            field.name(),
                            existing.data_type()
                        )))
                    };
                }
                let mut fields: Vec<Field> =
                    schema.fields().iter().map(|f| f.as_ref().clone()).collect();
                fields.push(field.clone());
                let new = self.layer.canonical_schema(&Schema::new(fields))?;
                validate_new_schema(&new, &snap.def)?;
                let meta = TableMeta {
                    schema: new.clone(),
                    ..snap.meta.as_ref().clone()
                };
                let body = self.encode(job, &new, Some(meta.clone()), &[], &[])?;
                Ok(Ready::Commit(Prepared {
                    schema: new,
                    meta: Some(meta),
                    adds: Vec::new(),
                    removes: Vec::new(),
                    body,
                }))
            }
            Work::Rewrite(rw) => self.prepare_rewrite(job, rw, snap).await,
            Work::Replace(r) => self.prepare_replace(job, r, snap).await,
        }
    }

    /// Looks up the files to remove (a missing one is [`Error::FilesChanged`]) and uploads the
    /// new rows once.
    async fn prepare_replace(
        &self,
        job: &Job,
        r: &ReplaceWork,
        snap: &Arc<Snapshot>,
    ) -> Result<Ready> {
        let live: HashMap<&str, &Arc<DataFile>> =
            snap.files.iter().map(|f| (f.path.as_str(), f)).collect();
        let mut removes = Vec::with_capacity(r.remove.len());
        for p in &r.remove {
            match live.get(p.as_str()) {
                Some(f) => removes.push(f.as_ref().clone()),
                None => return Err(Error::FilesChanged(p.clone())),
            }
        }
        let mut adds = r.ready.clone();
        let written = self
            .upload_rows(job, snap, &r.batches, "moraine::replace")
            .await?;
        adds.extend(written.into_iter().map(|mut f| {
            f.tags.extend(r.tags.clone());
            f
        }));
        if removes.is_empty() && adds.is_empty() && job.caller.is_empty() {
            return Ok(Ready::Nothing(snap.version));
        }
        fail::fail_point!("moraine::replace::after_upload");
        let body = self.encode(job, snap.schema(), None, &adds, &removes)?;
        Ok(Ready::Commit(Prepared {
            schema: snap.schema().clone(),
            meta: None,
            adds,
            removes,
            body,
        }))
    }

    /// Conforms new rows to the snapshot's schema and uploads them, or reuses an earlier
    /// attempt's files when the schema only grew since.
    async fn upload_rows(
        &self,
        job: &Job,
        snap: &Snapshot,
        batches: &[RecordBatch],
        point: &str,
    ) -> Result<Vec<DataFile>> {
        let schema = snap.schema();
        let reusable = job
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .uploaded
            .as_ref()
            .filter(|(s, _)| extends(s, schema))
            .map(|(_, f)| f.clone());
        if let Some(adds) = reusable {
            return Ok(adds);
        }
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }
        let batches = self.prepare_blobs(job, snap, batches).await?;
        let conformed: Vec<RecordBatch> = batches
            .iter()
            .map(|b| conform(b, schema, &snap.def))
            .collect::<Result<_>>()?;
        let sorted = files::sort_rows(schema, &snap.def, &conformed)?;
        if point == "moraine::append" {
            fail::fail_point!("moraine::append::before_upload");
        }
        let adds = self
            .upload(
                snap,
                sorted,
                crate::index::inline::Writer::Append,
                Some(&job.uploads),
            )
            .await?
            .0;
        job.state.lock().unwrap_or_else(|p| p.into_inner()).uploaded =
            Some((schema.clone(), adds.clone()));
        if point == "moraine::append" {
            fail::fail_point!("moraine::append::after_upload");
        }
        Ok(adds)
    }

    /// The new rows of `job` with their blob columns in stored form (the first
    /// step of the blob write order): done once per job, so attempts after a lost race reuse the objects.
    async fn prepare_blobs(
        &self,
        job: &Job,
        snap: &Snapshot,
        batches: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>> {
        let cols = crate::blob::columns(snap.schema());
        if cols.is_empty() {
            return Ok(batches.to_vec());
        }
        if let Some(done) = job
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .blobs
            .clone()
        {
            return Ok(done);
        }
        let clock = self.db.clock().clone();
        let deadline = snap.def.commit_deadline();
        self.staged.prune(clock.now(), deadline);
        let store = self.db.store.object_store().clone();
        let prefix = self.blob_prefix();
        let ctx = crate::blob::Ctx {
            store: &store,
            prefix: &prefix,
            clock: &clock,
            inline_bytes: snap.def.blob_inline_bytes,
            reuse: self.db.opts.blob_reuse,
            staged: &self.staged,
            deadline,
            use_staged: !job.restarted.load(Ordering::SeqCst),
            concurrency: self.db.opts.io_concurrency,
        };
        let mut done = crate::blob::Prepared::default();
        let mut out = Vec::with_capacity(batches.len());
        let mut failed = None;
        for b in batches {
            match crate::blob::prepare(&ctx, b, &cols, &mut done).await {
                Ok(p) => out.push(p),
                Err(e) => {
                    failed = Some(e);
                    break;
                }
            }
        }
        job.uploads
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .blobs(std::mem::take(&mut done.keys));
        if let Some(e) = failed {
            return Err(e);
        }
        // Staged blobs committed as they are: the commit clock runs from their upload.
        if let Some(at) = done.staged_from {
            let mut s = job.started.lock().unwrap_or_else(|p| p.into_inner());
            *s = (*s).min(at);
        }
        job.state.lock().unwrap_or_else(|p| p.into_inner()).blobs = Some(out.clone());
        Ok(out)
    }

    /// Writes sorted rows as data files of about the target size, each with the sidecars its
    /// writer builds before the commit (index before commit; the data PUT and the sidecar PUTs
    /// share a round trip).
    pub(crate) async fn upload(
        &self,
        snap: &Snapshot,
        sorted: RecordBatch,
        writer: crate::index::inline::Writer,
        uploads: Option<&Mutex<Uploads>>,
    ) -> Result<(Vec<DataFile>, usize)> {
        let clock = self.db.clock().clone();
        let store = self.db.store.object_store().clone();
        let prefix = self.layer.data_prefix().clone();
        let schema = snap.schema();
        let mut adds = Vec::new();
        let mut built = 0;
        if sorted.num_rows() == 0 {
            return Ok((adds, built));
        }
        for piece in files::split(sorted, snap.def.target_file_bytes) {
            let enc = files::encode(schema, &snap.def, &piece, clock.as_ref())?;
            let size = enc.body.len() as u64;
            let at = prefix.clone().join(enc.name.as_str());
            if let Some(u) = uploads {
                u.lock().unwrap_or_else(|p| p.into_inner()).file(&enc.name);
            }
            let data = store.put(&at, PutPayload::from(enc.body));
            let rows = piece.num_rows() as u64;
            let name = enc.name.clone();
            let sidecars = async {
                match self.build_inline(snap, &piece, writer).await? {
                    Some(b) => self.finish_inline(b, &name, rows, size).await,
                    None => Ok(0),
                }
            };
            // In parallel unless the database asks for one request at a time.
            if self.db.opts.io_concurrency > 1 {
                let (d, k) = futures::join!(data, sidecars);
                d?;
                built += k?;
            } else {
                data.await?;
                built += sidecars.await?;
            }
            adds.push(DataFile {
                path: enc.name,
                size,
                stats: enc.stats,
                modification_time: clock.wall_now().timestamp_millis(),
                sorted_by: Some(enc.sorted_by),
                added_in: 0,
                tags: Default::default(),
            });
        }
        Ok((adds, built))
    }

    async fn prepare_rewrite(
        &self,
        job: &Job,
        rw: &Rewrite,
        snap: &Arc<Snapshot>,
    ) -> Result<Ready> {
        let schema = snap.schema().clone();
        {
            let mut st = job.state.lock().unwrap_or_else(|p| p.into_inner());
            if st
                .reuse_schema
                .as_ref()
                .is_none_or(|s| !extends(s, &schema))
            {
                st.reuse.clear();
                st.reuse_schema = Some(schema.clone());
            }
        }
        let filter = self.compiled(&rw.matcher, &schema)?;
        let assignments = match &rw.assignments {
            Some(a) => Some(self.compile_assignments(a, &schema)?),
            None => None,
        };
        let mut removes = Vec::new();
        let mut adds = Vec::new();
        let mut uploaded_now = false;
        for f in snap.files.iter() {
            let known = job
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .reuse
                .get(&f.path)
                .cloned();
            let outcome = match known {
                Some(o) => o,
                None => {
                    let o = if self.holds_match(&rw.matcher, f, &schema).await? {
                        uploaded_now = true;
                        FileOutcome::Rewritten(
                            self.rewrite_file(
                                job,
                                snap,
                                f,
                                &rw.matcher,
                                filter.as_ref(),
                                assignments.as_deref(),
                            )
                            .await?,
                        )
                    } else {
                        FileOutcome::Untouched
                    };
                    job.state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .reuse
                        .insert(f.path.clone(), o.clone());
                    o
                }
            };
            if let FileOutcome::Rewritten(new) = outcome {
                removes.push(f.as_ref().clone());
                adds.extend(new);
            }
        }
        let had_upload = job
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .uploaded
            .is_some();
        let inserted = self
            .upload_rows(job, snap, &rw.insert, "moraine::rewrite")
            .await?;
        if !had_upload && !inserted.is_empty() {
            uploaded_now = true;
        }
        adds.extend(inserted);
        if removes.is_empty() && adds.is_empty() && job.caller.is_empty() {
            return Ok(Ready::Nothing(snap.version));
        }
        if uploaded_now {
            fail::fail_point!("moraine::rewrite::after_upload");
        }
        let body = self.encode(job, &schema, None, &adds, &removes)?;
        Ok(Ready::Commit(Prepared {
            schema,
            meta: None,
            adds,
            removes,
            body,
        }))
    }

    fn compiled(&self, m: &Matcher, schema: &SchemaRef) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        match m {
            Matcher::Keys(_) => Ok(None),
            Matcher::Predicate(e) => Ok(Some(self.db.physical_expr(e.clone(), schema)?)),
        }
    }

    fn compile_assignments(
        &self,
        a: &[(String, Expr)],
        schema: &SchemaRef,
    ) -> Result<Vec<(usize, Arc<dyn PhysicalExpr>)>> {
        a.iter()
            .map(|(c, e)| {
                let i = schema
                    .index_of(c)
                    .map_err(|_| column_not_found(c, schema))?;
                Ok((i, self.db.physical_expr(e.clone(), schema)?))
            })
            .collect()
    }

    /// Whether file `f` holds a row the matcher selects. Statistics and cached bloom filters
    /// answer "no" without a request; otherwise the key or filter columns of the pages that may
    /// match are read.
    async fn holds_match(&self, m: &Matcher, f: &DataFile, schema: &SchemaRef) -> Result<bool> {
        let path = self.layer.data_prefix().clone().join(f.path.as_str());
        let prune = match m {
            Matcher::Keys(k) => k.prune.clone(),
            Matcher::Predicate(e) => Some(e.clone()),
        };
        if let Some(p) = &prune {
            if !may_match(p, schema, &FileBounds(f)) {
                return Ok(false);
            }
            if bloom_excludes(&self.db.cache, &path, f.size, p) {
                return Ok(false);
            }
        }
        let (needed_names, physical) = match m {
            Matcher::Keys(k) => (k.cols.clone(), None),
            Matcher::Predicate(e) => {
                let mut names: Vec<String> = Vec::new();
                for c in e.column_refs() {
                    if !names.contains(&c.name) {
                        names.push(c.name.clone());
                    }
                }
                if names.is_empty() {
                    names.push(schema.field(0).name().clone());
                }
                let needed = project_schema(schema, &names)?;
                (names, Some(self.db.physical_expr(e.clone(), &needed)?))
            }
        };
        let needed = project_schema(schema, &needed_names)?;
        let q = FileQuery {
            table_schema: schema.clone(),
            output: needed_names,
            needed,
            filter: prune,
            physical,
        };
        let batches = read_file(
            &self.db.cache,
            self.db.store.object_store(),
            &path,
            f.size,
            &q,
        )
        .await?;
        match m {
            Matcher::Predicate(_) => Ok(batches.iter().any(|b| b.num_rows() > 0)),
            Matcher::Keys(k) => {
                for b in &batches {
                    if key_rows(b, &k.cols)?
                        .iter()
                        .any(|r| k.set.contains(r.as_ref()))
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Reads file `f` whole and writes its rows again without (or with updated) matching rows.
    async fn rewrite_file(
        &self,
        job: &Job,
        snap: &Snapshot,
        f: &DataFile,
        m: &Matcher,
        filter: Option<&Arc<dyn PhysicalExpr>>,
        assignments: Option<&[(usize, Arc<dyn PhysicalExpr>)]>,
    ) -> Result<Vec<DataFile>> {
        let schema = snap.schema();
        let path = self.layer.data_prefix().clone().join(f.path.as_str());
        let all: Vec<String> = schema.fields().iter().map(|x| x.name().clone()).collect();
        let q = FileQuery {
            table_schema: schema.clone(),
            output: all,
            needed: schema.clone(),
            filter: None,
            physical: None,
        };
        let parts = read_file(
            &self.db.cache,
            self.db.store.object_store(),
            &path,
            f.size,
            &q,
        )
        .await?;
        let parts: Vec<RecordBatch> = parts
            .iter()
            .map(|b| to_schema(b, schema))
            .collect::<Result<_>>()?;
        let batch = concat_batches(schema, &parts)?;
        let mask = match m {
            Matcher::Keys(k) => BooleanArray::from_iter(
                key_rows(&batch, &k.cols)?
                    .iter()
                    .map(|r| Some(k.set.contains(r.as_ref()))),
            ),
            Matcher::Predicate(_) => {
                let p = filter.ok_or_else(|| Error::Internal("filter not compiled".into()))?;
                let v = p.evaluate(&batch)?.into_array(batch.num_rows())?;
                let b = v
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::InvalidQuery("the filter is not boolean".into()))?;
                if b.null_count() > 0 {
                    prep_null_mask_filter(b)
                } else {
                    b.clone()
                }
            }
        };
        let rows = match assignments {
            None => filter_record_batch(&batch, &not(&mask)?)?,
            Some(a) => {
                let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
                for (i, e) in a {
                    let t = schema.field(*i).data_type();
                    let mut v = e.evaluate(&batch)?.into_array(batch.num_rows())?;
                    if v.data_type() != t {
                        v = cast(&v, t)?;
                    }
                    cols[*i] = zip(&mask, &v, &batch.column(*i))?;
                }
                let updated = RecordBatch::try_new(schema.clone(), cols)?;
                // Non-null columns stay non-null; the order may change (cluster key).
                let updated = conform(&updated, schema, &snap.def)?;
                files::sort_rows(schema, &snap.def, &[updated])?
            }
        };
        Ok(self
            .upload(
                snap,
                rows,
                crate::index::inline::Writer::Append,
                Some(&job.uploads),
            )
            .await?
            .0)
    }

    /// After an ambiguous put: reads the log from the version that put targeted until an entry
    /// is missing, looking for `txn` (case S1). Every entry it reads is applied to the handle.
    pub(super) async fn resolve(
        self: Arc<Self>,
        open: Arc<Mutex<Option<Arc<Snapshot>>>>,
        txn: TxnId,
    ) -> Result<Option<u64>> {
        let base = open
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .unwrap_or_else(|| self.current());
        let entries = self.probe(&base, base.version + 1).await?;
        let found = entries.iter().find(|e| e.has_txn(&txn)).map(|e| e.version);
        let mut s = base.apply(&entries)?;
        s.confirmed_at = self.db.clock().now();
        let s = self.install(s);
        if let Ok(mut g) = open.lock() {
            *g = Some(s);
        }
        Ok(found)
    }
}

fn own_entry(
    version: u64,
    p: &Prepared,
    txn: &TxnId,
    caller: &[TxnId],
    data_change: bool,
) -> LogEntry {
    LogEntry {
        version,
        meta: p.meta.clone().map(Arc::new),
        adds: p
            .adds
            .iter()
            .cloned()
            .map(|mut f| {
                f.added_in = version;
                f
            })
            .collect(),
        removes: p.removes.iter().map(|f| f.path.clone()).collect(),
        txns: std::iter::once(txn.clone())
            .chain(caller.iter().cloned())
            .collect(),
        txn_times: Vec::new(),
        data_change,
    }
}

/// A failed read inside an attempt changed nothing: transient ones retry the attempt. A data
/// file that is gone was removed by a commit the attempt has not seen and then collected (a
/// purge collects at once, D66): a conflict, so the next attempt plans on a fresh snapshot.
fn read_failure(e: Error) -> AttemptError {
    if e.is_missing_object() {
        AttemptError::Conflict(ConflictKind::ConcurrentModification)
    } else if transient(&e) {
        AttemptError::Throttled
    } else {
        AttemptError::Fatal(e)
    }
}
