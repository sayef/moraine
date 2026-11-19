//! Maintenance: idempotent jobs any client may run, none needed for
//! correctness.
//!
//! One pass of [`crate::Table::maintain`] runs, in order: the index build, the ingestor of
//! queued writes ([`crate::Table::submit`]), compaction, checkpoint and log cleanup, then vacuum
//! with sidecar collection. Each job first takes its advisory lease (D29), unless leases are
//! off; a job whose lease another client holds is skipped for this pass. The pass reads server
//! time once (`meta/clock/`), and every expiry decision uses it.

pub(crate) mod compact;
pub(crate) mod inbox;
pub(crate) mod lease;
pub(crate) mod logjob;
pub(crate) mod merge;
pub(crate) mod purge;
pub(crate) mod vacuum;
pub(crate) mod verify;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::index::{BuildOptions, BuildReport};
use crate::store::{ServerClock, server_now};
use crate::table::{Table, TableInner};

pub use compact::{CompactionPolicy, CompactionReport};
pub use inbox::{IngestReport, Submitted};
pub use lease::Lease;
pub use logjob::LogReport;
pub use purge::{Purge, PurgeReport};
pub use vacuum::VacuumReport;
pub use verify::{RepairReport, VerifyReport};

/// What one maintenance pass runs, and how: [`MaintainOptions::default`] runs every job,
/// [`MaintainOptions::none`] none, and the setters change one job or setting each.
///
/// ```
/// use moraine::MaintainOptions;
///
/// // Only checkpoints and log cleanup, for a table another process compacts.
/// let opts = MaintainOptions::none().checkpoint(true);
/// assert!(opts.compaction.is_none());
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MaintainOptions {
    /// Take each job's advisory lease first, and skip the job while another client holds it
    /// (default true). Every job is correct without them; they only avoid duplicated work.
    pub leases: bool,
    /// How long a lease lasts without renewal (default 5 minutes).
    pub lease_ttl: Duration,
    /// Build missing sidecars (default true), with these options.
    pub build_indexes: Option<BuildOptions>,
    /// Commit queued writes from the inbox (default true; a pass with an empty inbox costs one
    /// listing).
    pub ingest: bool,
    /// Compact with this policy (default [`CompactionPolicy::default`]); `None` skips it.
    pub compaction: Option<CompactionPolicy>,
    /// Write a checkpoint once this many entries have accumulated since the newest one;
    /// `None` uses the table's `delta.checkpointInterval` (20).
    pub checkpoint_interval: Option<u64>,
    /// Write checkpoints and clean up the log (default true).
    pub checkpoint: bool,
    /// Vacuum data files and collect sidecars (default true).
    pub vacuum: bool,
    /// Time between passes of [`crate::Table::spawn_maintenance`] (default 60 s).
    pub interval: Duration,
}

impl Default for MaintainOptions {
    fn default() -> Self {
        MaintainOptions {
            leases: true,
            lease_ttl: Duration::from_secs(300),
            build_indexes: Some(BuildOptions::default()),
            ingest: true,
            compaction: Some(CompactionPolicy::default()),
            checkpoint_interval: None,
            checkpoint: true,
            vacuum: true,
            interval: Duration::from_secs(60),
        }
    }
}

impl MaintainOptions {
    /// Every job off, leases and timings at their defaults: a base for running one job.
    pub fn none() -> MaintainOptions {
        MaintainOptions {
            build_indexes: None,
            ingest: false,
            compaction: None,
            checkpoint: false,
            vacuum: false,
            ..MaintainOptions::default()
        }
    }

    /// Whether each job takes its advisory lease first (default true).
    pub fn leases(mut self, on: bool) -> Self {
        self.leases = on;
        self
    }

    /// How long a lease lasts without renewal (default 5 minutes).
    pub fn lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Build missing sidecars with these options, or not at all with `None`.
    pub fn build_indexes(mut self, opts: impl Into<Option<BuildOptions>>) -> Self {
        self.build_indexes = opts.into();
        self
    }

    /// Whether to commit queued writes from the inbox (default true).
    pub fn ingest(mut self, on: bool) -> Self {
        self.ingest = on;
        self
    }

    /// Compact with this policy, or not at all with `None`.
    pub fn compaction(mut self, policy: impl Into<Option<CompactionPolicy>>) -> Self {
        self.compaction = policy.into();
        self
    }

    /// Write a checkpoint once this many entries have accumulated since the newest one;
    /// `None` uses the table's `delta.checkpointInterval`.
    pub fn checkpoint_interval(mut self, entries: impl Into<Option<u64>>) -> Self {
        self.checkpoint_interval = entries.into();
        self
    }

    /// Whether to write checkpoints and clean up the log (default true).
    pub fn checkpoint(mut self, on: bool) -> Self {
        self.checkpoint = on;
        self
    }

    /// Whether to vacuum data files and collect sidecars (default true).
    pub fn vacuum(mut self, on: bool) -> Self {
        self.vacuum = on;
        self
    }

    /// Time between passes of [`crate::Table::spawn_maintenance`] (default 60 s).
    pub fn interval(mut self, d: Duration) -> Self {
        self.interval = d;
        self
    }
}

/// What one maintenance pass did. A job that did not run is `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceReport {
    /// Sidecar builds.
    pub indexes: Option<BuildReport>,
    /// Queued writes committed from the inbox.
    pub ingest: Option<IngestReport>,
    /// Compaction steps.
    pub compaction: Option<CompactionReport>,
    /// Checkpoint and log cleanup.
    pub log: Option<LogReport>,
    /// Vacuum and sidecar collection.
    pub vacuum: Option<VacuumReport>,
    /// Jobs skipped because another client holds their lease.
    pub skipped: Vec<String>,
    /// Server time the pass ran at.
    pub server_time: Option<DateTime<Utc>>,
}

/// Maintenance state of one table handle.
#[derive(Debug, Default)]
pub(crate) struct HandleState {
    /// Leases this handle holds, by job.
    pub leases: HashMap<String, Lease>,
    /// Log entries read by vacuum: version to (server time, removed paths).
    pub removals: BTreeMap<u64, (DateTime<Utc>, Vec<String>)>,
    /// The last server-time probe; reused for a minute, its elapsed time read from the
    /// monotonic clock.
    pub server: Option<ServerClock>,
    /// Jobs whose lease another client held: not asked for again before this (monotonic) time.
    pub busy_until: HashMap<String, Duration>,
}

/// How long a client that found a lease held waits before asking again, at most.
const BUSY_RETRY: Duration = Duration::from_secs(1);

/// How long one server-time probe serves.
const SERVER_CLOCK_REUSE: Duration = Duration::from_secs(60);

/// Whether a job may run, and under which lease.
pub(crate) enum Turn {
    Run(Option<Lease>),
    Skip,
}

impl TableInner {
    /// Server time, from one probe object under `meta/clock/`.
    pub(crate) async fn server_clock(&self) -> Result<ServerClock> {
        let now = self.db.clock().now();
        if let Some(c) = self.maint.lock().ok().and_then(|g| g.server)
            && now.saturating_sub(c.at) < SERVER_CLOCK_REUSE
        {
            return Ok(c);
        }
        let c = server_now(
            self.db.store.object_store().as_ref(),
            &self.meta_prefix(),
            self.db.clock().as_ref(),
        )
        .await?;
        if let Ok(mut g) = self.maint.lock() {
            g.server = Some(c);
        }
        Ok(c)
    }

    fn leases<'a>(&'a self, holder: &'a str) -> lease::Leases<'a> {
        lease::Leases {
            store: self.db.store.object_store().as_ref(),
            prefix: self.meta_prefix().join("leases"),
            holder,
        }
    }

    /// Server time now: the last probe plus the monotonic time since.
    pub(crate) async fn server_time(&self) -> Result<DateTime<Utc>> {
        Ok(self.server_clock().await?.now(self.db.clock().as_ref()))
    }

    /// Takes, keeps or renews the lease of `job` when leases are on.
    pub(crate) async fn turn(
        &self,
        job: &str,
        leases: bool,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<Turn> {
        if !leases {
            return Ok(Turn::Run(None));
        }
        let mono = self.db.clock().now();
        let (held, busy) = self
            .maint
            .lock()
            .map(|g| (g.leases.get(job).cloned(), g.busy_until.get(job).copied()))
            .unwrap_or_default();
        if held.is_none() && busy.is_some_and(|until| mono < until) {
            return Ok(Turn::Skip);
        }
        let store = self.leases(&self.db.writer_id);
        let ttl_c = chrono::Duration::from_std(ttl).unwrap_or_default();
        if let Some(mut l) = held {
            // Renew once a third of the time to live has passed; the generation moves on.
            if l.expires - now > ttl_c * 2 / 3 {
                return Ok(Turn::Run(Some(l)));
            }
            if store.renew(&mut l, ttl, now).await? {
                if let Ok(mut g) = self.maint.lock() {
                    g.leases.insert(job.into(), l.clone());
                }
                return Ok(Turn::Run(Some(l)));
            }
            if let Ok(mut g) = self.maint.lock() {
                g.leases.remove(job);
            }
        }
        match store.acquire(job, ttl, now).await? {
            Some(l) => {
                if let Ok(mut g) = self.maint.lock() {
                    g.leases.insert(job.into(), l.clone());
                }
                Ok(Turn::Run(Some(l)))
            }
            None => {
                // Someone else holds it: ask again after a second (a holder may release it at
                // any time), or a third of the time to live if that is shorter.
                if let Ok(mut g) = self.maint.lock() {
                    g.busy_until
                        .insert(job.into(), mono + (ttl / 3).min(BUSY_RETRY));
                }
                Ok(Turn::Skip)
            }
        }
    }

    /// Releases every lease this handle holds.
    pub(crate) async fn release_leases(&self) -> Result<()> {
        let held: Vec<Lease> = self
            .maint
            .lock()
            .map(|mut g| g.leases.drain().map(|(_, l)| l).collect())
            .unwrap_or_default();
        if held.is_empty() {
            return Ok(());
        }
        let now = self.server_time().await?;
        let store = self.leases(&self.db.writer_id);
        for l in held {
            store.release(&l, now).await?;
        }
        Ok(())
    }

    pub(crate) async fn maintain(
        self: &Arc<Self>,
        opts: &MaintainOptions,
    ) -> Result<MaintenanceReport> {
        let clock = self.server_clock().await?;
        let now = || clock.now(self.db.clock().as_ref());
        let mut report = MaintenanceReport {
            server_time: Some(clock.server),
            ..Default::default()
        };
        let turn = |job: &'static str| {
            let at = now();
            async move { self.turn(job, opts.leases, opts.lease_ttl, at).await }
        };
        if let Some(b) = &opts.build_indexes {
            match turn("index").await? {
                Turn::Run(_) => report.indexes = Some(self.build_indexes(b.clone()).await?),
                Turn::Skip => report.skipped.push("index".into()),
            }
        }
        if opts.ingest {
            match turn("ingest").await? {
                Turn::Run(l) => {
                    report.ingest = Some(self.ingest(l.map(|l| l.fence()), usize::MAX).await?)
                }
                Turn::Skip => report.skipped.push("ingest".into()),
            }
        }
        if let Some(policy) = &opts.compaction {
            match turn("compact").await? {
                Turn::Run(l) => {
                    let at = now().timestamp_millis();
                    report.compaction = Some(self.compact(policy, l.map(|l| l.fence()), at).await?);
                }
                Turn::Skip => report.skipped.push("compact".into()),
            }
        }
        if opts.checkpoint {
            match turn("log").await? {
                Turn::Run(_) => {
                    report.log = Some(
                        self.checkpoint_and_clean(opts.checkpoint_interval, now(), true)
                            .await?,
                    )
                }
                Turn::Skip => report.skipped.push("log".into()),
            }
        }
        if opts.vacuum {
            match turn("vacuum").await? {
                Turn::Run(_) => report.vacuum = Some(self.vacuum(now(), true).await?),
                Turn::Skip => report.skipped.push("vacuum".into()),
            }
        }
        Ok(report)
    }
}

fn lease_name(job: &str) -> Result<()> {
    crate::index::spec::validate_index_name(job).map_err(|_| {
        crate::Error::InvalidQuery(format!(
            "lease name {job:?} does not match [a-z0-9][a-z0-9_-]{{0,63}}"
        ))
    })
}

/// Counters of a [`MaintenanceTask`].
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct MaintenanceMetrics {
    /// Passes run, failed ones included.
    pub passes: AtomicU64,
    /// Passes that returned an error.
    pub failures: AtomicU64,
    last_error: Mutex<Option<String>>,
    last_report: Mutex<Option<MaintenanceReport>>,
}

impl MaintenanceMetrics {
    /// The error of the last pass that failed.
    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|g| g.clone())
    }

    /// The report of the last pass that succeeded.
    pub fn last_report(&self) -> Option<MaintenanceReport> {
        self.last_report.lock().ok().and_then(|g| g.clone())
    }
}

/// Maintenance passes running on a timer ([`crate::Table::spawn_maintenance`]). Dropping it
/// stops them, as does dropping the last handle of the table.
#[derive(Debug)]
pub struct MaintenanceTask {
    handle: tokio::task::JoinHandle<()>,
    metrics: Arc<MaintenanceMetrics>,
}

impl MaintenanceTask {
    /// The task's counters, shared with the running task.
    pub fn metrics(&self) -> &Arc<MaintenanceMetrics> {
        &self.metrics
    }

    /// Stops the passes and releases the leases the table handle holds.
    pub async fn stop(self, table: &Table) -> Result<()> {
        self.handle.abort();
        table.inner.release_leases().await
    }
}

impl Drop for MaintenanceTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Table {
    /// Runs one maintenance pass and reports what it did.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// use moraine::MaintainOptions;
    /// let r = t.maintain(&MaintainOptions::default()).await?;
    /// if let Some(c) = &r.compaction {
    ///     println!("{} files merged into {}", c.files_removed, c.files_added);
    /// }
    /// if let Some(v) = &r.vacuum {
    ///     println!("{} files and {} sidecars deleted", v.files_deleted, v.sidecars_deleted);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn maintain(&self, opts: &MaintainOptions) -> Result<MaintenanceReport> {
        self.inner.maintain(opts).await
    }

    /// Runs [`Table::maintain`] every `opts.interval` on the Tokio runtime until the returned
    /// task is dropped or stopped, or the last handle of the table is dropped. A failed pass is
    /// counted and the next one runs as planned.
    pub fn spawn_maintenance(&self, opts: MaintainOptions) -> MaintenanceTask {
        let weak: Weak<TableInner> = Arc::downgrade(&self.inner);
        let metrics = Arc::new(MaintenanceMetrics::default());
        let m = metrics.clone();
        let clock = self.inner.db.clock().clone();
        let handle = tokio::spawn(async move {
            loop {
                let Some(t) = weak.upgrade() else {
                    return;
                };
                match t.maintain(&opts).await {
                    Ok(r) => {
                        if let Ok(mut g) = m.last_report.lock() {
                            *g = Some(r);
                        }
                    }
                    Err(e) => {
                        m.failures.fetch_add(1, Ordering::Relaxed);
                        if let Ok(mut g) = m.last_error.lock() {
                            *g = Some(e.to_string());
                        }
                    }
                }
                m.passes.fetch_add(1, Ordering::Relaxed);
                drop(t);
                clock.sleep(opts.interval).await;
            }
        });
        MaintenanceTask { handle, metrics }
    }

    /// Takes the advisory lease `job` (`[a-z0-9][a-z0-9_-]{0,63}`) for `ttl` when it is free,
    /// released or expired by server time; `None` while another client holds it. Commits made
    /// under it can be fenced ([`crate::Replace::fenced`]).
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// use std::time::Duration;
    /// if let Some(mut lease) = t.try_lease("fold", Duration::from_secs(60)).await? {
    ///     // ... work, renewing before the time to live runs out:
    ///     if !t.renew_lease(&mut lease, Duration::from_secs(60)).await? {
    ///         return Ok(()); // taken over: stop
    ///     }
    ///     t.release_lease(&lease).await?;
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn try_lease(&self, job: &str, ttl: Duration) -> Result<Option<Lease>> {
        lease_name(job)?;
        let now = self.inner.server_time().await?;
        self.inner
            .leases(&self.inner.db.writer_id)
            .acquire(job, ttl, now)
            .await
    }

    /// Renews a lease taken with [`Table::try_lease`]; its generation moves on. `false` when
    /// another client took it over.
    pub async fn renew_lease(&self, lease: &mut Lease, ttl: Duration) -> Result<bool> {
        let now = self.inner.server_time().await?;
        self.inner
            .leases(&self.inner.db.writer_id)
            .renew(lease, ttl, now)
            .await
    }

    /// Releases a lease, so another client may take it at once.
    pub async fn release_lease(&self, lease: &Lease) -> Result<()> {
        let now = self.inner.server_time().await?;
        self.inner
            .leases(&self.inner.db.writer_id)
            .release(lease, now)
            .await
    }

    /// Releases every lease this handle holds for maintenance and ingestion, so other clients
    /// may take them at once instead of after their time to live. Call it before a process
    /// that ran passes or `ingest` exits.
    pub async fn release_leases(&self) -> Result<()> {
        self.inner.release_leases().await
    }

    /// Runs compaction alone with `policy`, without a lease. [`CompactionPolicy::full`]
    /// merges every run of small files.
    pub async fn compact(&self, policy: &CompactionPolicy) -> Result<CompactionReport> {
        let now = self.inner.server_time().await?.timestamp_millis();
        self.inner.compact(policy, None, now).await
    }

    /// Writes a checkpoint at the newest version if none exists for it yet, without cleaning
    /// up the log; returns its version when one was written.
    pub async fn checkpoint(&self) -> Result<Option<u64>> {
        let now = self.inner.server_time().await?;
        Ok(self
            .inner
            .checkpoint_and_clean(Some(1), now, false)
            .await?
            .checkpoint)
    }

    /// Runs vacuum and sidecar collection alone, without a lease.
    pub async fn vacuum(&self) -> Result<VacuumReport> {
        let now = self.inner.server_time().await?;
        self.inner.vacuum(now, true).await
    }

    /// Checks that every data file of the newest version exists with its recorded size and
    /// that every current sidecar validates.
    pub async fn verify(&self) -> Result<VerifyReport> {
        self.inner.verify().await
    }

    /// Removes the table's references to missing data files in one commit and reports the
    /// rows lost. Manual: run it only after [`Table::verify`] reports missing
    /// files and the cause is understood.
    pub async fn repair(&self) -> Result<RepairReport> {
        self.inner.repair().await
    }
}
