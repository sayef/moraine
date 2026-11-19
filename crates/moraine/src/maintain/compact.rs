//! Compaction (D19, D29, D48): tiered, sorted, sidecars before the commit.
//!
//! Every file carries a level in its `add.tags` (`moraine.level`; appended files are level 0).
//! A compaction step takes a run of at least `fanout` files of one level that sit next to each
//! other in the snapshot's file order, merges their rows sorted by the table's cluster and
//! primary key, writes them as files of the next level split at the target size, builds their
//! sidecars, and swaps them in with one commit that changes no rows. The new files take the
//! place of the first file of the run, so file order, and with it the order of rows with equal
//! keys (D38), is the same as before. The lowest level with such a run goes first. When no
//! level has one but more than `max_files` small files are live, the longest run of small files
//! is merged whatever their levels.
//!
//! Files at three quarters of the target size or more are full and never picked, nor are files
//! younger than `min_file_age`, which an active upsert workload is likely to rewrite anyway.
//! A commit that loses to a delete, an update, an upsert or another compaction touching the
//! same files yields: nothing is retried in the same pass.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;

use crate::error::{Error, Result};
use crate::log::{DataFile, LEVEL_TAG, TxnId};
use crate::read::{FileQuery, read_file, to_schema};
use crate::snapshot::Snapshot;
use crate::table::{ReplaceWork, TableInner};
use crate::write as files;

/// Which files compaction merges, and how: [`CompactionPolicy::default`] (tiered) or
/// [`CompactionPolicy::full`], changed with the setters.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompactionPolicy {
    /// Merge a run of at least this many adjacent files of one level (at least 2).
    pub fanout: usize,
    /// With no tiered run available, merge anyway once more small files than this are live.
    pub max_files: usize,
    /// Only files older than this (by their writer's clock) are merged.
    pub min_file_age: Duration,
    /// Levels stop growing here; files at this level merge with each other.
    pub max_level: u32,
    /// The most input bytes (compressed, as stored) one step reads into memory.
    pub max_input_bytes: u64,
    /// Steps per maintenance pass, at most.
    pub max_steps: usize,
    /// Build the merged files' sidecars before committing them (D48). Without, they take the
    /// exact route until the index build job reaches them.
    pub build_sidecars: bool,
    /// Merge by streaming over the sorted inputs, with memory bounded by row groups rather
    /// than the input's size (D59). Inputs not sorted by the current sort columns are
    /// merged in memory either way.
    pub streaming: bool,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        CompactionPolicy {
            fanout: 4,
            max_files: 16,
            min_file_age: Duration::from_secs(60),
            max_level: 8,
            max_input_bytes: 256 << 20,
            max_steps: 8,
            build_sidecars: true,
            streaming: true,
        }
    }
}

impl CompactionPolicy {
    /// Merges every run of two or more adjacent small files, whatever their level or age, until
    /// none is left: the most compact layout, for a quiet table or a benchmark.
    pub fn full() -> CompactionPolicy {
        CompactionPolicy {
            fanout: 2,
            max_files: 0,
            min_file_age: Duration::ZERO,
            max_steps: usize::MAX,
            ..CompactionPolicy::default()
        }
    }

    /// Merge a run of at least this many adjacent files of one level (default 4, at least 2).
    pub fn fanout(mut self, files: usize) -> Self {
        self.fanout = files;
        self
    }

    /// With no tiered run available, merge anyway once more small files than this are live
    /// (default 16).
    pub fn max_files(mut self, files: usize) -> Self {
        self.max_files = files;
        self
    }

    /// Only merge files older than this (default 60 s). A file's age is server time minus the
    /// wall-clock time of the client that wrote it, so it is as exact as the two clocks agree;
    /// zero merges files of any age.
    pub fn min_file_age(mut self, age: Duration) -> Self {
        self.min_file_age = age;
        self
    }

    /// The level files stop growing at (default 8).
    pub fn max_level(mut self, level: u32) -> Self {
        self.max_level = level;
        self
    }

    /// The most input bytes one step reads into memory (default 256 MiB).
    pub fn max_input_bytes(mut self, bytes: u64) -> Self {
        self.max_input_bytes = bytes;
        self
    }

    /// Steps per maintenance pass, at most (default 8).
    pub fn max_steps(mut self, steps: usize) -> Self {
        self.max_steps = steps;
        self
    }

    /// Whether merged files get their sidecars before the commit (default true, D48).
    pub fn build_sidecars(mut self, on: bool) -> Self {
        self.build_sidecars = on;
        self
    }

    /// Whether sorted inputs are merged by streaming (default true, D59).
    pub fn streaming(mut self, on: bool) -> Self {
        self.streaming = on;
        self
    }
}

/// What compaction did in one pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompactionReport {
    /// Commits made.
    pub steps: usize,
    /// Files the steps removed.
    pub files_removed: usize,
    /// Files the steps added.
    pub files_added: usize,
    /// Bytes read from the inputs.
    pub bytes_read: u64,
    /// Bytes of the files written.
    pub bytes_written: u64,
    /// Sidecars built for the new files before their commit.
    pub sidecars_built: usize,
    /// Of those, sidecars merged from the inputs' sidecars without reading rows.
    pub sidecars_merged: usize,
    /// Steps merged by streaming.
    pub streamed: usize,
    /// Steps that lost to another commit (a delete, an update, an upsert or another
    /// compaction) or to a newer lease holder, and were dropped.
    pub yielded: usize,
}

/// One planned step: a run of adjacent files and the level of what replaces them.
#[derive(Debug)]
pub(crate) struct Step {
    pub files: Vec<Arc<DataFile>>,
    pub level: u32,
}

/// Rows a second assumed for sidecar builds before one has been measured: D61's single-thread
/// usearch rate on a heavily loaded host.
const ASSUMED_BUILD_RATE: f64 = 1000.0;
/// Share of the commit deadline a step's sidecar builds may take; the rest is for reading,
/// writing, uploading and the commit.
const BUILD_SHARE: f64 = 0.5;

/// The most rows one step of a table whose merged files get sidecars rebuilt from rows may
/// take, so their builds end within half the commit deadline at `rate` rows a second (D81).
pub(crate) fn max_step_rows(deadline: Duration, rate: Option<f64>) -> u64 {
    let rate = rate.unwrap_or(ASSUMED_BUILD_RATE).max(1.0);
    ((deadline.as_secs_f64() * BUILD_SHARE * rate) as u64).max(1)
}

/// Plans the next step on `snap`; `None` when nothing is due. A step takes at most
/// `max_rows` rows (as the files record them) unless that would leave it a single file.
pub(crate) fn plan(
    snap: &Snapshot,
    policy: &CompactionPolicy,
    now_ms: i64,
    max_rows: u64,
) -> Option<Step> {
    let target = snap.def.target_file_bytes;
    let full = target.saturating_mul(3) / 4;
    let min_age = policy.min_file_age.as_millis() as i64;
    // A file's age compares server time with the client wall clock that stamped the file, so a
    // file just written can look younger than zero (D91); no minimum age means any file.
    let small = |f: &DataFile| {
        f.size < full && (min_age == 0 || now_ms.saturating_sub(f.modification_time) >= min_age)
    };
    let fanout = policy.fanout.max(2);
    let level = |f: &DataFile| f.level().min(policy.max_level);
    // Files merge only with files of the same caller tags, which the merged files keep (D68).
    let same_tags = |a: &DataFile, b: &DataFile| user_tags(a) == user_tags(b);
    // Runs of adjacent small files of one level: (start, end, level).
    let files = &snap.files;
    let mut best: Option<(usize, usize, u32)> = None;
    let mut i = 0;
    while i < files.len() {
        if !small(&files[i]) {
            i += 1;
            continue;
        }
        let l = level(&files[i]);
        let mut j = i + 1;
        while j < files.len()
            && small(&files[j])
            && level(&files[j]) == l
            && same_tags(&files[i], &files[j])
        {
            j += 1;
        }
        if j - i >= fanout && best.is_none_or(|b| l < b.2) {
            best = Some((i, j, l));
        }
        i = j;
    }
    let (start, end, out_level) = match best {
        Some((s, e, l)) => (s, e, (l + 1).min(policy.max_level)),
        None => {
            let smalls = files.iter().filter(|f| small(f)).count();
            if smalls <= policy.max_files {
                return None;
            }
            // The longest run of adjacent small files, any level.
            let mut longest: Option<(usize, usize)> = None;
            let mut i = 0;
            while i < files.len() {
                if !small(&files[i]) {
                    i += 1;
                    continue;
                }
                let mut j = i + 1;
                while j < files.len() && small(&files[j]) && same_tags(&files[i], &files[j]) {
                    j += 1;
                }
                if j - i >= 2 && longest.is_none_or(|(s, e)| j - i > e - s) {
                    longest = Some((i, j));
                }
                i = j;
            }
            let (s, e) = longest?;
            let l = files[s..e].iter().map(|f| level(f)).max().unwrap_or(0);
            (s, e, (l + 1).min(policy.max_level))
        }
    };
    // Bound the input: a prefix of the run within the byte and row budgets, at least two files.
    let mut take = Vec::new();
    let mut bytes = 0u64;
    let mut rows = 0u64;
    for f in &files[start..end] {
        let r = f.rows().unwrap_or(0);
        if take.len() >= 2
            && (bytes + f.size > policy.max_input_bytes || rows.saturating_add(r) > max_rows)
        {
            break;
        }
        bytes += f.size;
        rows = rows.saturating_add(r);
        take.push(f.clone());
    }
    (take.len() >= 2).then_some(Step {
        files: take,
        level: out_level,
    })
}

impl TableInner {
    /// Runs compaction steps until none is due, the step limit is reached, or one yields.
    pub(crate) async fn compact(
        self: &Arc<Self>,
        policy: &CompactionPolicy,
        fence: Option<TxnId>,
        now_ms: i64,
    ) -> Result<CompactionReport> {
        let mut report = CompactionReport::default();
        while report.steps < policy.max_steps {
            let snap = self.refresh().await?;
            let Some(step) = plan(&snap, policy, now_ms, self.step_row_cap(&snap, policy)) else {
                break;
            };
            match self
                .compact_step(&snap, step, policy, fence.clone(), &mut report)
                .await
            {
                Ok(()) => report.steps += 1,
                Err(Error::FilesChanged(_))
                | Err(Error::Fenced { .. })
                | Err(Error::CommitDeadlineExceeded { .. }) => {
                    report.yielded += 1;
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(report)
    }

    /// The row cap of a step on `snap` (D81): none unless the merged files get sidecars of
    /// declared indexes built before the commit.
    fn step_row_cap(&self, snap: &Snapshot, policy: &CompactionPolicy) -> u64 {
        let indexed = policy.build_sidecars
            && crate::index::spec::parse_specs(&snap.meta.configuration)
                .is_ok_and(|s| !s.is_empty());
        if !indexed {
            return u64::MAX;
        }
        let state = self.db.index.table(self.idx_prefix().as_ref());
        let rate = *state.build_rate.lock().unwrap_or_else(|p| p.into_inner());
        max_step_rows(snap.def.commit_deadline(), rate)
    }

    async fn compact_step(
        self: &Arc<Self>,
        snap: &Arc<Snapshot>,
        step: Step,
        policy: &CompactionPolicy,
        fence: Option<TxnId>,
        report: &mut CompactionReport,
    ) -> Result<()> {
        let started = self.db.clock().now();
        let writer = if policy.build_sidecars {
            crate::index::inline::Writer::Compaction
        } else {
            crate::index::inline::Writer::Plain
        };
        report.bytes_read += step.files.iter().map(|f| f.size).sum::<u64>();
        let tags = step.files.first().map(|f| user_tags(f)).unwrap_or_default();
        let mut written = if policy.streaming && Self::can_stream(snap, &step.files) {
            // A k-way merge over the sorted inputs, memory bounded by row groups (D59).
            let s = self.merge_streaming(snap, &step.files, writer).await?;
            report.sidecars_built += s.sidecars + s.merged_sidecars;
            report.sidecars_merged += s.merged_sidecars;
            report.streamed += 1;
            s.files
        } else {
            let (w, built) = self.merge_in_memory(snap, &step, writer).await?;
            report.sidecars_built += built;
            w
        };
        fail::fail_point!("moraine::compact::after_read");
        for f in &mut written {
            f.tags.extend(tags.clone());
            f.tags.insert(LEVEL_TAG.into(), step.level.to_string());
        }
        report.bytes_written += written.iter().map(|f| f.size).sum::<u64>();
        fail::fail_point!("moraine::compact::after_upload");
        fail::fail_point!("moraine::compact::before_commit");
        let added = written.len();
        let work = ReplaceWork {
            operation: "OPTIMIZE",
            remove: step.files.iter().map(|f| f.path.clone()).collect(),
            batches: Vec::new(),
            ready: written,
            tags: Default::default(),
            data_change: false,
            fence,
            started: Some(started),
            txns: Vec::new(),
        };
        self.commit_replace(work).await?;
        report.files_removed += step.files.len();
        report.files_added += added;
        Ok(())
    }

    /// The in-memory path: every input in memory, concatenated and sorted. For inputs that
    /// are not all sorted by the current sort columns (written by another tool, or before the
    /// cluster key was declared), and when streaming is off.
    async fn merge_in_memory(
        self: &Arc<Self>,
        snap: &Arc<Snapshot>,
        step: &Step,
        writer: crate::index::inline::Writer,
    ) -> Result<(Vec<DataFile>, usize)> {
        let schema = snap.schema();
        let all: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let mut parts: Vec<RecordBatch> = Vec::new();
        for f in &step.files {
            let path = self.layer.data_prefix().clone().join(f.path.as_str());
            let q = FileQuery {
                table_schema: schema.clone(),
                output: all.clone(),
                needed: schema.clone(),
                filter: None,
                physical: None,
            };
            let batches = read_file(
                &self.db.cache,
                self.db.store.object_store(),
                &path,
                f.size,
                &q,
            )
            .await?;
            for b in batches {
                parts.push(to_schema(&b, schema)?);
            }
        }
        let merged = concat_batches(schema, &parts)?;
        drop(parts);
        // A stable sort: rows with equal keys keep the files' order and their order inside them.
        let sorted = files::sort_rows(schema, &snap.def, &[merged])?;
        self.upload(snap, sorted, writer, None).await
    }
}

/// A file's caller tags ([`crate::Replace::tag`]): its tags outside the reserved `moraine.`
/// namespace.
fn user_tags(f: &DataFile) -> std::collections::BTreeMap<String, String> {
    f.tags
        .iter()
        .filter(|(k, _)| !k.starts_with("moraine."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
