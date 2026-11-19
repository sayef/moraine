//! Building sidecars (D35).
//!
//! [`BundleBuild`] drives one kind's builder over a file's rows into a bundle, keeping memory
//! bounded: before building it sizes parts from the kind's per-row estimate, and whenever the
//! builder reports more than the budget it seals a part into its own section. The input is one
//! batch of 1,024 rows at a time, with only the indexed columns, and the output streams to a
//! temporary file.
//!
//! [`crate::Table::build_indexes`] is the builder job: it lists what is missing, largest files
//! first, reserves each build's budget from a byte semaphore shared by the database's tables,
//! builds, and uploads with put-if-absent unless the sidecar appeared meanwhile. A build that
//! fails for a data reason is retried with exponential backoff (1 minute doubling to 1 hour).

use std::collections::HashMap;
use std::io::{BufWriter, Seek, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::RecordBatch;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use super::bundle::{BundleMeta, BundleWriter};
use super::discover::{Listed, data_segment, sidecar_location};
use super::kind::{BuildCtx, BuildSummary, IndexBuilder, IndexKind};
use super::spec::{IndexSpec, parse_specs, spec_hash};
use crate::error::{Error, Result};
use crate::log::DataFile;
use crate::read::fetch::scan_windows;
use crate::schema::project_schema;
use crate::table::TableInner;

/// Share of the budget the estimate may fill before a part is sealed: estimates can be low.
const PART_FILL: f64 = 0.8;
/// Uploads above this go up in parts.
const SINGLE_PUT: u64 = 8 << 20;
/// First retry delay after a failed build, doubling up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(60);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

/// Drives one build. See the module documentation.
pub struct BundleBuild<W: Write + Seek> {
    builder: Box<dyn IndexBuilder>,
    writer: BundleWriter<W>,
    budget: usize,
    part_rows: u64,
    in_part: u64,
    /// The most the builder reported holding, sampled after every batch.
    pub peak_builder_bytes: usize,
}

impl<W: Write + Seek> BundleBuild<W> {
    /// Starts a build of `spec` for a source file of `source_rows` rows and `source_size`
    /// bytes, holding at most about `budget` bytes.
    pub fn new(
        kind: &dyn IndexKind,
        spec: &IndexSpec,
        source_rows: u64,
        source_size: u64,
        budget: usize,
        out: W,
    ) -> Result<Self> {
        Self::with_threads(kind, spec, source_rows, source_size, budget, 1, out)
    }

    /// [`BundleBuild::new`] with up to `threads` threads for the builder (D80).
    pub fn with_threads(
        kind: &dyn IndexKind,
        spec: &IndexSpec,
        source_rows: u64,
        source_size: u64,
        budget: usize,
        threads: usize,
        out: W,
    ) -> Result<Self> {
        let meta = BundleMeta {
            kind: kind.name().into(),
            kind_format: kind.format_version(),
            spec_hash: spec_hash(kind, spec),
            source_rows,
            source_size,
        };
        let ctx = BuildCtx {
            source_rows,
            budget_bytes: budget,
            threads: threads.max(1),
        };
        let per_row = kind.build_bytes_per_row(spec).max(1);
        let part_rows = ((budget as f64 * PART_FILL) as usize / per_row).max(1) as u64;
        Ok(BundleBuild {
            builder: kind.builder(spec, &ctx)?,
            writer: BundleWriter::new(out, meta)?,
            budget,
            part_rows,
            in_part: 0,
            peak_builder_bytes: 0,
        })
    }

    /// A build around a builder that already holds its rows, such as one from
    /// [`IndexKind::merge`]: [`BundleBuild::finish`] writes it out.
    pub fn from_builder(
        kind: &dyn IndexKind,
        spec: &IndexSpec,
        builder: Box<dyn IndexBuilder>,
        source_rows: u64,
        source_size: u64,
        out: W,
    ) -> Result<Self> {
        let meta = BundleMeta {
            kind: kind.name().into(),
            kind_format: kind.format_version(),
            spec_hash: spec_hash(kind, spec),
            source_rows,
            source_size,
        };
        Ok(BundleBuild {
            builder,
            writer: BundleWriter::new(out, meta)?,
            budget: usize::MAX,
            part_rows: u64::MAX,
            in_part: 0,
            peak_builder_bytes: 0,
        })
    }

    /// Feeds rows `first_row..` (the spec's columns, in order).
    pub fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()> {
        // Split batches so a part never overshoots its row count by more than a batch.
        let mut at = 0usize;
        while at < batch.num_rows() {
            let room = (self.part_rows - self.in_part).max(1) as usize;
            let n = room.min(batch.num_rows() - at);
            self.builder
                .add(first_row + at as u64, &batch.slice(at, n))?;
            at += n;
            self.in_part += n as u64;
            let held = self.builder.memory_bytes();
            self.peak_builder_bytes = self.peak_builder_bytes.max(held);
            if self.in_part >= self.part_rows || held >= self.budget {
                self.builder.seal_part(&mut self.writer)?;
                self.in_part = 0;
            }
        }
        Ok(())
    }

    /// Sets the source file's row count and size, known only once a streamed file is written.
    pub fn set_source(&mut self, rows: u64, size: u64) {
        self.writer.set_source(rows, size);
    }

    /// Finishes the bundle; returns the sink, the bundle's length and what was built.
    pub fn finish(self) -> Result<(W, u64, BuildSummary)> {
        let BundleBuild {
            builder,
            mut writer,
            ..
        } = self;
        let summary = builder.finish(&mut writer)?;
        let (w, len) = writer.finish(summary.indexed_rows)?;
        Ok((w, len, summary))
    }
}

/// Options of one [`crate::Table::build_indexes`] call: [`BuildOptions::default`] builds every
/// index, one build at a time.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BuildOptions {
    /// Only these indexes (all when empty).
    pub indexes: Vec<String>,
    /// Builds run at once (default 1).
    pub parallelism: usize,
    /// At most this many builds; the rest wait for the next call.
    pub max_builds: usize,
}

impl Default for BuildOptions {
    fn default() -> Self {
        BuildOptions {
            indexes: Vec::new(),
            parallelism: 1,
            max_builds: usize::MAX,
        }
    }
}

impl BuildOptions {
    /// Only build these indexes (all when empty).
    pub fn indexes<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.indexes = names.into_iter().map(Into::into).collect();
        self
    }

    /// Builds run at once (default 1).
    pub fn parallelism(mut self, builds: usize) -> Self {
        self.parallelism = builds;
        self
    }

    /// At most this many builds per call; the rest wait for the next call.
    pub fn max_builds(mut self, builds: usize) -> Self {
        self.max_builds = builds;
        self
    }
}

/// What a [`crate::Table::build_indexes`] call did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BuildReport {
    /// Sidecars built and uploaded.
    pub built: usize,
    /// Another builder uploaded the sidecar first (case V14).
    pub already_present: usize,
    /// Files below the kind's threshold: answered exactly, never given a sidecar (V3).
    pub exact_by_policy: usize,
    /// Builds that failed; each is retried after a backoff.
    pub failed: usize,
    /// Builds skipped because an earlier failure's backoff has not run out.
    pub backing_off: usize,
    /// Bytes of the sidecars uploaded.
    pub bytes_uploaded: u64,
    /// Sealed parts over all builds.
    pub parts: u64,
    /// Indexes whose kind this client has not registered.
    pub unregistered: Vec<String>,
    /// The most any builder reported holding (its own estimate; native memory included only
    /// as far as the kind knows it).
    pub peak_builder_bytes: usize,
}

/// How an index stands: rows covered by sidecars against total rows.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexStatus {
    /// The index's name.
    pub name: String,
    /// The index's kind.
    pub kind: String,
    /// The spec hash sidecars are named by; empty when the kind is not registered in this
    /// client.
    pub spec_hash: String,
    /// Data files in the snapshot.
    pub files_total: u64,
    /// Data files with a sidecar of the current spec.
    pub files_covered: u64,
    /// Data files below the kind's threshold, answered exactly by design (case V3).
    pub files_exact_by_policy: u64,
    /// Data files whose last build failed.
    pub files_failed: u64,
    /// Rows in the snapshot.
    pub rows_total: u64,
    /// Rows in files with a sidecar.
    pub rows_covered: u64,
    /// Rows in files below the kind's threshold, answered exactly by design (case V3).
    pub rows_exact_by_policy: u64,
    /// Bytes of the sidecars of the current spec.
    pub sidecar_bytes: u64,
}

impl IndexStatus {
    /// Rows a query answers on the exact route: neither covered by a sidecar nor below the
    /// kind's threshold. Index before commit keeps it at zero for files within its limits.
    pub fn rows_pending(&self) -> u64 {
        self.rows_total
            .saturating_sub(self.rows_covered)
            .saturating_sub(self.rows_exact_by_policy)
    }

    /// The share of rows on the exact route, files below the threshold included.
    pub fn exact_share(&self) -> f64 {
        if self.rows_total == 0 {
            0.0
        } else {
            (self.rows_total - self.rows_covered.min(self.rows_total)) as f64
                / self.rows_total as f64
        }
    }
}

/// Failed builds of one table: (data file, spec hash) to (failures, retry not before).
#[derive(Debug, Default)]
pub(crate) struct Failures(Mutex<HashMap<(String, String), (u32, Duration)>>);

impl Failures {
    fn blocked(&self, key: &(String, String), now: Duration) -> bool {
        self.0
            .lock()
            .ok()
            .and_then(|g| g.get(key).map(|(_, until)| *until > now))
            .unwrap_or(false)
    }

    fn record(&self, key: (String, String), now: Duration) {
        if let Ok(mut g) = self.0.lock() {
            let e = g.entry(key).or_insert((0, now));
            let delay = FIRST_BACKOFF
                .saturating_mul(1u32 << e.0.min(6))
                .min(MAX_BACKOFF);
            e.0 += 1;
            e.1 = now + delay;
        }
    }

    fn clear(&self, key: &(String, String)) {
        if let Ok(mut g) = self.0.lock() {
            g.remove(key);
        }
    }

    pub fn count_for(&self, hash: &str) -> u64 {
        self.0
            .lock()
            .map(|g| g.keys().filter(|(_, h)| h == hash).count() as u64)
            .unwrap_or(0)
    }
}

struct Job {
    file: Arc<DataFile>,
    spec: IndexSpec,
    kind: Arc<dyn IndexKind>,
    hash: String,
}

impl TableInner {
    /// See [`crate::Table::build_indexes`].
    pub(crate) async fn build_indexes(self: &Arc<Self>, opts: BuildOptions) -> Result<BuildReport> {
        let snap = self.refresh().await?;
        let specs = parse_specs(&snap.meta.configuration)?;
        let rt = &self.db.index;
        let state = rt.table(self.idx_prefix().as_ref());
        let clock = self.db.clock().clone();
        let map = state
            .discovery
            .map(
                self.db.store.object_store(),
                &self.idx_prefix(),
                snap.version,
                clock.now(),
                Duration::ZERO,
            )
            .await?;
        let mut report = BuildReport::default();
        let mut jobs = Vec::new();
        // The implicit key index (D19) is built like any other.
        let specs = specs.into_iter().chain(super::key::key_spec(&snap.def));
        for spec in specs {
            if !opts.indexes.is_empty() && !opts.indexes.contains(&spec.name) {
                continue;
            }
            let Ok(kind) = rt.kind(&spec.kind) else {
                report.unregistered.push(spec.name.clone());
                continue;
            };
            let kind = kind.clone();
            let hash = spec_hash(kind.as_ref(), &spec);
            for f in snap.files.iter() {
                let listed = map
                    .get(&(data_segment(&f.path), spec.name.clone()))
                    .is_some_and(|v| v.iter().any(|l| l.spec_hash == hash));
                if listed {
                    continue;
                }
                if f.rows().is_some_and(|r| !kind.wants_sidecar(&spec, r)) {
                    report.exact_by_policy += 1;
                    continue;
                }
                if state
                    .failures
                    .blocked(&(f.path.clone(), hash.clone()), clock.now())
                {
                    report.backing_off += 1;
                    continue;
                }
                jobs.push(Job {
                    file: f.clone(),
                    spec: spec.clone(),
                    kind: kind.clone(),
                    hash: hash.clone(),
                });
            }
        }
        // Largest files first: they cost queries the most.
        jobs.sort_by(|a, b| {
            b.file
                .size
                .cmp(&a.file.size)
                .then(a.file.path.cmp(&b.file.path))
        });
        jobs.truncate(opts.max_builds);
        use futures::StreamExt;
        let results: Vec<(Job, Result<Built>)> = futures::stream::iter(jobs)
            .map(|job| {
                let me = self.clone();
                async move {
                    let r = me.build_one(&job).await;
                    (job, r)
                }
            })
            .buffer_unordered(opts.parallelism.max(1))
            .collect()
            .await;
        for (job, r) in results {
            let key = (job.file.path.clone(), job.hash.clone());
            match r {
                Ok(Built::Uploaded { bytes, parts, peak }) => {
                    state.failures.clear(&key);
                    report.peak_builder_bytes = report.peak_builder_bytes.max(peak);
                    report.built += 1;
                    report.bytes_uploaded += bytes;
                    report.parts += parts;
                }
                Ok(Built::Present) => report.already_present += 1,
                Err(e) if is_data_error(&e) => {
                    state.failures.record(key, clock.now());
                    report.failed += 1;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(report)
    }

    async fn build_one(self: &Arc<Self>, job: &Job) -> Result<Built> {
        let rt = &self.db.index;
        let store = self.db.store.object_store().clone();
        let location = sidecar_location(
            &self.idx_prefix(),
            &job.file.path,
            &job.spec.name,
            &job.hash,
        );
        // Reserve this build's memory from the database-wide budget.
        let budget = rt.cfg.build_budget;
        let _permit = rt.reserve_build(budget).await?;
        let source_rows = match job.file.rows() {
            Some(r) => r,
            None => {
                let path = self
                    .layer
                    .data_prefix()
                    .clone()
                    .join(job.file.path.as_str());
                let f = self.db.cache.get(&store, &path, job.file.size).await?;
                f.reader_meta.metadata().file_metadata().num_rows().max(0) as u64
            }
        };
        // The system clock's randomness, not the database's: a simulated clock repeats its
        // stream across handles in one process, and two builds must never share a file.
        let tmp = rt.build_dir().join(format!(
            "build-{}.osix",
            crate::clock::random_name(&crate::clock::SystemClock::new())
        ));
        let _cleanup = RemoveOnDrop(tmp.clone());
        let file = std::fs::File::create(&tmp)?;
        let mut build = BundleBuild::with_threads(
            job.kind.as_ref(),
            &job.spec,
            source_rows,
            job.file.size,
            budget,
            rt.cfg.threads_for(budget, source_rows),
            BufWriter::with_capacity(1 << 20, file),
        )?;
        // Index columns cannot be dropped, so any version since the index was created has them.
        let schema = project_schema(self.current().schema(), &job.spec.columns)?;
        let path = self
            .layer
            .data_prefix()
            .clone()
            .join(job.file.path.as_str());
        let cache = &self.db.cache;
        // The rows are read here and fed to the builder on a blocking thread: one batch is in
        // flight at a time, so input memory is a batch, not the file.
        let (tx, rx) = std::sync::mpsc::sync_channel::<(u64, RecordBatch)>(1);
        let worker = tokio::task::spawn_blocking(move || -> Result<BundleBuild<_>> {
            for (first, batch) in rx {
                build.add(first, &batch)?;
            }
            Ok(build)
        });
        // Column data is fetched in windows of about 4 MiB, so the input never holds a whole
        // column chunk (16 MiB for 16,384 rows of 256-dimension floats).
        let read = scan_windows(
            cache,
            &store,
            &path,
            job.file.size,
            &schema,
            4 << 20,
            &mut |o, b| {
                let first = o.first().copied().unwrap_or(0);
                // A closed channel means the builder failed; its error is reported below.
                let _ = tx.send((first, b.clone()));
                Ok(())
            },
        )
        .await;
        drop(tx);
        let build = worker
            .await
            .map_err(|e| Error::Internal(format!("build task: {e}")))??;
        read?;
        let peak = build.peak_builder_bytes;
        let (mut w, len, summary) = tokio::task::spawn_blocking(move || build.finish())
            .await
            .map_err(|e| Error::Internal(format!("build task: {e}")))??;
        w.flush()?;
        drop(w);
        // Skip the upload when another builder won meanwhile (case V14).
        if store.head(&location).await.is_ok() {
            return Ok(Built::Present);
        }
        let etag = upload(&store, &location, &tmp, len).await?;
        let etag = match etag {
            Some(e) => e,
            None => return Ok(Built::Present),
        };
        rt.table(self.idx_prefix().as_ref()).discovery.add_own(
            &job.file.path,
            &job.spec.name,
            Listed {
                location,
                spec_hash: job.hash.clone(),
                size: len,
                etag,
            },
        );
        Ok(Built::Uploaded {
            bytes: len,
            parts: u64::from(summary.parts),
            peak,
        })
    }

    /// See [`crate::Table::index_status`].
    pub(crate) async fn index_status(self: &Arc<Self>) -> Result<Vec<IndexStatus>> {
        let snap = self.snapshot_for_query().await?;
        let specs = parse_specs(&snap.meta.configuration)?;
        let rt = &self.db.index;
        let state = rt.table(self.idx_prefix().as_ref());
        let clock = self.db.clock();
        let map = state
            .discovery
            .map(
                self.db.store.object_store(),
                &self.idx_prefix(),
                snap.version,
                clock.now(),
                rt.cfg.poll,
            )
            .await?;
        let mut out = Vec::new();
        for spec in specs {
            let kind = rt.kind(&spec.kind).ok();
            let hash = kind
                .map(|k| spec_hash(k.as_ref(), &spec))
                .unwrap_or_default();
            let mut s = IndexStatus {
                name: spec.name.clone(),
                kind: spec.kind.clone(),
                spec_hash: hash.clone(),
                files_total: snap.files.len() as u64,
                files_covered: 0,
                files_exact_by_policy: 0,
                files_failed: state.failures.count_for(&hash),
                rows_total: 0,
                rows_covered: 0,
                rows_exact_by_policy: 0,
                sidecar_bytes: 0,
            };
            for f in snap.files.iter() {
                let rows = f.rows().unwrap_or(0);
                s.rows_total += rows;
                let listed = map
                    .get(&(data_segment(&f.path), spec.name.clone()))
                    .and_then(|v| v.iter().find(|l| !hash.is_empty() && l.spec_hash == hash));
                if let Some(l) = listed {
                    s.files_covered += 1;
                    s.rows_covered += rows;
                    s.sidecar_bytes += l.size;
                } else if let Some(k) = kind
                    && f.rows().is_some_and(|r| !k.wants_sidecar(&spec, r))
                {
                    s.files_exact_by_policy += 1;
                    s.rows_exact_by_policy += rows;
                }
            }
            out.push(s);
        }
        Ok(out)
    }
}

enum Built {
    Uploaded { bytes: u64, parts: u64, peak: usize },
    Present,
}

/// Errors caused by the data (a corrupt file, a kind refusing the rows), as opposed to the
/// store or the configuration: retried with backoff, not returned.
fn is_data_error(e: &Error) -> bool {
    matches!(
        e,
        Error::Parquet(_) | Error::Arrow(_) | Error::InvalidQuery(_) | Error::Internal(_)
    )
}

/// Uploads the bundle at `tmp`: one put-if-absent up to 8 MiB, a multipart upload above.
/// Returns the new object's ETag, or `None` when the key appeared meanwhile.
pub(crate) async fn upload(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
    tmp: &std::path::Path,
    len: u64,
) -> Result<Option<Option<String>>> {
    use std::io::Read;
    if len <= SINGLE_PUT {
        let body = std::fs::read(tmp)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        return match store.put_opts(location, PutPayload::from(body), opts).await {
            Ok(r) => Ok(Some(r.e_tag)),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(None),
            Err(object_store::Error::NotImplemented { .. }) => {
                let body = std::fs::read(tmp)?;
                Ok(Some(
                    store.put(location, PutPayload::from(body)).await?.e_tag,
                ))
            }
            Err(e) => Err(e.into()),
        };
    }
    // Multipart: atomic on completion; streamed from the file in 8 MiB parts.
    let mut up = store.put_multipart(location).await?;
    let mut f = std::fs::File::open(tmp)?;
    let mut left = len;
    while left > 0 {
        let n = left.min(SINGLE_PUT) as usize;
        let mut buf = vec![0u8; n];
        f.read_exact(&mut buf)?;
        if let Err(e) = up.put_part(PutPayload::from(buf)).await {
            let _ = up.abort().await;
            return Err(e.into());
        }
        left -= n as u64;
    }
    Ok(Some(up.complete().await?.e_tag))
}

pub(crate) struct RemoveOnDrop(pub std::path::PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
