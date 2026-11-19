//! Index before commit (D58): the writer of a data file builds its sidecars from the
//! rows it is writing, before the commit that adds the file, so readers never see the file
//! without them and its rows never take the exact route.
//!
//! Appends do it for every index whose policy allows it ([`BeforeCommit`]), compaction for
//! every index (D48). The rows are fed as the file is written, a batch at a time, so a streamed
//! file needs no second read; the bundle's source row count and size go into its header when
//! the file is done. Each sidecar is put with the data file's round trip, under a name no other
//! client can know yet, so no listing or HEAD is needed.

use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow::array::RecordBatch;
use serde_json::Value;

use super::build::{BundleBuild, RemoveOnDrop, upload};
use super::discover::{Listed, sidecar_location};
use super::kind::IndexKind;
use super::spec::{IndexSpec, parse_specs, spec_hash};
use crate::error::{Error, Result};
use crate::snapshot::Snapshot;
use crate::table::TableInner;

/// Policy keys of an index spec (`policy`, not hashed) that bound what an append builds.
pub const BEFORE_COMMIT: &str = "before_commit";
pub const BEFORE_COMMIT_MAX_ROWS: &str = "before_commit_max_rows";
pub const BEFORE_COMMIT_MAX_BUILD_BYTES: &str = "before_commit_max_build_bytes";

/// Default largest file an append indexes before its commit.
pub const DEFAULT_MAX_ROWS: u64 = 1_000_000;
/// Default largest estimated build work (rows times the kind's bytes per row) of an append's
/// build: 2 GiB, about 900,000 rows of 256-dimension usearch F32 (D37's estimate).
pub const DEFAULT_MAX_BUILD_BYTES: u64 = 2 << 30;

/// What an append's index build before the commit may cost, from a spec's `policy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BeforeCommit {
    pub enabled: bool,
    pub max_rows: u64,
    pub max_build_bytes: u64,
}

impl BeforeCommit {
    pub fn of(spec: &IndexSpec) -> BeforeCommit {
        let p = &spec.policy;
        let num = |k: &str, d: u64| p.get(k).and_then(Value::as_u64).unwrap_or(d);
        BeforeCommit {
            enabled: p
                .get(BEFORE_COMMIT)
                .and_then(Value::as_bool)
                .unwrap_or(true),
            max_rows: num(BEFORE_COMMIT_MAX_ROWS, DEFAULT_MAX_ROWS),
            max_build_bytes: num(BEFORE_COMMIT_MAX_BUILD_BYTES, DEFAULT_MAX_BUILD_BYTES),
        }
    }

    /// Whether an append of `rows` rows builds this index before its commit.
    pub fn allows(&self, kind: &dyn IndexKind, spec: &IndexSpec, rows: u64) -> bool {
        self.enabled
            && rows <= self.max_rows
            && rows.saturating_mul(kind.build_bytes_per_row(spec) as u64) <= self.max_build_bytes
    }
}

/// Who writes the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Writer {
    /// An append or the new rows of an upsert: indexes whose policy allows it; the implicit key
    /// index from `key_index_inline_min_rows` rows (D19).
    Append,
    /// Compaction's merged files: every index (D48).
    Compaction,
    /// No sidecar: the index build job adds them later.
    Plain,
}

struct One {
    spec: IndexSpec,
    kind: Arc<dyn IndexKind>,
    hash: String,
    cols: Vec<usize>,
    build: BundleBuild<BufWriter<std::fs::File>>,
    tmp: RemoveOnDrop,
}

/// The sidecar builds of one data file being written.
pub(crate) struct InlineBuilds {
    builds: Vec<One>,
}

impl InlineBuilds {
    pub fn empty() -> InlineBuilds {
        InlineBuilds { builds: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.builds.is_empty()
    }

    /// Feeds rows `first_row..` of the file (in the table schema). Blocking work: the builds
    /// of several indexes run on threads of their own (D80).
    pub fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()> {
        const STEP: usize = 1024;
        let feed = |b: &mut One| -> Result<()> {
            let p = batch.project(&b.cols)?;
            let mut at = 0;
            while at < p.num_rows() {
                let n = STEP.min(p.num_rows() - at);
                b.build.add(first_row + at as u64, &p.slice(at, n))?;
                at += n;
            }
            Ok(())
        };
        if self.builds.len() <= 1 {
            return self.builds.iter_mut().try_for_each(feed);
        }
        std::thread::scope(|s| {
            let handles: Vec<_> = self
                .builds
                .iter_mut()
                .map(|b| s.spawn(move || feed(b)))
                .collect();
            handles.into_iter().try_for_each(|h| {
                h.join()
                    .unwrap_or_else(|_| Err(Error::Internal("an index build panicked".into())))
            })
        })
    }
}

impl TableInner {
    /// The specs a file of `rows` rows gets before its commit.
    pub(crate) fn inline_specs(
        &self,
        snap: &Snapshot,
        rows: u64,
        writer: Writer,
    ) -> Result<Vec<(IndexSpec, Arc<dyn IndexKind>)>> {
        let rt = &self.db.index;
        let mut out = Vec::new();
        if writer == Writer::Plain {
            return Ok(out);
        }
        for spec in parse_specs(&snap.meta.configuration)? {
            let Ok(kind) = rt.kind(&spec.kind) else {
                continue;
            };
            if !kind.wants_sidecar(&spec, rows) {
                continue;
            }
            if writer == Writer::Append
                && !BeforeCommit::of(&spec).allows(kind.as_ref(), &spec, rows)
            {
                continue;
            }
            out.push((spec, kind.clone()));
        }
        if let Some(spec) = super::key::key_spec(&snap.def)
            && let Ok(kind) = rt.kind(&spec.kind)
            && kind.wants_sidecar(&spec, rows)
            && (writer == Writer::Compaction || rows >= rt.cfg.key_inline_min_rows)
        {
            out.push((spec, kind.clone()));
        }
        Ok(out)
    }

    /// Starts the builds of `specs` for a file of about `rows_hint` rows of `snap`'s schema.
    pub(crate) fn start_inline(
        &self,
        snap: &Snapshot,
        specs: Vec<(IndexSpec, Arc<dyn IndexKind>)>,
        rows_hint: u64,
    ) -> Result<InlineBuilds> {
        let rt = &self.db.index;
        let schema = snap.schema();
        let mut builds = Vec::with_capacity(specs.len());
        for (spec, kind) in specs {
            let cols = spec
                .columns
                .iter()
                .map(|c| schema.index_of(c))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let hash = spec_hash(kind.as_ref(), &spec);
            let tmp = rt.build_dir().join(format!(
                "inline-{}.osix",
                crate::clock::random_name(&crate::clock::SystemClock::new())
            ));
            let tmp = RemoveOnDrop(tmp);
            let file = std::fs::File::create(&tmp.0)?;
            let build = BundleBuild::with_threads(
                kind.as_ref(),
                &spec,
                rows_hint,
                0,
                rt.cfg.build_budget,
                rt.cfg.threads_for(rt.cfg.build_budget, rows_hint),
                BufWriter::with_capacity(1 << 20, file),
            )?;
            builds.push(One {
                spec,
                kind,
                hash,
                cols,
                build,
                tmp,
            });
        }
        Ok(InlineBuilds { builds })
    }

    /// Finishes `builds` for data file `path` (relative to `data/`) of `rows` rows and `size`
    /// bytes, and uploads the sidecars. Returns the number uploaded.
    pub(crate) async fn finish_inline(
        &self,
        builds: InlineBuilds,
        path: &str,
        rows: u64,
        size: u64,
    ) -> Result<usize> {
        if builds.is_empty() {
            return Ok(0);
        }
        let done = tokio::task::spawn_blocking(move || -> Result<Vec<(One2, u64)>> {
            let finish = |mut b: One| -> Result<(One2, u64)> {
                b.build.set_source(rows, size);
                let (mut w, len, _) = b.build.finish()?;
                w.flush()?;
                drop(w);
                Ok((
                    One2 {
                        spec: b.spec,
                        hash: b.hash,
                        tmp: b.tmp,
                        _kind: b.kind,
                    },
                    len,
                ))
            };
            if builds.builds.len() <= 1 {
                return builds.builds.into_iter().map(finish).collect();
            }
            std::thread::scope(|s| {
                let handles: Vec<_> = builds
                    .builds
                    .into_iter()
                    .map(|b| s.spawn(move || finish(b)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            Err(Error::Internal("an index build panicked".into()))
                        })
                    })
                    .collect()
            })
        })
        .await
        .map_err(|e| Error::Internal(format!("inline build task: {e}")))??;
        let store = self.db.store.object_store().clone();
        let idx = self.idx_prefix();
        let state = self.db.index.table(idx.as_ref());
        let ups = done.into_iter().map(|(b, len)| {
            let (store, idx, state) = (store.clone(), idx.clone(), state.clone());
            async move {
                let location = sidecar_location(&idx, path, &b.spec.name, &b.hash);
                let etag = upload(&store, &location, &b.tmp.0, len).await?;
                drop(b.tmp);
                if let Some(etag) = etag {
                    state.discovery.add_own(
                        path,
                        &b.spec.name,
                        Listed {
                            location,
                            spec_hash: b.hash,
                            size: len,
                            etag,
                        },
                    );
                }
                Ok::<_, Error>(())
            }
        });
        use futures::{StreamExt, TryStreamExt};
        let n = futures::stream::iter(ups)
            .buffer_unordered(self.db.opts.io_concurrency.max(1))
            .try_collect::<Vec<()>>()
            .await?
            .len();
        Ok(n)
    }

    /// Builds the sidecars of a data file held in memory (`rows`, in the table schema) on a
    /// blocking thread, ready for [`TableInner::finish_inline`].
    pub(crate) async fn build_inline(
        &self,
        snap: &Snapshot,
        rows: &RecordBatch,
        writer: Writer,
    ) -> Result<Option<InlineBuilds>> {
        let n = rows.num_rows() as u64;
        let specs = self.inline_specs(snap, n, writer)?;
        if specs.is_empty() {
            return Ok(None);
        }
        let _permit = self
            .db
            .index
            .reserve_build(self.db.index.cfg.build_budget.saturating_mul(specs.len()))
            .await?;
        let mut builds = self.start_inline(snap, specs, n)?;
        let rows = rows.clone();
        let builds = tokio::task::spawn_blocking(move || -> Result<InlineBuilds> {
            builds.add(0, &rows)?;
            Ok(builds)
        })
        .await
        .map_err(|e| Error::Internal(format!("inline build task: {e}")))??;
        Ok(Some(builds))
    }
}

/// A finished build waiting for its upload.
struct One2 {
    spec: IndexSpec,
    hash: String,
    tmp: RemoveOnDrop,
    _kind: Arc<dyn IndexKind>,
}
