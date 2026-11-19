//! The `vec-hnsw` kind: a usearch HNSW graph per data file (spike item 10).
//!
//! Each part of a build is one section `hnsw.<n>` holding usearch's own file. Opening views the
//! sections in place (`view_from_buffer` over the 4,096-aligned mapped bytes), so a handle's
//! resident cost is small; the bundle bytes stay alive with the handle. Keys are row offsets.
//! With F32 vectors the graph's distances are exact; with F16 or I8 they are approximate and
//! the query rescores `rerank` extra candidates per hit from the rows.

use std::any::Any;
use std::io::Read;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use roaring::RoaringBitmap;
use serde_json::{Value, json};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use super::{ExactVector, Metric, VectorParams, Vectors, uint, with_defaults as merge};
use crate::error::{Error, Result};
use crate::index::bundle::{BundleView, SectionSink};
use crate::index::kind::{
    BuildCtx, BuildSummary, ExactScorer, FileIndex, Hit, IndexBuilder, IndexKind, Order, Query,
};
use crate::index::spec::IndexSpec;
use crate::index::topk::{Candidate, TopK};

/// Default `expansion_search` (D16, revisited against T9 in D37).
pub const DEFAULT_EXPANSION_SEARCH: u64 = 128;
/// Files below this many rows are always searched exactly (the design default).
pub const DEFAULT_MIN_ROWS_FOR_ANN: u64 = 10_000;

/// See the module documentation.
#[derive(Debug, Default)]
pub struct HnswKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Quant {
    F32,
    F16,
    I8,
}

impl Quant {
    fn parse(s: &str) -> Result<Quant> {
        match s {
            "f32" => Ok(Quant::F32),
            "f16" => Ok(Quant::F16),
            "i8" => Ok(Quant::I8),
            other => Err(Error::InvalidIndexSpec(format!(
                "unknown quantization {other:?} (f32, f16, i8)"
            ))),
        }
    }

    fn scalar(self) -> ScalarKind {
        match self {
            Quant::F32 => ScalarKind::F32,
            Quant::F16 => ScalarKind::F16,
            Quant::I8 => ScalarKind::I8,
        }
    }

    fn bytes(self) -> usize {
        match self {
            Quant::F32 => 4,
            Quant::F16 => 2,
            Quant::I8 => 1,
        }
    }
}

struct Params {
    v: VectorParams,
    quant: Quant,
    connectivity: usize,
    expansion_add: usize,
}

impl Params {
    fn of(spec: &IndexSpec) -> Result<Params> {
        let v = VectorParams::of(spec)?;
        let quant = Quant::parse(
            spec.params
                .get("quantization")
                .and_then(Value::as_str)
                .unwrap_or("f32"),
        )?;
        Ok(Params {
            v,
            quant,
            connectivity: uint(&spec.params, "connectivity", 16) as usize,
            expansion_add: uint(&spec.params, "expansion_add", 128) as usize,
        })
    }

    fn options(&self, expansion_search: usize) -> IndexOptions {
        IndexOptions {
            dimensions: self.v.dim,
            metric: match self.v.metric {
                Metric::Cosine => MetricKind::Cos,
                Metric::L2 => MetricKind::L2sq,
                Metric::InnerProduct => MetricKind::IP,
            },
            quantization: self.quant.scalar(),
            connectivity: self.connectivity,
            expansion_add: self.expansion_add,
            expansion_search,
            multi: false,
        }
    }
}

fn native(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("usearch: {e}"))
}

impl IndexKind for HnswKind {
    fn name(&self) -> &'static str {
        "vec-hnsw"
    }

    fn format_version(&self) -> u32 {
        1
    }

    /// usearch's file format follows its major and minor version.
    fn library_fingerprint(&self) -> String {
        let v = usearch::version();
        let mm: Vec<&str> = v.split('.').take(2).collect();
        format!("usearch-{}", mm.join("."))
    }

    fn order(&self) -> Order {
        Order::Ascending
    }

    fn resolve(&self, spec: &IndexSpec, schema: &Schema) -> Result<IndexSpec> {
        let mut p = VectorParams::resolve(spec, schema)?;
        p.entry("quantization").or_insert_with(|| "f32".into());
        p.entry("connectivity").or_insert_with(|| 16.into());
        p.entry("expansion_add").or_insert_with(|| 128.into());
        let mut s = spec.clone();
        s.params = Value::Object(p);
        let params = Params::of(&s)?;
        if !(2..=256).contains(&params.connectivity) || params.expansion_add == 0 {
            return Err(Error::InvalidIndexSpec(
                "connectivity must be 2 to 256 and expansion_add positive".into(),
            ));
        }
        let rerank = if params.quant == Quant::F32 { 0 } else { 2 };
        s.search = merge(
            &s.search,
            json!({"expansion_search": DEFAULT_EXPANSION_SEARCH, "rerank": rerank}),
        );
        s.policy = merge(
            &s.policy,
            json!({"min_rows_for_ann": DEFAULT_MIN_ROWS_FOR_ANN}),
        );
        Ok(s)
    }

    fn wants_sidecar(&self, spec: &IndexSpec, file_rows: u64) -> bool {
        file_rows >= uint(&spec.policy, "min_rows_for_ann", DEFAULT_MIN_ROWS_FOR_ANN)
    }

    fn build_bytes_per_row(&self, spec: &IndexSpec) -> usize {
        match Params::of(spec) {
            // The vector and the base layer's 2 * connectivity neighbour slots, doubled for node
            // headers, upper layers and allocator slack (measured: usearch reports about
            // 1.9 KiB per 256-dimension F32 row at connectivity 16; this gives 2.25 KiB).
            Ok(p) => (p.v.dim * p.quant.bytes() + 2 * p.connectivity * 4) * 2,
            Err(_) => 4096,
        }
    }

    fn builder(&self, spec: &IndexSpec, ctx: &BuildCtx) -> Result<Box<dyn IndexBuilder>> {
        let p = Params::of(spec)?;
        let per_row = self.build_bytes_per_row(spec).max(1);
        // The driver seals a part at 80 % of the budget by this estimate; reserve that much.
        let part_rows =
            ((ctx.budget_bytes * 4 / 5 / per_row) as u64).clamp(1, ctx.source_rows.max(1));
        Ok(Box::new(HnswBuilder {
            p,
            index: None,
            capacity: part_rows as usize,
            parts: 0,
            indexed: 0,
            threads: ctx.threads.max(1),
            pending_keys: Vec::new(),
            pending: Vec::new(),
        }))
    }

    fn open(&self, spec: &IndexSpec, bundle: BundleView) -> Result<Arc<dyn FileIndex>> {
        let p = Params::of(spec)?;
        let mut sections: Vec<(u32, &[u8])> = bundle
            .sections_with_prefix("hnsw.")
            .map(|(n, b)| {
                n["hnsw.".len()..]
                    .parse::<u32>()
                    .map(|i| (i, b))
                    .map_err(|_| Error::Internal(format!("vec-hnsw: bad section {n}")))
            })
            .collect::<Result<_>>()?;
        sections.sort_by_key(|(i, _)| *i);
        let mut parts = Vec::with_capacity(sections.len());
        for (_, bytes) in sections {
            let index =
                Index::new(&p.options(DEFAULT_EXPANSION_SEARCH as usize)).map_err(native)?;
            // SAFETY: `bytes` lives in `bundle`, which the handle keeps (field `_bundle`) for as
            // long as the index, and bundle bytes are never modified (mapped read-only).
            unsafe { index.view_from_buffer(bytes) }.map_err(native)?;
            if index.dimensions() != p.v.dim {
                return Err(Error::Internal(
                    "vec-hnsw: section dimension differs".into(),
                ));
            }
            parts.push(index);
        }
        Ok(Arc::new(HnswIndex {
            expansion: parts
                .iter()
                .map(|_| Mutex::new(DEFAULT_EXPANSION_SEARCH as usize))
                .collect(),
            parts,
            exact: p.quant == Quant::F32,
            _bundle: bundle,
        }))
    }

    fn exact(&self, spec: &IndexSpec, q: &Query<'_>) -> Result<Box<dyn ExactScorer>> {
        let params = VectorParams::of(spec)?;
        let query = params.check_query(q)?;
        Ok(Box::new(ExactVector { params, query }))
    }

    fn rerank(&self, spec: &IndexSpec, q: &Query<'_>) -> usize {
        let default = if Params::of(spec).is_ok_and(|p| p.quant == Quant::F32) {
            0
        } else {
            2
        };
        uint(q.params(), "rerank", default) as usize
    }

    fn validate(&self, spec: &IndexSpec, q: &Query<'_>) -> Result<()> {
        VectorParams::of(spec)?.check_query(q).map(drop)
    }
}

/// Rows each build thread is given at least: fewer are added on the caller's thread.
const ROWS_PER_THREAD: usize = 1024;
/// Rows taken from the shared queue at a time by a build thread.
const THREAD_BLOCK: usize = 64;

struct HnswBuilder {
    p: Params,
    index: Option<Index>,
    capacity: usize,
    parts: u32,
    indexed: u64,
    /// Threads the build may use (D80). With more than one, rows are gathered into `pending`
    /// until every thread has [`ROWS_PER_THREAD`] of them, then added at once.
    threads: usize,
    pending_keys: Vec<u64>,
    pending: Vec<f32>,
}

impl HnswBuilder {
    fn index(&mut self) -> Result<&Index> {
        if self.index.is_none() {
            let i =
                Index::new(&self.p.options(DEFAULT_EXPANSION_SEARCH as usize)).map_err(native)?;
            i.reserve_capacity_and_threads(self.capacity, self.threads)
                .map_err(native)?;
            self.index = Some(i);
        }
        self.index
            .as_ref()
            .ok_or_else(|| Error::Internal("vec-hnsw: no index".into()))
    }

    /// Adds the gathered rows, on up to `threads` threads. usearch takes concurrent adds once
    /// it has a slot per thread and room for every row; the threads take rows from a shared
    /// counter in small blocks, so a slow thread holds up no one. The graph then depends on how
    /// the threads interleave: a build on several threads is not byte-identical to another
    /// build of the same rows, though either answers within the same recall (D80).
    fn flush(&mut self) -> Result<()> {
        let n = self.pending_keys.len();
        if n == 0 {
            return Ok(());
        }
        let threads = self.threads.min(n / ROWS_PER_THREAD).max(1);
        let dim = self.p.v.dim;
        let all_threads = self.threads;
        let keys = std::mem::take(&mut self.pending_keys);
        let vecs = std::mem::take(&mut self.pending);
        let index = self.index()?;
        let need = index.size() + n;
        if need > index.capacity() {
            index
                .reserve_capacity_and_threads(need.max(index.capacity() * 2 + 1), all_threads)
                .map_err(native)?;
        }
        if threads <= 1 {
            for (i, k) in keys.iter().enumerate() {
                index
                    .add(*k, &vecs[i * dim..(i + 1) * dim])
                    .map_err(native)?;
            }
        } else {
            let next = std::sync::atomic::AtomicUsize::new(0);
            let failed: Mutex<Option<Error>> = Mutex::new(None);
            std::thread::scope(|s| {
                for _ in 0..threads {
                    s.spawn(|| {
                        loop {
                            let at =
                                next.fetch_add(THREAD_BLOCK, std::sync::atomic::Ordering::Relaxed);
                            if at >= n {
                                break;
                            }
                            for i in at..(at + THREAD_BLOCK).min(n) {
                                if let Err(e) = index.add(keys[i], &vecs[i * dim..(i + 1) * dim]) {
                                    let mut f = failed.lock().unwrap_or_else(|p| p.into_inner());
                                    f.get_or_insert(native(e));
                                    next.store(n, std::sync::atomic::Ordering::Relaxed);
                                    return;
                                }
                            }
                        }
                    });
                }
            });
            if let Some(e) = failed.into_inner().unwrap_or_else(|p| p.into_inner()) {
                return Err(e);
            }
            // Concurrent adds leave about one row in a thousand that no search reaches (a
            // neighbour list it was linked from was rewritten by another thread at the same
            // time). Each row added on the threads looks itself up; the few not found as their
            // own nearest are added again on this thread, which links them anew (D80).
            let lost: Mutex<Vec<usize>> = Mutex::new(Vec::new());
            let next = std::sync::atomic::AtomicUsize::new(0);
            std::thread::scope(|s| {
                for _ in 0..threads {
                    s.spawn(|| {
                        let mut mine = Vec::new();
                        loop {
                            let at =
                                next.fetch_add(THREAD_BLOCK, std::sync::atomic::Ordering::Relaxed);
                            if at >= n {
                                break;
                            }
                            for i in at..(at + THREAD_BLOCK).min(n) {
                                // An equal vector found first counts as found.
                                let found = index
                                    .search(&vecs[i * dim..(i + 1) * dim], 1)
                                    .is_ok_and(|m| {
                                        m.keys.first() == Some(&keys[i])
                                            || m.distances.first().is_some_and(|d| *d <= 1e-6)
                                    });
                                if !found {
                                    mine.push(i);
                                }
                            }
                        }
                        lost.lock().unwrap_or_else(|p| p.into_inner()).extend(mine);
                    });
                }
            });
            let mut lost = lost.into_inner().unwrap_or_else(|p| p.into_inner());
            lost.sort_unstable();
            for i in lost {
                index.remove(keys[i]).map_err(native)?;
                index
                    .add(keys[i], &vecs[i * dim..(i + 1) * dim])
                    .map_err(native)?;
            }
        }
        self.indexed += n as u64;
        // Keep the buffers' capacity for the next batch.
        self.pending_keys = keys;
        self.pending_keys.clear();
        self.pending = vecs;
        self.pending.clear();
        Ok(())
    }
}

impl IndexBuilder for HnswBuilder {
    fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()> {
        let v = Vectors::of(batch.column(0).as_ref())?;
        for i in 0..v.len() {
            let Some(x) = v.get(i).filter(|x| self.p.v.usable(x)) else {
                continue;
            };
            self.pending_keys.push(first_row + i as u64);
            self.pending.extend_from_slice(x);
        }
        // One thread adds as it goes; several wait until each has a share worth starting for.
        if self.threads <= 1 || self.pending_keys.len() >= self.threads * ROWS_PER_THREAD {
            self.flush()?;
        }
        Ok(())
    }

    fn memory_bytes(&self) -> usize {
        self.index.as_ref().map_or(0, |i| i.memory_usage())
            + self.pending.capacity() * 4
            + self.pending_keys.capacity() * 8
    }

    fn seal_part(&mut self, sink: &mut dyn SectionSink) -> Result<()> {
        self.flush()?;
        let Some(index) = self.index.take() else {
            return Ok(());
        };
        if index.size() == 0 {
            return Ok(());
        }
        // usearch writes its file itself; streaming it into the section keeps a second copy
        // out of memory.
        let tmp = std::env::temp_dir().join(format!(
            "moraine-hnsw-{}.usearch",
            crate::clock::random_name(&crate::clock::SystemClock::new())
        ));
        let path = tmp
            .to_str()
            .ok_or_else(|| Error::Internal("temporary path is not UTF-8".into()))?;
        let r = (|| -> Result<()> {
            index.save(path).map_err(native)?;
            drop(index);
            let mut f = std::fs::File::open(&tmp)?;
            let w = sink.section(&format!("hnsw.{}", self.parts))?;
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = f.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                w.write_all(&buf[..n])?;
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(&tmp);
        r?;
        self.parts += 1;
        Ok(())
    }

    fn finish(mut self: Box<Self>, sink: &mut dyn SectionSink) -> Result<BuildSummary> {
        self.seal_part(sink)?;
        Ok(BuildSummary {
            indexed_rows: self.indexed,
            parts: self.parts,
        })
    }
}

/// Fewer threads than this per file are not worth spawning: the parts are searched in turn.
const MIN_PART_THREADS: usize = 4;

struct HnswIndex {
    parts: Vec<Index>,
    exact: bool,
    /// Each graph's current `expansion_search`; searches of one part that change it take turns.
    expansion: Vec<Mutex<usize>>,
    _bundle: BundleView,
}

impl HnswIndex {
    /// Part `i`'s nearest `k` among the allowed rows, as (row offset, distance).
    fn search_one(
        &self,
        i: usize,
        vector: &[f32],
        params: &Value,
        allow: Option<&RoaringBitmap>,
        k: usize,
    ) -> Result<Vec<(u64, f32)>> {
        let (Some(p), Some(lock)) = (self.parts.get(i), self.expansion.get(i)) else {
            return Err(Error::Internal(format!("vec-hnsw: no part {i}")));
        };
        let ef = uint(params, "expansion_search", DEFAULT_EXPANSION_SEARCH).max(k as u64) as usize;
        let mut cur = lock.lock().unwrap_or_else(|p| p.into_inner());
        if *cur != ef {
            p.change_expansion_search(ef);
            *cur = ef;
        }
        let m = match allow {
            Some(a) => p
                .filtered_search(vector, k, |key: u64| {
                    u32::try_from(key).is_ok_and(|o| a.contains(o))
                })
                .map_err(native)?,
            None => p.search(vector, k).map_err(native)?,
        };
        Ok(m.keys.into_iter().zip(m.distances).collect())
    }

    fn hits(&self, found: impl IntoIterator<Item = (u64, f32)>, k: usize) -> Vec<Hit> {
        let mut top = TopK::new(Order::Ascending, k);
        let file: Arc<str> = "".into();
        for (key, d) in found {
            top.push(Candidate {
                file: file.clone(),
                offset: key,
                score: d,
                exact: self.exact,
            });
        }
        top.into_sorted()
            .into_iter()
            .map(|c| Hit {
                offset: c.offset,
                score: c.score,
                exact: c.exact,
            })
            .collect()
    }
}

impl FileIndex for HnswIndex {
    fn resident_bytes(&self) -> usize {
        self.parts.iter().map(|p| p.memory_usage()).sum::<usize>() + 4096
    }

    fn pieces(&self) -> usize {
        self.parts.len()
    }

    fn search_piece(
        &self,
        piece: usize,
        q: &Query<'_>,
        allow: Option<&RoaringBitmap>,
        k: usize,
    ) -> Result<Vec<Hit>> {
        let Query::Vector { vector, params } = q else {
            return Err(Error::InvalidQuery("vec-hnsw takes a vector query".into()));
        };
        let found = self.search_one(piece, vector, params, allow, k)?;
        Ok(self.hits(found, k))
    }

    fn search(&self, q: &Query<'_>, allow: Option<&RoaringBitmap>, k: usize) -> Result<Vec<Hit>> {
        let Query::Vector { vector, params } = q else {
            return Err(Error::InvalidQuery("vec-hnsw takes a vector query".into()));
        };
        let one = |i: usize| self.search_one(i, vector, params, allow, k);
        // A search that does not hand the parts to the shared pool (D83) searches a file's
        // parts at once, one thread each up to this file's share of the cores (D39, D52). A
        // search across many files already keeps the cores busy with one file each, and
        // threads of its own would only contend with them.
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let files = params
            .get(crate::index::query::FANOUT_PARAM)
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .max(1) as usize;
        let threads = (cores / files).min(self.parts.len());
        let found: Vec<Result<Vec<(u64, f32)>>> = if threads >= MIN_PART_THREADS {
            let per = self.parts.len().div_ceil(threads);
            let idx: Vec<usize> = (0..self.parts.len()).collect();
            std::thread::scope(|s| {
                let handles: Vec<_> = idx
                    .chunks(per)
                    .map(|chunk| s.spawn(move || chunk.iter().map(|i| one(*i)).collect::<Vec<_>>()))
                    .collect();
                handles
                    .into_iter()
                    .flat_map(|h| {
                        h.join().unwrap_or_else(|_| {
                            vec![Err(Error::Internal("a part search panicked".into()))]
                        })
                    })
                    .collect()
            })
        } else {
            (0..self.parts.len()).map(one).collect()
        };
        let mut all = Vec::new();
        for r in found {
            all.extend(r?);
        }
        Ok(self.hits(all, k))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
