//! The index-kind traits (D24, D29): what a kind of index must provide so the
//! bundle, cache, discovery, builder and query code can serve it without knowing it.
//!
//! The traits are public and object-safe; an application registers its own kinds with
//! [`crate::DatabaseBuilder::index_kind`]. A kind builds one sidecar per data file from the
//! file's rows ([`IndexBuilder`]), opens it ([`FileIndex`]), and answers the same queries
//! without a sidecar ([`IndexKind::exact`]), so "sidecar present" and "sidecar absent" are two
//! routes to the same answer.

use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use roaring::RoaringBitmap;
use serde_json::Value;

use super::bundle::{BundleView, SectionSink};
use super::spec::IndexSpec;
use crate::error::Result;

/// How hits of a kind rank.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Order {
    /// Smaller is better (distances).
    Ascending,
    /// Larger is better (scores).
    Descending,
}

/// One hit in one data file: the row offset in the file and its score.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Hit {
    /// The row's offset in the data file, from 0.
    pub offset: u64,
    /// The kind's score, ordered by [`IndexKind::order`].
    pub score: f32,
    /// Whether `score` is exact. Approximate scores (quantised sidecars) are rescored from the
    /// rows before they compete with exact ones.
    pub exact: bool,
}

/// A query, as a kind receives it.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Query<'a> {
    /// A nearest-neighbour query; `params` are the spec's search defaults merged with the
    /// query's overrides.
    Vector {
        /// The query vector.
        vector: &'a [f32],
        /// Search parameters.
        params: &'a Value,
    },
    /// Key lookups by canonical key bytes.
    Key {
        /// The keys, one encoded key each.
        keys: &'a [Vec<u8>],
    },
    /// Bytes an outside kind defines.
    Custom {
        /// The query, in the kind's own encoding.
        bytes: &'a [u8],
        /// Search parameters.
        params: &'a Value,
    },
}

impl Query<'_> {
    /// The search parameters (null for key lookups).
    pub fn params(&self) -> &Value {
        static NULL: Value = Value::Null;
        match self {
            Query::Vector { params, .. } | Query::Custom { params, .. } => params,
            Query::Key { .. } => &NULL,
        }
    }
}

/// What a builder is told about its input.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BuildCtx {
    /// Rows in the source file (all of them, indexed or not).
    pub source_rows: u64,
    /// Bytes the builder should stay under; the driver seals parts beyond it.
    pub budget_bytes: usize,
    /// Threads the builder may use (1 means its own thread only).
    pub threads: usize,
}

impl BuildCtx {
    /// A context for a builder of a file of `source_rows` rows, for driving a kind's builder
    /// directly (in a kind's tests, say).
    pub fn new(source_rows: u64, budget_bytes: usize, threads: usize) -> BuildCtx {
        BuildCtx {
            source_rows,
            budget_bytes,
            threads,
        }
    }
}

/// What a finished build reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildSummary {
    /// Rows the builder indexed (the others are skipped by every route alike).
    pub indexed_rows: u64,
    /// Sealed parts, the last one included.
    pub parts: u32,
}

/// Builds one data file's sidecar. Rows arrive in offset order, as batches holding the spec's
/// columns.
pub trait IndexBuilder: Send {
    /// `first_row` is the file offset of `batch`'s first row.
    fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()>;
    /// Bytes held, including native allocations the builder knows of.
    fn memory_bytes(&self) -> usize;
    /// Moves what is built so far out of memory into its own section(s) of `sink`. A kind
    /// whose sidecar cannot be split may write nothing and keep building.
    fn seal_part(&mut self, sink: &mut dyn SectionSink) -> Result<()>;
    /// Writes the last part and whatever ties the parts together.
    fn finish(self: Box<Self>, sink: &mut dyn SectionSink) -> Result<BuildSummary>;
}

/// An opened sidecar.
pub trait FileIndex: Send + Sync {
    /// Heap bytes held while open (mapped bytes not counted). Bounds the open-handle cache.
    fn resident_bytes(&self) -> usize;
    /// Up to `limit` hits among rows allowed by `allow` (all rows when `None`), best first.
    fn search(
        &self,
        q: &Query<'_>,
        allow: Option<&RoaringBitmap>,
        limit: usize,
    ) -> Result<Vec<Hit>>;
    /// Pieces a search of this index can be split into, searched independently (the parts of
    /// a vector file built in parts, D83). Hits of every piece, merged by score and cut to
    /// `limit`, are what [`FileIndex::search`] returns.
    fn pieces(&self) -> usize {
        1
    }
    /// Up to `limit` hits of piece `piece` (below [`FileIndex::pieces`]).
    fn search_piece(
        &self,
        piece: usize,
        q: &Query<'_>,
        allow: Option<&RoaringBitmap>,
        limit: usize,
    ) -> Result<Vec<Hit>> {
        let _ = piece;
        self.search(q, allow, limit)
    }
    /// Kind-specific calls (key lookups).
    fn as_any(&self) -> &dyn Any;
}

/// Scores rows on the exact route, and rescores approximate hits.
pub trait ExactScorer: Send + Sync {
    /// The score of each row of `batch` (the spec's columns, in order); `None` when the row
    /// can never be a hit (a null or unindexable value).
    fn score(&self, batch: &RecordBatch) -> Result<Vec<Option<f32>>>;
}

/// Where each row of a source file went in a merged file: `new[old offset]`, `None` when the
/// row was dropped.
#[derive(Clone, Debug, Default)]
pub struct RowMap {
    /// The new offset of each old offset.
    pub new: Vec<Option<u64>>,
}

/// One input of a merge: an opened source bundle and where its rows went.
#[non_exhaustive]
pub struct MergeInput<'a> {
    /// The source file's sidecar.
    pub bundle: &'a BundleView,
    /// Where the source file's rows went.
    pub rows: &'a RowMap,
}

impl<'a> MergeInput<'a> {
    /// An input from a source sidecar and its row map, for driving a kind's merge directly.
    pub fn new(bundle: &'a BundleView, rows: &'a RowMap) -> MergeInput<'a> {
        MergeInput { bundle, rows }
    }
}

/// A kind of index. See the module documentation.
pub trait IndexKind: Send + Sync + 'static {
    /// At most 8 ASCII bytes; written into every bundle header.
    fn name(&self) -> &'static str;
    /// Version of the kind's section layout. Part of the spec hash and the bundle header.
    fn format_version(&self) -> u32;
    /// Identifies the library producing the sections (its file format, or a probe of its
    /// output), so a library change yields new sidecars instead of misread ones.
    fn library_fingerprint(&self) -> String;
    /// Whether smaller or larger scores rank first.
    fn order(&self) -> Order;

    /// Checks the columns against the table schema and fills every default, so the stored spec
    /// is frozen.
    fn resolve(&self, spec: &IndexSpec, schema: &Schema) -> Result<IndexSpec>;
    /// Whether a file of `file_rows` rows gets a sidecar; smaller files stay on the exact route.
    fn wants_sidecar(&self, spec: &IndexSpec, file_rows: u64) -> bool;
    /// An upper bound of build memory per indexed row, for sizing parts before building.
    fn build_bytes_per_row(&self, spec: &IndexSpec) -> usize;

    /// A builder for one data file's sidecar.
    fn builder(&self, spec: &IndexSpec, ctx: &BuildCtx) -> Result<Box<dyn IndexBuilder>>;
    /// A builder already holding the merge of `inputs` (built without reading rows), when the
    /// kind can; `None` means "rebuild from the merged file". For compaction.
    fn merge(
        &self,
        spec: &IndexSpec,
        inputs: &[MergeInput<'_>],
        ctx: &BuildCtx,
    ) -> Result<Option<Box<dyn IndexBuilder>>> {
        let _ = (spec, inputs, ctx);
        Ok(None)
    }
    /// Opens a validated sidecar for searching.
    fn open(&self, spec: &IndexSpec, bundle: BundleView) -> Result<Arc<dyn FileIndex>>;

    /// The no-sidecar route: a scorer for `q`. It must rank like the sidecar route (exactly,
    /// or within the recall bound for approximate kinds).
    fn exact(&self, spec: &IndexSpec, q: &Query<'_>) -> Result<Box<dyn ExactScorer>>;
    /// Extra candidates per wanted hit to take from a sidecar and rescore exactly; 0 when the
    /// sidecar's scores are exact.
    fn rerank(&self, spec: &IndexSpec, q: &Query<'_>) -> usize {
        let _ = (spec, q);
        0
    }
    /// Checks a query before any file is touched (dimension, finite values).
    fn validate(&self, spec: &IndexSpec, q: &Query<'_>) -> Result<()> {
        let _ = (spec, q);
        Ok(())
    }
}
